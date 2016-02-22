##
# Copyright (c) 2012, 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import csscompressor
import hashlib
import itertools
import importlib
import gzip
import os
import logging
import subprocess
import tempfile
import shutil
import sys
import re

from metamagic.node import Node
from metamagic.utils import config, fs, debug
from metamagic.utils.datastructures import OrderedSet, OrderedIndex

from .resource import Resource, VirtualFile, AbstractFileSystemResource, AbstractFileResource
from .resource import EmptyResource
from .resource import call_publication_hooks, is_standalone
from .exceptions import ResourceError


class ResourceBucketError(ResourceError):
    pass


class ResourcePublisherError(ResourceError, fs.FSError):
    pass


class ResourceBucketMeta(fs.BucketMeta):
    def __new__(mcls, name, bases, dct, **kwargs):
        dct['resources'] = None
        cls = super().__new__(mcls, name, bases, dct, **kwargs)
        cls._init_published_list()
        return cls

    def _init_published_list(cls):
        def _published_key(res):
            try:
                gpp = res.__sx_resource_get_public_path__
            except AttributeError:
                result = res
            else:
                result = gpp()
            return result
        cls.published = OrderedIndex(key=_published_key)


class ResourceBucket(fs.BaseBucket, metaclass=ResourceBucketMeta, abstract=True):
    logger = logging.getLogger('metamagic.utils.resource')

    can_contain = (AbstractFileSystemResource, VirtualFile)

    @classmethod
    def url(cls, resource):
        try:
            return getattr(resource, cls.id.hex)
        except AttributeError:
            raise ResourceBucketError('unable to provide a url for an unpublished resource {!r}'.
                                      format(resource)) from None

    @classmethod
    def add(cls, resource):
        if not isinstance(resource, cls.can_contain):
            raise ResourceBucketError('resource bucket {!r} can\'t contain resource {!r}'.
                                      format(cls, resource))

        cls._error_if_abstract()
        if cls.resources is None:
            cls.resources = OrderedSet()
        cls.resources.add(resource)

    @classmethod
    def set_backends(cls, *backends):
        if len(backends) != 1:
            raise ResourceBucketError('invalid backend {!r} for resource bucket {!r}, '
                                      'an instance of BaseResourceBackend is expected'.
                                      format(backend, cls))
        super().set_backends(*backends)

    @classmethod
    def validate_backend(cls, backend):
        if not isinstance(backend, BaseResourceBackend):
            raise ResourceBucketError('invalid backend {!r} for resource bucket {!r}, '
                                      'an instance of BaseResourceBackend is expected'.
                                      format(backend, cls))

    @classmethod
    @debug.debug
    def build(cls, increment_set=None):
        """Called during Node.build phase"""

        node = Node.active
        if not node.packages:
            cls.logger.warning('node {} does not have any "packages" defined, this may result '
                               'in no resources being published'.format(node))

        if not node.targets:
            cls.logger.warning('node {} does not have any "targets" defined, this may result '
                               'in no resources being published'.format(node))

        buckets = {}
        recursive = increment_set is None

        for target in node.targets:
            if increment_set is None:
                target_buckets = target.collect_modules(node.packages, recursive=recursive)
            else:
                target_buckets = target.collect_modules(increment_set, recursive=recursive,
                                                                       all_derivatives=True)

            repacked = {}

            for bucket, modules in target_buckets.items():
                for mod in modules:
                    if not isinstance(mod, Resource):
                        continue

                    resources = Resource._list_resources(mod)

                    try:
                        bucket_mods = repacked[bucket]
                    except KeyError:
                        repacked[bucket] = OrderedSet(resources)
                    else:
                        bucket_mods.update(resources)

            for bucket, modules in repacked.items():
                try:
                    buckets[bucket].update(modules)
                except KeyError:
                    buckets[bucket] = modules

        filtered = {}

        for bucket, mods in buckets.items():
            if bucket is not None:
                for mod in mods:
                    if isinstance(mod, bucket.can_contain):
                        try:
                            filtered[bucket].add(mod)
                        except KeyError:
                            filtered[bucket] = OrderedSet([mod])

                        bucket.add(mod)

        for bucket in cls._iter_children(include_self=True):
            if bucket is not None:
                try:
                    resources = filtered[bucket]
                except KeyError:
                    pass
                else:
                    for backend in bucket.get_backends():
                        r = resources if increment_set else bucket.resources
                        backend.publish_bucket(bucket, r)


class BaseResourceBackend(fs.backends.BaseFSBackend):
    def publish_bucket(self, bucket):
        raise NotImplementedError


class ResourceFSBackend(BaseResourceBackend):
    assume_built = config.cvalue(False, type=bool)
    symlink_files = config.cvalue(True, type=bool)

    def __init__(self, *, path, pub_path, **kwargs):
        super().__init__(path=path, **kwargs)
        self.pub_path = pub_path

    def publish_bucket(self, bucket, resources=None):
        if resources is None:
            resources = bucket.resources
        else:
            if bucket.resources:
                resources = resources & bucket.resources
            else:
                resources = OrderedSet()

        if resources:
            bucket_id, bucket_path, bucket_pub_path = self._bucket_conf(bucket)
            self._publish_bucket(bucket, resources, bucket_id, bucket_path, bucket_pub_path)

    def _bucket_conf(self, bucket):
        bucket_id = bucket.id.hex
        bucket_path = os.path.join(self.path, bucket_id)
        os.makedirs(bucket_path, exist_ok=True, mode=(0o777 - self.umask))
        bucket_pub_path = os.path.join(self.pub_path, bucket_id)
        return bucket_id, bucket_path, bucket_pub_path

    @debug.debug
    def _publish_bucket(self, bucket, resources, bucket_id, bucket_path, bucket_pub_path):
        if self.assume_built:
            return

        for resource in resources:
            resource = call_publication_hooks(resource, bucket, self)

            if isinstance(resource, EmptyResource):
                """LINE [resource.publish.skip] Skipping empty resource
                bucket.__name__, resource
                """
                continue

            if isinstance(resource, AbstractFileSystemResource):
                self._publish_fs_resource(bucket_path, bucket_pub_path, resource)
            elif isinstance(resource, VirtualFile):
                self._publish_virtual_resource(bucket_path, bucket_pub_path, resource)
            else:
                continue

            """LINE [resource.publish] Published
            bucket.__name__, resource
            """

            # XXX
            # We assign here a pub-url for the current bucket to later be able
            # to tell front-end where to download them from.  Definitely need
            # more sane API.
            #
            # P.S. The idea is that a bucket may only have one publisher, and
            # hence, be published only once.  But resources may belong to many
            # buckets, and hence published many times.
            #
            pub_name = resource.__sx_resource_get_public_path__()
            pub_path = os.path.join(bucket_pub_path, pub_name)

            if isinstance(resource, AbstractFileResource):
                cache = resource.__sx_resource_get_cache_tag__()
                if cache:
                    pub_path += '?_cache={}'.format(cache)

            setattr(resource, bucket_id, pub_path)

            bucket.published.add(resource)

    def _fix_css_links(self, source, bucket_pub_path, *, rx=re.compile('///([^/]+)///')):
        # XXX
        #
        # This method patches links to media resources.
        # The problem with the current state of SCSS, is that in its current
        # architecture it's extremely slow.  The only possible way to speed it
        # up without a complete rewrite of compiler is to cache produced CSS.
        # However, this cache is created during the import phase, when it's
        # unknown what Node & and what configuration a system has.  Hence,
        # there is no way of guessing at what public URL resources will be
        # available.  Hence, this hack: "url" function in SCSS produces URLs like
        # "///media.module.name.object.name///", which are replaced by real
        # URLs here.
        #
        # NOTE: In case of refactoring, please update links to this comment
        # in "rendering.media" and this module.
        def cb(m):
            return os.path.join(bucket_pub_path, m.group(1))
        return rx.sub(cb, source)

    def _publish_fs_resource(self, bucket_path, bucket_pub_path, resource):
        src_path = resource.__sx_resource_path__
        dest_path = os.path.join(bucket_path, resource.__sx_resource_get_public_path__())

        if os.path.exists(dest_path):
            if os.path.islink(dest_path):
                if (os.stat(dest_path).st_ino == os.stat(src_path).st_ino and
                        self.symlink_files):
                    # same file
                    return
                else:
                    os.unlink(dest_path)

            else:
                # not a symlink, let's just remove it
                if os.path.isfile(dest_path):
                    os.remove(dest_path)
                else:
                    shutil.rmtree(dest_path)

        elif os.path.islink(dest_path):
            # broken symlink
            os.unlink(dest_path)

        if self.symlink_files:
            os.symlink(src_path, dest_path)
        else:
            if os.path.isdir(src_path):
                shutil.copytree(src_path, dest_path)
            else:
                shutil.copy2(src_path, dest_path)

    def _publish_virtual_resource(self, bucket_path, bucket_pub_path, resource):
        dest_path = os.path.join(bucket_path, resource.__sx_resource_get_public_path__())

        if os.path.exists(dest_path):
            os.remove(dest_path)

        source = resource.__sx_resource_get_source__()

        if dest_path.endswith('.css') and b'///' in source:
            #: Read the comment in "_fix_css_links"
            source = self._fix_css_links(source.decode('utf-8'), bucket_pub_path).encode('utf-8')

        with open(dest_path, 'wb+') as dest:
            dest.write(source)


class OptimizedFSBackend(ResourceFSBackend):
    '''Performs source compression for resources
    Use it for production purposes.'''

    gzip_output = config.cvalue(True, type=bool)
    js_sourcemaps = config.cvalue(True, type=bool)
    compiled_module_name = config.cvalue('__compiled__', type=str)
    closure_compiler_executable = config.cvalue('closure-compiler', type=str)
    pretty_output = config.cvalue(False, type=bool)
    logger = logging.getLogger('metamagic.utils.resource')

    def _get_file_hash(self, filename):
        md5 = hashlib.md5()

        with open(filename, 'rb') as out:
            while True:
                data = out.read(4096)
                if not data:
                    break
                md5.update(data)

        return md5.hexdigest()

    def _compiled_name(self, bucket, ext:str, suffix=None):
        name = self.compiled_module_name
        name += (bucket.__module__ + '.' + bucket.__name__).replace('.', '_')
        if suffix is not None:
            name += '.' + str(suffix)
        name += '.' + ext
        return name

    def _gzip_file(self, name):
        output_gz = name + '.gz'

        with open(name, 'rb') as f_in:
            with gzip.open(output_gz, 'wb', compresslevel=9) as f_out:
                f_out.writelines(f_in)

        stats = os.stat(name)
        try:
            os.utime(output_gz, (stats.st_atime, stats.st_mtime))
        except PermissionError as e:
            # This happens when don't own the file.  Not fatal.
            self.logger.warning('cannot set timestamp on gzipped resource',
                                exc_info=e)

    def _optimize_js(self, mods, bucket, bucket_id, bucket_path,
                                                    bucket_pub_path):

        from metamagic.utils.lang.javascript import CompiledJavascriptModule

        out_short_name = self._compiled_name(bucket, 'js')
        out_name = os.path.abspath(os.path.join(bucket_path, out_short_name))

        command = []

        command.append('cd "{}";'.format(bucket_path))
        command.append(self.closure_compiler_executable)

        command.append('--compilation_level SIMPLE_OPTIMIZATIONS')
        command.append('--warning_level QUIET')
        command.append('--language_in ECMASCRIPT5')
        command.append('--third_party')

        if self.pretty_output:
            command.append('--formatting PRETTY_PRINT')

        if self.js_sourcemaps:
            map_name = '{}.map'.format(out_short_name)

            command.append('--source_map_format V3')
            command.append('--create_source_map "{}"'.format(map_name))

        for mod in mods:
            if is_standalone(mod):
                # Skip modules marked as standalone
                bucket.published.add(mod)
            else:
                path = mod.__sx_resource_get_public_path__()
                command.append('--js "{}"'.format(path))


        command.append('--js_output_file "{}"'.format(out_short_name))

        command = ' '.join(command)

        status, result = subprocess.getstatusoutput(command)
        if status:
            raise ResourcePublisherError('{}\n\nFILE: {}'.format(result, out_name))

        if self.js_sourcemaps:
            with open(out_name, 'at') as f:
                f.write('\n\n//# sourceMappingURL={}.map\n'.format(out_short_name))

        if self.gzip_output:
            self._gzip_file(out_name)

        hash = self._get_file_hash(out_name)

        result = CompiledJavascriptModule(b'', out_short_name,
                                          '{}?_cache={}'.format(out_short_name, hash))
        pub_path = os.path.join(bucket_pub_path, result.__sx_resource_get_public_path__())
        setattr(result, bucket_id, pub_path)
        bucket.published.add(result)

    def _optimize_css(self, mods, bucket, bucket_id, bucket_path, bucket_pub_path):
        from metamagic.rendering.css import CompiledCSSModule

        buf = []
        for mod in mods:
            if is_standalone(mod):
                # Skip modules marked as standalone
                bucket.published.add(mod)
            else:

                if isinstance(mod, VirtualFile):
                    source = mod.__sx_resource_get_source__()
                    #: Read the comment in "_fix_css_links"
                    source = self._fix_css_links(source.decode('utf-8'),
                                                 bucket_pub_path)

                else:
                    with open(mod.__sx_resource_path__, 'rt') as i:
                        source = i.read()

                buf.append(source)

        wrap = 1000
        if self.pretty_output:
            wrap = 1

        compressed = csscompressor.compress_partitioned('\n'.join(buf),
                                                        max_linelen=wrap,
                                                        max_rules_per_file=3500)

        for idx, compressed_part in enumerate(compressed):
            name_suffix = idx if len(compressed) > 1 else None
            out_short_name = self._compiled_name(bucket, 'css', suffix=name_suffix)
            out_name = os.path.abspath(os.path.join(bucket_path, out_short_name))

            with open(out_name, 'wt') as f:
                f.write(compressed_part)

            if self.gzip_output:
                self._gzip_file(out_name)

            hash = self._get_file_hash(out_name)

            outmod = CompiledCSSModule(b'', out_short_name,
                                       '{}?_cache={}'.format(out_short_name, hash))
            pub_path = os.path.join(bucket_pub_path,
                                    outmod.__sx_resource_get_public_path__())
            setattr(outmod, bucket_id, pub_path)
            bucket.published.add(outmod)

    def _publish_bucket(self, bucket, resources, bucket_id, bucket_path, bucket_pub_path):
        if self.assume_built:
            return

        from metamagic.utils.lang.javascript import BaseJavaScriptModule
        from metamagic.rendering.css import BaseCSSModule, CSSMixinDerivative

        # Publish everything.
        # We'll also publish the optimized versions, but having non-optimized
        # resources published is required for JS Source Maps to function correctly.
        #
        super()._publish_bucket(bucket, resources, bucket_id, bucket_path, bucket_pub_path)

        # Collect JS & CSS resources
        #
        js_deps = OrderedSet()
        css_deps = OrderedSet()

        for res in bucket.published:
            if isinstance(res, BaseJavaScriptModule):
                js_deps.add(res)

            elif isinstance(res, BaseCSSModule) and not isinstance(res, CSSMixinDerivative):
                css_deps.add(res)

        # Reset bucket -- forget about all published resources in it
        bucket._init_published_list()

        if js_deps:
            self._optimize_js(js_deps, bucket, bucket_id, bucket_path, bucket_pub_path)

        if css_deps:
            self._optimize_css(css_deps, bucket, bucket_id, bucket_path, bucket_pub_path)


def _collect_published_resources(bucket, types):
    collected = []
    for mod in bucket.published:
        if isinstance(mod, types):
            collected.append(bucket.url(mod))
    return collected


def render_script_tags(bucket):
    from metamagic.utils.lang.javascript import BaseJavaScriptModule
    collected = _collect_published_resources(bucket, BaseJavaScriptModule)

    return '\n'.join(('<script src="{}" type="text/javascript"></script>'.format(path)
                                                                        for path in collected))


def render_style_tags(bucket):
    from metamagic.rendering.css import BaseCSSModule
    collected = _collect_published_resources(bucket, BaseCSSModule)

    return '\n'.join(('<link href="{}" type="text/css" rel="stylesheet"/>'.format(path)
                                                                        for path in collected))
