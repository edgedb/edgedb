##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import hashlib
import itertools
import gzip
import os
import logging
import subprocess
import sys
import re

from metamagic.node import Node
from metamagic.utils import config, fs, debug
from metamagic.utils.datastructures import OrderedSet

from .resource import Resource, VirtualFile, AbstractFileSystemResource
from .exceptions import ResourceError


class ResourceBucketError(ResourceError):
    pass


class ResourcePublisherError(ResourceError, fs.FSError):
    pass


class ResourceBucketMeta(fs.BucketMeta):
    def __new__(mcls, name, bases, dct, **kwargs):
        dct['resources'] = None
        dct['published'] = []
        return super().__new__(mcls, name, bases, dct, **kwargs)


class ResourceBucket(fs.BaseBucket, metaclass=ResourceBucketMeta, abstract=True):
    logger = logging.getLogger('metamagic.utils.resource')

    can_contain = (AbstractFileSystemResource, VirtualFile)

    @classmethod
    def url(cls, resource):
        try:
            return getattr(resource, cls.id.hex)
        except KeyError:
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
        cls.resources.append(resource)

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
    def get_import_list(cls, roots, tag=None):
        """Returns a topologically sorted set of modules imported by the specified list of
           root/runtimes pairs.  For each root, the entire import tree is guaranteed to
           be compatible with the associated set of runtimes.
        """

        import_list = OrderedSet(roots)

        def collect(mod, runtimes, *, visited, level, from_import_of=None):
            mod_name = mod.__name__

            visited_previously = mod_name in visited
            if not visited_previously:
                visited.add(mod_name)

            """LOG [resource.collect.tree]
            print('  ' * level, mod_name, 'VISITED' if visited_previously else '')
            """

            package = mod_name.rpartition('.')[0]
            if package:
                # Emulate the implicit import of __init__: the content of __init__
                # must always be evaluated before the content of any of the modules in
                # the corresponding package, but only if the package is in the same runtime
                # class.
                #
                package = sys.modules[package]

                if package.__name__ not in visited:
                    package_runtimes = cls._get_compatible_runtimes(package, (tag,) if tag else ())

                    if runtimes == package_runtimes:
                        """LOG [resource.collect.tree]
                        print('  ' * (level + 1), '--- __init__ ---')
                        """
                        yield from collect(package, runtimes, visited=visited, from_import_of=mod,
                                                                               level=level+1)

            # Put hard dependencies first
            imports = getattr(mod, '__sx_imports__', None)

            if imports:
                """LOG [resource.collect.tree]
                print('  ' * (level + 1), '--- imports ---')
                """
                for impmodname in imports:
                    impmod = sys.modules[impmodname]

                    if impmodname not in visited:
                        yield from collect(impmod, runtimes, visited=visited, level=level+1)
                    elif impmod is from_import_of:
                        # This module has triggered the processing of package
                        yield from_import_of
                        yield from _collect_soft_deps(from_import_of, runtimes, visited, level)

            if visited_previously:
                return

            # Then the module itself and its soft dependencies
            yield mod
            yield from _collect_soft_deps(mod, runtimes, visited, level)

        def _collect_soft_deps(mod, runtimes, visited, level):
            # Then push soft dependencies into the import list
            runtime_imports = getattr(mod, '__mm_runtime_imports__', None)
            if runtime_imports:
                for imname in runtime_imports:
                    ri = sys.modules[imname]
                    import_list.add(ri)

            # Then module derivatives
            derivatives = getattr(mod, '__mm_runtime_derivatives__', None)
            if derivatives:
                """LOG [resource.collect.tree]
                print('  ' * (level + 1), '--- derivatives ---')
                """

                for runtime, derivative in derivatives.items():
                    if runtime in runtimes:
                        yield from collect(derivative, runtimes, visited=visited, level=level+1)

        collected = OrderedSet()
        visited = set()

        while import_list:
            mod = import_list.pop()
            runtimes = cls._get_compatible_runtimes(mod, (tag,) if tag else ())
            collected.update(collect(mod, runtimes, visited=visited, level=0))
            import_list -= collected

        return collected

    @classmethod
    def _get_compatible_runtimes(cls, mod, tags):
        try:
            lang = mod.__language__
        except AttributeError:
            runtimes = None
        else:
            runtimes = lang.get_compatible_runtimes(mod, tags)

        return runtimes

    @classmethod
    @debug.debug
    def _collect_tagged_roots(cls, roots, tags):
        """Returns modules with runtime different from Python from a specified import tree and
           tagged with specified tags.  The modules are grouped by tag.
        """

        tags = frozenset(tags)

        def collect(mod, visited, *, level, consider_imports=True):
            mod_name = mod.__name__

            if mod_name in visited:
                return

            visited.add(mod_name)

            mod_tags = getattr(mod, '__mm_module_tags__', frozenset())
            actual_tags = mod_tags & tags
            runtimes = cls._get_compatible_runtimes(mod, actual_tags)

            imports = getattr(mod, '__sx_imports__', None)
            """LOG [resource.collect.roots]
            print('  ' * level, mod.__name__, tuple(actual_tags), runtimes)
            """

            if runtimes:
                package = mod_name.rpartition('.')[0]
                if package:
                    # The content of __init__ must always be put topologically before the content
                    # of any of the modules in the package.
                    #
                    package = sys.modules[package]

                    if package.__name__ not in visited:
                        yield from collect(package, visited=visited, consider_imports=False,
                                                                     level=level+1)

                if not actual_tags:
                    # There are tags, but none from the requested set
                    """LOG [resource.collect.roots]
                    print('  ' * level, mod.__name__, 'SKIP: NO MATCHING TAGS')
                    """
                    return

                # Found a compatible root
                yield mod, actual_tags
            else:
                # Dig deeper
                if consider_imports:
                    imports = getattr(mod, '__sx_imports__', None)
                    if imports:
                        """LOG [resource.collect.roots]
                        print('  ' * (level + 1), '--- imports ---')
                        """
                        for impmodname in imports:
                            impmod = sys.modules[impmodname]
                            yield from collect(impmod, visited, level=level+1)

                # XXX: consider active derivatives

        collected = OrderedSet()
        visited = set()

        for root in roots:
            collected.update(collect(root, visited, level=0))

        result = {}

        for mod, tags in collected:
            for tag in tags:
                try:
                    result[tag].append(mod)
                except KeyError:
                    result[tag] = [mod]

        return result

    @classmethod
    @debug.debug
    def build(cls):
        """Called during Node.build phase"""

        node = Node.active
        if not node.packages:
            cls.logger.warning('node {} does not have any "packages" defined, this may result '
                               'in no resources being published'.format(node))

        if not node.tags:
            cls.logger.warning('node {} does not have any tags defined, this may result '
                               'in no resources being published'.format(node))

        buckets = {}

        roots = cls._collect_tagged_roots(node.packages, node.tags)

        """LOG [resource.collect.roots]
        print('RUNTIME ROOTS BY TAG ============================')
        """

        for tag, tag_mods in roots.items():
            """LOG [resource.collect.roots]
            print('{} --------------------'.format(tag))
            import metamagic.utils.markup
            metamagic.utils.markup.dump(tag_mods, trim=False)
            print('-----------------------------------')
            """

            bucket = getattr(tag, 'resource_bucket', None)
            if bucket is None:
                continue

            # Go through the topologically sorted list of imports from this root
            # at segregate the modules into buckets.
            #
            mod_list = cls.get_import_list(tag_mods, tag)

            for mod in mod_list:
                if not isinstance(mod, Resource):
                    continue

                resources = Resource._list_resources(mod)

                try:
                    bucket_mods = buckets[bucket]
                except KeyError:
                    buckets[bucket] = OrderedSet(resources)
                else:
                    bucket_mods.update(resources)

        for bucket, mods in buckets.items():
            for mod in mods:
                if isinstance(mod, bucket.can_contain):
                    bucket.add(mod)

        for bucket in cls._iter_children(include_self=True):
            if bucket.resources:
                for backend in bucket.get_backends():
                    backend.publish_bucket(bucket)

    @classmethod
    def sync(cls, modified):
        for bucket in cls._iter_children(include_self=True):
            if bucket.resources:
                for backend in bucket.get_backends():
                    backend.sync_bucket(bucket, modified)


class BaseResourceBackend(fs.backends.BaseFSBackend):
    def publish_bucket(self, bucket):
        raise NotImplementedError


class ResourceFSBackend(BaseResourceBackend):
    def __init__(self, *, path, pub_path, **kwargs):
        super().__init__(path=path, **kwargs)
        self.pub_path = pub_path

    def sync_bucket(self, bucket, modified):
        resources = OrderedSet()
        for mod in modified:
            if mod in bucket.resources:
                resources.add(mod)

        bucket_id, bucket_path, bucket_pub_path = self._bucket_conf(bucket)
        self._publish_bucket(bucket, resources, bucket_id, bucket_path, bucket_pub_path)

    def publish_bucket(self, bucket):
        bucket.published = OrderedSet()
        bucket_id, bucket_path, bucket_pub_path = self._bucket_conf(bucket)
        self._publish_bucket(bucket, bucket.resources, bucket_id, bucket_path, bucket_pub_path)

    def _bucket_conf(self, bucket):
        bucket_id = bucket.id.hex
        bucket_path = os.path.join(self.path, bucket_id)
        os.makedirs(bucket_path, exist_ok=True, mode=(0o777 - self.umask))
        bucket_pub_path = os.path.join(self.pub_path, bucket_id)
        return bucket_id, bucket_path, bucket_pub_path

    @debug.debug
    def _publish_bucket(self, bucket, resources, bucket_id, bucket_path, bucket_pub_path):
        for resource in resources:
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
            pub_path = os.path.join(bucket_pub_path, resource.__sx_resource_public_path__)
            setattr(resource, bucket_id, pub_path)

            bucket.published.add(resource)

    def _fix_css_links(self, source, bucket_path, *, rx=re.compile('///([^/]+)///')):
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
            return os.path.join(bucket_path, m.group(1))
        return rx.sub(cb, source)

    def _publish_fs_resource(self, bucket_path, bucket_pub_path, resource):
        src_path = resource.__sx_resource_path__
        dest_path = os.path.join(bucket_path, resource.__sx_resource_public_path__)

        if os.path.exists(dest_path):
            if os.path.islink(dest_path):
                if os.stat(dest_path).st_ino == os.stat(src_path).st_ino:
                    # same file
                    return
                else:
                    os.unlink(dest_path)

            else:
                # not a symlink, let's just remove it
                if os.path.isfile(dest_path):
                    os.remove(dest_path)
                else:
                    os.rmdir(dest_path)

        elif os.path.islink(dest_path):
            # broken symlink
            os.unlink(dest_path)

        os.symlink(src_path, dest_path)

    def _publish_virtual_resource(self, bucket_path, bucket_pub_path, resource):
        dest_path = os.path.join(bucket_path, resource.__sx_resource_public_path__)

        if os.path.exists(dest_path):
            os.remove(dest_path)

        source = resource.__sx_resource_get_source__()

        if dest_path.endswith('.css') and b'///' in source:
            #: Read the comment in "_fix_css_links"
            source = self._fix_css_links(source.decode('utf-8'), bucket_pub_path).encode('utf-8')

        with open(dest_path, 'wb+') as dest:
            dest.write(source)


class OptimizedFSBackend(ResourceFSBackend):
    '''Compresses javascript and css files using YUI Compressor.
    Use it for production purposes.'''

    yui_compressor_path = config.cvalue('/usr/bin/yuicompressor', type=str,
                                        doc='Path to YUI Compressor executable')

    yui_compressor_jar = config.cvalue(None, type=str,
                                       doc='Path to YUI Compressor jar file, if no run '
                                           'script (yui_compressor_path) is available')

    java_path = config.cvalue('/usr/bin/java', type=str,
                              doc='Path to java executable, used in conjunction with '
                                  'yui_compressor_jar config option')

    gzip_output = config.cvalue(False, type=bool)
    compiled_module_name = config.cvalue('__compiled__', type=str)

    def _get_file_hash(self, filename):
        md5 = hashlib.md5()

        with open(filename, 'rb') as out:
            while True:
                data = out.read(4096)
                if not data:
                    break
                md5.update(data)

        return md5.hexdigest()

    def sync_bucket(self, bucket, modified):
        self._publish_bucket(bucket)

    def _publish_bucket(self, bucket, resources, bucket_id, bucket_path, bucket_pub_path):
        from metamagic.utils.lang.javascript import BaseJavaScriptModule, CompiledJavascriptModule
        from metamagic.rendering.css import ScssModule, ProxyScssModule, \
                                            CssModule, CompiledScssModule

        compressor_path = self.yui_compressor_path
        if compressor_path is None or self.yui_compressor_jar is not None:
            if self.yui_compressor_jar is None:
                raise ResourcePublisherError('Please configure {}.{}.yui_compressor_path '
                                             'config option'.
                                             format(self.__class__.__module__,
                                                    self.__class__.__name__))
            compressor_path = self.java_path + ' -jar ' + self.yui_compressor_jar

        js_deps = OrderedSet()
        css_deps = OrderedSet()

        for res in resources:
            if isinstance(res, BaseJavaScriptModule):
                js_deps.add(res)

            elif isinstance(res, (ScssModule, ProxyScssModule, CssModule)):
                css_deps.add(res)

            else:
                if isinstance(res, AbstractFileSystemResource):
                    self._publish_fs_resource(bucket_path, bucket_pub_path, res)
                elif isinstance(res, VirtualFile):
                    self._publish_virtual_resource(bucket_path, bucket_pub_path, res)
                else:
                    continue

                # Read XXX comment in "ResourceFSBackend._publish_bucket"
                pub_path = os.path.join(bucket_pub_path, res.__sx_resource_public_path__)
                setattr(res, bucket_id, pub_path)
                bucket.published.append(res)

        compiled_name = self.compiled_module_name
        compiled_name += (bucket.__module__ + '.' + bucket.__name__).replace('.', '_')

        for type, mod_cls, deps in (('js', CompiledJavascriptModule, js_deps),
                                    ('css', CompiledScssModule, css_deps)):

            output = os.path.abspath(os.path.join(bucket_path, '{}.{}'.format(compiled_name, type)))

            with open(output, 'wb') as out:
                for mod in deps:
                    if isinstance(mod, VirtualFile):
                        source = mod.__sx_resource_get_source__()
                        if type == 'css':
                            #: Read the comment in "_fix_css_links"
                            source = self._fix_css_links(source.decode('utf-8'), bucket_pub_path) \
                                                                                    .encode('utf-8')
                        out.write(source)
                    else:
                        with open(mod.__sx_resource_path__, 'rb') as i:
                            out.write(i.read())

                    if type == 'js':
                        out.write(b'\n;\n')

            command = [compressor_path,
                       '--line-break', '500',
                       '-v',
                       '--type', type,
                       output, '-o', output]

            command = ' '.join(command)

            status, result = subprocess.getstatusoutput(command)
            if status:
                raise ResourcePublisherError('{}\n\nFILE: {}'.format(result, output))

            output_gz = output + '.gz'
            if self.gzip_output:
                with open(output, 'rb') as f_in:
                    with gzip.open(output_gz, 'wb', compresslevel=9) as f_out:
                        f_out.writelines(f_in)

                hash = self._get_file_hash(output_gz)

                stats = os.stat(output)
                os.utime(output_gz, (stats.st_atime, stats.st_mtime))

            else:
                hash = self._get_file_hash(output)

                if os.path.exists(output_gz):
                    os.remove(output_gz)

            result = mod_cls(output.encode('utf-8'), compiled_name,
                             '{}.{}?_cache={}'.format(compiled_name, type, hash))
            pub_path = os.path.join(bucket_pub_path, result.__sx_resource_public_path__)
            setattr(result, bucket_id, pub_path)
            bucket.published.append(result)


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
    from metamagic.rendering.css import ScssModule, ProxyScssModule, CssModule
    collected = _collect_published_resources(bucket, (ScssModule, ProxyScssModule, CssModule))

    return '\n'.join(('<link href="{}" type="text/css" rel="stylesheet"/>'.format(path)
                                                                        for path in collected))
