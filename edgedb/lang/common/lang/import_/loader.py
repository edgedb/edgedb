##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import errno
import imp
import os
import pickle
import struct
import sys

from metamagic.utils.debug import debug

from . import cache as caches
from . import module as module_types
from . import utils as imp_utils


class LoaderIface:
    def get_proxy_module_class(self):
        return None

    def new_module(self, fullname):
        return imp.new_module(fullname)

    def invalidate_module(self, module):
        pass

    def get_code(self, modname):
        raise NotImplementedError

    def execute_module_code(self, module, code):
        raise NotImplementedError

    def execute_module(self, module):
        raise NotImplementedError

    def get_source_bytes(self, modname):
        raise NotImplementedError


class LoaderCommon:
    @debug
    def load_module(self, fullname):
        """LOG [lang.import.trace]
        import time

        try:
            tdata = LoaderCommon.__mm_trace_data__
        except AttributeError:
            tdata = LoaderCommon.__mm_trace_data__ = dict(indent=0, timings=[])

        start = time.monotonic()
        print('import: {}+{}'.format('  ' * tdata['indent'], fullname))
        tdata['indent'] += 1
        timing_idx = len(tdata['timings'])
        """

        module = self._load_module_impl(fullname)

        """LOG [lang.import.trace]
        tdata['indent'] -= 1
        end = time.monotonic()
        full_time = end - start
        self_time = full_time - sum(t[2] for t in tdata['timings'][timing_idx:])
        tdata['timings'].append((fullname, full_time, self_time))

        msg = 'import: {}*{} ({:.3f}ms, {:.3f}ms)'
        print(msg.format('  ' * tdata['indent'], fullname, full_time * 1000, self_time * 1000))
        """

        return module

    def _load_module_impl(self, fullname):
        try:
            module = sys.modules[fullname]
            is_reload = True
        except KeyError:
            module = self.new_module(fullname)
            sys.modules[fullname] = module
            is_reload = False

        try:
            module = self._init_module(module)
        except:
            if not is_reload:
                del sys.modules[fullname]
            raise
        else:
            sys.modules[module.__name__] = module

        return module

    def _init_module(self, module):
        orig_mod = module

        proxy_cls = self.get_proxy_module_class()
        proxied = proxy_cls and isinstance(orig_mod, module_types.BaseProxyModule)

        if proxied:
            module = orig_mod.__wrapped__

        reload = getattr(module, '__loaded__', False)

        if reload:
            try:
                orig_dict = module.__odict__
            except AttributeError:
                orig_dict = module.__dict__.copy()

            self.invalidate_module(module)

        module.__file__ = self.get_filename(module.__name__)

        module.__package__ = module.__name__

        modtags = self.get_modtags(module.__name__)
        if modtags:
            module.__mm_module_tags__ = modtags

        if self.is_package(module.__name__):
            module.__path__ = [os.path.dirname(module.__file__)]
        else:
            module.__package__ = module.__name__.rpartition('.')[0]

        module.__loader__ = self

        try:
            try:
                code = self._get_code(module)
                self.execute_module_code(module, code)
            except NotImplementedError:
                self.execute_module(module)
        except ImportError:
            # A reload has failed, revert the module to its original state and re-raise
            if reload:
                module.__dict__.update(orig_dict)

            raise

        try:
            package_tagmap = module.__mm_package_tagmap__
        except AttributeError:
            pass
        else:
            caches.package_tag_maps[module.__name__] = package_tagmap

        try:
            module_class = module.__sx_moduleclass__
        except AttributeError:
            pass
        else:
            _module = module_class(module.__name__)
            _module.__dict__.update(module.__dict__)
            module = _module

        try:
            finalize_load = module.__sx_finalize_load__
        except AttributeError:
            pass
        else:
            if isinstance(finalize_load, collections.Callable):
                try:
                    finalize_load()
                except NotImplementedError:
                    pass

        module.__loaded__ = True

        try:
            track_policy = module.__mm_track_dependencies__
        except AttributeError:
            pass
        else:
            caches.deptracked_modules[module.__name__] = track_policy

        result_mod = module
        if proxy_cls:
            assert issubclass(proxy_cls, module_types.BaseProxyModule)

            if proxied:
                orig_mod.__wrapped__ = module
                result_mod = orig_mod
            else:
                result_mod = proxy_cls(module.__name__, module)

        return result_mod

    def get_modtags(self, modname):
        steps = modname.split('.')

        for i in range(len(steps), 0, -1):
            prefix = '.'.join(steps[:i])
            try:
                tagmap = caches.package_tag_maps[prefix]
            except KeyError:
                pass
            else:
                for pattern, tags in tagmap.items():
                    if pattern.match('.'.join(steps[i:])):
                        return frozenset(tags)

    def update_module_attributes_from_code(self, module, code):
        pass


class SourceLoader:
    def code_from_source(self, modname, source_bytes, *, cache=None):
        raise NotImplementedError

    def get_source_bytes(self, fullname):
        path = self.get_filename(fullname)
        try:
            source_bytes = self.get_data(path)
        except IOError as e:
            raise ImportError('could not load source for "{}"'.format(fullname)) from e

        return source_bytes

    def get_source(self, fullname):
        return self.get_source_bytes(fullname).decode()

    def get_code(self, modname):
        source_bytes = self.get_source_bytes(modname)
        return self.code_from_source(modname, source_bytes)

    def _get_code(self, module):
        return self.get_code(module.__name__)

    def modver_from_path_stats(self, path_stats):
        return int(path_stats['mtime'])

    def _get_module_version(self, modname, imports):
        source_path = self.get_filename(modname)
        try:
            source_stats = self.path_stats(source_path)
        except FileNotFoundError:
            return 0
        else:
            return self.modver_from_path_stats(source_stats)

    def get_module_version(self, modname, imports):
        try:
            modver = caches.modver_cache[modname]
        except KeyError:
            modver = caches.modver_cache[modname] = self._get_module_version(modname, imports)

        return modver

    def is_deptracked(self, modname):
        steps = modname.split('.')

        for i in range(len(steps), 0, -1):
            prefix = '.'.join(steps[:i])
            try:
                deptracking_policy = caches.deptracked_modules[prefix]
            except KeyError:
                pass
            else:
                return deptracking_policy


class ModuleCacheMetaInfo:
    _cache_struct = struct.Struct('!QII')

    def __init__(self, modname, *, magic=None, modver=None, code_offset=None):
        self.modname = modname
        self.magic = magic
        self.modver = modver
        self.code_offset = code_offset

    def marshal(self):
        extras = self.marshal_extras()

        if extras:
            result = bytearray(self._cache_struct.pack(self.magic, self.modver, len(extras)))
            result.extend(extras)
        else:
            result = bytearray(self._cache_struct.pack(self.magic, self.modver, 0))

        return result

    @classmethod
    def unmarshal(cls, modname, data):
        try:
            magic, modver, extra_metainfo_size = cls._cache_struct.unpack_from(data)
        except struct.error as e:
            raise ImportError('could not unpack cached module metainformation') from e

        code_offset = cls._cache_struct.size

        if extra_metainfo_size:
            extra_data = data[code_offset:code_offset + extra_metainfo_size]
            code_offset += extra_metainfo_size

        result = cls(modname, magic=magic, modver=modver, code_offset=code_offset)

        if extra_metainfo_size:
            result.unmarshal_extras(extra_data)

        return result

    def marshal_extras(self):
        pass

    def unmarshal_extras(cls, data):
        pass


GENERAL_MAGIC = 1

class ModuleCache:
    metainfo_class = ModuleCacheMetaInfo

    def __init__(self, modname, loader):
        self._modname = modname
        self._loader = loader
        self._path = None
        self._code = None
        self._code_bytes = None
        self._metainfo = None
        self._metainfo_bytes = None

    def get_magic(self):
        return GENERAL_MAGIC

    @property
    def modname(self):
        return self._modname

    @property
    def path(self):
        if self._path is None:
            self._path = self._loader.get_cache_path(self._modname)

        return self._path

    metainfo_path = path

    def load_metainfo(self):
        if self._metainfo_bytes is None:
            metainfo_path = self.metainfo_path

            try:
                self._metainfo_bytes = self._loader.get_data(metainfo_path)
            except IOError as e:
                raise ImportError('could not read cached module metainformation') from e

        self._metainfo = self.__class__.metainfo_class.unmarshal(self._modname,
                                                                 self._metainfo_bytes)

    def fix(self):
        """Fixup any inconsistencies in the cache object

        Must only be called after the cache object was attempted to be loaded.
        """

        if self._metainfo is None:
            self._metainfo = self.__class__.metainfo_class(self._modname)

    @property
    def metainfo(self):
        """Return cached module metainformation.

        Raises: ImportError if information could not be loaded.
        """
        if self._metainfo is None:
            self.load_metainfo()

        return self._metainfo

    def load_code(self):
        if self._code_bytes is None:
            code_path = self.path

            try:
                self._code_bytes = self._loader.get_data(code_path)
            except IOError as e:
                raise ImportError('could not read cached module code') from e

            if self.path == self.metainfo_path:
                self._code_bytes = self._code_bytes[self.metainfo.code_offset:]

        self._code = self.unmarshal_code(self._code_bytes)
        return self._code

    @debug
    def marshal_code(self, code):
        """LOG [lang.import.cache.marshal]
        from pickle import _Pickler
        import io

        f = io.BytesIO()
        _Pickler(f).dump(code)
        return f.getvalue()
        """

        return pickle.dumps(code)

    @debug
    def unmarshal_code(self, bytedata):
        """LOG [lang.import.cache.unmarshal]
        from pickle import _Unpickler
        import io

        f = io.BytesIO(bytedata)

        try:
            return _Unpickler(f).load()
        except Exception as e:
            import metamagic.utils.markup
            metamagic.utils.markup.dump(e, trim=False)
            raise ImportError('could not unpack cached module code') from e
        """

        try:
            return pickle.loads(bytedata)
        except Exception as e:
            raise ImportError('could not unpack cached module code') from e

    def _get_code(self):
        if self._code is None:
            self.load_code()

        return self._code

    def _set_code(self, code):
        self._code = code

    code = property(_get_code, _set_code)

    def update_metainfo(self):
        try:
            magic = self.get_magic()
        except NotImplementedError:
            magic = 0

        self._metainfo.magic = magic

        try:
            get_deps = self.metainfo.get_dependencies
        except AttributeError:
            imports = None
        else:
            imports = get_deps()

        caches.invalidate_modver_cache(self._modname)
        self._metainfo.modver = self._loader.get_module_version(self._modname, imports)

    def dumpb_metainfo(self):
        if self._metainfo is None:
            self._metainfo = self.__class__.metainfo_class(self._modname)

        self.update_metainfo()
        return self._metainfo.marshal()

    def dumpb_code(self):
        if self._code is not None:
            return self.marshal_code(self._code)

    def dump(self):
        metainfo_path = self.metainfo_path
        code_path = self.path

        metainfo_bytes = self.dumpb_metainfo()
        code_bytes = self.dumpb_code()

        if metainfo_path == code_path:
            self._loader.set_data(code_path, metainfo_bytes + code_bytes)
        else:
            if metainfo_bytes:
                self._loader.set_data(metainfo_path, metainfo_bytes)

            if code_bytes:
                self._loader.set_data(code_path, code_bytes)

    def validate(self):
        metainfo = self.metainfo

        try:
            expected_magic = self.get_magic()
        except NotImplementedError:
            pass
        else:
            if metainfo.magic != expected_magic:
                raise ImportError('bad magic number in "{}" cache'.format(self._modname))

        try:
            get_deps = self.metainfo.get_dependencies
        except AttributeError:
            imports = None
        else:
            imports = get_deps()

        cur_modver = self._loader._get_module_version(self._modname, imports)

        if cur_modver != metainfo.modver:
            raise ImportError('"{}" cache is stale'.format(self._modname))

    def update_module_attributes_early(self, module):
        pass

    def update_module_attributes(self, module):
        module.__sx_modversion__ = self.metainfo.modver
        module.__cached__ = self.path


class CachingLoader:
    def get_code(self, modname):
        cache = self.create_cache(modname)
        code = None

        module = sys.modules[modname]

        if cache is not None:
            try:
                cache.update_module_attributes_early(module)
            except ImportError:
                pass

            try:
                cache.validate()
            except ImportError:
                pass
            else:
                try:
                    code = cache.code
                except ImportError:
                    pass

        if code is None:
            if cache is not None:
                # Fix any inconsistencies in cache object before re-compiling,
                # so that code_from_source can safely work with it.
                cache.fix()

            source_bytes = self.get_source_bytes(modname)
            code = self.code_from_source(modname, source_bytes, cache=cache)

            self.update_module_attributes_from_code(module, code)

            if not sys.dont_write_bytecode and cache is not None:
                cache.code = code
                try:
                    cache.dump()
                except NotImplementedError:
                    pass

        if cache is not None:
            cache.update_module_attributes(module)

        return code

    def create_cache(self, modname):
        """Create and return a new module cache object for the module"""
        return ModuleCache(modname, self)


class FileLoader:
    """Loader protocols for filesystem-based modules"""

    def __init__(self, modname, path):
        self.name = modname
        self.path = path

    def is_package(self, fullname):
        filename = self.get_filename(fullname).rpartition(os.path.sep)[2]
        return filename.rsplit('.', 1)[0] == '__init__'

    def get_filename(self, modname):
        return self.path

    def cache_path_from_source_path(self, source_path):
        return imp_utils.cache_from_source(source_path)

    def get_cache_path(self, modname):
        source_path = self.get_filename(modname)
        return self.cache_path_from_source_path(source_path)

    def path_stats(self, path):
        stats = os.stat(path)
        return {'mtime': stats.st_mtime, 'size': stats.st_size}

    def get_data(self, path):
        with open(path, 'rb') as f:
            return f.read()

    def set_data(self, path, data):
        dir = os.path.dirname(path)

        if dir and not os.path.exists(dir):
            try:
                os.makedirs(dir)
            except IOError as e:
                if e.errno == errno.EACCES:
                    return
                else:
                    raise

        try:
            with open(path, 'wb') as file:
                file.write(data)
        except IOError as e:
            if e.errno == errno.EACCES:
                return
            else:
                raise


class BufferLoader:
    """Loader protocols for buffer-based modules"""

    def __init__(self, modname, buffer, is_package=False, buffer_name='<buffer>'):
        self.name = modname
        self.path = buffer_name
        self.buffer = buffer
        self._is_package = is_package

    def is_package(self, fullname):
        if fullname != self.name:
            raise ValueError('this loader cannot handle {}'.format(fullname))

        return self._is_package

    def get_filename(self, modname):
        return self.path

    def get_data(self, path):
        if path != self.path:
            raise ValueError('this loader cannot handle {}'.format(path))

        return self.buffer

    def set_data(self, path, data):
        raise NotImplementedError


class SourceFileLoader(LoaderCommon, FileLoader, CachingLoader, SourceLoader, LoaderIface):
    pass


class SourceBufferLoader(LoaderCommon, BufferLoader, SourceLoader, LoaderIface):
    pass
