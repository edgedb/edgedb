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

from . import module as module_types
from . import utils as imp_utils


class LoaderIface:
    def get_proxy_module_class(self):
        return None

    def new_module(self, fullname):
        return imp.new_module(fullname)

    def invalidate_module(self, module):
        pass

    def get_code(self, module):
        raise NotImplementedError

    def execute_module_code(self, module, code):
        raise NotImplementedError

    def execute_module(self, module):
        raise NotImplementedError

    def get_source_bytes(self, modname):
        raise NotImplementedError


class LoaderCommon:
    def _load_module(self, fullname):
        try:
            module = sys.modules[fullname]
            is_reload = True
        except KeyError:
            module = self.new_module(fullname)
            sys.modules[fullname] = module
            is_reload = False

        try:
            return self._init_module(module)
        except:
            if not is_reload:
                del sys.modules[fullname]
            raise

    def _init_module(self, module):
        orig_mod = module

        proxy_cls = self.get_proxy_module_class()
        proxied = proxy_cls and isinstance(orig_mod, module_types.BaseProxyModule)

        if proxied:
            module = orig_mod.__wrapped__

        reload = getattr(module, '__loaded__', False)

        if reload:
            self.invalidate_module(module)

        module.__file__ = self.get_filename(module.__name__)

        module.__package__ = module.__name__

        if self.is_package(module.__name__):
            module.__path__ = [os.path.dirname(module.__file__)]
        else:
            module.__package__ = module.__name__.rpartition('.')[0]

        module.__loader__ = self

        try:
            code = self.get_code(module)
            self.execute_module_code(module, code)
        except NotImplementedError:
            self.execute_module(module)

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

        result_mod = module
        if proxy_cls:
            assert issubclass(proxy_cls, module_types.BaseProxyModule)

            if proxied:
                orig_mod.__wrapped__ = module
                result_mod = orig_mod
            else:
                result_mod = proxy_cls(module.__name__, module)

        sys.modules[module.__name__] = result_mod
        return result_mod


class SourceLoader:
    def code_from_source(self, module, source_bytes, *, cache=None):
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

    def get_code(self, module):
        source_bytes = self.get_source_bytes(module.__name__)
        return self.code_from_source(module, source_bytes)

    def modver_from_path_stats(self, path_stats):
        return int(path_stats['mtime'])

    def get_module_version(self, modname, cache):
        source_path = self.get_filename(modname)
        source_stats = self.path_stats(source_path)
        return self.modver_from_path_stats(source_stats)


class ModuleCacheMetaInfo:
    _cache_struct = struct.Struct('!III')

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

    def marshal_code(self, code):
        from pickle import _Pickler
        import io

        f = io.BytesIO()
        _Pickler(f).dump(code)
        res = f.getvalue()
        return res

        return pickle.dumps(code)

    def unmarshal_code(self, bytedata):
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
        self._metainfo.modver = self._loader.get_module_version(self._modname, self)

    def dumpb_metainfo(self):
        if self._metainfo is None:
            self._metainfo = self.__class__.metainfo_class(self._modname)

        self.update_metainfo()
        return self._metainfo.marshal()

    def dumpb_code(self):
        return self.marshal_code(self._code)

    def dump(self):
        metainfo_path = self.metainfo_path
        code_path = self.path

        metainfo_bytes = self.dumpb_metainfo()
        code_bytes = self.dumpb_code()

        if metainfo_path == code_path:
            self._loader.set_data(code_path, metainfo_bytes + code_bytes)
        else:
            self._loader.set_data(metainfo_path, metainfo_bytes)
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

        cur_modver = self._loader.get_module_version(self._modname, self)

        if cur_modver != metainfo.modver:
            raise ImportError('"{}" cache is stale'.format(self._modname))


class CachingLoader:
    def get_code(self, module):
        modname = module.__name__
        cache = self.create_cache(modname)

        code = None

        if cache is not None:
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
            code = self.code_from_source(module, source_bytes, cache=cache)

            if not sys.dont_write_bytecode and cache is not None:
                module.__sx_modversion__ = cache.metainfo.modver

                cache.code = code

                try:
                    cache.dump()
                except NotImplementedError:
                    pass
                else:
                    module.__cached__ = cache.path

        else:
            module.__sx_modversion__ = cache.metainfo.modver

        return code

    def create_cache(self, modname):
        """Create and return a new module cache object for the module"""
        return ModuleCache(modname, self)


class FileLoader:
    def __init__(self, modname, path):
        self._name = modname
        self._path = path

    def is_package(self, fullname):
        filename = self.get_filename(fullname).rpartition(os.path.sep)[2]
        return filename.rsplit('.', 1)[0] == '__init__'

    def get_filename(self, modname):
        return self._path

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


class SourceFileLoader(LoaderCommon, FileLoader, CachingLoader, SourceLoader, LoaderIface):
    pass
