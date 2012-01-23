##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import errno
import imp
import importlib.util
import os
import pickle
import struct
import types
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
    def code_from_source(self, module, source_bytes):
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


class CachingLoader:
    _cache_struct = struct.Struct('!II')

    def get_code(self, module):
        modname = module.__name__
        cache_path = self.get_cache_path(modname)

        source_version = None

        code = None

        if cache_path is not None:
            try:
                source_version = self.get_module_version(modname)
            except NotImplementedError:
                pass
            else:
                code = self.code_from_cache(modname, source_version, cache_path)

        if code is None:
            source_bytes = self.get_source_bytes(modname)
            code = self.code_from_source(module, source_bytes)

            if not sys.dont_write_bytecode and cache_path is not None and source_version is not None:
                data = self.cache_from_code(modname, code, source_version)
                try:
                    self.set_data(cache_path, data)
                    module.__cached__ = cache_path
                except NotImplementedError:
                    pass

        module.__sx_module_version__ = source_version

        return code

    def cache_from_code(self, modname, code, source_version):
        try:
            magic = self.get_cache_magic()
        except NotImplementedError:
            magic = 0

        data = bytearray(self._cache_struct.pack(magic, source_version))
        data.extend(self.marshal_cache_data(code))

        return data

    def code_from_cache(self, modname, source_version, cache_path):
        try:
            data = self.get_data(cache_path)
        except IOError:
            pass
        else:
            try:
                magic, cache_version = self._cache_struct.unpack_from(data)
                cache_data = data[self._cache_struct.size:]

                self.verify_cache(modname, magic, source_version, cache_version, cache_data)

                return self.unmarshal_cache_data(cache_data)

            except ImportError:
                pass

    def get_cache_magic(self):
        raise NotImplementedError

    def marshal_cache_data(self, code):
        return pickle.dumps(code)

    def unmarshal_cache_data(self, data):
        return pickle.loads(data)

    def verify_cache(self, modname, magic, source_version, cache_version, cache_data):
        try:
            expected_magic = self.get_cache_magic()
        except NotImplementedError:
            pass
        else:
            if magic != expected_magic:
                raise ImportError('bad magic number in "{}" cache'.format(modname))

        if source_version is not None and cache_version != source_version:
            raise ImportError('"{}" cache is stale'.format(modname))


class FileLoader:
    def __init__(self, modname, path):
        self._name = modname
        self._path = path

    def is_package(self, fullname):
        filename = self.get_filename(fullname).rpartition(os.path.sep)[2]
        return filename.rsplit('.', 1)[0] == '__init__'

    def get_filename(self, modname):
        return self._path

    def get_module_version(self, modname):
        source_path = self.get_filename(modname)
        return self.path_mtime(source_path)

    def cache_path_from_source_path(self, source_path):
        return imp_utils.cache_from_source(source_path)

    def get_cache_path(self, modname):
        source_path = self.get_filename(modname)
        return self.cache_path_from_source_path(source_path)

    def path_mtime(self, path):
        return int(os.stat(path).st_mtime)

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
