##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import errno
import importlib.abc
import importlib.util
import os
import pickle
import struct
import sys

from . import module as module_types
from . import utils as imp_utils


class LoaderCommon:
    _cache_struct = struct.Struct('!II')

    def get_proxy_module_class(self):
        return None

    def invalidate_module(self, module):
        pass

    def execute_module_code(self, module, code):
        raise NotImplementedError

    @importlib.util.module_for_loader
    def _load_module(self, module):
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
            module.__cached__ = imp_utils.cache_from_source(module.__file__)
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

    def get_cache_magic(self):
        raise NotImplementedError

    def unmarshal_cache_data(self, data):
        return pickle.loads(data)

    def marshal_cache_data(self, code):
        return pickle.dumps(code)

    def verify_cache(self, modname, magic, cache_timestamp, source_mtime, cache_data):
        try:
            expected_magic = self.get_cache_magic()
        except NotImplementedError:
            pass
        else:
            if magic != expected_magic:
                raise ImportError('bad magic number in "{}" cache'.format(modname))

        if source_mtime is not None:
            if cache_timestamp != source_mtime:
                raise ImportError('"{}" cache is stale'.format(modname))

    def code_from_cache(self, modname, data, source_mtime):
        magic, timestamp = self._cache_struct.unpack_from(data)
        data = data[self._cache_struct.size:]

        self.verify_cache(modname, magic, timestamp, source_mtime, data)

        data = self.unmarshal_cache_data(data)

        return data

    def code_from_source(self, modname, source_bytes):
        raise NotImplementedError

    def cache_from_code(self, modname, code, source_mtime):
        try:
            magic = self.get_cache_magic()
        except NotImplementedError:
            magic = 0

        data = bytearray(self._cache_struct.pack(magic, source_mtime))
        data.extend(self.marshal_cache_data(code))

        return data


class SourceLoader(LoaderCommon, importlib.abc.SourceLoader):
    def path_mtime(self, path):
        raise NotImplementedError

    def set_data(self, path, data):
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
        modname = module.__name__
        source_path = self.get_filename(modname)
        cache_path = imp_utils.cache_from_source(source_path)
        source_mtime = None

        code = None

        if cache_path is not None:
            try:
                source_mtime = self.path_mtime(source_path)
            except NotImplementedError:
                pass
            else:
                try:
                    data = self.get_data(cache_path)
                except IOError:
                    pass
                else:
                    try:
                        code = self.code_from_cache(modname, data, source_mtime)
                    except ImportError:
                        pass

        if code is None:
            source_bytes = self.get_source_bytes(modname)

            code = self.code_from_source(module, source_bytes)

            if not sys.dont_write_bytecode and cache_path is not None and source_mtime is not None:
                data = self.cache_from_code(modname, code, source_mtime)
                try:
                    self.set_data(cache_path, data)
                except NotImplementedError:
                    pass

        return code


class _FileLoader:
    def __init__(self, modname, path):
        self._name = modname
        self._path = path

    def get_filename(self, modname):
        return self._path

    def get_data(self, path):
        with open(path, 'rb') as f:
            return f.read()


class SourceFileLoader(_FileLoader, SourceLoader):
    def path_mtime(self, path):
        return int(os.stat(path).st_mtime)

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
