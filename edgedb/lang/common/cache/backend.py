##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import time

from metamagic.utils import config, abc, debug
from . import exceptions, abstract


class CacheBackendError(exceptions.CacheError):
    pass


class Backend(abstract.Backend):
    pass


class BlockingBackend(Backend):
    @abc.abstractmethod
    def get_blocking(self, key:bytes):
        """Must raise LookupError in case of not found key"""

    @abc.abstractmethod
    def set_blocking(self, key:bytes, value:bytes, expiry:float=None):
        pass

    @abc.abstractmethod
    def delete_blocking(self, key:bytes):
        """Must raise LookupError in case of not found key"""

    @abc.abstractmethod
    def contains_blocking(self, key:bytes):
        pass


class NonBlockingBackend(Backend):
    @abc.abstractmethod
    def get_nonblocking(self, key:bytes):
        """Must raise LookupError in case of not found key"""

    @abc.abstractmethod
    def set_nonblocking(self, key:bytes, value:bytes, expiry:float=None):
        pass

    @abc.abstractmethod
    def delete_nonblocking(self, key:bytes):
        """Must raise LookupError in case of not found key"""

    @abc.abstractmethod
    def contains_nonblocking(self, key:bytes):
        pass


class MemoryBackend(NonBlockingBackend):
    max_size = config.cvalue(32 * 1024 * 1024, type=int, doc='Maximum cache size in bytes')
    max_item_size = config.cvalue(1024 * 1024, type=int, doc='Maximum cache item size in bytes')

    def __init__(self):
        self._data = collections.OrderedDict()
        self._size = 0

    @property
    def size(self):
        return self._size

    @debug.debug
    def set_nonblocking(self, key:bytes, data:bytes, expiry:float=None):
        data_size = len(data)

        if data_size > self.max_item_size:
            raise CacheBackendError('data size ({}) of key {!r} exceeds ' \
                                     'maximum cache item size {}'. \
                                     format(data_size, key, self.max_item_size))

        if key in self._data:
            self.delete_nonblocking(key)

        while self._size + data_size > self.max_size and self._data:
            item = self._data.popitem(False)
            self._size -= len(item[1][0])

        self._size += data_size

        if expiry is not None:
            expiry = time.time() + expiry

        self._data[key] = (data, expiry)

        """LINE [cache.core] MEMORY PROVIDER SET
        key
        """

    @debug.debug
    def get_nonblocking(self, key:bytes):
        try:
            data, expiry = self._data[key]
        except KeyError:
            """LINE [cache.core] MEMORY PROVIDER GET FAIL
            key
            """
            raise LookupError('cache key {!r} not found'.format(key))
        else:
            """LINE [cache.core] MEMORY PROVIDER FOUND
            key
            """

            if expiry is not None and expiry < time.time():
                del self._data[key]
                raise LookupError('cache key {!r} expired'.format(key))

            self._data.move_to_end(key)
            return data

    def contains_nonblocking(self, key:bytes):
        return key in self._data

    def delete_nonblocking(self, key):
        try:
            data, expiry = self._data.pop(key)
        except KeyError:
            # TODO log?
            pass
        else:
            self._size -= len(data)
