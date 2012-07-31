##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import pickle
from datetime import timedelta

from semantix.utils import abc
from semantix.utils.algos.persistent_hash import persistent_hash
from . import backend, exceptions, abstract


class AbstractImplementation(abstract.Implementation):
    key_hash_function = persistent_hash

    @abc.abstractmethod
    def getitem(self, key:bytes):
        pass

    @abc.abstractmethod
    def setitem(self, key:bytes, value:bytes, expiry:float=None):
        pass

    @abc.abstractmethod
    def delitem(self, key:bytes):
        pass

    @abc.abstractmethod
    def contains(self, key:bytes):
        pass


class BaseImplementation(AbstractImplementation):
    compatible_backend_classes = (backend.BlockingBackend, backend.NonBlockingBackend)

    def __init__(self, backends):
        super().__init__(backends)
        self.meths_cache = {}

    def _backend_method(self, backend, methname):
        try:
            return self.meths_cache[(backend, methname)]
        except KeyError:
            pass

        try:
            meth = getattr(backend, '{}_nonblocking'.format(methname))
        except AttributeError:
            try:
                meth = getattr(backend, '{}_blocking'.format(methname))
            except AttributeError:
                raise exceptions.CacheError('unsupported backend {!r}'.format(backend))

        self.meths_cache[(backend, methname)] = meth
        return meth

    def getitem(self, key):
        for idx, backend in enumerate(self._backends):
            meth = self._backend_method(backend, 'get')

            try:
                value = meth(key)
            except LookupError:
                pass
            else:
                if idx:
                    for i in range(idx):
                        self._backends[i].set(key, value)
                return pickle.loads(value)

        raise KeyError('missing cache key {!r}'.format(key))

    def setitem(self, key, value, expiry=None):
        pickled_value = pickle.dumps(value)

        for backend in self._backends:
            meth = self._backend_method(backend, 'set')
            meth(key, pickled_value, expiry=expiry)

    def delitem(self, key):
        for backend in self._backends:
            meth = self._backend_method(backend, 'delete')
            meth(key)

    def contains(self, key):
        for backend in self._backends:
            meth = self._backend_method(backend, 'contains')

            if meth(key):
                return True

        return False
