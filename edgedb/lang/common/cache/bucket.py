##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import time
import functools
from datetime import timedelta

from metamagic.utils.algos import persistent_hash
from metamagic.utils.functional import hybridmethod
from metamagic.utils.debug import debug
from metamagic.utils import buckets as abstract, tracepoints
from . import implementation, tracepoints as cache_trace


def _key(prefix:str, hash:int) -> bytes:
    return ('{}:{}'.format(prefix, hash)).encode('latin-1')


class BucketMeta(abstract.BucketMeta):
    hash_function = persistent_hash.persistent_hash
    compatible_implementation_classes = None

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        cls_key = []
        for base in cls.__mro__:
            if isinstance(base, mcls):
                cls_key.append((base.__module__, base.__name__))

        cls._cls_version = None
        cls._cls_base_hash = mcls.hash_function(('bucket class base hash', tuple(cls_key)))
        cls._cls_versioned_hash = None

        cls.hash_function = mcls.hash_function
        return cls


class Bucket(abstract.Bucket, metaclass=BucketMeta):
    def __init__(self, bucket_id='<default>', *, parent=None):
        super().__init__(parent=parent)

        self._base_hash = None
        self._versioned_hash = None
        self._version = None

        if not isinstance(bucket_id, str):
            bucket_id = 'hashed:{}'.format(self.hash_function(bucket_id))
        self._bucket_id = bucket_id

        self._get_implementation()

    def _get_implementation(self):
        if self._cached_implementation is None:
            impl = super()._get_implementation()
            if impl is not None:
                self._init_version()

        return self._cached_implementation

    def _get_version(self):
        if self._version is None:
            self._init_version()
        return self._version

    def _init_version(self, bump_version=False):
        impl = self._get_implementation()
        if not impl:
            return

        base_hash = []
        current = self
        while current is not None:
            base_hash.append((current.__class__.__module__, current.__class__.__name__,
                              current.__class__._cls_get_version(),
                              current._bucket_id,
                              current._get_version() if current is not self else None))
            current = current._parent

        self._base_hash = self.hash_function(('bucket instance base hash', tuple(base_hash)))

        version_key = _key('bucket version', self._base_hash)
        try:
            current_version = impl.getitem(version_key)
        except KeyError:
            version = int(time.time())
            impl.setitem(version_key, version)
        else:
            if bump_version:
                version = int(time.time())

                if current_version >= version:
                    version = current_version + 1

                impl.setitem(version_key, version)
            else:
                version = current_version

        self._version = version

        self._versioned_hash = self.hash_function(('bucket instance versioned hash',
                                                   self._base_hash, self._version))

    def _reset_bucket(self):
        self._init_version(bump_version=True)
        for bucket in self._children:
            bucket._init_version()

    @hybridmethod
    def reset(it):
        if isinstance(it, Bucket):
            it._reset_bucket()
        else:
            it._reset_bucket_class()

    def _get_inst_key(self, key):
        real_key = self.hash_function((self._versioned_hash, key))
        return str(real_key).encode('latin-1')

    @debug
    def __getitem__(self, key):
        impl = self._ensure_implementation()

        hashed_key = self._get_inst_key(key)
        try:
            with tracepoints.if_tracing(cache_trace.CacheTracepoint,
                                        info='get; bucket: {}.{}'
                                             .format(self.__class__.__module__,
                                                     self.__class__.__name__)):

                value = impl.getitem(hashed_key)
            '''LINE [cache] CACHE HIT
            self, '{!r:.40}'.format(key)
            '''
            return value
        except KeyError:
            '''LINE [cache] CACHE MISS
            self, '{!r:.40}'.format(key)
            '''
            raise KeyError('missing cache key {!r}'.format(key))

    def _cast_expiry_to_seconds(self, expiry:(int,float,timedelta)):
        if expiry is None:
            return None

        if isinstance(expiry, (int, float)):
            return expiry

        if isinstance(expiry, timedelta):
            return expiry.total_seconds()

        raise ValueError('expected expiry as int, float or timedelta, got {}, {!r}'. \
                         format(type(expiry).__name__, expiry))

    @debug
    def set(self, key, value, expiry:(int, float, timedelta)=None):
        impl = self._get_implementation()
        if not impl:
            return

        '''LINE [cache] CACHE SET
        self, '{!r:.40}'.format(key), expiry
        '''

        expiry = self._cast_expiry_to_seconds(expiry)
        hashed_key = self._get_inst_key(key)

        with tracepoints.if_tracing(cache_trace.CacheTracepoint,
                                    info='set; bucket: {}.{}'
                                         .format(self.__class__.__module__,
                                                 self.__class__.__name__)):

            impl.setitem(hashed_key, value, expiry=expiry)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        impl = self._get_implementation()
        if not impl:
            return

        hashed_key = self._get_inst_key(key)

        with tracepoints.if_tracing(cache_trace.CacheTracepoint,
                                    info='delete; bucket: {}.{}'
                                         .format(self.__class__.__module__,
                                                 self.__class__.__name__)):
            impl.delitem(hashed_key)

    def __contains__(self, key):
        impl = self._get_implementation()
        if not impl:
            return False

        hashed_key = self._get_inst_key(key)
        return impl.contains(hashed_key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    @classmethod
    def _cls_get_version(cls):
        if cls._cls_version is None:
            cls._cls_init_version()
        return cls._cls_version

    @classmethod
    def _cls_init_version(cls, bump_version=False):
        if cls._cls_version is None or bump_version:
            backends = cls.get_backends()
            if not backends:
                '''non-initialized cache - no work'''
                return

            base_versions = ['class version']
            for base in cls.__mro__[1:]:
                if isinstance(base, BucketMeta):
                    base_versions.append(base._cls_get_version())

            cls_versioned_hash = cls.hash_function((tuple(base_versions), cls._cls_base_hash))
            cls_versioned_key = _key('bucket_cls_version', cls_versioned_hash)

            impl = cls.get_implementation()(backends)

            try:
                current_version = impl.getitem(cls_versioned_key)
            except KeyError:
                cls_version = int(time.time())
                impl.setitem(cls_versioned_key, cls_version)
            else:
                if bump_version:
                    cls_version = int(time.time())

                    if current_version >= cls_version:
                        cls_version = current_version + 1

                    impl.setitem(cls_versioned_key, cls_version)
                else:
                    cls_version = current_version

            cls._cls_version = cls_version
            cls._cls_versioned_hash = cls.hash_function(('bucket class versioned hash',
                                                         cls_versioned_hash, cls_version))

        return cls._cls_version

    @classmethod
    def _reset_bucket_class(cls):
        cls._cls_init_version(bump_version=True)

        def walker(cls):
            if isinstance(cls, BucketMeta):
                cls._cls_init_version()

                for bucket in cls._instances:
                    bucket._init_version()

                for subcls in cls.__subclasses__():
                    if isinstance(subcls, BucketMeta):
                        walker(subcls)

        walker(cls)


Bucket.set_implementation(implementation.BaseImplementation)
