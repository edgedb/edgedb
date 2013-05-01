##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import io
import uuid

from metamagic.utils import abc
from metamagic.utils import buckets as abstract
from metamagic.spin.core.commands.greenlet import yield_

from . import bucket
from .exceptions import FSError


class ImplementationMeta(abstract.ImplementationMeta):
    pass


class BaseImplementation(abstract.Implementation, metaclass=ImplementationMeta):
    @abc.abstractclassmethod
    def store_http_file(cls, bucket, id, file):
        pass

    @abc.abstractclassmethod
    def store_file(self, bucket, id, filename, name=None):
        pass

    @abc.abstractclassmethod
    def get_file_path(cls, bucket, id, filename):
        pass

    @abc.abstractclassmethod
    def get_file_pub_url(cls, bucket, id, filename):
        pass


class DefaultImplementation(BaseImplementation):
    compatible_backend_classes = bucket.Backend

    @classmethod
    def _ensure_backends(cls, bucket):
        backends = bucket.get_backends()
        if not backends:
            raise FSError('no backends found for bucket {!r}'.format(bucket))
        return backends

    @classmethod
    def store_http_file(cls, bucket, id, file):
        assert id and isinstance(id, uuid.UUID)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.store_http_file(bucket, id, file))

    @classmethod
    def store_file(cls, bucket, id, filename, name=None):
        assert id and isinstance(id, uuid.UUID)
        assert filename and isinstance(filename, str)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.store_file(bucket, id, filename, name=name))

    @classmethod
    def get_file_pub_url(cls, bucket, id, filename):
        assert id and isinstance(id, uuid.UUID)
        backends = cls._ensure_backends(bucket)
        return backends[0].get_file_pub_url(bucket, id, filename)

    @classmethod
    def get_file_path(cls, bucket, id, filename):
        assert id and isinstance(id, uuid.UUID)
        backends = cls._ensure_backends(bucket)
        return backends[0].get_file_path(bucket, id, filename)
