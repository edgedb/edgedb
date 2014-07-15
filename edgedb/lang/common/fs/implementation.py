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

from . import backends
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
    def store_stream(self, bucket, id, filename, stream):
        pass

    @abc.abstractclassmethod
    def create_link(self, bucket, link_id, id, filename=None):
        pass

    @abc.abstractclassmethod
    def delete_link(self, bucket, link_id):
        pass

    @abc.abstractclassmethod
    def get_file_path(cls, bucket, id, filename=None):
        pass

    @abc.abstractclassmethod
    def get_file_pub_url(cls, bucket, id, filename=None):
        pass

    @abc.abstractclassmethod
    def delete_file(cls, bucket, id, *, name):
        pass


class DefaultImplementation(BaseImplementation):
    compatible_backend_classes = backends.Backend

    @classmethod
    def _ensure_backends(cls, bucket):
        backends = bucket.get_backends()
        if not backends:
            raise FSError('no backends found for bucket {!r}'.format(bucket))
        return backends

    @classmethod
    def store_http_file(cls, bucket, id, file, *, allow_rewrite=False):
        assert id and isinstance(id, uuid.UUID)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.store_http_file(bucket, id, file, allow_rewrite=allow_rewrite))

    @classmethod
    def store_file(cls, bucket, id, filename, *, name=None, allow_rewrite=False):
        assert id and isinstance(id, uuid.UUID)
        assert filename and isinstance(filename, str)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.store_file(bucket, id, filename,
                                       name=name, allow_rewrite=allow_rewrite))

    @classmethod
    def store_stream(cls, bucket, id, filename, stream, *, allow_rewrite=False):
        assert id and isinstance(id, uuid.UUID)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.store_stream(bucket, id, filename, stream,
                                         allow_rewrite=allow_rewrite))

    @classmethod
    def create_link(cls, bucket, link_id, id, filename=None):
        assert link_id and isinstance(link_id, uuid.UUID)
        assert id and isinstance(id, uuid.UUID)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.create_link(bucket, link_id, id, filename))

    @classmethod
    def delete_link(cls, bucket, link_id):
        assert link_id and isinstance(link_id, uuid.UUID)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.delete_link(bucket, link_id))

    @classmethod
    def delete_file(cls, bucket, id, *, name):
        assert id and isinstance(id, uuid.UUID)

        backends = cls._ensure_backends(bucket)

        for backend in backends:
            yield_ (backend.delete_file(bucket, id, name=name))

    @classmethod
    def get_file_pub_url(cls, bucket, id, filename=None):
        assert id and isinstance(id, uuid.UUID)
        backends = cls._ensure_backends(bucket)
        return backends[0].get_file_pub_url(bucket, id, filename)

    @classmethod
    def get_file_path(cls, bucket, id, filename=None):
        assert id and isinstance(id, uuid.UUID)
        backends = cls._ensure_backends(bucket)
        return backends[0].get_file_path(bucket, id, filename)
