##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


__mm_track_dependencies__ = True

import uuid

from metamagic.utils import buckets as base_buckets
from metamagic import caos


class BucketMeta(base_buckets.BucketMeta):
    id_registry = {}
    name_registry = {}

    def __new__(mcls, name, bases, dct, *, abstract=False):
        dct['abstract'] = abstract
        cls = super().__new__(mcls, name, bases, dct)

        try:
            id = dct['id']
        except KeyError:
            if not abstract:
                raise TypeError('missing a required attribute "id" for a '
                                'non-abstract bucket class {}.{}'.
                                format(cls.__module__, cls.__name__))
        else:
            try:
                cls.id = uuid.UUID(id)
            except TypeError:
                raise TypeError('invalid bucket UUID {!r}'.format(id)) from None

            mcls.id_registry[cls.id] = cls

        cls.name = cls.__module__ + '.' + cls.__name__
        mcls.name_registry[cls.name] = cls

        return cls

    def __init__(cls, name, bases, dct, *, abstract=False):
        return super().__init__(name, bases, dct)

    @classmethod
    def get_bucket_class(mcls, bucket_id):
        if not isinstance(bucket_id, uuid.UUID):
            bucket_id = uuid.UUID(bucket_id)

        try:
            return mcls.id_registry[bucket_id]
        except KeyError:
            raise LookupError('unable to find bucket by id {!r}'.format(bucket_id))


class BaseBucket(base_buckets.Bucket, metaclass=BucketMeta, abstract=True):
    def __init__(self, *args, **kwargs):
        raise TypeError('storage Buckets are not meant to be instantiated')

    @classmethod
    def _error_if_abstract(cls):
        if cls.abstract:
            raise TypeError('unable to perform a file operation on an '
                            'abstract bucket {}.{}'.
                            format(cls.__module__, cls.__name__))

    @classmethod
    def get_bucket_entity(cls, session):
        schema = session.schema.metamagic.utils.fs.file

        with session.transaction():
            if cls.id:
                try:
                    return schema.Bucket.get(schema.Bucket.id == schema.Bucket.id(cls.id))
                except caos.session.EntityNotFoundError:
                    return schema.Bucket(name=cls.name, id=cls.id)
            else:
                try:
                    return schema.Bucket.get(schema.Bucket.name == cls.name)
                except caos.session.EntityNotFoundError:
                    return schema.Bucket(name=cls.name)

    @classmethod
    def configure(cls):
        """This hook is called during "Node.configure" phase"""

    @classmethod
    def build(cls):
        """This hook is called during "Node.build" phase"""


class Bucket(BaseBucket, abstract=True):
    @classmethod
    def store_http_file(cls, id, file, *, allow_rewrite=False):
        cls._error_if_abstract()
        return cls.get_implementation().store_http_file(cls, id, file, allow_rewrite=allow_rewrite)

    @classmethod
    def store_file(cls, id, filename, *, name=None, allow_rewrite=False):
        cls._error_if_abstract()
        return cls.get_implementation().store_file(cls, id, filename,
                                                   name=name, allow_rewrite=allow_rewrite)

    @classmethod
    def delete_file(cls, id, *, name=None):
        cls._error_if_abstract()
        return cls.get_implementation().delete_file(cls, id, name=name)

    @classmethod
    def get_file_pub_url(cls, id, filename):
        cls._error_if_abstract()
        return cls.get_implementation().get_file_pub_url(cls, id, filename)

    @classmethod
    def get_file_path(cls, id, filename):
        cls._error_if_abstract()
        return cls.get_implementation().get_file_path(cls, id, filename)
