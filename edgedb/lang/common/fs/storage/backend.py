##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import base64
import errno
import os
import itertools
import hashlib
import shutil
import uuid

from metamagic.utils import config, abc
from metamagic.utils import buckets as abstract
from metamagic.spin.protocols.http import types as http_types
from metamagic.spin.core import _coroutine
from metamagic.spin import abstractcoroutine

from .exceptions import StorageError


class BackendError(StorageError):
    pass


class Backend(abstract.Backend):
    @abstractcoroutine
    def store_file(self, bucket, id, file):
        pass

    @abc.abstractmethod
    def get_file_pub_url(bucket, id, filename):
        pass


class BaseFSBackend(Backend):
    umask = config.cvalue(0o002, type='int', doc='umask with wich files will be stored')

    def __init__(self, path, *, auto_create_path=True):
        """
        Parameters:

        path             - fs path to where the files can be stored
        auto_create_path - whether to create the "path" directory if it missing or not
        """

        self.path = os.path.abspath(path)
        self.auto_create_path = auto_create_path

        if not os.path.exists(self.path):
            if self.auto_create_path:
                os.mkdir(self.path, 0o777 - self.umask)

            if not os.path.exists(self.path):
                raise BackendError('unable to create directory {!r}'.format(self.path))

    def _get_base_name(self, bucket, id, filename):
        assert isinstance(id, uuid.UUID)

        if bucket.id:
            base = str(bucket.id)
        else:
            base = '{bucket_module}.{bucket_name}'.format(bucket_module=bucket.__module__,
                                                          bucket_name=bucket.__name__)

        new_id = base64.b32encode(hashlib.md5(id.bytes).digest()).decode('ascii')

        return os.path.join(base, new_id[:2], new_id[2:4], id.hex + '_' + filename)

    @_coroutine
    def store_http_file(self, bucket, id, file):
        if not isinstance(file, http_types.File):
            raise BackendError('unsupported file object: expected instance of'
                               'spin.http.types.File, got {!r}'.format(file))

        base = self._get_base_name(bucket, id,
                                   bucket.escape_filename(file.filename))
        path = os.path.join(self.path, base)

        if os.path.exists(path):
            raise BackendError('file names collision: {} already exists'.format(path))

        dir = os.path.dirname(path)
        os.makedirs(dir, exist_ok=True, mode=(0o777 - self.umask))

        yield file.save_to(path)
        os.chmod(path, 0o666 - self.umask)

    @_coroutine
    def store_file(self, bucket, id, filename, name=None):
        if not os.path.isfile(filename):
            raise BackendError('unable to locate file {!r}'.format(filename))

        if name is None:
            name = os.path.basename(filename)

        base = self._get_base_name(bucket, id, bucket.escape_filename(name))
        path = os.path.join(self.path, base)

        if os.path.exists(path):
            raise BackendError('file names collision: {} already exists'.format(path))

        dir = os.path.dirname(path)
        os.makedirs(dir, exist_ok=True, mode=(0o777 - self.umask))

        try:
            os.link(filename, path)
        except OSError as e:
            if e.errno == errno.EXDEV:
                shutil.copy(filename, path)
            else:
                raise

        os.chmod(path, 0o666 - self.umask)


class FSBackend(BaseFSBackend):
    def __init__(self, *args, pub_path, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_path = pub_path

    def get_file_path(self, bucket, id, filename):
        filename = bucket.escape_filename(filename)
        return os.path.join(self.path, self._get_base_name(bucket, id, filename))

    def get_file_pub_url(self, bucket, id, filename):
        filename = bucket.escape_filename(filename)
        return os.path.join(self.pub_path, self._get_base_name(bucket, id, filename))
