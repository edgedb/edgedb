##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import base64
import errno
import hashlib
import itertools
import os
import re
import shutil
import tempfile
import uuid

from metamagic.utils import buckets as base_buckets, config, abc

from metamagic.spin.protocols.http import types as http_types
from metamagic.spin.core import _coroutine
from metamagic.spin import abstractcoroutine
from metamagic import node

from .exceptions import FSError


class BackendError(FSError):
    pass


class Backend(base_buckets.Backend):
    pass


class BaseFSBackend(Backend):
    auto_create_path = config.cvalue(True, type=bool,
                                     doc='whether to create the "path" directory '
                                         'if it missing or not')

    umask = config.cvalue(None, type=int,
                          doc='umask with wich files will be stored. If unset then umask config '
                              'from default node or metamagic.node.Node will be used')

    def __init__(self, *, path, **kwargs):
        """
        Parameters:

        path             - fs path to where the files can be stored
        """

        super().__init__(**kwargs)

        if self.umask is None:
            node_cls = node.Node.default_cls or node.Node
            self.umask = node_cls.umask

        self.path = os.path.abspath(path)

        if not os.path.exists(self.path):
            if self.auto_create_path:
                os.mkdir(self.path, 0o777 - self.umask)

            if not os.path.exists(self.path):
                raise BackendError('unable to create directory {!r}'.format(self.path))

    def stop(self):
        pass


class FSBackend(BaseFSBackend):
    _re_escape = re.compile(r'[^\w\-\._]')

    # Don't change this constant, as FS in all existing project will need to be
    # converted
    _FN_LEN_LIMIT = 75

    def __init__(self, *args, pub_path, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_path = pub_path

    def escape_filename(self, filename):
        return self._re_escape.sub('_', filename).strip('-')

    def _get_base_name(self, bucket, id, filename):
        assert isinstance(id, uuid.UUID)

        base = str(bucket.id)

        new_id = base64.b32encode(hashlib.md5(id.bytes).digest()).decode('ascii')

        base_filename = id.hex

        if filename is not None:
            filename = base_filename + '_' + filename
        else:
            filename = base_filename

        if len(filename) > self._FN_LEN_LIMIT:
            if '.' in filename:
                extension = filename.rpartition('.')[2]
                limit = self._FN_LEN_LIMIT - len(extension) - 1
                if limit <= 0:
                    filename = filename[:self._FN_LEN_LIMIT]
                else:
                    filename = filename[:limit] + '.' + extension
            else:
                filename = filename[:self._FN_LEN_LIMIT]

        return os.path.join(base, new_id[:2], new_id[2:4], filename)

    def _get_path(self, bucket, id, filename, allow_rewrite):
        if filename:
            filename = self.escape_filename(filename)
        base = self._get_base_name(bucket, id, filename)
        path = os.path.join(self.path, base)

        if os.path.exists(path):
            if allow_rewrite:
                if os.path.islink(path):
                    os.unlink(path)
                else:
                    os.remove(path)
            else:
                raise BackendError('file names collision: {} already exists'\
                                    .format(path))

        dir = os.path.dirname(path)
        os.makedirs(dir, exist_ok=True, mode=(0o777 - self.umask))

        if filename and getattr(bucket, 'allow_empty_names', False):
            alias = self._get_base_name(bucket, id, None)
            alias = os.path.join(self.path, alias)

            if os.path.exists(alias):
                if os.path.islink(alias):
                    os.unlink(alias)
                else:
                    raise BackendError('file alias exists and is not a symlink: {}'\
                                       .format(alias))

            os.symlink(os.path.basename(path), alias)

        return path

    def _after_save(self, path):
        os.chmod(path, 0o666 - self.umask)

    @_coroutine
    def store_http_file(self, bucket, id, file, *, allow_rewrite=False):
        if not isinstance(file, http_types.File):
            raise BackendError('unsupported file object: expected instance of'
                               'spin.http.types.File, got {!r}'.format(file))

        path = self._get_path(bucket, id, file.filename, allow_rewrite)
        yield file.save_to(path)
        self._after_save(path)

    @_coroutine
    def store_file(self, bucket, id, filename, *, name=None, allow_rewrite=False):
        if not os.path.isfile(filename):
            raise BackendError('unable to locate file {!r}'.format(filename))

        if name is None:
            name = os.path.basename(filename)

        path = self._get_path(bucket, id, name, allow_rewrite)

        try:
            os.link(filename, path)
        except OSError as e:
            if e.errno == errno.EXDEV:
                shutil.copy(filename, path)
            else:
                raise

        self._after_save(path)

    @_coroutine
    def store_stream(self, bucket, id, name, stream, *, allow_rewrite=False):
        path = self._get_path(bucket, id, name, allow_rewrite)

        with open(path, 'wb') as f:
            f.write(stream.read())

        self._after_save(path)

    @_coroutine
    def create_link(self, bucket, link_id, id, filename=None):
        if filename is not None:
            filename = self.escape_filename(filename)
        else:
            aem = getattr(bucket, 'allow_empty_names', False)
            if not aem:
                raise ValueError('filename is required for this bucket')

        base_link_name = self._get_base_name(bucket, link_id, None)
        linkname = self._get_path(bucket, link_id, None, allow_rewrite=False)
        linkpath = os.path.join(self.path, linkname)

        basename = self._get_base_name(bucket, id, filename)
        updirs = ['..' for _ in range(base_link_name.count(os.path.sep))]
        updirs.append(basename)
        targetpath = os.path.join(*updirs)

        os.symlink(targetpath, linkpath)

    @_coroutine
    def delete_link(self, bucket, link_id):
        link_relpath = self._get_base_name(bucket, link_id, None)
        linkpath = os.path.join(self.path, link_relpath)
        if os.path.lexists(linkpath):
            os.unlink(linkpath)

    @_coroutine
    def delete_file(self, bucket, id, *, name=None):
        alias = self._get_base_name(bucket, id, None)

        if name is None:
            aem = getattr(bucket, 'allow_empty_names', False)
            if not aem:
                raise ValueError('filename is required for this bucket')

            name = os.readlink(alias)
            _, _, name = name.partition('_')

        base = self._get_base_name(bucket, id, self.escape_filename(name))
        path = os.path.join(self.path, base)

        if os.path.exists(path):
            os.remove(path)

        if os.path.exists(alias):
            os.unlink(alias)

    def get_file_path(self, bucket, id, filename=None):
        if filename is not None:
            filename = self.escape_filename(filename)
        else:
            aem = getattr(bucket, 'allow_empty_names', False)
            if not aem:
                raise ValueError('filename is required for this bucket')
        return os.path.join(self.path, self._get_base_name(bucket, id,
                                                           filename))

    def get_file_pub_url(self, bucket, id, filename=None):
        if filename is not None:
            filename = self.escape_filename(filename)
        else:
            aem = getattr(bucket, 'allow_empty_names', False)
            if not aem:
                raise ValueError('filename is required for this bucket')
        return os.path.join(self.pub_path, self._get_base_name(bucket, id,
                                                               filename))


class TemporaryFSBackend(FSBackend):
    def __init__(self, **kwargs):
        self.tmpdir = tempfile.mkdtemp()
        self._closed = False
        super().__init__(path=self.tmpdir, **kwargs)

    def stop(self):
        try:
            super().stop()
        finally:
            if not self._closed:
                self._closed = True
                shutil.rmtree(self.tmpdir)
