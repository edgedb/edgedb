##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import stat

from .exceptions import DaemonError
from . import lib


class PidFile:
    def __init__(self, path, pid=None, data=None):
        self._path = path
        self._data = data
        self._pid = pid
        self._file = None
        self._locked = False

    locked = property(lambda self: self._locked)

    def _prepare_file_content(self):
        buf = ''
        if self._pid is None:
            buf += str(os.getpid())
        else:
            buf += str(self._pid)
        if self._data:
            buf += '\n\n{}'.format(self._data)
        return buf

    def acquire(self):
        if self.locked:
            # No point in allowing re-entrance
            raise DaemonError('pid file is already acquired')

        path = self._path

        if os.path.exists(path):
            # If pid file already exists - check if it belongs to a
            # running process.  If not - it should be safe to remove it.
            with open(path, 'rt') as f:
                pid = int(f.readline())
                if lib.is_process_running(pid):
                    raise DaemonError('pid file {!r} exists and belongs to a running process'. \
                                      format(path))
                else:
                    os.unlink(path)

        self._file = open(path, 'wt')

        fileno = self._file.fileno()
        if not lib.lock_file(fileno):
            raise DaemonError('pid file {!r} already locked'.format(path))

        self._file.write(self._prepare_file_content())
        self._file.flush()

        lib.make_readonly(path)

        self._locked = True

    def release(self):
        if not self.locked:
            raise DaemonError('pid file is already released')

        if self._file:
            self._file.close()
            self._file = None

        if os.path.exists(self._path):
            os.remove(self._path)

        self._locked = False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        self.release()

    @classmethod
    def read(cls, path):
        with open(path, 'rt') as f:
            pid = int(f.readline())
            f.readline()
            data = f.read()

        return pid, (data or None)
