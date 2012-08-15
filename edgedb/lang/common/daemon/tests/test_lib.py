##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import multiprocessing

from semantix.utils.debug import assert_raises
from semantix.utils.daemon import pidfile, exceptions, lib

from . import base


class TestUtilsDaemonLib(base.BaseDaemonTestCase):
    def test_utils_daemon_lib_make_readonly(self, path):
        with open(path, 'a+t') as f:
            f.write('foo')
            f.flush()

        lib.make_readonly(path)

        with assert_raises(IOError):
            open(path, 'a+t')

        os.unlink(path)

    def test_utils_daemon_lib_lock_file(self, path):
        def locker(v, path=path):
            with open(path, 'rb') as f:
                v.value = int(lib.lock_file(f.fileno()))

        with open(path, 'wb') as f:
            f.write(b'1')
            f.flush()
            lib.lock_file(f.fileno())

            v = multiprocessing.Value('i', -1)
            p = multiprocessing.Process(target=locker, args=(v,))
            p.start()
            p.join()

            assert v.value == 0

        os.unlink(path)
