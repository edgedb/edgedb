##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import multiprocessing

from metamagic.utils.debug import assert_raises
from metamagic.utils.daemon import pidfile, exceptions, lib

from . import base


class TestUtilsDaemonLib(base.BaseDaemonTestCase):
    def test_utils_daemon_lib_make_readonly(self, pid):
        with open(pid, 'a+t') as f:
            f.write('foo')
            f.flush()

        lib.make_readonly(pid)

        with assert_raises(IOError):
            open(pid, 'a+t')

        os.unlink(pid)

    def test_utils_daemon_lib_lock_file(self, pid):
        def locker(v, pid=pid):
            with open(pid, 'rb') as f:
                v.value = int(lib.lock_file(f.fileno()))

        with open(pid, 'wb') as f:
            f.write(b'1')
            f.flush()
            lib.lock_file(f.fileno())

            v = multiprocessing.Value('i', -1)
            p = multiprocessing.Process(target=locker, args=(v,))
            p.start()
            p.join()

            assert v.value == 0

        os.unlink(pid)

    def test_utils_daemon_lib_dry_test(self, pid):
        assert not lib.is_process_started_by_superserver()
        assert not lib.is_process_started_by_init()
        assert lib.get_max_fileno()
        assert lib.is_process_running(os.getpid())
