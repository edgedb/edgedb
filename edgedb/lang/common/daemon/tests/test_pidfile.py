##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import os.path
import time
import multiprocessing

from metamagic.utils.debug import assert_raises
from metamagic.utils.daemon import lib
from metamagic.utils import daemon

from . import base


class TestUtilsDaemonPidfile(base.BaseDaemonTestCase):
    def test_utils_daemon_pidfile_basic(self, pid:base.Pid):
        path = pid
        assert not daemon.PidFile.is_locked(path)
        pid = daemon.PidFile(path)
        assert not pid.locked
        with pid as p:
            assert int(open(path, 'rt').read()) == os.getpid()
            assert pid.locked
            assert pid is p

            data = daemon.PidFile.read(path)
            assert data[0] == os.getpid()
            assert data[1] is None

            assert pid.is_locked(path)
            with assert_raises(daemon.DaemonError, error_re='already acquired'):
                pid.acquire()

        with assert_raises(daemon.DaemonError, error_re='already released'):
            pid.release()

        assert pid._file is None
        assert not pid.locked
        assert not os.path.exists(path)

    def test_utils_daemon_pidfile_locked(self, pid:base.Pid):
        path = pid

        def writer(v, path=path):
            time.sleep(0.1)
            try:
                with daemon.PidFile(path):
                    v.value = 0
            except daemon.DaemonError as e:
                v.value = 1

        v = multiprocessing.Value('i', -1)
        p = multiprocessing.Process(target=writer, args=(v,))
        p.start()

        pid = daemon.PidFile(path)
        with pid:
            p.join()

        assert v.value == 1 # Subprocess was unable to lock the pid file and got DaemonError
        assert not os.path.exists(path)

    def test_utils_daemon_pidfile_exists_running_pid(self, pid:base.Pid):
        path = pid

        with open(path, 'wt') as f:
            f.write(str(os.getpid()))

        pid = daemon.PidFile(path)
        with assert_raises(daemon.DaemonError, error_re='belongs to a running process'):
            with pid:
                assert 0

    def test_utils_daemon_pidfile_exists_nonrunning_pid(self, pid:base.Pid):
        path = pid

        pid = 60000
        while True:
            try:
                if not lib.is_process_running(pid):
                    break
            except PermissionError:
                pass
            pid += 1
            if pid > 65535:
                raise RuntimeError('unable to find a non-existent pid')

        with open(path, 'wt') as f:
            f.write(str(pid))

        pid = daemon.PidFile(path)
        with pid:
            assert pid.locked

    def test_utils_daemon_pidfile_with_data(self, pid:base.Pid):
        path = pid
        with daemon.PidFile(path, data='spam'):
            with open(path, 'rt') as f:
                assert f.read() == '{}\n\nspam'.format(os.getpid())

                pid, data = daemon.PidFile.read(path)
                assert pid == os.getpid()
                assert data == 'spam'
