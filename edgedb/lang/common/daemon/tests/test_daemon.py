##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import time
import multiprocessing
import contextlib
import signal

from semantix.utils import daemon, debug
from semantix.utils.daemon import lib as daemon_lib
from . import base


class TestUtilsDaemon(base.BaseDaemonTestCase):
    def daemonize(self, prog, *, args=None, kwargs=None, **context_kwargs):
        def runner(prog, args, kwargs):
            with daemon.DaemonContext(**context_kwargs):
                prog(*(args or ()), **(kwargs or {}))

        p = multiprocessing.Process(target=runner, args=(prog, args, kwargs))
        p.daemon = False
        p.start()
        p.join()

        time.sleep(0.1)

    def wait_pid(self, pid, maxtime=2.0):
        time_delta = 0.1
        max_iters = round(maxtime / time_delta)
        assert max_iters > 1

        if daemon.PidFile.is_locked(pid):
            i = 0
            while True:
                if i > max_iters:
                    raise RuntimeError('Waited for PID being released too long {}s'.format(maxtime))

                if daemon.PidFile.is_locked(pid):
                    time.sleep(time_delta)
                    i += 1
                else:
                    break
        time.sleep(time_delta)

    def assert_empty_files(self, *fns):
        for fn in fns:
            if os.path.exists(fn):
                with open(fn, 'rt') as f:
                    text = f.read()
                    if text != '':
                        raise AssertionError('expected {!r} to be an empty file, however it has '
                                             'the following content in it: {!r}'.
                                             format(fn, text))

    def assert_file_contains(self, fn, sub):
        assert sub and isinstance(sub, str)
        with open(fn, 'rt') as f:
            text = f.read()
            if sub not in text:
                raise AssertionError('expected to see {!r} in {!r}, file: {!r}'.
                                     format(sub, text, fn))

    def test_utils_daemon_functional_basic(self, pid, stderr):
        def prog():
            time.sleep(0.3)

        self.daemonize(prog, pidfile=pid)

        pidnum, _ = daemon.PidFile.read(pid)
        assert pidnum != os.getpid()

        os.kill(pidnum, 0)
        os.kill(pidnum, signal.SIGTSTP)
        assert os.path.exists(pid)

        time.sleep(0.3)
        assert not os.path.exists(pid)
        with debug.assert_raises(OSError):
            os.kill(pidnum, 0)
        self.wait_pid(pid)

    def test_utils_daemon_functional_conflict_pidfile(self, pid, stderr):
        def prog():
            time.sleep(0.2)

        self.daemonize(prog, pidfile=pid)
        self.daemonize(prog, pidfile=pid, stderr=stderr)

        self.assert_file_contains(stderr, 'exists and belongs to a running process')
        self.wait_pid(pid, maxtime=0.2)

    def test_utils_daemon_functional_with_fork(self, pid, stderr, fn1, fn2):
        def prog():
            if os.fork() == 0:
                with open(fn2, 'wt') as f:
                    f.write('{}\nham'.format(os.getpid()))
            else:
                with open(fn1, 'wt') as f:
                    f.write('{}\nspam'.format(os.getpid()))

        self.daemonize(prog, pidfile=pid)

        self.assert_file_contains(fn1, 'spam')
        self.assert_file_contains(fn2, 'ham')

        pid1, _ = daemon.PidFile.read(fn1)
        pid2, _ = daemon.PidFile.read(fn2)

        assert pid1 != pid2
        assert not daemon_lib.is_process_running(pid1)
        assert not daemon_lib.is_process_running(pid2)
        self.wait_pid(pid)

    def test_utils_daemon_functional_working_dir(self, pid, stderr):
        def prog():
            assert os.getcwd() == '/'

        self.daemonize(prog, pidfile=pid, stderr=stderr)
        self.assert_empty_files(stderr)
        self.wait_pid(pid)

    def test_utils_daemon_functional_file_std_err_out(self, pid, stderr, stdout):
        '''Tests open files for stderr & stdout'''

        def prog():
            print('PRINTING')
            1/0

        with open(stderr, 'wt') as er, open(stdout, 'wt') as out:
            self.daemonize(prog, pidfile=pid, stderr=er, stdout=out)

        self.assert_file_contains(stderr, 'ZeroDivisionError: division by zero')
        self.assert_file_contains(stdout, 'PRINTING')

        self.wait_pid(pid)

    def test_utils_daemon_functional_filename_std_err_out(self, pid, stderr, stdout):
        '''Tests filenames for stderr & stdout'''

        def prog():
            print('PRINTING')
            1/0

        self.daemonize(prog, pidfile=pid, stderr=stderr, stdout=stdout)

        self.assert_file_contains(stderr, 'ZeroDivisionError: division by zero')
        self.assert_file_contains(stdout, 'PRINTING')

        self.wait_pid(pid)

    def test_utils_daemon_functional_singals(self, pid, stderr, stdout, fn1, fn2):
        def SIGUSR1(*args, fn1=fn1):
            with open(fn1, 'wt') as f:
                f.write('SIGUSR1')

        def SIGUSR2(*args, fn2=fn2):
            print('SIGUSR2')

        def prog():
            time.sleep(0.2)

        self.daemonize(prog, pidfile=pid, stderr=stderr, stdout=stdout,
                       signal_map={signal.SIGUSR1: SIGUSR1, 'SIGUSR2': SIGUSR2})

        self.assert_empty_files(stderr, stdout)

        pidnum, _ = daemon.PidFile.read(pid)

        os.kill(pidnum, signal.SIGUSR1)
        os.kill(pidnum, signal.SIGUSR2)
        self.wait_pid(pid, maxtime=0.5)

        self.assert_empty_files(stderr)
        self.assert_file_contains(fn1, 'SIGUSR1')
        self.assert_file_contains(stdout, 'SIGUSR2')

        self.wait_pid(pid)

    def test_utils_daemon_functional_term_singal(self, pid, stderr, stdout, fn1, fn2):
        def prog():
            time.sleep(0.2)

        self.daemonize(prog, pidfile=pid, stderr=stderr)

        self.assert_empty_files(stderr, stdout)

        pidnum, _ = daemon.PidFile.read(pid)

        os.kill(pidnum, signal.SIGTERM)
        self.wait_pid(pid, maxtime=0.5)

        self.assert_empty_files(stdout)
        self.assert_file_contains(stderr, 'Termination on signal')

    def test_utils_daemon_functional_custom_pid_object(self, pid, stderr, stdout, fn1, fn2):
        def prog():
            time.sleep(0.2)

        self.daemonize(prog, pidfile=daemon.PidFile(pid, data='spam'),
                                                    stderr=stderr, stdout=stdout)

        self.assert_empty_files(stderr, stdout)
        pidnum, data = daemon.PidFile.read(pid)
        assert data == 'spam'

        self.wait_pid(pid, maxtime=1.0)

