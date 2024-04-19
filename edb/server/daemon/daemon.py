#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Implementation of PEP 3143."""

from __future__ import annotations
from typing import Optional

import atexit
import io
import os
import signal

from . import lib, pidfile as pidfile_module
from .exceptions import DaemonError


class DaemonContext:
    def __init__(
        self,
        *,
        pidfile: Optional[os.PathLike] = None,
        files_preserve: Optional[list] = None,
        working_directory: str = '/',
        umask: int = 0o022,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        detach_process: Optional[bool] = None,
        prevent_core: bool = True,
        stdin: Optional[io.FileIO] = None,
        stdout: Optional[io.FileIO] = None,
        stderr: Optional[io.FileIO] = None,
        signal_map: Optional[dict] = None
    ):

        self.pidfile = os.fspath(pidfile) if pidfile is not None else None
        self.files_preserve = files_preserve
        self.working_directory = working_directory
        self.umask = umask
        self.prevent_core = prevent_core
        self.signal_map = signal_map

        if stdin is not None and not isinstance(stdin, str):
            lib.validate_stream(stdin, stream_name='stdin')
        self.stdin = stdin

        if stdout is not None and not isinstance(stdout, str):
            lib.validate_stream(stdout, stream_name='stdout')
        self.stdout = stdout

        if stderr is not None and not isinstance(stderr, str):
            lib.validate_stream(stderr, stream_name='stderr')
        self.stderr = stderr

        self.uid = uid
        self.gid = gid

        if detach_process is None:
            self.detach_process = lib.is_detach_process_context_required()
        else:
            self.detach_process = detach_process

        self._is_open = False
        self._close_stdin = self._close_stdout = self._close_stderr = None
        self._stdin_name = self._stdout_name = self._stderr_name = None
        self._pidfile = None

    is_open = property(lambda self: self._is_open)

    def open(self):
        if self._is_open:
            return

        self._init_pidfile()

        if self.prevent_core:
            lib.prevent_core_dump()

        lib.change_umask(self.umask)
        lib.change_working_directory(self.working_directory)

        # Test that we can write to log files/output right after
        # chdir call
        self._test_sys_streams()

        if self.uid is not None:
            lib.change_process_uid(self.uid)

        if self.gid is not None:
            lib.change_process_gid(self.gid)

        if self.detach_process:
            lib.detach_process_context()

        self._setup_signals()

        if self._pidfile is not None:
            self._pidfile.acquire()

        self._close_all_open_files()
        self._open_sys_streams()

        self._is_open = True
        atexit.register(self.close)

    def close(self):
        if not self._is_open:
            return

        atexit.unregister(self.close)

        if self._pidfile is not None:
            self._pidfile.release()
            self._pidfile = None

        self._close_sys_streams()

        self._is_open = False

    def _close_sys_streams(self):
        if self._close_stdin:
            self._close_stdin.close()
            self._close_stdin = None
        self.stdin = None

        if self._close_stdout:
            self._close_stdout.close()
            self._close_stdout = None
        self.stdout = None

        if self._close_stderr:
            self._close_stderr.close()
            self._close_stderr = None
        self.stderr = None

    def _test_sys_streams(self):
        stderr = self.stderr or self._stderr_name
        if isinstance(stderr, str):
            open(stderr, 'at').close()

        stdout = self.stdout or self._stdout_name
        if isinstance(stdout, str):
            open(stdout, 'at').close()

    def _open_sys_streams(self):
        stdin = self.stdin or self._stdin_name
        if isinstance(stdin, str):
            self._stdin_name = stdin
            self._close_stdin = stdin = open(stdin, 'rt')
        else:
            self._stdin_name = getattr(stdin, 'name', None)
        lib.redirect_stream('stdin', stdin)

        stderr = self.stderr or self._stderr_name
        if isinstance(stderr, str):
            self._stderr_name = stderr
            self._close_stderr = stderr = open(stderr, 'at')
        else:
            self._stderr_name = getattr(stderr, 'name', None)
        lib.redirect_stream('stderr', stderr)

        stdout = self.stdout or self._stdout_name
        if isinstance(stdout, str):
            self._stdout_name = stdout
            self._close_stdout = stdout = open(stdout, 'at')
        else:
            self._stdout_name = getattr(stdout, 'name', None)
        lib.redirect_stream('stdout', stdout)

    def signal_reopen_sys_streams(self, signal_number, stack_frame):
        self._close_sys_streams()
        self._open_sys_streams()

    def signal_terminate(self, signal_number, stack_frame):
        raise SystemExit('Termination on signal {}'.format(signal_number))

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        self.close()

    def _close_all_open_files(self):
        excl = set()

        if self.files_preserve:
            excl.update(self.files_preserve)

        if self.stderr and not isinstance(self.stderr, str):
            excl.add(self.stderr.fileno())

        if self.stdin and not isinstance(self.stdin, str):
            excl.add(self.stdin.fileno())

        if self.stdout and not isinstance(self.stdout, str):
            excl.add(self.stdout.fileno())

        if self._pidfile is not None:
            pidfile = self._pidfile.fileno
            if pidfile is not None:
                excl.add(pidfile)

        lib.close_all_open_files(excl)

    def _setup_signals(self):
        signal_map = {
            'SIGTSTP': None,
            'SIGTTIN': None,
            'SIGTTOU': None,
            'SIGTERM': 'signal_terminate',
            'SIGHUP': 'signal_reopen_sys_streams'
        }

        if self.signal_map:
            signal_map.update(self.signal_map)

        for name, handler in signal_map.items():
            if isinstance(name, str):
                try:
                    num = getattr(signal, name)
                except AttributeError:
                    raise DaemonError('Invalid signal name {!r}'.format(name))
            elif isinstance(name, int):
                if name < 1 or name >= signal.NSIG:
                    raise DaemonError(
                        'Invalid signal number {!r}'.format(name))
                num = name
            else:
                raise DaemonError(
                    'Invalid signal {!r}, str or int expected'.format(name))

            if handler is None:
                signal.signal(num, signal.SIG_IGN)
            elif isinstance(handler, str):
                try:
                    handler = getattr(self, handler)
                except AttributeError:
                    raise DaemonError(
                        'Invalid signal {!r} handler name {!r}'.format(
                            name, handler))
                signal.signal(num, handler)
            else:
                if not callable(handler):
                    raise DaemonError(
                        'Excpected callable signal {!r} handler: {!r}'.format(
                            name, handler))
                signal.signal(num, handler)

    def _init_pidfile(self):
        if self.pidfile is None:
            return

        if isinstance(self.pidfile, str):
            self._pidfile = pidfile_module.PidFile(self.pidfile)
        else:
            if isinstance(self.pidfile, pidfile_module.PidFile):
                if self.pidfile.locked:
                    raise DaemonError(
                        'Pidfile object is already locked; '
                        'unable to initialize daemon context')
                self._pidfile = self.pidfile
            else:
                raise DaemonError(
                    'Invalid pidfile, str of PidFile expected, got {!r}'.
                    format(self.pidfile))
