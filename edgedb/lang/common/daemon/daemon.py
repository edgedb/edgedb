##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import atexit
import io
import os
import sys
import signal

from . import lib, pidfile as pidfile_module
from .exceptions import DaemonError


'''Implementation of PEP 3143'''


class DaemonContext:
    def __init__(self, *, pidfile:str,
                 files_preserve:list=None,
                 working_directory:str='/',
                 umask:int=0o022, uid:int=None, gid:int=None,
                 detach_process:bool=None, prevent_core:bool=True,
                 stdin:io.FileIO=None, stdout:io.FileIO=None, stderr:io.FileIO=None,
                 signal_map:dict=None):

        self.pidfile = pidfile
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

    is_open = property(lambda self: self._is_open)

    def open(self):
        if self._is_open:
            return

        self._init_pidfile()

        if self.prevent_core:
            lib.prevent_core_dump()

        lib.change_umask(self.umask)
        lib.change_working_directory(self.working_directory)

        if self.uid is not None:
            lib.change_process_uid(self.uid)

        if self.gid is not None:
            lib.change_process_gid(self.gid)

        if self.detach_process:
            lib.detach_process_context()

        self._setup_signals()

        self._close_all_open_files()

        stderr = self.stderr
        if isinstance(stderr, str):
            self._close_stderr = stderr = open(self.stderr, 'wt')
        lib.redirect_stream(sys.stderr, stderr)

        stdin = self.stdin
        if isinstance(stdin, str):
            self._close_stdin = stdin = open(self.stdin, 'rt')
        lib.redirect_stream(sys.stdin, stdin)

        stdout = self.stdout
        if isinstance(stdout, str):
            self._close_stdout = stdout = open(self.stdout, 'wt')
        lib.redirect_stream(sys.stdout, stdout)

        self._pidfile.acquire()

        self._is_open = True
        atexit.register(self.close)

    def close(self):
        if not self._is_open:
            return

        atexit.unregister(self.close)

        self._pidfile.release()
        self._pidfile = None

        if self._close_stdin:
            self._close_stdin.close()
            self._close_stdin = None

        if self._close_stdout:
            self._close_stdout.close()
            self._close_stdout = None

        if self._close_stderr:
            self._close_stderr.close()
            self._close_stderr = None

        self._is_open = False

    def terminate(self, signal_number, stack_frame):
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

        lib.close_all_open_files(excl)

    def _setup_signals(self):
        signal_map = {
            'SIGTSTP': None,
            'SIGTTIN': None,
            'SIGTTOU': None,
            'SIGTERM': 'terminate'
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
                    raise DaemonError('Invalid signal number {!r}'.format(name))
                num = name
            else:
                raise DaemonError('Invalid signal {!r}, str or int expected'.format(name))

            if handler is None:
                signal.signal(num, signal.SIG_IGN)
            elif isinstance(handler, str):
                try:
                    handler = getattr(self, handler)
                except AttributeError:
                    raise DaemonError('Invalid signal {!r} handler name {!r}'.format(name, handler))
                signal.signal(num, handler)
            else:
                if not callable(handler):
                    raise DaemonError('Excpected callable signal {!r} handler: {!r}'.
                                      format(name, handler))
                signal.signal(num, handler)

    def _init_pidfile(self):
        if isinstance(self.pidfile, str):
            self._pidfile = pidfile_module.PidFile(self.pidfile)
        else:
            if isinstance(self.pidfile, pidfile_module.PidFile):
                if self.pidfile.locked:
                    raise DaemonError('Pidfile object is already locked; unable to initialize '
                                      'daemon context')
                self._pidfile = self.pidfile
            else:
                raise DaemonError('Invalid pidfile, str of PidFile expected, got {!r}'.
                                  format(pidfile))
