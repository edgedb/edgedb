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


from __future__ import annotations
from typing import Optional

import errno
import io
import os
import fcntl
import logging
import resource
import stat
import socket
import sys

from .exceptions import DaemonError

logger = logging.getLogger('edb.server.daemon')


def is_process_running(pid: int):
    """Check if there is a running process with `pid`."""
    try:
        os.kill(pid, 0)
        return True
    except OSError as ex:
        if ex.errno == errno.ESRCH:
            return False
        else:
            raise


def lock_file(fileno: int):
    """Lock file.  Returns ``True`` if succeeded, ``False`` otherwise."""
    try:
        # Try to lock file exclusively and in non-blocking fashion
        fcntl.flock(fileno, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        return False
    else:
        return True


def make_readonly(path: str):
    """Make a file read-only."""
    assert os.path.isfile(path)
    os.chmod(path, stat.S_IROTH | stat.S_IRUSR | stat.S_IRGRP)


def change_working_directory(path: str):
    """Change the working directory for this process."""
    try:
        os.chdir(path)
    except OSError as ex:
        raise DaemonError(
            'Unable to change working directory to {!r}'.format(path)) from ex


def change_process_gid(gid: int):
    """Change the GID of this process.

    Requires appropriate OS privileges for this process.
    """
    try:
        os.setgid(gid)
    except OSError as ex:
        raise DaemonError(
            'Unable to change the owning GID to {!r}'.format(gid)) from ex


def change_process_uid(uid: int):
    """Change the UID of this process.

    Requires appropriate OS privileges for this process.
    """
    try:
        os.setuid(uid)
    except OSError as ex:
        raise DaemonError(
            'Unable to change the owning UID to {!r}'.format(uid)) from ex


def change_umask(mask: int):
    """Change process umask."""
    try:
        os.umask(mask)
    except (OSError, OverflowError) as ex:
        raise DaemonError('Unable to set process umask to {:#o}'.format(
            mask)) from ex


def prevent_core_dump():
    """Prevent this process from generating a core dump."""
    core_resource = resource.RLIMIT_CORE

    try:
        resource.getrlimit(core_resource)
    except ValueError as ex:
        raise DaemonError(
            'Unable to limit core dump size: '
            'system does not support RLIMIT_CORE resource limit') from ex

    # Set hard & soft limits to 0, i.e. no core dump at all
    resource.setrlimit(core_resource, (0, 0))


def detach_process_context():
    """Datach process context.

    Does it in three steps:

    1. Forks and exists parent process.
    This detaches us from shell, and since the child will have a new
    PID but will inherit the Group PID from parent, the new process
    will not be a group leader.

    2. Call 'setsid' to create a new session.
    This makes the process a session leader of a new session, process
    becomes the process group leader of a new process group and it
    doesn't have a controlling terminal.

    3. Form and exit parent again.
    This guarantees that the daemon is not a session leader, which
    prevents it from acquiring a controlling terminal.

    Reference: “Advanced Programming in the Unix Environment”,
    section 13.3, by W. Richard Stevens.
    """
    def fork_and_exit_parent(error_message):
        try:
            if os.fork() > 0:
                # Don't need to call 'sys.exit', as we don't want to
                # run any python interpreter clean-up handlers
                os._exit(0)
        except OSError as ex:
            raise DaemonError(
                '{}: [{}] {}'.format(error_message, ex.errno,
                                     ex.strerror)) from ex

    fork_and_exit_parent(error_message='Failed the first fork')
    os.setsid()
    fork_and_exit_parent(error_message='Failed the second fork')


def is_process_started_by_init():
    """Determine if the current process is started by 'init'."""
    # The 'init' process has its PID set to 1.
    return os.getppid() == 1


def is_socket(fd):
    """Determine if the file descriptor is a socket."""
    file_socket = socket.fromfd(fd, socket.AF_INET, socket.SOCK_RAW)

    try:
        file_socket.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
    except socket.error as ex:
        return ex.args[0] != errno.ENOTSOCK
    else:
        return True


def is_process_started_by_superserver():
    """Determine if the current process is started by the superserver."""
    # The internet superserver creates a network socket, and
    # attaches it to the standard streams of the child process.

    try:
        fileno = sys.__stdin__.fileno()
    except Exception:
        return False
    else:
        return is_socket(fileno)


def is_detach_process_context_required():
    """Determine whether detaching process context is required.

    Returns ``True`` if:
        - Process was started by `init`; or
        - Process was started by `inetd`.
    """
    return not is_process_started_by_init(
    ) and not is_process_started_by_superserver()


def get_max_fileno(default: int=2048):
    """Return the maximum number of open file descriptors."""
    limit = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if limit == resource.RLIM_INFINITY:
        return default
    return limit


def try_close_fileno(fileno: int):
    """Try to close fileno."""
    try:
        os.close(fileno)
    except OSError as ex:
        if ex.errno != errno.EBADF:
            raise DaemonError(
                'Failed to close file descriptor {}'.format(fileno))


def close_all_open_files(exclude: Optional[set] = None):
    """Close all open file descriptors."""
    maxfd = get_max_fileno()
    if exclude:
        for fd in reversed(range(maxfd)):
            if fd not in exclude:
                try_close_fileno(fd)
    else:
        for fd in reversed(range(maxfd)):
            try_close_fileno(fd)


def redirect_stream(stream_name: str, target_stream: io.FileIO):
    """Redirect a system stream to the specified file.

    If ``target_stream`` is None - redirect to devnull.
    """
    if target_stream is None:
        target_fd = os.open(os.devnull, os.O_RDWR)
    else:
        target_fd = target_stream.fileno()

    system_stream = getattr(sys, stream_name)
    os.dup2(target_fd, system_stream.fileno())
    setattr(sys, '__{}__'.format(stream_name), system_stream)


def validate_stream(stream, *, stream_name):
    """Check if `stream` is an open io.IOBase instance."""
    if not isinstance(stream, io.IOBase):
        raise DaemonError(
            'Invalid {} stream object, an instance of io.IOBase is expected'.
            format(stream_name))

    if stream.closed:
        raise DaemonError('Stream {} is already closed'.format(stream_name))
