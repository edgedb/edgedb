#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import *

import logging
import os
import socket
import sys


SD_LISTEN_FDS_START = 3


logger = logging.getLogger('edb.server')


def _stream_socket_from_fd(fd: int) -> Optional[socket.socket]:
    try:
        sock = socket.socket(fileno=fd)
    except OSError:
        logger.warning(
            f"activation file descriptor {fd} is not a socket "
            f", ignoring"
        )
        return None

    if sock.family not in {socket.AF_INET, socket.AF_INET6}:
        logger.warning(
            f"activation file descriptor {fd} is not an AF_INET[6] socket "
            f", ignoring"
        )
        return None

    if sock.type != socket.SOCK_STREAM:
        logger.warning(
            f"activation file descriptor {fd} is not an SOCK_STREAM "
            f"socket, ignoring"
        )
        return None

    return sock


def sd_notify(message: str) -> None:
    notify_socket = os.environ.get('NOTIFY_SOCKET')
    if not notify_socket:
        return

    if notify_socket[0] == '@':
        notify_socket = '\0' + notify_socket[1:]

    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sd_sock:
        try:
            sd_sock.connect(notify_socket)
            sd_sock.sendall(message.encode())
        except Exception as e:
            logger.info('Could not send systemd notification: %s', e)


def sd_get_activation_listen_sockets(
    names: list[str],
) -> dict[str, socket.socket]:
    listen_pid = os.environ.pop("LISTEN_PID", "")
    listen_fds = os.environ.pop("LISTEN_FDS", "")
    listen_fdnames = os.environ.pop("LISTEN_FDNAMES", "")

    if not listen_pid or not listen_fds:
        return {}

    try:
        expected_pid = int(listen_pid)
    except ValueError:
        logger.warning(
            "the value of LISTEN_PID environment variable "
            "is not a valid integer, ignoring socket activation data"
        )
        return {}

    if expected_pid != os.getpid():
        logger.warning(
            "the value of LISTEN_PID does not match the PID of this "
            "process, ignoring socket activation data"
        )
        return {}

    try:
        num_fds = int(listen_fds)
    except ValueError:
        logger.warning(
            "the value of LISTEN_FDS environment variable "
            "is not a valid integer, ignoring socket activation data"
        )
        return {}

    fd_names = listen_fdnames.split(":")
    fd_range = range(SD_LISTEN_FDS_START, SD_LISTEN_FDS_START + num_fds)
    sockets = {}

    for i, fd in enumerate(fd_range):
        os.set_inheritable(fd, False)

        try:
            name = fd_names[i]
        except IndexError:
            name = f"LISTEN_FD_{fd}"

        if names and name not in names:
            logger.warning(
                f"activation file descriptor {name} ({fd}) is not in any "
                f"of the passed --activation-socket-name= options, ignoring"
            )
            continue

        sock = _stream_socket_from_fd(fd)
        if sock is not None:
            sockets[name] = sock

    return sockets


if sys.platform == "darwin":
    import ctypes
    import ctypes.util

    syslib = ctypes.CDLL(ctypes.util.find_library('System'))
    syslib.launch_activate_socket.argypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.POINTER(ctypes.c_int)),
        ctypes.POINTER(ctypes.c_size_t),
    ]

    def _launch_activate_socket(name) -> list[int]:
        fds = ctypes.POINTER(ctypes.c_int)()
        num_fds = ctypes.c_size_t()
        result = syslib.launch_activate_socket(
            ctypes.c_char_p(name.encode("utf-8")),
            ctypes.byref(fds),
            ctypes.byref(num_fds),
        )
        if result == 0:
            return [fds[i] for i in range(num_fds.value)]
        else:
            logger.warning(f"launch_activate_socket({name}) returned {result}")
            return []

    def launchd_get_activation_listen_sockets(
        names: list[str],
    ) -> dict[str, socket.socket]:
        if not names:
            names = ["edgedb-server"]

        sockets = {}

        for name in names:
            fds = _launch_activate_socket(name)
            if len(fds) == 0:
                logger.warning(f"could not activate socket {name}")
                continue
            elif len(fds) != 1:
                logger.warning(
                    f"more than one socket returned by "
                    f"launch_activate_socket({name}), ignoring all but the "
                    f"first"
                )
                continue

            sock = _stream_socket_from_fd(fds[0])
            if sock is not None:
                sockets[name] = sock

        return sockets

else:
    def launchd_get_activation_listen_sockets(
        names: list[str],
    ) -> dict[str, socket.socket]:
        return {}


def get_activation_listen_sockets(
    names: list[str],
) -> dict[str, socket.socket]:
    if sys.platform == "darwin":
        sockets = launchd_get_activation_listen_sockets(names)
    else:
        sockets = sd_get_activation_listen_sockets(names)

    port = 0

    for name, sock in list(sockets.items()):
        this_port = sock.getsockname()[1]
        if port == 0:
            port = this_port
        elif port == this_port:
            continue
        else:
            logger.warning(
                f"activation sockets are not all on the same TCP port, "
                f"first socket is at {port} and socket {name!r} is at "
                f"{this_port}, ignoring the latter")
            sockets.pop(name)

    return sockets
