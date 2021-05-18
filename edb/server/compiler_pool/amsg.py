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

import asyncio
import functools
import logging
import os
import socket
import struct
import typing


logger = logging.getLogger("edb.server")
_uint64_unpacker = struct.Struct('!Q').unpack
_uint64_packer = struct.Struct('!Q').pack


def _wakeup_waiter(waiter, fut):
    if not waiter.done():
        waiter.set_result(fut.result())


class MessageStream:
    """Data stream that yields messages."""

    def __init__(self):
        self._buffer = b''
        self._curmsg_len = -1

    def feed_data(self, data):
        # TODO: rewrite to avoid buffer copies.
        self._buffer += data
        while self._buffer:
            if self._curmsg_len == -1:
                if len(self._buffer) >= 8:
                    self._curmsg_len = _uint64_unpacker(self._buffer[:8])[0]
                    self._buffer = self._buffer[8:]
                else:
                    return

            if self._curmsg_len > 0 and len(self._buffer) >= self._curmsg_len:
                msg = self._buffer[:self._curmsg_len]
                self._buffer = self._buffer[self._curmsg_len:]
                self._curmsg_len = -1
                yield msg
            else:
                return


class HiveProtocol(asyncio.Protocol):
    """The Protocol used by the Hive per connection to the Queen or Worker."""

    def __init__(self, *, loop, on_pid, on_connection_lost):
        self._loop = loop
        self._transport = None
        self._closed = False
        self._stream = MessageStream()
        self._resp_waiter = None
        self._resp_expected_id = -1
        self._on_pid = on_pid
        self._on_connection_lost = on_connection_lost
        self._pid = None

    def connection_made(self, tr):
        self._transport = tr

    def send(self, req_id: int, waiter: asyncio.Future, payload: bytes):
        if self._resp_waiter is not None and not self._resp_waiter.done():
            raise RuntimeError('FramedProtocol: another send() is in progress')
        self._resp_waiter = waiter
        self._resp_expected_id = req_id
        self._transport.writelines(
            (_uint64_packer(len(payload) + 8), _uint64_packer(req_id), payload)
        )

    def process_message(self, msg):
        msgview = memoryview(msg)
        req_id = _uint64_unpacker(msgview[:8])[0]
        if req_id != self._resp_expected_id:
            # This could have happened if the previous request got cancelled.
            return
        if self._resp_waiter is not None and not self._resp_waiter.done():
            self._resp_waiter.set_result(msgview[8:])
            self._resp_waiter = None
            self._resp_expected_id = -1

    def data_received(self, data):
        if self._pid is None:
            worker_type = data[:1]
            pid_data = data[1:9]
            data = data[9:]
            self._pid = _uint64_unpacker(pid_data)[0]
            self._on_pid(self, self._transport, self._pid, worker_type)
            if not data:
                return
        for msg in self._stream.feed_data(data):
            self.process_message(msg)

    def connection_lost(self, exc):
        self._closed = True

        if self._resp_waiter is not None:
            if exc is not None:
                self._resp_waiter.set_exception(exc)
            else:
                self._resp_waiter.set_exception(ConnectionError(
                    'lost connection to the worker during a call'))
            self._resp_waiter = None

        self._on_connection_lost(self._pid)


class HiveConnection:
    """An abstraction of the Hive's connections to the Queen and Workers."""

    def __init__(self, transport, protocol, loop, pid):
        self._transport = transport
        self._protocol = protocol
        self._loop = loop
        self._req_id_cnt = 0
        self.pid = pid

    def is_closed(self):
        return self._protocol._closed

    async def request(self, data: bytes) -> bytes:
        self._req_id_cnt += 1
        req_id = self._req_id_cnt

        waiter = self._loop.create_future()
        self._protocol.send(req_id, waiter, data)
        return await waiter

    def abort(self):
        self._transport.abort()


class QueenConnection:
    """Connection object used by the Queen's process."""

    WORKER_TYPE = b'Q'

    def __init__(self, sockname):
        self._sock = socket.socket(socket.AF_UNIX)
        self._sock.connect(sockname)
        self._sock.sendall(self.WORKER_TYPE + _uint64_packer(os.getpid()))
        self._stream = MessageStream()

    def _on_message(self, msg: bytes):
        msgview = memoryview(msg)
        req_id = _uint64_unpacker(msgview[:8])[0]
        return req_id, msgview[8:]

    def reply(self, req_id, payload):
        self._sock.sendall(
            b"".join(
                (
                    _uint64_packer(len(payload) + 8),
                    _uint64_packer(req_id),
                    payload,
                )
            )
        )

    def iter_request(self):
        while True:
            if self._sock is not None:
                data = self._sock.recv(4096)
            else:
                data = None
            if not data:
                self.abort()
                return
            yield from map(self._on_message, self._stream.feed_data(data))

    def abort(self):
        if self._sock is not None:
            self._sock.close()
            self._sock = None


class WorkerConnection(QueenConnection):
    """Connection object used by the the Worker's process."""

    WORKER_TYPE = b'W'


class HiveControlProtocol:
    def queen_connected(self, conn: HiveConnection):
        pass

    def queen_disconnected(self):
        pass

    def worker_connected(self, pid):
        pass

    def worker_disconnected(self, pid):
        pass


class Hive:

    _proto: HiveControlProtocol
    _queen: asyncio.Future[HiveConnection]
    _pids: typing.Dict[int, HiveConnection]

    def __init__(self, sockname, loop, control_protocol):
        self._sockname = sockname
        self._loop = loop
        self._srv = None
        self._pids = {}
        self._queen = loop.create_future()
        self._proto = control_protocol

    def _on_pid_connected(self, proto, tr, pid, worker_type):
        assert pid not in self._pids
        conn = HiveConnection(tr, proto, self._loop, pid)
        if worker_type == QueenConnection.WORKER_TYPE:
            if self._queen.done():
                raise RuntimeError(
                    "More than one Queen found in the same Hive!"
                )
            logger.info("The compiler Queen is ready.")
            self._proto.queen_connected(conn)
            self._queen.set_result(conn)
        else:
            self._pids[pid] = conn
            self._proto.worker_connected(pid)

    def _on_pid_disconnected(self, pid: typing.Optional[int]):
        if not pid:
            return
        if pid in self._pids:
            self._pids.pop(pid)
            self._proto.worker_disconnected(pid)
        elif self._queen.done():
            queen = self._queen.result()
            if pid == queen.pid:
                logger.error(
                    "We lost the compiler Queen - we won't be able to spawn "
                    "more compiler Workers until the Queen is back."
                )
                self._queen = self._loop.create_future()
                self._proto.queen_disconnected()

    def _proto_factory(self):
        return HiveProtocol(
            loop=self._loop,
            on_pid=self._on_pid_connected,
            on_connection_lost=self._on_pid_disconnected,
        )

    async def get_by_pid(self, pid):
        return self._pids[pid]

    async def start(self):
        self._srv = await self._loop.create_unix_server(
            self._proto_factory,
            path=self._sockname)

    async def stop(self):
        self._srv.close()
        await self._srv.wait_closed()
        for con in self._pids.values():
            con.abort()
        if self._queen.done():
            self._queen.result().abort()

    async def spawn_worker(self):
        if self._queen.done():
            queen = self._queen.result()
        else:
            waiter = self._loop.create_future()
            cb = functools.partial(_wakeup_waiter, waiter)
            self._queen.add_done_callback(cb)
            try:
                queen = await waiter
            except asyncio.CancelledError:
                try:
                    self._queen.remove_done_callback(cb)
                finally:
                    raise
        return await queen.request(b'')
