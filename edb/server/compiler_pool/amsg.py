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
import os
import struct


_len_unpacker = struct.Struct('!I').unpack
_len_packer = struct.Struct('!I').pack


class PoolClosedError(Exception):
    pass


class BaseFramedProtocol(asyncio.Protocol):

    def __init__(self, *, loop, con_waiter=None):
        self._loop = loop
        self._buffer = b''
        self._transport = None
        self._con_waiter = con_waiter
        self._curmsg_len = -1
        self._closed = False

    def process_message(self, msg):
        raise NotImplementedError

    def data_received(self, data):
        # TODO: rewrite to avoid buffer copies.
        self._buffer += data
        while self._buffer:
            if self._curmsg_len == -1:
                if len(self._buffer) >= 4:
                    self._curmsg_len = _len_unpacker(self._buffer[:4])[0]
                    self._buffer = self._buffer[4:]
                else:
                    return

            if self._curmsg_len > 0 and len(self._buffer) >= self._curmsg_len:
                msg = self._buffer[:self._curmsg_len]
                self._buffer = self._buffer[self._curmsg_len:]
                self._curmsg_len = -1
                self.process_message(msg)
            else:
                return

    def connection_made(self, tr):
        self._transport = tr
        if self._con_waiter is not None:
            self._con_waiter.set_result(True)
            self._con_waiter = None

    def connection_lost(self, exc):
        self._closed = True

        if self._con_waiter is not None:
            if exc is None:
                # The connection is aborted on our end
                self._con_waiter.set_result(None)
            else:
                self._con_waiter.set_exception(exc)
            self._con_waiter = None


class HubProtocol(BaseFramedProtocol):

    def __init__(self, *, loop, on_pid):
        super().__init__(loop=loop)
        self._msg_waiter = None
        self._on_pid = on_pid
        self._pid = None

    def send(self, waiter, payload: bytes):
        if self._msg_waiter is not None and not self._msg_waiter.done():
            raise RuntimeError('FramedProtocol: another send() is in progress')
        self._msg_waiter = waiter
        self._transport.writelines((_len_packer(len(payload)), payload))

    def process_message(self, msg):
        if self._msg_waiter is not None and not self._msg_waiter.done():
            self._msg_waiter.set_result(msg)
            self._msg_waiter = None

    def data_received(self, data):
        if self._pid is None:
            pid_data = data[:4]
            data = data[4:]
            self._pid = _len_unpacker(pid_data)[0]
            self._on_pid(self, self._transport, self._pid)
            if data:
                super().data_received(data)
        else:
            super().data_received(data)

    def connection_lost(self, exc):
        super().connection_lost(exc)

        if self._msg_waiter is not None:
            if exc is not None:
                self._msg_waiter.set_exception(exc)
            else:
                self._msg_waiter.set_exception(ConnectionError(
                    'lost connection to the worker during a call'))
            self._msg_waiter = None


class WorkerProtocol(BaseFramedProtocol):

    def __init__(self, loop, con_waiter, con):
        self._est = False
        self._con = con
        super().__init__(loop=loop, con_waiter=con_waiter)

    def reply(self, payload: bytes):
        self._transport.writelines((_len_packer(len(payload)), payload))

    def process_message(self, msg):
        self._con._on_message(msg)

    def connection_made(self, tr):
        super().connection_made(tr)
        tr.write(_len_packer(os.getpid()))

    def connection_lost(self, exc):
        super().connection_lost(exc)
        self._con._on_connection_lost(exc)


class HubConnection:

    def __init__(self, transport, protocol, loop):
        self._transport = transport
        self._protocol = protocol
        self._loop = loop

    def is_closed(self):
        return self._protocol._closed

    async def request(self, data: bytes) -> bytes:
        waiter = self._loop.create_future()
        self._protocol.send(waiter, data)
        return await waiter

    def abort(self):
        self._transport.abort()


class WorkerConnection:

    def __init__(self, loop):
        self._loop = loop
        self._msgs = asyncio.Queue(loop=loop)
        self._protocol = None
        self._transport = None
        self._con_lost_fut = loop.create_future()

    def is_closed(self):
        return self._protocol._closed

    def _on_message(self, msg: bytes):
        self._msgs.put_nowait(msg)

    def _on_connection_lost(self, exc):
        self._con_lost_fut.set_exception(
            PoolClosedError('connection to the pool is closed'))
        self._con_lost_fut._log_traceback = False

    async def reply(self, data):
        self._protocol.reply(data)

    async def next_request(self) -> bytes:
        getter = self._loop.create_task(self._msgs.get())
        await asyncio.wait(
            [getter, self._con_lost_fut],
            return_when=asyncio.FIRST_COMPLETED)

        if self._con_lost_fut.done():
            getter.cancel()
            return self._con_lost_fut.result()

        return getter.result()

    def abort(self):
        self._transport.abort()


async def worker_connect(sockname):
    loop = asyncio.get_running_loop()
    waiter = loop.create_future()
    con = WorkerConnection(loop)
    tr, pr = await loop.create_unix_connection(
        lambda: WorkerProtocol(loop=loop, con_waiter=waiter, con=con),
        path=sockname)
    con._protocol = pr
    con._transport = tr
    await waiter
    return con


class Server:

    def __init__(self, sockname, pool_size, loop):
        self._sockname = sockname
        self._loop = loop
        self._srv = None
        self._pids = {}
        self._pid_waiters = {}
        self._pool_size = pool_size
        self._ready_fut = loop.create_future()

    def _on_pid_connected(self, proto, tr, pid):
        assert pid not in self._pids
        self._pids[pid] = HubConnection(tr, proto, self._loop)
        if len(self._pids) == self._pool_size:
            self._ready_fut.set_result(True)

    def _proto_factory(self):
        return HubProtocol(loop=self._loop, on_pid=self._on_pid_connected)

    async def wait_until_ready(self):
        await self._ready_fut

    def iter_pids(self):
        return iter(self._pids)

    async def get_by_pid(self, pid):
        if not self._ready_fut.done():
            raise RuntimeError(
                'the message server does not yet have all workers connected')

        # Will raise if it was cancelled of there was an error.
        self._ready_fut.result()

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
