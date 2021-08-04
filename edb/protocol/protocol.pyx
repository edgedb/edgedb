#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


import asyncio
import re
import time

from edgedb import con_utils
from edgedb.protocol.asyncio_proto cimport AsyncIOProtocol
from edgedb.protocol.protocol cimport ReadBuffer, WriteBuffer

from . import messages


class Protocol(AsyncIOProtocol):
    pass


cdef class Connection:

    def __init__(self, pr, tr):
        self._protocol = pr
        self._transport = tr
        self.inbox = []

    async def connect(self):
        await self._protocol.connect()

    async def sync(self):
        await self.send(messages.Sync())
        reply = await self.recv()
        if not isinstance(reply, messages.ReadyForCommand):
            raise AssertionError(
                f'invalid response for Sync request: {reply!r}')
        return reply.transaction_state

    async def recv(self):
        while True:
            await self._protocol.wait_for_message()
            mtype = self._protocol.buffer.get_message_type()
            data = self._protocol.buffer.consume_message()
            msg = messages.ServerMessage.parse(mtype, data)

            if isinstance(msg, messages.LogMessage):
                self.inbox.append(msg)
                continue

            return msg

    async def recv_match(self, msgcls, **fields):
        message = await self.recv()
        if not isinstance(message, msgcls):
            raise AssertionError(
                f'expected for {msgcls.__name__} message, received '
                f'{type(message).__name__}')
        for fieldname, expected in fields.items():
            val = getattr(message, fieldname)
            if isinstance(expected, str):
                if not re.match(expected, val):
                    raise AssertionError(
                        f'{msgcls.__name__}.{fieldname} value {val!r} '
                        f'does not match expected regexp {expected!r}')
            else:
                if expected != val:
                    raise AssertionError(
                        f'{msgcls.__name__}.{fieldname} value {val!r} '
                        f'does not equal to expected {expected!r}')

    async def send(self, *msgs: messages.ClientMessage):
        cdef WriteBuffer buf

        for msg in msgs:
            out = msg.dump()
            buf = WriteBuffer.new()
            buf.write_bytes(out)
            self._protocol.write(buf)

    async def aclose(self):
        # TODO: Fix when edgedb-python implements proper cancellation
        self._protocol.abort()


async def new_connection(
    dsn: str = None,
    *,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    admin: str = None,
    database: str = None,
    tls_ca_file: str = None,
    tls_verify_hostname: bool = None,
    timeout: float = 60,
    use_tls: bool = True,
):
    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        admin=admin, database=database,
        tls_ca_file=tls_ca_file, tls_verify_hostname=tls_verify_hostname,
        timeout=timeout, command_timeout=None, server_settings=None,
        wait_until_available=timeout)

    loop = asyncio.get_running_loop()

    last_error = None
    addr = None
    for addr in addrs:
        before = time.monotonic()
        try:
            if timeout <= 0:
                raise asyncio.TimeoutError

            protocol_factory = lambda: Protocol(params, loop)

            if isinstance(addr, str):
                connector = loop.create_unix_connection(
                    protocol_factory, addr)
            else:
                connector = loop.create_connection(
                    protocol_factory, *addr,
                    ssl=params.ssl_ctx if use_tls else None,
                )

            before = time.monotonic()
            try:
                tr, pr = await asyncio.wait_for(connector, timeout=timeout)
            finally:
                timeout -= time.monotonic() - before

            return Connection(pr, tr)

        except (OSError, asyncio.TimeoutError, ConnectionError) as ex:
            last_error = ex

    raise last_error
