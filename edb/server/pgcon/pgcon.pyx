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


import asyncio
import codecs
import json

cimport cython
cimport cpython

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from edb import errors

from edb.schema import objects as s_obj
from edb.server import defines

from edb.pgsql import common as pg_common

from edb.server.pgproto cimport hton
from edb.server.pgproto cimport pgproto
from edb.server.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
    FRBuffer,
    frb_init,
    frb_read,
)

from edb.server import compiler
from edb.server.cache cimport stmt_cache
from edb.server.mng_port cimport edgecon

from edb.common import debug

from . import errors as pgerror


DEF DATA_BUFFER_SIZE = 100_000
DEF PREP_STMTS_CACHE = 100


cdef object CARD_NA = compiler.ResultCardinality.NOT_APPLICABLE


async def connect(addr, dbname):
    loop = asyncio.get_running_loop()

    _, protocol = await loop.create_unix_connection(
        lambda: PGProto(dbname, loop, addr), addr)

    await protocol.connect()
    return protocol


@cython.final
cdef class EdegDBCodecContext(pgproto.CodecContext):

    cdef:
        object _codec

    def __cinit__(self):
        self._codec = codecs.lookup('utf-8')

    cpdef get_text_codec(self):
        return self._codec

    cdef is_encoding_utf8(self):
        return True


@cython.final
cdef class PGProto:

    def __init__(self, dbname, loop, addr):
        self.buffer = ReadBuffer()

        self.loop = loop
        self.dbname = dbname

        self.transport = None
        self.msg_waiter = None

        self.prep_stmts = stmt_cache.StatementsCache(maxsize=PREP_STMTS_CACHE)

        self.connected_fut = loop.create_future()
        self.connected = False

        self.waiting_for_sync = False
        self.xact_status = PQTRANS_UNKNOWN

        self.backend_pid = -1
        self.backend_secret = -1

        self.last_parse_prep_stmts = []
        self.debug = debug.flags.server_proto

        self.pgaddr = addr

    def debug_print(self, *args):
        print(
            '::PGPROTO::',
            *args,
        )

    def get_pgaddr(self):
        return self.pgaddr

    def in_tx(self):
        return (
            self.xact_status == PQTRANS_INTRANS or
            self.xact_status == PQTRANS_INERROR
        )

    def is_connected(self):
        return bool(self.connected and self.transport is not None)

    def abort(self):
        if not self.transport:
            return
        self.transport.abort()
        self.transport = None
        self.connected = False

    def terminate(self):
        if not self.transport:
            return
        self.write(WriteBuffer.new_message(b'X').end_message())
        self.transport.close()
        self.transport = None
        self.connected = False

        if self.msg_waiter and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())

    async def sync(self):
        if self.waiting_for_sync:
            raise RuntimeError('a "sync" has already been requested')

        self.waiting_for_sync = True
        self.write(SYNC_MESSAGE)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'Z':
                self.parse_sync_message()
                return
            else:
                self.fallthrough()

    async def wait_for_sync(self):
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()
            if mtype == b'Z':
                self.parse_sync_message()
                return
            else:
                if not self.parse_notification():
                    if PG_DEBUG:
                        print(f'PGCon.wait_for_sync: discarding '
                              f'{chr(mtype)!r} message')
                    self.buffer.discard_message()

    cdef before_prepare(self, stmt_name, dbver, WriteBuffer outbuf):
        parse = 1

        while self.prep_stmts.needs_cleanup():
            stmt_name_to_clean = self.prep_stmts.cleanup_one()
            outbuf.write_buffer(
                self.make_clean_stmt_message(stmt_name_to_clean))

        if stmt_name in self.prep_stmts:
            if self.prep_stmts[stmt_name] == dbver:
                parse = 0
            else:
                outbuf.write_buffer(
                    self.make_clean_stmt_message(stmt_name))
                del self.prep_stmts[stmt_name]
                store_stmt = 1
        else:
            store_stmt = 1

        return parse, store_stmt

    async def parse_execute_json(self, sql, sql_hash, dbver,
                                 use_prep_stmt, args, textmode):
        cdef:
            WriteBuffer parse_buf
            WriteBuffer bind_buf
            WriteBuffer execute_buf
            WriteBuffer buf
            char *str
            ssize_t size
            bint parse = 1
            bint store_stmt = 0

        self.before_command()

        buf = WriteBuffer.new()

        if use_prep_stmt:
            stmt_name = sql_hash
            parse, store_stmt = self.before_prepare(
                stmt_name, dbver, buf)
        else:
            stmt_name = b''

        if parse:
            parse_buf = WriteBuffer.new_message(b'P')
            parse_buf.write_bytestring(stmt_name)  # statement name
            parse_buf.write_bytestring(sql)
            # we don't want to specify parameter types
            parse_buf.write_int16(0)
            parse_buf.end_message()
            buf.write_buffer(parse_buf)

        bind_buf = WriteBuffer.new_message(b'B')
        bind_buf.write_bytestring(b'')  # portal name
        bind_buf.write_bytestring(stmt_name)  # statement name
        bind_buf.write_int32(0x00010001)  # binary for all parameters
        bind_buf.write_int16(len(args))  # number of parameters

        for arg in args:
            jarg = json.dumps(arg)
            pgproto.jsonb_encode(DEFAULT_CODEC_CONTEXT, bind_buf, jarg)

        bind_buf.write_int32(0x00010001)  # binary for the output
        bind_buf.end_message()
        buf.write_buffer(bind_buf)

        execute_buf = WriteBuffer.new_message(b'E')
        execute_buf.write_bytestring(b'')  # portal name
        execute_buf.write_int32(0)  # return all rows
        execute_buf.end_message()
        buf.write_buffer(execute_buf)

        buf.write_bytes(SYNC_MESSAGE)

        self.write(buf)
        error = None
        self.waiting_for_sync = True
        data = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'D':
                    # DataRow
                    if data is not None:
                        error = RuntimeError(
                            f'received more than one DataRow '
                            f'for a JSON query {sql!r}')
                        self.buffer.discard_message()
                        continue

                    ncol = self.buffer.read_int16()
                    if ncol != 1:
                        error = RuntimeError(
                            f'received more than column in DataRow '
                            f'for a JSON query {sql!r}')
                        self.buffer.discard_message()
                        continue

                    coll = self.buffer.read_int32()
                    if coll == -1:
                        error = RuntimeError(
                            f'received NULL for a JSON query {sql!r}')
                        self.buffer.discard_message()
                        continue
                    if coll <= 2:
                        error = RuntimeError(
                            f'unable to unpack DataRow '
                            f'for a JSON query {sql!r}')
                        self.buffer.discard_message()
                        continue

                    if not textmode:
                        b = self.buffer.read_byte()
                        if b != 1:
                            error = RuntimeError(
                                f'invalid JSONB format for '
                                f'a JSON query {sql!r}')
                            self.buffer.discard_message()
                            continue

                        data = self.buffer.read_bytes(coll - 1)
                    else:
                        data = self.buffer.read_bytes(coll)

                elif mtype == b'E':
                    # ErrorResponse
                    fields = self.parse_error_message()
                    error = pgerror.BackendError(fields=fields)

                elif mtype == b'1':
                    # ParseComplete
                    self.buffer.discard_message()
                    if store_stmt:
                        self.prep_stmts[stmt_name] = dbver

                elif mtype in {b'C', b'n', b'2', b'I'}:
                    # CommandComplete
                    # NoData
                    # BindComplete
                    # EmptyQueryResponse
                    self.buffer.discard_message()

                elif mtype == b'Z':
                    # ReadyForQuery
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if error is not None:
            raise error
        if data is None:
            raise RuntimeError(
                f'no data received for a JSON query {sql!r}')
        return data

    async def parse_execute(self,
                            bint parse,
                            bint execute,
                            object query,
                            edgecon.EdgeConnection edgecon,
                            WriteBuffer bind_data,
                            bint send_sync,
                            bint use_prep_stmt):

        cdef:
            WriteBuffer packet
            WriteBuffer buf
            bytes stmt_name
            bint store_stmt = 0

            bint has_result = query.cardinality is not CARD_NA

            uint64_t msgs_num = len(query.sql)
            uint64_t msgs_parsed = 0
            uint64_t msgs_executed = 0
            uint64_t i

        self.before_command()

        if not parse and not execute:
            raise RuntimeError('invalid parse/execute call')

        packet = WriteBuffer.new()

        if use_prep_stmt:
            assert parse and execute
            stmt_name = query.sql_hash
            parse, store_stmt = self.before_prepare(
                stmt_name, query.dbver, packet)
        else:
            stmt_name = b''

        if parse:
            if len(self.last_parse_prep_stmts):
                for stmt_name_to_clean in self.last_parse_prep_stmts:
                    packet.write_buffer(
                        self.make_clean_stmt_message(stmt_name_to_clean))
                self.last_parse_prep_stmts.clear()

            if stmt_name == b'' and msgs_num > 1:
                i = 0
                for sql in query.sql:
                    pname = b'__p%d__' % i
                    self.last_parse_prep_stmts.append(pname)
                    buf = WriteBuffer.new_message(b'P')
                    buf.write_bytestring(pname)
                    buf.write_bytestring(sql)
                    buf.write_int16(0)
                    packet.write_buffer(buf.end_message())
                    i += 1
            else:
                if len(query.sql) != 1:
                    raise errors.InternalServerError(
                        'cannot PARSE more than one SQL query '
                        'in non-anonymous mode')
                msgs_num = 1
                buf = WriteBuffer.new_message(b'P')
                buf.write_bytestring(stmt_name)
                buf.write_bytestring(query.sql[0])
                buf.write_int16(0)
                packet.write_buffer(buf.end_message())

        if execute:
            assert bind_data is not None

            if stmt_name == b'' and msgs_num > 1:
                for s in self.last_parse_prep_stmts:
                    buf = WriteBuffer.new_message(b'B')
                    buf.write_bytestring(b'')  # portal name
                    buf.write_bytestring(s)  # statement name
                    buf.write_buffer(bind_data)
                    packet.write_buffer(buf.end_message())

                    buf = WriteBuffer.new_message(b'E')
                    buf.write_bytestring(b'')  # portal name
                    buf.write_int32(0)  # limit: 0 - return all rows
                    packet.write_buffer(buf.end_message())

            else:
                buf = WriteBuffer.new_message(b'B')
                buf.write_bytestring(b'')  # portal name
                buf.write_bytestring(stmt_name)  # statement name
                buf.write_buffer(bind_data)
                packet.write_buffer(buf.end_message())

                buf = WriteBuffer.new_message(b'E')
                buf.write_bytestring(b'')  # portal name
                buf.write_int32(0)  # limit: 0 - return all rows
                packet.write_buffer(buf.end_message())

        if send_sync:
            packet.write_bytes(SYNC_MESSAGE)
            self.waiting_for_sync = True
        else:
            packet.write_bytes(FLUSH_MESSAGE)
        self.write(packet)

        try:
            buf = None
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'D' and execute:
                        # DataRow
                        if not has_result:
                            raise errors.InternalServerError(
                                f'query that was inferred to have '
                                f'no data returned received a DATA package; '
                                f'query: {query.sql}')

                        if buf is None:
                            buf = WriteBuffer.new()

                        self.buffer.redirect_messages(buf, b'D')
                        if buf.len() >= DATA_BUFFER_SIZE:
                            edgecon.write(buf)
                            buf = None

                    elif mtype == b'C' and execute:  ## result
                        # CommandComplete
                        self.buffer.discard_message()
                        if buf is not None:
                            edgecon.write(buf)
                            buf = None
                        msgs_executed += 1
                        if msgs_executed == msgs_num:
                            return

                    elif mtype == b'1' and parse:
                        # ParseComplete
                        self.buffer.discard_message()
                        if store_stmt:
                            self.prep_stmts[stmt_name] = query.dbver
                        msgs_parsed += 1
                        if not execute and msgs_parsed == msgs_num:
                            return

                    elif mtype == b'E':  ## result
                        # ErrorResponse
                        er = self.parse_error_message()
                        raise pgerror.BackendError(fields=er)

                    elif mtype == b'n' and execute:
                        # NoData
                        self.buffer.discard_message()

                    elif mtype == b's' and execute:  ## result
                        # PortalSuspended
                        self.buffer.discard_message()
                        return

                    elif mtype == b'2' and execute:
                        # BindComplete
                        self.buffer.discard_message()

                    elif mtype == b'I' and execute:  ## result
                        # EmptyQueryResponse
                        self.buffer.discard_message()
                        return

                    elif mtype == b'3':
                        # CloseComplete
                        self.buffer.discard_message()

                    else:
                        self.fallthrough()

                finally:
                    self.buffer.finish_message()
        finally:
            if send_sync:
                await self.wait_for_sync()

    async def simple_query(self, bytes sql, bint ignore_data):
        cdef:
            WriteBuffer packet
            WriteBuffer buf

        self.before_command()

        buf = WriteBuffer.new_message(b'Q')
        buf.write_bytestring(sql)
        self.write(buf.end_message())

        exc = None
        result = None

        self.waiting_for_sync = True

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'D':
                    if ignore_data:
                        self.buffer.discard_message()
                    else:
                        ncol = self.buffer.read_int16()
                        row = []
                        for i in range(ncol):
                            coll = self.buffer.read_int32()
                            if coll == -1:
                                row.append(None)
                            else:
                                row.append(self.buffer.read_bytes(coll))
                        if result is None:
                            result = []
                        result.append(row)

                elif mtype == b'T':
                    # RowDescription
                    self.buffer.discard_message()

                elif mtype == b'C':
                    # CommandComplete
                    self.buffer.discard_message()

                elif mtype == b'E':
                    # ErrorResponse
                    exc = self.parse_error_message()

                elif mtype == b'I':
                    # EmptyQueryResponse
                    self.buffer.discard_message()

                elif mtype == b'Z':
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if exc:
            raise pgerror.BackendError(fields=exc)
        return result

    async def connect(self):
        cdef:
            WriteBuffer outbuf
            WriteBuffer buf
            char mtype
            int32_t status

        if self.connected_fut is not None:
            await self.connected_fut
        if self.connected:
            raise RuntimeError('already connected')
        if self.transport is None:
            raise RuntimeError('no transport object in connect()')

        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_bytestring(b'client_encoding')
        buf.write_bytestring(b'utf-8')

        buf.write_bytestring(b'edgedb_use_typeoids')
        buf.write_bytestring(b'false')

        buf.write_bytestring(b'search_path')
        buf.write_utf8('edgedb')

        buf.write_utf8('user')
        buf.write_utf8(defines.EDGEDB_SUPERUSER)

        buf.write_utf8('database')
        buf.write_utf8(self.dbname)

        buf.write_bytestring(b'')

        # Send the buffer
        outbuf = WriteBuffer()
        outbuf.write_int32(buf.len() + 4)
        outbuf.write_buffer(buf)
        self.write(outbuf)

        # Need this to handle first ReadyForQuery
        self.waiting_for_sync = True

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'R':
                    # Authentication...
                    status = self.buffer.read_int32()
                    if status != 0:
                        raise RuntimeError('unsupported auth method')

                elif mtype == b'K':
                    # BackendKeyData
                    self.backend_pid = self.buffer.read_int32()
                    self.backend_secret = self.buffer.read_int32()

                elif mtype == b'E':
                    # ErrorResponse
                    er = self.parse_error_message()
                    raise pgerror.BackendError(fields=er)

                elif mtype == b'Z':
                    # ReadyForQuery
                    self.parse_sync_message()
                    self.connected = True
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

    def before_command(self):
        if not self.connected:
            raise RuntimeError('not connected')

        if self.waiting_for_sync:
            raise RuntimeError('cannot issue new command')

    cdef write(self, buf):
        self.transport.write(buf)

    cdef fallthrough(self):
        if self.parse_notification():
            return

        cdef:
            char mtype = self.buffer.get_message_type()

        raise RuntimeError(
            f'unexpected message type {chr(mtype)!r}')

    cdef parse_notification(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == b'S':
            # ParameterStatus
            self.buffer.discard_message()
            return True

        elif mtype == b'A':
            # NotificationResponse
            self.buffer.discard_message()
            return True

        elif mtype == b'N':
            # NoticeResponse
            self.buffer.discard_message()
            return True

        return False

    cdef parse_error_message(self):
        cdef:
            char code
            bytes message
            dict parsed = {}

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break

            message = self.buffer.read_null_str()

            parsed[chr(code)] = message.decode()

        if self.debug:
            self.debug_print('ERROR', parsed)

        self.buffer.finish_message()
        return parsed

    cdef parse_sync_message(self):
        cdef char status

        if not self.waiting_for_sync:
            raise RuntimeError('unexpected sync')
        self.waiting_for_sync = False

        assert self.buffer.get_message_type() == b'Z'

        status = self.buffer.read_byte()

        if status == b'I':
            self.xact_status = PQTRANS_IDLE
        elif status == b'T':
            self.xact_status = PQTRANS_INTRANS
        elif status == b'E':
            self.xact_status = PQTRANS_INERROR
        else:
            self.xact_status = PQTRANS_UNKNOWN

        self.buffer.finish_message()

    cdef make_clean_stmt_message(self, bytes stmt_name):
        cdef WriteBuffer buf
        buf = WriteBuffer.new_message(b'C')
        buf.write_byte(b'S')
        buf.write_bytestring(stmt_name)
        return buf.end_message()

    async def wait_for_message(self):
        if self.buffer.take_message():
            return
        self.msg_waiter = self.loop.create_future()
        await self.msg_waiter

    def connection_made(self, transport):
        if self.transport is not None:
            raise RuntimeError('connection_made: invalid connection status')
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def connection_lost(self, exc):
        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())
            return

        if self.msg_waiter is not None and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

        self.transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if self.msg_waiter is not None and self.buffer.take_message():
            self.msg_waiter.set_result(True)
            self.msg_waiter = None

    def eof_received(self):
        pass


cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
cdef bytes FLUSH_MESSAGE = bytes(WriteBuffer.new_message(b'H').end_message())

cdef EdegDBCodecContext DEFAULT_CODEC_CONTEXT = EdegDBCodecContext()
