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
import decimal
import codecs
import hashlib
import json
import os.path
import socket

cimport cython
cimport cpython

from . cimport cpythonx

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from edb import errors

from edb.schema import objects as s_obj

from edb.pgsql.common import quote_literal as pg_ql

from edb.server.pgproto cimport hton
from edb.server.pgproto cimport pgproto
from edb.server.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_get_len,
)

from edb.server import buildmeta
from edb.server import compiler
from edb.server import defines
from edb.server.cache cimport stmt_cache
from edb.server.protocol cimport binary as edgecon

from edb.common import debug

from . import errors as pgerror


DEF DATA_BUFFER_SIZE = 100_000
DEF PREP_STMTS_CACHE = 100
DEF TCP_KEEPIDLE = 24
DEF TCP_KEEPINTVL = 2
DEF TCP_KEEPCNT = 3

DEF COPY_SIGNATURE = b"PGCOPY\n\377\r\n\0"


cdef object CARD_NO_RESULT = compiler.ResultCardinality.NO_RESULT


cdef bytes INIT_CON_SCRIPT = None


def _build_init_con_script() -> bytes:
    return (f'''
        CREATE TEMPORARY TABLE _edgecon_state (
            name text NOT NULL,
            value jsonb NOT NULL,
            type text NOT NULL CHECK(type = 'C' OR type = 'A' OR type = 'R'),
            UNIQUE(name, type)
        );

        CREATE TEMPORARY TABLE _edgecon_current_savepoint (
            sp_id bigint NOT NULL,
            _sentinel bigint DEFAULT -1,
            UNIQUE(_sentinel)
        );

        PREPARE _clear_state AS
            DELETE FROM _edgecon_state
            WHERE _edgecon_state.type != 'R';

        PREPARE _apply_state(jsonb) AS
            INSERT INTO
                _edgecon_state(name, value, type)
            SELECT
                e->>'name' AS name,
                e->'value' AS value,
                e->>'type' AS type
            FROM
                jsonb_array_elements($1::jsonb) AS e;

        INSERT INTO _edgecon_state
            (name, value, type)
        VALUES
            ('server_version', {pg_ql(buildmeta.get_version_json())}, 'R');
    ''').encode('utf-8')


async def connect(connargs, dbname):
    global INIT_CON_SCRIPT

    loop = asyncio.get_running_loop()

    host = connargs.get("host")
    port = connargs.get("port")

    if host.startswith('/'):
        addr = os.path.join(host, f'.s.PGSQL.{port}')
        _, pgcon = await loop.create_unix_connection(
            lambda: PGConnection(dbname, loop, connargs), addr)

    else:
        trans, pgcon = await loop.create_connection(
            lambda: PGConnection(dbname, loop, connargs), host=host, port=port)

        # TCP keepalive was initially added here for special cases where idle
        # connections are dropped silently on GitHub Action running test suite
        # against AWS RDS. We are keeping the TCP keepalive for generic
        # Postgres connections as the kernel overhead is considered low, and
        # in certain cases it does save us some reconnection time.
        sock = trans.get_extra_info('socket')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, 'TCP_KEEPIDLE'):
            sock.setsockopt(socket.IPPROTO_TCP,
                            socket.TCP_KEEPIDLE, TCP_KEEPIDLE)
            sock.setsockopt(socket.IPPROTO_TCP,
                            socket.TCP_KEEPINTVL, TCP_KEEPINTVL)
            sock.setsockopt(socket.IPPROTO_TCP,
                            socket.TCP_KEEPCNT, TCP_KEEPCNT)

    await pgcon.connect()

    if connargs['user'] != defines.EDGEDB_SUPERUSER:
        await pgcon.simple_query(
            f'SET SESSION AUTHORIZATION {defines.EDGEDB_SUPERUSER}'.encode(),
            ignore_data=True,
        )

    if INIT_CON_SCRIPT is None:
        INIT_CON_SCRIPT = _build_init_con_script()

    await pgcon.simple_query(INIT_CON_SCRIPT, ignore_data=True)

    return pgcon


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
cdef class PGConnection:

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
        self.server = None

        self.idle = True
        self.cancel_fut = None

    def debug_print(self, *args):
        print(
            '::PGCONN::',
            *args,
        )

    def get_pgaddr(self):
        return self.pgaddr

    def in_tx(self):
        return (
            self.xact_status == PQTRANS_INTRANS or
            self.xact_status == PQTRANS_INERROR
        )

    def is_cancelling(self):
        return self.cancel_fut is not None

    def start_pg_cancellation(self):
        if self.cancel_fut is not None:
            raise RuntimeError('another cancellation is in progress')
        self.cancel_fut = self.loop.create_future()

    def finish_pg_cancellation(self):
        assert self.cancel_fut is not None
        self.cancel_fut.set_result(True)

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
            self.msg_waiter = None

    async def set_server(self, server):
        assert self.dbname == defines.EDGEDB_SYSTEM_DB
        await self.simple_query(
            b'LISTEN __edgedb_sysevent__;',
            ignore_data=True
        )
        self.server = server

    async def signal_sysevent(self, event, **kwargs):
        assert self.dbname == defines.EDGEDB_SYSTEM_DB
        event = json.dumps({
            'event': event,
            'server_id': self.server._server_id,
            'args': kwargs,
        })
        query = f"""
            SELECT pg_notify(
                '__edgedb_sysevent__',
                {pg_ql(event)}
            )
        """.encode()
        await self.simple_query(query, True)

    async def sync(self):
        if self.waiting_for_sync:
            raise RuntimeError('a "sync" has already been requested')

        self.before_command()
        try:
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
        finally:
            await self.after_command()

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
                    if PG_DEBUG or self.debug:
                        self.debug_print(f'PGCon.wait_for_sync: discarding '
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

    async def _parse_execute_to_buf(
        self,
        sql,
        sql_hash,
        dbver,
        use_prep_stmt,
        args,
        WriteBuffer out,
    ):
        cdef:
            WriteBuffer parse_buf
            WriteBuffer bind_buf
            WriteBuffer execute_buf
            WriteBuffer buf
            ssize_t size
            bint parse = 1
            bint store_stmt = 0

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
        # number of parameters
        bind_buf.write_int16(<int16_t><uint16_t>(len(args)))

        for arg in args:
            if isinstance(arg, decimal.Decimal):
                jarg = str(arg)
            else:
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
                    self.buffer.redirect_messages(out, b'D', 0)

                elif mtype == b'E':
                    # ErrorResponse
                    er_cls, fields = self.parse_error_message()
                    error = er_cls(fields=fields)

                elif mtype == b'1':
                    # ParseComplete
                    self.buffer.discard_message()
                    if store_stmt:
                        self.prep_stmts[stmt_name] = dbver

                elif mtype in {b'C', b'n', b'2', b'I', b'3'}:
                    # CommandComplete
                    # NoData
                    # BindComplete
                    # EmptyQueryResponse
                    # CloseComplete
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

        return data

    async def _parse_execute_json(
        self,
        sql,
        sql_hash,
        dbver,
        use_prep_stmt,
        args,
    ):
        cdef:
            WriteBuffer out
            Py_buffer pybuf

        out = WriteBuffer.new()
        await self._parse_execute_to_buf(
            sql, sql_hash, dbver, use_prep_stmt, args, out)

        cpython.PyObject_GetBuffer(out, &pybuf, cpython.PyBUF_SIMPLE)
        try:
            if pybuf.len == 0:
                return None

            if pybuf.len < 11 or (<char*>pybuf.buf)[0] != b'D':
                data = cpython.PyBytes_FromStringAndSize(
                    <char*>pybuf.buf, pybuf.len)
                raise RuntimeError(
                    f'invalid protocol-level result of a JSON query '
                    f'sql:{sql} buf-len:{pybuf.len} buf:{data}')

            mlen = hton.unpack_int32(<char*>pybuf.buf + 1)

            if pybuf.len > mlen + 1:
                raise RuntimeError(
                    f'received more than one DataRow '
                    f'for a JSON query {sql!r}')

            ncol = hton.unpack_int16(<char*>pybuf.buf + 5)
            if ncol != 1:
                raise RuntimeError(
                    f'received more than column in DataRow '
                    f'for a JSON query {sql!r}')

            coll = hton.unpack_int32(<char*>pybuf.buf + 7)
            if coll == -1:
                raise RuntimeError(
                    f'received NULL for a JSON query {sql!r}')

            return cpython.PyBytes_FromStringAndSize(
                <char*>pybuf.buf + 11, mlen - 4 - 2 - 4)

        finally:
            cpython.PyBuffer_Release(&pybuf)

    async def parse_execute_json(
        self,
        sql,
        sql_hash,
        dbver,
        use_prep_stmt,
        args,
    ):
        self.before_command()
        try:
            return await self._parse_execute_json(
                sql,
                sql_hash,
                dbver,
                use_prep_stmt,
                args,
            )
        finally:
            await self.after_command()

    async def parse_execute_notebook(
        self,
        sql,
        dbver,
    ):
        cdef:
            WriteBuffer out
            Py_buffer pybuf

        self.before_command()
        try:
            out = WriteBuffer.new()
            await self._parse_execute_to_buf(sql, b'', dbver, False, (), out)

            cpython.PyObject_GetBuffer(out, &pybuf, cpython.PyBUF_SIMPLE)
            try:
                return cpython.PyBytes_FromStringAndSize(
                    <char*>pybuf.buf, pybuf.len)
            finally:
                cpython.PyBuffer_Release(&pybuf)

        finally:
            await self.after_command()

    def _build_apply_state_req(self, bytes serstate, WriteBuffer out):
        cdef:
            WriteBuffer buf

        buf = WriteBuffer.new_message(b'B')
        buf.write_bytestring(b'')  # portal name
        buf.write_bytestring(b'_clear_state')  # statement name
        buf.write_int16(0)  # number of format codes
        buf.write_int16(0)  # number of parameters
        buf.write_int16(0)  # number of result columns
        out.write_buffer(buf.end_message())

        buf = WriteBuffer.new_message(b'E')
        buf.write_bytestring(b'')  # portal name
        buf.write_int32(0)  # limit: 0 - return all rows
        out.write_buffer(buf.end_message())

        if serstate is not None:
            buf = WriteBuffer.new_message(b'B')
            buf.write_bytestring(b'')  # portal name
            buf.write_bytestring(b'_apply_state')  # statement name
            buf.write_int16(1)  # number of format codes
            buf.write_int16(1)  # binary
            buf.write_int16(1)  # number of parameters
            buf.write_int32(len(serstate) + 1)
            buf.write_byte(1)  # jsonb format version
            buf.write_bytes(serstate)
            buf.write_int16(0)  # number of result columns
            out.write_buffer(buf.end_message())

            buf = WriteBuffer.new_message(b'E')
            buf.write_bytestring(b'')  # portal name
            buf.write_int32(0)  # limit: 0 - return all rows
            out.write_buffer(buf.end_message())

    async def _parse_apply_state_resp(self, bytes serstate):
        cdef int num_completed = 0
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'2':
                # BindComplete
                self.buffer.discard_message()

            elif mtype == b'E':
                er_cls, er_fields = self.parse_error_message()
                raise er_cls(fields=er_fields)

            elif mtype == b'C':
                self.buffer.discard_message()
                num_completed += 1
                if (
                    (serstate is not None and num_completed == 2) or
                    (serstate is None and num_completed == 1)
                ):
                    return
            else:
                self.fallthrough()

    async def _parse_execute(
        self,
        object query,
        edgecon.EdgeConnection edgecon,
        WriteBuffer bind_data,
        bint use_prep_stmt,
        bytes state,
        dbver
    ):
        cdef:
            WriteBuffer out
            WriteBuffer buf
            bytes stmt_name
            bint store_stmt = 0
            bint parse = 1

            bint has_result = query.cardinality is not CARD_NO_RESULT

            uint64_t msgs_num = <uint64_t>(len(query.sql))
            uint64_t msgs_executed = 0
            uint64_t i

        out = WriteBuffer.new()

        if state is not None:
            self._build_apply_state_req(state, out)

        if use_prep_stmt:
            stmt_name = query.sql_hash
            parse, store_stmt = self.before_prepare(
                stmt_name, dbver, out)
        else:
            stmt_name = b''

        if parse:
            if len(self.last_parse_prep_stmts):
                for stmt_name_to_clean in self.last_parse_prep_stmts:
                    out.write_buffer(
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
                    out.write_buffer(buf.end_message())
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
                out.write_buffer(buf.end_message())

        assert bind_data is not None
        if stmt_name == b'' and msgs_num > 1:
            for s in self.last_parse_prep_stmts:
                buf = WriteBuffer.new_message(b'B')
                buf.write_bytestring(b'')  # portal name
                buf.write_bytestring(s)  # statement name
                buf.write_buffer(bind_data)
                out.write_buffer(buf.end_message())

                buf = WriteBuffer.new_message(b'E')
                buf.write_bytestring(b'')  # portal name
                buf.write_int32(0)  # limit: 0 - return all rows
                out.write_buffer(buf.end_message())
        else:
            buf = WriteBuffer.new_message(b'B')
            buf.write_bytestring(b'')  # portal name
            buf.write_bytestring(stmt_name)  # statement name
            buf.write_buffer(bind_data)
            out.write_buffer(buf.end_message())

            buf = WriteBuffer.new_message(b'E')
            buf.write_bytestring(b'')  # portal name
            buf.write_int32(0)  # limit: 0 - return all rows
            out.write_buffer(buf.end_message())

        out.write_bytes(SYNC_MESSAGE)
        self.waiting_for_sync = True
        self.write(out)

        try:
            if state is not None:
                await self._parse_apply_state_resp(state)

            buf = None
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'D':
                        # DataRow
                        if not has_result:
                            raise errors.InternalServerError(
                                f'query that was inferred to have '
                                f'no data returned received a DATA package; '
                                f'query: {query.sql}')

                        if buf is None:
                            buf = WriteBuffer.new()

                        self.buffer.redirect_messages(buf, b'D', 0)
                        if buf.len() >= DATA_BUFFER_SIZE:
                            edgecon.write(buf)
                            buf = None

                    elif mtype == b'C':  ## result
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
                            self.prep_stmts[stmt_name] = dbver

                    elif mtype == b'E':  ## result
                        # ErrorResponse
                        er_cls, er_fields = self.parse_error_message()
                        raise er_cls(fields=er_fields)

                    elif mtype == b'n':
                        # NoData
                        self.buffer.discard_message()

                    elif mtype == b's':  ## result
                        # PortalSuspended
                        self.buffer.discard_message()
                        return

                    elif mtype == b'2':
                        # BindComplete
                        self.buffer.discard_message()

                    elif mtype == b'I':  ## result
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
            await self.wait_for_sync()

    async def parse_execute(
        self,
        object query,
        edgecon.EdgeConnection edgecon,
        WriteBuffer bind_data,
        bint use_prep_stmt,
        bytes state,
        int dbver,
    ):
        self.before_command()
        try:
            return await self._parse_execute(
                query,
                edgecon,
                bind_data,
                use_prep_stmt,
                state,
                dbver
            )
        finally:
            await self.after_command()

    async def _simple_query(self, bytes sql, bint ignore_data, bytes state):
        cdef:
            WriteBuffer out
            WriteBuffer buf

        out = WriteBuffer.new()

        if state is not None:
            self._build_apply_state_req(state, out)
            # We must use SYNC and not FLUSH here, as otherwise
            # scripts that contain `SET TRANSACTION ISOLATION LEVEL` would
            # complain that transaction has already started (by our state
            # sync query) and the type of the transaction cannot be changed.
            out.write_bytes(SYNC_MESSAGE)

        buf = WriteBuffer.new_message(b'Q')
        buf.write_bytestring(sql)
        out.write_buffer(buf.end_message())

        self.write(out)

        exc = None
        result = None

        if state is not None:
            await self._parse_apply_state_resp(state)
            self.waiting_for_sync = True
            await self.wait_for_sync()

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

        if exc is not None:
            raise exc[0](fields=exc[1])
        return result

    async def simple_query(
        self,
        bytes sql,
        bint ignore_data,
        bytes state=None
    ):
        self.before_command()
        try:
            return await self._simple_query(sql, ignore_data, state)
        finally:
            await self.after_command()

    async def run_ddl(
        self,
        object query_unit,
        bytes state=None
    ):
        self.before_command()
        try:
            sql = b';'.join(query_unit.sql)
            ignore_data = query_unit.ddl_stmt_id is None
            data =  await self._simple_query(
                sql,
                ignore_data,
                state,
            )

            if query_unit.ddl_stmt_id:
                if data:
                    ret = json.loads(data[0][0])
                    if ret['ddl_stmt_id'] != query_unit.ddl_stmt_id:
                        raise RuntimeError(
                            'unrecognized data packet after a DDL command: '
                            'data_stmt_id do not match')
                    return ret
                else:
                    raise RuntimeError(
                        'missing the required data packet after a DDL command')
        finally:
            await self.after_command()

    async def _dump(self, block, output_queue, fragment_suggested_size):
        cdef:
            WriteBuffer buf
            WriteBuffer qbuf
            WriteBuffer out

        qbuf = WriteBuffer.new_message(b'Q')
        qbuf.write_bytestring(block.sql_copy_stmt)
        qbuf.end_message()

        self.write(qbuf)
        self.waiting_for_sync = True

        er = None
        out = None
        i = 0
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'H':
                # CopyOutResponse
                self.buffer.discard_message()

            elif mtype == b'd':
                # CopyData
                if out is None:
                    out = WriteBuffer.new()

                    if i == 0:
                        # The first COPY IN message is prefixed with
                        # `COPY_SIGNATURE` -- strip it.
                        first = self.buffer.consume_message()
                        if first[:len(COPY_SIGNATURE)] != COPY_SIGNATURE:
                            raise RuntimeError('invalid COPY IN message')

                        buf = WriteBuffer.new_message(b'd')
                        buf.write_bytes(first[len(COPY_SIGNATURE) + 8:])
                        buf.end_message()
                        out.write_buffer(buf)

                        if out._length >= fragment_suggested_size:
                            await output_queue.put((block, i, out))
                            i += 1
                            out = None

                        if (not self.buffer.take_message() or
                                self.buffer.get_message_type() != b'd'):
                            continue

                self.buffer.redirect_messages(
                    out, b'd', fragment_suggested_size)

                if out._length >= fragment_suggested_size:
                    await output_queue.put((block, i, out))
                    i += 1
                    out = None

            elif mtype == b'c':
                # CopyDone
                self.buffer.discard_message()

            elif mtype == b'C':
                # CommandComplete
                if out is not None:
                    await output_queue.put((block, i, out))
                self.buffer.discard_message()

            elif mtype == b'E':
                er = self.parse_error_message()

            elif mtype == b'Z':
                self.parse_sync_message()
                break

            else:
                self.fallthrough()

        if er is not None:
            raise er[0](fields=er[1])

    async def dump(self, input_queue, output_queue, fragment_suggested_size):
        self.before_command()
        try:
            while True:
                try:
                    block = input_queue.pop()
                except IndexError:
                    await output_queue.put(None)
                    return

                await self._dump(block, output_queue, fragment_suggested_size)
        finally:
            await self.after_command()

    async def _restore(self, sql, bytes data, tuple elided_cols):
        cdef:
            WriteBuffer buf
            WriteBuffer qbuf
            WriteBuffer out

            char* cbuf
            ssize_t clen
            ssize_t ncols

        qbuf = WriteBuffer.new_message(b'Q')
        qbuf.write_bytestring(sql)
        qbuf.end_message()

        self.write(qbuf)
        self.waiting_for_sync = True

        er = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'G':
                # CopyInResponse
                self.buffer.read_byte()
                ncols = self.buffer.read_int16()
                self.buffer.discard_message()
                break

            elif mtype == b'E':
                er = self.parse_error_message()

            elif mtype == b'Z':
                self.parse_sync_message()
                break

            else:
                self.fallthrough()

        if er is not None:
            raise er[0](fields=er[1])

        buf = WriteBuffer.new()
        cpython.PyBytes_AsStringAndSize(data, &cbuf, &clen)
        if elided_cols:
            self._elide_copy_cols(buf, cbuf, clen, ncols, elided_cols)
        else:
            if cbuf[0] != b'd':
                raise RuntimeError('unexpected dump data message structure')
            ln = <uint32_t>hton.unpack_int32(cbuf + 1)
            buf.write_byte(b'd')
            buf.write_int32(ln + len(COPY_SIGNATURE) + 8)
            buf.write_bytes(COPY_SIGNATURE)
            buf.write_int32(0)
            buf.write_int32(0)
            buf.write_cstr(cbuf + 5, clen - 5)

        self.write(buf)

        qbuf = WriteBuffer.new_message(b'c')
        qbuf.write_bytes(data)
        qbuf.end_message()
        self.write(qbuf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'C':
                # CommandComplete
                self.buffer.discard_message()

            elif mtype == b'E':
                er = self.parse_error_message()

            elif mtype == b'Z':
                self.parse_sync_message()
                break

        if er is not None:
            raise er[0](fields=er[1])

    cdef _elide_copy_cols(
        self,
        WriteBuffer wbuf,
        char* data,
        ssize_t data_len,
        ssize_t ncols,
        tuple elided_cols,
    ):
        """Rewrite the binary COPY stream to exclude the specified columns."""
        cdef:
            FRBuffer rbuf
            ssize_t i
            ssize_t real_ncols
            int8_t *elide
            int8_t elided
            int32_t datum_len
            char copy_msg_byte
            int16_t copy_msg_ncols
            const char *datum
            bint first = True
            bint received_eof = False

        real_ncols = ncols + len(elided_cols)
        frb_init(&rbuf, data, data_len)

        elide = <int8_t*>cpythonx.PyMem_Calloc(
            <size_t>real_ncols, sizeof(int8_t))

        try:
            for col in elided_cols:
                elide[col] = 1

            mbuf = WriteBuffer.new()

            while frb_get_len(&rbuf):
                if received_eof:
                    raise RuntimeError('received CopyData after EOF')
                mbuf.start_message(b'd')

                copy_msg_byte = frb_read(&rbuf, 1)[0]
                if copy_msg_byte != b'd':
                    raise RuntimeError(
                        'unexpected dump data message structure')
                frb_read(&rbuf, 4)

                if first:
                    mbuf.write_bytes(COPY_SIGNATURE)
                    mbuf.write_int32(0)
                    mbuf.write_int32(0)
                    first = False

                copy_msg_ncols = hton.unpack_int16(frb_read(&rbuf, 2))
                if copy_msg_ncols == -1:
                    # BINARY COPY EOF marker
                    mbuf.write_int16(copy_msg_ncols)
                    received_eof = True
                    mbuf.end_message()
                    wbuf.write_buffer(mbuf)
                    mbuf.reset()
                    continue
                else:
                    mbuf.write_int16(<int16_t>ncols)

                # Tuple data
                for i in range(real_ncols):
                    datum_len = hton.unpack_int32(frb_read(&rbuf, 4))
                    elided = elide[i]
                    if not elided:
                        mbuf.write_int32(datum_len)
                    if datum_len != -1:
                        datum = frb_read(&rbuf, datum_len)
                        if not elided:
                            mbuf.write_cstr(datum, datum_len)

                mbuf.end_message()
                wbuf.write_buffer(mbuf)
                mbuf.reset()
        finally:
            cpython.PyMem_Free(elide)

    async def restore(self, sql, bytes data, tuple elided_cols):
        self.before_command()
        try:
            await self._restore(sql, data, elided_cols)
        finally:
            await self.after_command()

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

        buf.write_bytestring(b'search_path')
        buf.write_bytestring(b'edgedb')

        buf.write_bytestring(b'timezone')
        buf.write_bytestring(b'UTC')

        buf.write_bytestring(b'default_transaction_isolation')
        buf.write_bytestring(b'repeatable read')

        buf.write_bytestring(b'intervalstyle')
        buf.write_bytestring(b'sql_standard')

        buf.write_bytestring(b'jit')
        buf.write_bytestring(b'off')

        buf.write_bytestring(b'user')
        buf.write_bytestring(self.pgaddr['user'].encode('utf-8'))

        buf.write_bytestring(b'database')
        buf.write_bytestring(self.dbname.encode('utf-8'))

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
                    if status == PGAUTH_SUCCESSFUL:
                        pass
                    elif status == PGAUTH_REQUIRED_PASSWORDMD5:
                        # Note: MD5 salt is passed as a four-byte sequence
                        md5_salt = self.buffer.read_bytes(4)
                        self.write(
                            self.make_auth_password_md5_message(md5_salt))

                    else:
                        raise RuntimeError(f'unsupported auth method: {status}')

                elif mtype == b'K':
                    # BackendKeyData
                    self.backend_pid = self.buffer.read_int32()
                    self.backend_secret = self.buffer.read_int32()

                elif mtype == b'E':
                    # ErrorResponse
                    er_cls, er_fields = self.parse_error_message()
                    raise er_cls(fields=er_fields)

                elif mtype == b'Z':
                    # ReadyForQuery
                    self.parse_sync_message()
                    self.connected = True
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

    def is_healthy_to_go_back_to_pool(self):
        return (
            self.connected and
            self.idle and
            self.cancel_fut is None and
            not self.waiting_for_sync and
            not self.in_tx()
        )

    cdef before_command(self):
        if not self.connected:
            raise RuntimeError(
                'pgcon: cannot issue new command: not connected')

        if self.waiting_for_sync:
            raise RuntimeError(
                'pgcon: cannot issue new command; waiting for sync')

        if not self.idle:
            raise RuntimeError(
                'pgcon: cannot issue new command; '
                'another command is in progress')

        if self.cancel_fut is not None:
            raise RuntimeError(
                'pgcon: cannot start new command while cancelling the '
                'previous one')

        self.idle = False

    async def after_command(self):
        if self.idle:
            raise RuntimeError('pgcon: idle while running a command')

        if self.cancel_fut is not None:
            await self.cancel_fut
            self.cancel_fut = None
            self.idle = True

            # If we were cancelling a command in Postgres there can be a
            # race between us calling `pg_cancel_backend()` and us receiving
            # the results of the successfully executed command.  If this
            # happens, we might get the *next command* cancelled. To minimize
            # the chance of that we do another SYNC.
            await self.sync()

        else:
            self.idle = True

    cdef write(self, buf):
        self.transport.write(buf)

    cdef fallthrough(self):
        if self.parse_notification():
            return

        cdef:
            char mtype = self.buffer.get_message_type()
        raise RuntimeError(
            f'unexpected message type {chr(mtype)!r}')

    cdef fallthrough_idle(self):
        cdef char mtype

        while self.buffer.take_message():
            if not self.parse_notification():
                mtype = self.buffer.get_message_type()
                raise RuntimeError(
                    f'unexpected message type {chr(mtype)!r} in IDLE state')

    cdef parse_notification(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == b'S':
            # ParameterStatus
            self.buffer.discard_message()
            return True

        elif mtype == b'A':
            # NotificationResponse
            self.buffer.read_int32()  # discard pid
            channel = self.buffer.read_null_str().decode()
            payload = self.buffer.read_null_str().decode()
            self.buffer.finish_message()

            if self.server is None:
                # The server is still initializing.
                return True

            if channel == '__edgedb_sysevent__':
                event_data = json.loads(payload)
                event = event_data.get('event')

                server_id = event_data.get('server_id')

                event_payload = event_data.get('args')
                if (
                    event in {
                        'schema-changes',
                        'global-schema-changes',
                    }
                    and server_id == self.server._server_id
                ):
                    return True

                if event == 'schema-changes':
                    dbname = event_payload['dbname']
                    self.server._on_remote_ddl(dbname)
                elif event == 'database-config-changes':
                    dbname = event_payload['dbname']
                    self.server._on_remote_database_config_change(dbname)
                elif event == 'system-config-changes':
                    self.server._on_remote_system_config_change()
                elif event == 'global-schema-changes':
                    self.server._on_global_schema_change()
                else:
                    raise AssertionError(f'unexpected system event: {event!r}')

            return True

        elif mtype == b'N':
            # NoticeResponse
            self.buffer.discard_message()
            return True

        return False

    cdef parse_error_message(self):
        cdef:
            char code
            str message
            dict fields = {}
            object cls = pgerror.BackendError

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break

            message = self.buffer.read_null_str().decode()

            if code == 67 and message == '57014':
                # 67 is b'C' -- error code
                # 57014 is Postgres' QueryCanceledError
                cls = pgerror.BackendQueryCancelledError

            fields[chr(code)] = message

        if self.debug:
            self.debug_print('ERROR', cls.__name__, fields)

        self.buffer.finish_message()
        return cls, fields

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

        if self.debug:
            self.debug_print('SYNC MSG', self.xact_status)

        self.buffer.finish_message()

    cdef make_clean_stmt_message(self, bytes stmt_name):
        cdef WriteBuffer buf
        buf = WriteBuffer.new_message(b'C')
        buf.write_byte(b'S')
        buf.write_bytestring(stmt_name)
        return buf.end_message()

    cdef make_auth_password_md5_message(self, bytes salt):
        cdef WriteBuffer msg

        msg = WriteBuffer.new_message(b'p')

        user = self.pgaddr.get('user') or ''
        password = self.pgaddr.get('password') or ''

        # 'md5' + md5(md5(password + username) + salt))
        userpass = (password + user).encode('ascii')
        hash = hashlib.md5(hashlib.md5(userpass).hexdigest().\
                encode('ascii') + salt).hexdigest().encode('ascii')

        msg.write_bytestring(b'md5' + hash)
        return msg.end_message()

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
        # Mark the connection as disconnected, so that
        # `self.is_healthy_to_go_back_to_pool()` surely returns False
        # for this connection.
        self.connected = False

        self.transport = None

        if self.server is not None:
            # only system db PGConnection has self.server
            self.server._on_sys_pgcon_connection_lost()

        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())
            return

        if self.msg_waiter is not None and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if self.connected and self.idle:
            assert self.msg_waiter is None
            self.fallthrough_idle()

        elif (self.msg_waiter is not None and
                self.buffer.take_message() and
                not self.msg_waiter.cancelled()):
            self.msg_waiter.set_result(True)
            self.msg_waiter = None

    def eof_received(self):
        pass


cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
cdef bytes FLUSH_MESSAGE = bytes(WriteBuffer.new_message(b'H').end_message())

cdef EdegDBCodecContext DEFAULT_CODEC_CONTEXT = EdegDBCodecContext()
