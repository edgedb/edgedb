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
import contextlib
import decimal
import codecs
import hashlib
import json
import logging
import os.path
import socket
import ssl as ssl_mod
import struct
import textwrap
import time

cimport cython
cimport cpython

from . cimport cpythonx

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from edb import buildmeta
from edb import errors
from edb.edgeql import qltypes

from edb.schema import objects as s_obj

from edb.pgsql import common as pgcommon
from edb.pgsql.common import quote_ident as pg_qi
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
    frb_slice_from,
)

from edb.server import compiler
from edb.server import defines
from edb.server.cache cimport stmt_cache
from edb.server.dbview cimport dbview
from edb.server import pgconnparams
from edb.server import metrics
from edb.server.protocol cimport binary as edgecon

from edb.common import debug

from . import errors as pgerror

include "scram.pyx"

DEF DATA_BUFFER_SIZE = 100_000
DEF PREP_STMTS_CACHE = 100
DEF TCP_KEEPIDLE = 24
DEF TCP_KEEPINTVL = 2
DEF TCP_KEEPCNT = 3

DEF COPY_SIGNATURE = b"PGCOPY\n\377\r\n\0"


cdef object CARD_NO_RESULT = compiler.Cardinality.NO_RESULT
cdef object FMT_NONE = compiler.OutputFormat.NONE
cdef dict POSTGRES_SHUTDOWN_ERR_CODES = {
    '57P01': 'admin_shutdown',
    '57P02': 'crash_shutdown',
}

cdef bytes INIT_CON_SCRIPT = None

cdef object logger = logging.getLogger('edb.server')


def _build_init_con_script(*, check_pg_is_in_recovery: bool) -> bytes:
    if check_pg_is_in_recovery:
        pg_is_in_recovery = ('''
        SELECT CASE WHEN pg_is_in_recovery() THEN
            edgedb.raise(
                NULL::bigint,
                'read_only_sql_transaction',
                msg => 'cannot use a hot standby'
            )
        END;
        ''').strip()
    else:
        pg_is_in_recovery = ''

    # The '_edgecon_state table' is used to store information about
    # the current session. The `type` column is one character, with one
    # of the following values:
    #
    # * 'C': a session-level config setting
    #
    # * 'B': a session-level config setting that's implemented by setting
    #   a corresponding Postgres config setting.
    #
    # * 'R': a "variable=value" record.

    return textwrap.dedent(f'''
        {pg_is_in_recovery}

        CREATE TEMPORARY TABLE _edgecon_state (
            name text NOT NULL,
            value jsonb NOT NULL,
            type text NOT NULL CHECK(
                type = 'C' OR type = 'R' OR type = 'B'),
            UNIQUE(name, type)
        );

        PREPARE _clear_state AS
            DELETE FROM _edgecon_state
            WHERE _edgecon_state.type != 'R';

        PREPARE _apply_state(jsonb) AS
            INSERT INTO
                _edgecon_state(name, value, type)
            SELECT
                (CASE
                    WHEN e->'type' = '"B"'::jsonb
                    THEN edgedb._apply_session_config(e->>'name', e->'value')
                    ELSE e->>'name'
                END) AS name,
                e->'value' AS value,
                e->>'type' AS type
            FROM
                jsonb_array_elements($1::jsonb) AS e;

        PREPARE _reset_session_config AS
            SELECT edgedb._reset_session_config();

        INSERT INTO _edgecon_state
            (name, value, type)
        VALUES
            ('server_version', {pg_ql(buildmeta.get_version_json())}, 'R');
    ''').strip().encode('utf-8')


def _set_tcp_keepalive(transport):
    # TCP keepalive was initially added here for special cases where idle
    # connections are dropped silently on GitHub Action running test suite
    # against AWS RDS. We are keeping the TCP keepalive for generic
    # Postgres connections as the kernel overhead is considered low, and
    # in certain cases it does save us some reconnection time.
    #
    # In case of high-availability Postgres, TCP keepalive is necessary to
    # disconnect from a failing master node, if no other failover information
    # is available.
    sock = transport.get_extra_info('socket')
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    # TCP_KEEPIDLE: the time (in seconds) the connection needs to remain idle
    # before TCP starts sending keepalive probes. This is socket.TCP_KEEPIDLE
    # on Linux, and socket.TCP_KEEPALIVE on macOS from Python 3.10.
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP,
                        socket.TCP_KEEPIDLE, TCP_KEEPIDLE)
    if hasattr(socket, 'TCP_KEEPALIVE'):
        sock.setsockopt(socket.IPPROTO_TCP,
                        socket.TCP_KEEPALIVE, TCP_KEEPIDLE)

    # TCP_KEEPINTVL: The time (in seconds) between individual keepalive probes.
    if hasattr(socket, 'TCP_KEEPINTVL'):
        sock.setsockopt(socket.IPPROTO_TCP,
                        socket.TCP_KEEPINTVL, TCP_KEEPINTVL)

    # TCP_KEEPCNT: The maximum number of keepalive probes TCP should send
    # before dropping the connection.
    if hasattr(socket, 'TCP_KEEPCNT'):
        sock.setsockopt(socket.IPPROTO_TCP,
                        socket.TCP_KEEPCNT, TCP_KEEPCNT)


async def _create_ssl_connection(protocol_factory, host, port, *,
                                 loop, ssl_context, ssl_is_advisory):
    tr, pr = await loop.create_connection(
        lambda: TLSUpgradeProto(loop, host, port,
                                ssl_context, ssl_is_advisory),
        host, port)
    _set_tcp_keepalive(tr)

    tr.write(struct.pack('!ll', 8, 80877103))  # SSLRequest message.

    try:
        do_ssl_upgrade = await pr.on_data
    except (Exception, asyncio.CancelledError):
        tr.close()
        raise

    if do_ssl_upgrade:
        try:
            new_tr = await loop.start_tls(
                tr, pr, ssl_context, server_hostname=host)
        except (Exception, asyncio.CancelledError):
            tr.close()
            raise
    else:
        new_tr = tr

    pg_proto = protocol_factory()
    pg_proto.is_ssl = do_ssl_upgrade
    pg_proto.connection_made(new_tr)
    new_tr.set_protocol(pg_proto)

    return new_tr, pg_proto


class _RetryConnectSignal(Exception):
    pass


async def _connect(connargs, dbname, ssl):

    loop = asyncio.get_running_loop()

    host = connargs.get("host")
    port = connargs.get("port")
    sslmode = connargs.get('sslmode', pgconnparams.SSLMode.prefer)

    if host.startswith('/'):
        addr = os.path.join(host, f'.s.PGSQL.{port}')
        _, pgcon = await loop.create_unix_connection(
            lambda: PGConnection(dbname, loop, connargs), addr)

    else:
        if ssl:
            _, pgcon = await _create_ssl_connection(
                lambda: PGConnection(dbname, loop, connargs),
                host,
                port,
                loop=loop,
                ssl_context=ssl,
                ssl_is_advisory=(sslmode == pgconnparams.SSLMode.prefer),
            )
        else:
            trans, pgcon = await loop.create_connection(
                lambda: PGConnection(dbname, loop, connargs),
                host=host, port=port)
            _set_tcp_keepalive(trans)

    try:
        await pgcon.connect()
    except pgerror.BackendError as e:
        pgcon.terminate()
        if not e.code_is(pgerror.ERROR_INVALID_AUTHORIZATION_SPECIFICATION):
            raise

        if (
            sslmode == pgconnparams.SSLMode.allow and not pgcon.is_ssl or
            sslmode == pgconnparams.SSLMode.prefer and pgcon.is_ssl
        ):
            # Trigger retry when:
            #   1. First attempt with sslmode=allow, ssl=None failed
            #   2. First attempt with sslmode=prefer, ssl=ctx failed while the
            #      server claimed to support SSL (returning "S" for SSLRequest)
            #      (likely because pg_hba.conf rejected the connection)
            raise _RetryConnectSignal()

        else:
            # but will NOT retry if:
            #   1. First attempt with sslmode=prefer failed but the server
            #      doesn't support SSL (returning 'N' for SSLRequest), because
            #      we already tried to connect without SSL thru ssl_is_advisory
            #   2. Second attempt with sslmode=prefer, ssl=None failed
            #   3. Second attempt with sslmode=allow, ssl=ctx failed
            #   4. Any other sslmode
            raise

    return pgcon


async def connect(connargs, dbname, backend_params):
    global INIT_CON_SCRIPT

    # This is different than parsing DSN and use the default sslmode=prefer,
    # because connargs can be set manually thru set_connection_params(), and
    # the caller should be responsible for aligning sslmode with ssl.
    sslmode = connargs.get('sslmode', pgconnparams.SSLMode.disable)
    ssl = connargs.get('ssl')
    if sslmode == pgconnparams.SSLMode.allow:
        try:
            pgcon = await _connect(connargs, dbname, ssl=None)
        except _RetryConnectSignal:
            pgcon = await _connect(connargs, dbname, ssl=ssl)
    elif sslmode == pgconnparams.SSLMode.prefer:
        try:
            pgcon = await _connect(connargs, dbname, ssl=ssl)
        except _RetryConnectSignal:
            pgcon = await _connect(connargs, dbname, ssl=None)
    else:
        pgcon = await _connect(connargs, dbname, ssl=ssl)

    if backend_params.has_create_role:
        sup_role = pgcommon.get_role_backend_name(
            defines.EDGEDB_SUPERUSER, tenant_id=backend_params.tenant_id)
        if connargs['user'] != sup_role:
            # We used to use SET SESSION AUTHORIZATION here, there're some
            # security differences over SET ROLE, but as we don't allow
            # accessing Postgres directly through EdgeDB, SET ROLE is mostly
            # fine here. (Also hosted backends like Postgres on DigitalOcean
            # support only SET ROLE)
            await pgcon.simple_query(
                f'SET ROLE {pg_qi(sup_role)}'.encode(),
                ignore_data=True,
            )

    if 'in_hot_standby' in pgcon.parameter_status:
        # in_hot_standby is always present in Postgres 14 and above
        if pgcon.parameter_status['in_hot_standby'] == 'on':
            # Abort if we're connecting to a hot standby
            pgcon.terminate()
            raise pgerror.BackendError(fields=dict(
                M="cannot use a hot standby",
                C=pgerror.ERROR_READ_ONLY_SQL_TRANSACTION,
            ))
        if INIT_CON_SCRIPT is None:
            INIT_CON_SCRIPT = _build_init_con_script(
                check_pg_is_in_recovery=False
            )
    else:
        # On lower versions of Postgres we use pg_is_in_recovery() to check if
        # it is a hot standby, and error out if it is.
        if INIT_CON_SCRIPT is None:
            INIT_CON_SCRIPT = _build_init_con_script(
                check_pg_is_in_recovery=True
            )

    await pgcon.simple_query(INIT_CON_SCRIPT, ignore_data=True)

    return pgcon


class TLSUpgradeProto(asyncio.Protocol):
    def __init__(self, loop, host, port, ssl_context, ssl_is_advisory):
        self.on_data = loop.create_future()
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.ssl_is_advisory = ssl_is_advisory

    def data_received(self, data):
        if data == b'S':
            self.on_data.set_result(True)
        elif (self.ssl_is_advisory and
              self.ssl_context.verify_mode == ssl_mod.CERT_NONE and
              data == b'N'):
            # ssl_is_advisory will imply that ssl.verify_mode == CERT_NONE,
            # since the only way to get ssl_is_advisory is from
            # sslmode=prefer. But be extra sure to disallow insecure
            # connections when the ssl context asks for real security.
            self.on_data.set_result(False)
        else:
            self.on_data.set_exception(
                ConnectionError(
                    'PostgreSQL server at "{host}:{port}" '
                    'rejected SSL upgrade'.format(
                        host=self.host, port=self.port)))

    def connection_lost(self, exc):
        if not self.on_data.done():
            if exc is None:
                exc = ConnectionError('unexpected connection_lost() call')
            self.on_data.set_exception(exc)


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

        self.waiting_for_sync = 0
        self.xact_status = PQTRANS_UNKNOWN

        self.backend_pid = -1
        self.backend_secret = -1
        self.parameter_status = dict()

        self.last_parse_prep_stmts = []
        self.debug = debug.flags.server_proto

        self.pgaddr = addr
        self.server = None
        self.is_system_db = False
        self.close_requested = False

        self.pinned_by = None

        self.idle = True
        self.cancel_fut = None

        self._is_ssl = False

        # Set to the error the connection has been aborted with
        # by the backend.
        self.aborted_with_error = None

        self.last_state = dbview.DEFAULT_STATE

    @property
    def is_ssl(self):
        return self._is_ssl

    @is_ssl.setter
    def is_ssl(self, value):
        self._is_ssl = value

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
        self.close_requested = True
        self.transport.abort()
        self.transport = None
        self.connected = False

    def terminate(self):
        if not self.transport:
            return
        self.close_requested = True
        self.write(WriteBuffer.new_message(b'X').end_message())
        self.transport.close()
        self.transport = None
        self.connected = False

        if self.msg_waiter and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

    def set_server(self, server):
        self.server = server

    def mark_as_system_db(self):
        if self.server.get_backend_runtime_params().has_create_database:
            assert defines.EDGEDB_SYSTEM_DB in self.dbname
        self.is_system_db = True

    async def listen_for_sysevent(self):
        try:
            if self.server.get_backend_runtime_params().has_create_database:
                assert defines.EDGEDB_SYSTEM_DB in self.dbname
            await self.simple_query(
                b'LISTEN __edgedb_sysevent__;',
                ignore_data=True
            )
        except Exception:
            try:
                self.abort()
            finally:
                raise

    async def signal_sysevent(self, event, **kwargs):
        if self.server.get_backend_runtime_params().has_create_database:
            assert defines.EDGEDB_SYSTEM_DB in self.dbname
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
            self.waiting_for_sync += 1
            self.write(_SYNC_MESSAGE)

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
        error = None
        try:
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()
                if mtype == b'Z':
                    self.parse_sync_message()
                    break
                elif mtype == b'E':
                    # ErrorResponse
                    er_cls, fields = self.parse_error_message()
                    error = er_cls(fields=fields)
                else:
                    if not self.parse_notification():
                        if PG_DEBUG or self.debug:
                            self.debug_print(f'PGCon.wait_for_sync: discarding '
                                            f'{chr(mtype)!r} message')
                        self.buffer.discard_message()
        finally:
            if error is not None:
                # Postgres might send an ErrorResponse if, e.g.
                # in implicit transaction fails to commit due to
                # serialization conflicts.
                raise error

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

    cdef write_sync(self, WriteBuffer outbuf):
        outbuf.write_bytes(_SYNC_MESSAGE)
        self.waiting_for_sync += 1

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

        self.write_sync(buf)

        self.write(buf)
        error = None
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
        started_at = time.monotonic()
        try:
            return await self._parse_execute_json(
                sql,
                sql_hash,
                dbver,
                use_prep_stmt,
                args,
            )
        finally:
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
            await self.after_command()

    async def _parse_execute_extract_single_data_frame(
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
                    f'invalid protocol-level result of a query '
                    f'sql:{sql} buf-len:{pybuf.len} buf:{data}')

            mlen = hton.unpack_int32(<char*>pybuf.buf + 1)

            if pybuf.len > mlen + 1:
                raise RuntimeError(
                    f'received more than one DataRow '
                    f'for a singleton-returning query {sql!r}')

            ncol = hton.unpack_int16(<char*>pybuf.buf + 5)
            if ncol != 1:
                raise RuntimeError(
                    f'received more than column in DataRow '
                    f'for a singleton-returning query {sql!r}')

            return cpython.PyBytes_FromStringAndSize(
                <char*>pybuf.buf + 7, mlen - 2 - 4)

        finally:
            cpython.PyBuffer_Release(&pybuf)

    async def parse_execute_extract_single_data_frame(
        self,
        sql,
        sql_hash,
        dbver,
        use_prep_stmt,
        args,
    ):
        self.before_command()
        started_at = time.monotonic()
        try:
            return await self._parse_execute_extract_single_data_frame(
                sql, sql_hash, dbver, use_prep_stmt, args)
        finally:
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
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
        started_at = time.monotonic()
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
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
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

        buf = WriteBuffer.new_message(b'B')
        buf.write_bytestring(b'')  # portal name
        buf.write_bytestring(b'_reset_session_config')  # statement name
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
        cdef:
            int num_completed = 0
            int expected_completed = 2

        if serstate is not None:
            expected_completed += 1

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'2' or mtype == b'D':
                # BindComplete or Data
                self.buffer.discard_message()

            elif mtype == b'E':
                er_cls, er_fields = self.parse_error_message()
                raise er_cls(fields=er_fields)

            elif mtype == b'C':
                self.buffer.discard_message()
                num_completed += 1
                if num_completed == expected_completed:
                    return
            else:
                self.fallthrough()

    @contextlib.asynccontextmanager
    async def parse_execute_script_context(self):
        self.before_command()
        started_at = time.monotonic()
        try:
            try:
                yield
            finally:
                while self.waiting_for_sync:
                    await self.wait_for_sync()
        finally:
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
            await self.after_command()

    cdef send_query_unit_group(
        self, object query_unit_group, object bind_datas, bytes state,
        ssize_t start, ssize_t end,
    ):
        cdef:
            WriteBuffer out
            WriteBuffer buf
            WriteBuffer bind_data

        out = WriteBuffer.new()

        if state is not None and start == 0:
            self._build_apply_state_req(state, out)

        for query_unit, bind_data in zip(
                query_unit_group.units[start:end], bind_datas):
            if query_unit.system_config:
                raise RuntimeError(
                    "CONFIGURE INSTANCE command is not allowed in scripts"
                )
            for sql in query_unit.sql:
                buf = WriteBuffer.new_message(b'P')
                buf.write_bytestring(b'')  # statement name
                buf.write_bytestring(sql)
                buf.write_int16(0)
                out.write_buffer(buf.end_message())

                buf = WriteBuffer.new_message(b'B')
                buf.write_bytestring(b'')  # portal name
                buf.write_bytestring(b'')  # statement name
                buf.write_buffer(bind_data)
                out.write_buffer(buf.end_message())

                buf = WriteBuffer.new_message(b'E')
                buf.write_bytestring(b'')  # portal name
                buf.write_int32(0)  # limit: 0 - return all rows
                out.write_buffer(buf.end_message())

        if end == len(query_unit_group.units):
            self.write_sync(out)
        else:
            out.write_bytes(FLUSH_MESSAGE)

        self.write(out)

    async def wait_for_state_resp(self, bytes state, bint state_sync):
        if state_sync:
            try:
                await self._parse_apply_state_resp(state)
            finally:
                await self.wait_for_sync()
        else:
            await self._parse_apply_state_resp(state)

    async def wait_for_command(
        self, *, bint ignore_data, edgecon.EdgeConnection edgecon=None
    ):
        cdef WriteBuffer buf = None

        result = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'D':
                    # DataRow
                    if ignore_data:
                        self.buffer.discard_message()
                    elif edgecon is None:
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
                    else:
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
                    return result

                elif mtype == b'1':
                    # ParseComplete
                    self.buffer.discard_message()

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
                    return result

                elif mtype == b'2':
                    # BindComplete
                    self.buffer.discard_message()

                elif mtype == b'I':  ## result
                    # EmptyQueryResponse
                    self.buffer.discard_message()
                    return result

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

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
            bint state_sync = 0

            bint has_result = query.cardinality is not CARD_NO_RESULT
            bint discard_result = query.output_format == FMT_NONE

            uint64_t msgs_num = <uint64_t>(len(query.sql))
            uint64_t msgs_executed = 0
            uint64_t i

        out = WriteBuffer.new()

        if state is not None:
            self._build_apply_state_req(state, out)
            if query.tx_id:
                # This query has START TRANSACTION in it.
                # Restoring state must be performed in a separate
                # implicit transaction (otherwise START TRANSACTION DEFERRABLE)
                # would fail. Hence - inject a SYNC after a state restore step.
                state_sync = 1
                self.write_sync(out)

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

        self.write_sync(out)
        self.write(out)

        # If no edgecon is passed, the caller is responsible for
        # handling the server response
        if edgecon is None:
            return

        try:
            if state is not None:
                await self.wait_for_state_resp(state, state_sync)

            buf = None
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'D':
                        # DataRow
                        if discard_result:
                            self.buffer.discard_message()
                            continue
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
        started_at = time.monotonic()
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
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
            if edgecon:
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
            self.write_sync(out)

        buf = WriteBuffer.new_message(b'Q')
        buf.write_bytestring(sql)
        out.write_buffer(buf.end_message())
        self.waiting_for_sync += 1

        self.write(out)

        exc = None
        result = None

        if state is not None:
            await self._parse_apply_state_resp(state)
            await self.wait_for_sync()

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
        started_at = time.monotonic()
        try:
            return await self._simple_query(sql, ignore_data, state)
        finally:
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
            await self.after_command()

    async def run_ddl(
        self,
        object query_unit,
        bytes state=None
    ):
        self.before_command()
        started_at = time.monotonic()
        try:
            sql = b';'.join(query_unit.sql)
            ignore_data = query_unit.ddl_stmt_id is None
            data =  await self._simple_query(
                sql,
                ignore_data,
                state,
            )
            return self.load_ddl_return(query_unit, data)
        finally:
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
            await self.after_command()

    def load_ddl_return(self, object query_unit, data):
        if query_unit.ddl_stmt_id:
            if data:
                ret = json.loads(data[0][0])
                if ret['ddl_stmt_id'] != query_unit.ddl_stmt_id:
                    raise RuntimeError(
                        'unrecognized data packet after a DDL command: '
                        'data_stmt_id do not match'
                    )
                return ret
            else:
                raise RuntimeError(
                    'missing the required data packet after a DDL command'
                )

    async def handle_ddl_in_script(self, object query_unit):
        data = None
        for sql in query_unit.sql:
            data = await self.wait_for_command(ignore_data=bool(data)) or data
        return self.load_ddl_return(query_unit, data)

    async def _dump(self, block, output_queue, fragment_suggested_size):
        cdef:
            WriteBuffer buf
            WriteBuffer qbuf
            WriteBuffer out

        qbuf = WriteBuffer.new_message(b'Q')
        qbuf.write_bytestring(block.sql_copy_stmt)
        qbuf.end_message()

        self.write(qbuf)
        self.waiting_for_sync += 1

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

    async def _restore(self, restore_block, bytes data, dict type_map):
        cdef:
            WriteBuffer buf
            WriteBuffer qbuf
            WriteBuffer out

            char* cbuf
            ssize_t clen
            ssize_t ncols

        qbuf = WriteBuffer.new_message(b'Q')
        qbuf.write_bytestring(restore_block.sql_copy_stmt)
        qbuf.end_message()

        self.write(qbuf)
        self.waiting_for_sync += 1

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
        if (
            restore_block.compat_elided_cols
            or any(desc for desc in restore_block.data_mending_desc)
        ):
            self._rewrite_copy_data(
                buf,
                cbuf,
                clen,
                ncols,
                restore_block.data_mending_desc,
                type_map,
                restore_block.compat_elided_cols,
            )
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

    cdef _rewrite_copy_data(
        self,
        WriteBuffer wbuf,
        char* data,
        ssize_t data_len,
        ssize_t ncols,
        tuple data_mending_desc,
        dict type_id_map,
        tuple elided_cols,
    ):
        """Rewrite the binary COPY stream."""
        cdef:
            FRBuffer rbuf
            FRBuffer datum_buf
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
                            datum_mending_desc = data_mending_desc[i]
                            if (
                                datum_mending_desc is not None
                                and datum_mending_desc.needs_mending
                            ):
                                frb_init(&datum_buf, datum, datum_len)
                                self._mend_copy_datum(
                                    mbuf,
                                    &datum_buf,
                                    datum_mending_desc,
                                    type_id_map,
                                )
                            else:
                                mbuf.write_cstr(datum, datum_len)

                mbuf.end_message()
                wbuf.write_buffer(mbuf)
                mbuf.reset()
        finally:
            cpython.PyMem_Free(elide)

    cdef _mend_copy_datum(
        self,
        WriteBuffer wbuf,
        FRBuffer *rbuf,
        object mending_desc,
        dict type_id_map,
    ):
        cdef:
            ssize_t remainder
            int32_t ndims
            int32_t i
            int32_t nelems
            int32_t dim
            const char *buf
            FRBuffer elem_buf
            int32_t elem_len
            object elem_mending_desc

        kind = mending_desc.schema_object_class

        if kind is qltypes.SchemaObjectClass.ARRAY_TYPE:
            # Dimensions and flags
            buf = frb_read(rbuf, 8)
            ndims = hton.unpack_int32(buf)
            wbuf.write_cstr(buf, 8)
            elem_mending_desc = mending_desc.elements[0]
            # Discard the original element OID.
            frb_read(rbuf, 4)
            # Write the correct element OID.
            elem_type_id = elem_mending_desc.schema_type_id
            elem_type_oid = type_id_map[elem_type_id]
            wbuf.write_int32(<int32_t>elem_type_oid)

            if ndims == 0:
                # Empty array
                return

            if ndims != 1:
                raise ValueError(
                    'unexpected non-single dimension array'
                )

            if mending_desc.needs_mending:
                # dim and lbound
                buf = frb_read(rbuf, 8)
                nelems = hton.unpack_int32(buf)
                wbuf.write_cstr(buf, 8)

                for i in range(nelems):
                    elem_len = hton.unpack_int32(frb_read(rbuf, 4))
                    wbuf.write_int32(elem_len)
                    frb_slice_from(&elem_buf, rbuf, elem_len)
                    self._mend_copy_datum(
                        wbuf,
                        &elem_buf,
                        mending_desc.elements[0],
                        type_id_map,
                    )

        elif kind is qltypes.SchemaObjectClass.TUPLE_TYPE:
            nelems = hton.unpack_int32(frb_read(rbuf, 4))
            wbuf.write_int32(nelems)

            for i in range(nelems):
                elem_mending_desc = mending_desc.elements[i]
                if elem_mending_desc is not None:
                    # Discard the original element OID.
                    frb_read(rbuf, 4)
                    # Write the correct element OID.
                    elem_type_id = elem_mending_desc.schema_type_id
                    elem_type_oid = type_id_map[elem_type_id]
                    wbuf.write_int32(<int32_t>elem_type_oid)

                    elem_len = hton.unpack_int32(frb_read(rbuf, 4))
                    wbuf.write_int32(elem_len)

                    if elem_len != -1:
                        frb_slice_from(&elem_buf, rbuf, elem_len)

                        if elem_mending_desc.needs_mending:
                            self._mend_copy_datum(
                                wbuf,
                                &elem_buf,
                                elem_mending_desc,
                                type_id_map,
                            )
                        else:
                            wbuf.write_frbuf(&elem_buf)
                else:
                    buf = frb_read(rbuf, 8)
                    wbuf.write_cstr(buf, 8)
                    elem_len = hton.unpack_int32(buf + 4)
                    if elem_len != -1:
                        wbuf.write_cstr(frb_read(rbuf, elem_len), elem_len)

        wbuf.write_frbuf(rbuf)

    async def restore(self, restore_block, bytes data, dict type_map):
        self.before_command()
        try:
            await self._restore(restore_block, data, type_map)
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
        buf.write_bytestring(b'serializable')

        buf.write_bytestring(b'intervalstyle')
        buf.write_bytestring(b'iso_8601')

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
        self.waiting_for_sync += 1

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

                    elif status == PGAUTH_REQUIRED_SASL:
                        await self._auth_sasl()

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

                elif mtype == b'S':
                    # ParameterStatus
                    name, value = self.parse_parameter_status_message()
                    self.parameter_status[name] = value

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

    def is_healthy(self):
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
        self.transport.write(memoryview(buf))

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
            if self.parse_notification():
                continue

            mtype = self.buffer.get_message_type()
            if mtype != b'E':  # ErrorResponse
                raise RuntimeError(
                    f'unexpected message type {chr(mtype)!r} '
                    f'in IDLE state')

            # We have an error message sent to us by the backend.
            # It is not safe to assume that the connection
            # is alive. We assume that it's dead and should be
            # marked as "closed".

            try:
                er_cls, fields = self.parse_error_message()
                self.aborted_with_error = er_cls(fields=fields)

                pgcode = fields['C']
                metrics.backend_connection_aborted.inc(1.0, pgcode)

                if pgcode in POSTGRES_SHUTDOWN_ERR_CODES:
                    pgreason = POSTGRES_SHUTDOWN_ERR_CODES[pgcode]
                    pgmsg = fields.get('M', pgreason)

                    logger.debug(
                        'backend connection aborted with a shutdown '
                        'error code %r(%s): %s',
                        pgcode, pgreason, pgmsg
                    )

                    if self.is_system_db:
                        self.server.set_pg_unavailable_msg(pgmsg)
                        self.server._on_sys_pgcon_failover_signal()

                else:
                    pgmsg = fields.get('M', '<empty message>')
                    logger.debug(
                        'backend connection aborted with an '
                        'error code %r: %s',
                        pgcode, pgmsg
                    )
            finally:
                self.abort()

    cdef parse_notification(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == b'S':
            # ParameterStatus
            name, value = self.parse_parameter_status_message()
            if self.is_system_db:
                self.server._on_sys_pgcon_parameter_status_updated(name, value)
            self.parameter_status[name] = value
            return True

        elif mtype == b'A':
            # NotificationResponse
            self.buffer.read_int32()  # discard pid
            channel = self.buffer.read_null_str().decode()
            payload = self.buffer.read_null_str().decode()
            self.buffer.finish_message()

            if not self.is_system_db:
                # The server is still initializing, or we're getting
                # notification from a non-system-db connection.
                return True

            if channel == '__edgedb_sysevent__':
                event_data = json.loads(payload)
                event = event_data.get('event')

                server_id = event_data.get('server_id')
                if server_id == self.server._server_id:
                    # We should only react to notifications sent
                    # by other edgedb servers. Reacting to events
                    # generated by this server must be implemented
                    # at a different layer.
                    return True

                event_payload = event_data.get('args')
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

            if (code == 67 and  # 67 is b'C' -- error code
                message == pgerror.ERROR_QUERY_CANCELLED):
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
        self.waiting_for_sync -= 1

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

    cdef parse_parameter_status_message(self):
        cdef:
            str name
            str value
        assert self.buffer.get_message_type() == b'S'
        name = self.buffer.read_null_str().decode()
        value = self.buffer.read_null_str().decode()
        self.buffer.finish_message()
        if self.debug:
            self.debug_print('PARAMETER STATUS MSG', name, value)
        return name, value

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

    async def _auth_sasl(self):
        methods = []
        auth_method = self.buffer.read_null_str()
        while auth_method:
            methods.append(auth_method)
            auth_method = self.buffer.read_null_str()
        self.buffer.finish_message()

        if not methods:
            raise RuntimeError(
                'the backend requested SASL authentication but did not '
                'offer any methods')

        for method in methods:
            if method in SCRAMAuthentication.AUTHENTICATION_METHODS:
                break
        else:
            raise RuntimeError(
                f'the backend offered the following SASL authentication '
                f'methods: {b", ".join(methods).decode()}, neither are '
                f'supported.'
            )

        user = self.pgaddr.get('user') or ''
        password = self.pgaddr.get('password') or ''
        scram = SCRAMAuthentication(method)

        msg = WriteBuffer.new_message(b'p')
        msg.write_bytes(scram.create_client_first_message(user))
        msg.end_message()
        self.write(msg)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'E':
                # ErrorResponse
                er_cls, er_fields = self.parse_error_message()
                raise er_cls(fields=er_fields)

            elif mtype == b'R':
                # Authentication...
                break

            else:
                self.fallthrough()

        status = self.buffer.read_int32()
        if status != PGAUTH_SASL_CONTINUE:
            raise RuntimeError(
                f'expected SASLContinue from the server, received {status}')

        server_response = self.buffer.consume_message()
        scram.parse_server_first_message(server_response)
        msg = WriteBuffer.new_message(b'p')
        client_final_message = scram.create_client_final_message(password)
        msg.write_bytes(client_final_message)
        msg.end_message()

        self.write(msg)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'E':
                # ErrorResponse
                er_cls, er_fields = self.parse_error_message()
                raise er_cls(fields=er_fields)

            elif mtype == b'R':
                # Authentication...
                break

            else:
                self.fallthrough()

        status = self.buffer.read_int32()
        if status != PGAUTH_SASL_FINAL:
            raise RuntimeError(
                f'expected SASLFinal from the server, received {status}')

        server_response = self.buffer.consume_message()
        if not scram.verify_server_final_message(server_response):
            raise pgerror.BackendError(fields=dict(
                M="server SCRAM proof does not match",
                C=pgerror.ERROR_INVALID_PASSWORD,
            ))

    async def wait_for_message(self):
        if self.buffer.take_message():
            return
        if self.transport is None:
            raise ConnectionAbortedError()
        self.msg_waiter = self.loop.create_future()
        await self.msg_waiter

    def connection_made(self, transport):
        if self.transport is not None:
            raise RuntimeError('connection_made: invalid connection status')
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def connection_lost(self, exc):
        # Mark the connection as disconnected, so that `self.is_healthy()`
        # surely returns False for this connection.
        self.connected = False

        self.transport = None

        if self.pinned_by is not None:
            pinned_by = self.pinned_by
            self.pinned_by = None
            pinned_by.on_aborted_pgcon(self)

        if self.is_system_db:
            self.server._on_sys_pgcon_connection_lost(exc)
        elif self.server is not None:
            if not self.close_requested:
                self.server._on_pgcon_broken()
            else:
                self.server._on_pgcon_lost()

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


# Underscored name for _SYNC_MESSAGE because it should always be emitted
# using write_sync(), which properly counts them
cdef bytes _SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
cdef bytes FLUSH_MESSAGE = bytes(WriteBuffer.new_message(b'H').end_message())

cdef EdegDBCodecContext DEFAULT_CODEC_CONTEXT = EdegDBCodecContext()
