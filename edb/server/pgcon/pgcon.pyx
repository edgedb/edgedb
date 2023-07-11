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

from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)

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
from collections import deque

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
from edb.pgsql import params as pg_params
from edb.pgsql import codegen as pg_codegen

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
from edb.server.protocol cimport pg_ext
from edb.server import pgconnparams
from edb.server import metrics

from edb.server.protocol cimport frontend

from edb.common import debug

from . import errors as pgerror

include "scram.pyx"

DEF DATA_BUFFER_SIZE = 100_000
DEF PREP_STMTS_CACHE = 100
DEF TCP_KEEPIDLE = 24
DEF TCP_KEEPINTVL = 2
DEF TCP_KEEPCNT = 3

DEF COPY_SIGNATURE = b"PGCOPY\n\377\r\n\0"

DEF TEXT_OID = 25

cdef object CARD_NO_RESULT = compiler.Cardinality.NO_RESULT
cdef object FMT_NONE = compiler.OutputFormat.NONE
cdef dict POSTGRES_SHUTDOWN_ERR_CODES = {
    '57P01': 'admin_shutdown',
    '57P02': 'crash_shutdown',
}

cdef bytes INIT_CON_SCRIPT = None
cdef str INIT_CON_SCRIPT_DATA = ''
cdef object EMPTY_SQL_STATE = b"{}"

cdef object logger = logging.getLogger('edb.server')

# The '_edgecon_state table' is used to store information about
# the current session. The `type` column is one character, with one
# of the following values:
#
# * 'C': a session-level config setting
#
# * 'B': a session-level config setting that's implemented by setting
#   a corresponding Postgres config setting.
# * 'A': an instance-level config setting from command-line arguments
# * 'E': an instance-level config setting from environment variable
SETUP_TEMP_TABLE_SCRIPT = '''
        CREATE TEMPORARY TABLE _edgecon_state (
            name text NOT NULL,
            value jsonb NOT NULL,
            type text NOT NULL CHECK(
                type = 'C' OR type = 'B' OR type = 'A' OR type = 'E'),
            UNIQUE(name, type)
        );
'''

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

    return textwrap.dedent(f'''
        {pg_is_in_recovery}

        {SETUP_TEMP_TABLE_SCRIPT}

        {INIT_CON_SCRIPT_DATA}

        PREPARE _clear_state AS
            DELETE FROM _edgecon_state WHERE type = 'C' OR type = 'B';

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

        PREPARE _apply_sql_state(jsonb) AS
            SELECT
                e.key AS name,
                pg_catalog.set_config(e.key, e.value, false) AS value
            FROM
                jsonb_each_text($1::jsonb) AS e;
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


async def connect(
    connargs: Dict[str, Any],
    dbname: str,
    backend_params: pg_params.BackendRuntimeParams,
    apply_init_script: bool = True,
):
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

    if (
        backend_params.has_create_role
        and backend_params.session_authorization_role
    ):
        sup_role = backend_params.session_authorization_role
        if connargs['user'] != sup_role:
            # We used to use SET SESSION AUTHORIZATION here, there're some
            # security differences over SET ROLE, but as we don't allow
            # accessing Postgres directly through EdgeDB, SET ROLE is mostly
            # fine here. (Also hosted backends like Postgres on DigitalOcean
            # support only SET ROLE)
            await pgcon.sql_execute(f'SET ROLE {pg_qi(sup_role)}'.encode())

    if 'in_hot_standby' in pgcon.parameter_status:
        # in_hot_standby is always present in Postgres 14 and above
        if pgcon.parameter_status['in_hot_standby'] == 'on':
            # Abort if we're connecting to a hot standby
            pgcon.terminate()
            raise pgerror.BackendError(fields=dict(
                M="cannot use a hot standby",
                C=pgerror.ERROR_READ_ONLY_SQL_TRANSACTION,
            ))

    if apply_init_script:
        if INIT_CON_SCRIPT is None:
            INIT_CON_SCRIPT = _build_init_con_script(
                # On lower versions of Postgres we use pg_is_in_recovery() to
                # check if it is a hot standby, and error out if it is.
                check_pg_is_in_recovery=(
                    'in_hot_standby' not in pgcon.parameter_status
                ),
            )
        await pgcon.sql_execute(INIT_CON_SCRIPT)

    return pgcon


def set_init_con_script_data(cfg):
    global INIT_CON_SCRIPT, INIT_CON_SCRIPT_DATA
    INIT_CON_SCRIPT = None
    INIT_CON_SCRIPT_DATA = (f'''
        INSERT INTO _edgecon_state
        SELECT * FROM jsonb_to_recordset({pg_ql(json.dumps(cfg))}::jsonb)
        AS cfg(name text, value jsonb, type text);
    ''').strip()


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


cdef class PGMessage:
    def __init__(
        self,
        PGAction action,
        bytes stmt_name=None,
        str portal_name=None,
        args=None,
        query_unit=None,
        fe_settings=None,
        injected=False,
    ):
        self.action = action
        self.stmt_name = stmt_name
        self.orig_portal_name = portal_name
        if portal_name:
            self.portal_name = b'u' + portal_name.encode("utf-8")
        else:
            self.portal_name = b''
        self.args = args
        self.query_unit = query_unit

        self.fe_settings = fe_settings
        self.valid = True
        self.injected = injected
        if self.query_unit is not None:
            self.frontend_only = self.query_unit.frontend_only
        else:
            self.frontend_only = False

    cdef inline bint is_frontend_only(self):
        return self.frontend_only

    def invalidate(self):
        self.valid = False

    cdef inline bint is_valid(self):
        return self.valid

    cdef inline bint is_injected(self):
        return self.injected

    def as_injected(self) -> PGMessage:
        return PGMessage(
            action=self.action,
            stmt_name=self.stmt_name,
            portal_name=self.orig_portal_name,
            args=self.args,
            query_unit=self.query_unit,
            fe_settings=self.fe_settings,
            injected=True,
        )

    def __repr__(self):
        rv = []
        if self.action == PGAction.START_IMPLICIT_TX:
            rv.append("START_IMPLICIT_TX")
        elif self.action == PGAction.PARSE:
            rv.append("PARSE")
        elif self.action == PGAction.BIND:
            rv.append("BIND")
        elif self.action == PGAction.DESCRIBE_STMT:
            rv.append("DESCRIBE_STMT")
        elif self.action == PGAction.DESCRIBE_STMT_ROWS:
            rv.append("DESCRIBE_STMT_ROWS")
        elif self.action == PGAction.DESCRIBE_PORTAL:
            rv.append("DESCRIBE_PORTAL")
        elif self.action == PGAction.EXECUTE:
            rv.append("EXECUTE")
        elif self.action == PGAction.CLOSE_STMT:
            rv.append("CLOSE_STMT")
        elif self.action == PGAction.CLOSE_PORTAL:
            rv.append("CLOSE_PORTAL")
        elif self.action == PGAction.FLUSH:
            rv.append("FLUSH")
        elif self.action == PGAction.SYNC:
            rv.append("SYNC")
        if self.stmt_name is not None:
            rv.append(f"stmt_name={self.stmt_name}")
        if self.orig_portal_name is not None:
            rv.append(f"portal_name={self.orig_portal_name!r}")
        if self.args is not None:
            rv.append(f"args={self.args}")
        rv.append(f"frontend_only={self.is_frontend_only()}")
        rv.append(f"injected={self.is_injected()}")
        if self.query_unit is not None:
            rv.append(f"query_unit={self.query_unit}")
        if len(rv) > 1:
            rv.insert(1, ":")
        return " ".join(rv)


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

        self.log_listeners = []

        self.pgaddr = addr
        self.server = None
        self.tenant = None
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

    cpdef set_stmt_cache_size(self, int maxsize):
        self.prep_stmts.resize(maxsize)

    @property
    def is_ssl(self):
        return self._is_ssl

    @is_ssl.setter
    def is_ssl(self, value):
        self._is_ssl = value

    def debug_print(self, *args):
        print(
            '::PGCONN::',
            hex(id(self)),
            f'pgpid: {self.backend_pid}',
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

    def get_server_parameter_status(self, parameter: str) -> Optional[str]:
        return self.parameter_status.get(parameter)

    def abort(self):
        if not self.transport:
            return
        self.close_requested = True
        self.transport.abort()
        self.transport = None
        self.connected = False
        self.prep_stmts.clear()

    def terminate(self):
        if not self.transport:
            return
        self.close_requested = True
        self.write(WriteBuffer.new_message(b'X').end_message())
        self.transport.close()
        self.transport = None
        self.connected = False
        self.prep_stmts.clear()

        if self.msg_waiter and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

    async def close(self):
        self.terminate()

    def set_tenant(self, tenant):
        self.tenant = tenant
        self.server = tenant.server

    def mark_as_system_db(self):
        if self.tenant.get_backend_runtime_params().has_create_database:
            assert defines.EDGEDB_SYSTEM_DB in self.dbname
        self.is_system_db = True

    def add_log_listener(self, cb):
        self.log_listeners.append(cb)

    async def listen_for_sysevent(self):
        try:
            if self.tenant.get_backend_runtime_params().has_create_database:
                assert defines.EDGEDB_SYSTEM_DB in self.dbname
            await self.sql_execute(b'LISTEN __edgedb_sysevent__;')
        except Exception:
            try:
                self.abort()
            finally:
                raise

    async def signal_sysevent(self, event, **kwargs):
        if self.tenant.get_backend_runtime_params().has_create_database:
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
        await self.sql_execute(query)

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
                    return self.parse_sync_message()
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

    cdef bint before_prepare(
        self,
        bytes stmt_name,
        int dbver,
        WriteBuffer outbuf,
    ):
        cdef bint parse = 1

        while self.prep_stmts.needs_cleanup():
            stmt_name_to_clean = self.prep_stmts.cleanup_one()
            if self.debug:
                self.debug_print(f"discarding ps {stmt_name_to_clean!r}")
            outbuf.write_buffer(
                self.make_clean_stmt_message(stmt_name_to_clean))

        if stmt_name in self.prep_stmts:
            if self.prep_stmts[stmt_name] == dbver:
                parse = 0
            else:
                if self.debug:
                    self.debug_print(f"discarding ps {stmt_name_to_clean!r}")
                outbuf.write_buffer(
                    self.make_clean_stmt_message(stmt_name))
                del self.prep_stmts[stmt_name]

        return parse

    cdef write_sync(self, WriteBuffer outbuf):
        outbuf.write_bytes(_SYNC_MESSAGE)
        self.waiting_for_sync += 1

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

    def _build_apply_sql_state_req(self, bytes state, WriteBuffer out):
        cdef:
            WriteBuffer buf

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

        if state != EMPTY_SQL_STATE:
            buf = WriteBuffer.new_message(b'B')
            buf.write_bytestring(b'')  # portal name
            buf.write_bytestring(b'_apply_sql_state')  # statement name
            buf.write_int16(1)  # number of format codes
            buf.write_int16(1)  # binary
            buf.write_int16(1)  # number of parameters
            buf.write_int32(len(state) + 1)
            buf.write_byte(1)  # jsonb format version
            buf.write_bytes(state)
            buf.write_int16(0)  # number of result columns
            out.write_buffer(buf.end_message())

            buf = WriteBuffer.new_message(b'E')
            buf.write_bytestring(b'')  # portal name
            buf.write_int32(0)  # limit: 0 - return all rows
            out.write_buffer(buf.end_message())

    async def _parse_apply_state_resp(self, int expected_completed):
        cdef:
            int num_completed = 0

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
        ssize_t start, ssize_t end, int dbver, object parse_array
    ):
        # parse_array is an array of booleans for output with the same size as
        # the query_unit_group, indicating if each unit is freshly parsed
        cdef:
            WriteBuffer out
            WriteBuffer buf
            WriteBuffer bind_data
            bytes stmt_name
            ssize_t idx = start

        out = WriteBuffer.new()
        parsed = set()

        if state is not None and start == 0:
            self._build_apply_state_req(state, out)

        for query_unit, bind_data in zip(
                query_unit_group.units[start:end], bind_datas):
            if query_unit.system_config:
                raise RuntimeError(
                    "CONFIGURE INSTANCE command is not allowed in scripts"
                )
            stmt_name = query_unit.sql_hash
            if stmt_name:
                assert len(query_unit.sql) == 1
                # The same EdgeQL query may show up twice in the same script.
                # We just need to know and skip if we've already parsed the
                # same query within current send batch, because self.prep_stmts
                # will be updated before the next batch, with maybe a different
                # dbver after DDL.
                if stmt_name not in parsed and self.before_prepare(
                    stmt_name, dbver, out
                ):
                    buf = WriteBuffer.new_message(b'P')
                    buf.write_bytestring(stmt_name)
                    buf.write_bytestring(query_unit.sql[0])
                    buf.write_int16(0)
                    out.write_buffer(buf.end_message())
                    parse_array[idx] = True
                    parsed.add(stmt_name)

                buf = WriteBuffer.new_message(b'B')
                buf.write_bytestring(b'')  # portal name
                buf.write_bytestring(stmt_name)
                buf.write_buffer(bind_data)
                out.write_buffer(buf.end_message())

                buf = WriteBuffer.new_message(b'E')
                buf.write_bytestring(b'')  # portal name
                buf.write_int32(0)  # limit: 0 - return all rows
                out.write_buffer(buf.end_message())

            else:
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

            idx += 1

        if end == len(query_unit_group.units):
            self.write_sync(out)
        else:
            out.write_bytes(FLUSH_MESSAGE)

        self.write(out)

    async def wait_for_state_resp(self, bytes state, bint state_sync):
        if state_sync:
            try:
                await self._parse_apply_state_resp(2 if state is None else 3)
            finally:
                await self.wait_for_sync()
            self.last_state = state
        else:
            await self._parse_apply_state_resp(2 if state is None else 3)

    async def wait_for_command(
        self,
        object query_unit,
        bint parse,
        int dbver,
        *,
        bint ignore_data,
        frontend.AbstractFrontendConnection fe_conn = None,
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
                    elif fe_conn is None:
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
                            fe_conn.write(buf)
                            buf = None

                elif mtype == b'C':  ## result
                    # CommandComplete
                    self.buffer.discard_message()
                    if buf is not None:
                        fe_conn.write(buf)
                        buf = None
                    return result

                elif mtype == b'1':
                    # ParseComplete
                    self.buffer.discard_message()
                    if parse:
                        self.prep_stmts[query_unit.sql_hash] = dbver

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

                elif mtype == b'3':
                    # CloseComplete
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
        query,
        frontend.AbstractFrontendConnection fe_conn,
        WriteBuffer bind_data,
        bint use_prep_stmt,
        bytes state,
        int dbver,
    ):
        cdef:
            WriteBuffer out
            WriteBuffer buf
            bytes stmt_name

            int32_t dat_len

            bint parse = 1
            bint state_sync = 0

            bint has_result = query.cardinality is not CARD_NO_RESULT
            bint discard_result = (
                fe_conn is not None and query.output_format == FMT_NONE)

            uint64_t msgs_num = <uint64_t>(len(query.sql))
            uint64_t msgs_executed = 0
            uint64_t i

        out = WriteBuffer.new()

        if state is not None:
            self._build_apply_state_req(state, out)
            if (
                query.tx_id or
                not query.is_transactional or
                query.append_rollback
            ):
                # This query has START TRANSACTION or non-transactional command
                # like CREATE DATABASE in it.
                # Restoring state must be performed in a separate
                # implicit transaction (otherwise START TRANSACTION DEFERRABLE
                # or CREATE DATABASE (since PG 14.7) would fail).
                # Hence - inject a SYNC after a state restore step.
                state_sync = 1
                self.write_sync(out)

        if query.append_rollback:
            if self.in_tx():
                sp_name = f'_edb_{time.monotonic_ns()}'
                sql = f'SAVEPOINT {sp_name}'.encode('utf-8')
            else:
                sp_name = None
                sql = b'START TRANSACTION'

            buf = WriteBuffer.new_message(b'P')
            buf.write_bytestring(b'')
            buf.write_bytestring(sql)
            buf.write_int16(0)
            out.write_buffer(buf.end_message())

            buf = WriteBuffer.new_message(b'B')
            buf.write_bytestring(b'')  # portal name
            buf.write_bytestring(b'')  # statement name
            buf.write_int16(0)  # number of format codes
            buf.write_int16(0)  # number of parameters
            buf.write_int16(0)  # number of result columns
            out.write_buffer(buf.end_message())

            buf = WriteBuffer.new_message(b'E')
            buf.write_bytestring(b'')  # portal name
            buf.write_int32(0)  # limit: 0 - return all rows
            out.write_buffer(buf.end_message())

            # Insert a SYNC as a boundary of the parsing logic later
            self.write_sync(out)

        if use_prep_stmt:
            stmt_name = query.sql_hash
            parse = self.before_prepare(
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

        if query.append_rollback:
            if sp_name:
                sql = f'ROLLBACK TO SAVEPOINT {sp_name}'.encode('utf-8')
            else:
                sql = b'ROLLBACK'

            buf = WriteBuffer.new_message(b'P')
            buf.write_bytestring(b'')
            buf.write_bytestring(sql)
            buf.write_int16(0)
            out.write_buffer(buf.end_message())

            buf = WriteBuffer.new_message(b'B')
            buf.write_bytestring(b'')  # portal name
            buf.write_bytestring(b'')  # statement name
            buf.write_int16(0)  # number of format codes
            buf.write_int16(0)  # number of parameters
            buf.write_int16(0)  # number of result columns
            out.write_buffer(buf.end_message())

            buf = WriteBuffer.new_message(b'E')
            buf.write_bytestring(b'')  # portal name
            buf.write_int32(0)  # limit: 0 - return all rows
            out.write_buffer(buf.end_message())

        self.write_sync(out)
        self.write(out)

        result = None

        try:
            if state is not None:
                await self.wait_for_state_resp(state, state_sync)

            if query.append_rollback:
                await self.wait_for_sync()

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
                        if not has_result and fe_conn is not None:
                            raise errors.InternalServerError(
                                f'query that was inferred to have '
                                f'no data returned received a DATA package; '
                                f'query: {query.sql}')

                        if fe_conn is None:
                            ncol = self.buffer.read_int16()
                            row = []
                            for i in range(ncol):
                                dat_len = self.buffer.read_int32()
                                if dat_len == -1:
                                    row.append(None)
                                else:
                                    row.append(
                                        self.buffer.read_bytes(dat_len))
                            if result is None:
                                result = []
                            result.append(row)
                        else:
                            if buf is None:
                                buf = WriteBuffer.new()

                            self.buffer.redirect_messages(buf, b'D', 0)
                            if buf.len() >= DATA_BUFFER_SIZE:
                                fe_conn.write(buf)
                                buf = None

                    elif mtype == b'C':  ## result
                        # CommandComplete
                        self.buffer.discard_message()
                        if buf is not None:
                            fe_conn.write(buf)
                            buf = None
                        msgs_executed += 1
                        if msgs_executed == msgs_num:
                            break

                    elif mtype == b'1' and parse:
                        # ParseComplete
                        self.buffer.discard_message()
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
                        break

                    elif mtype == b'2':
                        # BindComplete
                        self.buffer.discard_message()

                    elif mtype == b'I':  ## result
                        # EmptyQueryResponse
                        self.buffer.discard_message()
                        break

                    elif mtype == b'3':
                        # CloseComplete
                        self.buffer.discard_message()

                    else:
                        self.fallthrough()

                finally:
                    self.buffer.finish_message()
        finally:
            await self.wait_for_sync()

        return result

    async def parse_execute(
        self,
        *,
        query,
        WriteBuffer bind_data,
        frontend.AbstractFrontendConnection fe_conn = None,
        bint use_prep_stmt = False,
        bytes state = None,
        int dbver = 0,
    ):
        self.before_command()
        started_at = time.monotonic()
        try:
            return await self._parse_execute(
                query,
                fe_conn,
                bind_data,
                use_prep_stmt,
                state,
                dbver,
            )
        finally:
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
            await self.after_command()

    async def sql_fetch(
        self,
        sql: bytes | tuple[bytes, ...],
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
        use_prep_stmt: bool = False,
        state: Optional[bytes] = None,
    ) -> list[tuple[bytes, ...]]:
        cdef:
            WriteBuffer bind_data = WriteBuffer.new()
            int arg_len
            tuple sql_tuple

        if not isinstance(sql, tuple):
            sql_tuple = (sql,)
        else:
            sql_tuple = sql

        if use_prep_stmt:
            sql_digest = hashlib.sha1()
            for stmt in sql_tuple:
                sql_digest.update(stmt)
            sql_hash = sql_digest.hexdigest().encode('latin1')
        else:
            sql_hash = None

        query = compiler.QueryUnit(
            sql=sql_tuple,
            sql_hash=sql_hash,
            status=b"",
        )

        if len(args) > 32767:
            raise AssertionError(
                'the number of query arguments cannot exceed 32767')

        bind_data.write_int32(0x00010001)
        bind_data.write_int16(<int16_t>len(args))
        for arg in args:
            if arg is None:
                bind_data.write_int32(-1)
            else:
                arg_len = len(arg)
                if arg_len > 0x7fffffff:
                    raise ValueError("argument too long")
                bind_data.write_int32(<int32_t>arg_len)
                bind_data.write_bytes(arg)
        bind_data.write_int32(0x00010001)

        return await self.parse_execute(
            query=query,
            bind_data=bind_data,
            use_prep_stmt=use_prep_stmt,
            state=state,
        )

    async def sql_fetch_val(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
        use_prep_stmt: bool = False,
        state: Optional[bytes] = None,
    ) -> bytes:
        data = await self.sql_fetch(
            sql,
            args=args,
            use_prep_stmt=use_prep_stmt,
            state=state,
        )
        if data is None or len(data) == 0:
            return None
        elif len(data) > 1:
            raise RuntimeError(
                f"received too many rows for sql_fetch_val({sql!r})")
        row = data[0]
        if len(row) != 1:
            raise RuntimeError(
                f"received too many columns for sql_fetch_val({sql!r})")
        return row[0]

    async def sql_fetch_col(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
        use_prep_stmt: bool = False,
        state: Optional[bytes] = None,
    ) -> list[bytes]:
        data = await self.sql_fetch(
            sql,
            args=args,
            use_prep_stmt=use_prep_stmt,
            state=state,
        )
        if not data:
            return []
        else:
            if len(data[0]) != 1:
                raise RuntimeError(
                    f"received too many columns for sql_fetch_col({sql!r})")
            return [row[0] for row in data]

    async def _sql_execute(self, bytes sql):
        cdef:
            WriteBuffer out
            WriteBuffer buf

        out = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'Q')
        buf.write_bytestring(sql)
        out.write_buffer(buf.end_message())
        self.waiting_for_sync += 1

        self.write(out)

        exc = None
        result = None

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'D':
                    self.buffer.discard_message()

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
        else:
            return result

    async def sql_execute(self, sql: bytes | tuple[bytes, ...]) -> None:
        self.before_command()
        started_at = time.monotonic()

        if isinstance(sql, tuple):
            sql_string = b";\n".join(sql)
        else:
            sql_string = sql

        try:
            return await self._sql_execute(sql_string)
        finally:
            metrics.backend_query_duration.observe(time.monotonic() - started_at)
            await self.after_command()

    async def sql_apply_state(
        self,
        dbv: pg_ext.ConnectionView,
    ):
        self.before_command()
        try:
            state = dbv.serialize_state()
            if state is not None:
                buf = WriteBuffer.new()
                self._build_apply_sql_state_req(state, buf)
                self.write_sync(buf)
                self.write(buf)

                await self._parse_apply_state_resp(
                    2 if state != EMPTY_SQL_STATE else 1
                )
                await self.wait_for_sync()
                self.last_state = state
        finally:
            await self.after_command()

    async def sql_extended_query(
        self,
        actions: list[PGMessage],
        fe_conn: frontend.AbstractFrontendConnection,
        dbver: int,
        dbv: pg_ext.ConnectionView,
        send_sync_on_error: bool = False,
    ) -> tuple[bool, bool]:
        self.before_command()
        try:
            state = self._write_sql_extended_query(actions, dbver, dbv)
            if state is not None:
                await self._parse_apply_state_resp(
                    2 if state != EMPTY_SQL_STATE else 1
                )
                await self.wait_for_sync()
                self.last_state = state
            try:
                return await self._parse_sql_extended_query(
                    actions,
                    fe_conn,
                    dbver,
                    dbv,
                    send_sync_on_error=send_sync_on_error,
                )
            finally:
                if not dbv.in_tx():
                    self.last_state = dbv.serialize_state()
        finally:
            await self.after_command()

    def _write_sql_extended_query(
        self,
        actions: list[PGMessage],
        dbver: int,
        dbv: pg_ext.ConnectionView,
    ) -> bytes:
        cdef:
            WriteBuffer buf, msg_buf
            PGMessage action

        buf = WriteBuffer.new()
        state = None
        if not dbv.in_tx():
            state = dbv.serialize_state()
            self._build_apply_sql_state_req(state, buf)
            # We need to close the implicit transaction with a SYNC here
            # because the next command may be e.g. "BEGIN DEFERRABLE".
            self.write_sync(buf)
        prepared = set()
        for action in actions:
            if action.is_frontend_only():
                continue

            if action.action == PGAction.PARSE:
                sql_text, data = action.args
                if action.stmt_name in prepared:
                    action.frontend_only = True
                else:
                    be_parse = self.before_prepare(
                        action.stmt_name, dbver, buf
                    )
                    if not be_parse:
                        if self.debug:
                            self.debug_print(
                                'Parse cache hit', action.stmt_name, sql_text)
                        action.frontend_only = True
                if not action.is_frontend_only():
                    prepared.add(action.stmt_name)
                    msg_buf = WriteBuffer.new_message(b'P')
                    msg_buf.write_bytestring(action.stmt_name)
                    msg_buf.write_bytestring(sql_text)
                    msg_buf.write_bytes(data)
                    buf.write_buffer(msg_buf.end_message())
                    if self.debug:
                        self.debug_print(
                            'Parse', action.stmt_name, sql_text, data
                        )

            elif action.action == PGAction.BIND:
                if action.query_unit is not None and action.query_unit.prepare:
                    be_stmt_name = action.query_unit.prepare.be_stmt_name
                    if be_stmt_name in prepared:
                        action.frontend_only = True
                    else:
                        be_parse = self.before_prepare(
                            be_stmt_name, dbver, buf
                        )
                        if not be_parse:
                            if self.debug:
                                self.debug_print(
                                    'Parse cache hit', be_stmt_name)
                            action.frontend_only = True
                            prepared.add(be_stmt_name)

                if not action.is_frontend_only():
                    msg_buf = WriteBuffer.new_message(b'B')
                    msg_buf.write_bytestring(action.portal_name)
                    msg_buf.write_bytestring(action.stmt_name)
                    msg_buf.write_bytes(action.args)
                    buf.write_buffer(msg_buf.end_message())

            elif (
                action.action
                in (PGAction.DESCRIBE_STMT, PGAction.DESCRIBE_STMT_ROWS)
            ):
                msg_buf = WriteBuffer.new_message(b'D')
                msg_buf.write_byte(b'S')
                msg_buf.write_bytestring(action.stmt_name)
                buf.write_buffer(msg_buf.end_message())

            elif action.action == PGAction.DESCRIBE_PORTAL:
                msg_buf = WriteBuffer.new_message(b'D')
                msg_buf.write_byte(b'P')
                msg_buf.write_bytestring(action.portal_name)
                buf.write_buffer(msg_buf.end_message())

            elif action.action == PGAction.EXECUTE:
                if action.query_unit is not None and action.query_unit.prepare:
                    be_stmt_name = action.query_unit.prepare.be_stmt_name

                    if be_stmt_name in prepared:
                        action.frontend_only = True
                    else:
                        be_parse = self.before_prepare(
                            be_stmt_name, dbver, buf
                        )
                        if not be_parse:
                            if self.debug:
                                self.debug_print(
                                    'Parse cache hit', be_stmt_name)
                            action.frontend_only = True
                            prepared.add(be_stmt_name)

                if (
                    action.query_unit is not None
                    and action.query_unit.deallocate is not None
                    and self.before_prepare(
                        action.query_unit.deallocate.be_stmt_name, dbver, buf
                    )
                ):
                    # This prepared statement does not actually exist
                    # on this connection, so there's nothing to DEALLOCATE.
                    action.frontend_only = True

                if not action.is_frontend_only():
                    msg_buf = WriteBuffer.new_message(b'E')
                    msg_buf.write_bytestring(action.portal_name)
                    msg_buf.write_int32(action.args)
                    buf.write_buffer(msg_buf.end_message())

            elif action.action == PGAction.CLOSE_PORTAL:
                if action.query_unit is not None and action.query_unit.prepare:
                    be_stmt_name = action.query_unit.prepare.be_stmt_name
                    if be_stmt_name in prepared:
                        action.frontend_only = True

                if not action.is_frontend_only():
                    msg_buf = WriteBuffer.new_message(b'C')
                    msg_buf.write_byte(b'P')
                    msg_buf.write_bytestring(action.portal_name)
                    buf.write_buffer(msg_buf.end_message())

            elif action.action == PGAction.CLOSE_STMT:
                if action.query_unit is not None and action.query_unit.prepare:
                    be_stmt_name = action.query_unit.prepare.be_stmt_name
                    if be_stmt_name in prepared:
                        action.frontend_only = True

                if not action.is_frontend_only():
                    msg_buf = WriteBuffer.new_message(b'C')
                    msg_buf.write_byte(b'S')
                    msg_buf.write_bytestring(action.stmt_name)
                    buf.write_buffer(msg_buf.end_message())

            elif action.action == PGAction.FLUSH:
                msg_buf = WriteBuffer.new_message(b'H')
                buf.write_buffer(msg_buf.end_message())

            elif action.action == PGAction.SYNC:
                self.write_sync(buf)

        if action.action not in (PGAction.SYNC, PGAction.FLUSH):
            # Make sure _parse_sql_extended_query() complete by sending a FLUSH
            # to the backend, but we won't flush the results to the frontend
            # because it's not requested.
            msg_buf = WriteBuffer.new_message(b'H')
            buf.write_buffer(msg_buf.end_message())

        self.write(buf)
        return state

    async def _parse_sql_extended_query(
        self,
        actions: list[PGMessage],
        fe_conn: frontend.AbstractFrontendConnection,
        dbver: int,
        dbv: pg_ext.ConnectionView,
        send_sync_on_error: bool,
    ) -> tuple[bool, bool]:
        cdef:
            WriteBuffer buf, msg_buf
            PGMessage action
            bint ignore_till_sync = False

        buf = WriteBuffer.new()
        rv = True

        for action in actions:
            if self.debug:
                self.debug_print(
                    'ACTION', action, 'ignore_till_sync=', ignore_till_sync
                )

            if ignore_till_sync and action.action != PGAction.SYNC:
                continue
            elif action.action == PGAction.FLUSH:
                if buf.len() > 0:
                    fe_conn.write(buf)
                    fe_conn.flush()
                    buf = WriteBuffer.new()
                continue
            elif action.action == PGAction.START_IMPLICIT_TX:
                dbv.start_implicit()
                continue
            elif action.is_frontend_only():
                if action.action == PGAction.PARSE:
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(b'1')
                        buf.write_buffer(msg_buf.end_message())
                elif action.action == PGAction.BIND:
                    dbv.create_portal(
                        action.orig_portal_name, action.query_unit
                    )
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(b'2')  # BindComplete
                        buf.write_buffer(msg_buf.end_message())
                elif action.action == PGAction.DESCRIBE_STMT:
                    # ParameterDescription
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(b't')
                        msg_buf.write_int16(0)  # number of parameters
                        buf.write_buffer(msg_buf.end_message())
                elif action.action == PGAction.EXECUTE:
                    if action.query_unit.set_vars is not None:
                        assert len(action.query_unit.set_vars) == 1
                        # CommandComplete
                        msg_buf = WriteBuffer.new_message(b'C')
                        if next(
                            iter(action.query_unit.set_vars.values())
                        ) is None:
                            msg_buf.write_bytestring(b'RESET')
                        else:
                            msg_buf.write_bytestring(b'SET')
                        buf.write_buffer(msg_buf.end_message())
                    elif action.query_unit.get_var is not None:
                        # RowDescription
                        msg_buf = WriteBuffer.new_message(b'T')
                        msg_buf.write_int16(1)  # number of fields
                        # field name
                        msg_buf.write_str(action.query_unit.get_var, "utf-8")
                        # object ID of the table to identify the field
                        msg_buf.write_int32(0)
                        # attribute number of the column in prev table
                        msg_buf.write_int16(0)
                        # object ID of the field's data type
                        msg_buf.write_int32(TEXT_OID)
                        # data type size
                        msg_buf.write_int16(-1)
                        # type modifier
                        msg_buf.write_int32(-1)
                        # format code being used for the field
                        msg_buf.write_int16(0)
                        buf.write_buffer(msg_buf.end_message())

                        # DataRow
                        msg_buf = WriteBuffer.new_message(b'D')
                        msg_buf.write_int16(1)  # number of column values
                        msg_buf.write_len_prefixed_utf8(
                            dbv.current_fe_settings()[
                                action.query_unit.get_var
                            ]
                        )
                        buf.write_buffer(msg_buf.end_message())

                        # CommandComplete
                        msg_buf = WriteBuffer.new_message(b'C')
                        msg_buf.write_bytestring(b'SHOW')
                        buf.write_buffer(msg_buf.end_message())
                    elif not action.is_injected():
                        # NoData
                        msg_buf = WriteBuffer.new_message(b'n')
                        buf.write_buffer(msg_buf.end_message())
                        # CommandComplete
                        msg_buf = WriteBuffer.new_message(b'C')
                        assert action.query_unit.command_tag, \
                            "emulated SQL unit has no command_tag"
                        msg_buf.write_bytestring(action.query_unit.command_tag)
                        buf.write_buffer(msg_buf.end_message())

                    dbv.on_success(action.query_unit)
                    fe_conn.on_success(action.query_unit)
                elif action.action == PGAction.CLOSE_PORTAL:
                    dbv.close_portal_if_exists(action.orig_portal_name)
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(b'3') # CloseComplete
                        buf.write_buffer(msg_buf.end_message())
                elif action.action == PGAction.CLOSE_STMT:
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(b'3')  # CloseComplete
                        buf.write_buffer(msg_buf.end_message())
                if (
                    action.action == PGAction.DESCRIBE_STMT or
                    action.action == PGAction.DESCRIBE_PORTAL
                ):
                    if action.query_unit.set_vars is not None:
                        msg_buf = WriteBuffer.new_message(b'n')  # NoData
                        buf.write_buffer(msg_buf.end_message())
                    elif action.query_unit.get_var is not None:
                        # RowDescription
                        msg_buf = WriteBuffer.new_message(b'T')
                        msg_buf.write_int16(1)  # number of fields
                        # field name
                        msg_buf.write_str(action.query_unit.get_var, "utf-8")
                        # object ID of the table to identify the field
                        msg_buf.write_int32(0)
                        # attribute number of the column in prev table
                        msg_buf.write_int16(0)
                        # object ID of the field's data type
                        msg_buf.write_int32(TEXT_OID)
                        # data type size
                        msg_buf.write_int16(-1)
                        # type modifier
                        msg_buf.write_int32(-1)
                        # format code being used for the field
                        msg_buf.write_int16(0)
                        buf.write_buffer(msg_buf.end_message())
                continue

            while True:
                if not self.buffer.take_message():
                    if buf.len() > 0:
                        fe_conn.write(buf)
                        fe_conn.flush()
                        buf = WriteBuffer.new()
                    await self.wait_for_message()

                mtype = self.buffer.get_message_type()
                if self.debug:
                    self.debug_print(f'recv backend message: {chr(mtype)!r}')
                    if ignore_till_sync:
                        self.debug_print("ignoring until SYNC")

                if ignore_till_sync and mtype != b'Z':
                    self.buffer.discard_message()
                    continue

                if (
                    mtype == b'3'
                    and action.action != PGAction.CLOSE_PORTAL
                    and action.action != PGAction.CLOSE_STMT
                ):
                    # before_prepare() initiates LRU cleanup for
                    # prepared statements, so CloseComplete may
                    # appear here.
                    self.buffer.discard_message()
                    continue

                # ParseComplete
                if mtype == b'1' and action.action == PGAction.PARSE:
                    self.buffer.finish_message()
                    if self.debug:
                        self.debug_print('PARSE COMPLETE MSG')
                    self.prep_stmts[action.stmt_name] = dbver
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(mtype)
                        buf.write_buffer(msg_buf.end_message())
                    break

                # BindComplete
                elif mtype == b'2' and action.action == PGAction.BIND:
                    self.buffer.finish_message()
                    if self.debug:
                        self.debug_print('BIND COMPLETE MSG')
                    dbv.create_portal(
                        action.orig_portal_name, action.query_unit
                    )
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(mtype)
                        buf.write_buffer(msg_buf.end_message())
                    break

                elif (
                    # RowDescription or NoData
                    mtype == b'T' or mtype == b'n'
                ) and (
                    action.action == PGAction.DESCRIBE_STMT or
                    action.action == PGAction.DESCRIBE_STMT_ROWS or
                    action.action == PGAction.DESCRIBE_PORTAL
                ):
                    data = self.buffer.consume_message()
                    if self.debug:
                        self.debug_print('END OF DESCRIBE', mtype)
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(mtype)
                        msg_buf.write_bytes(data)
                        buf.write_buffer(msg_buf.end_message())
                    break

                elif (
                    mtype == b't'
                    and action.action == PGAction.DESCRIBE_STMT_ROWS
                ):
                    self.buffer.consume_message()

                elif (
                    # CommandComplete, EmptyQueryResponse, PortalSuspended
                    mtype == b'C' or mtype == b'I' or mtype == b's'
                ) and action.action == PGAction.EXECUTE:
                    data = self.buffer.consume_message()
                    if self.debug:
                        self.debug_print('END OF EXECUTE', mtype)
                    fe_conn.on_success(action.query_unit)
                    dbv.on_success(action.query_unit)

                    if (
                        action.query_unit is not None
                        and action.query_unit.prepare is not None
                    ):
                        be_stmt_name = action.query_unit.prepare.be_stmt_name
                        if self.debug:
                            self.debug_print(
                                f"remembering ps {be_stmt_name}, "
                                f"dbver {dbver}"
                            )
                        self.prep_stmts[be_stmt_name] = dbver

                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(mtype)
                        msg_buf.write_bytes(data)
                        buf.write_buffer(msg_buf.end_message())
                    break

                # CloseComplete
                elif mtype == b'3' and action.action == PGAction.CLOSE_PORTAL:
                    self.buffer.finish_message()
                    if self.debug:
                        self.debug_print('CLOSE COMPLETE MSG (PORTAL)')
                    dbv.close_portal_if_exists(action.orig_portal_name)
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(mtype)
                        buf.write_buffer(msg_buf.end_message())
                    break

                elif mtype == b'3' and action.action == PGAction.CLOSE_STMT:
                    self.buffer.finish_message()
                    if self.debug:
                        self.debug_print('CLOSE COMPLETE MSG (STATEMENT)')
                    if not action.is_injected():
                        msg_buf = WriteBuffer.new_message(mtype)
                        buf.write_buffer(msg_buf.end_message())
                    break

                elif mtype == b'E':  # ErrorResponse
                    rv = False
                    if self.debug:
                        self.debug_print('ERROR RESPONSE MSG', send_sync_on_error)
                    fe_conn.on_error(action.query_unit)
                    dbv.on_error()
                    self._rewrite_sql_error_response(action, buf)
                    fe_conn.write(buf)
                    fe_conn.flush()
                    buf = WriteBuffer.new()
                    ignore_till_sync = True
                    if send_sync_on_error:
                        be_buf = WriteBuffer.new()
                        if self.debug:
                            self.debug_print("sent backend message: 'Z'")
                        self.write_sync(be_buf)
                        self.write(be_buf)
                    else:
                        break

                elif mtype == b'Z':  # ReadyForQuery
                    ignore_till_sync = False
                    dbv.end_implicit()
                    status = self.parse_sync_message()
                    msg_buf = WriteBuffer.new_message(b'Z')
                    msg_buf.write_byte(status)
                    buf.write_buffer(msg_buf.end_message())

                    fe_conn.write(buf)
                    fe_conn.flush()
                    return True, True

                else:
                    if not action.is_injected():
                        if self.debug:
                            self.debug_print('REDIRECT OTHER MSG', mtype)
                        self.buffer.redirect_messages(buf, mtype, 0)
                    else:
                        logger.warning(
                            f"discarding unexpected backend message: "
                            f"{chr(mtype)!r}"
                        )
                        self.buffer.discard_message()

        if buf.len() > 0:
            fe_conn.write(buf)
        return rv, False

    def _write_error_position(
        self,
        msg_buf: WriteBuffer,
        query: bytes,
        pos_bytes: bytes,
        translation_data: Optional[pg_codegen.TranslationData],
        offset: int = 0,
    ):
        if translation_data:
            pos = int(pos_bytes.decode('utf8'))
            if offset > 0 or pos + offset > 0:
                pos += offset
            pos = translation_data.translate(pos)
            # pg uses 1-based indexes
            pos += 1
            pos_bytes = str(pos).encode('utf8')
            msg_buf.write_byte(b'P') # Position
        else:
            msg_buf.write_byte(b'q')  # Internal query
            msg_buf.write_bytestring(query)
            msg_buf.write_byte(b'p')  # Internal position
        msg_buf.write_bytestring(pos_bytes)

    cdef _rewrite_sql_error_response(self, PGMessage action, WriteBuffer buf):
        cdef WriteBuffer msg_buf

        if action.action == PGAction.PARSE:
            msg_buf = WriteBuffer.new_message(b'E')
            while True:
                field_type = self.buffer.read_byte()
                if field_type == b'P':  # Position
                    qu = (action.query_unit.translation_data
                          if action.query_unit else None)
                    self._write_error_position(
                        msg_buf,
                        action.args[0],
                        self.buffer.read_null_str(),
                        qu
                    )
                    continue
                else:
                    msg_buf.write_byte(field_type)
                    if field_type == b'\0':
                        break
                msg_buf.write_bytestring(
                    self.buffer.read_null_str()
                )
            self.buffer.finish_message()
            buf.write_buffer(msg_buf.end_message())
        elif action.action in (
            PGAction.BIND,
            PGAction.EXECUTE,
            PGAction.DESCRIBE_PORTAL,
            PGAction.CLOSE_PORTAL,
        ):
            portal_name = action.orig_portal_name
            msg_buf = WriteBuffer.new_message(b'E')
            message = None
            while True:
                field_type = self.buffer.read_byte()
                if field_type == b'C':  # Code
                    msg_buf.write_byte(field_type)
                    code = self.buffer.read_null_str()
                    msg_buf.write_bytestring(code)
                    if code == b'34000':
                        message = f'cursor "{portal_name}" does not exist'
                    elif code == b'42P03':
                        message = f'cursor "{portal_name}" already exists'
                elif field_type == b'M' and message:
                    msg_buf.write_byte(field_type)
                    msg_buf.write_bytestring(
                        message.encode('utf-8')
                    )
                elif field_type == b'P':
                    if action.query_unit is not None:
                        qu = action.query_unit
                        query_text = qu.query.encode("utf-8")
                        if qu.prepare is not None:
                            offset = -55
                            translation_data = qu.prepare.translation_data
                        else:
                            offset = 0
                            translation_data = qu.translation_data
                    else:
                        query_text = b""
                        translation_data = None
                        offset = 0

                    self._write_error_position(
                        msg_buf,
                        query_text,
                        self.buffer.read_null_str(),
                        translation_data,
                        offset,
                    )
                else:
                    msg_buf.write_byte(field_type)
                    if field_type == b'\0':
                        break
                    msg_buf.write_bytestring(
                        self.buffer.read_null_str()
                    )
            self.buffer.finish_message()
            buf.write_buffer(msg_buf.end_message())
        else:
            data = self.buffer.consume_message()
            msg_buf = WriteBuffer.new_message(b'E')
            msg_buf.write_bytes(data)
            buf.write_buffer(msg_buf.end_message())

    async def run_ddl(
        self,
        object query_unit,
        bytes state=None
    ):
        if query_unit.ddl_stmt_id is None:
            return await self.sql_execute(query_unit.sql)
        else:
            data = await self.sql_fetch(query_unit.sql, state=state)
            return self.load_ddl_return(query_unit, data)

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

    async def handle_ddl_in_script(
        self, object query_unit, bint parse, int dbver
    ):
        data = None
        for sql in query_unit.sql:
            data = await self.wait_for_command(
                query_unit, parse, dbver, ignore_data=bool(data)
            ) or data
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
                    self.transport.pause_reading()
                    await output_queue.put((block, i, out))
                    self.transport.resume_reading()
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
            # In case we errored while the transport was suspended.
            self.transport.resume_reading()
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

        for k, v in self.pgaddr['server_settings'].items():
            buf.write_bytestring(k.encode('utf-8'))
            buf.write_bytestring(v.encode('utf-8'))

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
                        self.tenant.set_pg_unavailable_msg(pgmsg)
                        self.tenant.on_sys_pgcon_failover_signal()

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
                self.tenant.on_sys_pgcon_parameter_status_updated(name, value)
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

                logger.debug("received system event: %s", event)

                event_payload = event_data.get('args')
                if event == 'schema-changes':
                    dbname = event_payload['dbname']
                    self.tenant.on_remote_ddl(dbname)
                elif event == 'database-config-changes':
                    dbname = event_payload['dbname']
                    self.tenant.on_remote_database_config_change(dbname)
                elif event == 'system-config-changes':
                    self.tenant.on_remote_system_config_change()
                elif event == 'global-schema-changes':
                    self.tenant.on_global_schema_change()
                elif event == 'database-changes':
                    self.tenant.on_remote_database_changes()
                elif event == 'extension-changes':
                    self.tenant.on_database_extensions_changes()
                elif event == 'ensure-database-not-used':
                    dbname = event_payload['dbname']
                    self.tenant.on_remote_database_quarantine(dbname)
                else:
                    raise AssertionError(f'unexpected system event: {event!r}')

            return True

        elif mtype == b'N':
            # NoticeResponse
            if self.log_listeners:
                _, fields = self.parse_error_message()
                severity = fields.get('V')
                message = fields.get('M')
                for listener in self.log_listeners:
                    self.loop.call_soon(listener, severity, message)
            else:
                self.buffer.discard_message()
            return True

        return False

    cdef parse_error_message(self):
        cdef:
            char code
            str value
            dict fields = {}
            object err_cls

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break
            value = self.buffer.read_null_str().decode()
            fields[chr(code)] = value

        self.buffer.finish_message()

        err_cls = pgerror.get_error_class(fields)
        if self.debug:
            self.debug_print('ERROR', err_cls.__name__, fields)

        return err_cls, fields

    cdef char parse_sync_message(self):
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
        return status

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
            self.tenant.on_sys_pgcon_connection_lost(exc)
        elif self.tenant is not None:
            if not self.close_requested:
                self.tenant.on_pgcon_broken()
            else:
                self.tenant.on_pgcon_lost()

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
