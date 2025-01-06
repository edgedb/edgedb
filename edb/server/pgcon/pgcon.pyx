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
import sys
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
from edb.server.compiler import dbstate
from edb.server import defines
from edb.server.cache cimport stmt_cache
from edb.server.dbview cimport dbview
from edb.server.protocol cimport args_ser
from edb.server.protocol cimport pg_ext
from edb.server import metrics

from edb.server.protocol cimport frontend

from edb.common import debug
from edb.common import typeutils

from . import errors as pgerror

DEF DATA_BUFFER_SIZE = 100_000
DEF PREP_STMTS_CACHE = 100

DEF COPY_SIGNATURE = b"PGCOPY\n\377\r\n\0"

DEF TEXT_OID = 25

cdef object CARD_NO_RESULT = compiler.Cardinality.NO_RESULT
cdef object FMT_NONE = compiler.OutputFormat.NONE
cdef dict POSTGRES_SHUTDOWN_ERR_CODES = {
    '57P01': 'admin_shutdown',
    '57P02': 'crash_shutdown',
}

cdef object EMPTY_SQL_STATE = b"{}"
cdef WriteBuffer NO_ARGS = args_ser.combine_raw_args()

cdef object logger = logging.getLogger('edb.server')


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

    def __init__(self, dbname):
        self.buffer = ReadBuffer()

        self.loop = asyncio.get_running_loop()
        self.dbname = dbname

        self.connection = None
        self.transport = None
        self.msg_waiter = None

        self.prep_stmts = stmt_cache.StatementsCache(maxsize=PREP_STMTS_CACHE)

        self.connected_fut = self.loop.create_future()
        self.connected = False

        self.waiting_for_sync = 0
        self.xact_status = PQTRANS_UNKNOWN

        self.backend_pid = -1
        self.backend_secret = -1
        self.parameter_status = dict()

        self.last_parse_prep_stmts = []
        self.debug = debug.flags.server_proto

        self.last_indirect_return = None

        self.log_listeners = []

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
            file=sys.stderr,
        )

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

    cdef inline str get_tenant_label(self):
        if self.tenant is None:
            return "system"
        else:
            return self.tenant.get_instance_name()

    cdef bint before_prepare(
        self,
        bytes stmt_name,
        int dbver,
        WriteBuffer outbuf,
    ):
        cdef bint parse = 1

        while self.prep_stmts.needs_cleanup():
            stmt_name_to_clean, _ = self.prep_stmts.cleanup_one()
            if self.debug:
                self.debug_print(f"discarding ps {stmt_name_to_clean!r}")
            outbuf.write_buffer(
                self.make_clean_stmt_message(stmt_name_to_clean))

        if stmt_name in self.prep_stmts:
            if self.prep_stmts[stmt_name] == dbver:
                parse = 0
            else:
                if self.debug:
                    self.debug_print(f"discarding ps {stmt_name!r}")
                outbuf.write_buffer(
                    self.make_clean_stmt_message(stmt_name))
                del self.prep_stmts[stmt_name]

        return parse

    cdef write_sync(self, WriteBuffer outbuf):
        outbuf.write_bytes(_SYNC_MESSAGE)
        self.waiting_for_sync += 1

    cdef send_sync(self):
        self.write(_SYNC_MESSAGE)
        self.waiting_for_sync += 1

    def _build_apply_state_req(self, bytes serstate, WriteBuffer out):
        cdef:
            WriteBuffer buf

        if self.debug:
            self.debug_print("Syncing state: ", serstate)

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
            metrics.backend_query_duration.observe(
                time.monotonic() - started_at, self.get_tenant_label()
            )
            await self.after_command()

    cdef send_query_unit_group(
        self, object query_unit_group, bint sync,
        object bind_datas, bytes state,
        ssize_t start, ssize_t end, int dbver, object parse_array,
        object query_prefix,
    ):
        # parse_array is an array of booleans for output with the same size as
        # the query_unit_group, indicating if each unit is freshly parsed
        cdef:
            WriteBuffer out
            WriteBuffer buf
            WriteBuffer bind_data
            bytes stmt_name
            ssize_t idx = start
            bytes sql
            tuple sqls

        out = WriteBuffer.new()
        parsed = set()

        if state is not None and start == 0:
            self._build_apply_state_req(state, out)

        # Build the parse_array first, closing statements if needed before
        # actually executing any command that may fail, in order to ensure
        # self.prep_stmts is always in sync with the actual open statements
        for query_unit in query_unit_group.units[start:end]:
            if query_unit.system_config:
                raise RuntimeError(
                    "CONFIGURE INSTANCE command is not allowed in scripts"
                )
            stmt_name = query_unit.sql_hash
            if stmt_name:
                # The same EdgeQL query may show up twice in the same script.
                # We just need to know and skip if we've already parsed the
                # same query within current send batch, because self.prep_stmts
                # will be updated before the next batch, with maybe a different
                # dbver after DDL.
                if stmt_name not in parsed and self.before_prepare(
                    stmt_name, dbver, out
                ):
                    parse_array[idx] = True
                    parsed.add(stmt_name)
            idx += 1
        idx = start

        for query_unit, bind_data in zip(
            query_unit_group.units[start:end], bind_datas):
            stmt_name = query_unit.sql_hash
            sql = query_unit.sql
            if query_prefix:
                sql = query_prefix + sql
            if stmt_name:
                if parse_array[idx]:
                    buf = WriteBuffer.new_message(b'P')
                    buf.write_bytestring(stmt_name)
                    buf.write_bytestring(sql)
                    buf.write_int16(0)
                    out.write_buffer(buf.end_message())
                    metrics.query_size.observe(
                        len(sql),
                        self.get_tenant_label(),
                        'compiled',
                    )

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
                buf = WriteBuffer.new_message(b'P')
                buf.write_bytestring(b'')  # statement name
                buf.write_bytestring(sql)
                buf.write_int16(0)
                out.write_buffer(buf.end_message())
                metrics.query_size.observe(
                    len(sql), self.get_tenant_label(), 'compiled'
                )

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

        if sync:
            self.write_sync(out)
        else:
            out.write_bytes(FLUSH_MESSAGE)

        self.write(out)

    async def force_error(self):
        self.before_command()

        # Send a bogus parse that will cause an error to be generated
        out = WriteBuffer.new()
        buf = WriteBuffer.new_message(b'P')
        buf.write_bytestring(b'')
        buf.write_bytestring(b'<INTERNAL ERROR IN GEL PGCON>')
        buf.write_int16(0)

        # Then do a sync to get everything executed and lined back up
        out.write_buffer(buf.end_message())
        self.write_sync(out)

        self.write(out)

        try:
            await self.wait_for_sync()
        except pgerror.BackendError as e:
            pass
        else:
            raise RuntimeError("Didn't get expected error!")
        finally:
            await self.after_command()

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

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

    async def _describe(
        self,
        query: bytes,
        param_type_oids: Optional[list[int]],
    ):
        cdef:
            WriteBuffer out

        out = WriteBuffer.new()

        buf = WriteBuffer.new_message(b"P")  # Parse
        buf.write_bytestring(b"")
        buf.write_bytestring(query)
        if param_type_oids:
            buf.write_int16(len(param_type_oids))
            for oid in param_type_oids:
                buf.write_int32(<int32_t>oid)
        else:
            buf.write_int16(0)
        out.write_buffer(buf.end_message())

        buf = WriteBuffer.new_message(b"D")  # Describe
        buf.write_byte(b"S")
        buf.write_bytestring(b"")
        out.write_buffer(buf.end_message())

        out.write_bytes(FLUSH_MESSAGE)

        self.write(out)

        param_desc = None
        result_desc = None

        try:
            buf = None
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'1':
                        # ParseComplete
                        self.buffer.discard_message()

                    elif mtype == b't':
                        # ParameterDescription
                        param_desc = self._decode_param_desc(self.buffer)

                    elif mtype == b'T':
                        # RowDescription
                        result_desc = self._decode_row_desc(self.buffer)
                        break

                    elif mtype == b'n':
                        # NoData
                        self.buffer.discard_message()
                        param_desc = []
                        result_desc = []
                        break

                    elif mtype == b'E':  ## result
                        # ErrorResponse
                        er_cls, er_fields = self.parse_error_message()
                        raise er_cls(fields=er_fields)

                    else:
                        self.fallthrough()

                finally:
                    self.buffer.finish_message()
        except Exception:
            self.send_sync()
            await self.wait_for_sync()
            raise

        if param_desc is None:
            raise RuntimeError(
                "did not receive ParameterDescription from backend "
                "in response to Describe"
            )

        if result_desc is None:
            raise RuntimeError(
                "did not receive RowDescription from backend "
                "in response to Describe"
            )

        return param_desc, result_desc

    def _decode_param_desc(self, buf: ReadBuffer):
        cdef:
            int16_t nparams
            uint32_t p_oid
            list result = []

        nparams = buf.read_int16()

        for _ in range(nparams):
            p_oid = <uint32_t>buf.read_int32()
            result.append(p_oid)

        return result

    def _decode_row_desc(self, buf: ReadBuffer):
        cdef:
            int16_t nfields

            bytes f_name
            uint32_t f_table_oid
            int16_t f_column_num
            uint32_t f_dt_oid
            int16_t f_dt_size
            int32_t f_dt_mod
            int16_t f_format

            list result

        nfields = buf.read_int16()

        result = []
        for _ in range(nfields):
            f_name = buf.read_null_str()
            f_table_oid = <uint32_t>buf.read_int32()
            f_column_num = buf.read_int16()
            f_dt_oid = <uint32_t>buf.read_int32()
            f_dt_size = buf.read_int16()
            f_dt_mod = buf.read_int32()
            f_format = buf.read_int16()

            result.append((f_name.decode("utf-8"), f_dt_oid))

        return result

    async def sql_describe(
        self,
        query: bytes,
        param_type_oids: Optional[list[int]] = None,
    ) -> tuple[list[int], list[tuple[str, int]]]:
        self.before_command()
        started_at = time.monotonic()
        try:
            return await self._describe(query, param_type_oids)
        finally:
            await self.after_command()

    async def _parse_execute(
        self,
        query,
        frontend.AbstractFrontendConnection fe_conn,
        WriteBuffer bind_data,
        bint use_prep_stmt,
        bytes state,
        int dbver,
        bint use_pending_func_cache,
        tx_isolation,
        list param_data_types,
        bytes query_prefix,
    ):
        cdef:
            WriteBuffer out
            WriteBuffer buf
            bytes stmt_name
            bytes sql
            tuple sqls
            bytes prologue_sql
            bytes epilogue_sql

            int32_t dat_len

            bint parse = 1
            bint state_sync = 0

            bint has_result = query.cardinality is not CARD_NO_RESULT
            bint discard_result = (
                fe_conn is not None and query.output_format == FMT_NONE)

            uint64_t msgs_num
            uint64_t msgs_executed = 0
            uint64_t i

        out = WriteBuffer.new()

        if state is not None:
            self._build_apply_state_req(state, out)
            if (
                query.tx_id
                or not query.is_transactional
                or query.run_and_rollback
                or tx_isolation is not None
            ):
                # This query has START TRANSACTION or non-transactional command
                # like CREATE DATABASE in it.
                # Restoring state must be performed in a separate
                # implicit transaction (otherwise START TRANSACTION DEFERRABLE
                # or CREATE DATABASE (since PG 14.7) would fail).
                # Hence - inject a SYNC after a state restore step.
                state_sync = 1
                self.write_sync(out)

        if query.run_and_rollback or tx_isolation is not None:
            if self.in_tx():
                sp_name = f'_edb_{time.monotonic_ns()}'
                prologue_sql = f'SAVEPOINT {sp_name}'.encode('utf-8')
            else:
                sp_name = None
                prologue_sql = b'START TRANSACTION'
                if tx_isolation is not None:
                    prologue_sql += (
                        f' ISOLATION LEVEL {tx_isolation._value_}'
                        .encode('utf-8')
                    )

            buf = WriteBuffer.new_message(b'P')
            buf.write_bytestring(b'')
            buf.write_bytestring(prologue_sql)
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

        if use_pending_func_cache and query.cache_func_call:
            sql, stmt_name = query.cache_func_call
            sqls = (query_prefix + sql,)
        else:
            sqls = (query_prefix + query.sql,) + query.db_op_trailer
            stmt_name = query.sql_hash

        msgs_num = <uint64_t>(len(sqls))

        if use_prep_stmt:
            parse = self.before_prepare(stmt_name, dbver, out)
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
                for sql in sqls:
                    pname = b'__p%d__' % i
                    self.last_parse_prep_stmts.append(pname)
                    buf = WriteBuffer.new_message(b'P')
                    buf.write_bytestring(pname)
                    buf.write_bytestring(sql)
                    buf.write_int16(0)
                    out.write_buffer(buf.end_message())
                    i += 1
                    metrics.query_size.observe(
                        len(sql), self.get_tenant_label(), 'compiled'
                    )
            else:
                if len(sqls) != 1:
                    raise errors.InternalServerError(
                        'cannot PARSE more than one SQL query '
                        'in non-anonymous mode')
                msgs_num = 1
                buf = WriteBuffer.new_message(b'P')
                buf.write_bytestring(stmt_name)
                buf.write_bytestring(sqls[0])
                if param_data_types:
                    buf.write_int16(len(param_data_types))
                    for oid in param_data_types:
                        buf.write_int32(<int32_t>oid)
                else:
                    buf.write_int16(0)
                out.write_buffer(buf.end_message())
                metrics.query_size.observe(
                    len(sqls[0]), self.get_tenant_label(), 'compiled'
                )

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

        if query.run_and_rollback or tx_isolation is not None:
            if query.run_and_rollback:
                if sp_name:
                    sql = f'ROLLBACK TO SAVEPOINT {sp_name}'.encode('utf-8')
                else:
                    sql = b'ROLLBACK'
            else:
                sql = b'COMMIT'

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
        elif query.append_tx_op:
            if query.tx_commit:
                sql = b'COMMIT'
            elif query.tx_rollback:
                sql = b'ROLLBACK'
            else:
                raise errors.InternalServerError(
                    "QueryUnit.append_tx_op is set but none of the "
                    "Query.tx_<foo> properties are"
                )

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

            if query.run_and_rollback or tx_isolation is not None:
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
                                f'query: {sqls}')

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
        WriteBuffer bind_data = NO_ARGS,
        list param_data_types = None,
        frontend.AbstractFrontendConnection fe_conn = None,
        bint use_prep_stmt = False,
        bytes state = None,
        int dbver = 0,
        bint use_pending_func_cache = 0,
        tx_isolation = None,
        query_prefix = None,
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
                use_pending_func_cache,
                tx_isolation,
                param_data_types,
                query_prefix or b'',
            )
        finally:
            metrics.backend_query_duration.observe(
                time.monotonic() - started_at, self.get_tenant_label()
            )
            await self.after_command()

    async def sql_fetch(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
        use_prep_stmt: bool = False,
        state: Optional[bytes] = None,
    ) -> list[tuple[bytes, ...]]:
        if use_prep_stmt:
            sql_digest = hashlib.sha1()
            sql_digest.update(sql)
            sql_hash = sql_digest.hexdigest().encode('latin1')
        else:
            sql_hash = None

        query = compiler.QueryUnit(
            sql=sql,
            sql_hash=sql_hash,
            status=b"",
        )

        return await self.parse_execute(
            query=query,
            bind_data=args_ser.combine_raw_args(args),
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
            metrics.backend_query_duration.observe(
                time.monotonic() - started_at, self.get_tenant_label()
            )
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
        actions,
        fe_conn: frontend.AbstractFrontendConnection,
        dbver: int,
        dbv: pg_ext.ConnectionView,
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
                )
            finally:
                if not dbv.in_tx():
                    self.last_state = dbv.serialize_state()
        finally:
            await self.after_command()

    def _write_sql_extended_query(
        self,
        actions,
        dbver: int,
        dbv: pg_ext.ConnectionView,
    ) -> bytes:
        cdef:
            WriteBuffer buf, msg_buf
            PGMessage action
            bint be_parse

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

            be_parse = True
            if action.action == PGAction.PARSE:
                sql_text, data = action.args
                if action.stmt_name in prepared:
                    action.frontend_only = True
                else:
                    if action.stmt_name:
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
                    metrics.query_size.observe(
                        len(sql_text), self.get_tenant_label(), 'compiled'
                    )
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
                        if be_stmt_name:
                            be_parse = self.before_prepare(
                                be_stmt_name, dbver, buf
                            )
                        if not be_parse:
                            if self.debug:
                                self.debug_print(
                                    'Parse cache hit', be_stmt_name)
                            action.frontend_only = True
                            prepared.add(be_stmt_name)

                if action.is_frontend_only():
                    pass
                elif isinstance(
                    action.query_unit.command_complete_tag, dbstate.TagUnpackRow
                ):
                    # in this case we are intercepting the only result row so
                    # we want to set its encoding to be binary
                    msg_buf = WriteBuffer.new_message(b'B')
                    msg_buf.write_bytestring(action.portal_name)
                    msg_buf.write_bytestring(action.stmt_name)

                    # skim over param format codes
                    param_formats = read_int16(action.args[0:2])
                    offset = 2 + param_formats * 2

                    # skim over param values
                    params = read_int16(action.args[offset:offset+2])
                    offset += 2
                    for p in range(params):
                        size = read_int32(action.args[offset:offset+4])
                        if size == -1:  # special case: NULL
                            size = 0
                        offset += 4 + size
                    msg_buf.write_bytes(action.args[0:offset])

                    # set the result formats
                    msg_buf.write_int16(1)  # number of columns
                    msg_buf.write_int16(1)  # binary encoding
                    buf.write_buffer(msg_buf.end_message())
                else:
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
                        if be_stmt_name:
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

                if action.is_frontend_only():
                    pass
                elif isinstance(
                    action.query_unit.command_complete_tag,
                    (dbstate.TagCountMessages, dbstate.TagUnpackRow),
                ):
                    # when executing TagUnpackRow, don't pass the limit through
                    msg_buf = WriteBuffer.new_message(b'E')
                    msg_buf.write_bytestring(action.portal_name)
                    msg_buf.write_int32(0)
                    buf.write_buffer(msg_buf.end_message())
                else:
                    # base case
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
        actions,
        fe_conn: frontend.AbstractFrontendConnection,
        dbver: int,
        dbv: pg_ext.ConnectionView,
    ) -> tuple[bool, bool]:
        cdef:
            WriteBuffer buf, msg_buf
            PGMessage action
            bint ignore_till_sync = False
            int32_t row_count

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
                        setting_name = action.query_unit.get_var

                        # RowDescription
                        msg_buf = WriteBuffer.new_message(b'T')
                        msg_buf.write_int16(1)  # number of fields
                        # field name
                        msg_buf.write_str(setting_name, "utf-8")
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
                        setting = dbv.current_fe_settings()[setting_name]
                        msg_buf.write_len_prefixed_utf8(
                            setting_to_sql(setting_name, setting)
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
                        assert isinstance(
                            action.query_unit.command_complete_tag,
                            dbstate.TagPlain,
                        ), "emulated SQL unit has no command_tag"
                        plain = action.query_unit.command_complete_tag
                        msg_buf.write_bytestring(plain.tag)
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

            row_count = 0
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
                    if action.stmt_name:
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
                    if (
                        mtype == b'T' and
                        isinstance(
                            action.query_unit.command_complete_tag,
                            dbstate.TagUnpackRow,
                        )
                    ):
                        # TagUnpackRow converts RowDescription into NoData
                        msg_buf = WriteBuffer.new_message(b'n')
                        buf.write_buffer(msg_buf.end_message())

                    elif not action.is_injected() and not (
                        mtype == b'n' and
                        action.action == PGAction.DESCRIBE_STMT_ROWS
                    ):
                        msg_buf = WriteBuffer.new_message(mtype)
                        msg_buf.write_bytes(data)
                        buf.write_buffer(msg_buf.end_message())
                    break

                elif (
                    mtype == b't'  # ParameterDescription
                    and action.action == PGAction.DESCRIBE_STMT_ROWS
                ):
                    self.buffer.consume_message()

                elif (
                    mtype == b't'  # ParameterDescription
                ):
                    # remap parameter descriptions

                    # The "external" parameters (that are visible to the user)
                    # don't include the internal params for globals and
                    # extracted constants.
                    # This chunk of code remaps the descriptions of internal
                    # params into external ones.
                    self.buffer.read_int16()  # count_internal
                    data_internal = self.buffer.consume_message()

                    msg_buf = WriteBuffer.new_message(b't')
                    external_params: int64_t = 0
                    if action.query_unit.params:
                        for index, param in enumerate(action.query_unit.params):
                            if not isinstance(param, dbstate.SQLParamExternal):
                                break
                            external_params = index + 1

                    msg_buf.write_int16(external_params)
                    msg_buf.write_bytes(data_internal[0:external_params * 4])

                    buf.write_buffer(msg_buf.end_message())

                elif (
                    mtype == b'T'  # RowDescription
                    and action.action == PGAction.EXECUTE
                    and isinstance(
                        action.query_unit.command_complete_tag,
                        dbstate.TagUnpackRow,
                    )
                ):
                    data = self.buffer.consume_message()

                    # tell the frontend connection that there is NoData
                    # because we intercept and unpack the DataRow.
                    msg_buf = WriteBuffer.new_message(b'n')
                    buf.write_buffer(msg_buf.end_message())
                elif (
                    mtype == b'D'  # DataRow
                    and action.action == PGAction.EXECUTE
                    and isinstance(
                        action.query_unit.command_complete_tag,
                        dbstate.TagUnpackRow,
                    )
                ):
                    # unpack a single row with a single column
                    data = self.buffer.consume_message()

                    field_size = read_int32(data[2:6])
                    val_bytes = data[6:6 + field_size]

                    row_count = int.from_bytes(val_bytes, "big", signed=True)
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
                        if be_stmt_name:
                            if self.debug:
                                self.debug_print(
                                    f"remembering ps {be_stmt_name}, "
                                    f"dbver {dbver}"
                                )
                            self.prep_stmts[be_stmt_name] = dbver

                    if (
                        not action.is_injected()
                        and action.query_unit.command_complete_tag
                    ):
                        tag = action.query_unit.command_complete_tag

                        msg_buf = WriteBuffer.new_message(mtype)
                        if isinstance(tag, dbstate.TagPlain):
                            msg_buf.write_bytestring(tag.tag)

                        elif isinstance(tag, (dbstate.TagCountMessages, dbstate.TagUnpackRow)):
                            msg_buf.write_bytes(bytes(tag.prefix, "utf-8"))

                            # This should return the number of modified rows by
                            # the top-level query, but we are returning the
                            # count of rows in the response. These two will
                            # always match because our compiled DML with always
                            # have a top-level SELECT with same number of rows
                            # as the DML stmt somewhere in the the CTEs.
                            msg_buf.write_str(str(row_count), "utf-8")

                        buf.write_buffer(msg_buf.end_message())

                    elif not action.is_injected():
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
                        self.debug_print('ERROR RESPONSE MSG')
                    fe_conn.on_error(action.query_unit)
                    dbv.on_error()
                    self._rewrite_sql_error_response(action, buf)
                    fe_conn.write(buf)
                    fe_conn.flush()
                    buf = WriteBuffer.new()
                    ignore_till_sync = True
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
                        messages_redirected = self.buffer.redirect_messages(
                            buf, mtype, 0
                        )

                        # DataRow
                        if mtype == b'D':
                            row_count += messages_redirected
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
        source_map: Optional[pg_codegen.SourceMap],
        offset: int = 0,
    ):
        if source_map:
            pos = int(pos_bytes.decode('utf8'))
            if offset > 0 or pos + offset > 0:
                pos += offset
            pos = source_map.translate(pos)
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
                    if action.query_unit is None:
                        source_map = None
                        offset = 0
                    else:
                        qu = action.query_unit
                        source_map = qu.source_map
                        offset = -qu.prefix_len
                    self._write_error_position(
                        msg_buf,
                        action.args[0],
                        self.buffer.read_null_str(),
                        source_map,
                        offset,
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
                            source_map = qu.prepare.source_map
                        else:
                            offset = 0
                            source_map = qu.source_map
                        offset -= qu.prefix_len
                    else:
                        query_text = b""
                        source_map = None
                        offset = 0

                    self._write_error_position(
                        msg_buf,
                        query_text,
                        self.buffer.read_null_str(),
                        source_map,
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

    def load_last_ddl_return(self, object query_unit):
        if query_unit.ddl_stmt_id:
            data = self.last_indirect_return
            if data:
                ret = json.loads(data)
                if ret['ddl_stmt_id'] != query_unit.ddl_stmt_id:
                    raise RuntimeError(
                        'unrecognized data notice after a DDL command: '
                        'data_stmt_id do not match: expected '
                        f'{query_unit.ddl_stmt_id!r}, got '
                        f'{ret["ddl_stmt_id"]!r}'
                    )
                return ret
            else:
                raise RuntimeError(
                    'missing the required data notice after a DDL command'
                )

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
        self.last_indirect_return = None

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
                metrics.backend_connection_aborted.inc(
                    1.0, self.get_tenant_label(), pgcode
                )

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
                elif event == 'ensure-database-not-used':
                    dbname = event_payload['dbname']
                    self.tenant.on_remote_database_quarantine(dbname)
                elif event == 'query-cache-changes':
                    dbname = event_payload['dbname']
                    keys = event_payload.get('keys')
                    self.tenant.on_remote_query_cache_change(dbname, keys=keys)
                else:
                    raise AssertionError(f'unexpected system event: {event!r}')

            return True

        elif mtype == b'N':
            # NoticeResponse
            _, fields = self.parse_error_message()
            severity = fields.get('V')
            message = fields.get('M')
            detail = fields.get('D')
            if (
                severity == "NOTICE"
                and message.startswith("edb:notice:indirect_return")
            ):
                self.last_indirect_return = detail
            elif self.log_listeners:
                for listener in self.log_listeners:
                    self.loop.call_soon(listener, severity, message)
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
        self.connected = True
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

# Settings that are enums or bools and should not be quoted.
# Can be retrived from PostgreSQL with:
#   SELECt name FROM pg_catalog.pg_settings WHERE vartype IN ('enum', 'bool');
cdef set ENUM_SETTINGS = {
    'allow_alter_system',
    'allow_in_place_tablespaces',
    'allow_system_table_mods',
    'archive_mode',
    'array_nulls',
    'autovacuum',
    'backslash_quote',
    'bytea_output',
    'check_function_bodies',
    'client_min_messages',
    'compute_query_id',
    'constraint_exclusion',
    'data_checksums',
    'data_sync_retry',
    'debug_assertions',
    'debug_logical_replication_streaming',
    'debug_parallel_query',
    'debug_pretty_print',
    'debug_print_parse',
    'debug_print_plan',
    'debug_print_rewritten',
    'default_toast_compression',
    'default_transaction_deferrable',
    'default_transaction_isolation',
    'default_transaction_read_only',
    'dynamic_shared_memory_type',
    'edb_stat_statements.save',
    'edb_stat_statements.track',
    'edb_stat_statements.track_planning',
    'edb_stat_statements.track_utility',
    'enable_async_append',
    'enable_bitmapscan',
    'enable_gathermerge',
    'enable_group_by_reordering',
    'enable_hashagg',
    'enable_hashjoin',
    'enable_incremental_sort',
    'enable_indexonlyscan',
    'enable_indexscan',
    'enable_material',
    'enable_memoize',
    'enable_mergejoin',
    'enable_nestloop',
    'enable_parallel_append',
    'enable_parallel_hash',
    'enable_partition_pruning',
    'enable_partitionwise_aggregate',
    'enable_partitionwise_join',
    'enable_presorted_aggregate',
    'enable_seqscan',
    'enable_sort',
    'enable_tidscan',
    'escape_string_warning',
    'event_triggers',
    'exit_on_error',
    'fsync',
    'full_page_writes',
    'geqo',
    'gss_accept_delegation',
    'hot_standby',
    'hot_standby_feedback',
    'huge_pages',
    'huge_pages_status',
    'icu_validation_level',
    'ignore_checksum_failure',
    'ignore_invalid_pages',
    'ignore_system_indexes',
    'in_hot_standby',
    'integer_datetimes',
    'intervalstyle',
    'jit',
    'jit_debugging_support',
    'jit_dump_bitcode',
    'jit_expressions',
    'jit_profiling_support',
    'jit_tuple_deforming',
    'krb_caseins_users',
    'lo_compat_privileges',
    'log_checkpoints',
    'log_connections',
    'log_disconnections',
    'log_duration',
    'log_error_verbosity',
    'log_executor_stats',
    'log_hostname',
    'log_lock_waits',
    'log_min_error_statement',
    'log_min_messages',
    'log_parser_stats',
    'log_planner_stats',
    'log_recovery_conflict_waits',
    'log_replication_commands',
    'log_statement',
    'log_statement_stats',
    'log_truncate_on_rotation',
    'logging_collector',
    'parallel_leader_participation',
    'password_encryption',
    'plan_cache_mode',
    'quote_all_identifiers',
    'recovery_init_sync_method',
    'recovery_prefetch',
    'recovery_target_action',
    'recovery_target_inclusive',
    'remove_temp_files_after_crash',
    'restart_after_crash',
    'row_security',
    'send_abort_for_crash',
    'send_abort_for_kill',
    'session_replication_role',
    'shared_memory_type',
    'ssl',
    'ssl_max_protocol_version',
    'ssl_min_protocol_version',
    'ssl_passphrase_command_supports_reload',
    'ssl_prefer_server_ciphers',
    'standard_conforming_strings',
    'stats_fetch_consistency',
    'summarize_wal',
    'sync_replication_slots',
    'synchronize_seqscans',
    'synchronous_commit',
    'syslog_facility',
    'syslog_sequence_numbers',
    'syslog_split_messages',
    'trace_connection_negotiation',
    'trace_notify',
    'trace_sort',
    'track_activities',
    'track_commit_timestamp',
    'track_counts',
    'track_functions',
    'track_io_timing',
    'track_wal_io_timing',
    'transaction_deferrable',
    'transaction_isolation',
    'transaction_read_only',
    'transform_null_equals',
    'update_process_title',
    'wal_compression',
    'wal_init_zero',
    'wal_level',
    'wal_log_hints',
    'wal_receiver_create_temp_slot',
    'wal_recycle',
    'wal_sync_method',
    'xmlbinary',
    'xmloption',
    'zero_damaged_pages',
}


cdef setting_to_sql(name, setting):
    is_enum = name.lower() in ENUM_SETTINGS

    assert typeutils.is_container(setting)
    return ', '.join(setting_val_to_sql(v, is_enum) for v in setting)


cdef inline str setting_val_to_sql(val: str | int | float, is_enum: bool):
    if isinstance(val, str):
        if is_enum:
            # special case: no quoting
            return val
        # quote as identifier
        return pg_qi(val)
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    raise NotImplementedError('cannot convert setting to SQL: ', val)


cdef inline int16_t read_int16(data: bytes):
    return int.from_bytes(data[0:2], "big", signed=True)

cdef inline int32_t read_int32(data: bytes):
    return int.from_bytes(data[0:4], "big", signed=True)
