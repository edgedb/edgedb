#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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


import logging
import hashlib
import os
import sys

from libc.stdint cimport int32_t, int16_t, uint32_t

from edb import errors
from edb.common import debug
from edb.pgsql.parser import exceptions as parser_errors
from edb.server import args as srvargs
from edb.server.pgcon import errors as pgerror
from edb.server.protocol cimport frontend

cdef object logger = logging.getLogger('edb.server')


cdef class PgConnection(frontend.FrontendConnection):
    def __init__(self, server, **kwargs):
        super().__init__(server, **kwargs)
        self._id = str(<int32_t><uint32_t>(int(self._id) % (2 ** 32)))

    cdef _main_task_created(self):
        self.server.on_pgext_client_connected(self)
        # complete the client initial message with a mocked type
        self.buffer.feed_data(b'\xff')

    def connection_lost(self, exc):
        self.server.on_pgext_client_disconnected(self)
        super().connection_lost(exc)

    cdef write_error(self, exc):
        cdef WriteBuffer buf

        if self.debug and not isinstance(exc, errors.BackendUnavailableError):
            self.debug_print('EXCEPTION', type(exc).__name__, exc)
            from edb.common.markup import dump
            dump(exc)

        if debug.flags.server and not isinstance(
            exc, errors.BackendUnavailableError
        ):
            self.loop.call_exception_handler({
                'message': (
                    'an error in edgedb protocol'
                ),
                'exception': exc,
                'protocol': self,
                'transport': self._transport,
            })

        message = str(exc)

        buf = WriteBuffer.new_message(b'E')

        if isinstance(exc, pgerror.BackendError):
            pass
        elif isinstance(exc, parser_errors.PSqlUnsupportedError):
            exc = pgerror.FeatureNotSupported(str(exc))
        elif isinstance(exc, parser_errors.PSqlParseError):
            exc = pgerror.new(
                pgerror.ERROR_SYNTAX_ERROR,
                str(exc),
                L=str(exc.lineno),
                P=str(exc.cursorpos),
            )
        elif isinstance(exc, errors.EdgeDBError):
            exc = pgerror.new(
                pgerror.ERROR_INTERNAL_ERROR,
                str(exc),
                hint=exc.hint,
                detail=exc.details,
            )
        else:
            exc = pgerror.new(
                pgerror.ERROR_INTERNAL_ERROR,
                str(exc),
                severity="FATAL",
            )

        for k, v in exc.fields.items():
            buf.write_byte(ord(k))
            buf.write_str(v, "utf-8")
        buf.write_byte(b'\0')

        self.write(buf.end_message())

    async def authenticate(self):
        cdef int16_t proto_ver_major, proto_ver_minor

        for first in (True, False):
            if not self.buffer.take_message():
                await self.wait_for_message(report_idling=True)

            proto_ver_major = self.buffer.read_int16()
            proto_ver_minor = self.buffer.read_int16()
            if proto_ver_major == 1234:
                if proto_ver_minor == 5678:  # CancelRequest
                    if self.debug:
                        self.debug_print("CancelRequest")
                    raise pgerror.FeatureNotSupported(
                        "CancelRequest is not supported"
                    )

                elif proto_ver_minor == 5679:  # SSLRequest
                    if self.debug:
                        self.debug_print("SSLRequest")
                    if not first:
                        raise pgerror.ProtocolViolation(
                            "found multiple SSLRequest"
                        )

                    self.buffer.finish_message()
                    if self._transport is None:
                        raise ConnectionAbortedError
                    if self.debug:
                        self.debug_print("N for SSLRequest")
                    self._transport.write(b'N')
                    # complete the next client message with a mocked type
                    self.buffer.feed_data(b'\xff')

                elif proto_ver_minor == 5680:  # GSSENCRequest
                    raise pgerror.FeatureNotSupported(
                        "GSSENCRequest is not supported"
                    )

                else:
                    raise pgerror.FeatureNotSupported()

            elif proto_ver_major == 3 and proto_ver_minor == 0:
                # StartupMessage with 3.0 protocol
                if self.debug:
                    self.debug_print("StartupMessage")
                await self._handle_startup_message()
                break

            else:
                raise pgerror.ProtocolViolation("invalid protocol version")

    def debug_print(self, *args):
        print("::PGEXT::", f"id:{self._id}", *args, file=sys.stderr)

    async def _handle_startup_message(self):
        cdef:
            WriteBuffer msg_buf
            WriteBuffer buf

        params = {}
        while True:
            name = self.buffer.read_null_str()
            if not name:
                break
            value = self.buffer.read_null_str()
            params[name.decode("utf-8")] = value.decode("utf-8")
        if self.debug:
            self.debug_print("StartupMessage params:", params)
        if "user" not in params:
            raise pgerror.ProtocolViolation(
                "StartupMessage must have a \"user\""
            )
        self.buffer.finish_message()

        user = params["user"]
        database = params.get("database", user)
        self.client_encoding = params.get("client_encoding", "utf8")
        logger.debug('received pg connection request by %s to database %s',
                     user, database)

        if not self.server.is_database_connectable(database):
            raise pgerror.InvalidAuthSpec(
                f'database {database!r} does not accept connections'
            )

        self.database = self.server.get_db(dbname=database)
        await self.database.introspection()

        await self._authenticate(user, database, params)

        buf = WriteBuffer()

        msg_buf = WriteBuffer.new_message(b'R')
        msg_buf.write_int32(0)
        msg_buf.end_message()
        buf.write_buffer(msg_buf)
        if self.debug:
            self.debug_print("AuthenticationOk")

        self.secret = os.urandom(4)
        msg_buf = WriteBuffer.new_message(b'K')
        msg_buf.write_int32(int(self._id))
        msg_buf.write_bytes(self.secret)
        msg_buf.end_message()
        buf.write_buffer(msg_buf)
        if self.debug:
            self.debug_print("BackendKeyData")

        conn = await self.get_pgcon()
        try:
            for name, value in conn.parameter_status.items():
                msg_buf = WriteBuffer.new_message(b'S')
                msg_buf.write_str(name, "utf-8")
                msg_buf.write_str(value, "utf-8")
                msg_buf.end_message()
                buf.write_buffer(msg_buf)
                if self.debug:
                    self.debug_print(f"ParameterStatus: {name}={value}")
        finally:
            self.maybe_release_pgcon(conn)

        buf.write_buffer(self.ready_for_query())
        if self.debug:
            self.debug_print("ReadyForQuery")

        self.write(buf)
        self.flush()

    cdef inline WriteBuffer ready_for_query(self):
        cdef WriteBuffer msg_buf
        msg_buf = WriteBuffer.new_message(b'Z')
        msg_buf.write_byte(b'I')
        return msg_buf.end_message()

    async def main_step(self, char mtype):
        if mtype == b'Q':
            try:
                query_str = self.buffer.read_null_str().decode(
                    self.client_encoding
                )
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Query", query_str)

                sql_source = await self.compile(query_str)
            except Exception as ex:
                self.write_error(ex)
                self.write(self.ready_for_query())
                self.flush()
                return

            conn = await self.get_pgcon()
            try:
                await conn.sql_simple_query(
                    sql_source, self, self.database.dbver
                )
            finally:
                self.maybe_release_pgcon(conn)

        else:
            if self.debug:
                self.debug_print(
                    "MESSAGE", chr(mtype), self.buffer.consume_message()
                )
            raise pgerror.FeatureNotSupported()

    async def compile(self, query_str):
        if self.debug:
            self.debug_print("Compile", query_str)
        key = hashlib.sha1(query_str.encode("utf-8")).digest()
        result = self.database.lookup_compiled_sql(key)
        if result is not None:
            return result
        compiler_pool = self.server.get_compiler_pool()
        result = await compiler_pool.compile_sql(
            self.dbname,
            self.database.user_schema,
            self.database._index._global_schema,
            self.database.reflection_cache,
            self.database.db_config,
            self.database._index.get_compilation_system_config(),
            query_str,
        )
        self.database.cache_compiled_sql(key, result)
        if self.debug:
            self.debug_print("Compile result", result)
        return result


def new_pg_connection(server):
    return PgConnection(
        server,
        passive=False,
        transport=srvargs.ServerConnTransport.TCP_PG,
        external_auth=False,
    )
