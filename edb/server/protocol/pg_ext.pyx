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
from collections import deque

from libc.stdint cimport int32_t, int16_t, uint32_t

from edb import errors
from edb.common import debug
from edb.pgsql.parser import exceptions as parser_errors
from edb.server import args as srvargs
from edb.server.pgcon import errors as pgerror
from edb.server.pgcon.pgcon cimport PGAction
from edb.server.protocol cimport frontend

cdef object logger = logging.getLogger('edb.server')


cdef class PgConnection(frontend.FrontendConnection):
    def __init__(self, server, sslctx, endpoint_security, **kwargs):
        super().__init__(server, **kwargs)
        self._id = str(<int32_t><uint32_t>(int(self._id) % (2 ** 32)))
        self.prepared_stmts = {}
        self.portals = {}
        self.ignore_till_sync = False

        self.sslctx = sslctx
        self.endpoint_security = endpoint_security
        self.is_tls = False

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
        elif isinstance(exc, errors.AuthenticationError):
            exc = pgerror.InvalidAuthSpec(str(exc), severity="FATAL")
        elif isinstance(exc, errors.BinaryProtocolError):
            exc = pgerror.ProtocolViolation(
                str(exc), detail=exc.details, severity="FATAL"
            )
        elif isinstance(exc, errors.UnsupportedFeatureError):
            exc = pgerror.FeatureNotSupported(str(exc))
        elif isinstance(exc, errors.EdgeDBError):
            args = dict(hint=exc.hint, detail=exc.details)
            if exc.line >= 0:
                args['L'] = str(exc.line)
            if exc.position >= 0:
                args['P'] = str(exc.position)
            exc = pgerror.new(
                exc.pgext_code or pgerror.ERROR_INTERNAL_ERROR,
                str(exc),
                **args,
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
                    pid = str(self.buffer.read_int32())
                    secret = self.buffer.read_bytes(4)
                    self.buffer.finish_message()

                    if self.debug:
                        self.debug_print("CancelRequest", pid, secret)
                    self.server.cancel_pgext_connection(pid, secret)
                    self.stop()
                    break

                elif proto_ver_minor == 5679:  # SSLRequest
                    if self.debug:
                        self.debug_print("SSLRequest")
                    if not first:
                        raise pgerror.ProtocolViolation(
                            "found multiple SSLRequest", severity="FATAL"
                        )

                    self.buffer.finish_message()
                    if self._transport is None:
                        raise ConnectionAbortedError
                    if self.debug:
                        self.debug_print("S for SSLRequest")
                    self._transport.write(b'S')
                    # complete the next client message with a mocked type
                    self.buffer.feed_data(b'\xff')
                    self._transport = await self.loop.start_tls(
                        self._transport,
                        self,
                        self.sslctx,
                        server_side=True,
                    )
                    self.is_tls = True

                elif proto_ver_minor == 5680:  # GSSENCRequest
                    raise pgerror.FeatureNotSupported(
                        "GSSENCRequest is not supported", severity="FATAL"
                    )

                else:
                    raise pgerror.FeatureNotSupported(severity="FATAL")

            elif proto_ver_major == 3 and proto_ver_minor == 0:
                # StartupMessage with 3.0 protocol
                if self.debug:
                    self.debug_print("StartupMessage")
                if (
                    not self.is_tls and self.endpoint_security ==
                    srvargs.ServerEndpointSecurityMode.Tls
                ):
                    raise pgerror.InvalidAuthSpec(
                        "TLS required due to server endpoint security",
                        severity="FATAL",
                    )

                await self._handle_startup_message()
                break

            else:
                raise pgerror.ProtocolViolation(
                    "invalid protocol version", severity="FATAL"
                )

    def cancel(self, secret):
        if (
            self.secret == secret and
            self._pinned_pgcon is not None and
            not self._pinned_pgcon.idle and
            self.server._accept_new_tasks
        ):
            self.server.create_task(
                self.server._cancel_pgcon_operation(self._pinned_pgcon),
                interruptable=False,
            )

    def debug_print(self, *args):
        print("::PGEXT::", f"id:{self._id}", *args, file=sys.stderr)

    cdef WriteBuffer _make_authentication_sasl_initial(self, list methods):
        cdef WriteBuffer msg_buf
        msg_buf = WriteBuffer.new_message(b'R')
        msg_buf.write_int32(10)
        for method in methods:
            msg_buf.write_bytestring(method)
        msg_buf.write_byte(b'\0')
        msg_buf.end_message()
        if self.debug:
            self.debug_print("AuthenticationSASL:", *methods)
        return msg_buf

    cdef _expect_sasl_initial_response(self):
        mtype = self.buffer.get_message_type()
        if mtype != b'p':
            raise pgerror.ProtocolViolation(
                f'expected SASL response, got message type {mtype}')
        selected_mech = self.buffer.read_null_str()
        try:
            client_first = self.buffer.read_len_prefixed_bytes()
        except BufferError:
            client_first = None
        self.buffer.finish_message()
        if self.debug:
            self.debug_print(
                "SASLInitialResponse:",
                selected_mech,
                len(client_first) if client_first else client_first,
            )
        if not client_first:
            # The client didn't send the Client Initial Response
            # in SASLInitialResponse, this is an error.
            raise pgerror.ProtocolViolation(
                'client did not send the Client Initial Response '
                'data in SASLInitialResponse')
        return selected_mech, client_first

    cdef WriteBuffer _make_authentication_sasl_msg(
        self, bytes data, bint final
    ):
        cdef WriteBuffer msg_buf
        msg_buf = WriteBuffer.new_message(b'R')
        if final:
            msg_buf.write_int32(12)
        else:
            msg_buf.write_int32(11)
        msg_buf.write_bytes(data)
        msg_buf.end_message()
        if self.debug:
            self.debug_print(
                "AuthenticationSASLFinal" if final
                else "AuthenticationSASLContinue",
                len(data),
            )
        return msg_buf

    cdef bytes _expect_sasl_response(self):
        mtype = self.buffer.get_message_type()
        if mtype != b'p':
            raise pgerror.ProtocolViolation(
                f'expected SASL response, got message type {mtype}')
        client_final = self.buffer.consume_message()
        if self.debug:
            self.debug_print("SASLResponse", len(client_final))
        return client_final

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
                "StartupMessage must have a \"user\"", severity="FATAL"
            )
        self.buffer.finish_message()

        user = params["user"]
        database = params.get("database", user)
        self.client_encoding = params.get("client_encoding", "utf8")
        logger.debug('received pg connection request by %s to database %s',
                     user, database)

        if not self.server.is_database_connectable(database):
            raise pgerror.InvalidAuthSpec(
                f'database {database!r} does not accept connections',
                severity="FATAL",
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
        self.ignore_till_sync = False
        msg_buf = WriteBuffer.new_message(b'Z')
        msg_buf.write_byte(b'I')
        return msg_buf.end_message()

    async def main_step(self, char mtype):
        cdef WriteBuffer buf

        if mtype == b'S':  # Sync
            self.buffer.finish_message()
            self.write(self.ready_for_query())
            self.flush()

        elif mtype == b'X':  # Terminate
            self.buffer.finish_message()
            if self.debug:
                self.debug_print("Terminate")
            self.close()
            return True

        elif self.ignore_till_sync:
            self.buffer.discard_message()

        elif mtype == b'Q':  # Query
            try:
                query_str = self.buffer.read_null_str().decode(
                    self.client_encoding
                )
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Query", query_str)

                sql_source = await self.compile(query_str)
                if not sql_source:
                    sql_source = ['']

            except Exception as ex:
                self.write_error(ex)
                self.write(self.ready_for_query())
                self.flush()

            else:
                conn = await self.get_pgcon()
                try:
                    self.prepared_stmts.pop("", None)
                    self.portals.pop("", None)

                    await conn.sql_simple_query(
                        sql_source, self, self.database.dbver
                    )
                    self.ignore_till_sync = False
                finally:
                    self.maybe_release_pgcon(conn)

        elif mtype == b'P' or mtype == b'B' or mtype == b'D' or mtype == b'E':
            # One of Parse, Bind, Describe or Execute starts an extended query
            try:
                actions = await self.extended_query()
            except Exception as ex:
                self.write_error(ex)
                self.flush()
                self.ignore_till_sync = True
            else:
                conn = await self.get_pgcon()
                try:
                    success = await conn.sql_extended_query(
                        actions, self, self.database.dbver)
                    self.ignore_till_sync = not success
                finally:
                    self.maybe_release_pgcon(conn)

        elif mtype == b'C':  # Close
            kind = self.buffer.read_byte()
            name = self.buffer.read_null_str().decode(self.client_encoding)
            self.buffer.finish_message()
            if self.debug:
                self.debug_print("Close", kind, repr(name))

            try:
                if kind == b'S':  # prepared statement
                    if name not in self.prepared_stmts:
                        raise pgerror.new(
                            pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                            f"prepared statement \"{name}\" does not exist",
                        )
                    # The prepared statement in the backend is managed by the
                    # LRU cache in pgcon.pyx, we don't close it here
                    self.prepared_stmts.pop(name)

                elif kind == b'P':  # portal
                    if name not in self.portals:
                        raise pgerror.new(
                            pgerror.ERROR_INVALID_CURSOR_NAME,
                            f"cursor \"{name}\" does not exist",
                        )
                    # No portal lives outside transactions, so we just clear
                    # the replay cache here
                    del self.portals[name]

                else:
                    raise pgerror.ProtocolViolation("invalid Close kind")

            except Exception as ex:
                self.write_error(ex)
                self.flush()
                self.ignore_till_sync = True

            else:
                buf = WriteBuffer.new_message(b'3')  # CloseComplete
                self.write(buf.end_message())

        elif mtype == b'H':  # Flush
            self.buffer.finish_message()
            if self.debug:
                self.debug_print("Flush")
            self.flush()

        else:
            if self.debug:
                self.debug_print(
                    "MESSAGE", chr(mtype), self.buffer.consume_message()
                )
            raise pgerror.FeatureNotSupported()

    async def extended_query(self):
        cdef:
            WriteBuffer buf
            int16_t i
            bytes data

        actions = deque()
        fresh_stmts = set()
        fresh_portals = set()
        executed_portals = set()
        suspended_portals = set()

        # Here we will exhaust the buffer and queue up actions for the backend.
        # Any error in this step will be handled in the outer main_step() -
        # the error will be returned, any remaining messages in the buffer will
        # be discarded until a Sync message is found (ignore_till_sync).
        # This also means no partial action is executed in the backend for now.
        while self.buffer.take_message():
            mtype = self.buffer.get_message_type()

            if mtype == b'P':  # Parse
                stmt_name = self.buffer.read_null_str().decode(
                    self.client_encoding
                )
                query_bytes = self.buffer.read_null_str()
                query_str = query_bytes.decode(self.client_encoding)
                data = self.buffer.consume_message()
                if self.debug:
                    self.debug_print("Parse", repr(stmt_name), query_str, data)

                if stmt_name and stmt_name in self.prepared_stmts:
                    raise pgerror.new(
                        pgerror.ERROR_DUPLICATE_PREPARED_STATEMENT,
                        f"prepared statement \"{stmt_name}\" already exists",
                    )

                sql_source = await self.compile(query_str)
                if len(sql_source) > 1:
                    raise pgerror.new(
                        pgerror.ERROR_SYNTAX_ERROR,
                        "cannot insert multiple commands into a prepared "
                        "statement",
                    )
                if sql_source:
                    sql_text = sql_source[0].encode("utf-8")
                else:
                    # Cluvio will try to execute an empty query
                    sql_text = b''
                parse_hash = hashlib.sha1(sql_text)
                parse_hash.update(data)
                parse_hash = b'p' + parse_hash.hexdigest().encode("latin1")
                action = (PGAction.PARSE, parse_hash, sql_text, data)
                actions.append(action + (True,))
                self.prepared_stmts[stmt_name] = parse_hash, action + (False,)
                fresh_stmts.add(stmt_name)

            elif mtype == b'B':  # Bind
                portal_name = self.buffer.read_null_str().decode(
                    self.client_encoding
                )
                stmt_name = self.buffer.read_null_str().decode(
                    self.client_encoding
                )
                data = self.buffer.consume_message()
                if self.debug:
                    self.debug_print(
                        "Bind", repr(portal_name), repr(stmt_name), data
                    )

                if portal_name:
                    if portal_name in self.portals:
                        raise pgerror.new(
                            pgerror.ERROR_DUPLICATE_CURSOR,
                            f"cursor \"{portal_name}\" already exists",
                        )
                else:
                    # previous unnamed portal is closed
                    executed_portals.discard(portal_name)
                    suspended_portals.discard(portal_name)
                if stmt_name not in self.prepared_stmts:
                    raise pgerror.new(
                        pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                        f"prepared statement \"{stmt_name}\" does not exist",
                    )

                # Replay Parse if it wasn't done in this extended_query() call
                parse_hash, parse_action = self.prepared_stmts[stmt_name]
                if stmt_name not in fresh_stmts:
                    actions.append(parse_action)
                    fresh_stmts.add(stmt_name)
                name_out = portal_name.encode("utf-8")
                if name_out:
                    name_out = b'u' + name_out
                action = (PGAction.BIND, name_out, parse_hash, data)
                actions.append(action + (True,))
                self.portals[portal_name] = (
                    stmt_name, parse_action, action + (False,)
                )
                fresh_portals.add(portal_name)

            elif mtype == b'D':  # Describe
                kind = self.buffer.read_byte()
                name = self.buffer.read_null_str().decode(self.client_encoding)
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Describe", kind, repr(name))

                if kind == b'S':  # prepared statement
                    if name not in self.prepared_stmts:
                        raise pgerror.new(
                            pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                            f"prepared statement \"{name}\" does not exist",
                        )
                    name_out, parse_action = self.prepared_stmts[name]
                    # Replay Parse if it wasn't done
                    # in this extended_query() call
                    if name not in fresh_stmts:
                        fresh_stmts.add(name)
                        actions.append(parse_action)

                elif kind == b'P':  # portal
                    if name not in self.portals:
                        raise pgerror.new(
                            pgerror.ERROR_INVALID_CURSOR_NAME,
                            f"cursor \"{name}\" does not exist",
                        )
                    if name not in fresh_portals:
                        # Replay Parse and/or Bind if they weren't done
                        # in this extended_query() call
                        (
                            stmt_name, parse_action, bind_action
                        ) = self.portals[name]
                        if stmt_name not in fresh_stmts:
                            fresh_stmts.add(stmt_name)
                            actions.append(parse_action)
                        fresh_portals.add(name)
                        actions.append(bind_action)
                    name_out = name.encode('utf-8')
                    if name_out:
                        name_out = b'u' + name_out

                else:
                    raise pgerror.ProtocolViolation("invalid Describe kind")

                actions.append((PGAction.DESCRIBE, kind, name_out))

            elif mtype == b'E':  # Execute
                portal_name = self.buffer.read_null_str().decode(
                    self.client_encoding
                )
                max_rows = self.buffer.read_int32()
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Execute", repr(portal_name), max_rows)

                if max_rows > 0:
                    suspended_portals.add(portal_name)
                if portal_name not in self.portals:
                    raise pgerror.new(
                        pgerror.ERROR_INVALID_CURSOR_NAME,
                        f"cursor \"{portal_name}\" does not exist",
                    )
                if portal_name not in fresh_portals:
                    # Replay Parse and/or Bind if they weren't done
                    # in this extended_query() call
                    (
                        stmt_name, parse_action, bind_action
                    ) = self.portals[portal_name]
                    if stmt_name not in fresh_stmts:
                        fresh_stmts.add(stmt_name)
                        actions.append(parse_action)
                    fresh_portals.add(portal_name)
                    actions.append(bind_action)

                executed_portals.add(portal_name)
                name_out = portal_name.encode('utf-8')
                if name_out:
                    name_out = b'u' + name_out
                actions.append((PGAction.EXECUTE, name_out, max_rows))

            elif mtype == b'C':  # Close
                kind = self.buffer.read_byte()
                name = self.buffer.read_null_str().decode(self.client_encoding)
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Close", kind, repr(name))

                if kind == b'S':  # prepared statement
                    if name not in self.prepared_stmts:
                        raise pgerror.new(
                            pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                            f"prepared statement \"{name}\" does not exist",
                        )
                    # The prepared statement in the backend is managed by the
                    # LRU cache in pgcon.pyx, we don't close it here
                    fresh_stmts.discard(name)
                    self.prepared_stmts.pop(name)
                    actions.append((PGAction.CLOSE_STMT,))

                elif kind == b'P':  # portal
                    if name not in self.portals:
                        raise pgerror.new(
                            pgerror.ERROR_INVALID_CURSOR_NAME,
                            f"cursor \"{name}\" does not exist",
                        )
                    fresh_portals.discard(name)
                    executed_portals.discard(name)
                    suspended_portals.discard(name)
                    del self.portals[name]
                    name_out = name.encode('utf-8')
                    if name_out:
                        name_out = b'u' + name_out
                    actions.append((PGAction.CLOSE_PORTAL, name_out))

                else:
                    raise pgerror.ProtocolViolation("invalid Close kind")

            elif mtype == b'H':  # Flush
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Flush")
                actions.append((PGAction.FLUSH,))

            elif mtype == b'S':  # Sync
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Sync")

                # Without explicit transaction support, a Sync means the end of
                # the implicit transaction. Thus, drop all the portals.
                self.portals.clear()
                actions.append((PGAction.SYNC, True))
                break

            else:
                # Other messages would cut off the current extended_query()
                break

        # When this pipelined packet ends, we must SYNC the backend pgcon
        # because it will be released before handling the next client message.
        # In this case, executed portals will be closed if not closed yet.
        if actions[-1][0] != PGAction.SYNC:
            for portal_name in executed_portals:
                self.portals.pop(portal_name, None)
            actions.append((PGAction.SYNC, False))
            if suspended_portals:
                raise pgerror.FeatureNotSupported(
                    "suspended cursor is not supported"
                )

        if self.debug:
            self.debug_print("extended_query", actions)
        return actions

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


def new_pg_connection(server, sslctx, endpoint_security):
    return PgConnection(
        server,
        sslctx,
        endpoint_security,
        passive=False,
        transport=srvargs.ServerConnTransport.TCP_PG,
        external_auth=False,
    )
