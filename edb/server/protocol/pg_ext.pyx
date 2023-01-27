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


import codecs
import collections
import contextlib
import copy
import encodings.aliases
import logging
import hashlib
import os
import sys
from collections import deque

cimport cython
import immutables
from libc.stdint cimport int32_t, int16_t, uint32_t

from edb import errors
from edb.common import debug
from edb.pgsql.parser import exceptions as parser_errors
from edb.server import args as srvargs
from edb.server.compiler import dbstate
from edb.server.pgcon import errors as pgerror
from edb.server.pgcon.pgcon cimport PGAction, PGMessage
from edb.server.protocol cimport frontend

cdef object logger = logging.getLogger('edb.server')
cdef object DEFAULT_SETTINGS = immutables.Map()
cdef object DEFAULT_FE_SETTINGS = immutables.Map({"search_path": "public"})

encodings.aliases.aliases["sql_ascii"] = "ascii"


class ExtendedQueryError(Exception):
    pass


@contextlib.contextmanager
def managed_error():
    try:
        yield
    except Exception as e:
        raise ExtendedQueryError(e)


@cython.final
cdef class ConnectionView:
    def __init__(self):
        self._settings = DEFAULT_SETTINGS
        self._fe_settings = DEFAULT_FE_SETTINGS
        self._in_tx_explicit = False
        self._in_tx_implicit = False
        self._in_tx_settings = None
        self._in_tx_fe_settings = None
        self._in_tx_fe_local_settings = None
        self._in_tx_portals = {}
        self._in_tx_new_portals = set()
        self._in_tx_savepoints = collections.deque()
        self._tx_error = False

    def current_settings(self):
        if self.in_tx():
            return self._in_tx_settings or DEFAULT_SETTINGS
        else:
            return self._settings or DEFAULT_SETTINGS

    cpdef inline current_fe_settings(self):
        if self.in_tx():
            return self._in_tx_fe_local_settings or DEFAULT_FE_SETTINGS
        else:
            return self._fe_settings or DEFAULT_FE_SETTINGS

    cdef inline fe_transaction_state(self):
        return dbstate.SQLTransactionState(
            in_tx=self.in_tx(),
            settings=self._fe_settings,
            in_tx_settings=self._in_tx_fe_settings,
            in_tx_local_settings=self._in_tx_fe_local_settings,
            savepoints=[sp[:3] for sp in self._in_tx_savepoints],
        )

    cpdef inline bint in_tx(self):
        return self._in_tx_explicit or self._in_tx_implicit

    cdef inline _reset_tx_state(
        self, bint chain_implicit, bint chain_explicit
    ):
        # This method is a part of ending a transaction. COMMIT must be handled
        # before calling this method. If any of the chain_* flag is set, a new
        # transaction will be opened immediately after clean-up.
        self._in_tx_implicit = chain_implicit
        self._in_tx_explicit = chain_explicit
        self._in_tx_settings = self._settings if self.in_tx() else None
        self._in_tx_fe_settings = self._fe_settings if self.in_tx() else None
        self._in_tx_fe_local_settings = (
            self._fe_settings if self.in_tx() else None
        )
        self._in_tx_portals.clear()
        self._in_tx_new_portals.clear()
        self._in_tx_savepoints.clear()
        self._tx_error = False

    def start_implicit(self):
        if self._in_tx_implicit:
            raise RuntimeError("already in implicit transaction")
        else:
            if not self.in_tx():
                self._in_tx_settings = self._settings
                self._in_tx_fe_settings = self._fe_settings
                self._in_tx_fe_local_settings = self._fe_settings
            self._in_tx_implicit = True

    def end_implicit(self):
        if not self._in_tx_implicit:
            raise RuntimeError("not in implicit transaction")
        if self._in_tx_explicit:
            # There is an explicit transaction, nothing to do other than
            # turning off the implicit flag so that we can start_implicit again
            self._in_tx_implicit = False
        else:
            # Commit or rollback the implicit transaction
            if not self._tx_error:
                self._settings = self._in_tx_settings
                self._fe_settings = self._in_tx_fe_settings
            self._reset_tx_state(False, False)

    def on_success(self, unit: dbstate.SQLQueryUnit):
        # Handle ROLLBACK first before self._tx_error
        if unit.tx_action == dbstate.TxAction.ROLLBACK:
            if not self._in_tx_explicit:
                # TODO: warn about "no tx" but still rollback implicit
                pass
            self._reset_tx_state(self._in_tx_implicit, unit.tx_chain)

        elif unit.tx_action == dbstate.TxAction.ROLLBACK_TO_SAVEPOINT:
            if not self._in_tx_explicit:
                if self._in_tx_implicit:
                    self._tx_error = True
                raise errors.TransactionError(
                    "ROLLBACK TO SAVEPOINT can only be used "
                    "in transaction blocks"
                )
            while self._in_tx_savepoints:
                (
                    sp_name,
                    fe_settings,
                    fe_local_settings,
                    settings,
                    new_portals,
                ) = self._in_tx_savepoints[-1]
                for name in new_portals:
                    self._in_tx_portals.pop(name, None)
                if sp_name == unit.sp_name:
                    new_portals.clear()
                    self._in_tx_settings = settings
                    self._in_tx_fe_settings = fe_settings
                    self._in_tx_fe_local_settings = fe_local_settings
                    self._in_tx_new_portals = new_portals
                    break
                else:
                    self._in_tx_savepoints.pop()
            else:
                self._tx_error = True
                raise errors.TransactionError(
                    f'savepoint "{unit.sp_name}" does not exist'
                )

        elif self._tx_error:
            raise errors.TransactionError(
                "current transaction is aborted, "
                "commands ignored until end of transaction block"
            )

        elif unit.tx_action == dbstate.TxAction.START:
            if self._in_tx_explicit:
                # TODO: warning: there is already a transaction in progress
                pass
            else:
                if not self.in_tx():
                    self._in_tx_settings = self._settings
                    self._in_tx_fe_settings = self._fe_settings
                    self._in_tx_fe_local_settings = self._fe_settings
                self._in_tx_explicit = True

        elif unit.tx_action == dbstate.TxAction.COMMIT:
            if not self._in_tx_explicit:
                # TODO: warning: there is no transaction in progress
                # but we still commit implicit transactions if any
                pass
            if self.in_tx():
                self._settings = self._in_tx_settings
                self._fe_settings = self._in_tx_fe_settings
            self._reset_tx_state(self._in_tx_implicit, unit.tx_chain)

        elif unit.tx_action == dbstate.TxAction.DECLARE_SAVEPOINT:
            if not self._in_tx_explicit:
                raise errors.TransactionError(
                    "SAVEPOINT can only be used in transaction blocks"
                )
            self._in_tx_new_portals = set()
            self._in_tx_savepoints.append((
                unit.sp_name,
                self._in_tx_fe_settings,
                self._in_tx_fe_local_settings,
                self._in_tx_settings,
                self._in_tx_new_portals,
            ))

        elif unit.tx_action == dbstate.TxAction.RELEASE_SAVEPOINT:
            pass

        if unit.set_vars:
            # only session settings here
            if unit.set_vars == {None: None}:  # RESET ALL
                if self.in_tx():
                    self._in_tx_settings = DEFAULT_SETTINGS
                    self._in_tx_fe_settings = DEFAULT_FE_SETTINGS
                    self._in_tx_fe_local_settings = DEFAULT_FE_SETTINGS
                else:
                    self._settings = DEFAULT_SETTINGS
                    self._fe_settings = DEFAULT_FE_SETTINGS
            else:
                if self.in_tx():
                    if unit.frontend_only:
                        if unit.is_local:
                            settings = self._in_tx_fe_local_settings.mutate()
                            for k, v in unit.set_vars.items():
                                if v is None:
                                    if k in DEFAULT_FE_SETTINGS:
                                        settings[k] = DEFAULT_FE_SETTINGS[k]
                                    else:
                                        settings.pop(k, None)
                                else:
                                    settings[k] = v
                            self._in_tx_fe_local_settings = settings.finish()
                        settings = self._in_tx_fe_settings.mutate()
                    else:
                        settings = self._in_tx_settings.mutate()
                elif not unit.is_local:
                    if unit.frontend_only:
                        settings = self._fe_settings.mutate()
                    else:
                        settings = self._settings.mutate()
                else:
                    return
                for k, v in unit.set_vars.items():
                    if v is None:
                        if unit.frontend_only and k in DEFAULT_FE_SETTINGS:
                            settings[k] = DEFAULT_FE_SETTINGS[k]
                        else:
                            settings.pop(k, None)
                    else:
                        settings[k] = v
                if self.in_tx():
                    if unit.frontend_only:
                        self._in_tx_fe_settings = settings.finish()
                    else:
                        self._in_tx_settings = settings.finish()
                else:
                    if unit.frontend_only:
                        self._fe_settings = settings.finish()
                    else:
                        self._settings = settings.finish()

    def on_error(self):
        self._tx_error = True

    cpdef inline close_portal(self, str name):
        try:
            return self._in_tx_portals.pop(name)
        except KeyError:
            raise pgerror.new(
                pgerror.ERROR_INVALID_CURSOR_NAME,
                f"cursor \"{name}\" does not exist",
            ) from None

    def create_portal(self, str name, query_unit):
        if not self.in_tx():
            raise RuntimeError(
                "portals cannot be created outside a transaction"
            )
        if name and name in self._in_tx_portals:
            raise pgerror.new(
                pgerror.ERROR_DUPLICATE_CURSOR,
                f"cursor \"{name}\" already exists",
            )
        self._in_tx_portals[name] = query_unit

    cdef inline find_portal(self, str name):
        try:
            return self._in_tx_portals[name]
        except KeyError:
            raise pgerror.new(
                pgerror.ERROR_INVALID_CURSOR_NAME,
                f"cursor \"{name}\" does not exist",
            ) from None


cdef class PgConnection(frontend.FrontendConnection):
    def __init__(self, server, sslctx, endpoint_security, **kwargs):
        super().__init__(server, **kwargs)
        self._dbview = ConnectionView()
        self._id = str(<int32_t><uint32_t>(int(self._id) % (2 ** 32)))
        self.prepared_stmts = {}
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

    cdef is_in_tx(self):
        return self._dbview.in_tx()

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
        if "client_encoding" in params:
            encoding = params["client_encoding"]
            client_encoding = encodings.normalize_encoding(encoding).upper()
            try:
                codecs.lookup(client_encoding)
            except LookupError:
                raise pgerror.new(
                    pgerror.ERROR_INVALID_PARAMETER_VALUE,
                    f'invalid value for parameter "client_encoding": "{encoding}"',
                )
            self._dbview._settings = self._dbview._settings.set(
                "client_encoding", client_encoding
            )
        else:
            client_encoding = "UTF8"

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
                if name == "client_encoding":
                    msg_buf.write_str(client_encoding, "utf-8")
                else:
                    msg_buf.write_str(value, "utf-8")
                msg_buf.end_message()
                buf.write_buffer(msg_buf)
                if self.debug:
                    self.debug_print(f"ParameterStatus: {name}={value}")
            self.write(buf)
            # Try to sync the settings, especially client_encoding.
            # sql_simple_query() will return a ReadyForQuery and flush the buf.
            await conn.sql_simple_query(
                [], self, self.database.dbver, self._dbview
            )
        finally:
            self.maybe_release_pgcon(conn)

    cdef inline WriteBuffer ready_for_query(self):
        cdef WriteBuffer msg_buf
        self.ignore_till_sync = False
        msg_buf = WriteBuffer.new_message(b'Z')
        if self._dbview.in_tx():
            if self._dbview._tx_error:
                msg_buf.write_byte(b'E')
            else:
                msg_buf.write_byte(b'T')
        else:
            msg_buf.write_byte(b'I')
        return msg_buf.end_message()

    async def main_step(self, char mtype):
        cdef WriteBuffer buf

        if self.debug:
            self.debug_print("main_step", mtype)

        if mtype == b'S':  # Sync
            self.buffer.finish_message()
            if self.debug:
                self.debug_print("Sync")
            if self._dbview._in_tx_implicit:
                actions = [PGMessage(PGAction.SYNC)]
                conn = await self.get_pgcon()
                try:
                    success = await conn.sql_extended_query(
                        actions, self, self.database.dbver, self._dbview)
                    self.ignore_till_sync = not success
                finally:
                    self.maybe_release_pgcon(conn)
            else:
                self.ignore_till_sync = False
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
            query_str = self.buffer.read_null_str().decode("utf8")
            self.buffer.finish_message()
            if self.debug:
                self.debug_print("Query", query_str)
            try:
                # Emulate Postgres to close the anonymous stmt/portal
                # once the Q message is taken
                self.prepared_stmts.pop("", None)
                try:
                    self._dbview.close_portal("")
                except pgerror.BackendError:
                    pass
                query_units = await self.compile(query_str, self._dbview)
            except Exception as ex:
                self.write_error(ex)
                self.write(self.ready_for_query())
                self.flush()

            else:
                conn = await self.get_pgcon()
                try:
                    await conn.sql_simple_query(
                        query_units, self, self.database.dbver, self._dbview
                    )
                    self.ignore_till_sync = False
                finally:
                    self.maybe_release_pgcon(conn)

        elif (
            mtype == b'P' or mtype == b'B' or mtype == b'D' or mtype == b'E' or
            # One of Parse, Bind, Describe or Execute starts an extended query
            mtype == b'C'  # or Close
        ):
            try:
                actions = await self.extended_query()
            except ExtendedQueryError as ex:
                self.write_error(ex.args[0])
                self.flush()
                self.ignore_till_sync = True
            else:
                conn = await self.get_pgcon()
                try:
                    success = await conn.sql_extended_query(
                        actions, self, self.database.dbver, self._dbview)
                    self.ignore_till_sync = not success
                finally:
                    self.maybe_release_pgcon(conn)

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
            bint in_implicit
            PGMessage parse_action
            ConnectionView dbv

        dbv = copy.deepcopy(self._dbview)
        actions = deque()
        fresh_stmts = set()
        in_implicit = self._dbview._in_tx_implicit

        # Here we will exhaust the buffer and queue up actions for the backend.
        # Any error in this step will be handled in the outer main_step() -
        # the error will be returned, any remaining messages in the buffer will
        # be discarded until a Sync message is found (ignore_till_sync).
        # This also means no partial action is executed in the backend for now.
        while self.buffer.take_message():
            if not in_implicit:
                actions.append(PGMessage(PGAction.START_IMPLICIT))
                in_implicit = True
                with managed_error():
                    dbv.start_implicit()
            mtype = self.buffer.get_message_type()

            if mtype == b'P':  # Parse
                stmt_name = self.buffer.read_null_str().decode("utf8")
                query_bytes = self.buffer.read_null_str()
                query_str = query_bytes.decode("utf8")
                data = self.buffer.consume_message()
                if self.debug:
                    self.debug_print("Parse", repr(stmt_name), query_str, data)

                with managed_error():
                    if stmt_name and stmt_name in self.prepared_stmts:
                        raise pgerror.new(
                            pgerror.ERROR_DUPLICATE_PREPARED_STATEMENT,
                            f"prepared statement \"{stmt_name}\" already "
                            f"exists",
                        )

                    fe_settings = dbv.current_fe_settings()
                    query_units = await self.compile(query_str, dbv)
                    if len(query_units) > 1:
                        raise pgerror.new(
                            pgerror.ERROR_SYNTAX_ERROR,
                            "cannot insert multiple commands into a prepared "
                            "statement",
                        )
                    unit = query_units[0]
                    sql_text = unit.query.encode("utf-8")
                    parse_hash = hashlib.sha1(sql_text)
                    parse_hash.update(data)
                    parse_hash = b'p' + parse_hash.hexdigest().encode("latin1")
                    actions.append(
                        PGMessage(
                            PGAction.PARSE,
                            stmt_name=parse_hash,
                            args=(sql_text, data, True),
                            query_unit=unit,
                        )
                    )
                    self.prepared_stmts[stmt_name] = PGMessage(
                        PGAction.PARSE,
                        stmt_name=parse_hash,
                        args=(sql_text, data, False),
                        query_unit=unit,
                        orig_query=query_str,
                        fe_settings=fe_settings,
                    )
                    fresh_stmts.add(stmt_name)

            elif mtype == b'B':  # Bind
                portal_name = self.buffer.read_null_str().decode("utf8")
                stmt_name = self.buffer.read_null_str().decode("utf8")
                data = self.buffer.consume_message()
                if self.debug:
                    self.debug_print(
                        "Bind", repr(portal_name), repr(stmt_name), data
                    )

                with managed_error():
                    if stmt_name not in self.prepared_stmts:
                        raise pgerror.new(
                            pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                            f"prepared statement \"{stmt_name}\" does not "
                            f"exist",
                        )

                    # Replay Parse if it wasn't done in this extended_query()
                    # call
                    parse_action = self.prepared_stmts[stmt_name]
                    if stmt_name not in fresh_stmts:

                        # HACK: some of the statically compiler-evaluated
                        # queries like `current_schema` depend on the
                        # fe_settings, we need to re-compile if the fe_settings
                        # mismatch.
                        fe_settings = dbv.current_fe_settings()
                        if parse_action.fe_settings is not fe_settings:
                            query_units = await self.compile(
                                parse_action.orig_query, dbv
                            )
                            if len(query_units) > 1:
                                raise pgerror.new(
                                    pgerror.ERROR_SYNTAX_ERROR,
                                    "cannot insert multiple commands into a "
                                    "prepared statement",
                                )
                            unit = query_units[0]
                            sql_text = unit.query.encode("utf-8")
                            parse_hash = hashlib.sha1(sql_text)
                            parse_hash.update(parse_action.args[1])
                            parse_hash = b'p' + parse_hash.hexdigest().encode(
                                "latin1"
                            )
                            parse_action = PGMessage(
                                PGAction.PARSE,
                                stmt_name=parse_hash,
                                args=(sql_text, parse_action.args[1], False),
                                query_unit=unit,
                                orig_query=parse_action.orig_query,
                                fe_settings=fe_settings,
                            )
                            self.prepared_stmts[stmt_name] = parse_action

                        actions.append(parse_action)
                        fresh_stmts.add(stmt_name)
                    actions.append(
                        PGMessage(
                            PGAction.BIND,
                            stmt_name=parse_action.stmt_name,
                            portal_name=portal_name,
                            args=data,
                            query_unit=parse_action.query_unit,
                        )
                    )
                    dbv.create_portal(portal_name, parse_action.query_unit)

            elif mtype == b'D':  # Describe
                kind = self.buffer.read_byte()
                name = self.buffer.read_null_str().decode("utf8")
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Describe", kind, repr(name))

                with managed_error():
                    if kind == b'S':  # prepared statement
                        if name not in self.prepared_stmts:
                            raise pgerror.new(
                                pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                                f"prepared statement \"{name}\" does not "
                                f"exist",
                            )
                        parse_action = self.prepared_stmts[name]
                        # Replay Parse if it wasn't done
                        # in this extended_query() call
                        if name not in fresh_stmts:
                            fresh_stmts.add(name)
                            actions.append(parse_action)
                        actions.append(
                            PGMessage(
                                PGAction.DESCRIBE_STMT,
                                stmt_name=parse_action.stmt_name,
                                query_unit=parse_action.query_unit,
                            )
                        )

                    elif kind == b'P':  # portal
                        actions.append(
                            PGMessage(
                                PGAction.DESCRIBE_PORTAL,
                                portal_name=name,
                                query_unit=dbv.find_portal(name),
                            )
                        )

                    else:
                        raise pgerror.ProtocolViolation(
                            "invalid Describe kind"
                        )

            elif mtype == b'E':  # Execute
                portal_name = self.buffer.read_null_str().decode("utf8")
                max_rows = self.buffer.read_int32()
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Execute", repr(portal_name), max_rows)

                with managed_error():
                    unit = dbv.find_portal(portal_name)
                    actions.append(
                        PGMessage(
                            PGAction.EXECUTE,
                            portal_name=portal_name,
                            args=max_rows,
                            query_unit=unit,
                        )
                    )
                    dbv.on_success(unit)

            elif mtype == b'C':  # Close
                kind = self.buffer.read_byte()
                name = self.buffer.read_null_str().decode("utf8")
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Close", kind, repr(name))

                with managed_error():
                    if kind == b'S':  # prepared statement
                        if name not in self.prepared_stmts:
                            raise pgerror.new(
                                pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                                f"prepared statement \"{name}\" does not "
                                f"exist",
                            )
                        # The prepared statement in the backend is managed by
                        # the LRU cache in pgcon.pyx, we don't close it here
                        fresh_stmts.discard(name)
                        self.prepared_stmts.pop(name)
                        actions.append(PGMessage(PGAction.CLOSE_STMT))

                    elif kind == b'P':  # portal
                        actions.append(
                            PGMessage(
                                PGAction.CLOSE_PORTAL,
                                portal_name=name,
                                query_unit=dbv.close_portal(name),
                            )
                        )

                    else:
                        raise pgerror.ProtocolViolation("invalid Close kind")

            elif mtype == b'H':  # Flush
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Flush")
                actions.append(PGMessage(PGAction.FLUSH))

            elif mtype == b'S':  # Sync
                in_implicit = False
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Sync")
                with managed_error():
                    actions.append(PGMessage(PGAction.SYNC))
                    dbv.end_implicit()
                break

            else:
                # Other messages would cut off the current extended_query()
                break

        if self.debug:
            self.debug_print("extended_query", actions)
        return actions

    async def compile(self, query_str, ConnectionView dbv):
        if self.debug:
            self.debug_print("Compile", query_str)
        fe_settings = dbv.current_fe_settings()
        key = (hashlib.sha1(query_str.encode("utf-8")).digest(), fe_settings)
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
            dbv.fe_transaction_state(),
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
