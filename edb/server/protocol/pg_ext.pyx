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
import json
import os
import sys
import time
import uuid
from collections import deque

cimport cython
import immutables
from libc.stdint cimport int32_t, int16_t, uint32_t

from edb import errors
from edb.common import debug
from edb.common.log import current_tenant
from edb.pgsql.parser import exceptions as parser_errors
import edb.pgsql.parser.parser as pg_parser
cimport edb.pgsql.parser.parser as pg_parser
from edb.server import args as srvargs
from edb.server import defines, metrics
from edb.server import tenant as edbtenant
from edb.server.compiler import dbstate
from edb.server.pgcon import errors as pgerror
from edb.server.pgcon.pgcon cimport PGAction, PGMessage, setting_to_sql
from edb.server.protocol cimport frontend

DEFAULT_SETTINGS = dbstate.DEFAULT_SQL_SETTINGS
DEFAULT_FE_SETTINGS = dbstate.DEFAULT_SQL_FE_SETTINGS

cdef object logger = logging.getLogger('edb.server')
cdef object DEFAULT_STATE = json.dumps(dict(DEFAULT_SETTINGS)).encode('utf-8')

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
cdef class PreparedStmt:

    def __init__(self, PGMessage parse_action, pg_parser.Source source):
        self.parse_action = parse_action
        self.source = source



@cython.final
cdef class ConnectionView:
    def __init__(self):
        self._in_tx_explicit = False
        self._in_tx_implicit = False

        # Kepp track of backend settings so that we can sync to use different
        # backend connections (pgcon) within the same frontend connection,
        # see serialize_state() below and its usages in pgcon.pyx.
        self._settings = DEFAULT_SETTINGS
        self._in_tx_settings = None

        # Frontend-only settings are defined by the high-level compiler, and
        # tracked only here, syncing between the compiler process,
        # see current_fe_settings(), fe_transaction_state() and usages below.
        self._fe_settings = DEFAULT_FE_SETTINGS
        self._in_tx_fe_settings = None
        self._in_tx_fe_local_settings = None

        self._in_tx_portals = {}
        self._in_tx_new_portals = set()
        self._in_tx_savepoints = collections.deque()
        self._tx_error = False
        self._session_state_db_cache = (DEFAULT_SETTINGS, DEFAULT_STATE)

    cpdef inline current_fe_settings(self):
        if self.in_tx():
            # For easier access, _in_tx_fe_local_settings is always a superset
            # of _in_tx_fe_settings; _in_tx_fe_settings only keeps track of
            # non-local settings, so that the local settings don't go across
            # transaction boundaries; this must be consistent with dbstate.py.
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
        self._in_tx_fe_local_settings = self._in_tx_fe_settings
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
                        if not unit.is_local:
                            settings = self._in_tx_fe_settings.mutate()
                            for k, v in unit.set_vars.items():
                                if v is None:
                                    if k in DEFAULT_FE_SETTINGS:
                                        settings[k] = DEFAULT_FE_SETTINGS[k]
                                    else:
                                        settings.pop(k, None)
                                else:
                                    settings[k] = v
                            self._in_tx_fe_settings = settings.finish()
                        settings = self._in_tx_fe_local_settings.mutate()
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
                        self._in_tx_fe_local_settings = settings.finish()
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

    cpdef inline close_portal_if_exists(self, str name):
        return self._in_tx_portals.pop(name, None)

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

    cdef inline portal_exists(self, str name):
        return name in self._in_tx_portals

    def serialize_state(self):
        if self.in_tx():
            raise errors.InternalServerError(
                'no need to serialize state while in transaction')
        if self._settings == DEFAULT_SETTINGS:
            return DEFAULT_STATE

        if self._session_state_db_cache is not None:
            if self._session_state_db_cache[0] == self._settings:
                return self._session_state_db_cache[1]

        rv = json.dumps({
            key: setting_to_sql(key, val) for key, val in self._settings.items()
        }).encode("utf-8")
        self._session_state_db_cache = (self._settings, rv)
        return rv


cdef class PgConnection(frontend.FrontendConnection):
    interface = "sql"

    def __init__(self, server, sslctx, endpoint_security, **kwargs):
        super().__init__(server, None, **kwargs)
        self._dbview = ConnectionView()
        self._id = str(<int32_t><uint32_t>(int(self._id) % (2 ** 32)))
        self.prepared_stmts = {}  # via extended query Parse
        self.sql_prepared_stmts = {}  # via a PREPARE statement
        self.sql_prepared_stmts_map = {}
        # Tracks prepared statements of operations
        # on *other* prepared statements.
        self.wrapping_prepared_stmts = {}
        self.ignore_till_sync = False

        self.sslctx = sslctx
        self.endpoint_security = endpoint_security
        self.is_tls = False

        self._disable_cache = debug.flags.disable_qcache

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
            if exc.code_is(pgerror.ERROR_SERIALIZATION_FAILURE):
                metrics.transaction_serialization_errors.inc(
                    1.0, self.get_tenant_label()
                )
        elif isinstance(exc, parser_errors.PSqlUnsupportedError):
            exc = pgerror.FeatureNotSupported(str(exc))
        elif isinstance(exc, parser_errors.PSqlSyntaxError):
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
            exc = pgerror.new(
                pgerror.ERROR_FEATURE_NOT_SUPPORTED,
                str(exc),
                L=str(exc.line) if exc.line >= 0 else None,
                P=str(exc.position + 1) if exc.position >= 0 else None,
            )
        elif isinstance(exc, errors.EdgeDBError):
            args = dict(hint=exc.hint, detail=exc.details)
            if exc.line >= 0:
                args['L'] = str(exc.line)
            if exc.position >= 0:
                # pg uses 1 based indexes for showing errors.
                args['P'] = str(exc.position + 1)
            exc = pgerror.new(
                exc.pgext_code or pgerror.ERROR_INTERNAL_ERROR,
                str(exc),
                **args,
            )
            if isinstance(exc, errors.TransactionSerializationError):
                metrics.transaction_serialization_errors.inc(
                    1.0, self.get_tenant_label()
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

    async def _handshake(self):
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
                    self.request_stop()
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
                    tenant = self.server.retrieve_tenant(
                        self._transport.get_extra_info("ssl_object")
                    )
                    if tenant is edbtenant.host_tenant:
                        tenant = None
                    self.tenant = tenant
                    if self.tenant is not None:
                        current_tenant.set(self.tenant.get_instance_name())
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

                await super()._handshake()
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
            self.tenant.accept_new_tasks
        ):
            self.tenant.create_task(
                self.tenant.cancel_pgcon_operation(self._pinned_pgcon),
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

    def check_readiness(self):
        if self.tenant.is_blocked():
            readiness_reason = self.tenant.get_readiness_reason()
            msg = "the server is not accepting requests"
            if readiness_reason:
                msg = f"{msg}: {readiness_reason}"
            raise pgerror.CannotConnectNowError(msg)
        elif not self.tenant.is_online():
            readiness_reason = self.tenant.get_readiness_reason()
            msg = "the server is going offline"
            if readiness_reason:
                msg = f"{msg}: {readiness_reason}"
            raise pgerror.CannotConnectNowError(msg)

    async def authenticate(self):
        cdef:
            WriteBuffer msg_buf
            WriteBuffer buf

        self.check_readiness()

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
                "client_encoding", (client_encoding,)
            )
        else:
            client_encoding = "UTF8"

        logger.debug('received pg connection request by %s to database %s',
                     user, database)

        if database == '__default__':
            database = self.tenant.default_database
        elif (
            database == defines.EDGEDB_OLD_DEFAULT_DB
            and self.tenant.maybe_get_db(
                dbname=defines.EDGEDB_OLD_DEFAULT_DB
            ) is None
        ):
            database = self.tenant.default_database

        user = self.tenant.resolve_user_name(user)

        await self._authenticate(user, database, params)

        logger.debug('successfully authenticated %s in database %s',
                     user, database)

        if not self.tenant.is_database_connectable(database):
            raise pgerror.InvalidAuthSpec(
                f'database {database!r} does not accept connections',
                severity="FATAL",
            )

        self.database = self.tenant.get_db(dbname=database)
        await self.database.introspection()

        self.dbname = database
        self.username = user

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

        async with self.with_pgcon() as conn:
            for name, value in conn.parameter_status.items():
                msg_buf = WriteBuffer.new_message(b'S')
                msg_buf.write_str(name, "utf-8")
                if name == "client_encoding":
                    value = client_encoding
                elif name == "server_version":
                    value = str(defines.PGEXT_POSTGRES_VERSION)
                elif name == "session_authorization":
                    value = user
                elif name == "application_name":
                    value = self.tenant.get_instance_name()
                msg_buf.write_str(value, "utf-8")
                msg_buf.end_message()
                buf.write_buffer(msg_buf)
                if self.debug:
                    self.debug_print(f"ParameterStatus: {name}={value}")
            self.write(buf)
            # Try to sync the settings, especially client_encoding.
            await conn.sql_apply_state(self._dbview)

        self.write(self.ready_for_query())
        self.flush()

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

    def on_success(self, query_unit):
        cdef:
            PreparedStmt stmt

        if query_unit.deallocate is not None:
            stmt_name = query_unit.deallocate.stmt_name
            self.sql_prepared_stmts.pop(stmt_name, None)
            self.sql_prepared_stmts_map.pop(stmt_name, None)
            self.prepared_stmts.pop(stmt_name, None)
            # If any wrapping prepared statements referred to this
            # prepared statement, invalidate them.
            for wrapping_ps in self.wrapping_prepared_stmts.pop(stmt_name, []):
                stmt = self.prepared_stmts.get(wrapping_ps)
                if stmt is not None:
                    stmt.parse_action.invalidate()

    def on_error(self, query_unit):
        cdef:
            PreparedStmt stmt

        if query_unit.prepare is not None:
            stmt_name = query_unit.prepare.stmt_name
            self.sql_prepared_stmts.pop(stmt_name, None)
            self.sql_prepared_stmts_map.pop(stmt_name, None)
            self.prepared_stmts.pop(stmt_name, None)
            # If any wrapping prepared statements referred to this
            # prepared statement, invalidate them.
            for wrapping_ps in self.wrapping_prepared_stmts.pop(stmt_name, []):
                stmt = self.prepared_stmts.get(wrapping_ps)
                if stmt is not None:
                    stmt.parse_action.invalidate()

    async def main_step(self, char mtype):
        try:
            await self._main_step(mtype)
        except pgerror.BackendError as ex:
            self.write_error(ex)
            self.write(self.ready_for_query())
            self.flush()
            self.request_stop()

    async def _main_step(self, char mtype):
        cdef:
            WriteBuffer buf
            ConnectionView dbv

        dbv = self._dbview

        self.check_readiness()

        if self.debug:
            self.debug_print("main_step", repr(chr(mtype)))
            if self.ignore_till_sync:
                self.debug_print("ignoring")

        if mtype == b'S':  # Sync
            self.buffer.finish_message()
            if self.debug:
                self.debug_print("Sync")
            if dbv._in_tx_implicit:
                actions = [PGMessage(PGAction.SYNC)]
                async with self.with_pgcon() as conn:
                    success, _ = await conn.sql_extended_query(
                        actions, self, self.database.dbver, dbv)
                    self.ignore_till_sync = not success
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
            try:
                query = self.buffer.read_null_str()
                metrics.query_size.observe(
                    len(query), self.get_tenant_label(), 'sql'
                )
                query_str = query.decode("utf8")
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Query", query_str)
                actions = await self.simple_query(query_str)
                del query_str, query
            except Exception as ex:
                self.write_error(ex)
                self.write(self.ready_for_query())
                self.flush()

            else:
                async with self.with_pgcon() as conn:
                    try:
                        _, rq_sent = await conn.sql_extended_query(
                            actions,
                            self,
                            self.database.dbver,
                            dbv,
                        )
                    except Exception as ex:
                        self.write_error(ex)
                        self.write(self.ready_for_query())
                    else:
                        if not rq_sent:
                            self.write(self.ready_for_query())

                self.flush()

        elif (
            mtype == b'P' or mtype == b'B' or mtype == b'D' or mtype == b'E' or
            # One of Parse, Bind, Describe or Execute starts an extended query
            mtype == b'C'  # or Close
        ):
            try:
                actions, exception = await self.extended_query()
            except ExtendedQueryError as ex:
                actions = ()
                exception = ex
            else:
                async with self.with_pgcon() as conn:
                    try:
                        success, _ = await conn.sql_extended_query(
                            actions, self, self.database.dbver, dbv)
                        self.ignore_till_sync = not success
                    except Exception as ex:
                        self.write_error(ex)
                        self.flush()
                        self.ignore_till_sync = True
            if exception:
                self.write_error(exception.args[0])
                self.flush()
                self.ignore_till_sync = True

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

    async def simple_query(self, query_str: str) -> list[PGMessage]:
        cdef:
            PreparedStmt stmt

        actions = []
        dbv = self._dbview

        source = pg_parser.NormalizedSource.from_string(query_str)
        query_units = await self.compile(source, dbv)

        # TODO: currently, normalization does not work with multiple queries
        # so we must re-run the compilation with non-normalized query.
        # Ideally we could detect this before compilation.
        if len(query_units) > 1:
            source = pg_parser.Source.from_string(query_str)
            query_units = await self.compile(source, dbv)

        already_in_implicit_tx = dbv._in_tx_implicit
        metrics.sql_queries.inc(
            len(query_units), self.tenant.get_instance_name()
        )
        self._query_count += len(query_units)

        if not already_in_implicit_tx:
            actions.append(PGMessage(PGAction.START_IMPLICIT_TX))

        for qu in query_units:
            if qu.execute is not None:
                fe_settings = dbv.current_fe_settings()
                known_be_name = (
                    self.sql_prepared_stmts_map.get(qu.execute.stmt_name))
                recompile = (
                    qu.fe_settings != fe_settings
                    or qu.execute.be_stmt_name != known_be_name.encode("utf-8")
                )
                actions.extend(await self._ensure_nested_ps_exists(
                    dbv, qu, force_recompilation=recompile))
            else:
                recompile = False
            if recompile:
                stmt, new_stmts = await self._parse_statement(
                    stmt_name=None,
                    query_str=qu.orig_query,
                    parse_data=b"\x00\x00",
                    dbv=dbv,
                    force_recompilation=True,
                    injected_action=True,
                )
            else:
                stmt, new_stmts = await self._parse_unit(
                    stmt_name=None,
                    unit=qu,
                    source=source,
                    parse_data=b"\x00\x00",
                    dbv=dbv,
                    injected_action=True,
                )
            parse_unit = stmt.parse_action.query_unit
            actions.append(stmt.parse_action)

            # 2 bytes: number of format codes (1)
            # 2 bytes: first format code (1) is binary
            #          (this implies that all args are binary)
            # 2 bytes: number of arguments (0)
            # 2 bytes: number of result format codes (0)
            #          (this implies that )
            bind_data = b"\x00\x01\x00\x01\x00\x00\x00\x00"
            # remap argumnets, which will inject globals
            bind_data = remap_arguments(
                bind_data,
                parse_unit.params,
                dbv.current_fe_settings(),
                source,
            )
            actions.append(
                PGMessage(
                    PGAction.BIND,
                    portal_name="",
                    stmt_name=parse_unit.stmt_name,
                    args=bind_data,
                    query_unit=parse_unit,
                    injected=True,
                )
            )

            actions.append(
                PGMessage(
                    PGAction.DESCRIBE_STMT_ROWS,
                    stmt_name=parse_unit.stmt_name,
                    query_unit=parse_unit,
                )
            )
            actions.append(
                PGMessage(
                    PGAction.EXECUTE,
                    args=0,
                    portal_name="",
                    query_unit=parse_unit,
                    injected=False,
                )
            )
            actions.append(
                PGMessage(
                    PGAction.CLOSE_PORTAL,
                    portal_name="",
                    query_unit=parse_unit,
                    injected=True,
                )
            )

        actions.append(PGMessage(PGAction.SYNC))

        return actions

    async def extended_query(self):
        cdef:
            WriteBuffer buf
            int16_t i
            bytes data
            bint in_implicit
            PreparedStmt stmt
            ConnectionView dbv

        # Extended-query pre-plays on a deeply-cloned temporary dbview so as to
        # compose the actions list with correct states; the actual changes to
        # dbview is applied in pgcon.pyx when the actions are actually executed
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
                actions.append(PGMessage(PGAction.START_IMPLICIT_TX))
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
                metrics.query_size.observe(
                    len(query_bytes), self.get_tenant_label(), 'sql'
                )

                with managed_error():
                    if (
                        stmt_name and (
                            stmt_name in self.prepared_stmts
                            or stmt_name in self.sql_prepared_stmts
                        )
                    ):
                        raise pgerror.new(
                            pgerror.ERROR_DUPLICATE_PREPARED_STATEMENT,
                            f"prepared statement \"{stmt_name}\" already "
                            f"exists",
                        )

                    stmt, new_stmts = await self._parse_statement(
                        stmt_name, query_str, data, dbv
                    )
                    if stmt.parse_action.query_unit.execute is not None:
                        actions.extend(
                            await self._ensure_nested_ps_exists(
                                dbv,
                                stmt.parse_action.query_unit,
                            )
                        )
                    fresh_stmts.update(new_stmts)
                    actions.append(stmt.parse_action)

            elif mtype == b'B':  # Bind
                portal_name = self.buffer.read_null_str().decode("utf8")
                stmt_name = self.buffer.read_null_str().decode("utf8")
                data = self.buffer.consume_message()
                if self.debug:
                    self.debug_print(
                        "Bind", repr(portal_name), repr(stmt_name), data
                    )

                with managed_error():
                    stmt = await self._ensure_ps_locality(
                        dbv,
                        stmt_name,
                        fresh_stmts,
                        actions,
                    )

                    try:
                        params = stmt.parse_action.query_unit.params
                        fe_settings = dbv.current_fe_settings()
                        data = remap_arguments(
                            data, params, fe_settings, stmt.source
                        )
                    except Exception as e:
                        # we return here instead of raising the exception
                        # because we want to also return the previous actions
                        return actions, ExtendedQueryError(e)

                    actions.append(
                        PGMessage(
                            PGAction.BIND,
                            stmt_name=stmt.parse_action.stmt_name,
                            portal_name=portal_name,
                            args=data,
                            query_unit=stmt.parse_action.query_unit,
                        )
                    )
                    dbv.create_portal(portal_name, stmt.parse_action.query_unit)

            elif mtype == b'D':  # Describe
                kind = self.buffer.read_byte()
                name = self.buffer.read_null_str().decode("utf8")
                self.buffer.finish_message()
                if self.debug:
                    self.debug_print("Describe", kind, repr(name))

                with managed_error():
                    if kind == b'S':  # prepared statement
                        stmt = await self._ensure_ps_locality(
                            dbv,
                            name,
                            fresh_stmts,
                            actions,
                        )
                        actions.append(
                            PGMessage(
                                PGAction.DESCRIBE_STMT,
                                stmt_name=stmt.parse_action.stmt_name,
                                query_unit=stmt.parse_action.query_unit,
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

                metrics.sql_queries.inc(1.0, self.tenant.get_instance_name())
                self._query_count += 1
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
                name_bytes = self.buffer.read_null_str()
                name = name_bytes.decode("utf8")
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
                        actions.append(
                            PGMessage(
                                PGAction.CLOSE_STMT,
                                stmt_name=name_bytes,
                            ),
                        )

                    elif kind == b'P':  # portal
                        actions.append(
                            PGMessage(
                                PGAction.CLOSE_PORTAL,
                                portal_name=name,
                                query_unit=dbv.close_portal(name),
                            ),
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
        return actions, None

    async def _ensure_ps_locality(
        self,
        dbv: ConnectionView,
        stmt_name: str,
        local_stmts: set[str],
        actions
    ) -> PreparedStmt:
        """Make sure given *stmt_name* is known by Postgres

        Frontend SQL connections do not normally own Postgres connections,
        so there is no affinity between them.  Thus, whenever we receive
        a message operating on some prepared statement, we must ensure
        that this statement has been prepared in the currently active
        Postgres connection.  We rely on pgcon LRU to actually make a
        decision on whether to issue the injected Parse messages.

        NB: this method mutates *local_stmts* and *actions*.
        """
        cdef:
            PreparedStmt stmt

        stmt = self.prepared_stmts.get(stmt_name)
        if stmt is None:
            raise pgerror.new(
                pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                f"prepared statement \"{stmt_name}\" does not "
                f"exist",
            )

        if stmt_name not in local_stmts:
            # Non-local statement, so inject its Parse.
            fe_settings = dbv.current_fe_settings()
            qu = stmt.parse_action.query_unit
            assert qu is not None
            if stmt.parse_action.fe_settings != fe_settings:
                # Some of the statically compiler-evaluated
                # queries like `current_schema` depend on the
                # fe_settings, we need to re-compile if the
                # fe_settings have changed.
                stmt.parse_action.invalidate()

            if (
                qu.execute is not None
                and (
                    qu.execute.be_stmt_name
                    != self.sql_prepared_stmts_map.get(
                        qu.execute.stmt_name).encode("utf-8")
                )
            ):
                # Likewise, re-compile if this is an EXECUTE query
                # and the translated name of the prepared statement
                # has changed (e.g. due to it having been deallocated
                # and prepared with a different query).
                stmt.parse_action.invalidate()

            if not stmt.parse_action.is_valid():
                parse_actions, new_stmts = await self._reparse(
                    stmt_name,
                    stmt.parse_action,
                    dbv,
                )
                local_stmts.update(new_stmts)
                actions.extend(parse_actions)
                stmt = self.prepared_stmts[stmt_name]
            else:
                actions.append(stmt.parse_action.as_injected())
                local_stmts.add(stmt_name)

        return stmt

    async def _reparse(
        self,
        str stmt_name,
        PGMessage parse_action,
        ConnectionView dbv,
    ):
        cdef:
            PreparedStmt outer_stmt
        actions = []
        qu = parse_action.query_unit
        assert qu is not None

        if self.debug:
            self.debug_print("reparsing", stmt_name, parse_action)

        if (
            qu.prepare is not None
            or qu.execute is not None
        ):
            actions.extend(
                await self._ensure_nested_ps_exists(
                    dbv,
                    qu,
                    force_recompilation=True,
                ),
            )

        outer_stmt, new_stmts = await self._parse_statement(
            stmt_name,
            qu.orig_query,
            parse_action.args[1],
            dbv,
            force_recompilation=True,
            injected_action=True,
        )

        actions.append(outer_stmt.parse_action)

        return actions, new_stmts

    async def _ensure_nested_ps_exists(
        self,
        dbv: ConnectionView,
        execute_unit: dbstate.SQLQueryUnit,
        force_recompilation: bool = False,
    ) -> list[PGMessage]:
        cdef:
            PreparedStmt sql_stmt

        exec_data = execute_unit.execute
        prep_qu = self.sql_prepared_stmts.pop(exec_data.stmt_name, None)
        actions = []

        if prep_qu is None:
            raise pgerror.new(
                pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                f"prepared statement "
                f"\"{exec_data.stmt_name}\" does not "
                f"exist",
            )

        sql_stmt, _ = await self._parse_statement(
            prep_qu.stmt_name.decode("utf-8"),
            prep_qu.orig_query,
            b"\x00\x00",
            dbv,
            injected_action=True,
            force_recompilation=force_recompilation,
        )
        actions.append(sql_stmt.parse_action)
        parse_stmt_name = sql_stmt.parse_action.stmt_name
        portal_name = parse_stmt_name.decode("utf-8")
        parse_query_unit = sql_stmt.parse_action.query_unit
        actions.append(
            PGMessage(
                PGAction.BIND,
                portal_name=portal_name,
                stmt_name=parse_stmt_name,
                args=b"\x00\x01\x00\x01\x00\x00\x00\x00",
                query_unit=parse_query_unit,
                injected=True,
            )
        )
        actions.append(
            PGMessage(
                PGAction.EXECUTE,
                args=0,
                portal_name=portal_name,
                query_unit=parse_query_unit,
                injected=True,
            )
        )
        actions.append(
            PGMessage(
                PGAction.CLOSE_PORTAL,
                portal_name=portal_name,
                query_unit=parse_query_unit,
                injected=True,
            )
        )
        return actions

    async def _parse_statement(
        self,
        stmt_name: str | None,
        query_str: str,
        parse_data: bytes,
        dbv: ConnectionView,
        force_recompilation: bool = False,
        injected_action: bool = False,
    ) -> Tuple[PreparedStmt, set[str]]:
        """Generate a PARSE action for *query_str*.

        The *query_str* string must contain exactly one SQL statement.
        """
        stmts = set()

        source = pg_parser.NormalizedSource.from_string(query_str)

        query_units = await self.compile(
            source, dbv, ignore_cache=force_recompilation
        )
        if len(query_units) > 1:
            raise pgerror.new(
                pgerror.ERROR_SYNTAX_ERROR,
                "cannot insert multiple commands into a prepared "
                "statement",
            )

        return await self._parse_unit(
            stmt_name,
            query_units[0],
            source,
            parse_data,
            dbv,
            injected_action=injected_action,
        )

    async def _parse_unit(
        self,
        stmt_name: str | None,
        unit: dbstate.SQLQueryUnit,
        source: pg_parser.Source,
        parse_data: bytes,
        dbv: ConnectionView,
        injected_action: bool = False,
    ) -> Tuple[PreparedStmt, set[str]]:
        stmts = set()

        fe_settings = dbv.current_fe_settings()
        nested_ps_name = None
        if unit.prepare is not None:
            # Statement-level PREPARE
            nested_ps_name = unit.prepare.stmt_name
            unit = self._validate_prepare_stmt(unit)
            stmts.add(nested_ps_name)
            self.sql_prepared_stmts[nested_ps_name] = unit
            self.sql_prepared_stmts_map[nested_ps_name] = (
                unit.prepare.be_stmt_name.decode("utf-8"))
        elif unit.execute is not None:
            # Statement-level EXECUTE
            nested_ps_name = unit.execute.stmt_name
            unit = self._validate_execute_stmt(unit)
        elif unit.deallocate is not None:
            # Statement-level DEALLOCATE
            nested_ps_name = unit.deallocate.stmt_name
            unit = self._validate_deallocate_stmt(unit)

        parse_data = remap_parameters(parse_data, unit.params)

        action = PGMessage(
            PGAction.PARSE,
            stmt_name=unit.stmt_name,
            args=(unit.query.encode("utf-8"), parse_data),
            query_unit=unit,
            fe_settings=fe_settings,
            injected=injected_action,
        )

        if stmt_name is not None and nested_ps_name is not None:
            # This is a prepared statement of an operation on *another*
            # prepared statement, and so we must track this relationship
            # in case the nested prepared statement gets deallocated.
            try:
                self.wrapping_prepared_stmts[nested_ps_name].add(stmt_name)
            except KeyError:
                self.wrapping_prepared_stmts[nested_ps_name] = set([stmt_name])

        stmt = PreparedStmt(
            parse_action=action,
            source=source,
        )

        if stmt_name is not None:
            self.prepared_stmts[stmt_name] = stmt
            stmts.add(stmt_name)

        return stmt, stmts

    async def compile(
        self, source: pg_parser.Source, ConnectionView dbv, ignore_cache=False
    ) -> List[dbstate.SQLQueryUnit]:
        if self.debug:
            self.debug_print("Compile", source.text())

        fe_settings = dbv.current_fe_settings()
        key = compute_cache_key(source, fe_settings)

        ignore_cache |= self._disable_cache

        result: List[dbstate.SQLQueryUnit]
        if not ignore_cache:
            result = self.database.lookup_compiled_sql(key)
            if result is not None:
                return result
        # Remember the schema version we are compiling on, so that we can
        # cache the result with the matching version. In case of concurrent
        # schema update, we're only storing an outdated cache entry, and
        # the next identical query could get recompiled on the new schema.
        schema_version = self.database.schema_version
        compiler_pool = self.server.get_compiler_pool()
        started_at = time.monotonic()
        try:
            result = await compiler_pool.compile_sql(
                self.dbname,
                self.database.user_schema_pickle,
                self.database._index._global_schema_pickle,
                self.database.reflection_cache,
                self.database.db_config,
                self.database._index.get_compilation_system_config(),
                source,
                dbv.fe_transaction_state(),
                self.sql_prepared_stmts_map,
                self.dbname,
                self.username,
                client_id=self.tenant.client_id,
            )
        finally:
            metrics.query_compilation_duration.observe(
                time.monotonic() - started_at,
                self.tenant.get_instance_name(),
                "sql",
                )
        self.database.cache_compiled_sql(key, result, schema_version)
        metrics.sql_compilations.inc(
            len(result), self.tenant.get_instance_name()
        )
        if self.debug:
            self.debug_print("Compile result", result)
        return result

    def _validate_prepare_stmt(self, qu):
        assert qu.prepare is not None
        stmt_name = qu.prepare.stmt_name
        if (
            stmt_name in self.prepared_stmts
            or stmt_name in self.sql_prepared_stmts
        ):
            raise pgerror.new(
                pgerror.ERROR_DUPLICATE_PREPARED_STATEMENT,
                f"prepared statement \"{stmt_name}\" "
                f"already exists",
            )
        return qu

    def _validate_execute_stmt(self, qu):
        assert qu.execute is not None
        stmt_name = qu.execute.stmt_name
        sql_ps = self.sql_prepared_stmts.get(stmt_name)
        if sql_ps is None:
            raise pgerror.new(
                pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                f"prepared statement \"{stmt_name}\" does "
                f"not exist",
            )
        return qu

    def _validate_deallocate_stmt(self, qu):
        assert qu.deallocate is not None
        stmt_name = qu.deallocate.stmt_name
        sql_ps = self.sql_prepared_stmts.get(stmt_name)
        if sql_ps is None:
            raise pgerror.new(
                pgerror.ERROR_INVALID_SQL_STATEMENT_NAME,
                f"prepared statement \"{stmt_name}\" does "
                f"not exist",
            )
        return qu


def compute_cache_key(
    source: pg_parser.Source, fe_settings: dbstate.SQLSettings
) -> bytes:
    h = hashlib.blake2b(source.cache_key())
    for key, value in fe_settings.items():
        if key.startswith('global '):
            continue
        h.update(hash(value).to_bytes(8, signed=True))
    return h.digest()


cdef bytes remap_arguments(
    data: bytes,
    params: list[dbstate.SQLParam] | None,
    fe_settings: dbstate.SQLSettings,
    source: pg_parser.Source,
):
    cdef:
        int16_t param_format_count
        int32_t offset
        int32_t arg_offset_external
        int16_t param_count_external
        int32_t size

    # The "external" parameters (that are visible to the user)
    # don't include the internal params for globals and extracted constants.

    # So when we send external params to postgres, we remap them
    # to correct positions and add the globals.

    buf = WriteBuffer.new()

    # remap param format codes
    param_format_count = read_int16(data[0:2])
    offset = 2
    if params:
        buf.write_int16(len(params))
        for i, param in enumerate(params):
            if isinstance(param, dbstate.SQLParamExternal):
                if param_format_count == 0:
                    buf.write_int16(0) # text
                elif param_format_count == 1:
                    buf.write_bytes(
                        data[offset:offset+2]
                    )
                else:
                    o = offset + i * 2
                    buf.write_bytes(data[o:o+2])
            elif isinstance(param, dbstate.SQLParamExtractedConst):
                buf.write_int16(0) # text
            else:
                # this is for globals
                buf.write_int16(1) # binary
    else:
        buf.write_int16(0)
    offset += param_format_count * 2

    # find positions of external args
    arg_count_external = read_int16(data[offset:offset+2])
    offset += 2
    arg_offset_external = offset
    for p in range(arg_count_external):
        size = read_int32(data[offset:offset+4])
        if size == -1:  # special case: NULL
            size = 0
        size += 4 # for size which is int32
        offset += size

    # write remapped args
    if params:
        buf.write_int16(len(params))

        param_count_external = 0
        for i, param in enumerate(params):
            if not isinstance(param, dbstate.SQLParamExternal):
                break
            param_count_external = i + 1
        if param_count_external != arg_count_external:
            raise pgerror.new(
                pgerror.ERROR_PROTOCOL_VIOLATION,
                f'bind message supplies {arg_count_external} '
                f'parameters, but prepared statement "" requires '
                f'{param_count_external}',
            )

        # write external args
        if arg_offset_external < offset:
            buf.write_bytes(data[arg_offset_external:offset])

        # write non-external args
        extracted_consts = list(source.variables().values())
        for (e, param) in enumerate(params[param_count_external:]):
            if isinstance(param, dbstate.SQLParamExtractedConst):
                buf.write_len_prefixed_bytes(extracted_consts[e])
            elif isinstance(param, dbstate.SQLParamGlobal):
                name = param.global_name
                setting_name = f'global {name.module}::{name.name}'
                values = fe_settings.get(setting_name, None)

                if values == None:
                    buf.write_int32(-1) # NULL
                else:
                    write_arg(buf, param.pg_type, values)
    else:
        buf.write_int16(0)

    # result format codes
    buf.write_bytes(data[offset:])
    return bytes(buf)


cdef bytes remap_parameters(
    data: bytes,
    params: list[dbstate.SQLParam] | None
):
    # Inject parameter type descriptions in parse message for parameters for
    # globals and extracted constants.

    if not params:
        return b"\x00\x00"

    buf = WriteBuffer.new()
    buf.write_int16(len(params))

    # copy the params specified by user
    specified_ext = read_int16(data[0:2])
    buf.write_bytes(data[2:2 + specified_ext*4])

    for index, param in enumerate(params):

        # already written
        if index < specified_ext:
            continue

        if isinstance(param, dbstate.SQLParamExternal):
            buf.write_int32(0)  # unspecified
        elif isinstance(param, dbstate.SQLParamExtractedConst):
            buf.write_int32(param.type_oid)
        elif isinstance(param, dbstate.SQLParamGlobal):
            buf.write_int32(0)  # unspecified
    assert len(bytes(buf)) == 2 + 4 * len(params)
    return bytes(buf)


cdef write_arg(
    buf: WriteBuffer, pg_type: tuple, values: dbstate.SQLSetting
):
    value = values[0]

    if pg_type == ('text',) and isinstance(value, str):
        val = str(value).encode('UTF-8')
        buf.write_len_prefixed_bytes(val)
    elif pg_type == ('uuid',) and isinstance(value, str):
        try:
            id = uuid.UUID(value)
            buf.write_len_prefixed_bytes(id.bytes)
        except ValueError:
            buf.write_int32(-1)  # NULL
    elif pg_type == ('int8',) and isinstance(value, int):
        buf.write_int32(8)
        buf.write_int64(value)
    elif pg_type == ('int4',) and isinstance(value, int):
        buf.write_int32(4)
        buf.write_int32(value)
    elif pg_type == ('int2',) and isinstance(value, int):
        buf.write_int32(2)
        buf.write_int16(value)
    elif pg_type == ('bool',):
        is_truthy = is_setting_truthy(value)
        if is_truthy == None:
            buf.write_int32(-1)
        else:
            buf.write_int32(1)
            buf.write_byte(1 if is_truthy else 0)
    elif pg_type == ('float8',) and isinstance(value, float):
        buf.write_int32(8)
        buf.write_double(value)
    elif pg_type == ('float4',) and isinstance(value, float):
        buf.write_int32(4)
        buf.write_float(value)
    else:
        buf.write_int32(-1)  # NULL
        raise RuntimeError(
            f"unimplemented glob type={pg_type}, value={type(value)}"
        )


def is_setting_truthy(value: str | int | float) -> bool | None:
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        value = value.lower()
        if value == 'o':
            # ambigious
            return None

        truthy_values = ('on', 'true', 'yes', '1')
        if any(t.startswith(value) for t in truthy_values):
            return True

        falsy_values = ('off', 'false', 'no', '0')
        if any(t.startswith(value) for t in falsy_values):
            return False
    return None


cdef inline int16_t read_int16(data: bytes):
    return int.from_bytes(data[0:2], "big", signed=True)


cdef inline int32_t read_int32(data: bytes):
    return int.from_bytes(data[0:4], "big", signed=True)


def new_pg_connection(server, sslctx, endpoint_security, connection_made_at):
    return PgConnection(
        server,
        sslctx,
        endpoint_security,
        passive=False,
        transport=srvargs.ServerConnTransport.TCP_PG,
        external_auth=False,
        connection_made_at=connection_made_at,
    )
