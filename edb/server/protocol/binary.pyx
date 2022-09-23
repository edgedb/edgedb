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
import base64
import collections
import hashlib
import json
import logging
import time
import statistics
import traceback
import sys

cimport cython
cimport cpython

from typing import Dict, List, Optional, Sequence, Tuple
from edb.server.protocol cimport cpythonx

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

import immutables
from jwcrypto import jwt

from edb import buildmeta
from edb import edgeql
from edb.edgeql import qltypes

from edb.server.pgproto cimport hton
from edb.server.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_read_all,
    frb_get_len,
)
from edb.server.pgproto.pgproto import UUID as pg_UUID

from edb.server.dbview cimport dbview

from edb.server import config

from edb.server import args as srvargs
from edb.server import compiler
from edb.server import defines as edbdef
from edb.server.compiler import errormech
from edb.server.compiler import enums
from edb.server.compiler import sertypes
from edb.server.protocol import execute
from edb.server.protocol cimport frontend
from edb.server.pgcon cimport pgcon
from edb.server.pgcon import errors as pgerror
from edb.server import metrics

from edb.schema import objects as s_obj

from edb import errors
from edb.errors import base as base_errors, EdgeQLSyntaxError
from edb.common import debug, taskgroup
from edb.common import context as pctx

from edb.protocol import messages

from edgedb import scram


include "./consts.pxi"


DEF FLUSH_BUFFER_AFTER = 100_000
cdef bytes EMPTY_TUPLE_UUID = s_obj.get_known_type_id('empty-tuple').bytes

cdef object CARD_NO_RESULT = compiler.Cardinality.NO_RESULT
cdef object CARD_AT_MOST_ONE = compiler.Cardinality.AT_MOST_ONE
cdef object CARD_MANY = compiler.Cardinality.MANY

cdef object FMT_BINARY = compiler.OutputFormat.BINARY
cdef object FMT_JSON = compiler.OutputFormat.JSON
cdef object FMT_JSON_ELEMENTS = compiler.OutputFormat.JSON_ELEMENTS
cdef object FMT_NONE = compiler.OutputFormat.NONE

cdef tuple DUMP_VER_MIN = (0, 7)
cdef tuple DUMP_VER_MAX = (1, 0)

cdef tuple MIN_PROTOCOL = edbdef.MIN_PROTOCOL
cdef tuple MAX_LEGACY_PROTOCOL = edbdef.MAX_LEGACY_PROTOCOL
cdef tuple CURRENT_PROTOCOL = edbdef.CURRENT_PROTOCOL

cdef object logger = logging.getLogger('edb.server')
cdef object log_metrics = logging.getLogger('edb.server.metrics')

DEF QUERY_HEADER_IMPLICIT_LIMIT = 0xFF01
DEF QUERY_HEADER_IMPLICIT_TYPENAMES = 0xFF02
DEF QUERY_HEADER_IMPLICIT_TYPEIDS = 0xFF03
DEF QUERY_HEADER_ALLOW_CAPABILITIES = 0xFF04
DEF QUERY_HEADER_EXPLICIT_OBJECTIDS = 0xFF05

DEF SERVER_HEADER_CAPABILITIES = 0x1001

DEF ALL_CAPABILITIES = 0xFFFFFFFFFFFFFFFF


def parse_capabilities_header(value: bytes) -> uint64_t:
    if len(value) != 8:
        raise errors.BinaryProtocolError(
            f'capabilities header must be exactly 8 bytes'
        )
    cdef uint64_t mask = hton.unpack_uint64(cpython.PyBytes_AS_STRING(value))
    return mask


cdef inline bint parse_boolean(value: bytes, header: str):
    cdef bytes lower = value.lower()
    if lower == b'true':
        return True
    elif lower == b'false':
        return False
    else:
        raise errors.BinaryProtocolError(
            f'{header} header must equal "true" or "false"'
        )


cdef class EdgeConnection(frontend.FrontendConnection):

    def __init__(
        self,
        server,
        *,
        external_auth: bool,
        passive: bool,
        transport: srvargs.ServerConnTransport,
        auth_data: bytes,
        conn_params: dict[str, str] | None,
        protocol_version: tuple[int, int] = CURRENT_PROTOCOL,
    ):
        self._con_status = EDGECON_NEW
        self._id = server.on_binary_client_created()
        self.server = server
        self._external_auth = external_auth

        self.loop = server.get_loop()
        self._dbview = None
        self.dbname = None

        self._transport = None
        self.buffer = ReadBuffer()

        self._cancelled = False
        self._stop_requested = False
        self._pgcon_released_in_connection_lost = False

        self._main_task = None
        self._msg_take_waiter = None
        self._write_waiter = None

        self._last_anon_compiled = None

        self._write_buf = None

        self.debug = debug.flags.server_proto
        self.query_cache_enabled = not (debug.flags.disable_qcache or
                                        debug.flags.edgeql_compile)

        self.authed = False
        self.idling = False
        self.started_idling_at = 0.0

        self.protocol_version = protocol_version
        self.min_protocol = MIN_PROTOCOL
        self.max_protocol = CURRENT_PROTOCOL

        self._conn_params = conn_params

        self._pinned_pgcon = None
        self._pinned_pgcon_in_tx = False
        self._get_pgcon_cc = 0

        self._in_dump_restore = False

        # In "passive" mode the protocol is instantiated to parse and execute
        # just what's in the buffer. It cannot "wait for message". This
        # is used to implement binary protocol over http+fetch.
        self._passive_mode = passive

        self._transport_proto = transport

        # Authentication data supplied by the transport (e.g. the content
        # of an HTTP Authorization header).
        self._auth_data = auth_data

    def __del__(self):
        # Should not ever happen, there's a strong ref to
        # every client connection until it hits connection_lost().
        if self._pinned_pgcon is not None:
            # XXX/TODO: add test diagnostics for this and
            # fail all tests if this ever happens.
            self.abort_pinned_pgcon()

    cdef inline dbview.DatabaseConnectionView get_dbview(self):
        if self._dbview is None:
            raise RuntimeError('Cannot access dbview while it is None')
        return self._dbview

    def get_id(self):
        return self._id

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    async def get_pgcon(self) -> pgcon.PGConnection:
        cdef dbview.DatabaseConnectionView _dbview
        if self._cancelled or self._pgcon_released_in_connection_lost:
            raise RuntimeError(
                'cannot acquire a pgconn; the connection is closed')
        self._get_pgcon_cc += 1
        try:
            if self._get_pgcon_cc > 1:
                raise RuntimeError('nested get_pgcon() calls are prohibited')
            _dbview = self.get_dbview()
            if _dbview.in_tx():
                #  In transaction. We must have a working pinned connection.
                if not self._pinned_pgcon_in_tx or self._pinned_pgcon is None:
                    raise RuntimeError(
                        'get_pgcon(): in dbview transaction, '
                        'but `_pinned_pgcon` is None')
                return self._pinned_pgcon
            if self._pinned_pgcon is not None:
                raise RuntimeError('there is already a pinned pgcon')
            conn = await self.server.acquire_pgcon(self.dbname)
            self._pinned_pgcon = conn
            conn.pinned_by = self
            return conn
        except Exception:
            self._get_pgcon_cc -= 1
            raise

    def maybe_release_pgcon(self, pgcon.PGConnection conn):
        cdef dbview.DatabaseConnectionView _dbview
        self._get_pgcon_cc -= 1
        if self._get_pgcon_cc < 0:
            raise RuntimeError(
                'maybe_release_pgcon() called more times than get_pgcon()')
        if self._pinned_pgcon is not conn:
            raise RuntimeError('mismatched released connection')

        _dbview = self.get_dbview()
        if _dbview.in_tx():
            if self._cancelled:
                # There could be a situation where we cancel the protocol while
                # it's in a transaction. In which case we want to immediately
                # return the connection to the pool (where it would be
                # discarded and re-opened.)
                conn.pinned_by = None
                self._pinned_pgcon = None
                if not self._pgcon_released_in_connection_lost:
                    self.server.release_pgcon(self.dbname, conn)
            else:
                self._pinned_pgcon_in_tx = True
        else:
            conn.pinned_by = None
            self._pinned_pgcon_in_tx = False
            self._pinned_pgcon = None
            if not self._pgcon_released_in_connection_lost:
                self.server.release_pgcon(self.dbname, conn)

    def on_aborted_pgcon(self, pgcon.PGConnection conn):
        try:
            self._pinned_pgcon = None

            if not self._pgcon_released_in_connection_lost:
                self.server.release_pgcon(self.dbname, conn, discard=True)

            if conn.aborted_with_error is not None:
                self.write_error(conn.aborted_with_error)
        finally:
            self.close()  # will flush

    def debug_print(self, *args):
        if self._dbview is None:
            # This may happen before dbview is initialized, e.g. sending errors
            # to non-TLS clients due to mandatory TLS.
            print(
                '::EDGEPROTO::',
                f'id:{self._id}',
                f'in_tx:{0}',
                f'tx_error:{0}',
                *args,
                file=sys.stderr,
            )
        else:
            print(
                '::EDGEPROTO::',
                f'id:{self._id}',
                f'in_tx:{int(self._dbview.in_tx())}',
                f'tx_error:{int(self._dbview.in_tx_error())}',
                *args,
                file=sys.stderr,
            )

    cdef write(self, WriteBuffer buf):
        # One rule for this method: don't write partial messages.
        if self._write_buf is not None:
            self._write_buf.write_buffer(buf)
            if self._write_buf.len() >= FLUSH_BUFFER_AFTER:
                self.flush()
        else:
            self._write_buf = buf

    cdef abort_pinned_pgcon(self):
        if self._pinned_pgcon is not None:
            self._pinned_pgcon.pinned_by = None
            self._pinned_pgcon.abort()
            self.server.release_pgcon(
                self.dbname, self._pinned_pgcon, discard=True)
            self._pinned_pgcon = None

    def is_idle(self, expiry_time: float):
        # A connection is idle if it awaits for the next message for
        # client for too long (even if it is in an open transaction!)
        return (
            self._con_status != EDGECON_BAD and
            self.idling and
            self.started_idling_at < expiry_time and
            not self._in_dump_restore
        )

    def is_alive(self):
        return (
            self._con_status == EDGECON_STARTED and
            self._transport is not None and
            not self._cancelled
        )

    def abort(self):
        self.abort_pinned_pgcon()
        self.stop_connection()

        if self._transport is not None:
            self._transport.abort()
            self._transport = None

    def close_for_idling(self):
        try:
            self.write_error(
                errors.IdleSessionTimeoutError(
                    'closing the connection due to idling')
            )
        finally:
            self.close()  # will flush

    def close(self):
        self.abort_pinned_pgcon()
        self.stop_connection()

        if self._transport is not None:
            self.flush()
            self._transport.close()
            self._transport = None

    def stop(self):
        # Actively stop a binary connection - this is used by the server
        # when it's stopping.

        self._stop_requested = True
        if self._msg_take_waiter is not None:
            if not self._msg_take_waiter.done():
                self._msg_take_waiter.cancel()

    cdef flush(self):
        if self._transport is None:
            # could be if the connection is lost and a coroutine
            # method is finalizing.
            raise ConnectionAbortedError
        if self._write_buf is not None and self._write_buf.len():
            buf = self._write_buf
            self._write_buf = None
            self._transport.write(memoryview(buf))

    async def wait_for_message(self, *, bint report_idling):
        if self.buffer.take_message():
            return
        if self._passive_mode:
            raise RuntimeError('cannot wait for more messages in passive mode')
        if self._transport is None:
            # could be if the connection is lost and a coroutine
            # method is finalizing.
            raise ConnectionAbortedError

        self._msg_take_waiter = self.loop.create_future()
        if report_idling:
            self.idling = True
            self.started_idling_at = time.monotonic()

        try:
            await self._msg_take_waiter
        finally:
            self.idling = False

        self.server.on_binary_client_after_idling(self)

    async def do_handshake(self):
        cdef:
            char mtype

        if self._transport_proto is srvargs.ServerConnTransport.HTTP:
            if self._conn_params is None:
                params = {}
            else:
                params = self._conn_params
        else:
            await self.wait_for_message(report_idling=True)
            mtype = self.buffer.get_message_type()
            if mtype != b'V':
                raise errors.BinaryProtocolError(
                    f'unexpected initial message: "{chr(mtype)}", '
                    f'expected "V"')

            params = await self._do_handshake()
            if self._conn_params is not None:
                params = self._conn_params + params

        return params

    async def auth(self, params):
        cdef:
            WriteBuffer msg_buf
            WriteBuffer buf

        user = params.get('user')
        if not user:
            raise errors.BinaryProtocolError(
                f'missing required connection parameter in ClientHandshake '
                f'message: "user"'
            )

        database = params.get('database')
        if not database:
            raise errors.BinaryProtocolError(
                f'missing required connection parameter in ClientHandshake '
                f'message: "database"'
            )

        logger.debug('received connection request by %s to database %s',
                     user, database)

        if not self.server.is_database_connectable(database):
            raise errors.AccessError(
                f'database {database!r} does not accept connections'
            )

        await self._start_connection(database)

        # The user has already been authenticated by other means
        # (such as the ability to write to a protected socket).
        if self._external_auth:
            authmethod_name = 'Trust'
        else:
            authmethod = await self.server.get_auth_method(
                user, self._transport_proto)
            authmethod_name = type(authmethod).__name__

        if authmethod_name == 'SCRAM':
            await self._auth_scram(user)
        elif authmethod_name == 'JWT':
            self._auth_jwt(user)
        elif authmethod_name == 'Trust':
            self._auth_trust(user)
        else:
            raise errors.InternalServerError(
                f'unimplemented auth method: {authmethod_name}')

        logger.debug('successfully authenticated %s in database %s',
                     user, database)

        if self._transport_proto is srvargs.ServerConnTransport.HTTP:
            return

        buf = WriteBuffer()

        msg_buf = WriteBuffer.new_message(b'R')
        msg_buf.write_int32(0)
        msg_buf.end_message()
        buf.write_buffer(msg_buf)

        msg_buf = WriteBuffer.new_message(b'K')
        # TODO: should send ID of this connection
        msg_buf.write_bytes(b'\x00' * 32)
        msg_buf.end_message()
        buf.write_buffer(msg_buf)

        buf.write_buffer(self.make_state_data_description_msg())

        self.write(buf)

        if self.server.in_dev_mode():
            pgaddr = dict(self.server._get_pgaddr())
            if pgaddr.get('password'):
                pgaddr['password'] = '********'
            pgaddr['database'] = self.server.get_pg_dbname(
                self.get_dbview().dbname
            )
            pgaddr.pop('ssl', None)
            if 'sslmode' in pgaddr:
                pgaddr['sslmode'] = pgaddr['sslmode'].name
            self.write_status(b'pgaddr', json.dumps(pgaddr).encode())

        self.write_status(
            b'suggested_pool_concurrency',
            str(self.server.get_suggested_client_pool_size()).encode()
        )
        self.write_status(
            b'system_config',
            self.server.get_report_config_data()
        )

        self.write(self.sync_status())

        self.flush()

    async def _do_handshake(self):
        cdef:
            uint16_t major
            uint16_t minor
            int i
            uint16_t reserved
            dict params = {}

        major = <uint16_t>self.buffer.read_int16()
        minor = <uint16_t>self.buffer.read_int16()

        self.protocol_version = major, minor

        nparams = <uint16_t>self.buffer.read_int16()
        for i in range(nparams):
            k = self.buffer.read_len_prefixed_utf8()
            v = self.buffer.read_len_prefixed_utf8()
            params[k] = v

        reserved = <uint16_t>self.buffer.read_int16()
        if reserved != 0:
            raise errors.BinaryProtocolError(
                f'unexpected value in reserved field of ClientHandshake')

        self.buffer.finish_message()

        negotiate = False
        if self.protocol_version < self.min_protocol:
            target_proto = self.min_protocol
            negotiate = True
        elif self.protocol_version > self.max_protocol:
            target_proto = self.max_protocol
            negotiate = True
        else:
            target_proto = self.protocol_version

        if negotiate:
            self.write(self.make_negotiate_protocol_version_msg(target_proto))
            self.flush()

        return params

    async def _start_connection(self, database: str) -> None:
        dbv = await self.server.new_dbview(
            dbname=database,
            query_cache=self.query_cache_enabled,
            protocol_version=self.protocol_version,
        )
        assert type(dbv) is dbview.DatabaseConnectionView
        self._dbview = <dbview.DatabaseConnectionView>dbv
        self.dbname = database

        self._con_status = EDGECON_STARTED

    def stop_connection(self) -> None:
        self._con_status = EDGECON_BAD

        if self._dbview is not None:
            self.server.remove_dbview(self._dbview)
            self._dbview = None

    def _auth_trust(self, user):
        roles = self.server.get_roles()
        if user not in roles:
            raise errors.AuthenticationError('authentication failed')

    def _auth_jwt(self, user):
        role = self.server.get_roles().get(user)
        if role is None:
            raise errors.AuthenticationError('authentication failed')

        if not self._auth_data:
            raise errors.AuthenticationError(
                'authentication failed: no authorization data provided')

        header_value = self._auth_data.decode("ascii")
        scheme, _, encoded_token = header_value.partition(" ")
        if scheme.lower() != "bearer":
            raise errors.AuthenticationError(
                'authentication failed: unrecognized authentication scheme')

        encoded_token = encoded_token.strip()
        if not encoded_token:
            raise errors.AuthenticationError(
                'authentication failed: malformed JWT')

        ekey = self.server.get_jwe_key()
        skey = self.server.get_jws_key()

        try:
            decrypted_token = jwt.JWT(
                key=ekey,
                algs=[
                    "RSA-OAEP-256",
                    "ECDH-ES",
                    "A128GCM",
                    "A192GCM",
                    "A256GCM",
                ],
                jwt=encoded_token,
            )
            token = jwt.JWT(
                key=skey,
                algs=["RS256", "ES256"],
                jwt=decrypted_token.claims,
            )
        except jwt.JWException as e:
            logger.debug('authentication failure', exc_info=True)
            raise errors.AuthenticationError(
                f'authentication failed: {e.args[0]}'
            ) from None
        except Exception as e:
            logger.debug('authentication failure', exc_info=True)
            raise errors.AuthenticationError(
                f'authentication failed: cannot decode JWT'
            ) from None

        namespace = "edgedb.server"

        try:
            claims = json.loads(token.claims)
        except Exception as e:
            raise errors.AuthenticationError(
                f'authentication failed: malformed claims section in JWT'
            ) from None

        if not claims.get(f"{namespace}.any_role"):
            token_roles = claims.get(f"{namespace}.roles")
            if not isinstance(token_roles, dict):
                raise errors.AuthenticationError(
                    f'authentication failed: malformed claims section in JWT'
                    f' expected mapping in "role_names"'
                )

            token_pw = token_roles.get(user)
            if token_pw is None:
                raise errors.AuthenticationError(
                    'authentication failed: role not authorized by this JWT')

            if token_pw != role["password"]:
                raise errors.AuthenticationError(
                    'authentication failed: mismatched password in JWT')

    async def _auth_scram(self, user):
        # Tell the client that we require SASL SCRAM auth.
        msg_buf = WriteBuffer.new_message(b'R')
        msg_buf.write_int32(10)
        # Number of auth methods followed by a series
        # of zero-terminated strings identifying each method,
        # sorted in the order of server preference.
        msg_buf.write_int32(1)
        msg_buf.write_len_prefixed_bytes(b'SCRAM-SHA-256')
        msg_buf.end_message()
        self.write(msg_buf)
        self.flush()

        selected_mech = None
        verifier = None
        mock_auth = False
        client_nonce = None
        cb_flag = None
        done = False

        while not done:
            if not self.buffer.take_message():
                await self.wait_for_message(report_idling=True)
            mtype = self.buffer.get_message_type()

            if selected_mech is None:
                if mtype != b'p':
                    raise errors.BinaryProtocolError(
                        f'expected SASL response, got message type {mtype}')
                # Initial response.
                selected_mech = self.buffer.read_len_prefixed_bytes()
                if selected_mech != b'SCRAM-SHA-256':
                    raise errors.BinaryProtocolError(
                        f'client selected an invalid SASL authentication '
                        f'mechanism')

                verifier, mock_auth = self._get_scram_verifier(user)
                client_first = self.buffer.read_len_prefixed_bytes()
                self.buffer.finish_message()

                if not client_first:
                    # The client didn't send the Client Initial Response
                    # in SASLInitialResponse, this is an error.
                    raise errors.BinaryProtocolError(
                        f'client did not send the Client Initial Response '
                        f'data in SASLInitialResponse')

                try:
                    bare_offset, cb_flag, authzid, username, client_nonce = (
                        scram.parse_client_first_message(client_first))
                except ValueError as e:
                    raise errors.BinaryProtocolError(str(e))

                client_first_bare = client_first[bare_offset:]

                if isinstance(cb_flag, str):
                    raise errors.BinaryProtocolError(
                        'malformed SCRAM message',
                        details='The client selected SCRAM-SHA-256 without '
                                'channel binding, but the SCRAM message '
                                'includes channel binding data.')

                if authzid:
                    raise errors.UnsupportedFeatureError(
                        'client uses SASL authorization identity, '
                        'which is not supported')

                server_nonce = scram.generate_nonce()
                server_first = scram.build_server_first_message(
                    server_nonce, client_nonce,
                    verifier.salt, verifier.iterations).encode('utf-8')

                # AuthenticationSASLContinue
                msg_buf = WriteBuffer.new_message(b'R')
                msg_buf.write_int32(11)
                msg_buf.write_len_prefixed_bytes(server_first)
                msg_buf.end_message()
                self.write(msg_buf)
                self.flush()

            else:
                if mtype != b'r':
                    raise errors.BinaryProtocolError(
                        f'expected SASL response, got message type {mtype}')
                # client final message
                client_final = self.buffer.read_len_prefixed_bytes()
                self.buffer.finish_message()

                try:
                    cb_data, client_proof, proof_len = (
                        scram.parse_client_final_message(
                            client_final, client_nonce, server_nonce))
                except ValueError as e:
                    raise errors.BinaryProtocolError(str(e)) from None

                client_final_without_proof = client_final[:-proof_len]

                cb_data_ok = (
                    (cb_flag is False and cb_data == b'biws')
                    or (cb_flag is True and cb_data == b'eSws')
                )
                if not cb_data_ok:
                    raise errors.BinaryProtocolError(
                        'malformed SCRAM message',
                        details='Unexpected SCRAM channel-binding attribute '
                                'in client-final-message.')

                if not scram.verify_client_proof(
                        client_first_bare, server_first,
                        client_final_without_proof,
                        verifier.stored_key, client_proof):
                    raise errors.AuthenticationError(
                        'authentication failed')

                if mock_auth:
                    # This user actually does not exist, so fail here.
                    raise errors.AuthenticationError(
                        'authentication failed')

                server_final = scram.build_server_final_message(
                    client_first_bare,
                    server_first,
                    client_final_without_proof,
                    verifier.server_key,
                )

                # AuthenticationSASLFinal
                msg_buf = WriteBuffer.new_message(b'R')
                msg_buf.write_int32(12)
                msg_buf.write_len_prefixed_utf8(server_final)
                msg_buf.end_message()
                self.write(msg_buf)
                self.flush()

                done = True

    def _get_scram_verifier(self, user):
        server = self.server
        roles = server.get_roles()

        rolerec = roles.get(user)
        if rolerec is not None:
            verifier_string = rolerec['password']
            if verifier_string is not None:
                try:
                    verifier = scram.parse_verifier(verifier_string)
                except ValueError:
                    raise errors.AuthenticationError(
                        f'invalid SCRAM verifier for user {user!r}') from None
                is_mock = False
                return verifier, is_mock

        # To avoid revealing the validity of the submitted user name,
        # generate a mock verifier using a salt derived from the
        # received user name and the cluster mock auth nonce.
        # The same approach is taken by Postgres.
        nonce = server.get_instance_data('mock_auth_nonce')
        salt = hashlib.sha256(nonce.encode() + user.encode()).digest()

        verifier = scram.SCRAMVerifier(
            mechanism='SCRAM-SHA-256',
            iterations=scram.DEFAULT_ITERATIONS,
            salt=salt[:scram.DEFAULT_SALT_LENGTH],
            stored_key=b'',
            server_key=b'',
        )
        is_mock = True
        return verifier, is_mock

    async def _execute_script(self, compiled: object, bind_args: bytes):
        cdef:
            pgcon.PGConnection conn
            dbview.DatabaseConnectionView dbv

        if self._cancelled:
            raise ConnectionAbortedError

        dbv = self.get_dbview()
        conn = await self.get_pgcon()

        try:
            await execute.execute_script(
                conn,
                dbv,
                compiled,
                bind_args,
                fe_conn=self,
            )
        finally:
            self.maybe_release_pgcon(conn)

    def _tokenize(self, eql: bytes) -> edgeql.Source:
        text = eql.decode('utf-8')
        if debug.flags.edgeql_disable_normalization:
            return edgeql.Source.from_string(text)
        else:
            return edgeql.NormalizedSource.from_string(text)

    async def _parse(
        self,
        dbview.QueryRequestInfo query_req,
    ) -> dbview.CompiledQuery:
        if self.debug:
            source = query_req.source
            text = source.text()
            self.debug_print('PARSE', text)
            self.debug_print('Cache key', source.cache_key())
            self.debug_print('Extra variables', source.variables(),
                             'after', source.first_extra())

        return await self.get_dbview().parse(query_req)

    cdef parse_cardinality(self, bytes card):
        if card[0] == CARD_MANY.value:
            return CARD_MANY
        elif card[0] == CARD_AT_MOST_ONE.value:
            return CARD_AT_MOST_ONE
        else:
            try:
                card_name = compiler.Cardinality(card[0]).name
            except ValueError:
                raise errors.BinaryProtocolError(
                    f'unknown expected cardinality "{repr(card)[2:-1]}"')
            else:
                raise errors.BinaryProtocolError(
                    f'cardinality {card_name} cannot be requested')

    cdef char render_cardinality(self, query_unit_group) except -1:
        return query_unit_group.cardinality.value

    cdef parse_output_format(self, bytes mode):
        if mode == b'j':
            return FMT_JSON
        elif mode == b'J':
            return FMT_JSON_ELEMENTS
        elif mode == b'b':
            return FMT_BINARY
        elif mode == b'n':
            return FMT_NONE
        else:
            raise errors.BinaryProtocolError(
                f'unknown output mode "{repr(mode)[2:-1]}"')

    cdef inline reject_headers(self):
        cdef int16_t nheaders = self.buffer.read_int16()
        if nheaders != 0:
            raise errors.BinaryProtocolError('unexpected headers')

    cdef dict parse_headers(self):
        cdef:
            dict attrs
            uint16_t num_fields
            str key
            str value

        attrs = {}
        num_fields = <uint16_t>self.buffer.read_int16()
        while num_fields:
            key = self.buffer.read_len_prefixed_utf8()
            value = self.buffer.read_len_prefixed_utf8()
            attrs[key] = value
            num_fields -= 1
        return attrs

    cdef inline ignore_headers(self):
        cdef:
            uint16_t num_fields

        num_fields = <uint16_t>self.buffer.read_int16()
        while num_fields:
            self.buffer.read_len_prefixed_utf8()
            self.buffer.read_len_prefixed_utf8()
            num_fields -= 1

    #############

    cdef WriteBuffer make_negotiate_protocol_version_msg(
        self,
        tuple target_proto,
    ):
        cdef:
            WriteBuffer msg

        # NegotiateProtocolVersion
        msg = WriteBuffer.new_message(b'v')
        # Highest supported major version of the protocol.
        msg.write_int16(target_proto[0])
        # Highest supported minor version of the protocol.
        msg.write_int16(target_proto[1])
        # No extensions are currently supported.
        msg.write_int16(0)

        msg.end_message()
        return msg

    cdef WriteBuffer make_command_data_description_msg(
        self, dbview.CompiledQuery query
    ):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'T')
        msg.write_int16(0)  # no headers
        msg.write_int64(<int64_t><uint64_t>query.query_unit_group.capabilities)
        msg.write_byte(self.render_cardinality(query.query_unit_group))

        in_data = query.query_unit_group.in_type_data
        msg.write_bytes(query.query_unit_group.in_type_id)
        msg.write_len_prefixed_bytes(in_data)

        out_data = query.query_unit_group.out_type_data
        msg.write_bytes(query.query_unit_group.out_type_id)
        msg.write_len_prefixed_bytes(out_data)

        msg.end_message()
        return msg

    cdef WriteBuffer make_state_data_description_msg(self):
        cdef WriteBuffer msg

        type_id, type_data = self.get_dbview().describe_state()

        msg = WriteBuffer.new_message(b's')
        msg.write_bytes(type_id.bytes)
        msg.write_len_prefixed_bytes(type_data)
        msg.end_message()

        return msg

    cdef WriteBuffer make_command_complete_msg(self, capabilities, status):
        cdef:
            WriteBuffer msg

        state_tid, state_data = self.get_dbview().encode_state()

        msg = WriteBuffer.new_message(b'C')
        msg.write_int16(0)  # no headers
        msg.write_int64(<int64_t><uint64_t>capabilities)
        msg.write_len_prefixed_bytes(status)

        msg.write_bytes(state_tid.bytes)
        msg.write_len_prefixed_bytes(state_data)

        return msg.end_message()

    async def _execute_rollback(self, compiled: dbview.CompiledQuery):
        cdef:
            dbview.DatabaseConnectionView _dbview
            pgcon.PGConnection conn

        query_unit = compiled.query_unit_group[0]
        _dbview = self.get_dbview()
        if not (
            query_unit.tx_savepoint_rollback or
            query_unit.tx_rollback or
            query_unit.tx_abort_migration
        ):
            _dbview.raise_in_tx_error()

        conn = await self.get_pgcon()
        try:
            if query_unit.sql:
                await conn.sql_execute(query_unit.sql)

            if query_unit.tx_abort_migration:
                _dbview.clear_tx_error()
            elif query_unit.tx_savepoint_rollback:
                _dbview.rollback_tx_to_savepoint(query_unit.sp_name)
            else:
                assert query_unit.tx_rollback
                _dbview.abort_tx()
        finally:
            self.maybe_release_pgcon(conn)

    async def _execute(
        self,
        compiled: dbview.CompiledQuery,
        bind_args: bytes,
        use_prep_stmt: bint,
    ):
        cdef:
            dbview.DatabaseConnectionView dbv
            pgcon.PGConnection conn

        dbv = self.get_dbview()
        conn = await self.get_pgcon()
        try:
            await execute.execute(
                conn,
                dbv,
                compiled,
                bind_args,
                fe_conn=self,
                use_prep_stmt=use_prep_stmt,
            )
        finally:
            self.maybe_release_pgcon(conn)

        query_unit = compiled.query_unit_group[0]
        if query_unit.system_config:
            self.write_log(
                EdgeSeverity.EDGE_SEVERITY_NOTICE,
                errors.LogMessage.get_code(),
                'server restart is required for the configuration '
                'change to take effect')

    cdef dbview.QueryRequestInfo parse_execute_request(self):
        cdef:
            uint64_t allow_capabilities = 0
            uint64_t compilation_flags = 0
            int64_t implicit_limit = 0
            bint inline_typenames = False
            bint inline_typeids = False
            bint inline_objectids = False
            object output_format
            bint expect_one = False
            bytes query

        allow_capabilities = <uint64_t>self.buffer.read_int64()
        compilation_flags = <uint64_t>self.buffer.read_int64()
        implicit_limit = self.buffer.read_int64()

        if implicit_limit < 0:
            raise errors.BinaryProtocolError(
                f'implicit limit cannot be negative'
            )

        inline_typenames = (
            compilation_flags
            & messages.CompilationFlag.INJECT_OUTPUT_TYPE_NAMES
        )
        inline_typeids = (
            compilation_flags
            & messages.CompilationFlag.INJECT_OUTPUT_TYPE_IDS
        )
        inline_objectids = (
            compilation_flags
            & messages.CompilationFlag.INJECT_OUTPUT_OBJECT_IDS
        )

        output_format = self.parse_output_format(self.buffer.read_byte())
        expect_one = (
            self.parse_cardinality(self.buffer.read_byte()) is CARD_AT_MOST_ONE
        )

        query = self.buffer.read_len_prefixed_bytes()
        if not query:
            raise errors.BinaryProtocolError('empty query')

        state_tid = self.buffer.read_bytes(16)
        state_data = self.buffer.read_len_prefixed_bytes()
        try:
            self.get_dbview().decode_state(state_tid, state_data)
        except errors.StateMismatchError:
            self.write(self.make_state_data_description_msg())
            raise

        return dbview.QueryRequestInfo(
            self._tokenize(query),
            self.protocol_version,
            output_format=output_format,
            expect_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            allow_capabilities=allow_capabilities,
        )

    async def parse(self):
        cdef:
            bytes eql
            dbview.QueryRequestInfo query_req
            WriteBuffer parse_complete
            WriteBuffer buf

        self._last_anon_compiled = None

        self.ignore_headers()

        query_req = self.parse_execute_request()
        compiled = await self._parse(query_req)

        buf = self.make_command_data_description_msg(compiled)

        # Cache compilation result in anticipation that the client
        # will follow up with an Execute immediately.
        #
        # N.B.: we cannot rely on query cache because not all units
        # are cacheable.
        self._last_anon_compiled = compiled
        self._last_anon_compiled_hash = hash(query_req)

        self.write(buf)
        self.flush()

    async def execute(self):
        cdef:
            dbview.QueryRequestInfo query_req
            dbview.DatabaseConnectionView _dbview
            bytes in_tid
            bytes out_tid
            bytes args

        self.ignore_headers()

        query_req = self.parse_execute_request()
        in_tid = self.buffer.read_bytes(16)
        out_tid = self.buffer.read_bytes(16)
        args = self.buffer.read_len_prefixed_bytes()

        self.buffer.finish_message()

        _dbview = self.get_dbview()

        if (
            self._last_anon_compiled is not None and
            hash(query_req) == self._last_anon_compiled_hash and
            in_tid == self._last_anon_compiled.query_unit_group.in_type_id and
            out_tid == self._last_anon_compiled.query_unit_group.out_type_id
        ):
            compiled = self._last_anon_compiled
            query_unit_group = compiled.query_unit_group
        else:
            query_unit_group = _dbview.lookup_compiled_query(query_req)
            if query_unit_group is None:
                if self.debug:
                    self.debug_print('EXECUTE /CACHE MISS', query_req.source.text())

                compiled = await self._parse(query_req)
                query_unit_group = compiled.query_unit_group
                if self._cancelled:
                    raise ConnectionAbortedError
            else:
                compiled = dbview.CompiledQuery(
                    query_unit_group=query_unit_group,
                    first_extra=query_req.source.first_extra(),
                    extra_counts=query_req.source.extra_counts(),
                    extra_blobs=query_req.source.extra_blobs(),
                )

        # Clear the _last_anon_compiled so that the next Execute - if
        # identical - will always lookup in the cache and honor the
        # `cacheable` flag to compile the query again.
        self._last_anon_compiled = None

        if query_unit_group.capabilities & ~query_req.allow_capabilities:
            raise query_unit_group.capabilities.make_error(
                query_req.allow_capabilities,
                errors.DisabledCapabilityError,
            )

        if query_unit_group.in_type_id != in_tid:
            self.write(self.make_command_data_description_msg(compiled))
            raise errors.ParameterTypeMismatchError(
                "specified parameter type(s) do not match the parameter "
                "types inferred from specified command(s)"
            )

        if query_unit_group.out_type_id != out_tid:
            # The client has no up-to-date information about the output,
            # so provide one.
            self.write(self.make_command_data_description_msg(compiled))

        if self.debug:
            self.debug_print('EXECUTE', query_req.source.text())

        metrics.edgeql_query_compilations.inc(1.0, 'cache')
        if (
            _dbview.in_tx_error()
            or query_unit_group[0].tx_savepoint_rollback
            or query_unit_group[0].tx_abort_migration
        ):
            assert len(query_unit_group) == 1
            await self._execute_rollback(compiled)
        elif len(query_unit_group) > 1:
            await self._execute_script(compiled, args)
        else:
            use_prep = (
                len(query_unit_group) == 1
                and bool(query_unit_group[0].sql_hash)
            )
            await self._execute(compiled, args, use_prep)

        if self._cancelled:
            raise ConnectionAbortedError

        if _dbview.is_state_desc_changed():
            self.write(self.make_state_data_description_msg())
        self.write(
            self.make_command_complete_msg(
                compiled.query_unit_group.capabilities,
                compiled.query_unit_group[-1].status,
            )
        )
        self.flush()

    async def sync(self):
        self.buffer.consume_message()
        self.write(self.sync_status())

        if self.debug:
            self.debug_print('SYNC')

        self.flush()

    async def legacy_main(self, params):
        raise NotImplementedError

    async def main(self):
        cdef:
            char mtype
            bint is_legacy

        try:
            params = await self.do_handshake()
            is_legacy = self.protocol_version <= MAX_LEGACY_PROTOCOL
            if not is_legacy:
                await self.auth(params)
        except Exception as ex:
            if self._transport is not None:
                # If there's no transport it means that the connection
                # was aborted, in which case we don't really care about
                # reporting the exception.

                self.write_error(ex)
                self.close()

                if not isinstance(ex, (errors.ProtocolError,
                                        errors.AuthenticationError)):
                    self.loop.call_exception_handler({
                        'message': (
                            'unhandled error in edgedb protocol while '
                            'accepting new connection'
                        ),
                        'exception': ex,
                        'protocol': self,
                        'transport': self._transport,
                        'task': self._main_task,
                    })

            return

        if is_legacy:
            return await self.legacy_main(params)

        self.authed = True
        self.server.on_binary_client_authed(self)

        try:
            while True:
                if self._cancelled:
                    self.abort()
                    return

                if self._stop_requested:
                    break

                if not self.buffer.take_message():
                    if self._passive_mode:
                        # In "passive" mode we only parse what's in the buffer
                        # and return. If there's any unparsed (incomplete) data
                        # in the buffer it's an error.
                        if self.buffer._length:
                            raise RuntimeError(
                                'unparsed data in the read buffer')
                        # Flush whatever data is in the internal buffer before
                        # returning.
                        self.flush()
                        return
                    await self.wait_for_message(report_idling=True)

                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'O':
                        await self.execute()

                    elif mtype == b'P':
                        await self.parse()

                    elif mtype == b'S':
                        await self.sync()

                    elif mtype == b'X':
                        self.close()
                        break

                    elif mtype == b'>':
                        await self.dump()

                    elif mtype == b'<':
                        # The restore protocol cannot send SYNC beforehand,
                        # so if an error occurs the server should send an
                        # ERROR message immediately.
                        await self.restore()

                    elif mtype == b'D':
                        raise errors.BinaryProtocolError(
                            "Describe message (D) is not supported in "
                            "protocol versions greater than 0.13")

                    elif mtype == b'E':
                        raise errors.BinaryProtocolError(
                            "Legacy Execute message (E) is not supported in "
                            "protocol versions greater than 0.13")

                    elif mtype == b'Q':
                        raise errors.BinaryProtocolError(
                            "ExecuteScript message (Q) is not supported in "
                            "protocol versions greater then 0.13")

                    else:
                        self.fallthrough()

                except ConnectionError:
                    raise

                except asyncio.CancelledError:
                    raise

                except Exception as ex:
                    if self._cancelled and \
                            isinstance(ex, pgerror.BackendQueryCancelledError):
                        # If we are cancelling the protocol (means that the
                        # client side of the connection has dropped and we
                        # need to gracefull cleanup and abort) we want to
                        # propagate the BackendQueryCancelledError exception.
                        #
                        # If we're not cancelling, we'll treat it just like
                        # any other error coming from Postgres (a query
                        # might get cancelled due to a variety of reasons.)
                        raise

                    # The connection has been aborted; there's nothing
                    # we can do except shutting this down.
                    if self._con_status == EDGECON_BAD:
                        return

                    self.get_dbview().tx_error()
                    self.buffer.finish_message()

                    self.write_error(ex)
                    self.flush()

                    # The connection was aborted while we were
                    # interpreting the error (via compiler/errmech.py).
                    if self._con_status == EDGECON_BAD:
                        return

                    await self.recover_from_error()

                else:
                    self.buffer.finish_message()

        except asyncio.CancelledError:
            # Happens when the connection is aborted, the backend is
            # being closed and propagates CancelledError to all
            # EdgeCon methods that await on, say, the compiler process.
            # We shouldn't have CancelledErrors otherwise, therefore,
            # in this situation we just silently exit.
            pass

        except (ConnectionError, pgerror.BackendQueryCancelledError):
            pass

        except Exception as ex:
            # We can only be here if an exception occurred during
            # handling another exception, in which case, the only
            # sane option is to abort the connection.

            self.loop.call_exception_handler({
                'message': (
                    'unhandled error in edgedb protocol while '
                    'handling an error'
                ),
                'exception': ex,
                'protocol': self,
                'transport': self._transport,
                'task': self._main_task,
            })

        finally:
            if self._stop_requested:
                self.write_log(
                    EdgeSeverity.EDGE_SEVERITY_NOTICE,
                    errors.LogMessage.get_code(),
                    'server is stopped; disconnecting now')
                self.close()
            else:
                # Abort the connection.
                # It might have already been cleaned up, but abort() is
                # safe to be called on a closed connection.
                self.abort()

    async def recover_from_error(self):
        # Consume all messages until sync.

        while True:

            if not self.buffer.take_message():
                await self.wait_for_message(report_idling=True)
            mtype = self.buffer.get_message_type()

            if mtype == b'S':
                await self.sync()
                return
            else:
                self.buffer.discard_message()

    cdef write_error(self, exc):
        cdef:
            WriteBuffer buf
            int16_t fields_len

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

        exc_code = None

        if isinstance(exc, pgerror.BackendError):
            exc = self.interpret_backend_error(exc)

        fields = {}
        if (isinstance(exc, errors.EdgeDBError) and
                type(exc) is not errors.EdgeDBError):
            exc_code = exc.get_code()
            fields.update(exc._attrs)

        internal_error_code = errors.InternalServerError.get_code()
        if not exc_code:
            exc_code = internal_error_code

        if (exc_code == internal_error_code
                and not fields.get(base_errors.FIELD_HINT)):
            fields[base_errors.FIELD_HINT] = (
                f'This is most likely a bug in EdgeDB. '
                f'Please consider opening an issue ticket '
                f'at https://github.com/edgedb/edgedb/issues/new'
                f'?template=bug_report.md'
            )

        try:
            formatted_error = exc.__formatted_error__
        except AttributeError:
            try:
                formatted_error = ''.join(
                    traceback.format_exception(
                        type(exc), exc, exc.__traceback__,
                        limit=50))
            except Exception:
                formatted_error = 'could not serialize error traceback'

        fields[base_errors.FIELD_SERVER_TRACEBACK] = formatted_error

        buf = WriteBuffer.new_message(b'E')
        buf.write_byte(<char><uint8_t>EdgeSeverity.EDGE_SEVERITY_ERROR)
        buf.write_int32(<int32_t><uint32_t>exc_code)
        buf.write_len_prefixed_utf8(str(exc))
        buf.write_int16(len(fields))
        for k, v in fields.items():
            buf.write_int16(<int16_t><uint16_t>k)
            buf.write_len_prefixed_utf8(str(v))
        buf.end_message()

        self.write(buf)

    cdef interpret_backend_error(self, exc):
        try:
            static_exc = errormech.static_interpret_backend_error(
                exc.fields)

            # only use the backend if schema is required
            if static_exc is errormech.SchemaRequired:
                exc = errormech.interpret_backend_error(
                    self.get_dbview().get_schema(),
                    exc.fields
                )
            elif isinstance(static_exc, (
                    errors.DuplicateDatabaseDefinitionError,
                    errors.UnknownDatabaseError)):
                tenant_id = self.server.get_tenant_id()
                message = static_exc.args[0].replace(f'{tenant_id}_', '')
                exc = type(static_exc)(message)
            else:
                exc = static_exc

        except Exception:
            exc = RuntimeError(
                'unhandled error while calling interpret_backend_error(); '
                'run with EDGEDB_DEBUG_SERVER to debug.')

        return exc

    cdef write_status(self, bytes name, bytes value):
        cdef:
            WriteBuffer buf

        buf = WriteBuffer.new_message(b'S')
        buf.write_len_prefixed_bytes(name)
        buf.write_len_prefixed_bytes(value)
        buf.end_message()

        self.write(buf)

    cdef write_log(self, EdgeSeverity severity, uint32_t code, str message):
        cdef:
            WriteBuffer buf

        if severity >= EdgeSeverity.EDGE_SEVERITY_ERROR:
            # This is an assertion.
            raise errors.InternalServerError(
                'emitting a log message with severity=ERROR')

        buf = WriteBuffer.new_message(b'L')
        buf.write_byte(<char><uint8_t>severity)
        buf.write_int32(<int32_t><uint32_t>code)
        buf.write_len_prefixed_utf8(message)
        buf.write_int16(0)  # number of headers
        buf.end_message()

        self.write(buf)

    cdef sync_status(self):
        cdef:
            WriteBuffer buf
            dbview.DatabaseConnectionView _dbview

        buf = WriteBuffer.new_message(b'Z')
        buf.write_int16(0)  # no headers

        # NOTE: EdgeDB and PostgreSQL current statuses can disagree.
        # For example, Postres can be "PQTRANS_INTRANS" whereas EdgeDB
        # would be "PQTRANS_INERROR". This can happen becuase some of
        # EdgeDB errors can happen at the compile stage, not even
        # reaching Postgres.

        _dbview = self.get_dbview()
        if _dbview.in_tx_error():
            buf.write_byte(b'E')
        elif _dbview.in_tx():
            buf.write_byte(b'T')
        else:
            buf.write_byte(b'I')

        return buf.end_message()

    cdef fallthrough(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == b'H':
            # Flush
            self.buffer.discard_message()
            self.flush()

        elif mtype == b'X':
            # Terminate
            self.buffer.discard_message()
            self.close()

        else:
            raise errors.BinaryProtocolError(
                f'unexpected message type {chr(mtype)!r}')

    def connection_made(self, transport):
        if not self.server._accepting_connections:
            transport.abort()
            return

        if self._con_status != EDGECON_NEW:
            raise errors.BinaryProtocolError(
                'invalid connection status while establishing the connection')
        self._transport = transport

        if self.server._accept_new_tasks:
            self._main_task = self.server.create_task(
                self.main(), interruptable=False
            )
            self.server.on_binary_client_connected(self)
        else:
            transport.abort()

    def connection_lost(self, exc):
        self.server.on_binary_client_disconnected(self)

        # Let's talk about cancellation.
        #
        # 1. Since we need to synchronize the state between Postgres and
        #    EdgeDB, we need to make sure we never do straight asyncio
        #    cancellation while some operation in pgcon is in flight.
        #
        #    Doing that can lead to the following few bad scenarios:
        #
        #       * pgcon connction being wrecked by asyncio.CancelledError;
        #
        #       * pgcon completing its operation and then, a rogue
        #         CancelledError preventing us to apply the new state
        #         to dbview/server config/etc.
        #
        # 2. It is safe to cancel `_msg_take_waiter` though. Cancelling it
        #    would abort protocol parsing, but there's no global state that
        #    needs syncing in protocol messages.
        #
        # 3. We can interrupt some operations like auth with a CancelledError.
        #    Again, those operations don't mutate global state.

        if (self._msg_take_waiter is not None and
                not self._msg_take_waiter.done()):
            # We're parsing the protocol. We can abort that.
            self._msg_take_waiter.cancel()

        if (
            self._main_task is not None
            and not self._main_task.done()
            and not self._cancelled
        ):

            # The main connection handling task is up and running.

            # First, let's set a flag to signal that we should cancel soon;
            # after all the client has already disconnected.
            self._cancelled = True

            if not self.authed:
                # We must be still authenticating. We can abort that.
                self._main_task.cancel()
            else:
                if (
                    self._pinned_pgcon is not None
                    and not self._pinned_pgcon.idle
                ):
                    # Looks like we have a Postgres connection acquired and
                    # it's actively running some command for us.  To make
                    # sure we're not leaving behind a heavy query, perform
                    # an explicit Postgres cancellation because a mere
                    # connection drop wouldn't necessarily abort the query
                    # right away). Additionally, we must discard the connection
                    # as we cannot be completely sure about its state. Postgres
                    # cancellation is signal-based and is addressed to a whole
                    # connection and not a concrete operation. The result is
                    # that we might be racing with the currently running query
                    # and if that completes before the cancellation signal
                    # reaches the backend, we'll be setting a trap for the
                    # _next_ query that is unlucky enough to pick up this
                    # Postgres backend from the connection pool.
                    # TODO(fantix): hold server shutdown to complete this task
                    if self.server._accept_new_tasks:
                        self.server.create_task(
                            self.server._cancel_and_discard_pgcon(
                                self._pinned_pgcon,
                                self.get_dbview().dbname,
                            ),
                            interruptable=False,
                        )
                    # Prevent the main task from releasing the same connection
                    # twice. This flag is for now only used in this case.
                    self._pgcon_released_in_connection_lost = True

                # In all other cases, we can just wait until the `main()`
                # coroutine notices that `self._cancelled` was set.
                # It would be a mistake to cancel the main task here, as it
                # could be unpacking results from pgcon and applying them
                # to the global state.
                #
                # Ultimately, the main() coroutine will be aborted, eventually,
                # and will call `self.abort()` to shut all things down.
        else:
            # The `main()` coroutine isn't running, it means that the
            # connection is already pretty much dead.  Nonetheless, call
            # abort() to make sure we've cleaned everything up properly.
            self.abort()

    def data_received(self, data):
        self.buffer.feed_data(data)
        if self._msg_take_waiter is not None and self.buffer.take_message():
            self._msg_take_waiter.set_result(True)
            self._msg_take_waiter = None

    def eof_received(self):
        pass

    def pause_writing(self):
        if self._write_waiter and not self._write_waiter.done():
            return
        self._write_waiter = self.loop.create_future()

    def resume_writing(self):
        if not self._write_waiter or self._write_waiter.done():
            return
        self._write_waiter.set_result(True)

    async def dump(self):
        cdef:
            WriteBuffer msg_buf
            dbview.DatabaseConnectionView _dbview

        self.reject_headers()
        self.buffer.finish_message()

        _dbview = self.get_dbview()
        if _dbview.txid:
            raise errors.ProtocolError(
                'DUMP must not be executed while in transaction'
            )

        server = self.server
        compiler_pool = server.get_compiler_pool()

        dbname = _dbview.dbname
        pgcon = await server.acquire_pgcon(dbname)
        self._in_dump_restore = True
        try:
            # To avoid having races, we want to:
            #
            #   1. start a transaction;
            #
            #   2. in the compiler process we connect to that transaction
            #      and re-introspect the schema in it.
            #
            #   3. all dump worker pg connection would work on the same
            #      connection.
            #
            # This guarantees that every pg connection and the compiler work
            # with the same DB state.

            await pgcon.sql_execute(
                b'''START TRANSACTION
                        ISOLATION LEVEL SERIALIZABLE
                        READ ONLY
                        DEFERRABLE;

                    -- Disable transaction or query execution timeout
                    -- limits. Both clients and the server can be slow
                    -- during the dump/restore process.
                    SET idle_in_transaction_session_timeout = 0;
                    SET statement_timeout = 0;
                ''',
            )

            user_schema = await server.introspect_user_schema(pgcon)
            global_schema = await server.introspect_global_schema(pgcon)
            db_config = await server.introspect_db_config(pgcon)
            dump_protocol = self.max_protocol

            schema_ddl, schema_dynamic_ddl, schema_ids, blocks = (
                await compiler_pool.describe_database_dump(
                    user_schema,
                    global_schema,
                    db_config,
                    dump_protocol,
                )
            )

            if schema_dynamic_ddl:
                for query in schema_dynamic_ddl:
                    result = await pgcon.sql_fetch_val(query.encode('utf-8'))
                    if result:
                        schema_ddl += '\n' + result.decode('utf-8')

            msg_buf = WriteBuffer.new_message(b'@')

            msg_buf.write_int16(3)  # number of headers
            msg_buf.write_int16(DUMP_HEADER_BLOCK_TYPE)
            msg_buf.write_len_prefixed_bytes(DUMP_HEADER_BLOCK_TYPE_INFO)
            msg_buf.write_int16(DUMP_HEADER_SERVER_VER)
            msg_buf.write_len_prefixed_utf8(str(buildmeta.get_version()))
            msg_buf.write_int16(DUMP_HEADER_SERVER_TIME)
            msg_buf.write_len_prefixed_utf8(str(int(time.time())))

            msg_buf.write_int16(dump_protocol[0])
            msg_buf.write_int16(dump_protocol[1])
            msg_buf.write_len_prefixed_utf8(schema_ddl)

            msg_buf.write_int32(len(schema_ids))
            for (tn, td, tid) in schema_ids:
                msg_buf.write_len_prefixed_utf8(tn)
                msg_buf.write_len_prefixed_utf8(td)
                assert len(tid) == 16
                msg_buf.write_bytes(tid)  # uuid

            msg_buf.write_int32(len(blocks))
            for block in blocks:
                assert len(block.schema_object_id.bytes) == 16
                msg_buf.write_bytes(block.schema_object_id.bytes)  # uuid
                msg_buf.write_len_prefixed_bytes(block.type_desc)

                msg_buf.write_int16(len(block.schema_deps))
                for depid in block.schema_deps:
                    assert len(depid.bytes) == 16
                    msg_buf.write_bytes(depid.bytes)  # uuid

            self._transport.write(memoryview(msg_buf.end_message()))
            self.flush()

            blocks_queue = collections.deque(blocks)
            output_queue = asyncio.Queue(maxsize=2)

            async with taskgroup.TaskGroup() as g:
                g.create_task(pgcon.dump(
                    blocks_queue,
                    output_queue,
                    DUMP_BLOCK_SIZE,
                ))

                nstops = 0
                while True:
                    if self._cancelled:
                        raise ConnectionAbortedError

                    out = await output_queue.get()
                    if out is None:
                        nstops += 1
                        if nstops == 1:
                            # we only have one worker right now
                            break
                    else:
                        block, block_num, data = out

                        msg_buf = WriteBuffer.new_message(b'=')
                        msg_buf.write_int16(4)  # number of headers

                        msg_buf.write_int16(DUMP_HEADER_BLOCK_TYPE)
                        msg_buf.write_len_prefixed_bytes(
                            DUMP_HEADER_BLOCK_TYPE_DATA)
                        msg_buf.write_int16(DUMP_HEADER_BLOCK_ID)
                        msg_buf.write_len_prefixed_bytes(
                            block.schema_object_id.bytes)
                        msg_buf.write_int16(DUMP_HEADER_BLOCK_NUM)
                        msg_buf.write_len_prefixed_bytes(
                            str(block_num).encode())
                        msg_buf.write_int16(DUMP_HEADER_BLOCK_DATA)
                        msg_buf.write_len_prefixed_buffer(data)

                        self._transport.write(memoryview(msg_buf.end_message()))
                        if self._write_waiter:
                            await self._write_waiter

            await pgcon.sql_execute(b"ROLLBACK;")

        finally:
            self._in_dump_restore = False
            server.release_pgcon(dbname, pgcon)

        msg_buf = WriteBuffer.new_message(b'C')
        msg_buf.write_int16(0)  # no headers
        msg_buf.write_int64(0)  # capabilities
        msg_buf.write_len_prefixed_bytes(b'DUMP')
        msg_buf.write_bytes(sertypes.NULL_TYPE_ID.bytes)
        msg_buf.write_len_prefixed_bytes(b'')
        self.write(msg_buf.end_message())
        self.flush()

    async def _execute_utility_stmt(self, eql: str, pgcon):
        cdef dbview.DatabaseConnectionView _dbview

        query_req = dbview.QueryRequestInfo(edgeql.Source.from_string(eql),
                                            self.protocol_version)

        _dbview = self.get_dbview()

        compiled = await _dbview.parse(query_req)
        query_unit_group = compiled.query_unit_group
        assert len(query_unit_group) == 1
        query_unit = query_unit_group[0]

        try:
            _dbview.start(query_unit)
            await pgcon.sql_execute(query_unit.sql)
        except Exception:
            _dbview.on_error()
            if (
                query_unit.tx_commit and
                not pgcon.in_tx() and
                _dbview.in_tx()
            ):
                # The COMMIT command has failed. Our Postgres connection
                # isn't in a transaction anymore. Abort the transaction
                # in dbview.
                _dbview.abort_tx()
            raise
        else:
            _dbview.on_success(query_unit, {})

    async def restore(self):
        cdef:
            WriteBuffer msg_buf
            char mtype
            dbview.DatabaseConnectionView _dbview

        _dbview = self.get_dbview()
        if _dbview.txid:
            raise errors.ProtocolError(
                'RESTORE must not be executed while in transaction'
            )

        self.reject_headers()
        self.buffer.read_int16()  # discard -j level

        # Now parse the embedded dump header message:

        server = self.server
        compiler_pool = server.get_compiler_pool()

        global_schema = _dbview.get_global_schema()
        user_schema = _dbview.get_user_schema()

        dump_server_ver_str = None
        headers_num = self.buffer.read_int16()
        for _ in range(headers_num):
            hdrname = self.buffer.read_int16()
            hdrval = self.buffer.read_len_prefixed_bytes()
            if hdrname == DUMP_HEADER_SERVER_VER:
                dump_server_ver_str = hdrval.decode('utf-8')

        proto_major = self.buffer.read_int16()
        proto_minor = self.buffer.read_int16()
        proto = (proto_major, proto_minor)
        if proto > DUMP_VER_MAX or proto < DUMP_VER_MIN:
            raise errors.ProtocolError(
                f'unsupported dump version {proto_major}.{proto_minor}')

        schema_ddl = self.buffer.read_len_prefixed_bytes()

        ids_num = self.buffer.read_int32()
        schema_ids = []
        for _ in range(ids_num):
            schema_ids.append((
                self.buffer.read_len_prefixed_utf8(),
                self.buffer.read_len_prefixed_utf8(),
                self.buffer.read_bytes(16),
            ))

        block_num = <uint32_t>self.buffer.read_int32()
        blocks = []
        for _ in range(block_num):
            blocks.append((
                self.buffer.read_bytes(16),
                self.buffer.read_len_prefixed_bytes(),
            ))

            # Ignore deps info
            for _ in range(self.buffer.read_int16()):
                self.buffer.read_bytes(16)

        self.buffer.finish_message()
        dbname = _dbview.dbname
        pgcon = await server.acquire_pgcon(dbname)

        self._in_dump_restore = True
        try:
            _dbview.decode_state(sertypes.NULL_TYPE_ID.bytes, b'')
            await self._execute_utility_stmt(
                'START TRANSACTION ISOLATION SERIALIZABLE',
                pgcon,
            )

            await pgcon.sql_execute(
                b'''
                    -- Disable transaction or query execution timeout
                    -- limits. Both clients and the server can be slow
                    -- during the dump/restore process.
                    SET idle_in_transaction_session_timeout = 0;
                    SET statement_timeout = 0;
                ''',
            )

            schema_sql_units, restore_blocks, tables = \
                await compiler_pool.describe_database_restore(
                    user_schema,
                    global_schema,
                    dump_server_ver_str,
                    schema_ddl,
                    schema_ids,
                    blocks,
                    proto,
                )

            for query_unit in schema_sql_units:
                new_types = None
                _dbview.start(query_unit)

                try:
                    if query_unit.config_ops:
                        for op in query_unit.config_ops:
                            if op.scope is config.ConfigScope.INSTANCE:
                                raise errors.ProtocolError(
                                    'CONFIGURE INSTANCE cannot be executed'
                                    ' in dump restore'
                                )

                    if query_unit.sql:
                        if query_unit.ddl_stmt_id:
                            ddl_ret = await pgcon.run_ddl(query_unit)
                            if ddl_ret and ddl_ret['new_types']:
                                new_types = ddl_ret['new_types']
                        else:
                            await pgcon.sql_execute(query_unit.sql)
                except Exception:
                    _dbview.on_error()
                    raise
                else:
                    _dbview.on_success(query_unit, new_types)

            restore_blocks = {
                b.schema_object_id: b
                for b in restore_blocks
            }

            disable_trigger_q = ''
            enable_trigger_q = ''
            for table in tables:
                disable_trigger_q += (
                    f'ALTER TABLE {table} DISABLE TRIGGER ALL;'
                )
                enable_trigger_q += (
                    f'ALTER TABLE {table} ENABLE TRIGGER ALL;'
                )

            await pgcon.sql_execute(disable_trigger_q.encode())

            # Send "RestoreReadyMessage"
            msg = WriteBuffer.new_message(b'+')
            msg.write_int16(0)  # no headers
            msg.write_int16(1)  # -j1
            self.write(msg.end_message())
            self.flush()

            while True:
                if not self.buffer.take_message():
                    # Don't report idling when restoring a dump.
                    # This is an edge case and the client might be
                    # legitimately slow.
                    await self.wait_for_message(report_idling=False)
                mtype = self.buffer.get_message_type()

                if mtype == b'=':
                    block_type = None
                    block_id = None
                    block_num = None
                    block_data = None

                    num_headers = self.buffer.read_int16()
                    for _ in range(num_headers):
                        header = self.buffer.read_int16()
                        if header == DUMP_HEADER_BLOCK_TYPE:
                            block_type = self.buffer.read_len_prefixed_bytes()
                        elif header == DUMP_HEADER_BLOCK_ID:
                            block_id = self.buffer.read_len_prefixed_bytes()
                            block_id = pg_UUID(block_id)
                        elif header == DUMP_HEADER_BLOCK_NUM:
                            block_num = self.buffer.read_len_prefixed_bytes()
                        elif header == DUMP_HEADER_BLOCK_DATA:
                            block_data = self.buffer.read_len_prefixed_bytes()

                    self.buffer.finish_message()

                    if (block_type is None or block_id is None
                            or block_num is None or block_data is None):
                        raise errors.ProtocolError('incomplete data block')

                    restore_block = restore_blocks[block_id]
                    type_id_map = self._build_type_id_map_for_restore_mending(
                        restore_block)
                    self._transport.pause_reading()
                    await pgcon.restore(restore_block, block_data, type_id_map)
                    self._transport.resume_reading()

                elif mtype == b'.':
                    self.buffer.finish_message()
                    break

                else:
                    self.fallthrough()

            await pgcon.sql_execute(enable_trigger_q.encode())

        except Exception:
            await pgcon.sql_execute(b'ROLLBACK')
            _dbview.abort_tx()
            raise

        else:
            await self._execute_utility_stmt('COMMIT', pgcon)

        finally:
            self._transport.resume_reading()
            self._in_dump_restore = False
            server.release_pgcon(dbname, pgcon)

        await server.introspect_db(dbname)

        if _dbview.is_state_desc_changed():
            self.write(self.make_state_data_description_msg())

        state_tid, state_data = _dbview.encode_state()

        msg = WriteBuffer.new_message(b'C')
        msg.write_int16(0)  # no headers
        msg.write_int64(0)  # capabilities
        msg.write_len_prefixed_bytes(b'RESTORE')
        msg.write_bytes(state_tid.bytes)
        msg.write_len_prefixed_bytes(state_data)
        self.write(msg.end_message())
        self.flush()

    def _build_type_id_map_for_restore_mending(self, restore_block):
        type_map = {}
        descriptor_stack = []

        if not restore_block.data_mending_desc:
            return type_map

        descriptor_stack.append(restore_block.data_mending_desc)
        while descriptor_stack:
            desc_tuple = descriptor_stack.pop()
            for desc in desc_tuple:
                if desc is not None:
                    type_map[desc.schema_type_id] = (
                        self.get_dbview().resolve_backend_type_id(
                            desc.schema_type_id,
                        )
                    )

                    descriptor_stack.append(desc.elements)

        return type_map


@cython.final
cdef class VirtualTransport:
    def __init__(self):
        self.buf = WriteBuffer.new()
        self.closed = False

    def write(self, data):
        self.buf.write_bytes(bytes(data))

    def _get_data(self):
        return bytes(self.buf)

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True

    def abort(self):
        self.closed = True


async def eval_buffer(
    server,
    database: str,
    data: bytes,
    conn_params: dict[str, str],
    protocol_version: tuple[int, int],
    auth_data: bytes,
    transport: srvargs.ServerConnTransport,
):
    cdef:
        VirtualTransport vtr
        EdgeConnection proto

    vtr = VirtualTransport()

    proto = new_edge_connection(
        server,
        passive=True,
        auth_data=auth_data,
        transport=transport,
        conn_params=conn_params,
        protocol_version=protocol_version,
    )

    proto.connection_made(vtr)
    if vtr.is_closing() or proto._main_task is None:
        raise RuntimeError(
            'cannot process the request, the server is shutting down')

    try:
        await proto._start_connection(database)
        proto.data_received(data)
        await proto._main_task
    except Exception as ex:
        proto.connection_lost(ex)
    else:
        proto.connection_lost(None)

    data = vtr._get_data()
    return data


include "binary_v0.pyx"


def new_edge_connection(
    server,
    *,
    external_auth: bool = False,
    passive: bool = False,
    transport: srvargs.ServerConnTransport = (
        srvargs.ServerConnTransport.TCP),
    auth_data: bytes = b'',
    protocol_version: tuple[int, int] = edbdef.CURRENT_PROTOCOL,
    conn_params: dict[str, str] | None = None,
):
    return EdgeConnectionBackwardsCompatible(
        server,
        external_auth=external_auth,
        passive=passive,
        transport=transport,
        auth_data=auth_data,
        protocol_version=protocol_version,
        conn_params=conn_params,
    )


async def run_script(
    server,
    database: str,
    user: str,
    script: str,
) -> None:
    cdef:
        EdgeConnection conn
        dbview.CompiledQuery compiled
    conn = new_edge_connection(server)
    await conn._start_connection(database)
    try:
        compiled = await conn.get_dbview().parse(
            dbview.QueryRequestInfo(
                edgeql.Source.from_string(script),
                conn.protocol_version,
                output_format=FMT_NONE,
            )
        )
        if len(compiled.query_unit_group) > 1:
            await conn._execute_script(compiled, b'')
        else:
            await conn._execute(compiled, b'', use_prep_stmt=0)
    except pgerror.BackendError as e:
        exc = conn.interpret_backend_error(e)
        if isinstance(exc, errors.EdgeDBError):
            raise exc from None
        else:
            raise exc
    finally:
        conn.close()
