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
import collections
import contextlib
import hashlib
import json
import logging
import time
import statistics
import traceback

cimport cython
cimport cpython

from typing import Dict, List, Optional, Sequence
from . cimport cpythonx

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

import immutables

from edb import edgeql
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

from edb.server import buildmeta
from edb.server import compiler
from edb.server import defines as edbdef
from edb.server.compiler import errormech
from edb.server.compiler import enums
from edb.server.pgcon cimport pgcon
from edb.server.pgcon import errors as pgerror

from edb.schema import objects as s_obj

from edb import errors
from edb.errors import base as base_errors, EdgeQLSyntaxError
from edb.common import debug, taskgroup
from edb.common import context as pctx

from edgedb import scram


include "./consts.pxi"


DEF FLUSH_BUFFER_AFTER = 100_000
cdef bytes ZERO_UUID = b'\x00' * 16
cdef bytes EMPTY_TUPLE_UUID = s_obj.get_known_type_id('empty-tuple').bytes

cdef object CARD_NO_RESULT = compiler.ResultCardinality.NO_RESULT
cdef object CARD_ONE = compiler.ResultCardinality.ONE
cdef object CARD_MANY = compiler.ResultCardinality.MANY

cdef object FMT_BINARY = compiler.IoFormat.BINARY
cdef object FMT_JSON = compiler.IoFormat.JSON
cdef object FMT_JSON_ELEMENTS = compiler.IoFormat.JSON_ELEMENTS
cdef object FMT_SCRIPT = compiler.IoFormat.SCRIPT

cdef tuple DUMP_VER_MIN = (0, 7)
cdef tuple DUMP_VER_MAX = (0, 9)

cdef object logger = logging.getLogger('edb.server')
cdef object log_metrics = logging.getLogger('edb.server.metrics')

DEF QUERY_OPT_IMPLICIT_LIMIT = 0xFF01
DEF QUERY_OPT_INLINE_TYPENAMES = 0xFF02
DEF QUERY_OPT_INLINE_TYPEIDS = 0xFF03
DEF QUERY_OPT_ALLOW_CAPABILITIES = 0xFF04

DEF SERVER_HEADER_CAPABILITIES = 0x1001

DEF ALL_CAPABILITIES = 0xFFFFFFFFFFFFFFFF


def parse_capabilities_header(value: bytes) -> uint64_t:
    if len(value) != 8:
        raise errors.BinaryProtocolError(
            f'capabilities header must be exactly 8 bytes'
        )
    cdef uint64_t mask = hton.unpack_uint64(cpython.PyBytes_AS_STRING(value))
    return mask


@cython.final
cdef class QueryRequestInfo:

    def __cinit__(
        self,
        source: edgeql.Source,
        io_format: object,
        expect_one: bint,
        implicit_limit: int,
        inline_typeids: bint,
        inline_typenames: bint,
        allow_capabilities: uint64_t,
    ):
        self.source = source
        self.io_format = io_format
        self.expect_one = expect_one
        self.implicit_limit = implicit_limit
        self.inline_typeids = inline_typeids
        self.inline_typenames = inline_typenames
        self.allow_capabilities = allow_capabilities

        self.cached_hash = hash((
            self.source.cache_key(),
            self.io_format,
            self.expect_one,
            self.implicit_limit,
            self.inline_typeids,
            self.inline_typenames,
        ))

    def __hash__(self):
        return self.cached_hash

    def __eq__(self, other: QueryRequestInfo) -> bool:
        return (
            self.source.cache_key() == other.source.cache_key() and
            self.io_format == other.io_format and
            self.expect_one == other.expect_one and
            self.implicit_limit == other.implicit_limit and
            self.inline_typeids == other.inline_typeids and
            self.inline_typenames == other.inline_typenames
        )


@cython.final
cdef class CompiledQuery:

    def __init__(self, object query_unit,
        first_extra: Optional[int]=None,
        int extra_count=0,
        bytes extra_blob=None
    ):
        self.query_unit = query_unit
        self.first_extra = first_extra
        self.extra_count = extra_count
        self.extra_blob = extra_blob


@cython.final
cdef class EdgeConnection:

    def __init__(self, server, external_auth: bool = False,
            max_protocol: tuple = CURRENT_PROTOCOL):
        self._con_status = EDGECON_NEW
        self._id = server.on_client_connected()
        self.port = server
        self._external_auth = external_auth

        self.loop = server.get_loop()
        self.dbview = None
        self._backend = None

        self._transport = None
        self.buffer = ReadBuffer()

        self._parsing = True
        self._reading_messages = False

        self._main_task = None
        self._msg_take_waiter = None
        self._write_waiter = None

        self._last_anon_compiled = None

        self._write_buf = None

        self.debug = debug.flags.server_proto
        self.query_cache_enabled = not (debug.flags.disable_qcache or
                                        debug.flags.edgeql_compile)

        self.server = server
        self.authed = False

        self.protocol_version = max_protocol
        self.max_protocol = max_protocol
        self.timer = Timer()

    def on_remote_ddl(self, dbver):
        if not self.dbview:
            return
        self.dbview.on_remote_ddl(dbver)

    def on_remote_config_change(self):
        if not self.dbview:
            return
        self.write_log(
            EdgeSeverity.EDGE_SEVERITY_DEBUG,
            errors.LogMessage.get_code(),
            'received configuration reload request',
        )
        self.dbview.on_remote_config_change()

    cdef get_backend(self):
        if self._con_status is EDGECON_BAD:
            # `self.sync()` is called from `recover_from_error`;
            # at that point the client might have been disconnected
            # because of the very same error in the protocol. So check
            # if we're still connected before trying to call methods
            # on pgcon.
            raise ConnectionAbortedError()

        if self._con_status in {EDGECON_STARTED, EDGECON_OK}:
            if self._backend is None:
                raise RuntimeError('backend is not available')
            return self._backend

        raise RuntimeError('requesting backend before it is initialized')

    def debug_print(self, *args):
        print(
            '::EDGEPROTO::',
            f'id:{self._id}',
            f'in_tx:{int(self.dbview.in_tx())}',
            f'tx_error:{int(self.dbview.in_tx_error())}',
            *args,
        )

    cdef write(self, WriteBuffer buf):
        # One rule for this method: don't write partial messages.
        if self._write_buf is not None:
            self._write_buf.write_buffer(buf)
            if self._write_buf.len() >= FLUSH_BUFFER_AFTER:
                self.flush()
        else:
            self._write_buf = buf

    cdef abort(self):
        self._con_status = EDGECON_BAD
        if self._transport is not None:
            self._transport.abort()
            self._transport = None
        if self._backend is not None:
            self.loop.create_task(self._backend.close())
            self._backend = None
        self.timer.log_all_stats()

    async def close(self):
        self._con_status = EDGECON_BAD
        if self._transport is not None:
            self.flush()
            self._transport.close()
            self._transport = None
        if self._backend is not None:
            await self._backend.close()
            self._backend = None
        self.timer.log_all_stats()

    cdef flush(self):
        if self._transport is None:
            # could be if the connection is lost and a coroutine
            # method is finalizing.
            raise ConnectionAbortedError
        if self._write_buf is not None and self._write_buf.len():
            buf = self._write_buf
            self._write_buf = None
            self._transport.write(buf)

    async def wait_for_message(self):
        if self.buffer.take_message():
            return
        if self._transport is None:
            # could be if the connection is lost and a coroutine
            # method is finalizing.
            raise ConnectionAbortedError
        self._msg_take_waiter = self.loop.create_future()
        await self._msg_take_waiter

    async def auth(self):
        cdef:
            char mtype
            WriteBuffer msg_buf
            WriteBuffer buf

        await self.wait_for_message()
        mtype = self.buffer.get_message_type()
        if mtype != b'V':
            raise errors.BinaryProtocolError(
                f'unexpected initial message: {mtype}, expected "V"')

        params = await self.do_handshake()

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

        if database == edbdef.EDGEDB_TEMPLATE_DB:
            # Prevent connections to the system template database,
            # which only purpose is to serve as a template for new
            # databases.
            raise errors.AccessError(
                f'database {edbdef.EDGEDB_TEMPLATE_DB!r} does not '
                f'accept connections'
            )

        await self._start_connection(database, user)

        # The user has already been authenticated by other means
        # (such as the ability to write to a protected socket).
        if self._external_auth:
            authmethod_name = 'Trust'
        else:
            authmethod = await self.port.get_server().get_auth_method(
                user, self._transport)
            authmethod_name = type(authmethod).__name__

        if authmethod_name == 'SCRAM':
            await self._auth_scram(user)
        elif authmethod_name == 'Trust':
            await self._auth_trust(user)
        else:
            raise errors.InternalServerError(
                f'unimplemented auth method: {authmethod_name}')

        logger.debug('successfully authenticated %s in database %s',
                     user, database)

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

        if self.port.in_dev_mode():
            pgaddr = dict(self.get_backend().pgcon.get_pgaddr())
            if pgaddr.get('password'):
                pgaddr['password'] = '********'
            msg_buf = WriteBuffer.new_message(b'S')
            msg_buf.write_len_prefixed_bytes(b'pgaddr')
            msg_buf.write_len_prefixed_utf8(json.dumps(pgaddr))
            msg_buf.end_message()
            buf.write_buffer(msg_buf)

        msg_buf = WriteBuffer.new_message(b'Z')
        msg_buf.write_int16(0)  # no headers
        msg_buf.write_byte(b'I')
        msg_buf.end_message()
        buf.write_buffer(msg_buf)

        self.write(buf)
        self.flush()

    async def do_handshake(self):
        cdef:
            uint16_t major
            uint16_t minor
            int i
            uint16_t nexts
            dict exts = {}
            dict params = {}

        major = <uint16_t>self.buffer.read_int16()
        minor = <uint16_t>self.buffer.read_int16()

        nparams = <uint16_t>self.buffer.read_int16()
        for i in range(nparams):
            k = self.buffer.read_len_prefixed_utf8()
            v = self.buffer.read_len_prefixed_utf8()
            params[k] = v

        nexts = <uint16_t>self.buffer.read_int16()

        for i in range(nexts):
            extname = self.buffer.read_len_prefixed_utf8()
            exts[extname] = self.parse_headers()

        self.buffer.finish_message()

        self.protocol_version = major, minor
        negotiate = nexts > 0
        if self.protocol_version < MIN_PROTOCOL:
            target_proto = MIN_PROTOCOL
            negotiate = True
        elif self.protocol_version > self.max_protocol:
            target_proto = self.max_protocol
            negotiate = True
        else:
            target_proto = self.protocol_version

        if negotiate:
            # NegotiateProtocolVersion
            buf = WriteBuffer.new_message(b'v')
            # Highest supported major version of the protocol.
            buf.write_int16(target_proto[0])
            # Highest supported minor version of the protocol.
            buf.write_int16(target_proto[1])
            # No extensions are currently supported.
            buf.write_int16(0)
            buf.end_message()

            self.write(buf)
            self.flush()

        return params

    @classmethod
    async def run_script(
        cls,
        server,
        database: str,
        user: str,
        script: str,
    ) -> None:
        conn = cls(server)
        await conn._start_connection(database, user)
        try:
            await conn._simple_query(
                script.encode('utf-8'),
                ALL_CAPABILITIES,
            )
        except pgerror.BackendError as e:
            exc = await conn._interpret_backend_error(e)
            if isinstance(exc, errors.EdgeDBError):
                raise exc from None
            else:
                raise exc
        finally:
            await conn.close()

    async def _start_connection(self, database: str, user: str) -> None:
        dbv = self.port.new_view(
            dbname=database,
            user=user,
            query_cache=self.query_cache_enabled,
        )
        assert type(dbv) is dbview.DatabaseConnectionView
        self.dbview = <dbview.DatabaseConnectionView>dbv

        self._backend = await self.port.new_backend(
            dbname=database, dbver=self.dbview.dbver)
        self._backend.pgcon.set_edgecon(self)
        self._con_status = EDGECON_STARTED

    async def _get_role_record(self, user):
        conn = self.get_backend().pgcon
        server = self.port.get_server()
        role_query = await server.get_sys_query(conn, 'role')
        json_data = await conn.parse_execute_json(
            role_query, b'__sys_role',
            dbver=b'', use_prep_stmt=True, args=(user,),
        )

        if json_data is not None:
            return json.loads(json_data.decode('utf-8'))
        else:
            return None

    async def _auth_trust(self, user):
        rolerec = await self._get_role_record(user)
        if rolerec is None:
            raise errors.AuthenticationError('authentication failed')

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
                await self.wait_for_message()
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

                verifier, mock_auth = await self._get_scram_verifier(user)
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
                        'client users SASL authorization identity, '
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

    async def _get_scram_verifier(self, user):
        rolerec = await self._get_role_record(user)
        if rolerec is not None:
            verifier_string = rolerec['password']
            if verifier_string is None:
                raise errors.AuthenticationError(
                    f'invalid SCRAM verifier for user {user!r}')

            try:
                verifier = scram.parse_verifier(verifier_string)
            except ValueError:
                raise errors.AuthenticationError(
                    f'invalid SCRAM verifier for user {user!r}') from None

            is_mock = False
        else:
            # To avoid revealing the validity of the submitted user name,
            # generate a mock verifier using a salt derived from the
            # received user name and the cluster mock auth nonce.
            # The same approach is taken by Postgres.
            server = self.port.get_server()
            nonce = await server.get_instance_data(
                self.get_backend().pgcon, 'mock_auth_nonce')
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

    async def recover_current_tx_info(self):
        ret = await self.get_backend().pgcon.simple_query(b'''
            SELECT s1.name AS n, s1.value AS v, s1.type AS t
                FROM _edgecon_state s1
            UNION ALL
            SELECT '' AS n, s2.sp_id::text AS v, 'S' AS t
                FROM _edgecon_current_savepoint s2;
        ''', ignore_data=False)

        conf = aliases = immutables.Map()
        sp_id = None

        if ret:
            for sname, svalue, stype in ret:
                sname = sname.decode()
                svalue = svalue.decode()

                if stype == b'C':
                    setting = config.get_settings()[sname]
                    pyval = config.value_from_json(setting, svalue)
                    conf = config.set_value(conf, sname, pyval, 'session')
                elif stype == b'A':
                    if not sname:
                        sname = None
                    aliases = aliases.set(sname, svalue)
                elif stype == b'S':
                    assert not sname
                    sp_id = int(svalue)
                # Ignore everything else in the state table.

        if self.debug:
            self.debug_print('RECOVER SP/ALIAS/CONF', sp_id, aliases, conf)

        if self.dbview.in_tx():
            self.dbview.rollback_tx_to_savepoint(sp_id, aliases, conf)
        else:
            self.dbview.recover_aliases_and_config(aliases, conf)

    #############

    async def _compile(
        self,
        query_req: QueryRequestInfo,
        *,
        stmt_mode: str = 'single',
    ):
        if self.dbview.in_tx_error():
            self.dbview.raise_in_tx_error()

        if self.dbview.in_tx():
            return await self.get_backend().compiler.call(
                'compile_in_tx',
                self.dbview.txid,
                query_req.source,
                query_req.io_format,
                query_req.expect_one,
                query_req.implicit_limit,
                query_req.inline_typeids,
                query_req.inline_typenames,
                stmt_mode,
            )
        else:
            return await self.get_backend().compiler.call(
                'compile',
                self.dbview.dbver,
                query_req.source,
                self.dbview.modaliases,
                self.dbview.get_session_config(),
                query_req.io_format,
                query_req.expect_one,
                query_req.implicit_limit,
                query_req.inline_typeids,
                query_req.inline_typenames,
                stmt_mode,
            )

    async def _compile_script(
        self,
        query: bytes,
        *,
        stmt_mode: str = 'single',
    ):
        with self.timer.timed("Query tokenization"):
            source = edgeql.Source.from_string(query.decode('utf-8'))

        if self.dbview.in_tx_error():
            self.dbview.raise_in_tx_error()

        if self.dbview.in_tx():
            return await self.get_backend().compiler.call(
                'compile_in_tx',
                self.dbview.txid,
                source,
                FMT_SCRIPT,
                False,
                0,
                False,
                False,
                stmt_mode,
            )
        else:
            return await self.get_backend().compiler.call(
                'compile',
                self.dbview.dbver,
                source,
                self.dbview.modaliases,
                self.dbview.get_session_config(),
                FMT_SCRIPT,
                False,
                0,
                False,
                False,
                stmt_mode,
            )

    async def _compile_rollback(self, bytes eql):
        assert self.dbview.in_tx_error()
        try:
            return await self.get_backend().compiler.call(
                'try_compile_rollback', self.dbview.dbver, eql)
        except Exception:
            self.dbview.raise_in_tx_error()

    async def _recover_script_error(self, eql, allow_capabilities):
        assert self.dbview.in_tx_error()

        query_unit, num_remain = await self._compile_rollback(eql)

        if not (allow_capabilities & enums.Capability.TRANSACTION):
            raise errors.DisabledCapabilityError(
                f"Cannot execute ROLLBACK command;"
                f" the TRANSACTION capability is disabled"
            )

        await self.get_backend().pgcon.simple_query(
            b';'.join(query_unit.sql), ignore_data=True)

        if query_unit.tx_savepoint_rollback:
            if self.debug:
                self.debug_print(f'== RECOVERY: ROLLBACK TO SP')
            await self.recover_current_tx_info()
        else:
            if self.debug:
                self.debug_print('== RECOVERY: ROLLBACK')
            assert query_unit.tx_rollback
            self.dbview.abort_tx()

        if num_remain:
            return 'skip_first', query_unit
        else:
            return 'done', query_unit

    async def simple_query(self):
        cdef:
            WriteBuffer msg
            WriteBuffer packet
            uint64_t allow_capabilities = ALL_CAPABILITIES

        headers = self.parse_headers()
        if headers:
            for k, v in headers.items():
                if k == QUERY_OPT_ALLOW_CAPABILITIES:
                    allow_capabilities = parse_capabilities_header(v)
                else:
                    raise errors.BinaryProtocolError(
                        f'unexpected message header: {k}'
                    )

        eql = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()
        if not eql:
            raise errors.BinaryProtocolError('empty query')

        if self.debug:
            self.debug_print('SIMPLE QUERY', eql)

        if self.dbview.in_tx_error():
            stmt_mode, query_unit = await self._recover_script_error(
                eql,
                allow_capabilities,
            )
            if stmt_mode == 'done':
                packet = WriteBuffer.new()
                packet.write_buffer(
                    self.make_command_complete_msg(query_unit))
                packet.write_buffer(self.pgcon_last_sync_status())
                self.write(packet)
                self.flush()
                return

        query_unit = await self._simple_query(eql, allow_capabilities)

        packet = WriteBuffer.new()
        packet.write_buffer(self.make_command_complete_msg(query_unit))
        packet.write_buffer(self.pgcon_last_sync_status())
        self.write(packet)
        self.flush()

    async def _simple_query(self, eql: bytes, allow_capabilities: uint64_t):
        stmt_mode = 'all'
        with self.timer.timed("Query compilation"):
            units = await self._compile_script(eql, stmt_mode=stmt_mode)

        for query_unit in units:
            if query_unit.capabilities & ~allow_capabilities:
                raise query_unit.capabilities.make_error(
                    allow_capabilities,
                    errors.DisabledCapabilityError,
                )

        new_type_ids = frozenset()
        for query_unit in units:
            self.dbview.start(query_unit)
            try:
                if query_unit.system_config:
                    await self._execute_system_config(query_unit)
                else:
                    if query_unit.is_transactional:
                        await self.get_backend().pgcon.simple_query(
                            b';'.join(query_unit.sql), ignore_data=True)
                    else:
                        for sql in query_unit.sql:
                            await self.get_backend().pgcon.simple_query(
                                sql, ignore_data=True)

                    if query_unit.config_ops:
                        await self.dbview.apply_config_ops(
                            self.get_backend().pgcon,
                            query_unit.config_ops)
            except ConnectionAbortedError:
                raise
            except Exception:
                self.dbview.on_error(query_unit)
                if (not self.get_backend().pgcon.in_tx() and
                        self.dbview.in_tx()):
                    # COMMIT command can fail, in which case the
                    # transaction is aborted.  This check workarounds
                    # that (until a better solution is found.)
                    self.dbview.abort_tx()
                    await self.recover_current_tx_info()
                raise
            else:
                side_effects = self.dbview.on_success(query_unit)
                if side_effects & dbview.SideEffects.SchemaChanges:
                    await self.get_backend().pgcon.signal_sysevent(
                        'schema-changes', dbver=self.dbview.dbver.hex())
                if side_effects & dbview.SideEffects.DatabaseConfigChanges:
                    await self.get_backend().pgcon.signal_sysevent(
                        'database-config-changes', dbname=self.dbview.dbname)
                if side_effects & dbview.SideEffects.SystemConfigChanges:
                    await self.get_backend().pgcon.signal_sysevent(
                        'system-config-changes')
                if query_unit.new_types:
                    new_type_ids |= query_unit.new_types

        if new_type_ids and self.dbview.in_tx():
            # This is a single script, potentially containing multiple
            # transactions (each of which would consist of multiple
            # query units).  In the end, if we're still in transaction
            # after executing the script and there were new types added
            # we want to update type IDs in the linked compiler.
            await self._update_type_ids(new_type_ids)

        return query_unit

    def _tokenize(self, eql: bytes) -> edgeql.Source:
        text = eql.decode('utf-8')
        if debug.flags.edgeql_disable_normalization:
            return edgeql.Source.from_string(text)
        else:
            return edgeql.NormalizedSource.from_string(text)

    async def _parse(
        self,
        bytes eql,
        QueryRequestInfo query_req,
    ) -> CompiledQuery:
        source = query_req.source

        if self.debug:
            self.debug_print('PARSE', eql)

        if self.debug:
            self.debug_print('Cache key', source.text())
            self.debug_print('Extra variables', source.variables(),
                             'after', source.first_extra())

        query_unit = self.dbview.lookup_compiled_query(query_req)
        cached = True
        if query_unit is None:
            # Cache miss; need to compile this query.
            cached = False

            if self.dbview.in_tx_error():
                # The current transaction is aborted; only
                # ROLLBACK or ROLLBACK TO TRANSACTION could be parsed;
                # try doing just that.
                query_unit, num_remain = await self._compile_rollback(eql)
                if num_remain:
                    # Raise an error if there were more than just a
                    # ROLLBACK in that 'eql' string.
                    self.dbview.raise_in_tx_error()
            else:
                with self.timer.timed("Query compilation"):
                    query_unit = await self._compile(
                        query_req,
                        stmt_mode='single',
                    )
                query_unit = query_unit[0]
            if query_unit.capabilities & ~query_req.allow_capabilities:
                raise query_unit.capabilities.make_error(
                    query_req.allow_capabilities,
                    errors.DisabledCapabilityError,
                )
        elif self.dbview.in_tx_error():
            # We have a cached QueryUnit for this 'eql', but the current
            # transaction is aborted.  We can only complete this Parse
            # command if the cached QueryUnit is a 'ROLLBACK' or
            # 'ROLLBACK TO SAVEPOINT' command.
            if not (query_unit.tx_rollback or query_unit.tx_savepoint_rollback):
                self.dbview.raise_in_tx_error()

        await self.get_backend().pgcon.parse_execute(
            1,           # =parse
            0,           # =execute
            query_unit,  # =query
            self,        # =edgecon
            None,        # =bind_data
            0,           # =send_sync
            0,           # =use_prep_stmt
        )

        if not cached and query_unit.cacheable:
            self.dbview.cache_compiled_query(query_req, query_unit)

        return CompiledQuery(
            query_unit=query_unit,
            first_extra=source.first_extra(),
            extra_count=source.extra_count(),
            extra_blob=source.extra_blob(),
        )

    cdef parse_cardinality(self, bytes card):
        if card == b'm':
            return CARD_MANY
        elif card == b'o':
            return CARD_ONE
        elif card == b'n':
            raise errors.BinaryProtocolError(
                'cardinality NO_RESULT cannot be requested')
        else:
            raise errors.BinaryProtocolError(
                f'unknown expected cardinality "{repr(card)[2:-1]}"')

    cdef char render_cardinality(self, query_unit) except -1:
        if query_unit.cardinality is CARD_NO_RESULT:
            return <char>(b'n')
        elif query_unit.cardinality is CARD_ONE:
            return <char>(b'o')
        elif query_unit.cardinality is CARD_MANY:
            return <char>(b'm')
        else:
            raise errors.InternalServerError(
                f'unknown cardinality {query_unit.cardinality!r}')

    cdef parse_io_format(self, bytes mode):
        if mode == b'j':
            return FMT_JSON
        elif mode == b'J':
            return FMT_JSON_ELEMENTS
        elif mode == b'b':
            return FMT_BINARY
        else:
            raise errors.BinaryProtocolError(
                f'unknown output mode "{repr(mode)[2:-1]}"')

    cdef parse_prepare_query_part(self, parse_stmt_name: bint):
        cdef:
            object io_format
            bytes eql
            dict headers
            uint64_t implicit_limit = 0
            bint inline_typeids = self.protocol_version <= (0, 8)
            uint64_t allow_capabilities = ALL_CAPABILITIES
            bint inline_typenames = False
            bytes stmt_name = b''

        headers = self.parse_headers()
        if headers:
            for k, v in headers.items():
                if k == QUERY_OPT_IMPLICIT_LIMIT:
                    implicit_limit = self._parse_implicit_limit(v)
                elif k == QUERY_OPT_INLINE_TYPEIDS:
                    inline_typeids = v.lower() == b'true'
                elif k == QUERY_OPT_INLINE_TYPENAMES:
                    inline_typenames = v.lower() == b'true'
                elif k == QUERY_OPT_ALLOW_CAPABILITIES:
                    allow_capabilities = parse_capabilities_header(v)
                else:
                    raise errors.BinaryProtocolError(
                        f'unexpected message header: {k}'
                    )

        io_format = self.parse_io_format(self.buffer.read_byte())
        expect_one = (
            self.parse_cardinality(self.buffer.read_byte()) is CARD_ONE
        )

        if parse_stmt_name:
            stmt_name = self.buffer.read_len_prefixed_bytes()
            if stmt_name:
                raise errors.UnsupportedFeatureError(
                    'prepared statements are not yet supported')

        eql = self.buffer.read_len_prefixed_bytes()
        if not eql:
            raise errors.BinaryProtocolError('empty query')

        with self.timer.timed("Query normalization"):
            source = self._tokenize(eql)

        query_req = QueryRequestInfo(
            source,
            io_format,
            expect_one,
            implicit_limit,
            inline_typeids,
            inline_typenames,
            allow_capabilities,
        )

        return eql, query_req, stmt_name


    cdef inline reject_headers(self):
        cdef int16_t nheaders = self.buffer.read_int16()
        if nheaders != 0:
            raise errors.BinaryProtocolError('unexpected headers')

    cdef dict parse_headers(self):
        cdef:
            dict attrs
            uint16_t num_fields
            uint16_t key
            bytes value

        attrs = {}
        num_fields = <uint16_t>self.buffer.read_int16()
        while num_fields:
            key = <uint16_t>self.buffer.read_int16()
            value = self.buffer.read_len_prefixed_bytes()
            attrs[key] = value
            num_fields -= 1
        return attrs

    cdef write_headers(self, buf: WriteBuffer, headers: dict):
        buf.write_int16(len(headers))
        for k, v in headers.items():
            buf.write_int16(<int16_t><uint16_t>k)
            buf.write_len_prefixed_utf8(str(v))

    cdef uint64_t _parse_implicit_limit(self, v: bytes) except <uint64_t>-1:
        cdef uint64_t implicit_limit

        limit = cpythonx.PyLong_FromUnicodeObject(
            v.decode(), 10)
        if limit < 0:
            raise errors.BinaryProtocolError(
                f'implicit limit cannot be negative'
            )
        try:
            implicit_limit = <uint64_t>cpython.PyLong_AsLongLong(
                limit
            )
        except OverflowError:
            raise errors.BinaryProtocolError(
                f'implicit limit out of range: {limit}'
            )

        return implicit_limit

    async def parse(self):
        cdef:
            bytes eql
            QueryRequestInfo query_req

        self._last_anon_compiled = None

        eql, query_req, stmt_name = self.parse_prepare_query_part(True)
        compiled_query = await self._parse(eql, query_req)

        buf = WriteBuffer.new_message(b'1')  # ParseComplete

        buf.write_int16(1)
        buf.write_int16(SERVER_HEADER_CAPABILITIES)
        buf.write_int32(sizeof(uint64_t))
        buf.write_int64(<int64_t>(
            <uint64_t>compiled_query.query_unit.capabilities
        ))

        buf.write_byte(self.render_cardinality(compiled_query.query_unit))
        buf.write_bytes(compiled_query.query_unit.in_type_id)
        buf.write_bytes(compiled_query.query_unit.out_type_id)
        buf.end_message()

        self._last_anon_compiled = compiled_query

        self.write(buf)

    #############

    cdef WriteBuffer make_describe_msg(self, CompiledQuery query):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'T')
        msg.write_int16(0)  # no headers

        msg.write_byte(self.render_cardinality(query.query_unit))

        in_data = query.query_unit.in_type_data
        msg.write_bytes(query.query_unit.in_type_id)
        msg.write_len_prefixed_bytes(in_data)

        out_data = query.query_unit.out_type_data
        msg.write_bytes(query.query_unit.out_type_id)
        msg.write_len_prefixed_bytes(out_data)

        msg.end_message()
        return msg

    cdef WriteBuffer make_command_complete_msg(self, query_unit):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'C')

        msg.write_int16(1)
        msg.write_int16(SERVER_HEADER_CAPABILITIES)
        msg.write_int32(sizeof(uint64_t))
        msg.write_int64(<int64_t><uint64_t>query_unit.capabilities)

        msg.write_len_prefixed_bytes(query_unit.status)
        return msg.end_message()

    async def describe(self):
        cdef:
            char rtype
            WriteBuffer msg

        self.reject_headers()

        rtype = self.buffer.read_byte()
        if rtype == b'T':
            # describe "type id"
            stmt_name = self.buffer.read_len_prefixed_bytes()

            if stmt_name:
                raise errors.UnsupportedFeatureError(
                    'prepared statements are not yet supported')
            else:
                if self._last_anon_compiled is None:
                    raise errors.TypeSpecNotFoundError(
                        'no prepared anonymous statement found')

                msg = self.make_describe_msg(self._last_anon_compiled)
                self.write(msg)

        else:
            raise errors.BinaryProtocolError(
                f'unsupported "describe" message mode {chr(rtype)!r}')

    async def _execute_system_config(self, query_unit):
        data = await self.get_backend().pgcon.simple_query(
            b';'.join(query_unit.sql), ignore_data=False)
        if data:
            # Prefer encoded op produced by the SQL command.
            config_ops = [config.Operation.from_json(r[0]) for r in data]
        else:
            # Otherwise, fall back to staticly evaluated op.
            config_ops = query_unit.config_ops
        await self.dbview.apply_config_ops(
            self.get_backend().pgcon, config_ops)

        # If this is a backend configuration setting we also
        # need to make sure it has been loaded.
        if query_unit.backend_config:
            await self.get_backend().pgcon.simple_query(
                b'SELECT pg_reload_conf()', ignore_data=True)

        if query_unit.config_requires_restart:
            self.write_log(
                EdgeSeverity.EDGE_SEVERITY_NOTICE,
                errors.LogMessage.get_code(),
                'server restart is required for the configuration '
                'change to take effect')

    async def _execute(self, compiled: CompiledQuery, bind_args,
                       bint parse, bint use_prep_stmt):
        query_unit = compiled.query_unit
        if self.dbview.in_tx_error():
            if not (query_unit.tx_savepoint_rollback or query_unit.tx_rollback):
                self.dbview.raise_in_tx_error()

            await self.get_backend().pgcon.simple_query(
                b';'.join(query_unit.sql), ignore_data=True)

            if query_unit.tx_savepoint_rollback:
                await self.recover_current_tx_info()
            else:
                assert query_unit.tx_rollback
                self.dbview.abort_tx()

            self.write(self.make_command_complete_msg(query_unit))
            return


        process_sync = False
        if self.buffer.take_message_type(b'S'):
            # A "Sync" message follows this "Execute" message;
            # send it right away.
            process_sync = True

        try:
            bound_args_buf = self.recode_bind_args(bind_args, compiled)

            self.dbview.start(query_unit)
            try:
                if query_unit.system_config:
                    await self._execute_system_config(query_unit)
                else:
                    await self.get_backend().pgcon.parse_execute(
                        parse,              # =parse
                        1,                  # =execute
                        query_unit,         # =query
                        self,               # =edgecon
                        bound_args_buf,     # =bind_data
                        process_sync,       # =send_sync
                        use_prep_stmt,      # =use_prep_stmt
                    )
                    if query_unit.config_ops:
                        await self.dbview.apply_config_ops(
                            self.get_backend().pgcon,
                            query_unit.config_ops)
            except ConnectionAbortedError:
                raise
            except Exception:
                self.dbview.on_error(query_unit)

                if not process_sync and self.dbview.in_tx():
                    # An exception occurred while in transaction.
                    # This "execute" command is not immediately followed by
                    # a "sync" command, so we don't know the current tx
                    # status of the Postgres connection.  Query it to
                    # be able to figure out the tx status of this EdgeDB
                    # connection with the next "if" block.
                    await self.get_backend().pgcon.sync()

                if (not self.get_backend().pgcon.in_tx() and
                        self.dbview.in_tx()):
                    # COMMIT command can fail, in which case the
                    # transaction is finished.  This check workarounds
                    # that (until a better solution is found.)
                    self.dbview.abort_tx()
                    await self.recover_current_tx_info()
                raise
            else:
                side_effects = self.dbview.on_success(query_unit)
                if side_effects & dbview.SideEffects.SchemaChanges:
                    await self.get_backend().pgcon.signal_sysevent(
                        'schema-changes', dbver=self.dbview.dbver.hex())
                if side_effects & dbview.SideEffects.DatabaseConfigChanges:
                    await self.get_backend().pgcon.signal_sysevent(
                        'database-config-changes', dbname=self.dbview.dbname)
                if side_effects & dbview.SideEffects.SystemConfigChanges:
                    await self.get_backend().pgcon.signal_sysevent(
                        'system-config-changes')

            self.write(self.make_command_complete_msg(query_unit))

            if process_sync:
                self.write(self.pgcon_last_sync_status())
                self.flush()
        except Exception:
            if process_sync:
                self.buffer.put_message()
            raise
        else:
            if process_sync:
                self.buffer.finish_message()

            if query_unit.new_types and self.dbview.in_tx():
                await self._update_type_ids(query_unit.new_types)

    async def _get_backend_tids(self, tids):
        conn = self.get_backend().pgcon
        server = self.port.get_server()
        query = await server.get_sys_query(conn, 'backend_tids')
        json_data = await conn.parse_execute_json(
            query, b'__sys_backend_tids',
            dbver=b'', use_prep_stmt=True, args=(list(tids),),
        )

        if json_data is not None:
            return json.loads(json_data.decode('utf-8'))
        else:
            return None

    async def _update_type_ids(self, new_types):
        # Inform the compiler process about the newly
        # appearing types, so type descriptors contain
        # the necessary backend data.  We only do this
        # when in a transaction, since otherwise the entire
        # schema will reload anyway due to a bumped dbver.
        try:
            ret = await self._get_backend_tids(new_types)
        except Exception:
            if self.dbview.in_tx():
                self.dbview.abort_tx()
            raise
        else:
            typemap = {}
            if ret:
                for entry in ret:
                    if entry['backend_id'] is not None:
                        typemap[entry['id']] = entry['backend_id']
            if typemap:
                return await self.get_backend().compiler.call(
                    'update_type_ids',
                    self.dbview.txid,
                    typemap)

    async def execute(self):
        cdef:
            WriteBuffer bound_args_buf
            bint process_sync
            uint64_t allow_capabilities = ALL_CAPABILITIES

        headers = self.parse_headers()
        if headers:
            for k, v in headers.items():
                if k == QUERY_OPT_ALLOW_CAPABILITIES:
                    allow_capabilities = parse_capabilities_header(v)
                else:
                    raise errors.BinaryProtocolError(
                        f'unexpected message header: {k}'
                    )

        stmt_name = self.buffer.read_len_prefixed_bytes()
        bind_args = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()
        query_unit = None

        if self.debug:
            self.debug_print('EXECUTE')

        if stmt_name:
            raise errors.UnsupportedFeatureError(
                'prepared statements are not yet supported')
        else:
            if self._last_anon_compiled is None:
                raise errors.BinaryProtocolError(
                    'no prepared anonymous statement found')

            compiled = self._last_anon_compiled

        if compiled.query_unit.capabilities & ~allow_capabilities:
            raise compiled.query_unit.capabilities.make_error(
                allow_capabilities,
                errors.DisabledCapabilityError,
            )

        await self._execute(compiled, bind_args, False, False)

    async def optimistic_execute(self):
        cdef:
            WriteBuffer bound_args_buf

            bytes query
            QueryRequestInfo query_req

            bint process_sync
            bytes in_tid
            bytes out_tid
            bytes bound_args

        self._last_anon_compiled = None

        query, query_req, _ = self.parse_prepare_query_part(False)

        in_tid = self.buffer.read_bytes(16)
        out_tid = self.buffer.read_bytes(16)
        bind_args = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

        query_unit = self.dbview.lookup_compiled_query(query_req)
        if query_unit is None:
            if self.debug:
                self.debug_print('OPTIMISTIC EXECUTE /REPARSE', query)

            compiled = await self._parse(query, query_req)
            self._last_anon_compiled = compiled
            query_unit = compiled.query_unit
        else:
            compiled = CompiledQuery(
                query_unit=query_unit,
                first_extra=query_req.source.first_extra(),
                extra_count=query_req.source.extra_count(),
                extra_blob=query_req.source.extra_blob(),
            )

        if query_unit.capabilities & ~query_req.allow_capabilities:
            raise query_unit.capabilities.make_error(
                query_req.allow_capabilities,
                errors.DisabledCapabilityError,
            )

        if (query_unit.in_type_id != in_tid or
                query_unit.out_type_id != out_tid):
            # The client has outdated information about type specs.
            if self.debug:
                self.debug_print('OPTIMISTIC EXECUTE /MISMATCH', query)

            self.write(self.make_describe_msg(compiled))

            # We must re-parse the query so that it becomes
            # "last anonymous statement" *in Postgres*.
            # Otherwise the `await self._execute` below would execute
            # some other query.
            compiled = await self._parse(query, query_req)
            self._last_anon_compiled = compiled
            return

        if self.debug:
            self.debug_print('OPTIMISTIC EXECUTE', query)

        self._last_anon_compiled = compiled

        await self._execute(
            compiled, bind_args, True, bool(query_unit.sql_hash))

    async def sync(self):
        self.buffer.consume_message()

        await self.get_backend().pgcon.sync()
        self.write(self.pgcon_last_sync_status())

        if self.debug:
            self.debug_print(
                'SYNC',
                (<pgcon.PGProto>(self.get_backend().pgcon)).xact_status,
            )

        self.flush()

    async def main(self):
        cdef:
            char mtype
            bint flush_sync_on_error

        try:
            await self.auth()
        except Exception as ex:
            if self._transport is not None:
                # If there's no transport it means that the connection
                # was aborted, in which case we don't really care about
                # reporting the exception.

                await self.write_error(ex)
                await self.close()

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

        self.authed = True
        self.server.on_client_authed()

        try:
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                flush_sync_on_error = False

                try:
                    if mtype == b'P':
                        await self.parse()

                    elif mtype == b'D':
                        await self.describe()

                    elif mtype == b'E':
                        await self.execute()

                    elif mtype == b'O':
                        await self.optimistic_execute()

                    elif mtype == b'Q':
                        flush_sync_on_error = True
                        await self.simple_query()

                    elif mtype == b'S':
                        await self.sync()

                    elif mtype == b'X':
                        self.abort()
                        break

                    elif mtype == b'>':
                        await self.dump()

                    elif mtype == b'<':
                        # The restore protocol cannot send SYNC beforehand,
                        # so if an error occurs the server should send an
                        # ERROR message immediately.
                        await self.restore()

                    else:
                        self.fallthrough()

                except ConnectionAbortedError:
                    raise

                except asyncio.CancelledError:
                    raise

                except Exception as ex:
                    if self._backend is None:
                        # The connection has been aborted; there's nothing
                        # we can do except shutting this down.
                        if self._con_status == EDGECON_BAD:
                            return
                        else:
                            raise

                    self.dbview.tx_error()
                    self.buffer.finish_message()

                    await self.write_error(ex)
                    self.flush()

                    if self._backend is None:
                        # The connection was aborted while we were
                        # interpreting the error (via compiler/errmech.py).
                        if self._con_status == EDGECON_BAD:
                            return
                        else:
                            raise

                    if flush_sync_on_error:
                        self.write(self.pgcon_last_sync_status())
                        self.flush()
                    else:
                        await self.recover_from_error()

                else:
                    self.buffer.finish_message()

        except asyncio.CancelledError:
            # Happens when the connection is aborted, the backend is
            # being closed and propagates CancelledError to all
            # EdgeCon methods that await on, say, the compiler process.
            # We shouldn't have CancelledErrors otherwise, therefore,
            # in this situation we just silently exit.
            self.abort()

        except ConnectionAbortedError:
            return

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

            self.abort()

    async def recover_from_error(self):
        # Consume all messages until sync.

        while True:

            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'S':
                await self.sync()
                return
            else:
                self.buffer.discard_message()

    async def write_error(self, exc):
        cdef:
            WriteBuffer buf
            int16_t fields_len

        if debug.flags.server:
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
            exc = await self._interpret_backend_error(exc)

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

        buf = WriteBuffer.new_message(b'E')
        buf.write_byte(<char><uint8_t>EdgeSeverity.EDGE_SEVERITY_ERROR)
        buf.write_int32(<int32_t><uint32_t>exc_code)

        buf.write_len_prefixed_utf8(str(exc))

        fields[base_errors.FIELD_SERVER_TRACEBACK] = formatted_error
        self.write_headers(buf, fields)

        buf.end_message()

        self.write(buf)

    async def _interpret_backend_error(self, exc):
        try:
            static_exc = errormech.static_interpret_backend_error(
                exc.fields)

            # only use the backend if schema is required
            if static_exc is errormech.SchemaRequired:
                if self.dbview.in_tx():
                    exc = await self.get_backend().compiler.call(
                        'interpret_backend_error_in_tx',
                        self.dbview.txid,
                        exc.fields)
                else:
                    exc = await self.get_backend().compiler.call(
                        'interpret_backend_error',
                        self.dbview.dbver,
                        exc.fields)
            else:
                exc = static_exc

        except Exception as ex:
            exc = RuntimeError(
                'unhandled error while calling interpret_backend_error()')

        return exc

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

    cdef pgcon_last_sync_status(self):
        cdef:
            pgcon.PGTransactionStatus xact_status
            WriteBuffer buf

        xact_status = <pgcon.PGTransactionStatus>(
            (<pgcon.PGProto>self.get_backend().pgcon).xact_status)

        buf = WriteBuffer.new_message(b'Z')
        buf.write_int16(0)  # no headers
        if xact_status == pgcon.PQTRANS_IDLE:
            buf.write_byte(b'I')
        elif xact_status == pgcon.PQTRANS_INTRANS:
            buf.write_byte(b'T')
        elif xact_status == pgcon.PQTRANS_INERROR:
            buf.write_byte(b'E')
        else:
            raise errors.InternalServerError(
                'unknown postgres connection status')
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
            self.abort()

        else:
            raise errors.BinaryProtocolError(
                f'unexpected message type {chr(mtype)!r}')

    cdef WriteBuffer recode_bind_args(self,
        bytes bind_args,
        CompiledQuery query,
    ):
        cdef:
            FRBuffer in_buf
            WriteBuffer out_buf = WriteBuffer.new()
            int32_t recv_args
            int32_t decl_args
            ssize_t in_len
            ssize_t i
            const char *data
            object array_tid
            has_reserved = self.protocol_version >= (0, 8)

        assert cpython.PyBytes_CheckExact(bind_args)
        frb_init(
            &in_buf,
            cpython.PyBytes_AS_STRING(bind_args),
            cpython.Py_SIZE(bind_args))

        # all parameters are in binary
        out_buf.write_int32(0x00010001)

        # number of elements in the tuple
        recv_args = hton.unpack_int32(frb_read(&in_buf, 4))
        decl_args = len(query.query_unit.in_type_args or ())

        if recv_args != decl_args:
            raise errors.QueryError(
                f"invalid argument count, "
                f"expected: {decl_args}, got: {recv_args}")

        if query.first_extra is not None:
            assert recv_args == query.first_extra, \
                f"argument count mismatch {recv_args} != {query.first_extra}"
            out_buf.write_int16(<int16_t>(recv_args + query.extra_count))
        else:
            out_buf.write_int16(<int16_t>recv_args)

        if query.query_unit.in_type_args:
            for param in query.query_unit.in_type_args:
                if has_reserved:
                    frb_read(&in_buf, 4)  # reserved
                in_len = hton.unpack_int32(frb_read(&in_buf, 4))
                out_buf.write_int32(in_len)

                if in_len < 0:
                    # This means argument value is NULL
                    if param.required:
                        raise errors.QueryError(
                            f"parameter ${param.name} is required")

                if in_len > 0:
                    data = frb_read(&in_buf, in_len)
                    # Ensure all array parameters have correct element OIDs as
                    # per Postgres' expectations.
                    if param.array_tid is not None:
                        # ndimensions + flags
                        out_buf.write_cstr(data, 8)
                        out_buf.write_int32(<int32_t>param.array_tid)
                        out_buf.write_cstr(&data[12], in_len - 12)
                    else:
                        out_buf.write_cstr(data, in_len)

        if query.first_extra is not None:
            out_buf.write_bytes(query.extra_blob)

        # All columns are in binary format
        out_buf.write_int32(0x00010001)
        return out_buf

    def connection_made(self, transport):
        if not self.server._accepting:
            transport.abort()
            return

        if self._con_status != EDGECON_NEW:
            raise errors.BinaryProtocolError(
                'invalid connection status while establishing the connection')
        self._transport = transport
        self._main_task = self.loop.create_task(self.main())

    def connection_lost(self, exc):
        if self.authed:
            self.server.on_client_disconnected()

        if (self._msg_take_waiter is not None and
                not self._msg_take_waiter.done()):
            self._msg_take_waiter.set_exception(ConnectionAbortedError())
            self._msg_take_waiter = None

        if self._write_waiter and not self._write_waiter.done():
            self._write_waiter.set_exception(ConnectionAbortedError())

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

    async def _init_dump_pgcon(self, pgcon, tx_snapshot_id, bint ro):
        query = b'START TRANSACTION ISOLATION LEVEL SERIALIZABLE'
        if ro:
            query += b' READ ONLY'

        await pgcon.simple_query(
            query,
            True
        )

        await pgcon.simple_query(
            f"SET TRANSACTION SNAPSHOT '{tx_snapshot_id}';".encode(),
            True
        )

    async def dump(self):
        cdef:
            WriteBuffer msg_buf

        self.reject_headers()
        self.buffer.finish_message()

        if self.dbview.txid:
            raise errors.ProtocolError(
                'DUMP must not be executed while in transaction'
            )

        dbname = self.dbview.dbname
        pgcon = await self.port.new_pgcon(dbname)

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

        await pgcon.simple_query(
            b'''START TRANSACTION
                    ISOLATION LEVEL SERIALIZABLE
                    READ ONLY
                    DEFERRABLE;
            ''',
            True
        )
        try:
            tx_snapshot_id = await pgcon.simple_query(
                b'SELECT pg_export_snapshot();', False)
            tx_snapshot_id = tx_snapshot_id[0][0].decode()

            schema_ddl, schema_ids, blocks = \
                await self.get_backend().compiler.call(
                    'describe_database_dump',
                    tx_snapshot_id,
                )

            msg_buf = WriteBuffer.new_message(b'@')

            msg_buf.write_int16(3)  # number of headers
            msg_buf.write_int16(DUMP_HEADER_BLOCK_TYPE)
            msg_buf.write_len_prefixed_bytes(DUMP_HEADER_BLOCK_TYPE_INFO)
            msg_buf.write_int16(DUMP_HEADER_SERVER_VER)
            msg_buf.write_len_prefixed_utf8(str(buildmeta.get_version()))
            msg_buf.write_int16(DUMP_HEADER_SERVER_TIME)
            msg_buf.write_len_prefixed_utf8(str(int(time.time())))

            msg_buf.write_int16(self.max_protocol[0])
            msg_buf.write_int16(self.max_protocol[1])
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

            self._transport.write(msg_buf.end_message())
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

                        self._transport.write(msg_buf.end_message())
                        if self._write_waiter:
                            await self._write_waiter

        finally:
            pgcon.terminate()

        msg_buf = WriteBuffer.new_message(b'C')
        msg_buf.write_int16(0)  # no headers
        msg_buf.write_len_prefixed_bytes(b'DUMP')
        self.write(msg_buf.end_message())
        self.flush()

    async def restore(self):
        cdef:
            WriteBuffer msg_buf
            char mtype

        if self.dbview.txid:
            raise errors.ProtocolError(
                'RESTORE must not be executed while in transaction'
            )

        self.reject_headers()
        self.buffer.read_int16()  # discard -j level

        # Now parse the embedded dump header message:

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
        dbname = self.dbview.dbname
        pgcon = await self.port.new_pgcon(dbname)

        try:
            await pgcon.simple_query(
                b'''START TRANSACTION
                        ISOLATION LEVEL SERIALIZABLE;
                ''',
                True
            )
            tx_snapshot_id = await pgcon.simple_query(
                b'SELECT pg_export_snapshot();', False)
            tx_snapshot_id = tx_snapshot_id[0][0].decode()

            schema_sql_units, restore_blocks, tables = \
                await self.get_backend().compiler.call(
                    'describe_database_restore',
                    tx_snapshot_id,
                    dump_server_ver_str,
                    schema_ddl,
                    schema_ids,
                    blocks,
                )

            for query_unit in schema_sql_units:
                if query_unit.system_config:
                    raise errors.ProtocolError(
                        'system config commands are not supported '
                        'during restore')
                elif query_unit.config_ops:
                    raise errors.ProtocolError(
                        'config commands are not supported '
                        'during restore')
                else:
                    await pgcon.simple_query(
                        b';'.join(query_unit.sql),
                        ignore_data=True)

            restore_blocks = {
                b.schema_object_id: b.sql_copy_stmt
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

            await pgcon.simple_query(
                disable_trigger_q.encode(),
                True
            )

            # Send "RestoreReadyMessage"
            msg = WriteBuffer.new_message(b'+')
            msg.write_int16(0)  # no headers
            msg.write_int16(1)  # -j1
            self.write(msg.end_message())
            self.flush()

            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
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

                    await pgcon.restore(
                        restore_blocks[block_id], block_data)

                elif mtype == b'.':
                    self.buffer.finish_message()
                    break

                else:
                    self.fallthrough()

            await pgcon.simple_query(
                enable_trigger_q.encode() + b'COMMIT;',
                True
            )

        finally:
            pgcon.terminate()

        msg = WriteBuffer.new_message(b'C')
        msg.write_int16(0)  # no headers
        msg.write_len_prefixed_bytes(b'RESTORE')
        self.write(msg.end_message())
        self.flush()


@cython.final
cdef class Timer:
    def __init__(self) -> None:
        self._durations: Dict[str, List[float]] = {}
        self._last_report_timestamp: Dict[str, float] = {}
        self._threshold_seconds = 300

    @contextlib.contextmanager
    def timed(self, operation: str):
        ts_start = time.monotonic()
        try:
            yield
        finally:
            ts_end = time.monotonic()
            duration = ts_end - ts_start
            series = self._durations.setdefault(operation, [])
            series.append(duration)
            self.maybe_log_stats(operation, series=series)

    def maybe_log_stats(
        self, operation: str, *, series: Sequence[float] = ()
    ) -> None:
        since_last_report = time.monotonic() - self._last_report_timestamp.get(operation, 0)
        if since_last_report < self._threshold_seconds:
            return

        self.log_operation_stats(operation, series=series)

    def log_all_stats(self) -> None:
        for operation in self._durations:
            self.log_operation_stats(operation)

    def log_operation_stats(
        self, operation: str, *, series: Sequence[float] = ()
    ) -> None:
        if not series:
            series = self._durations[operation]
        if len(series) < 2:
            return

        p = [0] + statistics.quantiles(series, n=100, method="inclusive")
        log_metrics.info(
            "%s stats: count=%d, p99=%.4f; p90=%.4f; p50=%.4f; max=%.4f",
            operation,
            len(series),
            p[99],
            p[90],
            p[50],  # median
            max(series),
        )
        self._last_report_timestamp[operation] = time.monotonic()
