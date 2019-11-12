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
import hashlib
import json
import logging
import traceback

cimport cython
cimport cpython

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

import immutables

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

from edb.server.dbview cimport dbview

from edb.server import config

from edb.server import compiler
from edb.server.compiler import errormech
from edb.server.pgcon cimport pgcon
from edb.server.pgcon import errors as pgerror

from edb.schema import objects as s_obj

from edb import errors
from edb.errors import base as base_errors
from edb.common import debug

from edgedb import scram


DEF FLUSH_BUFFER_AFTER = 100_000
cdef bytes ZERO_UUID = b'\x00' * 16
cdef bytes EMPTY_TUPLE_UUID = s_obj.get_known_type_id('empty-tuple').bytes

cdef object CAP_ALL = compiler.Capability.ALL

cdef object CARD_NA = compiler.ResultCardinality.NOT_APPLICABLE
cdef object CARD_ONE = compiler.ResultCardinality.ONE
cdef object CARD_MANY = compiler.ResultCardinality.MANY

cdef object logger = logging.getLogger('edb.server')


@cython.final
cdef class EdgeConnection:

    def __init__(self, server, external_auth: bool = False):
        self._con_status = EDGECON_NEW
        self._id = server.new_edgecon_id()
        self.port = server
        self._external_auth = external_auth

        self.loop = server.get_loop()
        self.dbview = None
        self.backend = None

        self._transport = None
        self.buffer = ReadBuffer()

        self._parsing = True
        self._reading_messages = False

        self._main_task = None
        self._msg_take_waiter = None

        self._last_anon_compiled = None

        self._write_buf = None

        self.debug = debug.flags.server_proto
        self.query_cache_enabled = not (debug.flags.disable_qcache or
                                        debug.flags.edgeql_compile)

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
        if self.backend is not None:
            self.loop.create_task(self.backend.close())
            self.backend = None

    cdef close(self):
        self.flush()
        self._con_status = EDGECON_BAD
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        if self.backend is not None:
            self.loop.create_task(self.backend.close())
            self.backend = None

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

        self._con_status = EDGECON_STARTED

        user = params.get('user')
        if not user:
            raise errors.BinaryProtocolError(
                f'missing required connection parameter in ClientHandshake '
                f'message: "user"'
            )

        database = params.get('database')
        if not user:
            raise errors.BinaryProtocolError(
                f'missing required connection parameter in ClientHandshake '
                f'message: "database"'
            )

        logger.debug('received connection request by %s to database %s',
                     user, database)

        dbv = self.port.new_view(
            dbname=database, user=user,
            query_cache=self.query_cache_enabled)
        assert type(dbv) is dbview.DatabaseConnectionView
        self.dbview = <dbview.DatabaseConnectionView>dbv

        self.backend = await self.port.new_backend(
            dbname=database, dbver=self.dbview.dbver)

        # The user has already been authenticated by other means
        # (such as the ability to write to a protected socket).
        if self._external_auth:
            authmethod_name = 'Trust'
        else:
            authmethod = await self.port.get_server().get_auth_method(
                user, database, self._transport)
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
            msg_buf = WriteBuffer.new_message(b'S')
            msg_buf.write_len_prefixed_bytes(b'pgaddr')
            msg_buf.write_len_prefixed_utf8(
                str(self.backend.pgcon.get_pgaddr()))
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
            uint16_t hi
            uint16_t lo
            int i
            uint16_t nexts
            dict exts = {}
            dict params = {}

        hi = <uint16_t>self.buffer.read_int16()
        lo = <uint16_t>self.buffer.read_int16()

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

        if hi != 1 or lo != 0 or nexts > 0:
            # NegotiateProtocolVersion
            buf = WriteBuffer.new_message(b'v')
            # Highest supported major version of the protocol.
            buf.write_int16(1)
            # Highest supported minor version of the protocol.
            buf.write_int16(0)
            # No extensions are currently supported.
            buf.write_int16(0)
            buf.end_message()

            self.write(buf)
            self.flush()

        return params

    async def _get_role_record(self, user):
        conn = self.backend.pgcon
        server = self.port.get_server()
        role_query = await server.get_sys_query(conn, 'role')
        json_data = await conn.parse_execute_json(
            role_query, b'__sys_role',
            dbver=0, use_prep_stmt=True, args=(user,),
        )

        if json_data is not None:
            return json.loads(json_data.decode('utf-8'))
        else:
            return None

    async def _auth_trust(self, user):
        rolerec = await self._get_role_record(user)
        if rolerec is None or not rolerec['allow_login']:
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
            if mtype != b'p':
                raise errors.BinaryProtocolError(
                    f'expected SASL response, got message type {mtype}')

            if selected_mech is None:
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
        if rolerec is not None and rolerec['allow_login']:
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
                self.backend.pgcon, 'mock_auth_nonce')
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
        ret = await self.backend.pgcon.simple_query(b'''
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
                    conf = conf.set(sname, pyval)
                elif stype == b'A':
                    if not sname:
                        sname = None
                    aliases = aliases.set(sname, svalue)
                else:
                    assert stype == b'S' and not sname
                    sp_id = int(svalue)

        if self.debug:
            self.debug_print('RECOVER SP/ALIAS/CONF', sp_id, aliases, conf)

        if self.dbview.in_tx():
            self.dbview.rollback_tx_to_savepoint(sp_id, aliases, conf)
        else:
            self.dbview.recover_aliases_and_config(aliases, conf)

    #############

    async def _compile(self, bytes eql, bint json_mode, bint expect_one,
                       str stmt_mode):

        if self.dbview.in_tx_error():
            self.dbview.raise_in_tx_error()

        if self.dbview.in_tx():
            return await self.backend.compiler.call(
                'compile_eql_in_tx',
                self.dbview.txid,
                eql,
                json_mode,
                expect_one,
                stmt_mode)
        else:
            return await self.backend.compiler.call(
                'compile_eql',
                self.dbview.dbver,
                eql,
                self.dbview.modaliases,
                self.dbview.get_session_config(),
                json_mode,
                expect_one,
                stmt_mode,
                CAP_ALL)

    async def _compile_rollback(self, bytes eql):
        assert self.dbview.in_tx_error()
        try:
            return await self.backend.compiler.call(
                'try_compile_rollback', self.dbview.dbver, eql)
        except Exception:
            self.dbview.raise_in_tx_error()

    async def _recover_script_error(self, eql):
        assert self.dbview.in_tx_error()

        query_unit, num_remain = await self._compile_rollback(eql)
        await self.backend.pgcon.simple_query(
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

        self.reject_headers()

        eql = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()
        if not eql:
            raise errors.BinaryProtocolError('empty query')

        if self.debug:
            self.debug_print('SIMPLE QUERY', eql)

        stmt_mode = 'all'
        if self.dbview.in_tx_error():
            stmt_mode, query_unit = await self._recover_script_error(eql)
            if stmt_mode == 'done':
                packet = WriteBuffer.new()
                packet.write_buffer(
                    self.make_command_complete_msg(query_unit))
                packet.write_buffer(self.pgcon_last_sync_status())
                self.write(packet)
                self.flush()
                return

        units = await self._compile(eql, False, False, stmt_mode)

        for query_unit in units:
            self.dbview.start(query_unit)
            try:
                if query_unit.system_config:
                    await self._execute_system_config(query_unit)
                else:
                    await self.backend.pgcon.simple_query(
                        b';'.join(query_unit.sql), ignore_data=True)
                    if query_unit.config_ops is not None:
                        await self.dbview.apply_config_ops(
                            self.backend.pgcon,
                            query_unit.config_ops)
            except ConnectionAbortedError:
                raise
            except Exception:
                self.dbview.on_error(query_unit)
                if not self.backend.pgcon.in_tx() and self.dbview.in_tx():
                    # COMMIT command can fail, in which case the
                    # transaction is aborted.  This check workarounds
                    # that (until a better solution is found.)
                    self.dbview.abort_tx()
                    await self.recover_current_tx_info()
                raise
            else:
                self.dbview.on_success(query_unit)
                if query_unit.new_types and self.dbview.in_tx():
                    await self._update_type_ids(query_unit)

        packet = WriteBuffer.new()
        packet.write_buffer(self.make_command_complete_msg(query_unit))
        packet.write_buffer(self.pgcon_last_sync_status())
        self.write(packet)
        self.flush()

    async def _parse(self, bytes eql, bint json_mode, bint expect_one):
        if self.debug:
            self.debug_print('PARSE', eql)

        query_unit = self.dbview.lookup_compiled_query(
            eql, json_mode, expect_one)
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
                query_unit = await self._compile(
                    eql, json_mode, expect_one, 'single')
                query_unit = query_unit[0]
        elif self.dbview.in_tx_error():
            # We have a cached QueryUnit for this 'eql', but the current
            # transaction is aborted.  We can only complete this Parse
            # command if the cached QueryUnit is a 'ROLLBACK' or
            # 'ROLLBACK TO SAVEPOINT' command.
            if not (query_unit.tx_rollback or query_unit.tx_savepoint_rollback):
                self.dbview.raise_in_tx_error()

        await self.backend.pgcon.parse_execute(
            1,           # =parse
            0,           # =execute
            query_unit,  # =query
            self,        # =edgecon
            None,        # =bind_data
            0,           # =send_sync
            0,           # =use_prep_stmt
        )

        if not cached and query_unit.cacheable:
            self.dbview.cache_compiled_query(
                eql, json_mode, expect_one, query_unit)

        return query_unit

    cdef parse_cardinality(self, bytes card):
        if card == b'm':
            return CARD_MANY
        elif card == b'o':
            return CARD_ONE
        elif card == b'n':
            raise errors.BinaryProtocolError(
                'cardinality N/A cannot be requested')
        else:
            raise errors.BinaryProtocolError(
                f'unknown expected cardinality "{repr(card)[2:-1]}"')

    cdef char render_cardinality(self, query_unit) except -1:
        if query_unit.cardinality is CARD_NA:
            return <char>(b'n')
        elif query_unit.cardinality is CARD_ONE:
            return <char>(b'o')
        elif query_unit.cardinality is CARD_MANY:
            return <char>(b'm')
        else:
            raise errors.InternalServerError(
                f'unknown cardinality {query_unit.cardinality!r}')

    cdef parse_json_mode(self, bytes mode):
        if mode == b'j':
            # json
            return True
        elif mode == b'b':
            # binary
            return False
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

    async def parse(self):
        cdef:
            bint json_mode
            bytes eql

        self._last_anon_compiled = None

        self.reject_headers()

        json_mode = self.parse_json_mode(self.buffer.read_byte())
        expect_one = (
            self.parse_cardinality(self.buffer.read_byte()) is CARD_ONE
        )

        stmt_name = self.buffer.read_len_prefixed_bytes()
        if stmt_name:
            raise errors.UnsupportedFeatureError(
                'prepared statements are not yet supported')

        eql = self.buffer.read_len_prefixed_bytes()
        if not eql:
            raise errors.BinaryProtocolError('empty query')

        query_unit = await self._parse(eql, json_mode, expect_one)

        buf = WriteBuffer.new_message(b'1')  # ParseComplete
        buf.write_int16(0)  # no headers
        buf.write_byte(self.render_cardinality(query_unit))
        buf.write_bytes(query_unit.in_type_id)
        buf.write_bytes(query_unit.out_type_id)
        buf.end_message()

        self._last_anon_compiled = query_unit

        self.write(buf)

    #############

    cdef WriteBuffer make_describe_msg(self, query_unit):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'T')
        msg.write_int16(0)  # no headers

        msg.write_byte(self.render_cardinality(query_unit))

        in_data = query_unit.in_type_data
        msg.write_bytes(query_unit.in_type_id)
        msg.write_len_prefixed_bytes(in_data)

        out_data = query_unit.out_type_data
        msg.write_bytes(query_unit.out_type_id)
        msg.write_len_prefixed_bytes(out_data)

        msg.end_message()
        return msg

    cdef WriteBuffer make_command_complete_msg(self, query_unit):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'C')
        msg.write_int16(0)  # no headers
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
        data = await self.backend.pgcon.simple_query(
            b';'.join(query_unit.sql), ignore_data=False)
        if data:
            # Prefer encoded op produced by the SQL command.
            config_ops = [config.Operation.from_json(r[0]) for r in data]
        else:
            # Otherwise, fall back to staticly evaluated op.
            config_ops = query_unit.config_ops
        await self.dbview.apply_config_ops(self.backend.pgcon, config_ops)

        # If this is a backend configuration setting we also
        # need to make sure it has been loaded.
        if query_unit.backend_config:
            await self.backend.pgcon.simple_query(
                b'SELECT pg_reload_conf()', ignore_data=True)

        if query_unit.config_requires_restart:
            self.write_log(
                EdgeSeverity.EDGE_SEVERITY_NOTICE,
                errors.LogMessage.get_code(),
                'server restart is required for the configuration '
                'change to take effect')

    async def _execute(self, query_unit, bind_args,
                       bint parse, bint use_prep_stmt):
        if self.dbview.in_tx_error():
            if not (query_unit.tx_savepoint_rollback or query_unit.tx_rollback):
                self.dbview.raise_in_tx_error()

            await self.backend.pgcon.simple_query(
                b';'.join(query_unit.sql), ignore_data=True)

            if query_unit.tx_savepoint_rollback:
                await self.recover_current_tx_info()
            else:
                assert query_unit.tx_rollback
                self.dbview.abort_tx()

            self.write(self.make_command_complete_msg(query_unit))
            return

        bound_args_buf = self.recode_bind_args(
            bind_args, query_unit.in_array_backend_tids)

        process_sync = False
        if self.buffer.take_message_type(b'S'):
            # A "Sync" message follows this "Execute" message;
            # send it right away.
            process_sync = True

        try:
            self.dbview.start(query_unit)
            try:
                if query_unit.system_config:
                    await self._execute_system_config(query_unit)
                else:
                    await self.backend.pgcon.parse_execute(
                        parse,              # =parse
                        1,                  # =execute
                        query_unit,         # =query
                        self,               # =edgecon
                        bound_args_buf,     # =bind_data
                        process_sync,       # =send_sync
                        use_prep_stmt,      # =use_prep_stmt
                    )
                    if query_unit.config_ops is not None:
                        await self.dbview.apply_config_ops(
                            self.backend.pgcon,
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
                    await self.backend.pgcon.sync()

                if not self.backend.pgcon.in_tx() and self.dbview.in_tx():
                    # COMMIT command can fail, in which case the
                    # transaction is finished.  This check workarounds
                    # that (until a better solution is found.)
                    self.dbview.abort_tx()
                    await self.recover_current_tx_info()
                raise
            else:
                self.dbview.on_success(query_unit)

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
                await self._update_type_ids(query_unit)

    async def _update_type_ids(self, query_unit):
        # Inform the compiler process about the newly
        # appearing types, so type descriptors contain
        # the necessary backend data.  We only do this
        # when in a transaction, since otherwise the entire
        # schema will reload anyway due to a bumped dbver.
        try:
            tids = ','.join(f"'{tid}'" for tid in query_unit.new_types)
            ret = await self.backend.pgcon.simple_query(b'''
                SELECT id, backend_id
                FROM edgedb.type
                WHERE id = any(ARRAY[%b]::uuid[])
            ''' % (tids.encode(),), ignore_data=False)
        except Exception:
            if self.dbview.in_tx():
                self.dbview.abort_tx()
            raise
        else:
            typemap = {}
            if ret:
                for tid, backend_tid in ret:
                    if backend_tid is not None:
                        typemap[tid.decode()] = int(backend_tid.decode())
            if typemap:
                return await self.backend.compiler.call(
                    'update_type_ids',
                    self.dbview.txid,
                    typemap)

    async def execute(self):
        cdef:
            WriteBuffer bound_args_buf
            bint process_sync

        self.reject_headers()
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

            query_unit = self._last_anon_compiled

        await self._execute(query_unit, bind_args, False, False)

    async def optimistic_execute(self):
        cdef:
            WriteBuffer bound_args_buf
            bint process_sync
            bytes in_tid
            bytes out_tid
            bytes bound_args

        self._last_anon_compiled = None

        self.reject_headers()
        json_mode = self.parse_json_mode(self.buffer.read_byte())
        expect_one = (
            self.parse_cardinality(self.buffer.read_byte()) is CARD_ONE
        )
        query = self.buffer.read_len_prefixed_bytes()
        in_tid = self.buffer.read_bytes(16)
        out_tid = self.buffer.read_bytes(16)
        bind_args = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

        if not query:
            raise errors.BinaryProtocolError('empty query')

        query_unit = self.dbview.lookup_compiled_query(
            query, json_mode, expect_one)
        if query_unit is None:
            if self.debug:
                self.debug_print('OPTIMISTIC EXECUTE /REPARSE', query)

            query_unit = await self._parse(query, json_mode, expect_one)
            self._last_anon_compiled = query_unit

        if (query_unit.in_type_id != in_tid or
                query_unit.out_type_id != out_tid):
            # The client has outdated information about type specs.
            if self.debug:
                self.debug_print('OPTIMISTIC EXECUTE /MISMATCH', query)

            self.write(self.make_describe_msg(query_unit))

            # We must re-parse the query so that it becomes
            # "last anonymous statement" *in Postgres*.
            # Otherwise the `await self._execute` below would execute
            # some other query.
            query_unit = await self._parse(query, json_mode, expect_one)
            self._last_anon_compiled = query_unit
            return

        if self.debug:
            self.debug_print('OPTIMISTIC EXECUTE', query)

        self._last_anon_compiled = query_unit

        await self._execute(
            query_unit, bind_args, True, bool(query_unit.sql_hash))

    async def sync(self):
        self.buffer.consume_message()

        await self.backend.pgcon.sync()
        self.write(self.pgcon_last_sync_status())

        if self.debug:
            self.debug_print(
                'SYNC', (<pgcon.PGProto>(self.backend.pgcon)).xact_status)

        self.flush()

    async def main(self):
        cdef:
            char mtype

        try:
            await self.auth()
        except Exception as ex:
            if self._transport is not None:
                # If there's no transport it means that the connection
                # was aborted, in which case we don't really care about
                # reporting the exception.

                await self.write_error(ex)
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

                    else:
                        self.fallthrough(False)

                except ConnectionAbortedError:
                    raise

                except asyncio.CancelledError:
                    raise

                except Exception as ex:
                    if self.backend is None:
                        # The connection has been aborted; there's nothing
                        # we can do except shutting this down.
                        if self._con_status == EDGECON_BAD:
                            return
                        else:
                            raise

                    self.dbview.tx_error()
                    self.buffer.finish_message()

                    await self.write_error(ex)
                    if self.backend is None:
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
            try:
                static_exc = errormech.static_interpret_backend_error(
                    exc.fields)

                # only use the backend if schema is required
                if static_exc is errormech.SchemaRequired:
                    exc = await self._interpret_backend_error(exc)
                else:
                    exc = static_exc

            except Exception as ex:
                exc = RuntimeError(
                    'unhandled error while calling interpret_backend_error()')

        fields = None
        fields_len = 0
        if (isinstance(exc, errors.EdgeDBError) and
                type(exc) is not errors.EdgeDBError):
            exc_code = exc.get_code()
            fields = exc._attrs
            fields_len = <int16_t><uint16_t>(len(fields))

        if not exc_code:
            exc_code = errors.InternalServerError.get_code()

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

        buf.write_int16(fields_len + 1)  # number of headers
        if fields is not None:
            for k, v in fields.items():
                buf.write_int16(<int16_t><uint16_t>k)
                buf.write_len_prefixed_utf8(str(v))
        buf.write_int16(base_errors.FIELD_SERVER_TRACEBACK)
        buf.write_len_prefixed_utf8(formatted_error)

        buf.end_message()

        self.write(buf)

    async def _interpret_backend_error(self, exc):
        if self.dbview.in_tx():
            return await self.backend.compiler.call(
                'interpret_backend_error_in_tx',
                self.dbview.txid,
                exc.fields)
        else:
            return await self.backend.compiler.call(
                'interpret_backend_error',
                self.dbview.dbver,
                exc.fields)

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
            (<pgcon.PGProto>self.backend.pgcon).xact_status)

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

    cdef fallthrough(self, bint ignore_unhandled):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == b'H':
            # Flush
            self.flush()
            self.buffer.discard_message()
            return

        if ignore_unhandled:
            self.buffer.discard_message()
        else:
            raise errors.BinaryProtocolError(
                f'unexpected message type {chr(mtype)!r}')

    cdef WriteBuffer recode_bind_args(self, bytes bind_args, dict array_tids):
        cdef:
            FRBuffer in_buf
            WriteBuffer out_buf = WriteBuffer.new()
            int32_t argsnum
            ssize_t in_len
            ssize_t i
            const char *data
            object array_tid

        assert cpython.PyBytes_CheckExact(bind_args)
        frb_init(
            &in_buf,
            cpython.PyBytes_AS_STRING(bind_args),
            cpython.Py_SIZE(bind_args))

        # all parameters are in binary
        out_buf.write_int32(0x00010001)

        # number of elements in the tuple
        argsnum = hton.unpack_int32(frb_read(&in_buf, 4))

        out_buf.write_int16(<int16_t>argsnum)

        if array_tids:
            # we have array parameters, ensure all of them
            # have correct element OIDs as per Postgres' expectations.
            for i in range(argsnum):
                in_len = hton.unpack_int32(frb_read(&in_buf, 4))
                out_buf.write_int32(in_len)
                if in_len > 0:
                    data = frb_read(&in_buf, in_len)
                    array_tid = array_tids.get(i)
                    if array_tid is not None:
                        # ndimensions + flags
                        out_buf.write_cstr(data, 8)
                        out_buf.write_int32(<int32_t>array_tid)
                        out_buf.write_cstr(&data[12], in_len - 12)
                    else:
                        out_buf.write_cstr(data, in_len)
        else:
            in_len = frb_get_len(&in_buf)
            out_buf.write_cstr(frb_read_all(&in_buf), in_len)

        # All columns are in binary format
        out_buf.write_int32(0x00010001)
        return out_buf

    def connection_made(self, transport):
        if self._con_status != EDGECON_NEW:
            raise errors.BinaryProtocolError(
                'invalid connection status while establishing the connection')
        self._transport = transport
        self._main_task = self.loop.create_task(self.main())

    def connection_lost(self, exc):
        if (self._msg_take_waiter is not None and
                not self._msg_take_waiter.done()):
            self._msg_take_waiter.set_exception(ConnectionAbortedError())
            self._msg_take_waiter = None

        self.abort()

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)
        if self._msg_take_waiter is not None and self.buffer.take_message():
            self._msg_take_waiter.set_result(True)
            self._msg_take_waiter = None

    def eof_received(self):
        pass
