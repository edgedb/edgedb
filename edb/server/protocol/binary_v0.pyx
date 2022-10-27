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


cdef tuple MIN_LEGACY_PROTOCOL = edbdef.MIN_LEGACY_PROTOCOL


from edb.server import args as srvargs
from edb.server.protocol cimport args_ser
from edb.server.protocol import execute


@cython.final
cdef class EdgeConnectionBackwardsCompatible(EdgeConnection):
    def __init__(
        self,
        server,
        *,
        external_auth: bool,
        passive: bool,
        transport: srvargs.ServerConnectionTransport,
        auth_data: bytes,
        conn_params: dict[str, str] | None,
        protocol_version: tuple[int, int],
    ):
        super().__init__(
            server,
            external_auth=external_auth,
            passive=passive,
            transport=transport,
            auth_data=auth_data,
            conn_params=conn_params,
            protocol_version=protocol_version,
        )
        self.min_protocol = MIN_LEGACY_PROTOCOL

    async def _do_handshake(self):
        cdef:
            uint16_t major
            uint16_t minor
            int i
            uint16_t nexts
            dict params = {}

        major = <uint16_t>self.buffer.read_int16()
        minor = <uint16_t>self.buffer.read_int16()

        self.protocol_version = major, minor

        nparams = <uint16_t>self.buffer.read_int16()
        for i in range(nparams):
            k = self.buffer.read_len_prefixed_utf8()
            v = self.buffer.read_len_prefixed_utf8()
            params[k] = v

        if self.protocol_version <= MAX_LEGACY_PROTOCOL:
            nexts = <uint16_t>self.buffer.read_int16()

            for i in range(nexts):
                extname = self.buffer.read_len_prefixed_utf8()
                self.legacy_parse_headers()
        else:
            nexts = 0

        self.buffer.finish_message()

        negotiate = nexts > 0
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

    async def legacy_parse(self):
        cdef:
            bytes eql
            dbview.QueryRequestInfo query_req

        self._last_anon_compiled = None

        eql, query_req, stmt_name = self.legacy_parse_prepare_query_part(True)
        compiled_query = await self._parse(query_req)

        buf = WriteBuffer.new_message(b'1')  # ParseComplete

        buf.write_int16(1)
        buf.write_int16(SERVER_HEADER_CAPABILITIES)
        buf.write_int32(sizeof(uint64_t))
        buf.write_int64(<int64_t>(
            <uint64_t>compiled_query.query_unit_group.capabilities
        ))

        buf.write_byte(
            self.render_cardinality(compiled_query.query_unit_group)
        )

        if self.protocol_version >= (0, 14):
            buf.write_bytes(compiled_query.query_unit_group.in_type_id)
            buf.write_len_prefixed_bytes(
                compiled_query.query_unit_group.in_type_data
            )

            buf.write_bytes(compiled_query.query_unit_group.out_type_id)
            buf.write_len_prefixed_bytes(
                compiled_query.query_unit_group.out_type_data)
        else:
            buf.write_bytes(compiled_query.query_unit_group.in_type_id)
            buf.write_bytes(compiled_query.query_unit_group.out_type_id)

        buf.end_message()

        self._last_anon_compiled = compiled_query

        self.write(buf)

    async def legacy_describe(self):
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

                msg = self.make_legacy_command_data_description_msg(
                    self._last_anon_compiled
                )
                self.write(msg)

        else:
            raise errors.BinaryProtocolError(
                f'unsupported "describe" message mode {chr(rtype)!r}')

    async def legacy_auth(self, params):
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

        token = params.get('token')

        logger.debug('received connection request by %s to database %s',
                     user, database)

        if (
            database in edbdef.EDGEDB_SPECIAL_DBS
            or not self.server.is_database_connectable(database)
        ):
            # Prevent connections to internal system databases,
            # which only purpose is to serve as a template for new
            # databases.
            raise errors.AccessError(
                f'database {database!r} does not '
                f'accept connections'
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
            self._auth_jwt(user, token)
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

    async def legacy_dump(self):
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
        msg_buf.write_len_prefixed_bytes(b'DUMP')
        self.write(msg_buf.end_message())
        self.flush()

    async def legacy_restore(self):
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
                    await pgcon.restore(restore_block, block_data, type_id_map)

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
            self._in_dump_restore = False
            server.release_pgcon(dbname, pgcon)

        await server.introspect_db(dbname)

        msg = WriteBuffer.new_message(b'C')
        msg.write_int16(0)  # no headers
        msg.write_len_prefixed_bytes(b'RESTORE')
        self.write(msg.end_message())
        self.flush()

    async def legacy_main(self, params):
        cdef:
            char mtype
            bint flush_sync_on_error

        try:
            await self.legacy_auth(params)
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

                flush_sync_on_error = False

                try:
                    if mtype == b'P':
                        await self.legacy_parse()

                    elif mtype == b'D':
                        if self.protocol_version >= (0, 14):
                            raise errors.BinaryProtocolError(
                                "Describe message (D) is not supported in "
                                "protocol versions greater than 0.13")
                        await self.legacy_describe()

                    elif mtype == b'E':
                        await self.legacy_execute()

                    elif mtype == b'O':
                        await self.legacy_optimistic_execute()

                    elif mtype == b'Q':
                        flush_sync_on_error = True
                        await self.legacy_simple_query()

                    elif mtype == b'S':
                        await self.sync()

                    elif mtype == b'X':
                        self.close()
                        break

                    elif mtype == b'>':
                        await self.legacy_dump()

                    elif mtype == b'<':
                        # The restore protocol cannot send SYNC beforehand,
                        # so if an error occurs the server should send an
                        # ERROR message immediately.
                        await self.legacy_restore()

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

                    if flush_sync_on_error:
                        self.write(self.sync_status())
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

    async def legacy_optimistic_execute(self):
        cdef:
            WriteBuffer bound_args_buf

            bytes query
            dbview.QueryRequestInfo query_req

            bytes in_tid
            bytes out_tid
            bytes bound_args

        self._last_anon_compiled = None

        query, query_req, _ = self.legacy_parse_prepare_query_part(False)

        in_tid = self.buffer.read_bytes(16)
        out_tid = self.buffer.read_bytes(16)
        bind_args = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

        query_unit_group = self.get_dbview().lookup_compiled_query(query_req)
        if query_unit_group is None:
            if self.debug:
                self.debug_print('OPTIMISTIC EXECUTE /REPARSE', query)

            compiled = await self._parse(query_req)
            self._last_anon_compiled = compiled
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
            self._last_anon_compiled = compiled

        if query_unit_group.capabilities & ~query_req.allow_capabilities:
            raise query_unit_group.capabilities.make_error(
                query_req.allow_capabilities,
                errors.DisabledCapabilityError,
            )

        if (
            query_unit_group.in_type_id != in_tid or
            query_unit_group.out_type_id != out_tid
        ):
            # The client has outdated information about type specs.
            if self.debug:
                self.debug_print('OPTIMISTIC EXECUTE /MISMATCH', query)

            self.write(self.make_legacy_command_data_description_msg(compiled))

            if self._cancelled:
                raise ConnectionAbortedError
            return

        if self.debug:
            self.debug_print('OPTIMISTIC EXECUTE', query)

        metrics.edgeql_query_compilations.inc(1.0, 'cache')
        await self._legacy_execute(
            compiled,
            bind_args,
            len(query_unit_group) == 1 and bool(query_unit_group[0].sql_hash),
        )

    cdef legacy_parse_prepare_query_part(self, parse_stmt_name: bint):
        cdef:
            object output_format
            bytes eql
            dict headers
            uint64_t implicit_limit = 0
            bint inline_typeids = False
            uint64_t allow_capabilities = ALL_CAPABILITIES
            bint inline_typenames = False
            bint inline_objectids = True
            bytes stmt_name = b''

        headers = self.legacy_parse_headers()
        if headers:
            for k, v in headers.items():
                if k == QUERY_HEADER_IMPLICIT_LIMIT:
                    implicit_limit = self._parse_implicit_limit(v)
                elif k == QUERY_HEADER_IMPLICIT_TYPEIDS:
                    inline_typeids = parse_boolean(v, "IMPLICIT_TYPEIDS")
                elif k == QUERY_HEADER_IMPLICIT_TYPENAMES:
                    inline_typenames = parse_boolean(v, "IMPLICIT_TYPENAMES")
                elif k == QUERY_HEADER_ALLOW_CAPABILITIES:
                    allow_capabilities = parse_capabilities_header(v)
                elif k == QUERY_HEADER_EXPLICIT_OBJECTIDS:
                    inline_objectids = not parse_boolean(v, "EXPLICIT_OBJECTIDS")
                else:
                    raise errors.BinaryProtocolError(
                        f'unexpected message header: {k}'
                    )

        output_format = self.parse_output_format(self.buffer.read_byte())
        expect_one = (
            self.parse_cardinality(self.buffer.read_byte()) is CARD_AT_MOST_ONE
        )

        if parse_stmt_name:
            stmt_name = self.buffer.read_len_prefixed_bytes()
            if stmt_name:
                raise errors.UnsupportedFeatureError(
                    'prepared statements are not yet supported')

        eql = self.buffer.read_len_prefixed_bytes()
        if not eql:
            raise errors.BinaryProtocolError('empty query')

        source = self._tokenize(eql)

        query_req = dbview.QueryRequestInfo(
            source,
            self.protocol_version,
            output_format=output_format,
            expect_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            allow_capabilities=allow_capabilities,
        )

        return eql, query_req, stmt_name

    async def _legacy_execute(self, compiled: dbview.CompiledQuery, bind_args,
                              bint use_prep_stmt):
        cdef:
            dbview.DatabaseConnectionView dbv
            pgcon.PGConnection conn

        query_unit = compiled.query_unit_group[0]
        dbv = self.get_dbview()
        if dbv.in_tx_error() or query_unit.tx_savepoint_rollback:
            if not (query_unit.tx_savepoint_rollback or query_unit.tx_rollback):
                dbv.raise_in_tx_error()

            conn = await self.get_pgcon()
            try:
                if query_unit.sql:
                    await conn.sql_execute(query_unit.sql)

                if query_unit.tx_savepoint_rollback:
                    dbv.rollback_tx_to_savepoint(query_unit.sp_name)
                else:
                    assert query_unit.tx_rollback
                    dbv.abort_tx()

            finally:
                self.maybe_release_pgcon(conn)
        else:
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

        self.write(self.make_legacy_command_complete_msg(query_unit))

    async def legacy_execute(self):
        cdef:
            WriteBuffer bound_args_buf
            uint64_t allow_capabilities = ALL_CAPABILITIES

        headers = self.legacy_parse_headers()
        if headers:
            for k, v in headers.items():
                if k == QUERY_HEADER_ALLOW_CAPABILITIES:
                    allow_capabilities = parse_capabilities_header(v)
                else:
                    raise errors.BinaryProtocolError(
                        f'unexpected message header: {k}'
                    )

        stmt_name = self.buffer.read_len_prefixed_bytes()
        bind_args = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

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

        if compiled.query_unit_group.capabilities & ~allow_capabilities:
            raise compiled.query_unit_group.capabilities.make_error(
                allow_capabilities,
                errors.DisabledCapabilityError,
            )

        await self._legacy_execute(compiled, bind_args, False)

    async def _legacy_compile_script(
        self,
        query: bytes,
        *,
        skip_first: bool,
    ):
        query_req = dbview.QueryRequestInfo(
            source=edgeql.Source.from_string(query.decode("utf-8")),
            protocol_version=self.protocol_version,
            output_format=FMT_NONE,
        )

        return await self.get_dbview()._compile(
            query_req,
            skip_first=skip_first,
        )

    async def _legacy_recover_script_error(
        self, eql: bytes, allow_capabilities
    ):
        assert self.get_dbview().in_tx_error()

        query_unit_group, num_remain = (
            await self.get_dbview().compile_rollback(eql))
        query_unit = query_unit_group[0]

        if not (allow_capabilities & enums.Capability.TRANSACTION):
            raise errors.DisabledCapabilityError(
                f"Cannot execute ROLLBACK command;"
                f" the TRANSACTION capability is disabled"
            )

        conn = await self.get_pgcon()
        try:
            if query_unit.sql:
                await conn.sql_execute(query_unit.sql)

            if query_unit.tx_savepoint_rollback:
                if self.debug:
                    self.debug_print(f'== RECOVERY: ROLLBACK TO SP')
                self.get_dbview().rollback_tx_to_savepoint(query_unit.sp_name)
            else:
                if self.debug:
                    self.debug_print('== RECOVERY: ROLLBACK')
                assert query_unit.tx_rollback
                self.get_dbview().abort_tx()
        finally:
            self.maybe_release_pgcon(conn)

        if num_remain:
            return query_unit, False
        else:
            return query_unit, True

    async def legacy_simple_query(self):
        cdef:
            WriteBuffer msg
            WriteBuffer packet
            uint64_t allow_capabilities = ALL_CAPABILITIES

        headers = self.legacy_parse_headers()
        if headers:
            for k, v in headers.items():
                if k == QUERY_HEADER_ALLOW_CAPABILITIES:
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

        skip_first = False
        if self.get_dbview().in_tx_error():
            query_unit, recovered = await self._legacy_recover_script_error(
                eql,
                allow_capabilities,
            )
            if recovered:
                packet = WriteBuffer.new()
                packet.write_buffer(
                    self.make_legacy_command_complete_msg(query_unit))
                packet.write_buffer(self.sync_status())
                self.write(packet)
                self.flush()
                return
            else:
                skip_first = True

        if self._cancelled:
            raise ConnectionAbortedError

        query_unit = await self._legacy_simple_query(
            eql, allow_capabilities, skip_first)

        packet = WriteBuffer.new()
        packet.write_buffer(self.make_legacy_command_complete_msg(query_unit))
        packet.write_buffer(self.sync_status())
        self.write(packet)
        self.flush()

    async def _legacy_simple_query(
        self,
        eql: bytes,
        allow_capabilities: uint64_t,
        skip_first: bint,
    ):
        cdef:
            bytes state = None, orig_state = None
            int i
            dbview.DatabaseConnectionView _dbview
            pgcon.PGConnection conn

        unit_group = await self._legacy_compile_script(
            eql, skip_first=skip_first)
        metrics.edgeql_query_compilations.inc(1.0, 'compiler')

        if self._cancelled:
            raise ConnectionAbortedError

        if unit_group.capabilities & ~allow_capabilities:
            raise unit_group.capabilities.make_error(
                allow_capabilities,
                errors.DisabledCapabilityError,
            )

        _dbview = self.get_dbview()
        if not _dbview.in_tx():
            orig_state = state = _dbview.serialize_state()

        conn = await self.get_pgcon()
        try:
            if conn.last_state == state:
                # the current status in conn is in sync with dbview, skip the
                # state restoring
                state = None
            for query_unit in unit_group.units:
                if self._cancelled:
                    raise ConnectionAbortedError

                new_types = None
                _dbview.start(query_unit)
                try:
                    if query_unit.create_db_template:
                        await self.server._on_before_create_db_from_template(
                            query_unit.create_db_template, _dbview.dbname
                        )
                    if query_unit.drop_db:
                        await self.server._on_before_drop_db(
                            query_unit.drop_db, _dbview.dbname)

                    if query_unit.system_config:
                        await execute.execute_system_config(
                            conn, _dbview, query_unit)
                    else:
                        if query_unit.sql:
                            if query_unit.ddl_stmt_id:
                                ddl_ret = await conn.run_ddl(query_unit, state)
                                if ddl_ret and ddl_ret['new_types']:
                                    new_types = ddl_ret['new_types']
                            elif query_unit.is_transactional:
                                await conn.sql_execute(
                                    query_unit.sql,
                                    state=state,
                                )
                            else:
                                i = 0
                                for sql in query_unit.sql:
                                    await conn.sql_execute(
                                        sql,
                                        state=state if i == 0 else None,
                                    )
                                    # only apply state to the first query.
                                    i += 1
                            if state is not None:
                                # state is restored, clear orig_state so that
                                # we can set conn.last_state correctly later
                                orig_state = None

                        if query_unit.create_db:
                            await self.server.introspect_db(
                                query_unit.create_db
                            )

                        if query_unit.drop_db:
                            self.server._on_after_drop_db(
                                query_unit.drop_db)

                        if query_unit.config_ops:
                            await _dbview.apply_config_ops(
                                conn,
                                query_unit.config_ops)
                except Exception:
                    _dbview.on_error()
                    if not conn.in_tx() and _dbview.in_tx():
                        # COMMIT command can fail, in which case the
                        # transaction is aborted.  This check workarounds
                        # that (until a better solution is found.)
                        _dbview.abort_tx()
                    raise
                else:
                    side_effects = _dbview.on_success(
                        query_unit, new_types)
                    if side_effects:
                        execute.signal_side_effects(_dbview, side_effects)
                    if not _dbview.in_tx():
                        state = _dbview.serialize_state()
                        if state is not orig_state:
                            # see the same comments in _legacy_execute()
                            conn.last_state = state
                finally:
                    if query_unit.create_db_template:
                        await self.server._allow_database_connections(
                            query_unit.create_db_template,
                        )
        finally:
            self.maybe_release_pgcon(conn)

        return query_unit

    cdef WriteBuffer make_legacy_command_data_description_msg(
        self, dbview.CompiledQuery query
    ):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'T')
        msg.write_int16(1)
        msg.write_int16(SERVER_HEADER_CAPABILITIES)
        msg.write_int32(sizeof(uint64_t))
        msg.write_int64(
            <int64_t>(<uint64_t>query.query_unit_group.capabilities)
        )

        msg.write_byte(self.render_cardinality(query.query_unit_group))

        in_data = query.query_unit_group.in_type_data
        msg.write_bytes(query.query_unit_group.in_type_id)
        msg.write_len_prefixed_bytes(in_data)

        out_data = query.query_unit_group.out_type_data
        msg.write_bytes(query.query_unit_group.out_type_id)
        msg.write_len_prefixed_bytes(out_data)

        msg.end_message()
        return msg

    cdef WriteBuffer make_legacy_command_complete_msg(self, query_unit):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'C')

        msg.write_int16(1)
        msg.write_int16(SERVER_HEADER_CAPABILITIES)
        msg.write_int32(sizeof(uint64_t))
        msg.write_int64(<int64_t><uint64_t>query_unit.capabilities)

        msg.write_len_prefixed_bytes(query_unit.status)
        return msg.end_message()

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

    cdef dict legacy_parse_headers(self):
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
