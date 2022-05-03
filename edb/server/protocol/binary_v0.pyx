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


@cython.final
cdef class EdgeConnectionBackwardsCompatible(EdgeConnection):
    def __init__(
        self,
        server,
        external_auth: bool = False,
        passive: bool = False
    ):
        super().__init__(server, external_auth, passive)
        self.min_protocol = MIN_LEGACY_PROTOCOL

    async def legacy_parse(self):
        cdef:
            bytes eql
            QueryRequestInfo query_req

        self._last_anon_compiled = None

        eql, query_req, stmt_name = self.legacy_parse_prepare_query_part(True)
        compiled_query = await self._parse(eql, query_req)

        buf = WriteBuffer.new_message(b'1')  # ParseComplete

        buf.write_int16(1)
        buf.write_int16(SERVER_HEADER_CAPABILITIES)
        buf.write_int32(sizeof(uint64_t))
        buf.write_int64(<int64_t>(
            <uint64_t>compiled_query.query_unit.capabilities
        ))

        buf.write_byte(self.render_cardinality(compiled_query.query_unit))

        if self.protocol_version >= (0, 14):
            buf.write_bytes(compiled_query.query_unit.in_type_id)
            buf.write_len_prefixed_bytes(
                compiled_query.query_unit.in_type_data)

            buf.write_bytes(compiled_query.query_unit.out_type_id)
            buf.write_len_prefixed_bytes(
                compiled_query.query_unit.out_type_data)
        else:
            buf.write_bytes(compiled_query.query_unit.in_type_id)
            buf.write_bytes(compiled_query.query_unit.out_type_id)

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

                msg = self.make_command_data_description_msg(
                    self._last_anon_compiled
                )
                self.write(msg)

        else:
            raise errors.BinaryProtocolError(
                f'unsupported "describe" message mode {chr(rtype)!r}')

    async def legacy_main(self, params):
        cdef:
            char mtype
            bint flush_sync_on_error

        try:
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
                                "protocols greater 0.13")
                        await self.legacy_describe()

                    elif mtype == b'E':
                        await self.legacy_execute()

                    elif mtype == b'O':
                        await self.legacy_optimistic_execute()

                    elif mtype == b'Q':
                        flush_sync_on_error = True
                        await self.simple_query()

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
            QueryRequestInfo query_req

            bytes in_tid
            bytes out_tid
            bytes bound_args

        self._last_anon_compiled = None

        query, query_req, _ = self.legacy_parse_prepare_query_part(False)

        in_tid = self.buffer.read_bytes(16)
        out_tid = self.buffer.read_bytes(16)
        bind_args = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

        query_unit = self.get_dbview().lookup_compiled_query(query_req)
        if query_unit is None:
            if self.debug:
                self.debug_print('OPTIMISTIC EXECUTE /REPARSE', query)

            compiled = await self._parse(query, query_req)
            self._last_anon_compiled = compiled
            query_unit = compiled.query_unit
            if self._cancelled:
                raise ConnectionAbortedError
        else:
            compiled = CompiledQuery(
                query_unit=query_unit,
                first_extra=query_req.source.first_extra(),
                extra_count=query_req.source.extra_count(),
                extra_blob=query_req.source.extra_blob(),
            )
            self._last_anon_compiled = compiled

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

            self.write(self.make_command_data_description_msg(compiled))

            if self._cancelled:
                raise ConnectionAbortedError
            return

        if self.debug:
            self.debug_print('OPTIMISTIC EXECUTE', query)

        metrics.edgeql_query_compilations.inc(1.0, 'cache')
        await self._execute(
            compiled, bind_args, bool(query_unit.sql_hash))

    cdef legacy_parse_prepare_query_part(self, parse_stmt_name: bint):
        cdef:
            object io_format
            bytes eql
            dict headers
            uint64_t implicit_limit = 0
            bint inline_typeids = False
            uint64_t allow_capabilities = ALL_CAPABILITIES
            bint inline_typenames = False
            bint inline_objectids = True
            bytes stmt_name = b''

        headers = self.parse_headers()
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

        io_format = self.parse_io_format(self.buffer.read_byte())
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

        query_req = QueryRequestInfo(
            source,
            self.protocol_version,
            io_format=io_format,
            expect_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            allow_capabilities=allow_capabilities,
        )

        return eql, query_req, stmt_name

    async def legacy_execute(self):
        cdef:
            WriteBuffer bound_args_buf
            uint64_t allow_capabilities = ALL_CAPABILITIES

        headers = self.parse_headers()
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

        await self._execute(compiled, bind_args, False)
