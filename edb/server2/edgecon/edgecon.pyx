#
# This sowurce file is part of the EdgeDB open source project.
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

cimport cython
cimport cpython

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

import immutables

from edb.server2.pgproto cimport hton
from edb.server2.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_read_all,
    frb_get_len,
)

from edb.server2.pgcon cimport pgcon
from edb.server2.pgcon import errors as pgerror

import asyncio

from edb import errors
from edb.lang.common import debug


DEF FLUSH_BUFFER_AFTER = 100_000


@cython.final
cdef class EdgeConnection:

    def __init__(self, server):
        self._con_status = EDGECON_NEW
        self._id = server.new_edgecon_id()
        self.server = server

        self.loop = server.get_loop()
        self.dbview = None
        self.backend = None

        self._transport = None
        self.buffer = ReadBuffer()

        self._parsing = True
        self._reading_messages = False

        self._main_task = None
        self._startup_msg_waiter = self.loop.create_future()
        self._msg_take_waiter = None

        self._last_anon_compiled = None

        self._write_buf = None

    cdef write(self, WriteBuffer buf):
        # One rule for this method: don't write partial messages.
        if self._write_buf is not None:
            self._write_buf.write_buffer(buf)
            if self._write_buf.len() >= FLUSH_BUFFER_AFTER:
                self.flush()
        else:
            self._write_buf = buf

    cdef flush(self):
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
            int16_t hi
            int16_t lo
            char mtype

            WriteBuffer msg_buf
            WriteBuffer buf

        await self._startup_msg_waiter

        hi = self.buffer.read_int16()
        lo = self.buffer.read_int16()
        if hi != 1 or lo != 0:
            raise errors.UnsupportedProtocolVersionError

        self._con_status = EDGECON_STARTED

        await self.wait_for_message()
        mtype = self.buffer.get_message_type()
        if mtype == b'0':
            user = self.buffer.read_utf8()
            password = self.buffer.read_utf8()
            database = self.buffer.read_utf8()

            # XXX implement auth
            self.dbview = self.server.new_view(
                dbname=database, user=user)
            self.backend = await self.server.new_backend(
                dbname=database, dbver=self.dbview.dbver)

            buf = WriteBuffer()

            msg_buf = WriteBuffer.new_message(b'R')
            msg_buf.write_int32(0)
            msg_buf.end_message()
            buf.write_buffer(msg_buf)

            msg_buf = WriteBuffer.new_message(b'K')
            msg_buf.write_int32(0)  # TODO: should send ID of this connection
            msg_buf.end_message()
            buf.write_buffer(msg_buf)

            msg_buf = WriteBuffer.new_message(b'Z')
            msg_buf.write_byte(b'I')
            msg_buf.end_message()
            buf.write_buffer(msg_buf)

            self.write(buf)
            self.flush()

            self.buffer.finish_message()

        else:
            self.fallthrough(False)

    #############

    async def _compile(self, bytes eql, bint json_mode):
        if self.dbview.in_tx:
            return await self.backend.compiler.call(
                'compile_eql_in_tx',
                self.dbview.txid,
                eql,
                json_mode)
        else:
            return await self.backend.compiler.call(
                'compile_eql',
                self.dbview.dbver,
                eql,
                self.dbview.modaliases,
                self.dbview.config,
                json_mode)

    async def _compile_script(self, bytes eql, bint json_mode,
                              bint legacy_mode, bint graphql_mode):

        if self.dbview.in_tx:
            return await self.backend.compiler.call(
                'compile_eql_script_in_tx',
                self.dbview.txid,
                eql,
                json_mode,
                legacy_mode,
                graphql_mode)
        else:
            return await self.backend.compiler.call(
                'compile_eql_script',
                self.dbview.dbver,
                eql,
                self.dbview.modaliases,
                self.dbview.config,
                json_mode,
                legacy_mode,
                graphql_mode)

    async def legacy(self):
        cdef:
            WriteBuffer msg
            WriteBuffer packet

        lang = self.buffer.read_byte()
        graphql = lang == b'g'

        eql = self.buffer.read_null_str()
        if not eql:
            raise errors.BinaryProtocolError('empty query')

        units = await self._compile_script(eql, True, True, graphql)

        resbuf = []
        for unit in units:
            self.dbview.start(unit)
            if unit.sql:
                try:
                    res = await self.backend.pgcon.simple_query(
                        unit.sql, ignore_data=False)
                except Exception as ex:
                    self.dbview.on_error(unit)
                    if not self.backend.pgcon.in_tx():
                        # COMMIT command can fail, in which case the
                        # transaction is finished.  This check workarounds
                        # that (until a better solution is found.)
                        self.dbview._new_tx_state()
                    raise
                else:
                    self.dbview.on_success(unit)

                if res is not None:
                    resbuf.extend(r[0] for r in res)
                else:
                    resbuf.append(b'null')

            else:
                # SET command or something else that doesn't involve
                # executing SQL.
                self.dbview.on_success(unit)
                resbuf.append(b'null')

        resbuf = b'[' + b', '.join(resbuf) + b']'
        msg = WriteBuffer.new_message(b'L')
        msg.write_bytes(resbuf)

        packet = WriteBuffer.new()
        packet.write_buffer(msg.end_message())
        packet.write_buffer(self.pgcon_last_sync_status())
        self.write(packet)

    async def parse(self):
        json_mode = False

        self._last_anon_compiled = None

        stmt_name = self.buffer.read_utf8()
        if stmt_name:
            raise errors.UnsupportedFeatureError(
                'prepared statements are not yet supported')

        eql = self.buffer.read_null_str()
        if not eql:
            raise errors.BinaryProtocolError('empty query')

        compiled = self.dbview.lookup_compiled_query(eql, json_mode)
        cached = True
        if compiled is None:
            cached = False
            compiled = await self._compile(eql, json_mode)

        await self.backend.pgcon.parse_execute(
            1, 0, compiled, self, None, 0, 0)

        if not cached and compiled.is_preparable():
            self.dbview.cache_compiled_query(eql, json_mode, compiled)

        self._last_anon_compiled = compiled

        buf = WriteBuffer.new_message(b'1')  # ParseComplete
        buf.write_bytes(compiled.in_type_id)
        buf.write_bytes(compiled.out_type_id)
        buf.end_message()

        self.write(buf)

    #############

    cdef make_describe_response(self, compiled):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'T')

        in_data = compiled.in_type_data
        msg.write_bytes(compiled.in_type_id)
        msg.write_int16(len(in_data))
        msg.write_bytes(in_data)

        out_data = compiled.out_type_data
        msg.write_bytes(compiled.out_type_id)
        msg.write_int16(len(out_data))
        msg.write_bytes(out_data)

        msg.end_message()
        return msg

    async def describe(self):
        cdef:
            char rtype
            WriteBuffer msg

        rtype = self.buffer.read_byte()
        if rtype == b'T':
            # describe "type id"
            stmt_name = self.buffer.read_utf8()

            if stmt_name:
                raise errors.UnsupportedFeatureError(
                    'prepared statements are not yet supported')
            else:
                if self._last_anon_compiled is None:
                    raise errors.TypeSpecNotFoundError(
                        'no prepared anonymous statement found')

                msg = self.make_describe_response(self._last_anon_compiled)
                self.write(msg)

        else:
            raise errors.BinaryProtocolError(
                f'unsupported "describe" message mode {chr(rtype)!r}')

    async def execute(self):
        cdef:
            WriteBuffer bound_args_buf
            bint send_sync

        stmt_name = self.buffer.read_utf8()
        bind_args = self.buffer.consume_message()
        compiled = None

        if stmt_name:
            raise errors.UnsupportedFeatureError(
                'prepared statements are not yet supported')
        else:
            if self._last_anon_compiled is None:
                raise errors.BinaryProtocolError(
                    'no prepared anonymous statement found')

            compiled = self._last_anon_compiled

        bound_args_buf = self.recode_bind_args(bind_args)

        send_sync = False
        if self.buffer.take_message_type(b'S'):
            # A "Sync" message follows this "Execute" message;
            # send it right away.
            send_sync = True
            self.buffer.finish_message()

        self.dbview.start(compiled)
        if compiled.sql:
            try:
                await self.backend.pgcon.parse_execute(
                    0, 1, compiled,
                    self, bound_args_buf,
                    send_sync, 0)
            except Exception:
                self.dbview.on_error(compiled)
                if not self.backend.pgcon.in_tx():
                    # COMMIT command can fail, in which case the
                    # transaction is finished.  This check workarounds
                    # that (until a better solution is found.)
                    self.dbview._new_tx_state()
                raise
            else:
                self.dbview.on_success(compiled)
        else:
            # SET command or something else that doesn't involve
            # executing SQL.
            self.dbview.on_success(compiled)

        self.write(WriteBuffer.new_message(b'C').end_message())

        if send_sync:
            self.write(self.pgcon_last_sync_status())
            self.flush()

    async def opportunistic_execute(self):
        cdef:
            WriteBuffer bound_args_buf
            bint send_sync
            bytes in_tid
            bytes out_tid
            bytes bound_args

        json_mode = False

        query = self.buffer.read_null_str()
        in_tid = self.buffer.read_bytes(16)
        out_tid = self.buffer.read_bytes(16)
        bound_args = self.buffer.consume_message()

        if not query:
            raise errors.BinaryProtocolError('empty query')

        compiled = self.dbview.lookup_compiled_query(query, json_mode)
        if (compiled is None or
                compiled.in_type_id != in_tid or
                compiled.out_type_id != out_tid):

            # Either the query is no longer compiled or the client has
            # outdated information about type specs.

            # Check if we need to compile this query.
            cached = True
            if compiled is None:
                cached = False
                compiled = await self._compile(query, json_mode)

            if not cached and compiled.is_preparable():
                self.dbview.cache_compiled_query(query, json_mode, compiled)

            send_sync = False
            if self.buffer.take_message_type(b'S'):
                # A "Sync" message follows this "Execute" message;
                # send it right away.
                send_sync = True
                self.buffer.finish_message()

            self.dbview.start(compiled)
            if compiled.sql:
                try:
                    await self.backend.pgcon.parse_execute(
                        1, 0, compiled, self, None, 0, 0)
                except Exception:
                    self.dbview.on_error(compiled)
                    if not self.backend.pgcon.in_tx():
                        # COMMIT command can fail, in which case the
                        # transaction is finished.  This check workarounds
                        # that (until a better solution is found.)
                        self.dbview._new_tx_state()
                    raise
                else:
                    self.dbview.on_success(compiled)
            else:
                self.dbview.on_success(compiled)

            self.write(self.make_describe_response(compiled))
            if send_sync:
                self.write(self.pgcon_last_sync_status())
                self.flush()

            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'E':
                        await self.execute()
                        return
                    else:
                        self.fallthrough(False)
                finally:
                    self.buffer.finish_message()

        else:
            send_sync = False
            if self.buffer.take_message_type(b'S'):
                # A "Sync" message follows this "Execute" message;
                # send it right away.
                send_sync = True
                self.buffer.finish_message()

            if compiled.sql:
                try:
                    await self.backend.pgcon.parse_execute(
                        1, 1,
                        compiled, self,
                        self.recode_bind_args(bound_args),
                        send_sync, compiled.is_preparable())
                except Exception:
                    self.dbview.on_error(compiled)
                    raise
                else:
                    self.dbview.on_success(compiled)
            else:
                self.dbview.on_success(compiled)

            if send_sync:
                self.write(self.pgcon_last_sync_status())
                self.flush()

    async def sync(self):
        cdef:
            WriteBuffer buf

        await self.backend.pgcon.sync()
        self.write(self.pgcon_last_sync_status())

        self.flush()

    async def main(self):
        cdef:
            char mtype

        try:
            await self.auth()
        except Exception as ex:
            await self.write_error(ex)
            self._transport.abort()

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

                legacy_mode = False

                try:
                    if mtype == b'P':
                        await self.parse()

                    elif mtype == b'D':
                        await self.describe()

                    elif mtype == b'E':
                        await self.execute()

                    elif mtype == b'O':
                        await self.opportunistic_execute()

                    elif mtype == b'S':
                        await self.sync()

                    elif mtype == b'L':
                        legacy_mode = True
                        await self.legacy()
                        self.flush()

                    else:
                        self.fallthrough(False)

                except Exception as ex:
                    self.dbview.tx_error()
                    self.buffer.finish_message()

                    await self.write_error(ex)

                    if legacy_mode:
                        self.write(self.pgcon_last_sync_status())
                        self.flush()
                    else:
                        await self.recover_from_error()

                else:
                    self.buffer.finish_message()

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

            self._transport.abort()

    async def recover_from_error(self):
        # Consume all messages until sync.

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'Z':
                self.sync()
                return
            else:
                self.fallthrough(True)

    async def write_error(self, exc):
        cdef:
            WriteBuffer buf

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
                exc = await self.backend.compiler.call(
                    'interpret_backend_error',
                    self.dbview.dbver,
                    exc.fields)
            except Exception as ex:
                exc = RuntimeError(
                    'unhandled error while calling interpret_backend_error()')

        fields = None
        if (isinstance(exc, errors.EdgeDBError) and
                type(exc) is not errors.EdgeDBError):
            exc_code = exc.get_code()
            fields = exc._attrs

        if not exc_code:
            exc_code = errors.InternalServerError.get_code()

        buf = WriteBuffer.new_message(b'E')
        buf.write_int32(<int32_t><uint32_t>exc_code)

        buf.write_utf8(str(exc))

        if fields is not None:
            for k, v in fields.items():
                assert len(k) == 1
                buf.write_byte(ord(k.encode()))
                buf.write_utf8(str(v))

        buf.write_byte(b'\x00')

        buf.end_message()

        self.write(buf)

    cdef pgcon_last_sync_status(self):
        cdef:
            pgcon.PGTransactionStatus xact_status
            WriteBuffer buf

        xact_status = <pgcon.PGTransactionStatus>(
            (<pgcon.PGProto>self.backend.pgcon).xact_status)

        buf = WriteBuffer.new_message(b'Z')
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

    cdef WriteBuffer recode_bind_args(self, bytes bind_args):
        cdef:
            FRBuffer in_buf
            WriteBuffer out_buf = WriteBuffer.new()
            int32_t argsnum
            ssize_t in_len

        assert cpython.PyBytes_CheckExact(bind_args)
        frb_init(
            &in_buf,
            cpython.PyBytes_AS_STRING(bind_args),
            cpython.Py_SIZE(bind_args))

        # all parameters are in binary
        out_buf.write_int32(0x00010001)

        frb_read(&in_buf, 4)  # ignore buffer length

        # number of elements in the tuple
        argsnum = hton.unpack_int32(frb_read(&in_buf, 4))

        out_buf.write_int16(<int16_t>argsnum)

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
        # self.server.edgecon_register(self)

    def connection_lost(self, exc):
        if (self._msg_take_waiter is not None and
                not self._msg_take_waiter.done()):
            self._msg_take_waiter.set_exception(ConnectionAbortedError())
            self._msg_take_waiter = None

        self._transport = None

        if self.backend is not None:
            self.loop.create_task(self.backend.close())

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if self._con_status == EDGECON_NEW and self.buffer.len() >= 4:
            self._startup_msg_waiter.set_result(True)

        elif self._msg_take_waiter is not None and self.buffer.take_message():
            self._msg_take_waiter.set_result(True)
            self._msg_take_waiter = None

    def eof_received(self):
        pass
