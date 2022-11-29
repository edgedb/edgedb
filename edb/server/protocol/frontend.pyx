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
import logging
import time

from edb import errors
from edb.common import debug
from edb.server import args as srvargs
from edb.server.pgcon import errors as pgerror

DEF FLUSH_BUFFER_AFTER = 100_000
cdef object logger = logging.getLogger('edb.server')


cdef class AbstractFrontendConnection:

    cdef write(self, WriteBuffer buf):
        raise NotImplementedError

    cdef flush(self):
        raise NotImplementedError


cdef class FrontendConnection(AbstractFrontendConnection):

    def __init__(
        self,
        server,
        *,
        passive: bool,
        transport: srvargs.ServerConnTransport,
        external_auth: bool,
    ):
        self._id = server.on_binary_client_created()
        self.server = server
        self.loop = server.get_loop()
        self.dbname = None

        self._transport = None
        self._write_buf = None
        self._write_waiter = None

        self.buffer = ReadBuffer()
        self._msg_take_waiter = None

        self.idling = False
        self.started_idling_at = 0.0

        # In "passive" mode the protocol is instantiated to parse and execute
        # just what's in the buffer. It cannot "wait for message". This
        # is used to implement binary protocol over http+fetch.
        self._passive_mode = passive

        self.authed = False
        self._main_task = None
        self._cancelled = False
        self._stop_requested = False

        self.debug = debug.flags.server_proto

        self._transport_proto = transport
        self._external_auth = external_auth

    def get_id(self):
        return self._id

    # I/O write methods, implements AbstractFrontendConnection

    cdef write(self, WriteBuffer buf):
        # One rule for this method: don't write partial messages.
        if self._write_buf is not None:
            self._write_buf.write_buffer(buf)
            if self._write_buf.len() >= FLUSH_BUFFER_AFTER:
                self.flush()
        else:
            self._write_buf = buf

    cdef flush(self):
        if self._transport is None:
            # could be if the connection is lost and a coroutine
            # method is finalizing.
            raise ConnectionAbortedError
        if self._write_buf is not None and self._write_buf.len():
            buf = self._write_buf
            self._write_buf = None
            self._transport.write(memoryview(buf))

    def pause_writing(self):
        if self._write_waiter and not self._write_waiter.done():
            return
        self._write_waiter = self.loop.create_future()

    def resume_writing(self):
        if not self._write_waiter or self._write_waiter.done():
            return
        self._write_waiter.set_result(True)

    # I/O read methods

    def data_received(self, data):
        self.buffer.feed_data(data)
        if self._msg_take_waiter is not None and self.buffer.take_message():
            self._msg_take_waiter.set_result(True)
            self._msg_take_waiter = None

    def eof_received(self):
        pass

    cdef _after_idling(self):
        # Hook for EdgeConnection
        pass

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

        self._after_idling()

    def is_idle(self, expiry_time: float):
        # A connection is idle if it awaits for the next message for
        # client for too long (even if it is in an open transaction!)
        return self.idling and self.started_idling_at < expiry_time

    # establishing a new connection

    cdef _main_task_created(self):
        pass

    cdef _main_task_stopped_normally(self):
        pass

    def connection_made(self, transport):
        if not self.server._accepting_connections:
            transport.abort()
            return

        self._transport = transport

        if self.server._accept_new_tasks:
            self._main_task = self.server.create_task(
                self.main(), interruptable=False
            )
            self._main_task_created()
        else:
            transport.abort()

    # main skeleton

    async def main_step(self, char mtype):
        raise NotImplementedError

    cdef write_error(self, exc):
        raise NotImplementedError

    async def main(self):
        cdef char mtype

        try:
            rv = await self.authenticate()
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

        # HACK for legacy protocol
        if rv is not None:
            return await rv

        self.authed = True

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
                if await self.main_step(mtype):
                    break

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
                self._main_task_stopped_normally()
                self.close()
            else:
                # Abort the connection.
                # It might have already been cleaned up, but abort() is
                # safe to be called on a closed connection.
                self.abort()

    # shutting down the connection

    def close(self):
        if self._transport is not None:
            self.flush()
            self._transport.close()
            self._transport = None

    def abort(self):
        if self._transport is not None:
            self._transport.abort()
            self._transport = None

    def stop(self):
        # Actively stop a frontend connection - this is used by the server
        # when it's stopping.
        self._stop_requested = True
        if self._msg_take_waiter is not None:
            if not self._msg_take_waiter.done():
                self._msg_take_waiter.cancel()

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def is_alive(self):
        return self._transport is not None and not self._cancelled

    cdef _cancel(self):
        pass

    def connection_lost(self, exc):
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
                self._cancel()

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

    # Authentication

    async def authenticate(self):
        raise NotImplementedError

    async def _auth_scram(self, user):
        raise NotImplementedError

    def _auth_jwt(self, user, params):
        raise NotImplementedError

    def _auth_trust(self, user):
        roles = self.server.get_roles()
        if user not in roles:
            raise errors.AuthenticationError('authentication failed')

    async def _authenticate(self, user, database, params):
        self.dbname = database

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
            self._auth_jwt(user, params)
        elif authmethod_name == 'Trust':
            self._auth_trust(user)
        else:
            raise errors.InternalServerError(
                f'unimplemented auth method: {authmethod_name}')

        logger.debug('successfully authenticated %s in database %s',
                     user, database)
