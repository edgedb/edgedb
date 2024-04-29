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
import contextlib
import logging
import time

from edgedb import scram

from edb import errors
from edb.common import debug
from edb.server import args as srvargs, metrics
from edb.server.pgcon import errors as pgerror

from . cimport auth_helpers


DEF FLUSH_BUFFER_AFTER = 100_000
cdef object logger = logging.getLogger('edb.server')


cdef class AbstractFrontendConnection:

    cdef write(self, WriteBuffer buf):
        raise NotImplementedError

    cdef flush(self):
        raise NotImplementedError


cdef class FrontendConnection(AbstractFrontendConnection):
    interface = "frontend"

    def __init__(
        self,
        server,
        tenant,
        *,
        passive: bool,
        transport: srvargs.ServerConnTransport,
        external_auth: bool,
        connection_made_at: float | None = None,
    ):
        self._id = server.on_binary_client_created()
        self.server = server
        self.tenant = tenant
        self.loop = server.get_loop()
        self.dbname = None

        self._pinned_pgcon = None
        self._pinned_pgcon_in_tx = False
        self._get_pgcon_cc = 0

        self.connection_made_at = connection_made_at
        self._query_count = 0
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
        self._pgcon_released_in_connection_lost = False

        self.debug = debug.flags.server_proto

        self._transport_proto = transport
        self._external_auth = external_auth

    def get_id(self):
        return self._id

    cdef is_in_tx(self):
        return False

    # backend connection

    def __del__(self):
        # Should not ever happen, there's a strong ref to
        # every client connection until it hits connection_lost().
        if self._pinned_pgcon is not None:
            # XXX/TODO: add test diagnostics for this and
            # fail all tests if this ever happens.
            self.abort_pinned_pgcon()

    async def get_pgcon(self) -> pgcon.PGConnection:
        if self._cancelled or self._pgcon_released_in_connection_lost:
            raise RuntimeError(
                'cannot acquire a pgconn; the connection is closed')
        self._get_pgcon_cc += 1
        try:
            if self._get_pgcon_cc > 1:
                raise RuntimeError('nested get_pgcon() calls are prohibited')
            if self.is_in_tx():
                #  In transaction. We must have a working pinned connection.
                if not self._pinned_pgcon_in_tx or self._pinned_pgcon is None:
                    raise RuntimeError(
                        'get_pgcon(): in dbview transaction, '
                        'but `_pinned_pgcon` is None')
                return self._pinned_pgcon
            if self._pinned_pgcon is not None:
                raise RuntimeError('there is already a pinned pgcon')
            conn = await self.tenant.acquire_pgcon(self.dbname)
            self._pinned_pgcon = conn
            conn.pinned_by = self
            return conn
        except Exception:
            self._get_pgcon_cc -= 1
            raise

    def maybe_release_pgcon(self, pgcon.PGConnection conn):
        self._get_pgcon_cc -= 1
        if self._get_pgcon_cc < 0:
            raise RuntimeError(
                'maybe_release_pgcon() called more times than get_pgcon()')
        if self._pinned_pgcon is not conn:
            raise RuntimeError('mismatched released connection')

        if self.is_in_tx():
            if self._cancelled:
                # There could be a situation where we cancel the protocol while
                # it's in a transaction. In which case we want to immediately
                # return the connection to the pool (where it would be
                # discarded and re-opened.)
                conn.pinned_by = None
                self._pinned_pgcon = None
                if not self._pgcon_released_in_connection_lost:
                    self.tenant.release_pgcon(
                        self.dbname,
                        conn,
                        discard=debug.flags.server_clobber_pg_conns,
                    )
            else:
                self._pinned_pgcon_in_tx = True
        else:
            conn.pinned_by = None
            self._pinned_pgcon_in_tx = False
            self._pinned_pgcon = None
            if not self._pgcon_released_in_connection_lost:
                self.tenant.release_pgcon(
                    self.dbname,
                    conn,
                    discard=debug.flags.server_clobber_pg_conns,
                )

    @contextlib.asynccontextmanager
    async def with_pgcon(self):
        con = await self.get_pgcon()
        try:
            yield con
        finally:
            self.maybe_release_pgcon(con)

    def on_aborted_pgcon(self, pgcon.PGConnection conn):
        try:
            self._pinned_pgcon = None

            if not self._pgcon_released_in_connection_lost:
                self.tenant.release_pgcon(self.dbname, conn, discard=True)

            if conn.aborted_with_error is not None:
                self.write_error(conn.aborted_with_error)
        finally:
            self.close()  # will flush

    cdef abort_pinned_pgcon(self):
        if self._pinned_pgcon is not None:
            self._pinned_pgcon.pinned_by = None
            self._pinned_pgcon.abort()
            self.tenant.release_pgcon(
                self.dbname, self._pinned_pgcon, discard=True)
            self._pinned_pgcon = None

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

    def get_tenant_label(self):
        if self.tenant is None:
            return "unknown"
        else:
            return self.tenant.get_instance_name()

    def connection_made(self, transport):
        if self.tenant is None:
            self._transport = transport
            self._main_task = self.loop.create_task(self.handshake())
            self._main_task_created()
        elif self.tenant.is_accepting_connections():
            self._transport = transport
            self._main_task = self.tenant.create_task(
                self.main(), interruptable=False
            )
            self._main_task_created()
        else:
            transport.abort()

    async def handshake(self):
        try:
            await self._handshake()
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
                        f'unhandled error in {self.__class__.__name__} while '
                        'accepting new connection'
                    ),
                    'exception': ex,
                    'protocol': self,
                    'transport': self._transport,
                    'task': self._main_task,
                })

    async def _handshake(self):
        if self.tenant is None:
            self.tenant = self.server.get_default_tenant()
        if self.tenant.is_accepting_connections():
            self._main_task = self.tenant.create_task(
                self.main(), interruptable=False
            )
        else:
            if self._transport is not None:
                self._transport.abort()

    # main skeleton

    async def main_step(self, char mtype):
        raise NotImplementedError

    cdef write_error(self, exc):
        raise NotImplementedError

    async def main(self):
        cdef char mtype

        try:
            await self.authenticate()
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
                        f'unhandled error in {self.__class__.__name__} while '
                        'accepting new connection'
                    ),
                    'exception': ex,
                    'protocol': self,
                    'transport': self._transport,
                    'task': self._main_task,
                })

            return

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

        except ConnectionError:
            metrics.connection_errors.inc(
                1.0, self.get_tenant_label(),
            )

        except pgerror.BackendQueryCancelledError:
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

    cdef stop_connection(self):
        pass

    def close(self):
        self.abort_pinned_pgcon()
        self.stop_connection()
        if self._transport is not None:
            self.flush()
            self._transport.close()
            self._transport = None

    def abort(self):
        self.abort_pinned_pgcon()
        self.stop_connection()
        if self._transport is not None:
            self._transport.abort()
            self._transport = None

    def request_stop(self):
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

        if self.connection_made_at is not None:
            tenant_label = self.get_tenant_label()
            metrics.client_connection_duration.observe(
                time.monotonic() - self.connection_made_at,
                tenant_label,
                self.interface,
            )
            if self.authed:
                metrics.queries_per_connection.observe(
                    self._query_count, tenant_label, self.interface
                )
            if isinstance(exc, ConnectionError):
                metrics.connection_errors.inc(1.0, tenant_label)

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

            # Make sure nothing is blocked on flow control.
            # (Currently only dump uses this.)
            self.resume_writing()

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
                    if self.tenant.accept_new_tasks:
                        self.tenant.create_task(
                            self.tenant.cancel_and_discard_pgcon(
                                self._pinned_pgcon, self.dbname
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

    # Authentication

    async def authenticate(self):
        raise NotImplementedError

    def _auth_jwt(self, user, database, params):
        raise NotImplementedError

    def _auth_trust(self, user):
        roles = self.tenant.get_roles()
        if user not in roles:
            raise errors.AuthenticationError('authentication failed')

    async def _authenticate(self, user, database, params):
        # The user has already been authenticated by other means
        # (such as the ability to write to a protected socket).
        if self._external_auth:
            authmethods = [
                self.server.config_settings.get_type_by_name('cfg::Trust')()
            ]
        else:
            authmethods = await self.tenant.get_auth_methods(
                user, self._transport_proto)

        auth_errors = {}

        for authmethod in authmethods:
            authmethod_name = authmethod._tspec.name.split('::')[1]

            try:
                if authmethod_name == 'SCRAM':
                    await self._auth_scram(user)
                elif authmethod_name == 'JWT':
                    self._auth_jwt(user, database, params)
                elif authmethod_name == 'Trust':
                    self._auth_trust(user)
                elif authmethod_name == 'Password':
                    raise errors.AuthenticationError(
                        'authentication failed: '
                        'Simple password authentication required but it is '
                        'only supported for HTTP endpoints'
                    )
                elif authmethod_name == 'mTLS':
                    auth_helpers.auth_mtls_with_user(self._transport, user)
                else:
                    raise errors.InternalServerError(
                        f'unimplemented auth method: {authmethod_name}')
            except errors.AuthenticationError as e:
                auth_errors[authmethod_name] = e
            else:
                break

        if len(auth_errors) == len(authmethods):
            if len(auth_errors) > 1:
                desc = "; ".join(
                    f"{k}: {e.args[0]}" for k, e in auth_errors.items())
                raise errors.AuthenticationError(
                    f"all authentication methods failed: {desc}")
            else:
                raise next(iter(auth_errors.values()))

    cdef WriteBuffer _make_authentication_sasl_initial(self, list methods):
        raise NotImplementedError

    cdef _expect_sasl_initial_response(self):
        raise NotImplementedError

    cdef WriteBuffer _make_authentication_sasl_msg(
        self, bytes data, bint final
    ):
        raise NotImplementedError

    cdef bytes _expect_sasl_response(self):
        raise NotImplementedError

    async def _auth_scram(self, user):
        cdef WriteBuffer msg_buf

        # Tell the client that we require SASL SCRAM auth.
        msg_buf = self._make_authentication_sasl_initial([b'SCRAM-SHA-256'])
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
                # Initial response.
                (
                    selected_mech, client_first
                ) = self._expect_sasl_initial_response()
                if selected_mech != b'SCRAM-SHA-256':
                    raise errors.BinaryProtocolError(
                        f'client selected an invalid SASL authentication '
                        f'mechanism')
                verifier, mock_auth = auth_helpers.scram_get_verifier(
                    self.tenant, user)

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

                msg_buf = self._make_authentication_sasl_msg(server_first, 0)
                self.write(msg_buf)
                self.flush()

            else:
                # client final message
                client_final = self._expect_sasl_response()
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
                ).encode('utf-8')

                # AuthenticationSASLFinal
                msg_buf = self._make_authentication_sasl_msg(server_final, 1)
                self.write(msg_buf)
                self.flush()

                done = True
