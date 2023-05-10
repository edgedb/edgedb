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

"""A specialized client API for EdgeDB tests.

Historically EdgeDB tests relied on a very specific client API that
is no longer supported by edgedb-python. Here we implement that API
(for example, transactions can be nested and are non-retrying).
"""

from __future__ import annotations
import typing

import abc
import asyncio
import enum
import functools
import random
import socket
import ssl
import time

from edgedb import abstract
from edgedb import errors
from edgedb import con_utils
from edgedb import enums as edgedb_enums
from edgedb import options
from edgedb.protocol import protocol  # type: ignore

from edb.protocol import protocol as edb_protocol  # type: ignore


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


class BaseTransaction(abc.ABC):

    ID_COUNTER = 0

    def __init__(self, owner):
        self._connection = owner
        self._state = TransactionState.NEW
        self._managed = False
        self._nested = False

        type(self).ID_COUNTER += 1
        self._id = f'raw_tx_{self.ID_COUNTER}'

    def is_active(self) -> bool:
        return self._state is TransactionState.STARTED

    def __check_state_base(self, opname):
        if self._state is TransactionState.COMMITTED:
            raise errors.InterfaceError(
                f'cannot {opname}; the transaction is already committed')
        if self._state is TransactionState.ROLLEDBACK:
            raise errors.InterfaceError(
                f'cannot {opname}; the transaction is already rolled back')
        if self._state is TransactionState.FAILED:
            raise errors.InterfaceError(
                f'cannot {opname}; the transaction is in error state')

    def __check_state(self, opname):
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise errors.InterfaceError(
                    f'cannot {opname}; the transaction is not yet started')
            self.__check_state_base(opname)

    def _make_start_query(self):
        self.__check_state_base('start')
        if self._state is TransactionState.STARTED:
            raise errors.InterfaceError(
                'cannot start; the transaction is already started')
        return self._make_start_query_inner()

    @abc.abstractmethod
    def _make_start_query_inner(self):
        ...

    def _make_commit_query(self):
        self.__check_state('commit')

        return 'COMMIT;'

    def _make_rollback_query(self):
        self.__check_state('rollback')

        if self._connection._top_xact is self:
            self._connection._top_xact = None

        if self._nested:
            query = f'ROLLBACK TO SAVEPOINT {self._id};'
        else:
            query = 'ROLLBACK;'

        return query

    async def start(self) -> None:
        query = self._make_start_query()
        try:
            await self._connection.execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def commit(self) -> None:
        if self._managed:
            raise errors.InterfaceError(
                'cannot manually commit from within an `async with` block')
        await self._commit()

    async def _commit(self) -> None:
        query = self._make_commit_query()
        try:
            await self._connection.execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED

    async def rollback(self) -> None:
        if self._managed:
            raise errors.InterfaceError(
                'cannot manually rollback from within an `async with` block')
        await self._rollback()

    async def _rollback(self) -> None:
        query = self._make_rollback_query()
        try:
            await self._connection.execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK


class RawTransaction(BaseTransaction):
    def _make_start_query_inner(self):
        con = self._connection

        if con._top_xact is None:
            con._top_xact = self
        else:
            # Nested transaction block
            self._nested = True

        if self._nested:
            query = f'DECLARE SAVEPOINT {self._id};'
        else:
            query = 'START TRANSACTION;'

        return query

    def _make_commit_query(self):
        query = super()._make_commit_query()

        if self._connection._top_xact is self:
            self._connection._top_xact = None

        if self._nested:
            query = f'RELEASE SAVEPOINT {self._id};'

        return query

    def _make_rollback_query(self):
        query = super()._make_rollback_query()

        if self._connection._top_xact is self:
            self._connection._top_xact = None

        if self._nested:
            query = f'ROLLBACK TO SAVEPOINT {self._id};'

        return query

    async def __aenter__(self):
        if self._managed:
            raise errors.InterfaceError(
                'cannot enter context: already in an `async with` block')
        self._managed = True
        await self.start()
        return self

    async def __aexit__(self, extype, ex, tb):
        try:
            if extype is not None:
                await self._rollback()
            else:
                await self._commit()
        finally:
            self._managed = False


class Iteration(BaseTransaction, abstract.AsyncIOExecutor):
    def __init__(self, retry, connection, iteration):
        super().__init__(connection)
        self._options = retry._options.transaction_options
        self.__retry = retry
        self.__iteration = iteration
        self.__started = False

    async def __aenter__(self):
        if self._managed:
            raise errors.InterfaceError(
                'cannot enter context: already in an `async with` block')
        self._managed = True
        return self

    async def __aexit__(self, extype, ex, tb):
        self._managed = False
        if not self.__started:
            return False

        try:
            if extype is None:
                await self._commit()
            else:
                await self._rollback()
        except errors.EdgeDBError as err:
            if ex is None:
                # On commit we don't know if commit is succeeded before the
                # database have received it or after it have been done but
                # network is dropped before we were able to receive a response
                raise err
            # If we were going to rollback, look at original error
            # to find out whether we want to retry, regardless of
            # the rollback error.
            # In this case we ignore rollback issue as original error is more
            # important, e.g. in case `CancelledError` it's important
            # to propagate it to cancel the whole task.
            # NOTE: rollback error is always swallowed, should we use
            # on_log_message for it?

        if (
            extype is not None and
            issubclass(extype, errors.EdgeDBError) and
            ex.has_tag(errors.SHOULD_RETRY)
        ):
            return self.__retry._retry(ex)

    def _make_start_query_inner(self):
        return self._options.start_transaction_query()

    def _get_query_cache(self) -> abstract.QueryCache:
        return self._connection._query_cache

    async def _query(self, query_context: abstract.QueryContext):
        await self._ensure_transaction()
        return await self._connection.raw_query(query_context)

    async def _execute(self, query: abstract.ExecuteContext) -> None:
        await self._ensure_transaction()
        await self._connection._execute(query)

    async def _ensure_transaction(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `async with transaction:`"
            )
        if not self.__started:
            self.__started = True
            if self._connection.is_closed():
                await self._connection.connect(
                    single_attempt=self.__iteration != 0
                )
            await self.start()


class Retry:
    def __init__(self, connection):
        self._connection = connection
        self._iteration = 0
        self._done = False
        self._next_backoff = 0
        self._options = connection._options

    def _retry(self, exc):
        self._last_exception = exc
        rule = self._options.retry_options.get_rule_for_exception(exc)
        if self._iteration >= rule.attempts:
            return False
        self._done = False
        self._next_backoff = rule.backoff(self._iteration)
        return True

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Note: when changing this code consider also
        # updating Retry.__next__.
        if self._done:
            raise StopAsyncIteration
        if self._next_backoff:
            await asyncio.sleep(self._next_backoff)
        self._done = True
        iteration = Iteration(self, self._connection, self._iteration)
        self._iteration += 1
        return iteration


class Connection(options._OptionsMixin, abstract.AsyncIOExecutor):

    _top_xact: RawTransaction | None = None

    def __init__(self, connect_args, *, test_no_tls=False):
        super().__init__()
        self._connect_args = connect_args
        self._protocol = None
        self._transport = None
        self._query_cache = abstract.QueryCache(
            codecs_registry=protocol.CodecsRegistry(),
            query_cache=protocol.QueryCodecsCache(),
        )
        self._test_no_tls = test_no_tls
        self._params = None
        self._log_listeners = set()

    def add_log_listener(self, callback):
        self._log_listeners.add(callback)

    def remove_log_listener(self, callback):
        self._log_listeners.discard(callback)

    def _get_state(self):
        return self._options.state

    def _on_log_message(self, msg):
        if self._log_listeners:
            loop = asyncio.get_running_loop()
            for cb in self._log_listeners:
                loop.call_soon(cb, self, msg)

    def _shallow_clone(self):
        con = self.__class__.__new__(self.__class__)
        con._connect_args = self._connect_args
        con._protocol = self._protocol
        con._query_cache = self._query_cache
        con._test_no_tls = self._test_no_tls
        con._params = self._params
        return con

    def _get_query_cache(self) -> abstract.QueryCache:
        return self._query_cache

    async def _query(self, query_context: abstract.QueryContext):
        await self.ensure_connected()
        return await self.raw_query(query_context)

    async def _execute(self, script: abstract.ExecuteContext) -> None:
        await self.ensure_connected()
        await self._protocol.execute(
            query=script.query.query,
            args=script.query.args,
            kwargs=script.query.kwargs,
            reg=script.cache.codecs_registry,
            qc=script.cache.query_cache,
            output_format=protocol.OutputFormat.NONE,
            allow_capabilities=edgedb_enums.Capability.ALL,  # type: ignore
            state=script.state and script.state.as_dict(),
        )

    async def ensure_connected(self):
        if self.is_closed():
            await self.connect()
        return self

    async def raw_query(self, query_context: abstract.QueryContext):
        return await self._protocol.query(
            query=query_context.query.query,
            args=query_context.query.args,
            kwargs=query_context.query.kwargs,
            reg=query_context.cache.codecs_registry,
            qc=query_context.cache.query_cache,
            output_format=query_context.query_options.output_format,
            expect_one=query_context.query_options.expect_one,
            required_one=query_context.query_options.required_one,
            allow_capabilities=edgedb_enums.Capability.ALL,  # type: ignore
            state=query_context.state and query_context.state.as_dict(),
        )

    async def _fetchall(
        self,
        query: str,
        *args,
        __limit__: int = 0,
        __typeids__: bool = False,
        __typenames__: bool = False,
        __allow_capabilities__: edgedb_enums.Capability = (
            edgedb_enums.Capability.ALL),
        **kwargs,
    ):
        await self.ensure_connected()
        return await self._protocol.query(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._query_cache.codecs_registry,
            qc=self._query_cache.query_cache,
            implicit_limit=__limit__,
            inline_typeids=__typeids__,
            inline_typenames=__typenames__,
            output_format=protocol.OutputFormat.BINARY,
            allow_capabilities=__allow_capabilities__,
        )

    async def _fetchall_json(
        self,
        query: str,
        *args,
        __limit__: int = 0,
        **kwargs,
    ):
        await self.ensure_connected()
        return await self._protocol.query(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._query_cache.codecs_registry,
            qc=self._query_cache.query_cache,
            implicit_limit=__limit__,
            inline_typenames=False,
            output_format=protocol.OutputFormat.JSON,
        )

    async def _fetchall_json_elements(self, query: str, *args, **kwargs):
        await self.ensure_connected()
        return await self._protocol.query(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._query_cache.codecs_registry,
            qc=self._query_cache.query_cache,
            output_format=protocol.OutputFormat.JSON_ELEMENTS,
            allow_capabilities=edgedb_enums.Capability.EXECUTE,  # type: ignore
        )

    def _clear_codecs_cache(self):
        self._query_cache.codecs_registry.clear_cache()

    def _get_last_status(self) -> typing.Optional[str]:
        if self._protocol is None:
            return None
        status = self._protocol.last_status
        if status is not None:
            status = status.decode()
        return status

    def _get_last_capabilities(
        self,
    ) -> typing.Optional[edgedb_enums.Capability]:
        if self._protocol is None:
            return None
        else:
            return self._protocol.last_capabilities

    def is_closed(self):
        return self._protocol is None or not self._protocol.connected

    async def connect(self, single_attempt=False):
        self._params, client_config = con_utils.parse_connect_arguments(
            **self._connect_args,
            command_timeout=None,
            server_settings=None,
        )
        start = time.monotonic()
        if single_attempt:
            max_time = 0
        else:
            max_time = start + client_config.wait_until_available
        iteration = 1

        while True:
            addr = self._params.address
            try:
                await asyncio.wait_for(
                    self.connect_addr(),
                    client_config.connect_timeout,
                )
            except TimeoutError as e:
                if iteration > 1 and time.monotonic() >= max_time:
                    raise errors.ClientConnectionTimeoutError(
                        f"connecting to {addr} failed in"
                        f" {client_config.connect_timeout} sec"
                    ) from e
            except errors.ClientConnectionError as e:
                if (
                    not e.has_tag(errors.SHOULD_RECONNECT) or
                    (iteration > 1 and time.monotonic() >= max_time)
                ):
                    nice_err = e.__class__(
                        con_utils.render_client_no_connection_error(
                            e,
                            addr,
                            attempts=iteration,
                            duration=time.monotonic() - start,
                        ))
                    raise nice_err from e.__cause__
            else:
                return

            iteration += 1
            await asyncio.sleep(0.01 + random.random() * 0.2)

    async def connect_addr(self):
        tr = None
        loop = asyncio.get_running_loop()
        addr = self._params.address
        protocol_factory = functools.partial(
            edb_protocol.Protocol, self._params, loop
        )

        try:
            if isinstance(addr, str):
                # UNIX socket
                tr, pr = await loop.create_unix_connection(
                    protocol_factory, addr
                )
            elif self._test_no_tls:
                tr, pr = await loop.create_connection(protocol_factory, *addr)
            else:
                try:
                    tr, pr = await loop.create_connection(
                        protocol_factory, *addr, ssl=self._params.ssl_ctx
                    )
                except ssl.CertificateError as e:
                    raise con_utils.wrap_error(e) from e
                except ssl.SSLError as e:
                    if e.reason == 'CERTIFICATE_VERIFY_FAILED':
                        raise con_utils.wrap_error(e) from e
                    tr, pr = await loop.create_connection(
                        protocol_factory,
                        *addr,
                    )
                else:
                    con_utils.check_alpn_protocol(
                        tr.get_extra_info('ssl_object')
                    )
        except socket.gaierror as e:
            # All name resolution errors are considered temporary
            raise errors.ClientConnectionFailedTemporarilyError(str(e)) from e
        except OSError as e:
            raise con_utils.wrap_error(e) from e
        except Exception:
            if tr is not None:
                tr.close()
            raise

        pr.set_connection(self)

        try:
            await pr.connect()
        except OSError as e:
            if tr is not None:
                tr.close()
            raise con_utils.wrap_error(e) from e
        except BaseException:
            if tr is not None:
                tr.close()
            raise

        self._protocol = pr
        self._transport = tr

    def retrying_transaction(self) -> Retry:
        return Retry(self)

    def transaction(self) -> RawTransaction:
        return RawTransaction(self)

    def is_in_transaction(self):
        return self._protocol.is_in_transaction()

    def get_settings(self) -> typing.Dict[str, typing.Any]:
        return self._protocol.get_settings()

    @property
    def dbname(self) -> str:
        return self._params.database

    def connected_addr(self):
        return self._params.address

    async def aclose(self):
        if not self.is_closed():
            try:
                self._protocol.terminate()
                await self._protocol.wait_for_disconnect()
            except (Exception, asyncio.CancelledError):
                self.terminate()
                raise

    def terminate(self):
        if not self.is_closed():
            self._protocol.abort()

    def get_transport(self):
        return self._transport


async def async_connect_test_client(
    dsn: typing.Optional[str] = None,
    host: typing.Optional[str] = None,
    port: typing.Optional[int] = None,
    credentials: typing.Optional[str] = None,
    credentials_file: typing.Optional[str] = None,
    user: typing.Optional[str] = None,
    password: typing.Optional[str] = None,
    secret_key: typing.Optional[str] = None,
    database: typing.Optional[str] = None,
    tls_ca: typing.Optional[str] = None,
    tls_ca_file: typing.Optional[str] = None,
    tls_security: typing.Optional[str] = None,
    test_no_tls: bool = False,
    wait_until_available: int = 30,
    timeout: int = 10,
) -> Connection:
    return await Connection(
        {
            "dsn": dsn,
            "host": host,
            "port": port,
            "credentials": credentials,
            "credentials_file": credentials_file,
            "user": user,
            "password": password,
            "secret_key": secret_key,
            "database": database,
            "timeout": timeout,
            "tls_ca": tls_ca,
            "tls_ca_file": tls_ca_file,
            "tls_security": tls_security,
            "wait_until_available": wait_until_available,
        },
        test_no_tls=test_no_tls,
    ).ensure_connected()
