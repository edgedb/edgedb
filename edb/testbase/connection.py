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

import enum

import edgedb
from edgedb import enums as edgedb_enums


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


class RawTransactionError(Exception):
    pass


class RawTransaction:

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
            raise RawTransactionError(
                f'cannot {opname}; the transaction is already committed')
        if self._state is TransactionState.ROLLEDBACK:
            raise RawTransactionError(
                f'cannot {opname}; the transaction is already rolled back')
        if self._state is TransactionState.FAILED:
            raise RawTransactionError(
                f'cannot {opname}; the transaction is in error state')

    def __check_state(self, opname):
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise RawTransactionError(
                    f'cannot {opname}; the transaction is not yet started')
            self.__check_state_base(opname)

    def _make_start_query(self):
        self.__check_state_base('start')
        if self._state is TransactionState.STARTED:
            raise RawTransactionError(
                'cannot start; the transaction is already started')

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
        self.__check_state('commit')

        if self._connection._top_xact is self:
            self._connection._top_xact = None
        if self._nested:
            query = f'RELEASE SAVEPOINT {self._id};'
        else:
            query = 'COMMIT;'

        return query

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
            await self._connection._inner._impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def commit(self) -> None:
        if self._managed:
            raise RawTransactionError(
                'cannot manually commit from within an `async with` block')
        await self._commit()

    async def _commit(self) -> None:
        query = self._make_commit_query()
        try:
            await self._connection._inner._impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED

    async def rollback(self) -> None:
        if self._managed:
            raise RawTransactionError(
                'cannot manually rollback from within an `async with` block')
        await self._rollback()

    async def _rollback(self) -> None:
        query = self._make_rollback_query()
        try:
            await self._connection._inner._impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK

    async def __aenter__(self):
        if self._managed:
            raise RawTransactionError(
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


class Connection(edgedb.AsyncIOConnection):

    _top_xact: RawTransaction | None = None

    def retrying_transaction(self) -> typing.Any:
        return edgedb.AsyncIOConnection.transaction(self)

    def transaction(self) -> RawTransaction:  # type: ignore
        return RawTransaction(self)

    async def _execute(
        self,
        query: str,
        args,
        kwargs,
        io_format,
        expect_one=False,
        required_one=False,
    ):
        inner = self._inner
        if not inner._impl or inner._impl.is_closed():
            await self._reconnect()
        result, _ = \
            await self._inner._impl._protocol.execute_anonymous(
                query=query,
                args=args,
                kwargs=kwargs,
                reg=inner._codecs_registry,
                qc=inner._query_cache,
                io_format=io_format,
                expect_one=expect_one,
                required_one=required_one,
                allow_capabilities=edgedb_enums.Capability.ALL,  # type: ignore
            )
        return result

    async def execute(self, query: str) -> None:
        inner = self._inner
        if not inner._impl or inner._impl.is_closed():
            await self._reconnect()
        await inner._impl._protocol.simple_query(
            query, edgedb_enums.Capability.ALL  # type: ignore
        )


async def async_connect_test_client(*args, **kwargs) -> Connection:
    return await edgedb.async_connect_raw(
        *args,
        connection_class=Connection,
        **kwargs
    )  # type: ignore
