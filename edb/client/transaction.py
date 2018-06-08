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


import enum

from . import exceptions as edgedb_errors


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


ISOLATION_LEVELS = {'read_committed', 'serializable', 'repeatable_read'}


class Transaction:
    """Represents a transaction or savepoint block.

    Transactions are created by calling the
    :meth:`Connection.transaction() <connection.Connection.transaction>`
    function.
    """

    __slots__ = ('_connection', '_isolation', '_readonly', '_deferrable',
                 '_state', '_nested', '_id', '_managed')

    def __init__(self, connection, isolation, readonly, deferrable):
        if isolation not in ISOLATION_LEVELS:
            raise ValueError(
                'isolation is expected to be either of {}, '
                'got {!r}'.format(ISOLATION_LEVELS, isolation))

        if isolation != 'serializable':
            if readonly:
                raise ValueError(
                    '"readonly" is only supported for '
                    'serializable transactions')

            if deferrable and not readonly:
                raise ValueError(
                    '"deferrable" is only supported for '
                    'serializable readonly transactions')

        self._connection = connection
        self._isolation = isolation
        self._readonly = readonly
        self._deferrable = deferrable
        self._state = TransactionState.NEW
        self._nested = False
        self._id = None
        self._managed = False

    async def __aenter__(self):
        if self._managed:
            raise edgedb_errors.InterfaceError(
                'cannot enter context: already in an `async with` block')
        self._managed = True
        await self.start()

    async def __aexit__(self, extype, ex, tb):
        try:
            if extype is not None:
                await self.__rollback()
            else:
                await self.__commit()
        finally:
            self._managed = False

    async def start(self):
        """Enter the transaction or savepoint block."""
        self.__check_state_base('start')
        if self._state is TransactionState.STARTED:
            raise edgedb_errors.InterfaceError(
                'cannot start; the transaction is already started')

        con = self._connection

        if con._top_xact is None:
            con._top_xact = self
        else:
            # Nested transaction block
            top_xact = con._top_xact
            if self._isolation != top_xact._isolation:
                raise edgedb_errors.InterfaceError(
                    'nested transaction has a different isolation level: '
                    'current {!r} != outer {!r}'.format(
                        self._isolation, top_xact._isolation))
            self._nested = True

        if self._nested:
            query = 'START TRANSACTION;'
        else:
            if self._isolation == 'read_committed':
                query = 'START TRANSACTION;'
            elif self._isolation == 'repeatable_read':
                query = 'START TRANSACTION ISOLATION LEVEL REPEATABLE READ;'
            else:
                query = 'START TRANSACTION ISOLATION LEVEL SERIALIZABLE'
                if self._readonly:
                    query += ' READ ONLY'
                if self._deferrable:
                    query += ' DEFERRABLE'
                query += ';'

        try:
            await self._connection.execute(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    def __check_state_base(self, opname):
        if self._state is TransactionState.COMMITTED:
            raise edgedb_errors.InterfaceError(
                'cannot {}; the transaction is already committed'.format(
                    opname))
        if self._state is TransactionState.ROLLEDBACK:
            raise edgedb_errors.InterfaceError(
                'cannot {}; the transaction is already rolled back'.format(
                    opname))
        if self._state is TransactionState.FAILED:
            raise edgedb_errors.InterfaceError(
                'cannot {}; the transaction is in error state'.format(
                    opname))

    def __check_state(self, opname):
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise edgedb_errors.InterfaceError(
                    'cannot {}; the transaction is not yet started'.format(
                        opname))
            self.__check_state_base(opname)

    async def __commit(self):
        self.__check_state('commit')

        if self._connection._top_xact is self:
            self._connection._top_xact = None

        query = 'COMMIT;'

        try:
            await self._connection.execute(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED

    async def __rollback(self):
        self.__check_state('rollback')

        if self._connection._top_xact is self:
            self._connection._top_xact = None

        query = 'ROLLBACK;'

        try:
            await self._connection.execute(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK

    async def commit(self):
        """Exit the transaction or savepoint block and commit changes."""
        if self._managed:
            raise edgedb_errors.InterfaceError(
                'cannot manually commit from within an `async with` block')
        await self.__commit()

    async def rollback(self):
        """Exit the transaction or savepoint block and rollback changes."""
        if self._managed:
            raise edgedb_errors.InterfaceError(
                'cannot manually rollback from within an `async with` block')
        await self.__rollback()

    def __repr__(self):
        attrs = []
        attrs.append('state:{}'.format(self._state.name.lower()))

        attrs.append(self._isolation)
        if self._readonly:
            attrs.append('readonly')
        if self._deferrable:
            attrs.append('deferrable')

        if self.__class__.__module__.startswith('edb.'):
            mod = 'edb'
        else:
            mod = self.__class__.__module__

        return '<{}.{} {} {:#x}>'.format(
            mod, self.__class__.__name__, ' '.join(attrs), id(self))
