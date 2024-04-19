#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import edgedb

from edb.testbase import server as tb


class Barrier:
    def __init__(self, number):
        self._counter = number
        self._cond = asyncio.Condition()

    async def ready(self):
        if self._counter == 0:
            return
        async with self._cond:
            self._counter -= 1
            assert self._counter >= 0, self._counter
            if self._counter == 0:
                self._cond.notify_all()
            else:
                await self._cond.wait_for(lambda: self._counter == 0)


class TestServerConcurrentTransactions(tb.QueryTestCase):

    TRANSACTION_ISOLATION = False

    SETUP = '''
        CREATE TYPE Counter {
            CREATE PROPERTY name -> str {
                CREATE CONSTRAINT exclusive;
            };
            CREATE PROPERTY value -> int32 {
                SET default := 0;
            };
        };

        CREATE TYPE Foo {
            CREATE PROPERTY name -> str {
                CREATE CONSTRAINT exclusive;
            };
        };
        CREATE TYPE Bar EXTENDING Foo;
    '''

    async def test_server_concurrent_conflict_retry_1(self):
        await self.execute_conflict_1('counter2')

    async def test_server_concurrent_conflict_no_retry_1(self):
        with self.assertRaises(edgedb.TransactionSerializationError):
            await self.execute_conflict_1(
                'counter3',
                edgedb.RetryOptions(attempts=1, backoff=edgedb.default_backoff)
            )

    async def execute_conflict_1(self, name, options=None):
        q = '''
            SELECT (
                INSERT Counter {
                    name := <str>$name,
                    value := 1,
                } UNLESS CONFLICT ON .name
                ELSE (
                    UPDATE Counter
                    SET { value := .value + 1 }
                )
            ).value
        '''
        f = lambda tx: tx.query_single(q, name=name)

        results, iterations = await self.execute_concurrent_txs(f, f, options)
        self.assertEqual(set(results), {1, 2})
        self.assertEqual(iterations, 3)

    async def test_server_concurrent_conflict_retry_2(self):
        await self.execute_conflict_2('counter4')

    async def test_server_concurrent_conflict_no_retry_2(self):
        with self.assertRaises(edgedb.TransactionSerializationError):
            await self.execute_conflict_2(
                'counter5',
                edgedb.RetryOptions(attempts=1, backoff=edgedb.default_backoff)
            )

    async def execute_conflict_2(self, name, options=None):
        q = '''
            FOR name IN {<str>$name} UNION (
                SELECT (
                    INSERT Counter {
                        name := name,
                        value := 1,
                    } UNLESS CONFLICT ON .name
                    ELSE (
                        UPDATE Counter
                        SET { value := .value + 1 }
                    )
                ).value
            )
        '''
        f = lambda tx: tx.query_single(q, name=name)

        results, iterations = await self.execute_concurrent_txs(f, f, options)
        self.assertEqual(set(results), {1, 2})
        self.assertEqual(iterations, 3)

    async def test_server_concurrent_inserts_1(self):
        f = lambda tx: tx.execute('INSERT Foo { name := "foo" }')
        with self.assertRaises(edgedb.ConstraintViolationError):
            await self.execute_concurrent_txs(f, f)

        await self.assert_query_result(
            r"""
                SELECT Foo { name } FILTER .name = "foo"
            """,
            [{'name': 'foo'}],
        )

    async def test_server_concurrent_inserts_2(self):
        f1 = lambda tx: tx.execute('INSERT Foo { name := "foo" }')
        f2 = lambda tx: tx.execute('INSERT Bar { name := "foo" }')
        with self.assertRaises(edgedb.ConstraintViolationError):
            await self.execute_concurrent_txs(f1, f2)

        await self.assert_query_result(
            r"""
                SELECT Foo { name } FILTER .name = "foo"
            """,
            [{'name': 'foo'}],
        )

    async def execute_concurrent_txs(self, f1, f2, options=None):
        con2 = await self.connect(database=self.get_database_name())
        self.addCleanup(con2.aclose)

        barrier = Barrier(2)
        lock = asyncio.Lock()
        iterations = 0

        async def transaction1(con, f):
            async for tx in con.retrying_transaction():
                nonlocal iterations
                iterations += 1
                held = False
                try:
                    async with tx:
                        # Run SELECT 1 to actually start the transaction.
                        await tx.query("SELECT 1")

                        # Start both transactions at the same initial data.
                        await barrier.ready()

                        await lock.acquire()
                        held = True
                        res = await f(tx)
                finally:
                    if held:
                        lock.release()
                        held = False
            return res

        con = self.con
        if options:
            con = con.with_retry_options(options)
            con2 = con2.with_retry_options(options)

        results = await asyncio.wait_for(asyncio.gather(
            transaction1(con, f1),
            transaction1(con2, f2),
            return_exceptions=True,
        ), 60)
        for e in results:
            if isinstance(e, BaseException):
                raise e

        return results, iterations
