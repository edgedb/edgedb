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


import edgedb

from edb.server import _testbase as tb
from edb.tools import test


class TestTransactions(tb.QueryTestCase):
    SETUP = """
        CREATE TYPE test::TransactionTest EXTENDING std::Object {
            CREATE PROPERTY test::name -> std::str;
        };
    """

    ISOLATED_METHODS = False

    async def test_transaction_regular_01(self):
        self.assertIsNone(self.con._top_xact)
        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)

        with self.assertRaises(ZeroDivisionError):
            async with tr as with_tr:
                self.assertIs(self.con._top_xact, tr)

                # We don't return the transaction object from __aenter__,
                # to make it harder for people to use '.rollback()' and
                # '.commit()' from within an 'async with' block.
                self.assertIsNone(with_tr)

                await self.query('''
                    INSERT test::TransactionTest {
                        name := 'Test Transaction'
                    };
                ''')

                1 / 0

        self.assertIsNone(self.con._top_xact)

        result = await self.query('''
            SELECT
                test::TransactionTest
            FILTER
                test::TransactionTest.name = 'Test Transaction';
        ''')

        self.assertEqual(result[0], [])

    @test.not_implemented('savepoints are not supported yet')
    async def test_transaction_nested_01(self):
        self.assertIsNone(self.con._top_xact)
        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)

        with self.assertRaises(ZeroDivisionError):
            async with tr:
                self.assertIs(self.con._top_xact, tr)

                async with self.con.transaction():
                    self.assertIs(self.con._top_xact, tr)

                    await self.query('''
                        INSERT test::TransactionTest {
                            name := 'TXTEST 1'
                        };
                    ''')

                self.assertIs(self.con._top_xact, tr)

                with self.assertRaises(ZeroDivisionError):
                    in_tr = self.con.transaction()
                    async with in_tr:

                        self.assertIs(self.con._top_xact, tr)

                        await self.query('''
                            INSERT test::TransactionTest {
                                name := 'TXTEST 2'
                            };
                        ''')

                        1 / 0

                result = await self.query('''
                    SELECT
                        test::TransactionTest {
                            name
                        }
                    FILTER
                        test::TransactionTest.name LIKE 'TXTEST%';
                ''')

                recs = result[0]

                self.assertEqual(len(recs), 1)
                self.assertEqual(recs[0]['name'], 'TXTEST 1')
                self.assertIs(self.con._top_xact, tr)

                1 / 0

        self.assertIs(self.con._top_xact, None)

        result = await self.query('''
            SELECT
                test::TransactionTest {
                    name
                }
            FILTER
                test::TransactionTest.name LIKE 'TXTEST%';
        ''')

        recs = result[0]
        self.assertEqual(len(recs), 0)

    @test.not_implemented('savepoints are not supported yet')
    async def test_transaction_nested_02(self):
        await self.assert_query_result(r"""
            # test some explicit nested transactions without errors
            SELECT test::TransactionTest{name};

            START TRANSACTION;
                INSERT test::TransactionTest{name:='q1'};
                INSERT test::TransactionTest{name:='q2'};
                SELECT test::TransactionTest.name;

                DECLARE SAVEPOINT f1;
                    INSERT test::TransactionTest{name:='w1'};
                    SELECT test::TransactionTest.name;
                RESTORE SAVEPOINT f1;
                SELECT test::TransactionTest.name;

                DECLARE SAVEPOINT f2;
                    INSERT test::TransactionTest{name:='e1'};
                    SELECT test::TransactionTest.name;
                RELEASE SAVEPOINT f2;
                SELECT test::TransactionTest.name;

            ROLLBACK;
            SELECT test::TransactionTest.name;
        """, [
            [],
            None,  # transaction start
            [1],  # insert
            [1],  # insert
            {'q1', 'q2'},

            None,  # transaction start
            [1],  # insert
            {'q1', 'q2', 'w1'},
            None,  # transaction rollback
            {'q1', 'q2'},

            None,  # transaction start
            [1],  # insert
            {'q1', 'q2', 'e1'},
            None,  # transaction commit
            {'q1', 'q2', 'e1'},

            None,  # transaction rollback
            [],
        ])

    async def test_transaction_interface_errors(self):
        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    'cannot start; .* already started'):
            async with tr:
                await tr.start()

        self.assertTrue(repr(tr).startswith(
            '<edgedb.transaction.Transaction state:rolledback'))

        self.assertIsNone(self.con._top_xact)

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    'cannot start; .* already rolled back'):
            async with tr:
                pass

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    'cannot manually commit.*async with'):
            async with tr:
                await tr.commit()

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    'cannot manually rollback.*async with'):
            async with tr:
                await tr.rollback()

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    'cannot enter context:.*async with'):
            async with tr:
                async with tr:
                    pass
