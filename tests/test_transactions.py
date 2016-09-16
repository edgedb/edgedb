##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import _testbase as tb
from edgedb.client import exceptions


class TestTransactions(tb.QueryTestCase):
    SETUP = """
        CREATE LINK test::name {
            SET mapping := '11';
            SET readonly := False;
        };

        CREATE CONCEPT test::TransactionTest INHERITING std::Object {
            CREATE LINK test::name TO std::str {
                SET mapping := '11';
                SET readonly := False;
            };
        };
    """

    async def test_transaction_regular(self):
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

                await self.con.execute('''
                    INSERT test::TransactionTest {
                        test::name := 'Test Transaction'
                    };
                ''')

                1 / 0

        self.assertIsNone(self.con._top_xact)

        result = await self.con.execute('''
            SELECT
                test::Object
            WHERE
                test::Object.name = 'Test Transaction';
        ''')

        self.assertEqual(result[0], [])

    async def test_transaction_nested(self):
        self.assertIsNone(self.con._top_xact)
        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)

        with self.assertRaises(ZeroDivisionError):
            async with tr:
                self.assertIs(self.con._top_xact, tr)

                async with self.con.transaction():
                    self.assertIs(self.con._top_xact, tr)

                    await self.con.execute('''
                        INSERT test::TransactionTest {
                            test::name := 'TXTEST 1'
                        };
                    ''')

                self.assertIs(self.con._top_xact, tr)

                with self.assertRaises(ZeroDivisionError):
                    in_tr = self.con.transaction()
                    async with in_tr:

                        self.assertIs(self.con._top_xact, tr)

                        await self.con.execute('''
                            INSERT test::TransactionTest {
                                test::name := 'TXTEST 2'
                            };
                        ''')

                        1 / 0

                result = await self.con.execute('''
                    SELECT
                        test::Object {
                            name
                        }
                    WHERE
                        test::Object.name LIKE 'TXTEST%';
                ''')

                recs = result[0]

                self.assertEqual(len(recs), 1)
                self.assertEqual(recs[0]['name'], 'TXTEST 1')
                self.assertIs(self.con._top_xact, tr)

                1 / 0

        self.assertIs(self.con._top_xact, None)

        result = await self.con.execute('''
            SELECT
                test::Object {
                    name
                }
            WHERE
                test::Object.name LIKE 'TXTEST%';
        ''')

        recs = result[0]
        self.assertEqual(len(recs), 0)

    async def test_transaction_interface_errors(self):
        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(exceptions.InterfaceError,
                                    'cannot start; .* already started'):
            async with tr:
                await tr.start()

        self.assertTrue(repr(tr).startswith(
            '<edgedb.Transaction state:rolledback'))

        self.assertIsNone(self.con._top_xact)

        with self.assertRaisesRegex(exceptions.InterfaceError,
                                    'cannot start; .* already rolled back'):
            async with tr:
                pass

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(exceptions.InterfaceError,
                                    'cannot manually commit.*async with'):
            async with tr:
                await tr.commit()

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(exceptions.InterfaceError,
                                    'cannot manually rollback.*async with'):
            async with tr:
                await tr.rollback()

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(exceptions.InterfaceError,
                                    'cannot enter context:.*async with'):
            async with tr:
                async with tr:
                    pass
