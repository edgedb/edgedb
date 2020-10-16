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


import os.path

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLVolatility(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'volatility.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'volatility_setup.edgeql')

    async def test_edgeql_volatility_function_01(self):
        result = await self.con.query(
            r"""
                WITH MODULE test
                SELECT Obj {
                    # immutable function should only be called once,
                    # generating the same value for all Objs
                    x := vol_immutable()
                };
            """
        )

        self.assertEqual(
            len(set(res.x for res in result)), 1,
            'more than one value for the same vol_immutable() call'
        )

    async def test_edgeql_volatility_function_02(self):
        result = await self.con.query(
            r"""
                WITH MODULE test
                SELECT Obj {
                    # stable function should only be called once,
                    # generating the same value for all Objs
                    x := vol_stable()
                };
            """
        )

        self.assertEqual(
            len(set(res.x for res in result)), 1,
            'more than one value for the same vol_stable() call'
        )

    async def test_edgeql_volatility_function_03(self):
        result = await self.con.query(
            r"""
                WITH MODULE test
                SELECT Obj {
                    # volatile function should be called once for each
                    # Obj, generating different values
                    x := vol_volatile()
                };
            """
        )

        self.assertNotEqual(
            len(set(res.x for res in result)), 1,
            'only one value for multiple vol_volatile() calls'
        )

    async def test_edgeql_volatility_function_04(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable function call can be
                    # factored out and called once per query
                    x := 1 IF Obj.n > 0 ELSE err_immutable()
                };
            ''')

    async def test_edgeql_volatility_function_05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Obj {
                # this condition is true for all of the Objs and the
                # stable function call cannot be factored out
                x := 1 IF Obj.n > 0 ELSE err_stable()
            };
        ''', [
            {'x': 1},
            {'x': 1},
            {'x': 1},
        ])

    async def test_edgeql_volatility_function_06(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Obj {
                # this condition is true for all of the Objs and the
                # volatile function call cannot be factored out
                x := 1 IF Obj.n > 0 ELSE err_volatile()
            };
        ''', [
            {'x': 1},
            {'x': 1},
            {'x': 1},
        ])

    async def test_edgeql_volatility_operator_01(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable operation can be factored out
                    # and called once per query
                    x := 1 IF Obj.n > 0 ELSE (1/0)
                };
            ''')

    async def test_edgeql_volatility_cast_01(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable cast can be factored out
                    # and called once per query
                    x := 1 IF Obj.n > 0 ELSE (<int64>(<float64>1)/0)
                };
            ''')

    async def test_edgeql_volatility_cast_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Obj {
                # this condition is true for all of the Objs and the
                # stable cast (<json>) cannot be factored out
                x := 1 IF Obj.n > 0 ELSE (<int64>(<json>1)/0)
            };
        ''', [
            {'x': 1},
            {'x': 1},
            {'x': 1},
        ])

    async def test_edgeql_volatility_for_01(self):
        await self.assert_query_result(
            r'''
                SELECT count(DISTINCT (FOR x in {1,2} UNION (
                    uuid_generate_v1mc())));
            ''',
            [2],
        )

    async def test_edgeql_volatility_for_02(self):
        await self.assert_query_result(
            r'''
                WITH X := (FOR x in {1,2} UNION (uuid_generate_v1mc(), x))
                SELECT count(DISTINCT X.0);
            ''',
            [2],
        )

    async def test_edgeql_volatility_for_03(self):
        await self.assert_query_result(
            r'''
                WITH X := (FOR y in {1, 2} UNION (
                               FOR x in {1,2} UNION (uuid_generate_v1mc(), x)))
                SELECT count(DISTINCT X.0);
            ''',
            [4],
        )

    async def test_edgeql_volatility_for_04(self):
        await self.assert_query_result(
            r'''
                WITH X := (FOR y in {1, 2} UNION (
                               (0,
                                (FOR x in {1,2} UNION (
                                     uuid_generate_v1mc(), x)))))
                SELECT count(DISTINCT X.1.0);
            ''',
            [4],
        )

    async def test_edgeql_volatility_for_05(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test,
                     X := (FOR y in {1, 2} UNION (
                               (uuid_generate_v1mc(),
                                (INSERT Obj { n := y }))))
                SELECT count(DISTINCT X.0);
            ''',
            [2],
        )

    async def test_edgeql_volatility_for_06(self):
        await self.assert_query_result(
            r'''
                SELECT count(DISTINCT (FOR x in {1,1} UNION (
                    uuid_generate_v1mc())));
            ''',
            [2],
        )

    async def test_edgeql_volatility_for_07(self):
        await self.assert_query_result(
            r'''
                SELECT count(DISTINCT (FOR x in {(),()} UNION (
                    uuid_generate_v1mc())));
            ''',
            [2],
        )

    async def test_edgeql_volatility_for_08(self):
        await self.assert_query_result(
            r'''
                SELECT count(DISTINCT (FOR x in {({1,2}, 0).1} UNION (
                    uuid_generate_v1mc())));
            ''',
            [2],
        )

    async def test_edgeql_volatility_for_09(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT count(
                    DISTINCT (FOR x in {(Obj { x := random() }).x} UNION (
                        uuid_generate_v1mc())));
            ''',
            [3],
        )

    async def test_edgeql_volatility_for_10(self):
        # We would eventually like to compute this correctly instead
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "volatile aliased expressions may not be used "
                    "inside FOR bodies"):
                await self.con.execute(
                    r'''
                    WITH x := random() FOR y in {1,2,3} UNION (x);
                    ''',
                )

    async def test_edgeql_volatility_select_clause_01a(self):
        # Spurious failure probability: 1/100!

        # We need a nested SELECT because of bug #1816
        # loses the ORDER BY otherwise
        await self.assert_query_result(
            r'''
                WITH X := enumerate((SELECT _gen_series(0,99)
                                     ORDER BY random()))
                SELECT all(X.0 = X.1);
            ''',
            [False],
        )

    async def test_edgeql_volatility_select_clause_01(self):
        # Spurious failure probability: 1/100!

        # We need a nested SELECT because of bug #1816
        # loses the ORDER BY otherwise
        await self.assert_query_result(
            r'''
                WITH X := enumerate((SELECT _gen_series(0,99)
                                     ORDER BY random()))
                SELECT all((FOR x in {X} UNION (x.0 = x.1)))
            ''',
            [False],
        )

    async def test_edgeql_volatility_select_clause_02(self):
        # Spurious failure probability: 1/2^99
        await self.assert_query_result(
            r'''
                SELECT count((SELECT _gen_series(0,99) FILTER random() > 0.5))
                       NOT IN {0, 100};
            ''',
            [True],
        )

    @test.xfail('triggers issue #1818')
    async def test_edgeql_volatility_select_clause_03(self):
        # Spurious failure probability: 1/2^100 I think

        # We want to test that the two SELECTs do separate FILTERs
        # This is written in an awful way because of a bug with WITH.
        await self.assert_query_result(
            r'''
                FOR X in {
                    array_agg(
                       (FOR x in {0, 1} UNION (SELECT _gen_series(0,100)
                        FILTER random() > 0.5)))}
                UNION (
                SELECT count(array_unpack(X))
                         != 2*count(DISTINCT array_unpack(X)));
            ''',
            [True],
        )

    async def test_edgeql_volatility_select_clause_04(self):
        # Spurious failure probability: 1/2^100 I think

        # This is just the test above but manually...

        result = await self.con.query(
            r'''
                FOR x in {0, 1} UNION (
                    SELECT _gen_series(0,100) FILTER random() > 0.5
                )
            ''',
        )

        self.assertNotEqual(
            2 * len(set(result)), len(result),
            'SELECT in FOR loop not doing independent filters'
        )

    async def test_edgeql_volatility_select_clause_05(self):
        # Spurious failure probability: 1/2^99
        await self.assert_query_result(
            r'''
                WITH X := (FOR x in {_gen_series(0,99)} UNION (()))
                SELECT count((SELECT X FILTER random() > 0.5))
                       NOT IN {0, 100};
            ''',
            [True],
        )

    async def test_edgeql_volatility_select_clause_06(self):
        # Spurious failure probability: 1/2^99
        await self.assert_query_result(
            r'''
                WITH X := (_gen_series(0,99), 0).1
                SELECT count((SELECT X FILTER random() > 0.5))
                       NOT IN {0, 100};
            ''',
            [True],
        )

    async def test_edgeql_volatility_with_01(self):
        # We would eventually like to compute this correctly instead
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "volatile aliased expressions may not be used "
                    "in multiple subqueries"):
                await self.con.execute(
                    r'''
                    WITH X := random() SELECT sum(X) = sum(X);
                    ''',
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "volatile aliased expressions may not be used "
                    "in multiple subqueries"):
                await self.con.execute(
                    r'''
                    WITH X := random(), Y := X SELECT sum(Y) = sum(Y);
                    ''',
                )

    async def test_edgeql_volatility_update_clause_01(self):
        # Spurious failure probability: 1/2^99
        await self.con.execute(r'''
            WITH MODULE test
            FOR x in {_gen_series(4,100)} UNION (
            INSERT Obj { n := x })
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT count(Obj)
            ''',
            [100],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test,
                     X := (UPDATE Obj FILTER random() > 0.5
                           SET { n := -1 })
                SELECT count(X) NOT IN {0, 100}
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test,
                     X := (SELECT Obj FILTER .n < 0)
                SELECT count(X) != 0 AND count(X) != 100
            ''',
            [True],
        )

    async def test_edgeql_volatility_delete_clause_01(self):
        # Spurious failure probability: 1/2^99
        await self.con.execute(r'''
            WITH MODULE test
            FOR x in {_gen_series(4,100)} UNION (
            INSERT Obj { n := x })
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test,
                     X := (DELETE Obj FILTER random() > 0.5)
                SELECT count(X) NOT IN {0, 100}
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT count(Obj) != 0 AND count(Obj) != 100
            ''',
            [True],
        )

    async def test_edgeql_volatility_errors_01(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=42):
                await self.con.execute(
                    r"""
                    SELECT test::Obj.n + random()
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=42):
                await self.con.execute(
                    r"""
                    SELECT (test::Obj.n, random())
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation"):
                await self.con.execute(
                    r"""
                    SELECT ({1,2}, random())
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=28):
                await self.con.execute(
                    r"""
                    SELECT random() + test::Obj.n
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=36):
                await self.con.execute(
                    r"""
                    SELECT {1,2} + (FOR x in {1,2,3} UNION (x*random()))
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=36):
                await self.con.execute(
                    r"""
                    SELECT ({1,2}, (INSERT test::Obj { n := 100 }))
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=64):
                await self.con.execute(
                    r"""
                    SELECT ({1,2},
                            (FOR i in {1,2,3} UNION (
                                 INSERT test::Obj { n := i })))
                    """
                )
