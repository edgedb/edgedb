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


import json
import os.path

import edgedb

from edb.testbase import server as tb
from edb.testbase import serutils
from edb.tools import test


class TestEdgeQLVolatility(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'volatility.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'volatility_setup.edgeql')

    def _check_crossproduct(self, res):
        ns = set()
        pairs = set()
        for row in res:
            ns.add(row[0])
            pairs.add((row[0], row[1]))

        self.assertEqual(
            pairs,
            {(n1, n2) for n1 in ns for n2 in ns},
        )

    def test_loop(self, n=None, *, single=False):
        async def json_query(*args, **kwargs):
            q = self.con.query_single_json if single else self.con.query_json
            res = await q(*args, **kwargs)
            return json.loads(res)

        async def native_query(*args, **kwargs):
            q = self.con.query_single if single else self.con.query
            res = await q(*args, **kwargs)
            return serutils.serialize(res)

        async def native_query_typenames(*args, **kwargs):
            res = await self.con._fetchall(*args, **kwargs, __typenames__=True)
            if single:
                assert len(res) == 1
                res = res[0]
            return serutils.serialize(res)

        qs = [json_query, native_query, native_query_typenames]
        if n is None:
            n = len(qs)
        for i in range(n):
            yield qs[i % len(qs)]

    async def test_edgeql_volatility_function_01(self):
        result = await self.con.query(
            r"""
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

    async def test_edgeql_volatility_function_03a(self):
        result = await self.con.query(
            r"""
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

    async def test_edgeql_volatility_function_03b(self):
        result = await self.con.query(
            r"""
                SELECT Obj {
                    # volatile function should be called once for each
                    # Obj, generating different values
                    x := (vol_volatile(),)
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
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable function call can be
                    # factored out and called once per query
                    x := 1 IF Obj.n > 0 ELSE err_immutable()
                };
            ''')

    async def test_edgeql_volatility_function_05(self):
        await self.assert_query_result(r'''
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
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable cast can be factored out
                    # and called once per query
                    x := 1 IF Obj.n > 0 ELSE (<int64>(<float64>1)/0)
                };
            ''')

    async def test_edgeql_volatility_cast_02(self):
        await self.assert_query_result(r'''
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
                WITH X := (FOR y in {1, 2} UNION (
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
                SELECT count(
                    DISTINCT (FOR x in {(Obj { x := random() }).x} UNION (
                        uuid_generate_v1mc())));
            ''',
            [3],
        )

    async def test_edgeql_volatility_for_10(self):
        res = await self.con.query(
            r'''
            WITH x := random() FOR y in {1,2,3} UNION (x);
            ''',
        )
        self.assertEqual(len(set(res)), 1)

    async def test_edgeql_volatility_for_11(self):
        await self.assert_query_result(
            r'''
                WITH X := ((FOR x in {(Obj { x := random() })} UNION (x.x))),
                SELECT count(DISTINCT X)
            ''',
            [3],
        )

        await self.assert_query_result(
            r'''
                WITH X := ((FOR x in {(Obj { x := random() })} UNION (x.x))),
                SELECT count(X)
            ''',
            [3],
        )

    async def test_edgeql_volatility_for_12(self):
        await self.assert_query_result(
            r'''
                WITH X := ((FOR x in {(Obj { x := random() }).x} UNION (x))),
                SELECT count(DISTINCT X)
            ''',
            [3],
        )

        await self.assert_query_result(
            r'''
                WITH X := ((FOR x in {(Obj { x := random() }).x} UNION (x))),
                SELECT count(X)
            ''',
            [3],
        )

    async def test_edgeql_volatility_with_and_use_01(self):
        await self.assert_query_result(
            r'''
                WITH X := (Obj { x := random() }).x,
                SELECT count(DISTINCT X);
            ''',
            [3],
        )

        await self.assert_query_result(
            r'''
                WITH X := (Obj { x := random() }).x,
                SELECT count(X);
            ''',
            [3],
        )

    async def test_edgeql_volatility_with_and_use_02(self):
        await self.assert_query_result(
            r'''
                WITH X := (SELECT Obj { x := random() }).x,
                SELECT count(DISTINCT X);
            ''',
            [3],
        )

        await self.assert_query_result(
            r'''
                WITH X := (SELECT Obj { x := random() }).x,
                SELECT count(X);
            ''',
            [3],
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
        await self.assert_query_result(
            r'''
                WITH X := random() SELECT sum(X) = sum(X);
            ''',
            [True],
        )

    async def test_edgeql_volatility_with_02(self):
        await self.assert_query_result(
            r'''
                WITH X := random(), Y := X SELECT sum(Y) = sum(Y)
            ''',
            [True],
        )

    async def test_edgeql_volatility_with_03(self):
        await self.assert_query_result(
            r'''
                WITH W := random(),
                     Z := W,
                SELECT W = Z;
            ''',
            [True],
        )

    async def test_edgeql_volatility_with_04(self):
        await self.assert_query_result(
            r'''
                WITH W := {random(), random()},
                     Z := W+0,
                SELECT _ := (W = Z) ORDER BY _;
            ''',
            [False, False, True, True],
        )

    async def test_edgeql_volatility_with_05(self):
        await self.con.execute(r'''
            CREATE TYPE Foo { CREATE PROPERTY asdf -> tuple<float64> };
        ''')

        await self.con.query(r'''
            WITH X := (random(),) SELECT X.0;
        ''')

        await self.con.query(r'''
            WITH X := {(random(),),(random(),)} SELECT X.0;
        ''')

    async def test_edgeql_volatility_update_clause_01(self):
        # Spurious failure probability: 1/2^99
        await self.con.execute(r'''
            FOR x in {_gen_series(4,100)} UNION (
            INSERT Obj { n := x })
        ''')

        await self.assert_query_result(
            r'''
                SELECT count(Obj)
            ''',
            [100],
        )

        await self.assert_query_result(
            r'''
                WITH X := (UPDATE Obj FILTER random() > 0.5
                           SET { n := -1 })
                SELECT count(X) NOT IN {0, 100}
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH X := (SELECT Obj FILTER .n < 0)
                SELECT count(X) != 0 AND count(X) != 100
            ''',
            [True],
        )

    async def test_edgeql_volatility_delete_clause_01(self):
        # Spurious failure probability: 1/2^99
        await self.con.execute(r'''
            FOR x in {_gen_series(4,100)} UNION (
            INSERT Obj { n := x })
        ''')

        await self.assert_query_result(
            r'''
                WITH X := (DELETE Obj FILTER random() > 0.5)
                SELECT count(X) NOT IN {0, 100}
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT count(Obj) != 0 AND count(Obj) != 100
            ''',
            [True],
        )

    async def test_edgeql_volatility_select_with_objects_01(self):
        for query in self.test_loop(10):
            res = await query("""
                WITH W := (SELECT Obj FILTER random() > 0.5),
                SELECT ((SELECT W {n}), (SELECT W {n}))
            """)

            self._check_crossproduct(
                [(row[0]['n'], row[1]['n']) for row in res])

    async def test_edgeql_volatility_select_with_objects_02(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT Obj {n, m := random()}
                FILTER .m > 0.3 ORDER BY .m;
            """)

            for row in res:
                self.assertGreater(row['m'], 0.3)
            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums))

    async def test_edgeql_volatility_select_with_objects_03(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT {
                    o := (
                        SELECT Obj {n, m := random()}
                        FILTER .m  > 0.3 ORDER BY .m
                    )
                };
            """)

            res = res[0]['o']

            for row in res:
                self.assertGreater(row['m'], 0.3)
            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums))

    async def test_edgeql_volatility_select_with_objects_04(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT {
                    o := (SELECT (
                        SELECT Obj {n, m := random()}
                        FILTER .m  > 0.3 ORDER BY .m
                    ))
                }
            """)

            res = res[0]['o']

            for row in res:
                self.assertGreater(row['m'], 0.3)
            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums))

    async def test_edgeql_volatility_select_with_objects_05(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT {
                    o := (SELECT (
                        SELECT Obj {n, m := random()}
                        FILTER .m  > 0.3
                    ) ORDER BY .m)
                }
            """)

            res = res[0]['o']

            for row in res:
                self.assertGreater(row['m'], 0.3)
            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums))

    async def test_edgeql_volatility_select_with_objects_06(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT (
                    SELECT Obj {n, m := random()}
                ) FILTER .m > 0.3 ORDER BY .m
            """)

            for row in res:
                self.assertGreater(row['m'], 0.3)
            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums))

    async def test_edgeql_volatility_select_with_objects_07(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT (
                    SELECT Obj {n, m := {random(), random()}}
                ) ORDER BY max(.m)
            """)

            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums, key=max))

    async def test_edgeql_volatility_select_with_objects_08(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT (
                    SELECT Obj {n, m := (random(), random())}
                ) ORDER BY max(.m)
            """)

            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums))

    async def test_edgeql_volatility_select_with_objects_09(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT (
                    SELECT Obj {n, m := [random(), random()]}
                ) ORDER BY max(.m)
            """)

            nums = [row['m'] for row in res]
            self.assertEqual(nums, sorted(nums))

    async def test_edgeql_volatility_select_with_objects_10(self):
        for query in self.test_loop():
            res = await query("""
                WITH X := (Obj { m := random()},)
                SELECT X.0;
            """)

            self.assertEqual(len(res), 3)

    async def test_edgeql_volatility_select_objects_optional_01(self):
        for _ in range(10):
            await self.assert_query_result(
                r'''
                WITH X := (SELECT Obj {
                    m := (SELECT .n FILTER random() > 0.5) }),
                SELECT count(X);
                ''',
                [3],
            )

    async def test_edgeql_volatility_select_objects_optional_02(self):
        for query in self.test_loop(10, single=True):
            res = await query("""
                WITH X := (SELECT Obj {
                    m := (SELECT .n FILTER random() > 0.5) }),
                SELECT {
                    foo := (SELECT X {n, m}),
                    baz := (SELECT X.m),
                };
            """)

            foos = [x['m'] for x in res['foo'] if x['m'] is not None]
            self.assertEqual(set(foos), set(res['baz']))

    async def test_edgeql_volatility_select_hard_objects_01a(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj {m := next()}),
                SELECT (O.m, O.m);
            """)

            self.assertEqual(len(res), 3)
            for row in res:
                self.assertEqual(row[0], row[1])

            # Make sure it is really volatile
            self.assertNotEqual(res[0][0], res[1][0])

    async def test_edgeql_volatility_select_hard_objects_01b(self):
        for query in self.test_loop():
            # one side in a subquery, one not
            res = await query("""
                WITH O := (SELECT Obj {m := next()}),
                SELECT ((SELECT O.m), O.m);
            """)

            self.assertEqual(len(res), 3)
            for row in res:
                self.assertEqual(row[0], row[1])

    async def test_edgeql_volatility_select_hard_objects_02a(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj {m := next()}),
                SELECT ((SELECT O.m), (SELECT O.m));
            """)

            self.assertEqual(len(res), 9)
            self._check_crossproduct(res)

    async def test_edgeql_volatility_select_hard_objects_02b(self):
        for query in self.test_loop(10):
            res = await query("""
                WITH O := (SELECT Obj {m := random()} FILTER .m > 0.3),
                SELECT ((SELECT O.m), (SELECT O.m));
            """)

            for row in res:
                self.assertGreater(row[0], 0.3)
            self._check_crossproduct(res)

    async def test_edgeql_volatility_select_hard_objects_03(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj {m := next()}),
                SELECT (O {m}, O {m});
            """)

            self.assertEqual(len(res), 3)
            for row in res:
                self.assertEqual(row[0]['m'], row[1]['m'])

    async def test_edgeql_volatility_select_hard_objects_04a(self):
        # TODO: this, but wrapped in DISTINCT
        # (which breaks the serialization, ugh!)
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj {m := next()}),
                SELECT ((SELECT O {m}), (SELECT O {m}));
            """)

            self._check_crossproduct(
                [(row[0]['m'], row[1]['m']) for row in res])

    async def test_edgeql_volatility_select_hard_objects_04b(self):
        # TODO: this, but wrapped in DISTINCT
        # (which breaks the serialization, ugh!)
        for query in self.test_loop(10):
            res = await query("""
                WITH O := (SELECT Obj {m := random()} FILTER .m > 0.3),
                SELECT ((SELECT O {m}), (SELECT O {m}));
            """)

            for row in res:
                self.assertGreater(row[0]['m'], 0.3)
            self._check_crossproduct(
                [(row[0]['m'], row[1]['m']) for row in res])

    async def test_edgeql_volatility_select_hard_objects_05a(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT {m := next()} LIMIT 1),
                SELECT (O {m}, O {m});
            """)

            self.assertEqual(len(res), 1)
            for row in res:
                self.assertEqual(row[0]['m'], row[1]['m'])

    async def test_edgeql_volatility_select_hard_objects_05b(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT {m := next()} LIMIT 1),
                SELECT assert_exists((O {m}, O {m}));
            """)

            self.assertEqual(len(res), 1)
            for row in res:
                self.assertEqual(row[0]['m'], row[1]['m'])

    async def test_edgeql_volatility_select_hard_objects_06(self):
        # now let's try it with a multi prop
        res = await self.con.query("""
            WITH O := (SELECT Obj {m := {next(), next()} })
            SELECT ((SELECT O {m}), (SELECT O {m}));
        """)

        self._check_crossproduct([(row[0].m, row[1].m) for row in res])

    async def test_edgeql_volatility_select_hard_objects_07(self):
        # now let's try it with a multi prop
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj {m := {next(), next()} })
                SELECT ((O {m}), (O {m}));
            """)

            self.assertEqual(len(res), 3)
            for row in res:
                self.assertEqual(row[0]['m'], row[1]['m'])

    async def test_edgeql_volatility_select_hard_objects_08a(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (SELECT Obj {m := next()}),
                SELECT {
                    foo := (SELECT O {n, m}),
                    bar := (SELECT O {n, m}),
                };
            """)

            self.assertEqual(
                {(x['n'], x['m']) for x in res['foo']},
                {(x['n'], x['m']) for x in res['bar']},
            )
            self.assertEqual(len(res['foo']), 3)

    async def test_edgeql_volatility_select_hard_objects_08b(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (SELECT Obj {m := next()} LIMIT 1),
                SELECT {
                    foo := (SELECT O {n, m}),
                    bar := (SELECT O {n, m}),
                };
            """)

            self.assertEqual(res['foo']['n'], res['bar']['n'])
            self.assertEqual(res['foo']['m'], res['bar']['m'])

    async def test_edgeql_volatility_select_hard_objects_09(self):
        await self.assert_query_result(r'''
            WITH O := (SELECT Obj {m := next()}),
            SELECT {
                foo := (SELECT O),
                bar := (SELECT O),
            };
        ''', [
            {
                'foo': [{"id": str}, {"id": str}, {"id": str}],
                'bar': [{"id": str}, {"id": str}, {"id": str}],
            }
        ])

    async def test_edgeql_volatility_select_nested_01a(self):
        for query in self.test_loop(10, single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         m := next(),
                         friends := (SELECT Tgt FILTER random() > 0.4)
                     }),
                SELECT {
                    a := (SELECT O {m, friends: {n}} ORDER BY .m),
                    b := (SELECT O {m, friends: {n}} ORDER BY .m),
                };
            """)

            nums = [row['m'] for row in res['a']]
            self.assertEqual(nums, sorted(nums))
            self.assertEqual(len(res['a']), 3)
            for ra, rb in zip(res['a'], res['b']):
                self.assertEqual(ra['m'], rb['m'])
                self.assertEqual(
                    {x['n'] for x in ra['friends']},
                    {x['n'] for x in rb['friends']},
                )

    async def test_edgeql_volatility_select_nested_1b(self):
        # same as 1b but without a shape on friends
        for query in self.test_loop(10, single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         m := next(),
                         friends := (SELECT Tgt FILTER random() > 0.4)
                     }),
                SELECT {
                    a := (SELECT O {m, friends} ORDER BY .m),
                    b := (SELECT O {m, friends} ORDER BY .m),
                };
            """)

            nums = [row['m'] for row in res['a']]
            self.assertEqual(nums, sorted(nums))
            self.assertEqual(len(res['a']), 3)
            for ra, rb in zip(res['a'], res['b']):
                self.assertEqual(ra['m'], rb['m'])
                self.assertEqual(
                    {x['id'] for x in ra['friends']},
                    {x['id'] for x in rb['friends']},
                )
                self.assertLessEqual(len(ra['friends']), 4)

    async def test_edgeql_volatility_select_nested_02(self):
        for query in self.test_loop(10, single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         m := next(),
                         friends := (SELECT .tgt FILTER random() > 0.4)
                     }),
                SELECT {
                    a := (SELECT O {m, friends: {n}} ORDER BY .m),
                    b := (SELECT O {m, friend_nums := .friends.n} ORDER BY .m),
                };
            """)

            nums = [row['m'] for row in res['a']]
            self.assertEqual(nums, sorted(nums))
            self.assertEqual(len(res['a']), 3)
            for ra, rb in zip(res['a'], res['b']):
                self.assertEqual(ra['m'], rb['m'])
                self.assertEqual(
                    {x['n'] for x in ra['friends']},
                    set(rb['friend_nums']),
                )

    async def test_edgeql_volatility_select_nested_03a(self):
        for query in self.test_loop(10, single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         m := next(),
                         friends := (SELECT .tgt { x := random() })
                     }),
                SELECT {
                    a := (SELECT O {m, friends: {x}} ORDER BY .m),
                    b := (SELECT O {m, friend_nums := .friends.x} ORDER BY .m),
                };
            """)

            nums = [row['m'] for row in res['a']]
            self.assertEqual(nums, sorted(nums))
            self.assertEqual(len(res['a']), 3)
            for ra, rb in zip(res['a'], res['b']):
                self.assertEqual(ra['m'], rb['m'])
                self.assertEqual(
                    {x['x'] for x in ra['friends']},
                    set(rb['friend_nums']),
                )

    async def test_edgeql_volatility_select_nested_03b(self):
        for query in self.test_loop(10, single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         m := next(),
                         friends := (SELECT (SELECT .tgt) { @x := next() })
                     }),
                SELECT {
                    a := (SELECT O {m, friends: {@x}} ORDER BY .m),
                    b := (SELECT O {m, friend_nums := .friends@x} ORDER BY .m),
                };
            """)

            nums = [row['m'] for row in res['a']]
            self.assertEqual(nums, sorted(nums))
            self.assertEqual(len(res['a']), 3)
            for ra, rb in zip(res['a'], res['b']):
                self.assertEqual(ra['m'], rb['m'])
                self.assertEqual(
                    {x['@x'] for x in ra['friends']},
                    set(rb['friend_nums']),
                )

    async def test_edgeql_volatility_select_nested_04a(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         friends := (SELECT Tgt { x := next() } )
                     }),
                SELECT {
                    a := (SELECT O {friends: {n, x}}),
                    b := (SELECT O {friends: {n, x}}),
                };
            """)

            self.assertEqual(len(res['a']), 3)
            for ra, rb in zip(res['a'], res['b']):
                self.assertEqual(len(ra['friends']), 4)
                self.assertEqual(
                    sorted((x['n'], x['x']) for x in ra['friends']),
                    sorted((x['n'], x['x']) for x in rb['friends']),
                )

    async def test_edgeql_volatility_select_nested_04b(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         tgt: { x := next() }
                     }),
                SELECT {
                    a := (SELECT O {tgt: {n, x}}),
                    b := (SELECT O {tgt: {n, x}}),
                };
            """)

            self.assertEqual(len(res['a']), 3)
            for ra, rb in zip(res['a'], res['b']):
                self.assertEqual(len(ra['tgt']), 2)
                self.assertEqual(
                    sorted((x['n'], x['x']) for x in ra['tgt']),
                    sorted((x['n'], x['x']) for x in rb['tgt']),
                )

    async def test_edgeql_volatility_select_nested_05(self):
        for query in self.test_loop(10, single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         m := rand_int(100),
                         friends := (SELECT Tgt { x := next() }
                                     FILTER random() > 0.4)
                     }),
                SELECT {
                    a := (SELECT O {m, n, friends: {n, x}, ha := .friends.x}),
                    b := (SELECT O {
                              m,
                              friends_tuples := (.friends.n, .friends.x),
                              friend_sums := sum(.friends.x),
                          }),
                    c := (O.n, O.friends {n, x}, O.friends {n, x}),
                };
            """)

            cs = {x['n']: [] for x in res['a']}
            for rc in res['c']:
                self.assertEqual(rc[1]['n'], rc[2]['n'])
                self.assertEqual(rc[1]['x'], rc[2]['x'])
                cs[rc[0]].append([rc[1]['n'], rc[1]['x']])

            for ra, rb in zip(res['a'], res['b']):
                self.assertLessEqual(len(ra['friends']), 4)

                self.assertEqual(
                    sorted(x['x'] for x in ra['friends']),
                    sorted(ra['ha']),
                )

                self.assertEqual(
                    sorted([x['n'], x['x']] for x in ra['friends']),
                    sorted(rb['friends_tuples']),
                )

                self.assertEqual(
                    sorted(cs[ra['n']]),
                    sorted(rb['friends_tuples']),
                )

                self.assertEqual(sum(ra['ha']), rb['friend_sums'])

    async def test_edgeql_volatility_select_nested_06a(self):
        # here we want some deduplicating to happen
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         friends := (SELECT Tgt { x := next() })
                     }),
                SELECT {
                    x := (O { friends: {x} }),
                    y := O.friends.x,
                };
            """)

            self.assertEqual(len(res['y']), 4)
            all_xs = {t['x'] for r in res['x'] for t in r['friends']}
            self.assertTrue(set(res['y']).issubset(all_xs))

    async def test_edgeql_volatility_select_nested_06b(self):
        # here we want some deduplicating to happen
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (SELECT Obj {
                         friends := (SELECT Tgt { x := next() })
                     }),
                SELECT {
                    x := (O { friends: {n, x} }),
                    y := O.friends {n, x},
                };
            """)

            self.assertEqual(len(res['y']), 4)
            all_xs = {(t['n'], t['x']) for r in res['x'] for t in r['friends']}
            y = {(t['n'], t['x']) for t in res['y']}
            self.assertTrue(y.issubset(all_xs))

    async def test_edgeql_volatility_select_nested_06c(self):
        # here we want some deduplicating to happen
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj {
                         friends := (SELECT Tgt { x := next() })
                     }),
                SELECT ((SELECT O.friends.x), (SELECT O.friends.x));
            """)

            self.assertEqual(len(res), 16)

    async def test_edgeql_volatility_select_nested_06d(self):
        # here we want some deduplicating to happen
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj {
                         friends := (SELECT Tgt { x := next() })
                     }),
                SELECT O.friends;
            """)

            self.assertEqual(len(res), 4)

    async def test_edgeql_volatility_select_nested_06e(self):
        # here we want some deduplicating to happen
        # same as above but with an extra select
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT (SELECT Obj {
                         friends := (SELECT Tgt { x := next() })
                     })),
                SELECT O.friends;
            """)

            self.assertEqual(len(res), 4)

    async def test_edgeql_volatility_select_nested_07a(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT Obj {
                    n,
                    tgt: {
                        n,
                    } FILTER random() < 0.5
                }
                FILTER EXISTS (.tgt);
            """)

            for row in res:
                self.assertGreater(len(row['tgt']), 0)

    async def test_edgeql_volatility_select_nested_07b(self):
        for query in self.test_loop(10):
            res = await query("""
                SELECT Obj {
                    n,
                    tgts := (SELECT .tgt {
                        n,
                    } FILTER random() < 0.5)
                }
                FILTER EXISTS (.tgts);
            """)

            for row in res:
                self.assertGreater(len(row['tgts']), 0)

    @test.xfail("Arrays containing objects are hard; TODO: fail?")
    async def test_edgeql_volatility_select_arrays_01(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := [(SELECT Obj {m := next()})],
                SELECT {
                    foo := (SELECT O[0] {m}),
                    bar := (SELECT O[0] {m}),
                };
            """)

            self.assertEqual(res['foo'], res['bar'])
            self.assertEqual(len(res['foo']), 3)

    async def test_edgeql_volatility_select_tuples_01(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := ((SELECT Obj {m := next()}),),
                SELECT {
                    foo := (SELECT O.0 {n, m}),
                    bar := (SELECT O.0 {n, m}),
                };
            """)

            self.assertEqual(res['foo'], res['bar'])
            self.assertEqual(len(res['foo']), 3)

    async def test_edgeql_volatility_select_tuples_02(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (z := ((SELECT Obj {m := next()}),)),
                SELECT {
                    foo := (SELECT O.z.0 {n, m}),
                    bar := (SELECT O.z.0 {n, m}),
                    os := O,
                    ms := O.z.0.m,
                };
            """)

            self.assertEqual(res['foo'], res['bar'])
            self.assertEqual(len(res['foo']), 3)
            self.assertEqual(
                {x['m'] for x in res['foo']},
                set(res['ms']),
            )

    async def test_edgeql_volatility_select_tuples_03(self):
        await self.assert_query_result(r'''
            WITH X := ((SELECT Obj { m := next() }),),
                 Y := ((SELECT Obj { m := next() }),),
            SELECT count((SELECT (X, Y) FILTER X = Y));
        ''', [
            3,
        ])

        await self.assert_query_result(r'''
            WITH X := ((SELECT Obj { m := next() }),),
                 Y := ((SELECT Obj { m := next() }),),
            SELECT count((SELECT (X, Y) FILTER X < Y));
        ''', [
            3,
        ])

        await self.assert_query_result(r'''
            WITH X := ((SELECT Obj { m := next() }),),
                 Y := (Obj,),
            SELECT count((SELECT (X, Y) FILTER X < Y));
        ''', [
            3,
        ])

    async def test_edgeql_volatility_insert_01(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH
                    Foo := (SELECT (
                        INSERT Obj {n := 10}
                    ) { m := next() })
                SELECT {
                    foo := Foo {n, m},
                    bar := Foo {n, m},
                };
            """)

            self.assertEqual(res['foo']['n'], 10)
            self.assertEqual(res['foo']['m'], res['bar']['m'])

    async def test_edgeql_volatility_nested_link_01(self):
        # next() should get called once for each Obj/Tgt pair
        for query in self.test_loop():
            res = await query(
                r"""
                    SELECT Obj {
                        l := (SELECT Tgt { m := next() }),
                    };
                """
            )

            nums = [t['m'] for o in res for t in o['l']]
            self.assertEqual(len(nums), len(set(nums)))

    async def test_edgeql_volatility_hack_01a(self):
        await self.assert_query_result(r'''
            SELECT (FOR x IN {1,2} UNION (SELECT Obj { m := x }))
            { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_01b(self):
        await self.assert_query_result(r'''
            SELECT (FOR x IN {1,2} UNION ((SELECT Obj) { m := x }))
            { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_01c(self):
        await self.assert_query_result(r'''
            SELECT (FOR x IN {1,2} UNION (Obj { m := x }))
            { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_02(self):
        await self.assert_query_result(r'''
            WITH X := (FOR x IN {1,2} UNION (SELECT Obj { m := x }))
            SELECT X { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_03a(self):
        await self.assert_query_result(r'''
            WITH X := (WITH x := {1,2}, SELECT (x, Obj {m := x})).1
            SELECT X { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_03b(self):
        await self.assert_query_result(r'''
            WITH X := (WITH x := {1,2}, SELECT (x, Obj {m := x}).1)
            SELECT X { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_04a(self):
        await self.assert_query_result(r'''
            SELECT (WITH x := {1,2}, SELECT (x, Obj {m := x})).1
            { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_04b(self):
        await self.assert_query_result(r'''
            SELECT (WITH x := {1,2}, SELECT (x, Obj {m := x}).1)
            { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_05a(self):
        await self.assert_query_result(r'''
            SELECT (WITH x := {(SELECT Tgt FILTER .n < 3)},
                    SELECT (x.n, Obj {m := x.n})).1
            { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_hack_05b(self):
        await self.assert_query_result(r'''
            WITH X :=  (WITH x := {(SELECT Tgt FILTER .n < 3)},
                        SELECT (x.n, Obj {m := x.n})).1,
            SELECT X { n, m } ORDER BY .m THEN .n;
        ''', [
            {"m": 1, "n": 1},
            {"m": 1, "n": 2},
            {"m": 1, "n": 3},
            {"m": 2, "n": 1},
            {"m": 2, "n": 2},
            {"m": 2, "n": 3},
        ])

    async def test_edgeql_volatility_for_like_hard_01(self):
        for query in self.test_loop():
            res = await query("""
                WITH
                    O := (SELECT Obj { x := next() }),
                    Z := (O, (SELECT O { n, x, y := -.x })).1
                SELECT Z { n, x, y };
            """)

            self.assertEqual(len(res), 3)
            self.assertNotEqual(res[0]['x'], res[1]['x'])
            for obj in res:
                self.assertEqual(obj['x'], -obj['y'])

    @test.xfail("column definition list is only allowed ...")
    async def test_edgeql_volatility_for_like_hard_02(self):
        # Weird stuff is happening here!
        # 1. Putting basically anything other than O as the 1st tuple el works
        # 2. If we reorder the arguments it works
        # 3. If we add a real shape to the nested O, it works
        for query in self.test_loop():
            res = await query("""
                WITH
                    O := (SELECT Obj { x := next() }),
                    Z := (O, ({ o := O })).1
                SELECT Z { o: {n, x} };
            """)

            self.assertEqual(len(res), 3)
            self.assertNotEqual(res[0]['o']['x'], res[1]['o']['x'])

    @test.xfail("column definition list is only allowed ...")
    async def test_edgeql_volatility_for_like_hard_03(self):
        for query in self.test_loop():
            res = await query("""
                WITH
                    O := (SELECT Obj { x := next() }),
                    Za := (O, ({ o := O })),
                    Z := Za.1
                SELECT Z { o: {n, x} };
            """)

            self.assertEqual(len(res), 3)
            self.assertNotEqual(res[0]['o']['x'], res[1]['o']['x'])

    async def test_edgeql_volatility_for_hard_01(self):
        for query in self.test_loop():
            res = await query("""
                WITH Z := (FOR O IN {(
                    SELECT Obj { x := next() }
                )} UNION (
                    SELECT O { y := -.x }
                )),
                SELECT Z { n, x, y };
            """)

            self.assertEqual(len(res), 3)
            self.assertNotEqual(res[0]['x'], res[1]['x'])
            for obj in res:
                self.assertEqual(obj['x'], -obj['y'])

    async def test_edgeql_volatility_for_hard_02(self):
        for query in self.test_loop():
            res = await query("""
                WITH Z := (FOR O IN {(
                    SELECT Obj { x := next() }
                )} UNION (
                    SELECT { a := O { n, x, y := -.x } }
                )),
                SELECT Z { a: { n, x, y }};
            """)

            self.assertEqual(len(res), 3)
            self.assertNotEqual(res[0]['a']['x'], res[1]['a']['x'])
            for obj in res:
                self.assertEqual(obj['a']['x'], -obj['a']['y'])

    async def test_edgeql_volatility_for_hard_03(self):
        for query in self.test_loop():
            res = await query("""
                WITH Z := (FOR O IN {(
                    SELECT Obj {
                        tgt: { x := next() }
                    }
                )} UNION (
                    SELECT O {tgt: {n, x, y := -.x}}
                )),
                SELECT Z { tgt: {n, x, y} };
            """)

            self.assertEqual(len(res), 3)
            for obj in res:
                for tgt in obj['tgt']:
                    self.assertEqual(tgt['x'], -tgt['y'])

    async def test_edgeql_volatility_for_hard_04(self):
        for query in self.test_loop():
            res = await query("""
                WITH Z := (FOR O IN {(
                    SELECT Obj {
                        tgt: { x := next() }
                    }
                )} UNION (
                    SELECT { a := (O {tgt: {n, x, y := -.x}}) }
                )),
                SELECT Z { a: {tgt: {n, x, y} } };
            """)

            self.assertEqual(len(res), 3)
            for obj in res:
                for tgt in obj['a']['tgt']:
                    self.assertEqual(tgt['x'], -tgt['y'])

    async def test_edgeql_volatility_for_hard_05(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH Z := Obj { m := next() },
                     Y := (FOR k in {Z} UNION (k.m)),
                SELECT { z := Z.m, y := Y };
            """)

            self.assertEqual(set(res['z']), set(res['y']))

    async def test_edgeql_volatility_rebind_flat_01(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj { x := next() }),
                     Z := (SELECT O {y := -.x}),
                SELECT Z { n, x, y };
            """)

            self.assertEqual(len(res), 3)
            for obj in res:
                self.assertEqual(obj['x'], -obj['y'])

    async def test_edgeql_volatility_rebind_flat_02(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj { x := next() }),
                     Z := (SELECT O {x, y := -.x}),
                SELECT Z { n, x, y };
            """)

            self.assertEqual(len(res), 3)
            for obj in res:
                self.assertEqual(obj['x'], -obj['y'])

    async def test_edgeql_volatility_rebind_flat_03(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (SELECT Obj { x := next() }),
                     Z := (SELECT O {x := .x}),
                SELECT (Z.n, (SELECT Z.x), (SELECT Z.x));
            """)

            self.assertEqual(len(res), 3)
            for _, x1, x2 in res:
                self.assertEqual(x1, x2)

    async def test_edgeql_volatility_rebind_nested_01(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (
                    SELECT Obj {
                        tgt: { x := next() }
                    }
                ),
                Z := (SELECT O {tgt: {n, x, y := -.x}}),
                SELECT Z { tgt: {n, x, y} };
            """)

            self.assertEqual(len(res), 3)
            for obj in res:
                for tgt in obj['tgt']:
                    self.assertEqual(tgt['x'], -tgt['y'])

    async def test_edgeql_volatility_rebind_nested_02(self):
        for query in self.test_loop():
            res = await query("""
                WITH O := (
                    SELECT Obj {
                        tgt: { x := next() }
                    }
                ),
                Z := (SELECT O {tgt: {n, y := -.x}}),
                SELECT Z { tgt: {n, x, y} };
            """)

            self.assertEqual(len(res), 3)
            for obj in res:
                for tgt in obj['tgt']:
                    self.assertEqual(tgt['x'], -tgt['y'])

    async def test_edgeql_volatility_rebind_nested_03(self):
        for query in self.test_loop(single=True):
            res = await query("""
                WITH O := (
                    SELECT Obj {
                        tgt: { x := next() }
                    }
                ),
                Z := { o := (SELECT O {tgt: {n, y := -.x}}) },
                SELECT Z { o: {tgt: {n, x, y}} };
            """)

            for obj in res['o']:
                for tgt in obj['tgt']:
                    self.assertEqual(tgt['x'], -tgt['y'])

    async def test_edgeql_volatility_shape_array_01(self):
        for query in self.test_loop():
            res = await query("""
                WITH X := { multi x := [next()] },
                SELECT ((SELECT X.x), (SELECT X.x));
            """)

            self.assertEqual(len(res), 1)
            for obj in res:
                self.assertEqual(obj[0], obj[1])

    async def test_edgeql_volatility_shape_array_02(self):
        for query in self.test_loop():
            res = await query("""
                WITH X := { x := [next()] },
                SELECT ((SELECT X.x), (SELECT X.x));
            """)

            self.assertEqual(len(res), 1)
            for obj in res:
                self.assertEqual(obj[0], obj[1])

    async def test_edgeql_volatility_errors_01(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=36):
                await self.con.execute(
                    r"""
                    SELECT Obj.n + random()
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation",
                    _position=36):
                await self.con.execute(
                    r"""
                    SELECT (Obj.n, random())
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
                    SELECT random() + Obj.n
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
                    SELECT ({1,2}, (INSERT Obj { n := 100 }))
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
                                 INSERT Obj { n := i })))
                    """
                )

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    "can not take cross product of volatile operation"):
                await self.con.execute(
                    r"""
                    WITH X := (WITH x := {1,2},
                    SELECT (x, Obj {m := vol_id(x)})).1
                    SELECT X;
                    """
                )
