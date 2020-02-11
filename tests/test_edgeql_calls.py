#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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

from edb.testbase import server as tb

from edb.tools import test


class TestEdgeQLFuncCalls(tb.QueryTestCase):

    async def test_edgeql_calls_01(self):
        await self.con.execute('''
            CREATE FUNCTION test::call1(
                s: str,
                VARIADIC a: int64,
                NAMED ONLY suffix: str = '-suf',
                NAMED ONLY prefix: str = 'pref-'
            ) -> std::str
                USING (
                    SELECT prefix ++ s ++ <str>sum(array_unpack(a)) ++ suffix
                );
        ''')

        await self.assert_query_result(
            r'''SELECT test::call1('-');''',
            ['pref--0-suf'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', suffix := 's1');''',
            ['pref--0s1'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', prefix := 'p1');''',
            ['p1-0-suf'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', suffix := 's1', prefix := 'p1');''',
            ['p1-0s1'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', 1);''',
            ['pref--1-suf'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', 1, suffix := 's1');''',
            ['pref--1s1'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', 1, prefix := 'p1');''',
            ['p1-1-suf'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', 1, 2, 3, 4, 5);''',
            ['pref--15-suf'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', 1, 2, 3, 4, 5, suffix := 's1');''',
            ['pref--15s1'],
        )

        await self.assert_query_result(
            r'''SELECT test::call1('-', 1, 2, 3, 4, 5, prefix := 'p1');''',
            ['p1-15-suf'],
        )

        await self.assert_query_result(
            r'''
                SELECT test::call1('-', 1, 2, 3, 4, 5, prefix := 'p1',
                                   suffix := 'aaa');
            ''',
            ['p1-15aaa'],
        )

    async def test_edgeql_calls_02(self):
        await self.con.execute('''
            CREATE FUNCTION test::call2(
                VARIADIC a: anytype
            ) -> std::str {
                USING (
                    SELECT '=' ++ <str>len(a) ++ '='
                );
            }
        ''')

        await self.assert_query_result(
            r'''SELECT test::call2('a', 'b');''',
            ['=2='],
        )
        await self.assert_query_result(
            r'''SELECT test::call2(4, 2, 0);''',
            ['=3='],
        )

    async def test_edgeql_calls_03(self):
        await self.con.execute('''
            CREATE FUNCTION test::call3(
                a: int32,
                NAMED ONLY b: int32
            ) -> int32
                USING EdgeQL $$
                    SELECT a + b
                $$;
        ''')

        cases = [
            'SELECT test::call3(1);',
            'SELECT test::call3(1, 2);',
            'SELECT test::call3(1, 2, 3);',
            'SELECT test::call3(b := 1);',
            'SELECT test::call3(1, 2, b := 1);',
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'could not find a function variant'):
                async with self.con.transaction():
                    await self.con.execute(c)

    @test.not_implemented(
        'type of the "[]" default cannot be determined for array<anytype>')
    async def test_edgeql_calls_04(self):
        await self.con.execute('''
            CREATE FUNCTION test::call4(
                a: int32,
                NAMED ONLY b: array<anytype> = []
            ) -> int32
                USING EdgeQL $$
                    SELECT a + len(b)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call4(100);''',
            [100],
        )

        await self.assert_query_result(
            r'''SELECT test::call4(100, b := <int32>[]);''',
            [100],
        )

        await self.assert_query_result(
            r'''SELECT test::call4(100, b := [1, 2]);''',
            [102],
        )
        await self.assert_query_result(
            r'''SELECT test::call4(100, b := ['a', 'b']);''',
            [102],
        )

    async def test_edgeql_calls_05(self):
        await self.con.execute('''
            CREATE FUNCTION test::call5(
                a: int64,
                NAMED ONLY b: OPTIONAL int64 = <int64>{}
            ) -> int64
                USING EdgeQL $$
                    SELECT a + b ?? -100
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call5(1);''',
            [-99],
        )

        await self.assert_query_result(
            r'''SELECT test::call5(<int32>2);''',
            [-98],
        )

        await self.assert_query_result(
            r'''SELECT test::call5(1, b := 20);''',
            [21],
        )

        await self.assert_query_result(
            r'''SELECT test::call5(1, b := <int16>10);''',
            [11],
        )

    async def test_edgeql_calls_06(self):
        await self.con.execute('''
            CREATE FUNCTION test::call6(
                VARIADIC a: int64
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>sum(array_unpack(a));;
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call6();''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT test::call6(1, 2, 3);''',
            [6],
        )

        await self.assert_query_result(
            r'''SELECT test::call6(<int16>1, <int32>2, 3);''',
            [6],
        )

    async def test_edgeql_calls_07(self):
        await self.con.execute('''
            CREATE FUNCTION test::call7(
                a: int64 = 1,
                b: int64 = 2,
                c: int64 = 3,
                NAMED ONLY d: int64 = 4,
                NAMED ONLY e: int64 = 5
            ) -> array<int64>
                USING EdgeQL $$
                    SELECT [a, b, c, d, e]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call7();''',
            [[1, 2, 3, 4, 5]],
        )

        await self.assert_query_result(
            r'''SELECT test::call7(e := 100);''',
            [[1, 2, 3, 4, 100]],
        )

        await self.assert_query_result(
            r'''SELECT test::call7(d := 200);''',
            [[1, 2, 3, 200, 5]],
        )

        await self.assert_query_result(
            r'''SELECT test::call7(20, 30, d := 200);''',
            [[20, 30, 3, 200, 5]],
        )

        await self.assert_query_result(
            r'''SELECT test::call7(20, 30, e := 42, d := 200);''',
            [[20, 30, 3, 200, 42]],
        )

        await self.assert_query_result(
            r'''SELECT test::call7(20, 30, 1, d := 200, e := 42);''',
            [[20, 30, 1, 200, 42]],
        )

        cases = [
            'SELECT test::call7(1, 2, 3, 4, 5);'
            'SELECT test::call7(1, 2, 3, 4);'
            'SELECT test::call7(1, z := 1);'
            'SELECT test::call7(1, 2, 3, z := 1);'
            'SELECT test::call7(1, 2, 3, 4, z := 1);'
            'SELECT test::call7(1, 2, 3, d := 1, z := 10);'
            'SELECT test::call7(1, 2, 3, d := 1, e := 2, z := 10);'
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'could not find a function variant'):
                async with self.con.transaction():
                    await self.con.execute(c)

    async def test_edgeql_calls_08(self):
        await self.con.execute('''
            CREATE FUNCTION test::call8(
                a: int64 = 1,
                NAMED ONLY b: int64 = 2
            ) -> int64
                USING EdgeQL $$
                    SELECT a + b
                $$;

            CREATE FUNCTION test::call8(
                a: float64 = 1.0,
                NAMED ONLY b: int64 = 2
            ) -> int64
                USING EdgeQL $$
                    SELECT 1000 + <int64>a + b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call8(1);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT test::call8(1.0);''',
            [1003],
        )

        await self.assert_query_result(
            r'''SELECT test::call8(1, b := 10);''',
            [11],
        )

        await self.assert_query_result(
            r'''SELECT test::call8(1.0, b := 10);''',
            [1011],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function test::call8 is not unique'):
            async with self.con.transaction():
                await self.con.execute('SELECT test::call8();')

    async def test_edgeql_calls_09(self):
        await self.assert_query_result(
            r'''SELECT sum({1, 2, 3});''',
            {6},
        )

        await self.assert_query_result(
            r'''SELECT sum({<int32>1, 2, 3});''',
            {6},
        )

        await self.assert_query_result(
            r'''SELECT sum({<float32>1, 2, 3});''',
            {6},
        )

        await self.assert_query_result(
            r'''SELECT sum({<float32>1, <int32>2, 3});''',
            {6},
        )

        await self.assert_query_result(
            r'''SELECT sum({<int16>1, <int32>2, <decimal>3});''',
            {6},
        )

        await self.assert_query_result(
            r'''SELECT sum({1.1, 2.2, 3});''',
            {6.3},
        )

    async def test_edgeql_calls_10(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF sum({1, 2, 3})).name;''',
            {'std::int64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF sum({<int32>1, 2, 3})).name;''',
            {'std::int64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF sum({<float32>1, 2, 3})).name;''',
            {'std::float64'},
        )

        await self.assert_query_result(
            r'''
                SELECT (INTROSPECT TYPEOF
                        sum({<float32>1, <int32>2, 3})).name;
            ''',
            {'std::float64'},
        )

        await self.assert_query_result(
            r'''
                SELECT (INTROSPECT TYPEOF
                        sum({<int16>1, <int32>2, <decimal>3})).name;
            ''',
            {'std::decimal'},
        )

        await self.assert_query_result(
            r'''
                SELECT (INTROSPECT TYPEOF
                        sum({<int16>1, <int32>2, <bigint>3})).name;
            ''',
            {'std::bigint'},
        )

        await self.assert_query_result(
            r'''
                SELECT (INTROSPECT TYPEOF
                        sum({<int16>1, 2, <decimal>3})).name;
            ''',
            {'std::decimal'},
        )

        await self.assert_query_result(
            r'''
                SELECT (INTROSPECT TYPEOF
                        sum({1, <float32>2.1, <float64>3})).name;
            ''',
            {'std::float64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF sum({1.1, 2.2, 3.3})).name;''',
            {'std::float64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF
                        sum({<float32>1, <int32>2, <float32>3})).name;''',
            {'std::float64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF
                        sum({<float32>1, <float32>2, <float32>3})).name;''',
            {'std::float32'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF sum({1.1, 2.2, 3})).name;''',
            {'std::float64'},
        )

    async def test_edgeql_calls_11(self):
        await self.con.execute('''
            CREATE FUNCTION test::call11(
                a: array<int32>
            ) -> int64
                USING EdgeQL $$
                    SELECT sum(array_unpack(a))
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call11([<int16>1, <int16>22]);''',
            [23],
        )

        await self.assert_query_result(
            r'''SELECT test::call11([<int16>1, <int32>23]);''',
            [24],
        )

        await self.assert_query_result(
            r'''SELECT test::call11([<int32>1, <int32>24]);''',
            [25],
        )

        cases = [
            'SELECT test::call11([<int32>1, 1.1]);',
            'SELECT test::call11([<int32>1, <float32>1]);',
            'SELECT test::call11([1, 2]);',
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'could not find a function variant'):
                async with self.con.transaction():
                    await self.con.execute(c)

    @test.not_implemented(
        "this results in 2 PG functions: `(anynonarray)->bigint` and "
        "`(bigint)->bigint`; PG fails with 'function is not unique' "
        "at the call site")
    async def test_edgeql_calls_12(self):
        await self.con.execute('''
            CREATE FUNCTION test::call12(
                a: anyint
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>a + 100
                $$;

            CREATE FUNCTION test::call12(
                a: int64
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>a + 1
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call12(<int32>1);''',
            [101],
        )

        await self.assert_query_result(
            r'''SELECT test::call12(1);''',
            [2],
        )

    async def test_edgeql_calls_13(self):
        await self.con.execute('''
            CREATE FUNCTION test::inner(
                a: anytype
            ) -> int64
                USING EdgeQL $$
                    SELECT 1;
                $$;

            CREATE FUNCTION test::call13(
                a: anytype
            ) -> int64
                USING EdgeQL $$
                    SELECT test::inner(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call13('aaa');''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call13(b'aaaa');''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call13([1, 2, 3, 4, 5]);''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call13(['a', 'b']);''',
            [{}],
        )

        await self.con.execute('''
            CREATE FUNCTION test::inner(
                a: str
            ) -> int64
                USING EdgeQL $$
                    SELECT 2;
                $$;

            CREATE FUNCTION test::call13_2(
                a: anytype
            ) -> int64
                USING EdgeQL $$
                    SELECT test::inner(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call13_2('aaa');''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT test::call13_2(b'aaaa');''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call13_2([1, 2, 3, 4, 5]);''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call13_2(['a', 'b']);''',
            [{}],
        )

    async def test_edgeql_calls_14(self):
        await self.con.execute('''
            CREATE FUNCTION test::call14(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call14('aaa');''',
            [['aaa']],
        )

        self.assertEqual(
            await self.con.fetchall(r'''SELECT test::call14(b'aaaa');'''),
            [[b'aaaa']]
        )

        await self.assert_query_result(
            r'''SELECT test::call14(1);''',
            [[1]],
        )

    async def test_edgeql_calls_15(self):
        await self.con.execute('''
            CREATE FUNCTION test::call15(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a, a, a]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call15('aaa');''',
            [['aaa', 'aaa', 'aaa']],
        )

        await self.assert_query_result(
            r'''SELECT test::call15(1);''',
            [[1, 1, 1]],
        )

    async def test_edgeql_calls_16(self):
        await self.con.execute('''
            CREATE FUNCTION test::call16(
                a: array<anytype>,
                idx: int64
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[idx]
                $$;

            CREATE FUNCTION test::call16(
                a: array<anytype>,
                idx: str
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[<int64>idx + 1]
                $$;

            CREATE FUNCTION test::call16(
                a: anyscalar,
                idx: int64
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[idx]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call16([1, 2, 3], 1);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT test::call16(['a', 'b', 'c'], 1);''',
            ['b'],
        )

        await self.assert_query_result(
            r'''SELECT test::call16([1, 2, 3], '1');''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT test::call16(['a', 'b', 'c'], '1');''',
            ['c'],
        )

        await self.assert_query_result(
            r'''SELECT test::call16('xyz', 1);''',
            ['y'],
        )

    async def test_edgeql_calls_17(self):
        await self.con.execute('''
            CREATE FUNCTION test::call17(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a, a, a]
                $$;

            CREATE FUNCTION test::call17(
                a: str
            ) -> array<str>
                USING EdgeQL $$
                    SELECT ['!!!!', a, '!!!!']
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call17(2);''',
            [[2, 2, 2]],
        )

        await self.assert_query_result(
            r'''SELECT test::call17('aaa');''',
            [['!!!!', 'aaa', '!!!!']],
        )

    async def test_edgeql_calls_18(self):
        await self.con.execute('''
            CREATE FUNCTION test::call18(
                VARIADIC a: anytype
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call18(2);''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call18(1, 2, 3);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT test::call18('a', 'b');''',
            [2],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant'):

            async with self.con.transaction():
                await self.con.execute('SELECT test::call18(1, 2, "a");')

    @test.not_implemented(
        "PG fails with 'return type record[] is not supported'")
    async def test_edgeql_calls_19(self):
        # XXX: Postgres raises the following error for this:
        #    return type record[] is not supported for SQL functions

        await self.con.execute('''
            CREATE FUNCTION test::call19(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a]
                $$;
        ''')

        await self.con.execute('SELECT test::call19((1,2));')

    @test.xfail(
        "Polymorphic callable matching is currently too dumb to realize "
        "that `+` _is_ defined for 'anyreal', even though there are multiple "
        "actual forms defined.")
    async def test_edgeql_calls_20(self):
        await self.con.execute('''
            CREATE FUNCTION test::call20_1(
                a: anyreal, b: anyreal
            ) -> anyreal
                USING EdgeQL $$
                    SELECT a + b
                $$;

            CREATE FUNCTION test::call20_2(
                a: anyscalar, b: anyscalar
            ) -> bool
                USING EdgeQL $$
                    SELECT a < b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call20_1(10, 20);''',
            [30],
        )

        await self.assert_query_result(
            r'''SELECT test::call20_2(1, 2);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT test::call20_2('b', 'a');''',
            [False],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant'):
            async with self.con.transaction():
                await self.con.execute('SELECT test::call20_1(1, "1");')

    async def test_edgeql_calls_21(self):
        await self.con.execute('''
            CREATE FUNCTION test::call21(
                a: array<anytype>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call21(<array<str>>[]);''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT test::call21([1,2]);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT test::call21(['a', 'b', 'c']);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT test::call21([(1, 2), (2, 3), (3, 4), (4, 5)]);''',
            [4],
        )

    async def test_edgeql_calls_22(self):
        await self.con.execute('''
            CREATE FUNCTION test::call22(
                a: str, b: str
            ) -> str
                USING EdgeQL $$
                    SELECT a ++ b
                $$;

            CREATE FUNCTION test::call22(
                a: array<anytype>, b: array<anytype>
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT a ++ b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call22('a', 'b');''',
            ['ab'],
        )

        await self.assert_query_result(
            r'''SELECT test::call22(['a'], ['b']);''',
            [
                ['a', 'b'],
            ]
        )

    async def test_edgeql_calls_23(self):
        await self.con.execute('''
            CREATE FUNCTION test::call23(
                a: anytype,
                idx: int64
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[idx]
                $$;

            CREATE FUNCTION test::call23(
                a: anytype,
                idx: int32
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[-idx:]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call23('abcde', 2);''',
            ['c'],
        )

        await self.assert_query_result(
            r'''SELECT test::call23('abcde', <int32>2);''',
            ['de'],
        )

        self.assertEqual(
            await self.con.fetchone(
                r'''SELECT test::call23(to_json('[{"a":"b"}]'), 0);'''),
            '{"a": "b"}')
        self.assertEqual(
            await self.con.fetchall_json(
                r'''SELECT test::call23(to_json('[{"a":"b"}]'), 0);'''),
            '[{"a": "b"}]')

    async def test_edgeql_calls_24(self):
        await self.con.execute('''
            CREATE FUNCTION test::call24() -> str
                USING EdgeQL $$
                    SELECT 'ab' ++ 'cd'
                $$;

            CREATE FUNCTION test::call24(
                a: str
            ) -> str
                USING EdgeQL $$
                    SELECT a ++ '!'
                $$;
        ''')

        await self.assert_query_result(
            r'''select test::call24();''',
            ['abcd'],
        )

        await self.assert_query_result(
            r'''select test::call24('aaa');''',
            ['aaa!'],
        )

    async def test_edgeql_calls_26(self):
        await self.con.execute('''
            CREATE FUNCTION test::call26(
                a: array<anyscalar>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call26(['aaa']);''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call26([b'', b'aa']);''',
            [2],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant'):
            async with self.con.transaction():
                await self.con.execute('SELECT test::call26([(1, 2)]);')

    async def test_edgeql_calls_27(self):
        await self.con.execute('''
            CREATE FUNCTION test::call27(
                a: array<anyint>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call27([<int32>1, <int32>2]);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT test::call27([1, 2, 3]);''',
            [3],
        )

        cases = [
            "SELECT test::call27(['aaa']);",
            "SELECT test::call27([b'', b'aa']);",
            "SELECT test::call27([1.0, 2.1]);",
            "SELECT test::call27([('a',), ('b',)]);",
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'could not find a function variant'):
                async with self.con.transaction():
                    await self.con.execute(c)

    @test.not_implemented(
        "we get two `(anynonarray)->bigint` PG functions which is ambiguous")
    async def test_edgeql_calls_28(self):
        await self.con.execute('''
            CREATE FUNCTION test::call28(
                a: array<anyint>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;

            CREATE FUNCTION test::call28(
                a: array<anyscalar>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a) + 1000
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call28([<int32>1, <int32>2]);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT test::call28([1, 2, 3]);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT test::call28(['a', 'b']);''',
            [1002],
        )

    async def test_edgeql_calls_29(self):
        await self.con.execute('''
            CREATE FUNCTION test::call29(
                a: anyint
            ) -> anyint
                USING EdgeQL $$
                    SELECT a + 1
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call29(10);''',
            [11],
        )

    async def test_edgeql_calls_30(self):
        await self.con.execute('''
            CREATE FUNCTION test::call30(
                a: anyint
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>a + 100
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call30(10);''',
            [110],
        )

        await self.assert_query_result(
            r'''SELECT test::call30(<int32>20);''',
            [120],
        )

    async def test_edgeql_calls_31(self):
        await self.con.execute('''
            CREATE FUNCTION test::call31(
                a: anytype
            ) -> anytype
                USING EdgeQL $$
                    SELECT a
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call31(10);''',
            [10],
        )

        await self.assert_query_result(
            r'''SELECT test::call31('aa');''',
            ['aa'],
        )

        await self.assert_query_result(
            r'''SELECT test::call31([1, 2]);''',
            [[1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT test::call31([1, 2])[0];''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=1001, b:=1002)).a;''',
            [1001],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=1001, b:=1002)).1;''',
            [1002],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=['a', 'b'], b:=['x', 'y'])).1;''',
            [['x', 'y']],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=['a', 'b'], b:=['x', 'y'])).a[1];''',
            ['b'],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=1001, b:=1002));''',
            [{"a": 1001, "b": 1002}],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=[(x:=1)])).a[0].x;''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=[(x:=1)])).0[0].x;''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=[(x:=1)])).0[0].0;''',
            [{}],
        )

        await self.assert_query_result(
            r'''SELECT test::call31((a:=[(x:=1)])).a[0];''',
            [{"x": 1}],
        )

    @test.not_implemented(
        "This fails in Postgres with "
        "'function edgedb_test.call32(bigint[], smallint[]) does not exist'. "
        "To fix, polymorphic function calls must cast into a common type "
        "before calling.")
    async def test_edgeql_calls_32(self):
        await self.con.execute('''
            CREATE FUNCTION test::call32(
                a: anytype, b: anytype
            ) -> anytype
                USING EdgeQL $$
                    SELECT a ++ b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call32([1], [<int16>2]);''',
            [
                [1, 2],
            ]
        )

    async def test_edgeql_calls_33(self):
        # Tuple argument

        await self.con.execute('''
            CREATE FUNCTION test::call33(
                a: tuple<int64, tuple<int64>>,
                b: tuple<foo: int64, bar: str>
            ) -> int64
                USING EdgeQL $$
                    SELECT a.0 + b.foo
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call33((1, (2,)), (foo := 10, bar := 'bar'));''',
            [
                11,
            ]
        )

        await self.con.execute('''
            CREATE FUNCTION test::call33_2(
                a: array<tuple<int64, int64>>
            ) -> int64
                USING EdgeQL $$
                    SELECT a[0].0
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call33_2([(1, 2), (3, 4)]);''',
            [
                1,
            ]
        )

    async def test_edgeql_calls_34(self):
        # Tuple return

        await self.con.execute('''
            CREATE FUNCTION test::call34(
                a: int64
            ) -> tuple<int64, tuple<foo: int64>>
                USING EdgeQL $$
                    SELECT (a, ((a + 1),))
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call34(1);''',
            [
                [1, {'foo': 2}]
            ]
        )

        await self.assert_query_result(
            r'''SELECT test::call34(1).1.foo;''',
            [
                2
            ]
        )

        await self.con.execute('''
            CREATE FUNCTION test::call34_2(
                a: int64
            ) -> array<tuple<int64>>
                USING EdgeQL $$
                    SELECT [(a,)]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call34_2(1);''',
            [
                [[1]]
            ]
        )

    async def test_edgeql_calls_35(self):
        # define a function with positional arguments with defaults
        await self.con.execute('''
            CREATE FUNCTION test::call35(
                a: int64 = 1,
                b: int64 = 2
            ) -> int64
                USING EdgeQL $$
                    SELECT a + b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT test::call35();''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT test::call35(2);''',
            [4],
        )

        await self.assert_query_result(
            r'''SELECT test::call35(2, 3);''',
            [5],
        )
