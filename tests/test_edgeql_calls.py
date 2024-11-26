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

    NO_FACTOR = True

    async def test_edgeql_calls_01(self):
        await self.con.execute('''
            CREATE FUNCTION call1(
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
            r'''SELECT call1('-');''',
            ['pref--0-suf'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', suffix := 's1');''',
            ['pref--0s1'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', prefix := 'p1');''',
            ['p1-0-suf'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', suffix := 's1', prefix := 'p1');''',
            ['p1-0s1'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', 1);''',
            ['pref--1-suf'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', 1, suffix := 's1');''',
            ['pref--1s1'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', 1, prefix := 'p1');''',
            ['p1-1-suf'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', 1, 2, 3, 4, 5);''',
            ['pref--15-suf'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', 1, 2, 3, 4, 5, suffix := 's1');''',
            ['pref--15s1'],
        )

        await self.assert_query_result(
            r'''SELECT call1('-', 1, 2, 3, 4, 5, prefix := 'p1');''',
            ['p1-15-suf'],
        )

        await self.assert_query_result(
            r'''
                SELECT call1('-', 1, 2, 3, 4, 5, prefix := 'p1',
                                   suffix := 'aaa');
            ''',
            ['p1-15aaa'],
        )

    async def test_edgeql_calls_02(self):
        await self.con.execute('''
            CREATE FUNCTION call2(
                VARIADIC a: anytype
            ) -> std::str {
                USING (
                    SELECT '=' ++ <str>len(a) ++ '='
                );
            }
        ''')

        await self.assert_query_result(
            r'''SELECT call2('a', 'b');''',
            ['=2='],
        )
        await self.assert_query_result(
            r'''SELECT call2(4, 2, 0);''',
            ['=3='],
        )

    async def test_edgeql_calls_03(self):
        await self.con.execute('''
            CREATE FUNCTION call3(
                a: int32,
                NAMED ONLY b: int32
            ) -> int32
                USING EdgeQL $$
                    SELECT a + b
                $$;
        ''')

        cases = [
            'SELECT call3(1);',
            'SELECT call3(1, 2);',
            'SELECT call3(1, 2, 3);',
            'SELECT call3(b := 1);',
            'SELECT call3(1, 2, b := 1);',
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'function .+ does not exist'):
                async with self.con.transaction():
                    await self.con.execute(c)

    @test.not_implemented(
        'type of the "[]" default cannot be determined for array<anytype>')
    async def test_edgeql_calls_04(self):
        await self.con.execute('''
            CREATE FUNCTION call4(
                a: int32,
                NAMED ONLY b: array<anytype> = []
            ) -> int32
                USING EdgeQL $$
                    SELECT a + len(b)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call4(100);''',
            [100],
        )

        await self.assert_query_result(
            r'''SELECT call4(100, b := <int32>[]);''',
            [100],
        )

        await self.assert_query_result(
            r'''SELECT call4(100, b := [1, 2]);''',
            [102],
        )
        await self.assert_query_result(
            r'''SELECT call4(100, b := ['a', 'b']);''',
            [102],
        )

    async def test_edgeql_calls_05(self):
        await self.con.execute('''
            CREATE FUNCTION call5(
                a: int64,
                NAMED ONLY b: OPTIONAL int64 = <int64>{}
            ) -> int64
                USING EdgeQL $$
                    SELECT a + b ?? -100
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call5(1);''',
            [-99],
        )

        await self.assert_query_result(
            r'''SELECT call5(<int32>2);''',
            [-98],
        )

        await self.assert_query_result(
            r'''SELECT call5(1, b := 20);''',
            [21],
        )

        await self.assert_query_result(
            r'''SELECT call5(1, b := <int16>10);''',
            [11],
        )

        await self.assert_query_result(
            r'''SELECT call5(<int32>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT call5(<int32>{}, b := <int32>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT call5(<int32>{}, b := 50);''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT call5(1, b := <int32>{});''',
            [-99],
        )

        await self.assert_query_result(
            r'''
            WITH X := (SELECT _:={1,2,3} FILTER _ < 0)
            SELECT call5(1, b := X);''',
            [-99],
        )

    async def test_edgeql_calls_06(self):
        await self.con.execute('''
            CREATE FUNCTION call6(
                VARIADIC a: int64
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>sum(array_unpack(a))
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call6();''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT call6(1, 2, 3);''',
            [6],
        )

        await self.assert_query_result(
            r'''SELECT call6(<int16>1, <int32>2, 3);''',
            [6],
        )

    async def test_edgeql_calls_07(self):
        await self.con.execute('''
            CREATE FUNCTION call7(
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
            r'''SELECT call7();''',
            [[1, 2, 3, 4, 5]],
        )

        await self.assert_query_result(
            r'''SELECT call7(e := 100);''',
            [[1, 2, 3, 4, 100]],
        )

        await self.assert_query_result(
            r'''SELECT call7(d := 200);''',
            [[1, 2, 3, 200, 5]],
        )

        await self.assert_query_result(
            r'''SELECT call7(20, 30, d := 200);''',
            [[20, 30, 3, 200, 5]],
        )

        await self.assert_query_result(
            r'''SELECT call7(20, 30, e := 42, d := 200);''',
            [[20, 30, 3, 200, 42]],
        )

        await self.assert_query_result(
            r'''SELECT call7(20, 30, 1, d := 200, e := 42);''',
            [[20, 30, 1, 200, 42]],
        )

        cases = [
            'SELECT call7(1, 2, 3, 4, 5);'
            'SELECT call7(1, 2, 3, 4);'
            'SELECT call7(1, z := 1);'
            'SELECT call7(1, 2, 3, z := 1);'
            'SELECT call7(1, 2, 3, 4, z := 1);'
            'SELECT call7(1, 2, 3, d := 1, z := 10);'
            'SELECT call7(1, 2, 3, d := 1, e := 2, z := 10);'
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'function .+ does not exist'):
                async with self.con.transaction():
                    await self.con.execute(c)

    async def test_edgeql_calls_08(self):
        await self.con.execute('''
            CREATE FUNCTION call8(
                a: int64 = 1,
                NAMED ONLY b: int64 = 2
            ) -> int64
                USING EdgeQL $$
                    SELECT a + b
                $$;

            CREATE FUNCTION call8(
                a: float64 = 1.0,
                NAMED ONLY b: int64 = 2
            ) -> int64
                USING EdgeQL $$
                    SELECT 1000 + <int64>a + b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call8(1);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT call8(1.0);''',
            [1003],
        )

        await self.assert_query_result(
            r'''SELECT call8(1, b := 10);''',
            [11],
        )

        await self.assert_query_result(
            r'''SELECT call8(1.0, b := 10);''',
            [1011],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function call8 is not unique'):
            async with self.con.transaction():
                await self.con.execute('SELECT call8();')

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
            CREATE FUNCTION call11(
                a: array<int32>
            ) -> int64
                USING EdgeQL $$
                    SELECT sum(array_unpack(a))
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call11([<int16>1, <int16>22]);''',
            [23],
        )

        await self.assert_query_result(
            r'''SELECT call11([<int16>1, <int32>23]);''',
            [24],
        )

        await self.assert_query_result(
            r'''SELECT call11([<int32>1, <int32>24]);''',
            [25],
        )

        cases = [
            'SELECT call11([<int32>1, 1.1]);',
            'SELECT call11([<int32>1, <float32>1]);',
            'SELECT call11([1, 2]);',
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'function .+ does not exist'):
                async with self.con.transaction():
                    await self.con.execute(c)

    @test.not_implemented(
        "this results in 2 PG functions: `(anynonarray)->bigint` and "
        "`(bigint)->bigint`; PG fails with 'function is not unique' "
        "at the call site")
    async def test_edgeql_calls_12(self):
        await self.con.execute('''
            CREATE FUNCTION call12(
                a: anyint
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>a + 100
                $$;

            CREATE FUNCTION call12(
                a: int64
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>a + 1
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call12(<int32>1);''',
            [101],
        )

        await self.assert_query_result(
            r'''SELECT call12(1);''',
            [2],
        )

    async def test_edgeql_calls_13(self):
        await self.con.execute('''
            CREATE FUNCTION inner(
                a: anytype
            ) -> int64
                USING (
                    SELECT 1
                );

            CREATE FUNCTION call13(
                a: anytype
            ) -> int64
                USING (
                    SELECT inner(a)
                );
        ''')

        await self.assert_query_result(
            r'''SELECT call13('aaa');''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call13(b'aaaa');''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call13([1, 2, 3, 4, 5]);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call13(['a', 'b']);''',
            [1],
        )

        await self.con.execute('''
            CREATE FUNCTION inner(
                a: str
            ) -> int64
                USING EdgeQL $$
                    SELECT 2
                $$;

            CREATE FUNCTION call13_2(
                a: anytype
            ) -> int64
                USING EdgeQL $$
                    SELECT inner(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call13_2('aaa');''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT call13_2(b'aaaa');''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call13_2([1, 2, 3, 4, 5]);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call13_2(['a', 'b']);''',
            [1],
        )

    async def test_edgeql_calls_13_sdl(self):
        await self.migrate("""
            function inner(a: anytype) -> str
                using ("anytype");

            function inner(a: int64) -> str
                using ("int64");

            function call13_sdl(a: anytype) -> str
                using (inner(a));
        """)

        await self.assert_query_result(
            r"SELECT call13_sdl(1.0)",
            ["anytype"],
        )

        await self.assert_query_result(
            r"SELECT call13_sdl(1)",
            ["int64"],
        )

    async def test_edgeql_calls_14(self):
        await self.con.execute('''
            CREATE FUNCTION call14(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call14('aaa');''',
            [['aaa']],
        )

        self.assertEqual(
            await self.con.query(r'''SELECT call14(b'aaaa');'''),
            [[b'aaaa']]
        )

        await self.assert_query_result(
            r'''SELECT call14(1);''',
            [[1]],
        )

    async def test_edgeql_calls_15(self):
        await self.con.execute('''
            CREATE FUNCTION call15(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a, a, a]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call15('aaa');''',
            [['aaa', 'aaa', 'aaa']],
        )

        await self.assert_query_result(
            r'''SELECT call15(1);''',
            [[1, 1, 1]],
        )

    async def test_edgeql_calls_16(self):
        await self.con.execute('''
            CREATE FUNCTION call16(
                a: array<anytype>,
                idx: int64
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[idx]
                $$;

            CREATE FUNCTION call16(
                a: array<anytype>,
                idx: str
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[<int64>idx + 1]
                $$;

            CREATE FUNCTION call16(
                a: anyscalar,
                idx: int64
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[idx]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call16([1, 2, 3], 1);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT call16(['a', 'b', 'c'], 1);''',
            ['b'],
        )

        await self.assert_query_result(
            r'''SELECT call16([1, 2, 3], '1');''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT call16(['a', 'b', 'c'], '1');''',
            ['c'],
        )

        await self.assert_query_result(
            r'''SELECT call16('xyz', 1);''',
            ['y'],
        )

    async def test_edgeql_calls_17(self):
        await self.con.execute('''
            CREATE FUNCTION call17(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a, a, a]
                $$;

            CREATE FUNCTION call17(
                a: str
            ) -> array<str>
                USING EdgeQL $$
                    SELECT ['!!!!', a, '!!!!']
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call17(2);''',
            [[2, 2, 2]],
        )

        await self.assert_query_result(
            r'''SELECT call17('aaa');''',
            [['!!!!', 'aaa', '!!!!']],
        )

    async def test_edgeql_calls_18(self):
        await self.con.execute('''
            CREATE FUNCTION call18(
                VARIADIC a: anytype
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call18(2);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call18(1, 2, 3);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT call18('a', 'b');''',
            [2],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function .+ does not exist'):

            async with self.con.transaction():
                await self.con.execute('SELECT call18(1, 2, "a");')

    @test.not_implemented(
        "PG fails with 'return type record[] is not supported'")
    async def test_edgeql_calls_19(self):
        # XXX: Postgres raises the following error for this:
        #    return type record[] is not supported for SQL functions

        await self.con.execute('''
            CREATE FUNCTION call19(
                a: anytype
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT [a]
                $$;
        ''')

        await self.con.execute('SELECT call19((1,2));')
        await self.con.execute('SELECT call19((1,));')

    @test.xerror(
        "Polymorphic callable matching is currently too dumb to realize "
        "that `+` _is_ defined for 'anyreal', even though there are multiple "
        "actual forms defined.")
    async def test_edgeql_calls_20(self):
        await self.con.execute('''
            CREATE FUNCTION call20_1(
                a: anyreal, b: anyreal
            ) -> anyreal
                USING EdgeQL $$
                    SELECT a + b
                $$;

            CREATE FUNCTION call20_2(
                a: anyscalar, b: anyscalar
            ) -> bool
                USING EdgeQL $$
                    SELECT a < b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call20_1(10, 20);''',
            [30],
        )

        await self.assert_query_result(
            r'''SELECT call20_2(1, 2);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT call20_2('b', 'a');''',
            [False],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function .+ does not exist'):
            async with self.con.transaction():
                await self.con.execute('SELECT call20_1(1, "1");')

    async def test_edgeql_calls_21(self):
        await self.con.execute('''
            CREATE FUNCTION call21(
                a: array<anytype>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call21(<array<str>>[]);''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT call21([1,2]);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT call21(['a', 'b', 'c']);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT call21([(1, 2), (2, 3), (3, 4), (4, 5)]);''',
            [4],
        )

    async def test_edgeql_calls_22(self):
        await self.con.execute('''
            CREATE FUNCTION call22(
                a: str, b: str
            ) -> str
                USING EdgeQL $$
                    SELECT a ++ b
                $$;

            CREATE FUNCTION call22(
                a: array<anytype>, b: array<anytype>
            ) -> array<anytype>
                USING EdgeQL $$
                    SELECT a ++ b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call22('a', 'b');''',
            ['ab'],
        )

        await self.assert_query_result(
            r'''SELECT call22(['a'], ['b']);''',
            [
                ['a', 'b'],
            ]
        )

    async def test_edgeql_calls_23(self):
        await self.con.execute('''
            CREATE FUNCTION call23(
                a: anytype,
                idx: int64
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[idx]
                $$;

            CREATE FUNCTION call23(
                a: anytype,
                idx: int32
            ) -> anytype
                USING EdgeQL $$
                    SELECT a[-idx:]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call23('abcde', 2);''',
            ['c'],
        )

        await self.assert_query_result(
            r'''SELECT call23('abcde', <int32>2);''',
            ['de'],
        )

        self.assertEqual(
            await self.con.query_single(
                r'''SELECT call23(to_json('[{"a":"b"}]'), 0);'''),
            '{"a": "b"}')
        self.assertEqual(
            await self.con.query_json(
                r'''SELECT call23(to_json('[{"a":"b"}]'), 0);'''),
            '[{"a": "b"}]')

    async def test_edgeql_calls_24(self):
        await self.con.execute('''
            CREATE FUNCTION call24() -> str
                USING EdgeQL $$
                    SELECT 'ab' ++ 'cd'
                $$;

            CREATE FUNCTION call24(
                a: str
            ) -> str
                USING EdgeQL $$
                    SELECT a ++ '!'
                $$;
        ''')

        await self.assert_query_result(
            r'''select call24();''',
            ['abcd'],
        )

        await self.assert_query_result(
            r'''select call24('aaa');''',
            ['aaa!'],
        )

    async def test_edgeql_calls_26(self):
        await self.con.execute('''
            CREATE FUNCTION call26(
                a: array<anyscalar>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call26(['aaa']);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call26([b'', b'aa']);''',
            [2],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function .+ does not exist'):
            async with self.con.transaction():
                await self.con.execute('SELECT call26([(1, 2)]);')

    async def test_edgeql_calls_27(self):
        await self.con.execute('''
            CREATE FUNCTION call27(
                a: array<anyint>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call27([<int32>1, <int32>2]);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT call27([1, 2, 3]);''',
            [3],
        )

        cases = [
            "SELECT call27(['aaa']);",
            "SELECT call27([b'', b'aa']);",
            "SELECT call27([1.0, 2.1]);",
            "SELECT call27([('a',), ('b',)]);",
        ]

        for c in cases:
            with self.assertRaisesRegex(
                    edgedb.QueryError,
                    r'function .+ does not exist'):
                async with self.con.transaction():
                    await self.con.execute(c)

    @test.not_implemented(
        "we get two `(anynonarray)->bigint` PG functions which is ambiguous")
    async def test_edgeql_calls_28(self):
        await self.con.execute('''
            CREATE FUNCTION call28(
                a: array<anyint>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a)
                $$;

            CREATE FUNCTION call28(
                a: array<anyscalar>
            ) -> int64
                USING EdgeQL $$
                    SELECT len(a) + 1000
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call28([<int32>1, <int32>2]);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT call28([1, 2, 3]);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT call28(['a', 'b']);''',
            [1002],
        )

    async def test_edgeql_calls_29(self):
        await self.con.execute('''
            CREATE FUNCTION call29(
                a: anyint
            ) -> anyint
                USING EdgeQL $$
                    SELECT a + 1
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call29(10);''',
            [11],
        )

    async def test_edgeql_calls_30(self):
        await self.con.execute('''
            CREATE FUNCTION call30(
                a: anyint
            ) -> int64
                USING EdgeQL $$
                    SELECT <int64>a + 100
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call30(10);''',
            [110],
        )

        await self.assert_query_result(
            r'''SELECT call30(<int32>20);''',
            [120],
        )

    async def test_edgeql_calls_31(self):
        await self.con.execute('''
            CREATE FUNCTION call31(
                a: anytype
            ) -> anytype
                USING EdgeQL $$
                    SELECT a
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call31(10);''',
            [10],
        )

        await self.assert_query_result(
            r'''SELECT call31('aa');''',
            ['aa'],
        )

        await self.assert_query_result(
            r'''SELECT call31([1, 2]);''',
            [[1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT call31([1, 2])[0];''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=1001, b:=1002)).a;''',
            [1001],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=1001, b:=1002)).1;''',
            [1002],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=['a', 'b'], b:=['x', 'y'])).1;''',
            [['x', 'y']],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=['a', 'b'], b:=['x', 'y'])).a[1];''',
            ['b'],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=1001, b:=1002));''',
            [{"a": 1001, "b": 1002}],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=[(x:=1)])).a[0].x;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=[(x:=1)])).0[0].x;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=[(x:=1)])).0[0].0;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT call31((a:=[(x:=1)])).a[0];''',
            [{"x": 1}],
        )

    @test.not_implemented(
        "This fails in Postgres with "
        "'function edgedb_test.call32(bigint[], smallint[]) does not exist'. "
        "To fix, polymorphic function calls must cast into a common type "
        "before calling.")
    async def test_edgeql_calls_32(self):
        await self.con.execute('''
            CREATE FUNCTION call32(
                a: anytype, b: anytype
            ) -> anytype
                USING EdgeQL $$
                    SELECT a ++ b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call32([1], [<int16>2]);''',
            [
                [1, 2],
            ]
        )

    async def test_edgeql_calls_33(self):
        # Tuple argument

        await self.con.execute('''
            CREATE FUNCTION call33(
                a: tuple<int64, tuple<int64>>,
                b: tuple<foo: int64, bar: str>
            ) -> int64
                USING EdgeQL $$
                    SELECT a.0 + b.foo
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call33((1, (2,)), (foo := 10, bar := 'bar'));''',
            [
                11,
            ]
        )

    async def test_edgeql_calls_34(self):
        # Tuple argument

        await self.con.execute('''
            CREATE FUNCTION call34(
                a: array<tuple<int64, int64>>
            ) -> int64
                USING EdgeQL $$
                    SELECT a[0].0
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call34([(1, 2), (3, 4)]);''',
            [
                1,
            ]
        )

    async def test_edgeql_calls_35a(self):
        # Tuple return

        await self.con.execute('''
            CREATE FUNCTION call35(
                a: int64
            ) -> tuple<int64, tuple<foo: int64>>
                USING EdgeQL $$
                    SELECT (a, ((a + 1),))
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call35(1);''',
            [
                [1, {'foo': 2}]
            ]
        )

        await self.assert_query_result(
            r'''SELECT call35(1).1.foo;''',
            [
                2
            ]
        )

    async def test_edgeql_calls_35b(self):
        # Tuple return with a deep and unavoidable implicit cast

        await self.con.execute('''
            CREATE FUNCTION call35(
                a: tuple<int64, array<tuple<int64>>>
            ) -> tuple<int64, array<tuple<foo: int64>>>
                USING EdgeQL $$
                    SELECT a
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call35((1, [(2,)]));''',
            [
                [1, [{'foo': 2}]]
            ]
        )

        await self.assert_query_result(
            r'''SELECT call35((1, [(2,)])).1[0].foo;''',
            [
                2
            ]
        )

    async def test_edgeql_calls_35c(self):
        # Array return with a tuple forcing a cast

        await self.con.execute('''
            CREATE SCALAR TYPE Foo extending str;
            CREATE FUNCTION call35() -> array<tuple<Foo>>
            USING (SELECT [('1',)] ++ [('2',)]);
        ''')

        await self.assert_query_result(
            r'''SELECT call35();''',
            [[["1"], ["2"]]],
        )

    async def test_edgeql_calls_36(self):
        await self.con.execute('''
            CREATE FUNCTION call36(
                a: int64
            ) -> array<tuple<int64>>
                USING EdgeQL $$
                    SELECT [(a,)]
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call36(1);''',
            [
                [[1]]
            ]
        )

    async def test_edgeql_calls_37(self):
        # define a function with positional arguments with defaults
        await self.con.execute('''
            CREATE FUNCTION call37(
                a: int64 = 1,
                b: int64 = 2
            ) -> int64
                USING EdgeQL $$
                    SELECT a + b
                $$;
        ''')

        await self.assert_query_result(
            r'''SELECT call37();''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT call37(2);''',
            [4],
        )

        await self.assert_query_result(
            r'''SELECT call37(2, 3);''',
            [5],
        )

    async def test_edgeql_calls_38(self):
        # Test a function taking an object as an argument.
        await self.con.execute('''
            CREATE TYPE C38 { CREATE PROPERTY name -> str };
            INSERT C38 { name := 'yay' };
            CREATE FUNCTION call38(
                a: C38
            ) -> OPTIONAL str
                USING (
                    SELECT a.name
                );
        ''')

        await self.assert_query_result(
            r'''SELECT call38(C38);''',
            ['yay'],
        )

    async def test_edgeql_calls_39(self):
        await self.con.execute('''
            CREATE FUNCTION call39(
                foo: str
            ) -> str
                USING (foo);
        ''')

        await self.assert_query_result(
            r'''SELECT call39("identity");''',
            ['identity'],
        )

    async def test_edgeql_calls_40(self):
        await self.con.execute('''
            CREATE TYPE Rectangle {
                CREATE REQUIRED PROPERTY width -> int64;
                CREATE REQUIRED PROPERTY height -> int64;
            };

            INSERT Rectangle { width := 2, height := 3 };

            CREATE FUNCTION call40(
                r: Rectangle
            ) -> int64
                USING (r.width * r.height);
        ''')

        await self.assert_query_result(
            r'''SELECT call40(Rectangle);''',
            [6],
        )

    async def test_edgeql_calls_41(self):
        await self.con.execute('''
            CREATE FUNCTION call41(
                a: int64, b: int64
            ) -> SET OF int64
                USING ({a, b});
        ''')

        await self.assert_query_result(
            r'''SELECT call41(1, 2);''',
            [1, 2],
        )

    async def test_edgeql_calls_42(self):
        await self.con.execute('''
            CREATE FUNCTION call42(
                a: int64, b: int64
            ) -> SET OF tuple<int64, str>
                USING ({(a, '1'), (b, '2')});
        ''')

        await self.assert_query_result(
            r'''SELECT call42(1, 2);''',
            [[1, '1'], [2, '2']],
        )

        await self.assert_query_result(
            r'''SELECT call42(1, 2).0;''',
            [1, 2],
        )

    async def test_edgeql_calls_obj_01(self):
        await self.con.execute("""
            CREATE TYPE Shape;
            CREATE TYPE FlatShape;
            CREATE TYPE Rectangle EXTENDING FlatShape {
                CREATE REQUIRED PROPERTY w -> float64;
                CREATE REQUIRED PROPERTY h -> float64;
            };

            CREATE TYPE Circle EXTENDING FlatShape {
                CREATE REQUIRED PROPERTY r -> float64;
            };

            # Use -1 as the error indicator, as we don't have the means
            # to raise errors directly yet.
            CREATE FUNCTION area(s: FlatShape) -> float64 USING (-1);
            CREATE FUNCTION area(s: Rectangle) -> float64 USING (s.w * s.h);
            CREATE FUNCTION area(s: Circle) -> float64 USING (s.r ^ 2 * 3.14);

            INSERT Rectangle { w := 10, h := 20 };
            INSERT Circle { r := 10 };
        """)

        # Check for "manual" abstract function dispatch, where the top
        # function is defined to explicitly return an error condition.
        await self.assert_query_result(
            r"""
                SELECT FlatShape {
                    tn := .__type__.name,
                    area := area(FlatShape),
                }
                ORDER BY .tn
            """,
            [{
                "tn": "default::Circle",
                "area": 314.0,
            }, {
                "tn": "default::Rectangle",
                "area": 200.0,
            }]
        )

        # Non-polymorphic calls should work also.
        await self.assert_query_result(
            r"""
                SELECT area(Circle);
            """,
            [314.0],
        )

        await self.assert_query_result(
            r"""
                SELECT area(Rectangle);
            """,
            [200.0],
        )

        # Test that opaque object sources work as well.
        await self.assert_query_result(
            r"""
                WITH r := (Rectangle, [Rectangle])
                SELECT (area(r.0), area(r.1[0]))
            """,
            [[200.0, 200.0]],
        )

        # The top parent does _not_ have a definition of area, so
        # calling it on it is still an error (even if there are definitions
        # for all subtypes).
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'function "area\(.*: default::Shape\)" does not exist',
        ):
            await self.con.execute("SELECT area(Shape)")

    async def test_edgeql_calls_obj_02(self):
        # Similar to test_edgeql_calls_obj_01, but
        # the functions are set-returning.
        await self.con.execute("""
            CREATE TYPE Shape;
            CREATE TYPE FlatShape;
            CREATE TYPE Rectangle EXTENDING FlatShape {
                CREATE PROPERTY w -> float64;
                CREATE PROPERTY h -> float64;
            };

            CREATE TYPE Circle EXTENDING FlatShape {
                CREATE PROPERTY r -> float64;
            };

            # Use -1 as the error indicator, as we don't have the means
            # to raise errors directly yet.
            CREATE FUNCTION dimensions(s: FlatShape) -> SET OF float64
                USING (-1);
            CREATE FUNCTION dimensions(s: Rectangle) -> SET OF float64
                USING ({s.w, s.h});
            CREATE FUNCTION dimensions(s: Circle) -> SET OF float64
                USING (s.r);

            INSERT Rectangle { w := 10, h := 20 };
            INSERT Circle { r := 5 };
        """)

        # Check for "manual" abstract function dispatch, where the top
        # function is defined to explicitly return an error condition.
        await self.assert_query_result(
            r"""
                SELECT FlatShape {
                    tn := .__type__.name,
                    dimensions := dimensions(FlatShape),
                }
                ORDER BY .tn
            """,
            [{
                "tn": "default::Circle",
                "dimensions": [5],
            }, {
                "tn": "default::Rectangle",
                "dimensions": [10, 20],
            }]
        )

        # Non-polymorphic calls should work also.
        await self.assert_query_result(
            r"""
                SELECT dimensions(Circle);
            """,
            [5],
        )

        await self.assert_query_result(
            r"""
                SELECT dimensions(Rectangle);
            """,
            [10, 20],
        )

    async def test_edgeql_calls_obj_03(self):
        await self.con.execute("""
            CREATE TYPE Person {
                CREATE REQUIRED PROPERTY name -> str;
            };
            CREATE FUNCTION fight(one: Person, two: Person) -> str
                USING (one.name ++ " fights " ++ two.name);
            CREATE FUNCTION fight(one: str, two: str) -> str
                USING (one ++ " fights " ++ two);
            CREATE FUNCTION fight(one: Person, two: str) -> str
                USING (one.name ++ " fights " ++ two);
            CREATE FUNCTION fight(one: str, two: Person) -> str
                USING (one ++ " fights " ++ two.name);
            CREATE FUNCTION fight(names: array<str>) -> str
                USING (array_join(names, " fights "));

            INSERT Person { name := "Sub-Zero" };
            INSERT Person { name := "Scorpion" };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    Scorpion := (SELECT Person FILTER .name = "Scorpion"),
                    SubZero := (SELECT Person FILTER .name = "Sub-Zero"),
                SELECT
                    fight(Scorpion, SubZero);
            """,
            ["Scorpion fights Sub-Zero"],
        )

        await self.assert_query_result(
            r"""
                WITH
                    Scorpion := (SELECT Person FILTER .name = "Scorpion"),
                    SubZero := (SELECT Person FILTER .name = "Sub-Zero"),
                SELECT
                    fight(Scorpion.name, SubZero.name);
            """,
            ["Scorpion fights Sub-Zero"],
        )

        await self.assert_query_result(
            r"""
                WITH
                    Scorpion := (SELECT Person FILTER .name = "Scorpion"),
                    SubZero := (SELECT Person FILTER .name = "Sub-Zero"),
                SELECT
                    fight(Scorpion.name, SubZero);
            """,
            ["Scorpion fights Sub-Zero"],
        )

        await self.assert_query_result(
            r"""
                WITH
                    Scorpion := (SELECT Person FILTER .name = "Scorpion"),
                    SubZero := (SELECT Person FILTER .name = "Sub-Zero"),
                SELECT
                    fight(Scorpion, SubZero.name);
            """,
            ["Scorpion fights Sub-Zero"],
        )

        await self.assert_query_result(
            r"""
                WITH
                    Scorpion := (SELECT Person FILTER .name = "Scorpion"),
                    SubZero := (SELECT Person FILTER .name = "Sub-Zero"),
                SELECT
                    fight([Scorpion.name, SubZero.name]);
            """,
            ["Scorpion fights Sub-Zero"],
        )

        await self.con.execute("DROP FUNCTION fight(one: Person, two: Person)")

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'function "fight\(.*default::Person.*\)" does not exist',
        ):
            await self.con.execute(
                r"""
                WITH
                    Scorpion := (SELECT Person FILTER .name = "Scorpion"),
                    SubZero := (SELECT Person FILTER .name = "Sub-Zero"),
                SELECT
                    fight(Scorpion, SubZero);SELECT area(Shape)
                """,
            )

    async def test_edgeql_calls_obj_04(self):
        await self.con.execute("""
            CREATE FUNCTION thing(s: schema::Constraint) -> OPTIONAL str
                USING (s.name ++ s.expr);

            CREATE FUNCTION frob(s: schema::Object) -> str
                USING ("ahhhh");
            CREATE FUNCTION frob(s: schema::Constraint) -> OPTIONAL str
                USING (s.name ++ s.expr);
            CREATE FUNCTION frob(s: schema::Pointer) -> OPTIONAL str
                USING (s.name ++ <str>s.required);
        """)

    async def test_edgeql_calls_obj_05(self):
        await self.con.execute("""
            CREATE TYPE Ghost {
                CREATE PROPERTY name -> str;
            };

            CREATE FUNCTION boo(s: Ghost) -> set of str
                USING ("oh my, " ++ s.name ++ " scared me!");

            INSERT Ghost { name := 'Casper' };
        """)

        await self.assert_query_result(
            "SELECT boo((SELECT Ghost))",
            ["oh my, Casper scared me!"],
        )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r'newly created or updated objects cannot be passed to functions',
        ):
            await self.con.execute(
                r"SELECT boo((UPDATE Ghost SET { name := 'Tom' }))",
            )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r'newly created or updated objects cannot be passed to functions',
        ):
            await self.con.execute(
                r"SELECT boo((INSERT Ghost { name := 'Jack' }));",
            )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r'newly created or updated objects cannot be passed to functions',
        ):
            await self.con.execute(
                r"""
                WITH friendly := (INSERT Ghost { name := 'Jack' })
                SELECT boo(friendly);
                """,
            )

    async def test_edgeql_call_builtin_obj(self):
        await self.con.execute(
            r"""
                CREATE FUNCTION get_obj(name: str) ->
                  SET OF schema::Object USING (
                    SELECT schema::Object FILTER .name = name);
            """,
        )

        res = await self.con._fetchall("""
            SELECT get_obj('std::BaseObject')
        """, __typenames__=True)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].__tname__, "schema::ObjectType")
