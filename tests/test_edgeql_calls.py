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


import unittest

from edb.server import _testbase as tb
from edb.client import exceptions as exc


class TestEdgeQLFuncCalls(tb.QueryTestCase):

    async def test_edgeql_calls_01(self):
        await self.con.execute('''
            CREATE FUNCTION test::call1(
                s: str,
                VARIADIC a: int64,
                NAMED ONLY suffix: str = '-suf',
                NAMED ONLY prefix: str = 'pref-'
            ) -> std::str
                FROM EdgeQL $$
                    SELECT prefix + s + <str>sum(array_unpack(a)) + suffix;
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call1('-');
                SELECT test::call1('-', suffix := 's1');
                SELECT test::call1('-', prefix := 'p1');
                SELECT test::call1('-', suffix := 's1', prefix := 'p1');
                SELECT test::call1('-', 1);
                SELECT test::call1('-', 1, suffix := 's1');
                SELECT test::call1('-', 1, prefix := 'p1');
                SELECT test::call1('-', 1, 2, 3, 4, 5);
                SELECT test::call1('-', 1, 2, 3, 4, 5, suffix := 's1');
                SELECT test::call1('-', 1, 2, 3, 4, 5, prefix := 'p1');
                SELECT test::call1('-', 1, 2, 3, 4, 5, prefix := 'p1',
                                   suffix := 'aaa');
            ''', [
                ['pref--0-suf'],
                ['pref--0s1'],
                ['p1-0-suf'],
                ['p1-0s1'],
                ['pref--1-suf'],
                ['pref--1s1'],
                ['p1-1-suf'],
                ['pref--15-suf'],
                ['pref--15s1'],
                ['p1-15-suf'],
                ['p1-15aaa'],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call1(
                    s: str,
                    VARIADIC a: int64,
                    NAMED ONLY suffix: str,
                    NAMED ONLY prefix: str);
            ''')

    async def test_edgeql_calls_02(self):
        await self.con.execute('''
            CREATE FUNCTION test::call2(
                VARIADIC a: anytype
            ) -> std::str
                FROM EdgeQL $$
                    SELECT '=' + <str>len(a) + '='
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call2('a', 'b');
                SELECT test::call2(4, 2, 0);
            ''', [
                ['=2='],
                ['=3='],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call2(VARIADIC a: anytype);
            ''')

    async def test_edgeql_calls_03(self):
        await self.con.execute('''
            CREATE FUNCTION test::call3(
                a: int32,
                NAMED ONLY b: int32
            ) -> int32
                FROM EdgeQL $$
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

        try:
            for c in cases:
                with self.assertRaisesRegex(
                        exc.EdgeQLError,
                        r'could not find a function variant'):
                    await self.con.execute(c)
        finally:
            await self.con.execute('''
                DROP FUNCTION test::call3(
                    a: int32,
                    NAMED ONLY b: int32
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_04(self):
        await self.con.execute('''
            CREATE FUNCTION test::call4(
                a: int32,
                NAMED ONLY b: array<anytype> = []
            ) -> int32
                FROM EdgeQL $$
                    SELECT a + len(b)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call4(100);
                SELECT test::call4(100, b := <int32>[]);
                SELECT test::call4(100, b := [1, 2]);
                SELECT test::call4(100, b := ['a', 'b']);
            ''', [
                [100],
                [100],
                [102],
                [102]
            ])
        finally:
            await self.con.execute('''
                DROP FUNCTION test::call4(
                    a: int32,
                    NAMED ONLY b: array<anytype>
                );
            ''')

    async def test_edgeql_calls_05(self):
        await self.con.execute('''
            CREATE FUNCTION test::call5(
                a: int64,
                NAMED ONLY b: OPTIONAL int64 = <int64>{}
            ) -> int64
                FROM EdgeQL $$
                    SELECT a + b ?? -100
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call5(1);
                SELECT test::call5(<int32>2);
                SELECT test::call5(1, b := 20);
                SELECT test::call5(1, b := <int16>10);
            ''', [
                [-99],
                [-98],
                [21],
                [11],
            ])
        finally:
            await self.con.execute('''
                DROP FUNCTION test::call5(
                    a: int64,
                    NAMED ONLY b: OPTIONAL int64 = <int64>{}
                );
            ''')

    async def test_edgeql_calls_06(self):
        await self.con.execute('''
            CREATE FUNCTION test::call6(
                VARIADIC a: int64
            ) -> int64
                FROM EdgeQL $$
                    SELECT <int64>sum(array_unpack(a));;
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call6();
                SELECT test::call6(1, 2, 3);
                SELECT test::call6(<int16>1, <int32>2, 3);
            ''', [
                [0],
                [6],
                [6],
            ])
        finally:
            await self.con.execute('''
                DROP FUNCTION test::call6(
                    VARIADIC a: int64
                );
            ''')

    async def test_edgeql_calls_07(self):
        await self.con.execute('''
            CREATE FUNCTION test::call7(
                a: int64 = 1,
                b: int64 = 2,
                c: int64 = 3,
                NAMED ONLY d: int64 = 4,
                NAMED ONLY e: int64 = 5
            ) -> array<int64>
                FROM EdgeQL $$
                    SELECT [a, b, c, d, e]
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call7();
                SELECT test::call7(e := 100);
                SELECT test::call7(d := 200);
                SELECT test::call7(20, 30, d := 200);
                SELECT test::call7(20, 30, e := 42, d := 200);
                SELECT test::call7(20, 30, 1, d := 200, e := 42);
            ''', [
                [[1, 2, 3, 4, 5]],
                [[1, 2, 3, 4, 100]],
                [[1, 2, 3, 200, 5]],
                [[20, 30, 3, 200, 5]],
                [[20, 30, 3, 200, 42]],
                [[20, 30, 1, 200, 42]],
            ])

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
                        exc.EdgeQLError,
                        r'could not find a function variant'):
                    await self.con.execute(c)

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call7(
                    a: int64 = 1,
                    b: int64 = 2,
                    c: int64 = 3,
                    NAMED ONLY d: int64 = 4,
                    NAMED ONLY e: int64 = 5
                );
            ''')

    async def test_edgeql_calls_08(self):
        await self.con.execute('''
            CREATE FUNCTION test::call8(
                a: int64 = 1,
                NAMED ONLY b: int64 = 2
            ) -> int64
                FROM EdgeQL $$
                    SELECT a + b
                $$;

            CREATE FUNCTION test::call8(
                a: float64 = 1.0,
                NAMED ONLY b: int64 = 2
            ) -> int64
                FROM EdgeQL $$
                    SELECT 1000 + <int64>a + b
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call8(1);
                SELECT test::call8(1.0);
                SELECT test::call8(1, b := 10);
                SELECT test::call8(1.0, b := 10);
            ''', [
                [3],
                [1003],
                [11],
                [1011],
            ])

            with self.assertRaisesRegex(
                    exc.EdgeQLError,
                    r'function test::call8 is not unique'):
                await self.con.execute('SELECT test::call8();')

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call8(
                    a: int64,
                    NAMED ONLY b: int64
                );

                DROP FUNCTION test::call8(
                    a: float64,
                    NAMED ONLY b: int64
                );
            ''')

    async def test_edgeql_calls_09(self):
        await self.assert_query_result(r'''
            SELECT sum({1, 2, 3});
            SELECT sum({<int32>1, 2, 3});
            SELECT sum({<float32>1, 2, 3});

            SELECT sum({<float32>1, <int32>2, 3});
            SELECT sum({<int16>1, <int32>2, <decimal>3});

            SELECT sum({1.1, 2.2, 3});
        ''', [
            {6},
            {6},
            {6},

            {6},
            {6},

            {6.3},
        ])

    async def test_edgeql_calls_10(self):
        await self.assert_query_result(r'''
            SELECT sum({1, 2, 3}).__type__.name;
            SELECT sum({<int32>1, 2, 3}).__type__.name;
            SELECT sum({<float32>1, 2, 3}).__type__.name;

            SELECT sum({<float32>1, <int32>2, 3}).__type__.name;
            SELECT sum({<int16>1, <int32>2, <decimal>3}).__type__.name;

            SELECT sum({<int16>1, 2, <decimal>3}).__type__.name;
            SELECT sum({1, <float32>2.1, <float64>3}).__type__.name;
            SELECT sum({1.1, 2.2, 3.3}).__type__.name;

            SELECT sum({<float32>1, <int32>2, <float32>3}).__type__.name;
            SELECT sum({<float32>1, <float32>2, <float32>3}).__type__.name;
            SELECT sum({1.1, 2.2, 3}).__type__.name;
        ''', [
            {'std::decimal'},
            {'std::decimal'},
            {'std::float64'},

            {'std::float64'},
            {'std::decimal'},

            {'std::decimal'},
            {'std::float64'},
            {'std::float64'},

            {'std::float64'},
            {'std::float32'},
            {'std::float64'},
        ])

    async def test_edgeql_calls_11(self):
        await self.con.execute('''
            CREATE FUNCTION test::call11(
                a: array<int32>
            ) -> decimal
                FROM EdgeQL $$
                    SELECT sum(array_unpack(a))
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call11([<int16>1, <int16>22]);
                SELECT test::call11([<int16>1, <int32>23]);
                SELECT test::call11([<int32>1, <int32>24]);
            ''', [
                [23],
                [24],
                [25],
            ])

            cases = [
                'SELECT test::call11([<int32>1, 1.1]);',
                'SELECT test::call11([<int32>1, <float32>1]);',
                'SELECT test::call11([1, 2]);',
            ]

            for c in cases:
                with self.assertRaisesRegex(
                        exc.EdgeQLError,
                        r'could not find a function variant'):
                    await self.con.execute(c)

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call11(
                    a: array<int32>
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_12(self):
        await self.con.execute('''
            CREATE FUNCTION test::call12(
                a: anyint
            ) -> int64
                FROM EdgeQL $$
                    SELECT <int64>a + 100
                $$;

            CREATE FUNCTION test::call12(
                a: int64
            ) -> int64
                FROM EdgeQL $$
                    SELECT <int64>a + 1
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call12(<int32>1);
                SELECT test::call12(1);
            ''', [
                [101],
                [2],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call12(
                    a: anyint
                );

                DROP FUNCTION test::call12(
                    a: int64
                );
            ''')

    async def test_edgeql_calls_13(self):
        await self.con.execute('''
            CREATE FUNCTION test::call13(
                a: anytype
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call13('aaa');
                SELECT test::call13(b'aaaa');
                SELECT test::call13([1, 2, 3, 4, 5]);
                SELECT test::call13(['a', 'b']);
            ''', [
                [3],
                [4],
                [5],
                [2],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call13(
                    a: anytype
                );
            ''')

    async def test_edgeql_calls_14(self):
        await self.con.execute('''
            CREATE FUNCTION test::call14(
                a: anytype
            ) -> array<anytype>
                FROM EdgeQL $$
                    SELECT [a]
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call14('aaa');
                SELECT test::call14(b'aaaa');
                SELECT test::call14(1);
            ''', [
                [['aaa']],
                [[r'\x61616161']],
                [[1]],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call14(
                    a: anytype
                );
            ''')

    async def test_edgeql_calls_15(self):
        await self.con.execute('''
            CREATE FUNCTION test::call15(
                a: anytype
            ) -> array<anytype>
                FROM EdgeQL $$
                    SELECT [a, a, a]
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call15('aaa');
                SELECT test::call15(1);
            ''', [
                [['aaa', 'aaa', 'aaa']],
                [[1, 1, 1]],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call15(
                    a: anytype
                );
            ''')

    async def test_edgeql_calls_16(self):
        await self.con.execute('''
            CREATE FUNCTION test::call16(
                a: array<anytype>,
                idx: int64
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a[idx]
                $$;

            CREATE FUNCTION test::call16(
                a: array<anytype>,
                idx: str
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a[<int64>idx + 1]
                $$;

            CREATE FUNCTION test::call16(
                a: anyscalar,
                idx: int64
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a[idx]
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call16([1, 2, 3], 1);
                SELECT test::call16(['a', 'b', 'c'], 1);

                SELECT test::call16([1, 2, 3], '1');
                SELECT test::call16(['a', 'b', 'c'], '1');

                SELECT test::call16('xyz', 1);
            ''', [
                [2],
                ['b'],

                [3],
                ['c'],

                ['y'],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call16(
                    a: array<anytype>,
                    idx: int64
                );
                DROP FUNCTION test::call16(
                    a: array<anytype>,
                    idx: str
                );
                DROP FUNCTION test::call16(
                    a: anyscalar,
                    idx: int64
                );
            ''')

    async def test_edgeql_calls_17(self):
        await self.con.execute('''
            CREATE FUNCTION test::call17(
                a: anytype
            ) -> array<anytype>
                FROM EdgeQL $$
                    SELECT [a, a, a]
                $$;

            CREATE FUNCTION test::call17(
                a: str
            ) -> array<str>
                FROM EdgeQL $$
                    SELECT ['!!!!', a, '!!!!']
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call17(2);
                SELECT test::call17('aaa');
            ''', [
                [[2, 2, 2]],
                [['!!!!', 'aaa', '!!!!']],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call17(
                    a: anytype
                );
                DROP FUNCTION test::call17(
                    a: str
                );
            ''')

    async def test_edgeql_calls_18(self):
        await self.con.execute('''
            CREATE FUNCTION test::call18(
                VARIADIC a: anytype
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call18(2);
                SELECT test::call18(1, 2, 3);
                SELECT test::call18('a', 'b');
            ''', [
                [1],
                [3],
                [2],
            ])

            with self.assertRaisesRegex(
                    exc.EdgeQLError,
                    r'could not find a function variant'):

                await self.con.execute('SELECT test::call18(1, 2, "a");')

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call18(
                    VARIADIC a: anytype
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_19(self):
        # XXX: Postgres raises the following error for this:
        #    return type record[] is not supported for SQL functions

        await self.con.execute('''
            CREATE FUNCTION test::call19(
                a: anytype
            ) -> array<anytype>
                FROM EdgeQL $$
                    SELECT [a]
                $$;
        ''')

        try:
            await self.con.execute('SELECT test::call19((1,2));')

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call19(
                    a: anytype
                );
            ''')

    async def test_edgeql_calls_20(self):
        await self.con.execute('''
            CREATE FUNCTION test::call20_1(
                a: anytype, b: anytype
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a + b
                $$;

            CREATE FUNCTION test::call20_2(
                a: anytype, b: anytype
            ) -> bool
                FROM EdgeQL $$
                    SELECT a < b
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call20_1(10, 20);

                SELECT test::call20_2(1, 2);
                SELECT test::call20_2('b', 'a');
            ''', [
                [30],

                [True],
                [False],
            ])

            with self.assertRaisesRegex(
                    exc.EdgeQLError,
                    r'could not find a function variant'):

                await self.con.execute('SELECT test::call20_1(1, "1");')

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call20_1(
                    a: anytype, b: anytype
                );
                DROP FUNCTION test::call20_2(
                    a: anytype, b: anytype
                );
            ''')

    async def test_edgeql_calls_21(self):
        await self.con.execute('''
            CREATE FUNCTION test::call21(
                a: array<anytype>
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call21(<array<str>>[]);
                SELECT test::call21([1,2]);
                SELECT test::call21(['a', 'b', 'c']);
                SELECT test::call21([(1, 2), (2, 3), (3, 4), (4, 5)]);
            ''', [
                [0],
                [2],
                [3],
                [4],
            ])
        finally:
            await self.con.execute('''
                DROP FUNCTION test::call21(
                    a: array<anytype>
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_22(self):
        # XXX Requires a polymorphic "+" operator

        await self.con.execute('''
            CREATE FUNCTION test::call22(
                a: anytype, b: anytype
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a + b
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call22('a', 'b');
            ''', [
                ['ab'],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call22(
                    a: anytype, b: anytype
                );
            ''')

    async def test_edgeql_calls_23(self):
        await self.con.execute('''
            CREATE FUNCTION test::call23(
                a: anytype,
                idx: int64
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a[idx]
                $$;

            CREATE FUNCTION test::call23(
                a: anytype,
                idx: int32
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a[-idx:]
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call23('abcde', 2);
                SELECT test::call23(to_json('[{"a":"b"}]'), 0);
                SELECT test::call23('abcde', <int32>2);
            ''', [
                ['c'],
                [{"a": "b"}],
                ['de'],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call23(
                    a: anytype,
                    idx: int64
                );
                DROP FUNCTION test::call23(
                    a: anytype,
                    idx: int32
                );
            ''')

    async def test_edgeql_calls_24(self):
        await self.con.execute('''
            CREATE FUNCTION test::call24() -> str
                FROM EdgeQL $$
                    SELECT 'ab' + 'cd'
                $$;

            CREATE FUNCTION test::call24(
                a: str
            ) -> str
                FROM EdgeQL $$
                    SELECT a + '!'
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                select test::call24();
                select test::call24('aaa');
            ''', [
                ['abcd'],
                ['aaa!'],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call24();
                DROP FUNCTION test::call24(a: str);
            ''')

    async def test_edgeql_calls_25(self):
        await self.con.execute('''
            CREATE FUNCTION test::call25(
                a: anyscalar
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call25('aaa');
                SELECT test::call25(b'');
            ''', [
                [3],
                [0],
            ])

            with self.assertRaisesRegex(
                    exc.EdgeQLError,
                    r'could not find a function variant'):

                await self.con.execute('SELECT test::call25([1]);')

            with self.assertRaisesRegex(
                    exc.EdgeQLError,
                    r'could not find a function variant'):

                await self.con.execute('SELECT test::call25([(1, 2)]);')

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call25(
                    a: anyscalar
                );
            ''')

    async def test_edgeql_calls_26(self):
        await self.con.execute('''
            CREATE FUNCTION test::call26(
                a: array<anyscalar>
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call26(['aaa']);
                SELECT test::call26([b'', b'aa']);
            ''', [
                [1],
                [2],
            ])

            with self.assertRaisesRegex(
                    exc.EdgeQLError,
                    r'could not find a function variant'):

                await self.con.execute('SELECT test::call26([(1, 2)]);')

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call26(
                    a: array<anyscalar>
                );
            ''')

    async def test_edgeql_calls_27(self):
        await self.con.execute('''
            CREATE FUNCTION test::call27(
                a: array<anyint>
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call27([<int32>1, <int32>2]);
                SELECT test::call27([1, 2, 3]);
            ''', [
                [2],
                [3],
            ])

            cases = [
                "SELECT test::call27(['aaa']);",
                "SELECT test::call27([b'', b'aa']);",
                "SELECT test::call27([1.0, 2.1]);",
                "SELECT test::call27([('a',), ('b',)]);",
            ]

            for c in cases:
                with self.assertRaisesRegex(
                        exc.EdgeQLError,
                        r'could not find a function variant'):

                    await self.con.execute(c)

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call27(
                    a: array<anyint>
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_28(self):
        await self.con.execute('''
            CREATE FUNCTION test::call28(
                a: array<anyint>
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a)
                $$;

            CREATE FUNCTION test::call28(
                a: array<anyscalar>
            ) -> int64
                FROM EdgeQL $$
                    SELECT len(a) + 1000
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call28([<int32>1, <int32>2]);
                SELECT test::call28([1, 2, 3]);

                SELECT test::call28(['a', 'b']);
            ''', [
                [2],
                [3],

                [1002],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call28(
                    a: array<anyint>
                );
                DROP FUNCTION test::call28(
                    a: array<anyscalar>
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_29(self):
        await self.con.execute('''
            CREATE FUNCTION test::call29(
                a: anyint
            ) -> anyint
                FROM EdgeQL $$
                    SELECT a + 1
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call29(10);
            ''', [
                [11],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call29(
                    a: anyint
                );
            ''')

    async def test_edgeql_calls_30(self):
        await self.con.execute('''
            CREATE FUNCTION test::call30(
                a: anyint
            ) -> int64
                FROM EdgeQL $$
                    SELECT <int64>a + 100
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call30(10);
                SELECT test::call30(<int32>20);
            ''', [
                [110],
                [120],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call30(
                    a: anyint
                );
            ''')

    async def test_edgeql_calls_31(self):
        await self.con.execute('''
            CREATE FUNCTION test::call31(
                a: anytype
            ) -> anytype
                FROM EdgeQL $$
                    SELECT a
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call31(10);
                SELECT test::call31('aa');

                SELECT test::call31([1, 2]);
                SELECT test::call31([1, 2])[0];

                SELECT test::call31((a:=1001, b:=1002)).a;
                SELECT test::call31((a:=1001, b:=1002)).1;

                SELECT test::call31((a:=['a', 'b'], b:=['x', 'y'])).1;
                SELECT test::call31((a:=['a', 'b'], b:=['x', 'y'])).a[1];

                SELECT test::call31((a:=1001, b:=1002));

                SELECT test::call31((a:=[(x:=1)])).a[0].x;
                SELECT test::call31((a:=[(x:=1)])).0[0].x;
                SELECT test::call31((a:=[(x:=1)])).0[0].0;
                SELECT test::call31((a:=[(x:=1)])).a[0];
            ''', [
                [10],
                ['aa'],

                [[1, 2]],
                [1],

                [1001],
                [1002],

                [['x', 'y']],
                ['b'],

                [{"a": 1001, "b": 1002}],

                [1],
                [1],
                [1],
                [{"x": 1}],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call31(
                    a: anytype
                );
            ''')
