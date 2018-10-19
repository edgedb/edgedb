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


import os.path
import unittest

from edb.server import _testbase as tb
from edb.client import exceptions as exc


class TestEdgeQLFuncCalls(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.eql')

    async def test_edgeql_calls_01(self):
        await self.con.execute('''
            CREATE FUNCTION test::call1(
                $s: str,
                VARIADIC $a: int64,
                NAMED ONLY $suffix: str = '-suf',
                NAMED ONLY $prefix: str = 'pref-'
            ) -> std::str
                FROM EdgeQL $$
                    SELECT $prefix + $s + <str>sum(array_unpack($a)) + $suffix
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call1('-');
                SELECT test::call1('-', $suffix := 's1');
                SELECT test::call1('-', $prefix := 'p1');
                SELECT test::call1('-', $suffix := 's1', $prefix := 'p1');
                SELECT test::call1('-', 1);
                SELECT test::call1('-', 1, $suffix := 's1');
                SELECT test::call1('-', 1, $prefix := 'p1');
                SELECT test::call1('-', 1, 2, 3, 4, 5);
                SELECT test::call1('-', 1, 2, 3, 4, 5, $suffix := 's1');
                SELECT test::call1('-', 1, 2, 3, 4, 5, $prefix := 'p1');
                SELECT test::call1('-', 1, 2, 3, 4, 5, $prefix := 'p1',
                                   $suffix := 'aaa');
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
                    $s: str,
                    VARIADIC $a: int64,
                    NAMED ONLY $suffix: str,
                    NAMED ONLY $prefix: str);
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_02(self):
        await self.con.execute('''
            CREATE FUNCTION test::call2(
                VARIADIC $a: any
            ) -> std::str
                FROM EdgeQL $$
                    SELECT '=' + <str>len($a) + '='
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call1('a', 'b');
                SELECT test::call1(4, 2, 0);
            ''', [
                ['=0='],
                ['=2='],
                ['=3='],
            ])

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call2(VARIADIC $a: any);
            ''')

    async def test_edgeql_calls_03(self):
        await self.con.execute('''
            CREATE FUNCTION test::call3(
                $a: int32,
                NAMED ONLY $b: int32
            ) -> int32
                FROM EdgeQL $$
                    SELECT $a + $b
                $$;
        ''')

        cases = [
            'SELECT test::call3(1);',
            'SELECT test::call3(1, 2);',
            'SELECT test::call3(1, 2, 3);',
            'SELECT test::call3($b := 1);',
            'SELECT test::call3(1, 2, $b := 1);',
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
                    $a: int32,
                    NAMED ONLY $b: int32
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_04(self):
        await self.con.execute('''
            CREATE FUNCTION test::call4(
                $a: int32,
                NAMED ONLY $b: array<any> = []
            ) -> int32
                FROM EdgeQL $$
                    SELECT $a + len($b)
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call4(100);
                SELECT test::call4(100, $b := <int32>[]);
                SELECT test::call4(100, $b := [1, 2]);
                SELECT test::call4(100, $b := ['a', 'b']);
            ''', [
                [100],
                [100],
                [102],
                [102]
            ])
        finally:
            await self.con.execute('''
                DROP FUNCTION test::call4(
                    $a: int32,
                    NAMED ONLY $b: array<any>
                );
            ''')

    async def test_edgeql_calls_05(self):
        await self.con.execute('''
            CREATE FUNCTION test::call5(
                $a: int64,
                NAMED ONLY $b: OPTIONAL int64 = <int64>{}
            ) -> int64
                FROM EdgeQL $$
                    SELECT $a + $b ?? -100
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call5(1);
                SELECT test::call5(<int32>2);
                SELECT test::call5(1, $b := 20);
                SELECT test::call5(1, $b := <int16>10);
            ''', [
                [-99],
                [-98],
                [21],
                [11],
            ])
        finally:
            await self.con.execute('''
                DROP FUNCTION test::call5(
                    $a: int64,
                    NAMED ONLY $b: OPTIONAL int64 = <int64>{}
                );
            ''')

    async def test_edgeql_calls_06(self):
        await self.con.execute('''
            CREATE FUNCTION test::call6(
                VARIADIC $a: int64
            ) -> int64
                FROM EdgeQL $$
                    SELECT <int64>sum(array_unpack($a))
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
                    VARIADIC $a: int64
                );
            ''')

    async def test_edgeql_calls_07(self):
        await self.con.execute('''
            CREATE FUNCTION test::call7(
                $a: int64 = 1,
                $b: int64 = 2,
                $c: int64 = 3,
                NAMED ONLY $d: int64 = 4,
                NAMED ONLY $e: int64 = 5
            ) -> array<int64>
                FROM EdgeQL $$
                    SELECT [$a, $b, $c, $d, $e]
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call7();
                SELECT test::call7($e := 100);
                SELECT test::call7($d := 200);
                SELECT test::call7(20, 30, $d := 200);
                SELECT test::call7(20, 30, $e := 42, $d := 200);
                SELECT test::call7(20, 30, 1, $d := 200, $e := 42);
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
                'SELECT test::call7(1, $z := 1);'
                'SELECT test::call7(1, 2, 3, $z := 1);'
                'SELECT test::call7(1, 2, 3, 4, $z := 1);'
                'SELECT test::call7(1, 2, 3, $d := 1, $z := 10);'
                'SELECT test::call7(1, 2, 3, $d := 1, $e := 2, $z := 10);'
            ]

            for c in cases:
                with self.assertRaisesRegex(
                        exc.EdgeQLError,
                        r'could not find a function variant'):
                    await self.con.execute(c)

        finally:
            await self.con.execute('''
                DROP FUNCTION test::call7(
                    $a: int64 = 1,
                    $b: int64 = 2,
                    $c: int64 = 3,
                    NAMED ONLY $d: int64 = 4,
                    NAMED ONLY $e: int64 = 5
                );
            ''')

    async def test_edgeql_calls_08(self):
        await self.con.execute('''
            CREATE FUNCTION test::call8(
                $a: int64 = 1,
                NAMED ONLY $b: int64 = 2
            ) -> int64
                FROM EdgeQL $$
                    SELECT $a + $b
                $$;

            CREATE FUNCTION test::call8(
                $a: float64 = 1.0,
                NAMED ONLY $b: int64 = 2
            ) -> int64
                FROM EdgeQL $$
                    SELECT 1000 + <int64>$a + $b
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::call8(1);
                SELECT test::call8(1.0);
                SELECT test::call8(1, $b := 10);
                SELECT test::call8(1.0, $b := 10);
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
                    $a: int64,
                    NAMED ONLY $b: int64
                );

                DROP FUNCTION test::call8(
                    $a: float64,
                    NAMED ONLY $b: int64
                );
            ''')

    async def test_edgeql_calls_09(self):
        await self.assert_query_result(r'''
            SELECT sum({1, 2, 3});
            SELECT sum({<int32>1, 2, 3});
            SELECT sum({<float32>1, 2, 3});

            SELECT sum({<float32>1, <int32>2, 3});
            SELECT sum({<float32>1, <int32>2, <decimal>3});
            SELECT sum({<int16>1, <int32>2, <decimal>3});

            SELECT sum({1.1, 2.2, 3});
        ''', [
            {6},
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
            SELECT sum({<float32>1, <int32>2, <decimal>3}).__type__.name;
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
                $a: array<int32>
            ) -> decimal
                FROM EdgeQL $$
                    SELECT sum(array_unpack($a))
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
                    $a: array<int32>
                );
            ''')

    @unittest.expectedFailure
    async def test_edgeql_calls_12(self):
        await self.con.execute('''
            CREATE FUNCTION test::call12(
                $a: anyint
            ) -> int64
                FROM EdgeQL $$
                    SELECT <int64>$a + 100
                $$;

            CREATE FUNCTION test::call12(
                $a: int64
            ) -> int64
                FROM EdgeQL $$
                    SELECT <int64>$a + 1
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
                    $a: anyint
                );

                DROP FUNCTION test::call12(
                    $a: int64
                );
            ''')
