#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
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

import edgedb

from edb.testbase import server as tb


class TestEdgeQLFunctionsInline(tb.QueryTestCase):
    NO_FACTOR = True

    async def test_edgeql_functions_inline_basic_01(self):
        await self.con.execute('''
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_02(self):
        await self.con.execute('''
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (x * x + 2 * x + 1);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [4],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [4, 9, 16],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [4, 9, 16],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_03(self):
        await self.con.execute('''
            create function foo(x: int64, y: int64) -> int64 {
                set is_inlined := true;
                using (x + y);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, 1)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, {10, 20, 30})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, y)'
            '    )'
            ')',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_04(self):
        await self.con.execute('''
            create function foo(x: int64 = 9) -> int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [9],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_05(self):
        await self.con.execute('''
            create function foo(x: int64) -> optional int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_06(self):
        await self.con.execute('''
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_07(self):
        await self.con.execute('''
            create function foo(x: int64, y: int64 = 90) -> int64 {
                set is_inlined := true;
                using (x + y);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [91],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(<int64>{}, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, 1)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, {10, 20, 30})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(1, y))',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, y)'
            '    )'
            ')',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_08(self):
        await self.con.execute('''
            create function foo(x: int64 = 9, y: int64 = 90) -> int64 {
                set is_inlined := true;
                using (x + y);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [99],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [91],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(<int64>{}, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, 1)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, {10, 20, 30})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(1, y))',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, y)'
            '    )'
            ')',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_09(self):
        await self.con.execute('''
            create function foo(variadic x: int64) -> int64 {
                set is_inlined := true;
                using (sum(array_unpack(x)));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [0],
        )
        await self.assert_query_result(
            'select foo(1,<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{},1)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, {10, 20, 30}, 100)',
            [111, 112, 113, 121, 122, 123, 131, 132, 133],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, y, 100)'
            '    )'
            ')',
            [111, 112, 113, 121, 122, 123, 131, 132, 133],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_10(self):
        await self.con.execute('''
            create function foo(named only a: int64) -> int64 {
                set is_inlined := true;
                using (a);
            };
        ''')
        await self.assert_query_result(
            'select foo(a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(a := 1)',
            [1],
        )
        await self.assert_query_result(
            'select foo(a := {1,2,3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(a := x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_11(self):
        await self.con.execute('''
            create function foo(x: int64, named only a: int64) -> int64 {
                set is_inlined := true;
                using (x + a);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, a := 10)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, a := {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := {10, 20, 30})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x, a := 10))',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(1, a := y))',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, a := y)'
            '    )'
            ')',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_12(self):
        await self.con.execute('''
            create function foo(
                x: int64 = 9,
                named only a: int64
            ) -> int64 {
                set is_inlined := true;
                using (x + a);
            };
        ''')
        await self.assert_query_result(
            'select foo(a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(a := 10)',
            [19],
        )
        await self.assert_query_result(
            'select foo(a := {10, 20, 30})',
            [19, 29, 39],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(<int64>{}, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, a := 10)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, a := {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := {10, 20, 30})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x, a := 10))',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(a := y))',
            [19, 29, 39],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(1, a := y))',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, a := y)'
            '    )'
            ')',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_13(self):
        await self.con.execute('''
            create function foo(
                x: int64,
                named only a: int64 = 90
            ) -> int64 {
                set is_inlined := true;
                using (x + a);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [91],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(<int64>{}, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, a := 10)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, a := {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := {10, 20, 30})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x, a := 10))',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(1, a := y))',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, a := y)'
            '    )'
            ')',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_14(self):
        await self.con.execute('''
            create function foo(
                x: int64 = 9,
                named only a: int64 = 90
            ) -> int64 {
                set is_inlined := true;
                using (x + a);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [99],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [91],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(a := 10)',
            [19],
        )
        await self.assert_query_result(
            'select foo(a := {10, 20, 30})',
            [19, 29, 39],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(<int64>{}, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, a := 10)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, a := 10)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := 10)',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, a := {10, 20, 30})',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, a := {10, 20, 30})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [91, 92, 93],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x, a := 10))',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(a := y))',
            [19, 29, 39],
            sort=True,
        )
        await self.assert_query_result(
            'for y in {10, 20, 30} union (select foo(1, a := y))',
            [11, 21, 31],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, a := y)'
            '    )'
            ')',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_15(self):
        await self.con.execute('''
            create function foo(
                x: int64,
                y: int64 = 90,
                variadic z: int64,
                named only a: int64,
                named only b: int64 = 90000
            ) -> int64 {
                set is_inlined := true;
                using (x + y + sum(array_unpack(z)) + a + b);
            };
        ''')
        await self.assert_query_result(
            'select foo(1, a := 1000)',
            [91091],
        )
        await self.assert_query_result(
            'select foo(1, 10, a := 1000)',
            [91011],
        )
        await self.assert_query_result(
            'select foo(1, a := 1000, b := 10000)',
            [11091],
        )
        await self.assert_query_result(
            'select foo(1, 10, a := 1000, b := 10000)',
            [11011],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, a := 1000)',
            [91111],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, a := 1000, b := 10000)',
            [11111],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, 200, a := 1000)',
            [91311],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, 200, a := 1000, b := 10000)',
            [11311],
        )

    async def test_edgeql_functions_inline_basic_16(self):
        await self.con.execute('''
            create function foo(x: optional int64) -> optional int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_17(self):
        await self.con.execute('''
            create function foo(
                x: optional int64
            ) -> int64 {
                set is_inlined := true;
                using (x ?? 5);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [5],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_18(self):
        await self.con.execute('''
            create function foo(
                x: optional int64 = 9
            ) -> int64 {
                set is_inlined := true;
                using (x ?? 5);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [9],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [5],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_basic_19(self):
        await self.con.execute('''
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (for y in {x, x + 1, x + 2} union (y));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1, 2, 3],
        )
        await self.assert_query_result(
            'select foo({11, 21, 31})',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {11, 21, 31} union (select foo(x))',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

    async def test_edgeql_functions_inline_array_01(self):
        await self.con.execute('''
            create function foo(x: int64) -> array<int64> {
                set is_inlined := true;
                using ([x]);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [[1], [2], [3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_array_02(self):
        await self.con.execute('''
            create function foo(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [[1], [2, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_array_03(self):
        await self.con.execute('''
            create function foo(
                x: array<int64> = [9]
            ) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [[9]],
        )
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [[1], [2, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_array_04(self):
        await self.con.execute('''
            create function foo(x: array<int64>) -> int64 {
                set is_inlined := true;
                using (sum(array_unpack(x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [1],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [1, 5],
            sort=True,
        )

    async def test_edgeql_functions_inline_array_05(self):
        await self.con.execute('''
            create function foo(x: array<int64>) -> set of int64 {
                set is_inlined := true;
                using (array_unpack(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [1],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_tuple_01(self):
        await self.con.execute('''
            create function foo(x: int64) -> tuple<int64> {
                set is_inlined := true;
                using ((x,));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [(1,), (2,), (3,)],
            sort=True,
        )

    async def test_edgeql_functions_inline_tuple_02(self):
        await self.con.execute('''
            create function foo(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [(1,), (2,), (3,)],
            sort=True,
        )

    async def test_edgeql_functions_inline_tuple_03(self):
        await self.con.execute('''
            create function foo(
                x: tuple<int64> = (9,)
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [(9,)],
        )
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [(1,), (2,), (3,)],
        )

    async def test_edgeql_functions_inline_tuple_04(self):
        await self.con.execute('''
            create function foo(
                x: tuple<int64>
            ) -> int64 {
                set is_inlined := true;
                using (x.0);
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [1],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_tuple_05(self):
        await self.con.execute('''
            create function foo(x: int64) -> tuple<a: int64> {
                set is_inlined := true;
                using ((a:=x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [{'a': 1}],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [{'a': 1}, {'a': 2}, {'a': 3}],
        )

    async def test_edgeql_functions_inline_tuple_06(self):
        await self.con.execute('''
            create function foo(
                x: tuple<a: int64>
            ) -> tuple<a: int64> {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [{'a': 1}],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [{'a': 1}, {'a': 2}, {'a': 3}],
        )

    async def test_edgeql_functions_inline_tuple_07(self):
        await self.con.execute('''
            create function foo(
                x: tuple<a: int64> = (a:=9)
            ) -> tuple<a: int64> {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [{'a': 9}],
        )
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [{'a': 1}],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [{'a': 1}, {'a': 2}, {'a': 3}],
        )

    async def test_edgeql_functions_inline_tuple_08(self):
        await self.con.execute('''
            create function foo(
                x: tuple<a: int64>
            ) -> int64 {
                set is_inlined := true;
                using (x.a);
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [1],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [1, 2, 3],
        )

    async def test_edgeql_functions_inline_object_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: int64) -> optional Bar {
                set is_inlined := true;
                using ((select Bar{a} filter .a = x limit 1));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(-1).a',
            [],
        )
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: Bar) -> Bar {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: optional Bar) -> optional Bar {
                set is_inlined := true;
                using (x ?? (select Bar filter .a = 1 limit 1));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: Bar) -> int64 {
                set is_inlined := true;
                using (x.a);
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_05(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: Bar) -> set of Bar {
                set is_inlined := true;
                using ((select Bar{a} filter .a <= x.a));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 1, 1, 2, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_06(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using ((select Bar{a} filter .a <= x).a);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1,2,3})',
            [1, 1, 1, 2, 2, 3],
            sort=True,
        )

    @tb.needs_factoring
    async def test_edgeql_functions_inline_object_07(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo() -> int64 {
                set is_inlined := true;
                using (count(Bar));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [3],
        )
        await self.assert_query_result(
            'select (foo(), foo())',
            [[3, 3]],
            sort=True,
        )
        await self.assert_query_result(
            'select (Bar.a, foo())',
            [[1, 3], [2, 3], [3, 3]],
            sort=True,
        )
        await self.assert_query_result(
            'select (foo(), Bar.a)',
            [[3, 1], [3, 2], [3, 3]],
            sort=True,
        )
        await self.assert_query_result(
            'select (Bar.a, foo(), Bar.a, foo())',
            [[1, 3, 1, 3], [2, 3, 2, 3], [3, 3, 3, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_08(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo() -> set of tuple<int64, int64> {
                set is_inlined := true;
                using ((Bar.a, count(Bar)));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [[1, 1], [2, 1], [3, 1]],
        )
        await self.assert_query_result(
            'select (foo(), foo())',
            [
                [[1, 1], [1, 1]], [[1, 1], [2, 1]], [[1, 1], [3, 1]],
                [[2, 1], [1, 1]], [[2, 1], [2, 1]], [[2, 1], [3, 1]],
                [[3, 1], [1, 1]], [[3, 1], [2, 1]], [[3, 1], [3, 1]],
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select (Bar.a, foo())',
            [
                [1, [1, 1]], [1, [2, 1]], [1, [3, 1]],
                [2, [1, 1]], [2, [2, 1]], [2, [3, 1]],
                [3, [1, 1]], [3, [2, 1]], [3, [3, 1]],
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select (foo(), Bar.a)',
            [
                [[1, 1], 1], [[1, 1], 2], [[1, 1], 3],
                [[2, 1], 1], [[2, 1], 2], [[2, 1], 3],
                [[3, 1], 1], [[3, 1], 2], [[3, 1], 3],
            ],
            sort=True,
        )

    @tb.needs_factoring
    async def test_edgeql_functions_inline_object_09(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: Bar) -> tuple<int64, int64> {
                set is_inlined := true;
                using ((x.a, count(Bar)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select (Bar.a, foo((select Bar filter .a = 1)))',
            [[1, [1, 3]]],
        )
        await self.assert_query_result(
            'select (Bar.a, foo((select detached Bar filter .a = 1)))',
            [[1, [1, 3]], [2, [1, 3]], [3, [1, 3]]],
            sort=True,
        )
        await self.assert_query_result(
            'select (Bar.a, foo(Bar))',
            [[1, [1, 3]], [2, [2, 3]], [3, [3, 3]]],
            sort=True,
        )
        await self.assert_query_result(
            'select (foo(Bar), foo(Bar))',
            [[[1, 3], [1, 3]], [[2, 3], [2, 3]], [[3, 3], [3, 3]]],
            sort=True,
        )
        await self.assert_query_result(
            'select (foo(Bar), foo(detached Bar))',
            [
                [[1, 3], [1, 3]], [[1, 3], [2, 3]], [[1, 3], [3, 3]],
                [[2, 3], [1, 3]], [[2, 3], [2, 3]], [[2, 3], [3, 3]],
                [[3, 3], [1, 3]], [[3, 3], [2, 3]], [[3, 3], [3, 3]],
            ],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_10(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: Bar) -> set of Baz {
                set is_inlined := true;
                using ((select Baz filter .b <= x.a));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [4, 4, 4, 5, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_11(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: Bar | Baz) -> Bar | Baz {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar | Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Baz filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Baz)).a',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {Bar, Baz})).a',
            [1, 2, 3, 4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_12(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: int64) -> optional Bar | Baz {
                set is_inlined := true;
                using ((select {Bar, Baz} filter .a = x limit 1));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 4}).a',
            [1, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({0, 1, 2, 3, 4, 5, 6, 7, 8}).a',
            [1, 2, 3, 4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_13(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: Bar | Baz) -> optional Bar {
                set is_inlined := true;
                using (x[is Bar]);
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar | Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Baz filter .a = 4)).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Baz)).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select {Bar, Baz})).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_14(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: Bar | Baz) -> optional int64 {
                set is_inlined := true;
                using (
                    x[is Baz].b
                )
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<Baz>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar | Baz>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar))',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Baz filter .a = 4))',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Baz))',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {Bar, Baz}))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_15(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: Bar | Baz) -> optional int64 {
                set is_inlined := true;
                using (
                    if x is Bar
                    then x.a*2
                    else 10 + assert_exists(x[is Baz]).b
                )
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<Baz>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar | Baz>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [2],
        )
        await self.assert_query_result(
            'select foo((select Bar))',
            [2, 4, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Baz filter .a = 4))',
            [11],
        )
        await self.assert_query_result(
            'select foo((select Baz))',
            [11, 12, 13],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {Bar, Baz}))',
            [2, 4, 6, 11, 12, 13],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_16(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Bar2 extending Bar;
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Bar2{a := 4};
            insert Bar2{a := 5};
            insert Bar2{a := 6};
            create function foo(x: Bar) -> optional Bar2 {
                set is_inlined := true;
                using (x[is Bar2]);
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar2>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Bar2 filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Bar2)).a',
            [4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_object_17(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{
                b := 4,
                bar := assert_exists((select Bar filter .a = 1 limit 1)),
            };
            insert Baz{
                b := 5,
                bar := assert_exists((select Bar filter .a = 2 limit 1)),
            };
            insert Baz{
                b := 6,
                bar := assert_exists((select Bar filter .a = 3 limit 1)),
            };
            create function foo(x: Baz) -> Bar {
                set is_inlined := true;
                using (x.bar);
            };
        ''')
        await self.assert_query_result(
            'select foo(<Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Baz filter .b = 4)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Baz)).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_shape_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select Bar{'
            '    a,'
            '    b := foo(.a)'
            '} order by .a',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 3, 'b': 3},
            ],
        )

    async def test_edgeql_functions_inline_shape_02(self):
        await self.con.execute('''
            create type Bar {
                create property a -> int64;
            };
            insert Bar{};
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: optional int64) -> optional int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select Bar{'
            '    a,'
            '    b := foo(.a)'
            '} order by .a',
            [
                {'a': None, 'b': None},
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 3, 'b': 3},
            ],
        )

    async def test_edgeql_functions_inline_shape_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: optional int64) -> set of int64 {
                set is_inlined := true;
                using ({10 + x, 20 + x, 30 + x});
            };
        ''')
        await self.assert_query_result(
            'select Bar{'
            '    a,'
            '    b := foo(.a)'
            '} order by .a',
            [
                {'a': 1, 'b': [11, 21, 31]},
                {'a': 2, 'b': [12, 22, 32]},
                {'a': 3, 'b': [13, 23, 33]},
            ],
        )

    async def test_edgeql_functions_inline_shape_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo() -> int64 {
                set is_inlined := true;
                using (count(Bar));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [3],
        )
        await self.assert_query_result(
            'select Bar {'
            '    a,'
            '    n := foo(),'
            '} order by .a',
            [{'a': 1, 'n': 3}, {'a': 2, 'n': 3}, {'a': 3, 'n': 3}],
        )

    async def test_edgeql_functions_inline_shape_05(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo() -> set of tuple<int64, int64> {
                set is_inlined := true;
                using ((Bar.a, count(Bar)));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [[1, 1], [2, 1], [3, 1]],
        )
        await self.assert_query_result(
            'select Bar {'
            '    a,'
            '    n := foo(),'
            '} order by .a',
            [
                {'a': 1, 'n': [[1, 1], [2, 1], [3, 1]]},
                {'a': 2, 'n': [[1, 1], [2, 1], [3, 1]]},
                {'a': 3, 'n': [[1, 1], [2, 1], [3, 1]]},
            ],
        )

    async def test_edgeql_functions_inline_shape_06(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: Bar) -> tuple<int64, int64> {
                set is_inlined := true;
                using ((x.a, count(Bar)));
            };
        ''')
        await self.assert_query_result(
            'select Bar {'
            '    a,'
            '    n := foo(Bar),'
            '} order by .a',
            [
                {'a': 1, 'n': [1, 3]},
                {'a': 2, 'n': [2, 3]},
                {'a': 3, 'n': [3, 3]},
            ],
        )

    async def test_edgeql_functions_inline_shape_07(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: int64) -> Bar {
                set is_inlined := true;
                using (assert_exists((select Bar filter .a = x limit 1)));
            };
        ''')
        await self.assert_query_result(
            'select Baz{'
            '    a,'
            '    c := foo(.b).a,'
            '} order by .a',
            [
                {'a': 4, 'c': 1},
                {'a': 5, 'c': 2},
                {'a': 6, 'c': 3},
            ],
        )

    async def test_edgeql_functions_inline_shape_08(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            insert Baz{a := 7, b := 4};
            create function foo(x: int64) -> optional Bar {
                set is_inlined := true;
                using ((select Bar filter .a = x limit 1));
            };
        ''')
        await self.assert_query_result(
            'select Baz{'
            '    a,'
            '    c := foo(.b).a,'
            '} order by .a',
            [
                {'a': 4, 'c': 1},
                {'a': 5, 'c': 2},
                {'a': 6, 'c': 3},
                {'a': 7, 'c': None},
            ],
        )

    async def test_edgeql_functions_inline_shape_09(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using ((select Bar filter .a <= x));
            };
        ''')
        await self.assert_query_result(
            'select Baz{'
            '    a,'
            '    c := foo(.b).a,'
            '} order by .a',
            [
                {'a': 4, 'c': [1]},
                {'a': 5, 'c': [1, 2]},
                {'a': 6, 'c': [1, 2, 3]},
            ],
        )

    async def test_edgeql_functions_inline_shape_10(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{
                b := 4,
                bar := assert_exists((select Bar filter .a = 1 limit 1)),
            };
            insert Baz{
                b := 5,
                bar := assert_exists((select Bar filter .a = 2 limit 1)),
            };
            insert Baz{
                b := 6,
                bar := assert_exists((select Bar filter .a = 3 limit 1)),
            };
            create function foo(x: Bar) -> Bar {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select Baz{'
            '    a := foo(.bar).a,'
            '    b,'
            '} order by .a',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_shape_11(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{
                b := 4,
                bar := assert_exists((select Bar filter .a = 1 limit 1)),
            };
            insert Baz{
                b := 5,
                bar := assert_exists((select Bar filter .a = 2 limit 1)),
            };
            insert Baz{
                b := 6,
                bar := assert_exists((select Bar filter .a = 3 limit 1)),
            };
            create function foo(x: Bar) -> int64 {
                set is_inlined := true;
                using (x.a);
            };
        ''')
        await self.assert_query_result(
            'select Baz{'
            '    a := foo(.bar),'
            '    b,'
            '} order by .a',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_shape_12(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create multi link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{
                b := 4,
                bar := assert_exists((select Bar filter .a <= 1)),
            };
            insert Baz{
                b := 5,
                bar := assert_exists((select Bar filter .a <= 2)),
            };
            insert Baz{
                b := 6,
                bar := assert_exists((select Bar filter .a <= 3)),
            };
            create function foo(x: Bar) -> Bar {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select Baz{'
            '    a := foo(.bar).a,'
            '    b,'
            '} order by .b',
            [
                {'a': [1], 'b': 4},
                {'a': [1, 2], 'b': 5},
                {'a': [1, 2, 3], 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_shape_13(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar {
                    create property b -> int64;
                };
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{
                bar := assert_exists((select Bar filter .a = 1 limit 1)) {
                    @b := 4
                },
            };
            insert Baz{
                bar := assert_exists((select Bar filter .a = 2 limit 1)) {
                    @b := 5
                }
            };
            insert Baz{
                bar := assert_exists((select Bar filter .a = 3 limit 1)) {
                    @b := 6
                }
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
        ''')
        await self.assert_query_result(
            'select Baz{'
            '    a := .bar.a,'
            '    b := foo(.bar@b),'
            '} order by .a',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_global_01(self):
        await self.con.execute('''
            create global a := 1;
            create function foo() -> int64 {
                set is_inlined := true;
                using (global a);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [1],
        )

    async def test_edgeql_functions_inline_global_02(self):
        await self.con.execute('''
            create global a -> int64;
            create function foo() -> optional int64 {
                set is_inlined := true;
                using (global a);
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [],
        )

        await self.con.execute('''
            set global a := 1;
        ''')
        await self.assert_query_result(
            'select foo()',
            [1],
        )

    async def test_edgeql_functions_inline_global_03(self):
        await self.con.execute('''
            create global a := 1;
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (global a + x);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_global_04(self):
        await self.con.execute('''
            create global a -> int64;
            create function foo(x: int64) -> optional int64 {
                set is_inlined := true;
                using (global a + x)
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [],
            sort=True,
        )

        await self.con.execute('''
            set global a := 1;
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_01(self):
        # Directly passing parameter
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x)
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(x))
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_02(self):
        # Indirectly passing parameter
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(x + 1))
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [4],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [4, 9, 16],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [4, 9, 16],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_03(self):
        # Calling same inner function with different parameters
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo(x: int64, y: int64) -> int64 {
                set is_inlined := true;
                using (inner(x) + inner(y));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<int64>{}, 1)',
            [],
        )
        await self.assert_query_result(
            'select foo(1, 10)',
            [101],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, 10)',
            [101, 104, 109],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(1, {10, 20, 30})',
            [101, 401, 901],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}, {10, 20, 30})',
            [101, 104, 109, 401, 404, 409, 901, 904, 909],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    for y in {10, 20, 30} union ('
            '        select foo(x, y)'
            '    )'
            ')',
            [101, 104, 109, 401, 404, 409, 901, 904, 909],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_04(self):
        # Directly passing parameter with default
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo(x: int64 = 9) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [81],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 4, 9],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 4, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_05(self):
        # Indirectly passing parameter with default
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo(x: int64 = 9) -> int64 {
                set is_inlined := true;
                using (inner(x+1));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [100],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [4],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [4, 9, 16],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [4, 9, 16],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_06(self):
        # Inner function with default parameter
        await self.con.execute('''
            create function inner(x: int64 = 9) -> int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo1() -> int64 {
                set is_inlined := true;
                using (inner());
            };
            create function foo2(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo1()',
            [81],
        )
        await self.assert_query_result(
            'select foo2(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo2(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo2({1, 2, 3})',
            [1, 4, 9],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo2(x))',
            [1, 4, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_07(self):
        # Directly passing optional parameter
        await self.con.execute('''
            create function inner(x: optional int64) -> optional int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo(x: optional int64) -> int64 {
                set is_inlined := true;
                using (inner(x) ?? 99);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [99],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 4, 9],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 4, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_08(self):
        # Indirectly passing optional parameter
        await self.con.execute('''
            create function inner(x: optional int64) -> optional int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo(x: optional int64) -> int64 {
                set is_inlined := true;
                using (inner(x+1) ?? 99);
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [99],
        )
        await self.assert_query_result(
            'select foo(1)',
            [4],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [4, 9, 16],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [4, 9, 16],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_09(self):
        # Inner function with optional parameter
        await self.con.execute('''
            create function inner(x: optional int64) -> int64 {
                set is_inlined := true;
                using ((x * x) ?? 99)
            };
            create function foo1() -> int64 {
                set is_inlined := true;
                using (inner(<int64>{}));
            };
            create function foo2(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo1()',
            [99],
        )
        await self.assert_query_result(
            'select foo2(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo2(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo2({1, 2, 3})',
            [1, 4, 9],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo2(x))',
            [1, 4, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_10(self):
        # Directly passing variadic parameter
        await self.con.execute('''
            create function inner(x: array<int64>) -> int64 {
                set is_inlined := true;
                using (sum(array_unpack(x)))
            };
            create function foo(variadic x: int64) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [0],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo(1, 2, 3)',
            [6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2}, {10, 20})',
            [11, 12, 21, 22],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_11(self):
        # Indirectly passing variadic parameter
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x)
            };
            create function foo(variadic x: int64) -> int64 {
                set is_inlined := true;
                using (inner(sum(array_unpack(x))));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [0],
        )
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo(1, 2, 3)',
            [6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2}, {10, 20})',
            [11, 12, 21, 22],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_12(self):
        # Inner function with variadic parameter
        await self.con.execute('''
            create function inner(variadic x: int64) -> int64 {
                set is_inlined := true;
                using (sum(array_unpack(x)))
            };
            create function foo1() -> int64 {
                set is_inlined := true;
                using (inner());
            };
            create function foo2(x: int64, y: int64, z: int64) -> int64 {
                set is_inlined := true;
                using (inner(x, y, z));
            };
        ''')
        await self.assert_query_result(
            'select foo1()',
            [0],
        )
        await self.assert_query_result(
            'select foo2(<int64>{}, <int64>{}, <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo2(1, 2, 3)',
            [6],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo2(x, x * 10, x * 100))',
            [111, 222, 333],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_13(self):
        # Directly passing named parameter
        await self.con.execute('''
            create function inner(named only a: int64) -> int64 {
                set is_inlined := true;
                using (a * a)
            };
            create function foo(named only a: int64) -> int64 {
                set is_inlined := true;
                using (inner(a := a));
            };
        ''')
        await self.assert_query_result(
            'select foo(a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(a := 1)',
            [1],
        )
        await self.assert_query_result(
            'select foo(a := {1, 2, 3})',
            [1, 4, 9],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(a := x))',
            [1, 4, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_14(self):
        # Indirectly passing named parameter
        await self.con.execute('''
            create function inner(named only a: int64) -> int64 {
                set is_inlined := true;
                using (a * a)
            };
            create function foo(named only a: int64) -> int64 {
                set is_inlined := true;
                using (inner(a := a + 1));
            };
        ''')
        await self.assert_query_result(
            'select foo(a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(a := 1)',
            [4],
        )
        await self.assert_query_result(
            'select foo(a := {1, 2, 3})',
            [4, 9, 16],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(a := x))',
            [4, 9, 16],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_15(self):
        # Passing named parameter as positional
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x * x)
            };
            create function foo(named only a: int64) -> int64 {
                set is_inlined := true;
                using (inner(a));
            };
        ''')
        await self.assert_query_result(
            'select foo(a := <int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(a := 1)',
            [1],
        )
        await self.assert_query_result(
            'select foo(a := {1, 2, 3})',
            [1, 4, 9],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(a := x))',
            [1, 4, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_16(self):
        # Passing positional parameter as named
        await self.con.execute('''
            create function inner(named only a: int64) -> int64 {
                set is_inlined := true;
                using (a * a)
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(a := x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [1, 4, 9],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 4, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_17(self):
        # Variety of paremeter types
        await self.con.execute('''
            create function inner1(x: int64, y: int64) -> int64 {
                set is_inlined := true;
                using (x + y)
            };
            create function inner2(x: array<int64>) -> int64 {
                set is_inlined := true;
                using (sum(array_unpack(x)))
            };
            create function foo(
                x: int64,
                y: int64 = 90,
                variadic z: int64,
                named only a: int64,
                named only b: int64 = 90000
            ) -> int64 {
                set is_inlined := true;
                using (inner1(x, a) + inner1(y, b) + inner2(z));
            };
        ''')
        await self.assert_query_result(
            'select foo(1, a := 1000)',
            [91091],
        )
        await self.assert_query_result(
            'select foo(1, 10, a := 1000)',
            [91011],
        )
        await self.assert_query_result(
            'select foo(1, a := 1000, b := 10000)',
            [11091],
        )
        await self.assert_query_result(
            'select foo(1, 10, a := 1000, b := 10000)',
            [11011],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, a := 1000)',
            [91111],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, a := 1000, b := 10000)',
            [11111],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, 200, a := 1000)',
            [91311],
        )
        await self.assert_query_result(
            'select foo(1, 10, 100, 200, a := 1000, b := 10000)',
            [11311],
        )

    async def test_edgeql_functions_inline_nested_basic_18(self):
        # For in inner function
        await self.con.execute('''
            create function inner(x: int64) -> set of int64 {
                set is_inlined := true;
                using (for y in {x, x + 1, x + 2} union (y))
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(10)',
            [10, 11, 12],
        )
        await self.assert_query_result(
            'select foo({10, 20, 30})',
            [10, 11, 12, 20, 21, 22, 30, 31, 32],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {10, 20, 30} union (select foo(x))',
            [10, 11, 12, 20, 21, 22, 30, 31, 32],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_19(self):
        # For in outer function
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x)
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (for y in {x, x + 1, x + 2} union (inner(y)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(10)',
            [10, 11, 12],
        )
        await self.assert_query_result(
            'select foo({10, 20, 30})',
            [10, 11, 12, 20, 21, 22, 30, 31, 32],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {10, 20, 30} union (select foo(x))',
            [10, 11, 12, 20, 21, 22, 30, 31, 32],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_basic_20(self):
        # Deeply nested
        await self.con.execute('''
            create function inner1(x: int64) -> int64 {
                set is_inlined := true;
                using (x+1)
            };
            create function inner2(x: int64) -> int64 {
                set is_inlined := true;
                using (inner1(x+2))
            };
            create function inner3(x: int64) -> int64 {
                set is_inlined := true;
                using (inner2(x+3))
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner3(x+4))
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [11],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [11, 12, 13],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_01(self):
        # Return array from inner function
        await self.con.execute('''
            create function inner(x: int64) -> array<int64> {
                set is_inlined := true;
                using ([x]);
            };
            create function foo(x: int64) -> array<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [[1], [2], [3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_02(self):
        # Access array element in inner function
        await self.con.execute('''
            create function inner(x: array<int64>) -> int64 {
                set is_inlined := true;
                using (x[0]);
            };
            create function foo(x: array<int64>) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [1],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [1, 2],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_03(self):
        # Access array element in outer function
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: array<int64>) -> int64 {
                set is_inlined := true;
                using (inner(x[0]));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [1],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [1, 2],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_04(self):
        # Directly passing array parameter
        await self.con.execute('''
            create function inner(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [[1], [2, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_05(self):
        # Indirectly passing array parameter
        await self.con.execute('''
            create function inner(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (inner((select x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [[1], [2, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_06(self):
        # Inner function with array parameter
        await self.con.execute('''
            create function inner(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: int64) -> array<int64> {
                set is_inlined := true;
                using (inner([x]));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [[1], [2], [3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_07(self):
        # Directly passing array parameter with default
        await self.con.execute('''
            create function inner(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: array<int64> = [9]
            ) -> array<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [[9]],
        )
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [[1], [2, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_08(self):
        # Directly passing array parameter with default
        await self.con.execute('''
            create function inner(x: array<int64>) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: array<int64> = [9]
            ) -> array<int64> {
                set is_inlined := true;
                using (inner((select x)));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [[9]],
        )
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [[1]],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [[1], [2, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_09(self):
        # Inner function with array parameter with default
        await self.con.execute('''
            create function inner(x: array<int64> = [9]) -> array<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo1() -> array<int64> {
                set is_inlined := true;
                using (inner());
            };
            create function foo2(
                x: array<int64>
            ) -> array<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo1()',
            [[9]],
        )
        await self.assert_query_result(
            'select foo2(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo2([1])',
            [[1]],
        )
        await self.assert_query_result(
            'select foo2({[1], [2, 3]})',
            [[1], [2, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_10(self):
        # Unpack array in inner function
        await self.con.execute('''
            create function inner(x: array<int64>) -> set of int64 {
                set is_inlined := true;
                using (array_unpack(x));
            };
            create function foo(x: array<int64>) -> set of int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [1],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_array_11(self):
        # Unpack array in outer function
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: array<int64>) -> set of int64 {
                set is_inlined := true;
                using (inner(array_unpack(x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<array<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo([1])',
            [1],
        )
        await self.assert_query_result(
            'select foo({[1], [2, 3]})',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_tuple_01(self):
        # Return tuple from inner function
        await self.con.execute('''
            create function inner(x: int64) -> tuple<int64> {
                set is_inlined := true;
                using ((x,));
            };
            create function foo(x: int64) -> tuple<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [(1,), (2,), (3,)],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}).0',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_tuple_02(self):
        # Return named tuple from inner function
        await self.con.execute('''
            create function inner(x: int64) -> tuple<a: int64> {
                set is_inlined := true;
                using ((a := x));
            };
            create function foo(x: int64) -> tuple<a: int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [{'a': 1}],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}) order by .a',
            [{'a': 1}, {'a': 2}, {'a': 3}],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_tuple_03(self):
        # Accessing tuple element in inner function
        await self.con.execute('''
            create function inner(
                x: tuple<int64>
            ) -> int64 {
                set is_inlined := true;
                using (x.0);
            };
            create function foo(
                x: tuple<int64>
            ) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [1],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_tuple_04(self):
        # Accessing tuple element in outer function
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: tuple<int64>
            ) -> int64 {
                set is_inlined := true;
                using (inner(x.0));
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [1],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [1, 2, 3],
        )

    async def test_edgeql_functions_inline_nested_tuple_05(self):
        # Accessing named tuple element in inner function
        await self.con.execute('''
            create function inner(
                x: tuple<a: int64>
            ) -> int64 {
                set is_inlined := true;
                using (x.a);
            };
            create function foo(
                x: tuple<a: int64>
            ) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<a: int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((a := 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo({(a := 1), (a := 2), (a := 3)})',
            [1, 2, 3],
        )

    async def test_edgeql_functions_inline_nested_tuple_06(self):
        # Accessing named tuple element in outer function
        await self.con.execute('''
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: tuple<a: int64>
            ) -> int64 {
                set is_inlined := true;
                using (inner(x.a));
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((a := 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo({(a := 1), (a := 2), (a := 3)})',
            [1, 2, 3],
        )

    async def test_edgeql_functions_inline_nested_tuple_07(self):
        # Directly passing tuple parameter
        await self.con.execute('''
            create function inner(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [(1,), (2,), (3,)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_tuple_08(self):
        # Indirectly passing tuple parameter
        await self.con.execute('''
            create function inner(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (inner((select x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [(1,), (2,), (3,)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_tuple_09(self):
        # Inner function with tuple parameter
        await self.con.execute('''
            create function inner(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: int64
            ) -> tuple<int64> {
                set is_inlined := true;
                using (inner((x,)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [(1,), (2,), (3,)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_tuple_10(self):
        # Directly passing a tuple parameter with default
        await self.con.execute('''
            create function inner(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: tuple<int64> = (9,)
            ) -> tuple<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [(9,)],
        )
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [(1,), (2,), (3,)],
        )

    async def test_edgeql_functions_inline_nested_tuple_11(self):
        # Indirectly passing tuple parameter with default
        await self.con.execute('''
            create function inner(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo(
                x: tuple<int64> = (9,)
            ) -> tuple<int64> {
                set is_inlined := true;
                using (inner((select x)));
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [(9,)],
        )
        await self.assert_query_result(
            'select foo(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((1,))',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo({(1,), (2,), (3,)})',
            [(1,), (2,), (3,)],
        )

    async def test_edgeql_functions_inline_nested_tuple_12(self):
        # Inner function with tuple parameter with default
        await self.con.execute('''
            create function inner(
                x: tuple<int64> = (9,)
            ) -> tuple<int64> {
                set is_inlined := true;
                using (x);
            };
            create function foo1() -> tuple<int64> {
                set is_inlined := true;
                using (inner());
            };
            create function foo2(
                x: tuple<int64>
            ) -> tuple<int64> {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo1()',
            [(9,)],
        )
        await self.assert_query_result(
            'select foo2(<tuple<int64>>{})',
            [],
        )
        await self.assert_query_result(
            'select foo2((1,))',
            [(1,)],
        )
        await self.assert_query_result(
            'select foo2({(1,), (2,), (3,)})',
            [(1,), (2,), (3,)],
        )

    async def test_edgeql_functions_inline_nested_object_01(self):
        # Directly passing object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: Bar) -> Bar {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: Bar) -> Bar {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_02(self):
        # Indirectly passing object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: Bar) -> Bar {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: Bar) -> Bar {
                set is_inlined := true;
                using (inner((select x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_03(self):
        # Inner function with object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: Bar) -> Bar {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: int64) -> optional Bar {
                set is_inlined := true;
                using (inner((select Bar filter .a = x limit 1)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3, 4}).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_04(self):
        # Inner function returning object
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: int64) -> optional Bar {
                set is_inlined := true;
                using ((select Bar filter .a = x limit 1));
            };
            create function foo(x: int64) -> optional Bar {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3, 4}).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_05(self):
        # Outer function returning object
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: int64) -> optional Bar {
                set is_inlined := true;
                using ((select Bar filter .a = inner(x) limit 1));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3, 4}).a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_06(self):
        # Inner function returning set of object
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: int64) -> set of Bar {
                set is_inlined := true;
                using ((select Bar filter .a <= x));
            };
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}).a',
            [1, 1, 1, 2, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_07(self):
        # Outer function returning set of object
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using ((select Bar filter .a <= inner(x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(2).a',
            [1, 2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3}).a',
            [1, 1, 1, 2, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_08(self):
        # Directly passing optional object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: optional Bar) -> optional int64 {
                set is_inlined := true;
                using (x.a ?? 99);
            };
            create function foo(x: optional Bar) -> optional int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [99],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_09(self):
        # Indirectly passing optional object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: optional Bar) -> optional int64 {
                set is_inlined := true;
                using (x.a ?? 99);
            };
            create function foo(x: optional Bar) -> optional int64 {
                set is_inlined := true;
                using (inner((select x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [99],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_10(self):
        # Inner function with optional object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: optional Bar) -> int64 {
                set is_inlined := true;
                using (x.a ?? 99);
            };
            create function foo1() -> int64 {
                set is_inlined := true;
                using (inner(<Bar>{}));
            };
            create function foo2(x: Bar) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo1()',
            [99],
        )
        await self.assert_query_result(
            'select foo2(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo2((select Bar filter .a = 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo2((select Bar))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_11(self):
        # Check path factoring
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner() -> set of tuple<int64, int64> {
                set is_inlined := true;
                using ((Bar.a, count(Bar)));
            };
            create function foo() -> set of tuple<int64, int64> {
                set is_inlined := true;
                using (inner());
            };
        ''')
        await self.assert_query_result(
            'select foo()',
            [[1, 1], [2, 1], [3, 1]],
        )
        await self.assert_query_result(
            'select (foo(), foo())',
            [
                [[1, 1], [1, 1]], [[1, 1], [2, 1]], [[1, 1], [3, 1]],
                [[2, 1], [1, 1]], [[2, 1], [2, 1]], [[2, 1], [3, 1]],
                [[3, 1], [1, 1]], [[3, 1], [2, 1]], [[3, 1], [3, 1]],
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select (Bar.a, foo())',
            [
                [1, [1, 1]], [1, [2, 1]], [1, [3, 1]],
                [2, [1, 1]], [2, [2, 1]], [2, [3, 1]],
                [3, [1, 1]], [3, [2, 1]], [3, [3, 1]],
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select (foo(), Bar.a)',
            [
                [[1, 1], 1], [[1, 1], 2], [[1, 1], 3],
                [[2, 1], 1], [[2, 1], 2], [[2, 1], 3],
                [[3, 1], 1], [[3, 1], 2], [[3, 1], 3],
            ],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_12(self):
        # Check path factoring
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner1(x: Bar) -> int64 {
                set is_inlined := true;
                using (x.a);
            };
            create function inner2(x: Bar) -> int64 {
                set is_inlined := true;
                using (count(Bar));
            };
            create function foo(x: Bar) -> tuple<int64, int64> {
                set is_inlined := true;
                using ((inner1(x), inner2(x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [[1, 3]],
        )
        await self.assert_query_result(
            'select ('
            '    foo((select Bar filter .a = 1)),'
            '    foo((select Bar filter .a = 2)),'
            ')',
            [[[1, 3], [2, 3]]],
        )
        await self.assert_query_result(
            'select foo((select Bar))',
            [[1, 3], [2, 3], [3, 3]],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_13(self):
        # Directly passing complex type object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function inner(x: Bar | Baz) -> Bar | Baz {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: Bar | Baz) -> Bar | Baz {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar | Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Baz filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Baz)).a',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {Bar, Baz})).a',
            [1, 2, 3, 4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_14(self):
        # Indirectly passing complex type object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function inner(x: Bar | Baz) -> Bar | Baz {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: Bar | Baz) -> Bar | Baz {
                set is_inlined := true;
                using (inner((select x)));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar | Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Baz filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Baz)).a',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {Bar, Baz})).a',
            [1, 2, 3, 4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_15(self):
        # Inner function with complex type object parameter
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function inner(x: Bar | Baz) -> Bar | Baz {
                set is_inlined := true;
                using (x);
            };
            create function foo1(x: Bar) -> Bar | Baz {
                set is_inlined := true;
                using (inner(x));
            };
            create function foo2(x: Baz) -> Bar | Baz {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo1(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo2(<Baz>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo1((select Bar filter .a = 1)).a',
            [1],
        )
        await self.assert_query_result(
            'select foo1((select Bar)).a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select foo2((select Baz filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo2((select Baz)).a',
            [4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_16(self):
        # Type intersection in inner function
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Bar2 extending Bar;
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Bar2{a := 4};
            insert Bar2{a := 5};
            insert Bar2{a := 6};
            create function inner(x: Bar) -> optional Bar2 {
                set is_inlined := true;
                using (x[is Bar2]);
            };
            create function foo(x: Bar) -> optional Bar2 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar2>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Bar2 filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Bar2)).a',
            [4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_17(self):
        # Type intersection in outer function
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Bar2 extending Bar;
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Bar2{a := 4};
            insert Bar2{a := 5};
            insert Bar2{a := 6};
            create function inner(x: Bar2) -> optional Bar2 {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: Bar) -> optional Bar2 {
                set is_inlined := true;
                using (inner(x[is Bar2]));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo(<Bar2>{}).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1)).a',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Bar2 filter .a = 4)).a',
            [4],
        )
        await self.assert_query_result(
            'select foo((select Bar)).a',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select Bar2)).a',
            [4, 5, 6],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_object_18(self):
        # Access linked object in inner function
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar;
            };
            create type Bazz {
                create required link baz -> Baz;
            };
            insert Bazz{baz := (insert Baz{bar := (insert Bar{a := 1})})};
            insert Bazz{baz := (insert Baz{bar := (insert Bar{a := 2})})};
            insert Bazz{baz := (insert Baz{bar := (insert Bar{a := 3})})};
            create function inner1(x: Bar) -> int64 {
                set is_inlined := true;
                using (x.a);
            };
            create function inner2(x: Baz) -> int64 {
                set is_inlined := true;
                using (inner1(x.bar));
            };
            create function foo(x: Bazz) -> int64 {
                set is_inlined := true;
                using (inner2(x.baz));
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bazz>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bazz filter .baz.bar.a = 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo((select Bazz))',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_shape_01(self):
        # Put result of inner function taking Bar.a into Bar
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: Bar) -> int64 {
                set is_inlined := true;
                using ((select x{a, b := inner(x.a)}).b);
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [1],
        )
        await self.assert_query_result(
            'select foo(Bar)',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_shape_02(self):
        # Put result of inner function taking Bar into Bar
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: Bar) -> int64 {
                set is_inlined := true;
                using (x.a + 90);
            };
            create function foo(x: Bar) -> tuple<int64, int64> {
                set is_inlined := true;
                using (
                    with y := (select x{a, b := inner(x)})
                    select (y.a, y.b)
                );
            };
        ''')
        await self.assert_query_result(
            'select foo(<Bar>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select Bar filter .a = 1))',
            [(1, 91)],
        )
        await self.assert_query_result(
            'select foo(Bar)',
            [(1, 91), (2, 92), (3, 93)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_shape_03(self):
        # Put result of inner function taking number into Bar
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x + 90);
            };
            create function foo(x: int64) -> set of tuple<int64, int64> {
                set is_inlined := true;
                using (
                    with y := (select Bar{a, b := inner(x)})
                    select (y.a, y.b)
                );
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(1, 91), (2, 91), (3, 91)],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(Bar.a)',
            [
                (1, 91), (1, 92), (1, 93),
                (2, 91), (2, 92), (2, 93),
                (3, 91), (3, 92), (3, 93),
            ],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_shape_04(self):
        # Put result of inner function using Bar into Bar
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function inner() -> int64 {
                set is_inlined := true;
                using (count(Bar));
            };
            create function foo(x: int64) -> set of tuple<int64, int64> {
                set is_inlined := true;
                using (
                    with y := (select Bar{a, b := inner()} filter .a = x)
                    select (y.a, y.b)
                );
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(1, 3)],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [(1, 3), (2, 3), (3, 3)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_shape_05(self):
        # Put result of inner function taking Baz.b and returning Bar into Baz
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property a -> int64;
                create required property b -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{a := 4, b := 1};
            insert Baz{a := 5, b := 2};
            insert Baz{a := 6, b := 3};
            create function inner(x: int64) -> Bar {
                set is_inlined := true;
                using (assert_exists((select Bar filter .a = x limit 1)));
            };
            create function foo(x: int64) -> set of tuple<int64, int64> {
                set is_inlined := true;
                using (
                    with y := (select Baz{a, c := inner(.b).a} filter .b = x)
                    select (y.a, y.b)
                );
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(4, 1)],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [(4, 1), (5, 2), (6, 3)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_shape_06(self):
        # Put result of inner function taking Baz.bar into Baz
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{
                b := 4,
                bar := assert_exists((select Bar filter .a = 1 limit 1)),
            };
            insert Baz{
                b := 5,
                bar := assert_exists((select Bar filter .a = 2 limit 1)),
            };
            insert Baz{
                b := 6,
                bar := assert_exists((select Bar filter .a = 3 limit 1)),
            };
            create function inner(x: Bar) -> int64 {
                set is_inlined := true;
                using (x.a);
            };
            create function foo(x: int64) -> set of tuple<int64, int64> {
                set is_inlined := true;
                using (
                    with y := (select Baz{a := inner(.bar), b} filter .a = x)
                    select (y.a, y.b)
                );
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(1, 4)],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [(1, 4), (2, 5), (3, 6)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_shape_07(self):
        # Put result of inner function taking Baz.bar@b into Baz
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar {
                    create property b -> int64;
                };
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Baz{
                bar := assert_exists((select Bar filter .a = 1 limit 1)) {
                    @b := 4
                }
            };
            insert Baz{
                bar := assert_exists((select Bar filter .a = 2 limit 1)) {
                    @b := 5
                }
            };
            insert Baz{
                bar := assert_exists((select Bar filter .a = 3 limit 1)) {
                    @b := 6
                }
            };
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (x);
            };
            create function foo(x: int64) -> set of tuple<int64, int64> {
                set is_inlined := true;
                using (
                    with y := (
                        select Baz{a := .bar.a, b := inner(.bar@b)}
                        filter .a = x
                    )
                    select (y.a, y.b)
                );
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [(1, 4)],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [(1, 4), (2, 5), (3, 6)],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_01(self):
        # Use computed global in inner function
        await self.con.execute('''
            create global a := 1;
            create function inner(x: int64) -> int64 {
                set is_inlined := true;
                using (global a + x);
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_02(self):
        # Use non-computed global in inner function
        await self.con.execute('''
            create global a -> int64;
            create function inner(x: int64) -> optional int64 {
                set is_inlined := true;
                using (global a + x);
            };
            create function foo(x: int64) -> optional int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [],
            sort=True,
        )

        await self.con.execute('''
            set global a := 1;
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_03(self):
        # Pass computed global to inner function
        await self.con.execute('''
            create global a := 1;
            create function inner(x: int64, y: int64) -> int64 {
                set is_inlined := true;
                using (x + y);
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(global a, x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_04(self):
        # Pass non-computed global to inner function
        await self.con.execute('''
            create global a -> int64;
            create function inner(x: int64, y: int64) -> optional int64 {
                set is_inlined := true;
                using (x + y);
            };
            create function foo(x: int64) -> optional int64 {
                set is_inlined := true;
                using (inner(global a, x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [],
            sort=True,
        )

        await self.con.execute('''
            set global a := 1;
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_05(self):
        # Use computed global in inner non-inlined function
        # - inlined > non-inlined
        await self.con.execute('''
            create global a := 1;
            create function inner(x: int64) -> int64 {
                using (global a + x);
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_06(self):
        # Use non-computed global in inner non-inlined function
        # - inlined > non-inlined
        await self.con.execute('''
            create global a -> int64;
            create function inner(x: int64) -> optional int64 {
                using (global a + x);
            };
            create function foo(x: int64) -> optional int64 {
                set is_inlined := true;
                using (inner(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [],
            sort=True,
        )

        await self.con.execute('''
            set global a := 1;
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_07(self):
        # Use computed global nested in non-inlined function
        # - non-inlined > inlined > non-inlined
        await self.con.execute('''
            create global a := 1;
            create function inner1(x: int64) -> int64 {
                using (global a + x);
            };
            create function inner2(x: int64) -> int64 {
                set is_inlined := true;
                using (inner1(x));
            };
            create function foo(x: int64) -> int64 {
                using (inner2(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_08(self):
        # Use non-computed global nested in non-inlined function
        # - non-inlined > inlined > non-inlined
        await self.con.execute('''
            create global a -> int64;
            create function inner1(x: int64) -> optional int64 {
                using (global a + x);
            };
            create function inner2(x: int64) -> optional int64 {
                set is_inlined := true;
                using (inner1(x));
            };
            create function foo(x: int64) -> optional int64 {
                using (inner2(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [],
            sort=True,
        )

        await self.con.execute('''
            set global a := 1;
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_09(self):
        # Use computed global in deeply nested inner non-inlined function
        # - inlined > inlined > inlined > non-inlined
        await self.con.execute('''
            create global a := 1;
            create function inner1(x: int64) -> int64 {
                using (global a + x);
            };
            create function inner2(x: int64) -> int64 {
                set is_inlined := true;
                using (inner1(x));
            };
            create function inner3(x: int64) -> int64 {
                set is_inlined := true;
                using (inner2(x));
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using (inner3(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_nested_global_10(self):
        # Use computed global in deeply nested inner non-inlined function
        # - inlined > inlined > inlined > non-inlined
        await self.con.execute('''
            create global a -> int64;
            create function inner1(x: int64) -> optional int64 {
                using (global a + x);
            };
            create function inner2(x: int64) -> optional int64 {
                set is_inlined := true;
                using (inner1(x));
            };
            create function inner3(x: int64) -> optional int64 {
                set is_inlined := true;
                using (inner2(x));
            };
            create function foo(x: int64) -> optional int64 {
                set is_inlined := true;
                using (inner3(x));
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [],
            sort=True,
        )

        await self.con.execute('''
            set global a := 1;
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(1)',
            [2],
        )
        await self.assert_query_result(
            'select foo({1, 2, 3})',
            [2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_modifying_cardinality_01(self):
        await self.con.execute('''
            create function foo(x: int64) -> int64 {
                set volatility := schema::Volatility.Modifying;
                using (x)
            };
        ''')
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )

    async def test_edgeql_functions_inline_modifying_cardinality_02(self):
        await self.con.execute('''
            create function foo(x: int64) -> int64 {
                set volatility := schema::Volatility.Modifying;
                using (x)
            };
        ''')
        with self.assertRaisesRegex(
            edgedb.QueryError,
            'possibly an empty set passed as non-optional argument '
            'into modifying function'
        ):
            await self.con.execute('''
                select foo(<int64>{})
            ''')

    async def test_edgeql_functions_inline_modifying_cardinality_03(self):
        await self.con.execute('''
            create function foo(x: int64) -> int64 {
                set volatility := schema::Volatility.Modifying;
                using (x)
            };
        ''')
        with self.assertRaisesRegex(
            edgedb.QueryError,
            'possibly more than one element passed into modifying function'
        ):
            await self.con.execute('''
                select foo({1, 2, 3})
            ''')

    async def test_edgeql_functions_inline_modifying_cardinality_04(self):
        await self.con.execute('''
            create function foo(x: optional int64) -> optional int64 {
                set volatility := schema::Volatility.Modifying;
                using (x)
            };
        ''')
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )

    async def test_edgeql_functions_inline_modifying_cardinality_05(self):
        await self.con.execute('''
            create function foo(x: optional int64) -> optional int64 {
                set volatility := schema::Volatility.Modifying;
                using (x)
            };
        ''')
        await self.assert_query_result(
            'select foo(<int64>{})',
            [],
        )

    async def test_edgeql_functions_inline_modifying_cardinality_06(self):
        await self.con.execute('''
            create function foo(x: optional int64) -> optional int64 {
                set volatility := schema::Volatility.Modifying;
                using (x)
            };
        ''')
        with self.assertRaisesRegex(
            edgedb.QueryError,
            'possibly more than one element passed into modifying function'
        ):
            await self.con.execute('''
                select foo({1, 2, 3})
            ''')

    async def test_edgeql_functions_inline_insert_basic_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo() -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := 1 }));
            };
        ''')

        await self.assert_query_result(
            'select foo().a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

    async def test_edgeql_functions_inline_insert_basic_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x }))
            };
        ''')

        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

    async def test_edgeql_functions_inline_insert_basic_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using ((insert Bar{ a := x }).a)
            };
        ''')

        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

    async def test_edgeql_functions_inline_insert_basic_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x + 1 }))
            };
        ''')

        await self.assert_query_result(
            'select foo(1).a',
            [2],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2],
        )

    async def test_edgeql_functions_inline_insert_basic_05(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using ((insert Bar{ a := 2 * x + 1 }).a + 10)
            };
        ''')

        await self.assert_query_result(
            'select foo(1)',
            [13],
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )

    async def test_edgeql_functions_inline_insert_basic_06(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64 = 0) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x }))
            };
        ''')

        await self.assert_query_result(
            'select foo().a',
            [0],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0],
        )

        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1],
        )

    async def test_edgeql_functions_inline_insert_basic_07(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: optional int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x ?? 0 }))
            };
        ''')

        await self.assert_query_result(
            'select foo(<int64>{}).a',
            [0],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0],
        )

        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_basic_08(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(named only x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x }))
            };
        ''')

        await self.assert_query_result(
            'select foo(x := 1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

    async def test_edgeql_functions_inline_insert_basic_09(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(variadic x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := sum(array_unpack(x)) }))
            };
        ''')

        await self.assert_query_result(
            'select foo().a',
            [0],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0],
        )

        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1],
            sort=True,
        )

        await self.assert_query_result(
            'select foo(2, 3).a',
            [5],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 5],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_basic_10(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
                create required property b -> int64;
            };
            create function foo(x: int64, y: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x, b := y }))
            };
        ''')

        await self.assert_query_result(
            'select foo(1, 10){a, b}'
            'order by .a then .b',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [{'a': 1, 'b': 10}],
        )

    async def test_edgeql_functions_inline_insert_iterator_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x }))
            };
        ''')

        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4} union (select foo(x).a)',
            [2, 3, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4],
            sort=True,
        )

        await self.assert_query_result(
            'select if true then foo(5).a else 99',
            [5],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4, 5],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then foo(6).a else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4, 5],
            sort=True,
        )
        await self.assert_query_result(
            'select if true then 99 else foo(7).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4, 5],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then 99 else foo(8).a',
            [8],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4, 5, 8],
            sort=True,
        )

        await self.assert_query_result(
            'select foo(9).a ?? 99',
            [9],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4, 5, 8, 9],
            sort=True,
        )
        await self.assert_query_result(
            'select 99 ?? foo(10).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4, 5, 8, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_iterator_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
                create required property b -> int64;
            };
            create function foo(x: int64, y: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x, b := y }))
            };
        ''')

        await self.assert_query_result(
            'select foo(1, 10){a, b}'
            'order by .a then .b',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [{'a': 1, 'b': 10}],
        )

        await self.assert_query_result(
            'select ('
            '    for x in {2, 3} union('
            '        for y in {20, 30} union('
            '            select foo(x, y)'
            '        )'
            '    )'
            '){a, b}'
            'order by .a then .b',
            [
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
            ],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
            ],
        )

        await self.assert_query_result(
            'select ('
            '    if true'
            '    then foo(5, 50)'
            '    else (select Bar filter .a = 1)'
            '){a, b}'
            'order by .a then .b',
            [{'a': 5, 'b': 50}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
                {'a': 5, 'b': 50},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then foo(6, 60)'
            '    else (select Bar filter .a = 1)'
            '){a, b}'
            'order by .a then .b',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
                {'a': 5, 'b': 50},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then (select Bar filter .a = 1)'
            '    else foo(7, 70)'
            '){a, b}'
            'order by .a then .b',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
                {'a': 5, 'b': 50},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then (select Bar filter .a = 1)'
            '    else foo(8, 80)'
            '){a, b}'
            'order by .a then .b',
            [{'a': 8, 'b': 80}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
                {'a': 5, 'b': 50},
                {'a': 8, 'b': 80},
            ],
        )

        await self.assert_query_result(
            'select (foo(9, 90) ?? (select Bar filter .a = 1)){a, b}',
            [{'a': 9, 'b': 90}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
                {'a': 5, 'b': 50},
                {'a': 8, 'b': 80},
                {'a': 9, 'b': 90},
            ],
        )
        await self.assert_query_result(
            'select ((select Bar filter .a = 1) ?? foo(10, 100)){a, b}',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Bar{a, b}'
            'order by .a then .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
                {'a': 5, 'b': 50},
                {'a': 8, 'b': 80},
                {'a': 9, 'b': 90},
            ],
        )

    async def test_edgeql_functions_inline_insert_iterator_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using (
                    for y in {x, x + 1, x + 2} union (
                        (insert Bar{ a := y })
                    )
                )
            };
        ''')

        await self.assert_query_result(
            'select foo(1).a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )

        await self.assert_query_result(
            'for x in {11, 21, 31} union (select foo(x).a)',
            [11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33],
            sort=True,
        )

        await self.assert_query_result(
            'select if true then foo(51).a else 99',
            [51, 52, 53],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                1, 2, 3,
                11, 12, 13,
                21, 22, 23,
                31, 32, 33,
                51, 52, 53,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then foo(61).a else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                1, 2, 3,
                11, 12, 13,
                21, 22, 23,
                31, 32, 33,
                51, 52, 53,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select if true then 99 else foo(71).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                1, 2, 3,
                11, 12, 13,
                21, 22, 23,
                31, 32, 33,
                51, 52, 53,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then 99 else foo(81).a',
            [81, 82, 83],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                1, 2, 3,
                11, 12, 13,
                21, 22, 23,
                31, 32, 33,
                51, 52, 53,
                81, 82, 83,
            ],
            sort=True,
        )

        await self.assert_query_result(
            'select foo(91).a ?? 99',
            [91, 92, 93],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                1, 2, 3,
                11, 12, 13,
                21, 22, 23,
                31, 32, 33,
                51, 52, 53,
                81, 82, 83,
                91, 92, 93,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select 99 ?? foo(101).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                1, 2, 3,
                11, 12, 13,
                21, 22, 23,
                31, 32, 33,
                51, 52, 53,
                81, 82, 83,
                91, 92, 93,
            ],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_iterator_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: bool, y: int64) -> optional Bar {
                set is_inlined := true;
                using (
                    if x then (insert Bar{ a := y }) else <Bar>{}
                )
            };
        ''')

        await self.assert_query_result(
            'select foo(false, 0).a',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )
        await self.assert_query_result(
            'select foo(true, 1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4, 5} union (select foo(x % 2 = 0, x).a)',
            [2, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4],
            sort=True,
        )

        await self.assert_query_result(
            'select if true then foo(false, 6).a else 99',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select if true then foo(true, 6).a else 99',
            [6],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then foo(false, 7).a else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then foo(true, 7).a else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select if true then 99 else foo(false, 8).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select if true then 99 else foo(true, 8).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then 99 else foo(false, 9).a',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then 99 else foo(true, 9).a',
            [9],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6, 9],
            sort=True,
        )

        await self.assert_query_result(
            'select foo(false, 10).a ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6, 9],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(true, 10).a ?? 99',
            [10],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6, 9, 10],
            sort=True,
        )
        await self.assert_query_result(
            'select 99 ?? foo(false, 11).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6, 9, 10],
            sort=True,
        )
        await self.assert_query_result(
            'select 99 ?? foo(true, 11).a',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 4, 6, 9, 10],
            sort=True,
        )

    @unittest.skip('Cannot correlate same set inside and outside DML')
    async def test_edgeql_functions_inline_insert_correlate_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> tuple<Bar, int64> {
                set is_inlined := true;
                using (((insert Bar{ a := x }), x))
            };
        ''')

        await self.assert_query_result(
            'select foo(1)',
            [[[], 1]],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4} union (select foo(x).a)',
            [2, 3, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4],
            sort=True,
        )

    @unittest.skip('Cannot correlate same set inside and outside DML')
    async def test_edgeql_functions_inline_insert_correlate_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> int64 {
                set is_inlined := true;
                using ((insert Bar{ a := 2 * x + 1 }).a + x * x)
            };
        ''')

        await self.assert_query_result(
            'select foo(1)',
            [4],
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4} union (select foo(x))',
            [9, 16, 25],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3, 5, 7, 9],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_correlate_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> tuple<int64, int64> {
                set is_inlined := true;
                using ((
                    (insert Bar{ a := x }).a,
                    (insert Bar{ a := x + 1 }).a,
                ))
            };
        ''')

        await self.assert_query_result(
            'select foo(1)',
            [[1, 2]],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2],
            sort=True,
        )

        await self.assert_query_result(
            'for x in {11, 21, 31} union (select foo(x))',
            [[11, 12], [21, 22], [31, 32]],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 11, 12, 21, 22, 31, 32],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_correlate_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64, y: int64) -> tuple<int64, int64> {
                set is_inlined := true;
                using ((
                    (insert Bar{ a := x }).a,
                    (insert Bar{ a := y }).a,
                ))
            };
        ''')

        await self.assert_query_result(
            'select foo(1, 2)',
            [[1, 2]],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2],
            sort=True,
        )

        await self.assert_query_result(
            'for x in {1, 5} union ('
            '    for y in {10, 20} union ('
            '        select foo(x + y, x + y + 1)'
            '    )'
            ')',
            [[11, 12], [15, 16], [21, 22], [25, 26]],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 11, 12, 15, 16, 21, 22, 25, 26],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_correlate_05(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64, y: int64) -> int64 {
                set is_inlined := true;
                using ((insert Bar{ a := 2 * x + 1 }).a + y)
            };
        ''')

        await self.assert_query_result(
            'select foo(1, 10)',
            [13],
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )

        await self.assert_query_result(
            'for x in {2, 3} union('
            '    for y in {20, 30} union('
            '        select foo(x, y)'
            '    )'
            ')',
            [25, 27, 35, 37],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3, 5, 5, 7, 7],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_conflict_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
                create constraint exclusive on (.a)
            };
            create function foo(x: int64) -> Bar {
                set is_inlined := true;
                using ((
                    insert Bar{a := x}
                    unless conflict on .a
                    else ((update Bar set {a := x + 10}))
                ))
            };
        ''')

        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x).a)',
            [2, 3, 11],
            sort=True
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3, 11],
        )

    async def test_edgeql_functions_inline_insert_conflict_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create type Baz {
                create link bar -> Bar;
                create constraint exclusive on (.bar)
            };
            create function foo(x: Bar) -> Baz {
                set is_inlined := true;
                using ((
                    insert Baz{bar := x}
                    unless conflict on .bar
                    else ((
                        update Baz set {bar := (insert Bar{a := x.a + 10})}
                    ))
                ))
            };
        ''')

        await self.assert_query_result(
            'select foo('
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            ').bar.a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
        )
        await self.assert_query_result(
            'select Baz.bar.a',
            [1],
        )

        await self.assert_query_result(
            'for x in {1, 2, 3} union ('
            '    select foo('
            '        assert_exists((select Bar filter .a = x limit 1))'
            '    ).bar.a'
            ')',
            [2, 3, 11],
            sort=True
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 11],
        )
        await self.assert_query_result(
            'select Baz.bar.a',
            [2, 3, 11],
        )

    async def test_edgeql_functions_inline_insert_link_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(n: int64, x: Bar) -> Baz {
                set is_inlined := true;
                using ((insert Baz{ b := n, bar := x }))
            };
        ''')

        await self.assert_query_result(
            'select foo('
            '    4,'
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b}',
            [{'a': 1, 'b': 4}],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a',
            [{'a': 1, 'b': 4}],
        )

        await self.assert_query_result(
            'select foo('
            '    5,'
            '    assert_exists((select Bar filter .a = 2 limit 1))'
            '){a := .bar.a, b}',
            [{'a': 2, 'b': 5}],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
            ],
        )

    async def test_edgeql_functions_inline_insert_link_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create multi link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: int64, y: int64) -> Baz {
                set is_inlined := true;
                using (
                    (insert Baz{
                        b := x,
                        bar := (select Bar filter .a <= y),
                    })
                );
            };
        ''')

        await self.assert_query_result(
            'select foo(4, 1){a := .bar.a, b}',
            [{'a': [1], 'b': 4}],
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [{'a': [1], 'b': 4}],
        )

        await self.assert_query_result(
            'select foo(5, 2){a := .bar.a, b}',
            [{'a': [1, 2], 'b': 5}],
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [
                {'a': [1], 'b': 4},
                {'a': [1, 2], 'b': 5},
            ],
        )

    async def test_edgeql_functions_inline_insert_link_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            create function foo(x: int64, y: int64) -> Baz {
                set is_inlined := true;
                using (
                    (insert Baz {
                        b := y,
                        bar := (insert Bar{ a := x })
                    })
                );
            };
        ''')

        await self.assert_query_result(
            'select foo(1, 4).b',
            [4],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b',
            [{'a': 1, 'b': 4}],
        )

        await self.assert_query_result(
            'select foo(2, 5).b',
            [5],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2],
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
            ],
        )

    async def test_edgeql_functions_inline_insert_link_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            create function foo(x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar {a := x}))
            };
        ''')

        await self.assert_query_result(
            'select (insert Baz{b := 4, bar := foo(1)})'
            '{a := .bar.a, b} order by .b',
            [{'a': 1, 'b': 4}],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b',
            [{'a': 1, 'b': 4}],
        )

        await self.assert_query_result(
            'select (insert Baz{b := 5, bar := foo(2)})'
            '{a := .bar.a, b} order by .b',
            [{'a': 2, 'b': 5}],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2],
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
            ],
        )

    async def test_edgeql_functions_inline_insert_link_iterator_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Bar{a := 4};
            create function foo(n: int64, x: Bar) -> Baz {
                set is_inlined := true;
                using ((insert Baz{ b := n, bar := x }))
            };
        ''')

        await self.assert_query_result(
            'select foo('
            '    1, assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b} order by .a then .b',
            [{'a': 1, 'b': 1}],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [{'a': 1, 'b': 1}],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4} union ('
            '    select foo('
            '        x, assert_exists((select Bar filter .a = 2 limit 1))'
            '    ).b'
            ')',
            [2, 3, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
            ],
        )

        await self.assert_query_result(
            'select ('
            '    if true'
            '    then foo('
            '        5, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).b'
            '    else 99'
            ')',
            [5],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then foo('
            '        6, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).b'
            '    else 99'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then 99'
            '    else foo('
            '        7, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).b'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then 99'
            '    else foo('
            '        8, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).b'
            ')',
            [8],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
                {'a': 3, 'b': 8},
            ],
        )

        await self.assert_query_result(
            'select foo('
            '    9, assert_exists((select Bar filter .a = 4 limit 1))'
            ').b ?? 99',
            [9],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
                {'a': 3, 'b': 8},
                {'a': 4, 'b': 9},
            ],
        )
        await self.assert_query_result(
            'select 99 ?? foo('
            '    9, assert_exists((select Bar filter .a = 4 limit 1))'
            ').b',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
                {'a': 3, 'b': 8},
                {'a': 4, 'b': 9},
            ],
        )

    async def test_edgeql_functions_inline_insert_link_iterator_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create multi link bar -> Bar;
            };
            create function foo(x: int64, y: int64) -> Baz {
                set is_inlined := true;
                using (
                    (insert Baz {
                        b := y,
                        bar := (for z in {x, x + 1, x + 2} union(
                            (insert Bar{ a := z })
                        ))
                    })
                );
            };
        ''')

        await self.assert_query_result(
            'select foo(10, 1).b',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [10, 11, 12],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [{'a': [10, 11, 12], 'b': 1}],
        )

        await self.assert_query_result(
            'for x in {20, 30} union ('
            '    for y in {2, 3} union ('
            '        select foo(x, y).b'
            '    )'
            ')',
            [2, 2, 3, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                10, 11, 12,
                20, 20, 21, 21, 22, 22,
                30, 30, 31, 31, 32, 32,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [30, 31, 32], 'b': 2},
                {'a': [20, 21, 22], 'b': 3},
                {'a': [30, 31, 32], 'b': 3},
            ],
        )

        await self.assert_query_result(
            'select if true then foo(40, 4).b else 999',
            [4],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                10, 11, 12,
                20, 20, 21, 21, 22, 22,
                30, 30, 31, 31, 32, 32,
                40, 41, 42,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [30, 31, 32], 'b': 2},
                {'a': [20, 21, 22], 'b': 3},
                {'a': [30, 31, 32], 'b': 3},
                {'a': [40, 41, 42], 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select if false then foo(50, 5).b else 999',
            [999],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                10, 11, 12,
                20, 20, 21, 21, 22, 22,
                30, 30, 31, 31, 32, 32,
                40, 41, 42,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [30, 31, 32], 'b': 2},
                {'a': [20, 21, 22], 'b': 3},
                {'a': [30, 31, 32], 'b': 3},
                {'a': [40, 41, 42], 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select if true then 999 else foo(60, 6).b',
            [999],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                10, 11, 12,
                20, 20, 21, 21, 22, 22,
                30, 30, 31, 31, 32, 32,
                40, 41, 42,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [30, 31, 32], 'b': 2},
                {'a': [20, 21, 22], 'b': 3},
                {'a': [30, 31, 32], 'b': 3},
                {'a': [40, 41, 42], 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select if false then 999 else foo(70, 7).b',
            [7],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                10, 11, 12,
                20, 20, 21, 21, 22, 22,
                30, 30, 31, 31, 32, 32,
                40, 41, 42,
                70, 71, 72,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [30, 31, 32], 'b': 2},
                {'a': [20, 21, 22], 'b': 3},
                {'a': [30, 31, 32], 'b': 3},
                {'a': [40, 41, 42], 'b': 4},
                {'a': [70, 71, 72], 'b': 7},
            ],
        )

        await self.assert_query_result(
            'select foo(80, 8).b ?? 999',
            [8],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                10, 11, 12,
                20, 20, 21, 21, 22, 22,
                30, 30, 31, 31, 32, 32,
                40, 41, 42,
                70, 71, 72,
                80, 81, 82,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [30, 31, 32], 'b': 2},
                {'a': [20, 21, 22], 'b': 3},
                {'a': [30, 31, 32], 'b': 3},
                {'a': [40, 41, 42], 'b': 4},
                {'a': [70, 71, 72], 'b': 7},
                {'a': [80, 81, 82], 'b': 8},
            ],
        )
        await self.assert_query_result(
            'select 999 ?? foo(90, 9).b',
            [999],
        )
        await self.assert_query_result(
            'select Bar.a',
            [
                10, 11, 12,
                20, 20, 21, 21, 22, 22,
                30, 30, 31, 31, 32, 32,
                40, 41, 42,
                70, 71, 72,
                80, 81, 82,
            ],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {a := .bar.a, b} order by .b then sum(.a)',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [30, 31, 32], 'b': 2},
                {'a': [20, 21, 22], 'b': 3},
                {'a': [30, 31, 32], 'b': 3},
                {'a': [40, 41, 42], 'b': 4},
                {'a': [70, 71, 72], 'b': 7},
                {'a': [80, 81, 82], 'b': 8},
            ],
        )

    async def test_edgeql_functions_inline_insert_link_iterator_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Bar{a := 4};
            create function foo(n: int64, x: Bar, flag: bool) -> optional Baz {
                set is_inlined := true;
                using (
                    if flag then (insert Baz{ b := n, bar := x }) else <Baz>{}
                )
            };
        ''')

        await self.assert_query_result(
            'select foo('
            '    0, assert_exists((select Bar filter .a = 1 limit 1)), false'
            '){a := .bar.a, b} order by .a then .b',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [],
        )
        await self.assert_query_result(
            'select foo('
            '    1, assert_exists((select Bar filter .a = 1 limit 1)), true'
            '){a := .bar.a, b} order by .a then .b',
            [{'a': 1, 'b': 1}],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [{'a': 1, 'b': 1}],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4} union ('
            '    select foo('
            '        x,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        false,'
            '    ).b'
            ')',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [{'a': 1, 'b': 1}],
        )
        await self.assert_query_result(
            'for x in {2, 3, 4} union ('
            '    select foo('
            '        x,'
            '        assert_exists((select Bar filter .a = 2 limit 1)),'
            '        true,'
            '    ).b'
            ')',
            [2, 3, 4],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
            ],
        )

        await self.assert_query_result(
            'select ('
            '    if true'
            '    then foo('
            '        5,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        false,'
            '    ).b'
            '    else 99'
            ')',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then foo('
            '        6,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        false,'
            '    ).b'
            '    else 99'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then 99'
            '    else foo('
            '        7,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        false,'
            '    ).b'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then 99'
            '    else foo('
            '        8,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        false,'
            '    ).b'
            ')',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then foo('
            '        9,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        true,'
            '    ).b'
            '    else 99'
            ')',
            [9],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then foo('
            '        10,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        true,'
            '    ).b'
            '    else 99'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then 99'
            '    else foo('
            '        11,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        true,'
            '    ).b'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then 99'
            '    else foo('
            '        12,'
            '        assert_exists((select Bar filter .a = 3 limit 1)),'
            '        true,'
            '    ).b'
            ')',
            [12],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
                {'a': 3, 'b': 12},
            ],
        )

        await self.assert_query_result(
            'select foo('
            '    13, assert_exists((select Bar filter .a = 4 limit 1)), false'
            ').b ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
                {'a': 3, 'b': 12},
            ],
        )
        await self.assert_query_result(
            'select 99 ?? foo('
            '    14, assert_exists((select Bar filter .a = 4 limit 1)), false'
            ').b',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
                {'a': 3, 'b': 12},
            ],
        )
        await self.assert_query_result(
            'select foo('
            '    15, assert_exists((select Bar filter .a = 4 limit 1)), true'
            ').b ?? 99',
            [15],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
                {'a': 3, 'b': 12},
                {'a': 4, 'b': 15},
            ],
        )
        await self.assert_query_result(
            'select 99 ?? foo('
            '    16, assert_exists((select Bar filter .a = 4 limit 1)), true'
            ').b',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 9},
                {'a': 3, 'b': 12},
                {'a': 4, 'b': 15},
            ],
        )

    async def test_edgeql_functions_inline_insert_linkprop_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar {
                    create property b -> int64;
                }
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(x: Bar) -> Baz {
                set is_inlined := true;
                using ((insert Baz{ bar := x { @b := 10 } }))
            };
        ''')

        await self.assert_query_result(
            'select foo('
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b := .bar@b}',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a',
            [{'a': 1, 'b': 10}],
        )

    async def test_edgeql_functions_inline_insert_linkprop_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar {
                    create property b -> int64;
                }
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            create function foo(n: int64, x: Bar) -> Baz {
                set is_inlined := true;
                using ((insert Baz{ bar := x { @b := n } }))
            };
        ''')

        await self.assert_query_result(
            'select foo('
            '    4,'
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b := .bar@b}',
            [{'a': 1, 'b': 4}],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a',
            [{'a': 1, 'b': 4}],
        )

    async def test_edgeql_functions_inline_insert_linkprop_iterator_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar {
                    create property b -> int64;
                }
            };
            insert Bar{a := 1};
            insert Bar{a := 2};
            insert Bar{a := 3};
            insert Bar{a := 4};
            create function foo(n: int64, x: Bar) -> Baz {
                set is_inlined := true;
                using ((insert Baz{ bar := x { @b := n } }))
            };
        ''')

        await self.assert_query_result(
            'select foo('
            '    1,'
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b := .bar@b}',
            [{'a': 1, 'b': 1}],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a',
            [{'a': 1, 'b': 1}],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4} union ('
            '    select foo('
            '        x, assert_exists((select Bar filter .a = 2 limit 1))'
            '    ).bar@b'
            ')',
            [2, 3, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
            ],
        )

        await self.assert_query_result(
            'select ('
            '    if true'
            '    then foo('
            '        5, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).bar@b'
            '    else 99'
            ')',
            [5],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then foo('
            '        6, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).bar@b'
            '    else 99'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then 99'
            '    else foo('
            '        7, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).bar@b'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then 99'
            '    else foo('
            '        8, assert_exists((select Bar filter .a = 3 limit 1))'
            '    ).bar@b'
            ')',
            [8],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
                {'a': 3, 'b': 8},
            ],
        )

        await self.assert_query_result(
            'select foo('
            '    9, assert_exists((select Bar filter .a = 4 limit 1))'
            ').bar@b ?? 99',
            [9],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
                {'a': 3, 'b': 8},
                {'a': 4, 'b': 9},
            ],
        )
        await self.assert_query_result(
            'select 99 ?? foo('
            '    9, assert_exists((select Bar filter .a = 4 limit 1))'
            ').bar@b',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a then .b',
            [
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 2, 'b': 3},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': 5},
                {'a': 3, 'b': 8},
                {'a': 4, 'b': 9},
            ],
        )

    async def test_edgeql_functions_inline_insert_nested_01(self):
        # Simple inner modifying function
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function inner(x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x }));
            };
            create function foo(x: int64) -> Bar {
                set is_inlined := true;
                using (inner(x));
            };
        ''')

        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )

        await self.assert_query_result(
            'for x in {2, 3, 4} union (foo(x).a)',
            [2, 3, 4],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3, 4],
            sort=True,
        )

    async def test_edgeql_functions_inline_insert_nested_02(self):
        # Putting the result of an inner modifying function into shape
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create required link bar -> Bar;
            };
            create function inner1(x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x }))
            };
            create function inner2(x: int64, y: int64) -> Baz {
                set is_inlined := true;
                using ((insert Baz{ b := y, bar := inner1(x) }))
            };
            create function foo(x: int64, y: int64) -> Baz {
                set is_inlined := true;
                using (inner2(x, y))
            };
        ''')

        await self.assert_query_result(
            'select foo(1, 10){a := .bar.a, b := .b}',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .b} order by .a',
            [{'a': 1, 'b': 10}],
        )

        await self.assert_query_result(
            'select ('
            '    for x in {2, 3} union ('
            '        for y in {20, 30} union ('
            '            foo(x, y){a := .bar.a, b := .b}'
            '        )'
            '    )'
            ') order by .a then .b',
            [
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 2, 3, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .b} order by .a',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
            ],
        )

    async def test_edgeql_functions_inline_insert_nested_03(self):
        # Putting the result of an inner modifying function into shape with
        # link property
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar {
                    create property b -> int64;
                };
            };
            create function inner1(x: int64) -> Bar {
                set is_inlined := true;
                using ((insert Bar{ a := x }))
            };
            create function inner2(x: int64, y: int64) -> Baz {
                set is_inlined := true;
                using ((insert Baz{ bar := inner1(x){ @b := y } }))
            };
            create function foo(x: int64, y: int64) -> Baz {
                set is_inlined := true;
                using (inner2(x, y))
            };
        ''')

        await self.assert_query_result(
            'select foo(1, 10){a := .bar.a, b := .bar@b}',
            [{'a': 1, 'b': 10}],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a',
            [{'a': 1, 'b': 10}],
        )

        await self.assert_query_result(
            'select ('
            '    for x in {2, 3} union ('
            '        for y in {20, 30} union ('
            '            foo(x, y){a := .bar.a, b := .bar@b}'
            '        )'
            '    )'
            ') order by .a then .b',
            [
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 2, 3, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': 2, 'b': 30},
                {'a': 3, 'b': 20},
                {'a': 3, 'b': 30},
            ],
        )

    async def test_edgeql_functions_inline_update_basic_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using ((update Bar set { a := x }));
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(1).a',
            [1, 1, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 1, 1],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_basic_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64, y: int64) -> set of int64 {
                set is_inlined := true;
                using ((update Bar filter .a <= y set { a := x }).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0, 0)',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1)',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2)',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 3)',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_basic_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(
                named only m: int64,
                named only n: int64,
            ) -> set of int64 {
                set is_inlined := true;
                using ((update Bar filter .a <= n set { a := m }).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(m := 0, n := 0)',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(m := 0, n := 1)',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(m := 0, n := 2)',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(m := 0, n := 3)',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_basic_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(
                x: optional int64,
                y: optional int64,
            ) -> set of int64 {
                set is_inlined := true;
                using ((update Bar filter .a <= y ?? 9 set { a := x ?? 9 }).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(<int64>{}, <int64>{})',
            [9, 9, 9],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [9, 9, 9],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(<int64>{}, 2)',
            [9, 9],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3, 9, 9],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2, <int64>{})',
            [2, 2, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 2, 2],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0, 0)',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1)',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2)',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 3)',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_basic_05(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(
                x: int64,
                variadic y: int64,
            ) -> set of int64 {
                set is_inlined := true;
                using (
                    (
                        update Bar
                        filter .a <= sum(array_unpack(y))
                        set { a := x }
                    ).a
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1)',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1, 2)',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_iterator_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64, y: int64) -> set of int64 {
                set is_inlined := true;
                using ((update Bar filter .a <= y set { a := x }).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0, 0)',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1)',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2)',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 3)',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(0, x))',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(0, x))',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x - 1, 0))',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x - 1, 3))',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {1} union (select foo(x - 1, x))',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {2, 3} union (select foo(x - 1, x))',
            [1, 1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 1, 2],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x - 1, x))',
            [0, 1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 2],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(0, 2) else 99',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(0, 2) else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(0, 2)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(0, 2)',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0, 0) ?? 99',
            [99],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2) ?? 99',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(0, 2)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_iterator_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64, y: int64) -> set of int64 {
                set is_inlined := true;
                using (
                    for z in {0, 1} union (
                        (update Bar filter .a <= y + z set { a := x + z }).a
                    )
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0, 0)',
            [1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1)',
            [0, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2)',
            [0, 0, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 1],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 3)',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(0, x))',
            [1, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 1, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(0, x))',
            [0, 1, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 1],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x - 1, 0))',
            [1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x - 1, 3))',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {1} union (select foo(x - 1, x))',
            [0, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {2, 3} union (select foo(x - 1, x))',
            [1, 1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 1, 2],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x - 1, x))',
            [0, 1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 2],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(0, 1) else 99',
            [0, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(0, 1) else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(0, 1)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(0, 1)',
            [0, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 3],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0, -1) ?? 99',
            [99],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1) ?? 99',
            [0, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(0, 1)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_iterator_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(
                x: int64, y: int64, z: bool
            ) -> set of int64 {
                set is_inlined := true;
                using (
                    if z
                    then (update Bar filter .a <= y set { a := x }).a
                    else <int64>{}
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2, false)',
            [],
        )
        await self.assert_query_result(
            'select foo(0, 3, false)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2, true)',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 3, true)',
            [0, 0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 0],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(0, x, false))',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'for x in {2, 3} union (select foo(x - 1, x, false))',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(0, x, true))',
            [0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {2, 3} union (select foo(x - 1, x, true))',
            [1, 1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 1, 2],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(0, 2, false) else 99',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select if false then foo(0, 2, false) else 99',
            [99],
        )
        await self.assert_query_result(
            'select if true then 99 else foo(0, 2, false)',
            [99],
        )
        await self.assert_query_result(
            'select if false then 99 else foo(0, 2, false)',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then foo(0, 2, true) else 99',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(0, 2, true) else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(0, 2, true)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(0, 2, true)',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0, 0, false) ?? 99',
            [99],
            sort=True,
        )
        await self.assert_query_result(
            'select foo(0, 2, false) ?? 99',
            [99],
            sort=True,
        )
        await self.assert_query_result(
            'select 99 ?? foo(0, 2, false)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 0, true) ?? 99',
            [99],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 2, true) ?? 99',
            [0, 0],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 0, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(0, 2, true)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_link_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar;
            };
            create function foo(n: int64, x: Bar) -> set of Baz {
                set is_inlined := true;
                using ((update Baz filter .b <= n set { bar := x }))
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
                insert Baz{b := 4};
                insert Baz{b := 5};
                insert Baz{b := 6};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo('
            '    4,'
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b}',
            [
                {'a': 1, 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': None, 'b': 5},
                {'a': None, 'b': 6},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo('
            '    5,'
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b}',
            [
                {'a': 1, 'b': 4},
                {'a': 1, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': 1, 'b': 5},
                {'a': None, 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_update_link_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create multi link bar -> Bar;
            };
            create function foo(x: int64, y: int64) -> set of Baz {
                set is_inlined := true;
                using (
                    (update Baz filter .b <= x set {
                        bar := (select Bar filter .a <= y),
                    })
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
                insert Baz{b := 4};
                insert Baz{b := 5};
                insert Baz{b := 6};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(4, 1){a := .bar.a, b}',
            [
                {'a': [1], 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [
                {'a': [1], 'b': 4},
                {'a': [], 'b': 5},
                {'a': [], 'b': 6},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(5, 2){a := .bar.a, b}',
            [
                {'a': [1, 2], 'b': 4},
                {'a': [1, 2], 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [
                {'a': [1, 2], 'b': 4},
                {'a': [1, 2], 'b': 5},
                {'a': [], 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_update_link_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create optional link bar -> Bar;
            };
            create function foo(x: int64, y: int64) -> set of Baz {
                set is_inlined := true;
                using (
                    (update Baz filter .b <= x set {
                        bar := (insert Bar{a := y}),
                    })
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 4};
                insert Baz{b := 5};
                insert Baz{b := 6};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(4, 1){a := .bar.a, b}',
            [
                {'a': 1, 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': None, 'b': 5},
                {'a': None, 'b': 6},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(5, 2){a := .bar.a, b}',
            [
                {'a': 2, 'b': 4},
                {'a': 2, 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 2],
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [
                {'a': 2, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': None, 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_update_link_iterator_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar;
            };
            create function foo(n: int64, x: Bar) -> set of Baz {
                set is_inlined := true;
                using ((update Baz filter .b = n set { bar := x }))
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
                insert Bar{a := 4};
                insert Baz{b := 10};
                insert Baz{b := 20};
                insert Baz{b := 30};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo('
            '    10,'
            '    assert_exists((select Bar filter .a = 1 limit 1))'
            '){a := .bar.a, b}',
            [
                {'a': 1, 'b': 10},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 10},
                {'a': None, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select ('
            '    for x in {1, 2} union('
            '        select foo('
            '            x * 10,'
            '            assert_exists((select Bar filter .a = x limit 1))'
            '        ).b'
            '    )'
            ')',
            [10, 20],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 10},
                {'a': 2, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then foo('
            '        10,'
            '        assert_exists((select Bar filter .a = 1 limit 1)),'
            '    ).b'
            '    else 99'
            ')',
            [10],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 10},
                {'a': None, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then foo('
            '        10,'
            '        assert_exists((select Bar filter .a = 1 limit 1)),'
            '    ).b'
            '    else 99'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 10},
                {'a': None, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select ('
            '    if true'
            '    then 99'
            '    else foo('
            '        10,'
            '        assert_exists((select Bar filter .a = 1 limit 1)),'
            '    ).b'
            ')',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 10},
                {'a': None, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select ('
            '    if false'
            '    then 99'
            '    else foo('
            '        10,'
            '        assert_exists((select Bar filter .a = 1 limit 1)),'
            '    ).b'
            ')',
            [10],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 10},
                {'a': None, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo('
            '    10,'
            '    assert_exists((select Bar filter .a = 1 limit 1)),'
            ').b ?? 99',
            [10],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 10},
                {'a': None, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo('
            '    10,'
            '    assert_exists((select Bar filter .a = 1 limit 1)),'
            ').b',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 10},
                {'a': None, 'b': 20},
                {'a': None, 'b': 30},
            ],
        )

    async def test_edgeql_functions_inline_update_link_iterator_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create multi link bar -> Bar;
            };
            create function foo(x: int64, y: int64) -> set of Baz {
                set is_inlined := true;
                using ((
                    update Baz filter .b = x set {
                        bar := (for z in {y, y + 1, y + 2} union (
                               insert Bar{a := z}
                            )
                        )
                    }
                ))
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 1};
                insert Baz{b := 2};
                insert Baz{b := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(1, 10){a := .bar.a, b}',
            [
                {'a': [10, 11, 12], 'b': 1},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2} union (select foo(x, x * 10){a := .bar.a, b})',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [20, 21, 22], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(1, 10).b else 99',
            [1],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(1, 10).b else 99',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [], 'b': 1},
                {'a': [], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(1, 10).b',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [], 'b': 1},
                {'a': [], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(1, 10).b',
            [1],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(1, 10).b ?? 99',
            [1],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [10, 11, 12], 'b': 1},
                {'a': [], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(1, 10).b',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': [], 'b': 1},
                {'a': [], 'b': 2},
                {'a': [], 'b': 3},
            ],
        )

    async def test_edgeql_functions_inline_update_link_iterator_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar;
            };
            create function foo(x: int64, y: int64, flag: bool) -> set of Baz {
                set is_inlined := true;
                using ((
                    update Baz filter .b = x set {
                        bar := (
                            if flag
                            then (insert Bar{a := y})
                            else <Bar>{}
                        )
                    }
                ))
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 1};
                insert Baz{b := 2};
                insert Baz{b := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(1, 10, false){a := .bar.a, b}',
            [
                {'a': None, 'b': 1},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1, 10, true){a := .bar.a, b}',
            [
                {'a': 10, 'b': 1},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [10],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 10, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2} union ('
            '    select foo(x, x * 10, false){a := .bar.a, b}'
            ')',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2} union ('
            '    select foo(x, x * 10, true){a := .bar.a, b}'
            ')',
            [
                {'a': 10, 'b': 1},
                {'a': 20, 'b': 2},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 10, 'b': 1},
                {'a': 20, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(1, 10, false).bar.a else 99',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(1, 10, false).bar.a else 99',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(1, 10, false).bar.a',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(1, 10, false).bar.a',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then foo(1, 10, true).bar.a else 99',
            [10],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 10, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(1, 10, true).bar.a else 99',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(1, 10, true).bar.a',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(1, 10, true).bar.a',
            [10],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 10, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(1, 10, false).bar.a ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(1, 10, false).bar.a',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1, 10, true).bar.a ?? 99',
            [10],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 10, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(1, 10, true).bar.a',
            [99],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 1},
                {'a': None, 'b': 2},
                {'a': None, 'b': 3},
            ],
        )

    async def test_edgeql_functions_inline_update_linkprop_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required link bar -> Bar {
                    create property b -> int64;
                }
            };
            create function foo(x: int64, y: int64) -> set of Baz {
                set is_inlined := true;
                using ((
                    update Baz filter .bar.a <= x set {
                        bar := .bar { @b := y }
                    }
                ))
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{bar := (insert Bar{a := 1})};
                insert Baz{bar := (insert Bar{a := 2})};
                insert Baz{bar := (insert Bar{a := 3})};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(2, 4){a := .bar.a, b := .bar@b}',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b := .bar@b} order by .a',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 4},
                {'a': 3, 'b': None},
            ],
        )

    async def test_edgeql_functions_inline_update_nested_01(self):
        # Simple inner modifying function
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function inner(x: int64) -> set of Bar {
                set is_inlined := true;
                using ((update Bar set { a := x }));
            };
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using (inner(x));
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(1).a',
            [1, 1, 1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 1, 1],
            sort=True,
        )

    async def test_edgeql_functions_inline_update_nested_02(self):
        # Putting the result of an inner modifying function into shape
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create multi link bar -> Bar;
            };
            create function inner1(y: int64) -> set of Bar {
                set is_inlined := true;
                using ((update Bar filter .a <= y set { a := .a - 1 }));
            };
            create function inner2(x: int64, y: int64) -> set of Baz {
                set is_inlined := true;
                using (
                    (update Baz filter .b <= x set {
                        bar := assert_distinct(inner1(y)),
                    })
                );
            };
            create function foo(x: int64, y: int64) -> set of Baz {
                set is_inlined := true;
                using (inner2(x, y));
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
                insert Baz{b := 4};
                insert Baz{b := 5};
                insert Baz{b := 6};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(4, 1){a := .bar.a, b}',
            [
                {'a': [0], 'b': 4},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [
                {'a': [0], 'b': 4},
                {'a': [], 'b': 5},
                {'a': [], 'b': 6},
            ],
        )

        # Inner update will return an empty set for all subsequent calls.
        await reset_data()
        await self.assert_query_result(
            'select foo(5, 2){a := .bar.a, b}',
            [
                {'a': [0, 1], 'b': 4},
                {'a': [], 'b': 5},
            ],
        )
        await self.assert_query_result(
            'select Bar.a',
            [0, 1, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz {'
            '    a := (select .bar order by .a).a,'
            '    b,'
            '} order by .b',
            [
                {'a': [0, 1], 'b': 4},
                {'a': [], 'b': 5},
                {'a': [], 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_delete_basic_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using ((delete Bar filter .a <= x));
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2).a',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )

    async def test_edgeql_functions_inline_delete_basic_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using ((delete Bar filter .a <= x).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(3)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

    async def test_edgeql_functions_inline_delete_basic_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(named only m: int64) -> set of int64 {
                set is_inlined := true;
                using ((delete Bar filter .a <= m).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(m := 0)',
            [],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(m := 1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(m := 2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(m := 3)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

    async def test_edgeql_functions_inline_delete_basic_04(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: optional int64) -> set of int64 {
                set is_inlined := true;
                using ((delete Bar filter .a <= x ?? 9).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(<int64>{})',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(3)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

    async def test_edgeql_functions_inline_delete_basic_05(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(
                variadic x: int64,
            ) -> set of int64 {
                set is_inlined := true;
                using (
                    (
                        delete Bar
                        filter .a <= sum(array_unpack(x))
                    ).a
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, 1, 2)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

    async def test_edgeql_functions_inline_delete_iterator_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using ((delete Bar filter .a <= x).a);
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(3)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(x))',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(2) else 99',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(2) else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(2)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0) ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2) ?? 99',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(2)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_delete_iterator_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (
                    for z in {0, 1} union (
                        (delete Bar filter .a <= x).a
                    )
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(3)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(x))',
            [1],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {1, 2, 3} union (select foo(x))',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(2) else 99',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(2) else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(2)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0) ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2) ?? 99',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(2)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_delete_iterator_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function foo(
                x: int64, y: bool
            ) -> set of int64 {
                set is_inlined := true;
                using (
                    if y
                    then (delete Bar filter .a <= x).a
                    else <int64>{}
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(2, false)',
            [],
        )
        await self.assert_query_result(
            'select foo(3, false)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2, true)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(3, true)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(x, false))',
            [],
        )
        await self.assert_query_result(
            'for x in {2, 3} union (select foo(x, false))',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {0, 1} union (select foo(x, true))',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'for x in {2, 3} union (select foo(x, true))',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )

        await reset_data()
        await self.assert_query_result(
            'select if true then foo(2, false) else 99',
            [],
        )
        await self.assert_query_result(
            'select if false then foo(2, false) else 99',
            [99],
        )
        await self.assert_query_result(
            'select if true then 99 else foo(2, false)',
            [99],
        )
        await self.assert_query_result(
            'select if false then 99 else foo(2, false)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then foo(2, true) else 99',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then foo(2, true) else 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if true then 99 else foo(2, true)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select if false then 99 else foo(2, true)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
            sort=True,
        )

        await reset_data()
        await self.assert_query_result(
            'select foo(0, false) ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select foo(2, false) ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select 99 ?? foo(2, false)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(0, true) ?? 99',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2, true) ?? 99',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select 99 ?? foo(2, true)',
            [99],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )

    async def test_edgeql_functions_inline_delete_policy_target_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar {
                    on target delete allow;
                };
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (
                    (delete Bar filter .a <= x).a
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 4, bar := (insert Bar{a := 1})};
                insert Baz{b := 5, bar := (insert Bar{a := 2})};
                insert Baz{b := 6, bar := (insert Bar{a := 3})};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 4},
                {'a': None, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(3)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': None, 'b': 4},
                {'a': None, 'b': 5},
                {'a': None, 'b': 6},
            ],
        )

    async def test_edgeql_functions_inline_delete_policy_target_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar {
                    on target delete delete source;
                };
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (
                    (delete Bar filter .a <= x).a
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 4, bar := (insert Bar{a := 1})};
                insert Baz{b := 5, bar := (insert Bar{a := 2})};
                insert Baz{b := 6, bar := (insert Bar{a := 3})};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b}',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(1)',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b}',
            [
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2)',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b}',
            [
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(3)',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b}',
            [],
        )

    async def test_edgeql_functions_inline_delete_policy_source_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar {
                    on source delete allow;
                };
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (
                    (delete Baz filter .b <= x).b
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 4, bar := (insert Bar{a := 1})};
                insert Baz{b := 5, bar := (insert Bar{a := 2})};
                insert Baz{b := 6, bar := (insert Bar{a := 3})};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(4)',
            [4],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(5)',
            [4, 5],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(6)',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [],
        )

    async def test_edgeql_functions_inline_delete_policy_source_02(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar {
                    on source delete delete target;
                };
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (
                    (delete Baz filter .b <= x).b
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 4, bar := (insert Bar{a := 1})};
                insert Baz{b := 5, bar := (insert Bar{a := 2})};
                insert Baz{b := 6, bar := (insert Bar{a := 3})};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(4)',
            [4],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(5)',
            [4, 5],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 3, 'b': 6},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(6)',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [],
        )

    async def test_edgeql_functions_inline_delete_policy_source_03(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create type Baz {
                create required property b -> int64;
                create link bar -> Bar {
                    on source delete delete target if orphan;
                };
            };
            create function foo(x: int64) -> set of int64 {
                set is_inlined := true;
                using (
                    (delete Baz filter .b <= x).b
                );
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Baz;
                delete Bar;
                insert Baz{b := 4, bar := (insert Bar{a := 1})};
                insert Baz{b := 5, bar := (insert Bar{a := 2})};
                insert Baz{b := 6, bar := (insert Bar{a := 3})};
                insert Baz{
                    b := 7,
                    bar := assert_exists((select Bar filter .a = 1 limit 1)),
                };
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(0)',
            [],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 4},
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
                {'a': 1, 'b': 7},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(4)',
            [4],
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 2, 3],
            sort=True,
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 2, 'b': 5},
                {'a': 3, 'b': 6},
                {'a': 1, 'b': 7},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(5)',
            [4, 5],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1, 3],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 3, 'b': 6},
                {'a': 1, 'b': 7},
            ],
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(6)',
            [4, 5, 6],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [1],
        )
        await self.assert_query_result(
            'select Baz{a := .bar.a, b} order by .b',
            [
                {'a': 1, 'b': 7},
            ],
        )

    async def test_edgeql_functions_inline_delete_nested_01(self):
        await self.con.execute('''
            create type Bar {
                create required property a -> int64;
            };
            create function inner(x: int64) -> set of Bar {
                set is_inlined := true;
                using ((delete Bar filter .a <= x));
            };
            create function foo(x: int64) -> set of Bar {
                set is_inlined := true;
                using (inner(x));
            };
        ''')

        async def reset_data():
            await self.con.execute('''
                delete Bar;
                insert Bar{a := 1};
                insert Bar{a := 2};
                insert Bar{a := 3};
            ''')

        await reset_data()
        await self.assert_query_result(
            'select foo(1).a',
            [1],
        )
        await self.assert_query_result(
            'select Bar.a',
            [2, 3],
            sort=True,
        )
        await reset_data()
        await self.assert_query_result(
            'select foo(2).a',
            [1, 2],
            sort=True,
        )
        await self.assert_query_result(
            'select Bar.a',
            [3],
        )
