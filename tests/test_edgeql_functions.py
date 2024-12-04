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


import base64
import decimal
import json
import math
import os.path
import random
import uuid

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLFunctions(tb.QueryTestCase):
    NO_FACTOR = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.edgeql')

    async def test_edgeql_functions_count_01(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := (
                        # User is simply employed as an object to be augmented
                        SELECT User {
                            count := 4,
                            all_issues := Issue
                        } FILTER .name = 'Elvis'
                    )
                SELECT x.count = count(x.all_issues);
            """,
            [True]
        )

    async def test_edgeql_functions_count_02(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := (
                        # User is simply employed as an object to be augmented
                        SELECT User {
                            count := count(Issue),
                            all_issues := Issue
                        } FILTER .name = 'Elvis'
                    )
                SELECT x.count = count(x.all_issues);
            """,
            [True]
        )

    async def test_edgeql_functions_count_03(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := (
                        # User is simply employed as an object to be augmented
                        SELECT User {
                            count := count(<int64>Issue.number),
                            all_issues := <int64>Issue.number
                        } FILTER .name = 'Elvis'
                    )
                SELECT x.count = count(x.all_issues);
            """,
            [True]
        )

    async def test_edgeql_functions_array_agg_01(self):
        await self.assert_query_result(
            r'''SELECT array_agg({1, 2, 3});''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            r'''SELECT array_agg({3, 2, 3});''',
            [[3, 2, 3]],
        )

        await self.assert_query_result(
            r'''SELECT array_agg({3, 3, 2});''',
            [[3, 3, 2]],
        )

    async def test_edgeql_functions_array_agg_02(self):
        await self.assert_query_result(
            r'''SELECT array_agg({1, 2, 3})[0];''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT array_agg({3, 2, 3})[1];''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT array_agg({3, 3, 2})[-1];''',
            [2],
        )

    async def test_edgeql_functions_array_agg_03(self):
        await self.assert_query_result(
            r'''
                WITH x := {3, 1, 2}
                SELECT array_agg(x ORDER BY x);
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            r'''
                WITH x := {3, 1, 2}
                SELECT array_agg(x ORDER BY x) = [1, 2, 3];
            ''',
            [True],
        )

    async def test_edgeql_functions_array_agg_04(self):
        await self.assert_query_result(
            r'''
                WITH x := {3, 1, 2}
                SELECT contains(array_agg(x ORDER BY x), 2);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH x := {3, 1, 2}
                SELECT contains(array_agg(x ORDER BY x), 5);
            ''',
            [False],
        )

        await self.assert_query_result(
            r'''
                WITH x := {3, 1, 2}
                SELECT contains(array_agg(x ORDER BY x), 5);
            ''',
            [False],
        )

    async def test_edgeql_functions_array_agg_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'expression returns value of indeterminate type'):

            await self.con.execute("""
                SELECT array_agg({});
            """)

    async def test_edgeql_functions_array_agg_06(self):
        await self.assert_query_result(
            '''SELECT array_agg(<int64>{});''',
            [[]],
        )

        await self.assert_query_result(
            '''SELECT array_agg(DISTINCT <int64>{});''',
            [[]],
        )

    async def test_edgeql_functions_array_agg_07(self):
        await self.assert_query_result(
            r'''
                SELECT array_agg((SELECT schema::ObjectType FILTER False));
            ''',
            [[]]
        )

        await self.assert_query_result(
            r'''
                SELECT array_agg(
                    (SELECT schema::ObjectType
                     FILTER <str>schema::ObjectType.id = '~')
                );
            ''',
            [[]]
        )

    async def test_edgeql_functions_array_agg_08(self):
        await self.assert_query_result(
            r'''
                WITH x := <int64>{}
                SELECT array_agg(x);
            ''',
            [[]]
        )

        await self.assert_query_result(
            r'''
                WITH x := (SELECT schema::ObjectType FILTER False)
                SELECT array_agg(x);
            ''',
            [[]]
        )

        await self.assert_query_result(
            r'''
                WITH x := (
                    SELECT schema::ObjectType
                    FILTER <str>schema::ObjectType.id = '~'
                )
                SELECT array_agg(x);
            ''',
            [[]]
        )

    async def test_edgeql_functions_array_agg_09(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT
                    ObjectType {
                        l := array_agg(
                            ObjectType.properties.name
                            FILTER
                                ObjectType.properties.name IN {
                                    'id',
                                    'name'
                                }
                            ORDER BY ObjectType.properties.name ASC
                        )
                    }
                FILTER
                    ObjectType.name = 'schema::Object';
            """,
            [{
                'l': ['id', 'name']
            }]
        )

    async def test_edgeql_functions_array_agg_10(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.con.query(r"""
                SELECT array_agg(
                    [<str>Issue.number, Issue.status.name]
                    ORDER BY Issue.number);
            """)

    @tb.needs_factoring
    async def test_edgeql_functions_array_agg_11(self):
        await self.assert_query_result(
            r"""
                SELECT array_agg(
                    (<str>Issue.number, Issue.status.name)
                    ORDER BY Issue.number
                )[1];
            """,
            [['2', 'Open']]
        )

    async def test_edgeql_functions_array_agg_12(self):
        await self.assert_query_result(
            r'''
                SELECT
                    array_agg(User{name} ORDER BY User.name);
            ''',
            [[{'name': 'Elvis'}, {'name': 'Yury'}]]
        )

        result = await self.con.query(r'''
            SELECT
                array_agg(User{name} ORDER BY User.name);
        ''')

        self.assertEqual(result[0][0].name, 'Elvis')
        self.assertEqual(result[0][1].name, 'Yury')

    async def test_edgeql_functions_array_agg_13(self):
        await self.assert_query_result(
            r'''
                SELECT
                    Issue {
                        number,
                        watchers_array := array_agg(Issue.watchers {name})
                    }
                FILTER
                    EXISTS Issue.watchers
                ORDER BY
                    Issue.number;
            ''',
            [
                {'number': '1', 'watchers_array': [{'name': 'Yury'}]},
                {'number': '2', 'watchers_array': [{'name': 'Elvis'}]},
                {'number': '3', 'watchers_array': [{'name': 'Elvis'}]}
            ]
        )

    async def test_edgeql_functions_array_agg_14(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.con.query(r'''
                SELECT array_agg(array_agg(User.name));
            ''')

    @tb.needs_factoring
    async def test_edgeql_functions_array_agg_15(self):
        await self.assert_query_result(
            r'''
                SELECT array_agg(
                    ([([User.name],)],) ORDER BY User.name
                );
            ''',
            [       # result set
                [   # array_agg
                    [[[['Elvis']]]], [[[['Yury']]]],
                ]
            ]
        )

    async def test_edgeql_functions_array_agg_16(self):
        await self.assert_query_result(
            r'''
                SELECT array_agg(   # outer array
                    (               # tuple
                        array_agg(  # array
                            (       # tuple
                                array_agg(User.name ORDER BY User.name),
                            )
                        ),
                    )
                );
            ''',
            [       # result set
                [   # outer array_agg
                    [[[['Elvis', 'Yury']]]]
                ]
            ]
        )

    async def test_edgeql_functions_array_agg_17(self):
        await self.assert_query_result(
            '''SELECT count(array_agg({}))''',
            [1],
        )

    async def test_edgeql_functions_array_agg_18(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'expression returns value of indeterminate type'):
            await self.con.execute(
                '''SELECT array_agg({})''',
            )

    async def test_edgeql_functions_array_agg_19(self):
        await self.assert_query_result(
            r'''FOR X in {array_agg(0)} UNION (SELECT array_unpack(X));''',
            [0],
        )

        await self.assert_query_result(
            r'''
                FOR X in {array_agg((0, 1))}
                UNION (SELECT array_unpack(X));
            ''',
            [[0, 1]],
        )

        await self.assert_query_result(
            r'''FOR X in {array_agg((0, 1))} UNION (X);''',
            [[[0, 1]]],
        )

    async def test_edgeql_functions_array_agg_20(self):
        await self.assert_query_result(
            r'''
                SELECT Issue { te := array_agg(.time_estimate) };
            ''',
            tb.bag([{"te": [3000]}, {"te": []}, {"te": []}, {"te": []}]),
        )

        await self.assert_query_result(
            r'''
                SELECT Issue { te := array_agg(.time_estimate UNION 3000) };
            ''',
            tb.bag(
                [{"te": [3000, 3000]}, {"te": [3000]},
                 {"te": [3000]}, {"te": [3000]}],
            )
        )

    async def test_edgeql_functions_array_agg_21(self):
        await self.assert_query_result(
            r'''
            WITH X := array_agg((1, 2)),
            SELECT X FILTER X[0].0 = 1;
            ''',
            [[[1, 2]]],
        )

    async def test_edgeql_functions_array_agg_22(self):
        await self.assert_query_result(
            r'''
            WITH X := array_agg((foo := 1, bar := 2)),
            SELECT X FILTER X[0].foo = 1;
            ''',
            [[{"bar": 2, "foo": 1}]],
        )

    async def test_edgeql_functions_array_agg_23(self):
        await self.assert_query_result(
            r'''
            SELECT X := array_agg((foo := 1, bar := 2)) FILTER X[0].foo = 1;
            ''',
            [[{"bar": 2, "foo": 1}]],
        )

    async def test_edgeql_functions_array_unpack_01(self):
        await self.assert_query_result(
            r'''SELECT [1, 2];''',
            [[1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT array_unpack([1, 2]);''',
            [1, 2],
        )

        await self.assert_query_result(
            r'''SELECT array_unpack([10, 20]) - 1;''',
            [9, 19],
        )

    async def test_edgeql_functions_array_unpack_02(self):
        await self.assert_query_result(
            # array_agg and array_unpack are inverses of each other
            r'''SELECT array_agg(array_unpack([1, 2, 3])) = [1, 2, 3];''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT array_unpack(array_agg({1, 2, 3}));''',
            {1, 2, 3},
        )

    async def test_edgeql_functions_array_unpack_03(self):
        await self.assert_query_result(
            r'''
                # array_agg and array_unpack are inverses of each other
                SELECT array_unpack(array_agg(Issue.number));
            ''',
            {'1', '2', '3', '4'},
        )

    async def test_edgeql_functions_array_unpack_04(self):
        await self.assert_query_result(
            r'''
                # array_agg and array_unpack are inverses of each other
                SELECT array_unpack(array_agg(Issue)){number};
            ''',
            [
                {'number': '1'},
                {'number': '2'},
                {'number': '3'},
                {'number': '4'},
            ],
            sort=lambda x: x['number']
        )

    async def test_edgeql_functions_array_unpack_05(self):
        await self.assert_query_result(
            r'''SELECT array_unpack([(1,)]).0;''',
            [1],
        )

    async def test_edgeql_functions_array_unpack_06(self):
        # We have a special case optimization for "IN array_unpack" so
        # it's worth testing it.

        await self.assert_query_result(
            r'''SELECT 1 IN array_unpack([1]);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 2 IN array_unpack([1]);''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 2 NOT IN array_unpack([1]);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 1 IN array_unpack({[1,2,3], [4,5,6]});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 0 IN array_unpack({[1,2,3], [4,5,6]});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 1 NOT IN array_unpack({[1,2,3], [4,5,6]});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 0 NOT IN array_unpack({[1,2,3], [4,5,6]});''',
            [True],
        )

        await self.assert_query_result(
            r"""
            SELECT ("foo", 1) IN array_unpack([("foo", 1), ("bar", 2)]);
            """,
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 2 IN array_unpack(<array<int64>>{});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 2 NOT IN array_unpack(<array<int64>>{});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 1n IN array_unpack([1n]);''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select 1n in array_unpack(
                    <array<bigint>><array<str>>to_json('["1"]'))
            ''',
            [True],
        )

    async def test_edgeql_functions_array_fill_01(self):
        await self.assert_query_result(
            r'''select array_fill(0, 5);''',
            [[0] * 5],
        )

        await self.assert_query_result(
            r'''select array_fill('n/a', 5);''',
            [['n/a'] * 5],
        )

        await self.assert_query_result(
            r'''
                with
                    date0 := <cal::local_date>'2022-05-01',
                    date1 := <cal::local_date>'2022-05-01'
                select array_fill(date0, 5) =
                    [date1, date1, date1, date1, date1];
            ''',
            [True],
        )

    @test.xerror("""
        edb.errors.InternalServerError: return type record[] is not supported
        for SQL functions
    """)
    async def test_edgeql_functions_array_fill_02(self):
        await self.assert_query_result(
            r'''select array_fill((1, 'hello'), 5);''',
            [[(1, 'hello')] * 5],
        )

    @test.xerror("""
        edb.errors.InternalServerError: return type record[] is not supported
        for SQL functions
    """)
    async def test_edgeql_functions_array_fill_03(self):
        await self.assert_query_result(
            r'''select array_fill((a := 1, b := 'hello'), 5);''',
            # This is not strictly equivalent in Python, but the resulting
            # array should be equal by value.
            [[{'a': 1, 'b': 'hello'}] * 5],
        )

    async def test_edgeql_functions_array_fill_04(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "array size exceeds the maximum allowed"
        ):
            async with self.con.transaction():
                await self.con.query(r'select array_fill(0, 2147480000);')

        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "array size exceeds the maximum allowed"
        ):
            async with self.con.transaction():
                await self.con.query(r'select array_fill(0, 2147483647);')

        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "array size exceeds the maximum allowed"
        ):
            async with self.con.transaction():
                await self.con.query(r'select array_fill(0, 12147480000);')

    async def test_edgeql_functions_array_replace_01(self):
        await self.assert_query_result(
            r'''select array_replace([1, 1, 2, 3, 5], 1, 99);''',
            [[99, 99, 2, 3, 5]],
        )

        await self.assert_query_result(
            r'''select array_replace([1, 1, 2, 3, 5], 6, 99);''',
            [[1, 1, 2, 3, 5]],
        )

    async def test_edgeql_functions_array_replace_02(self):
        await self.assert_query_result(
            r'''select array_replace(['h', 'e', 'l', 'l', 'o'], 'l', 'L');''',
            [['h', 'e', 'L', 'L', 'o']],
        )

        await self.assert_query_result(
            r'''select array_replace(['h', 'e', 'l', 'l', 'o'], 'z', '!');''',
            [['h', 'e', 'l', 'l', 'o']],
        )

    async def test_edgeql_functions_array_replace_03(self):
        await self.assert_query_result(
            r'''
            select array_replace(
                [(0, 'a'), (10, 'b'), (3, 'hello'), (0, 'a')],
                (0, 'a'), (99, '!')
            );
            ''',
            [[(99, '!'), (10, 'b'), (3, 'hello'), (99, '!')]],
        )

        await self.assert_query_result(
            r'''
            select array_replace(
                [(0, 'a'), (10, 'b'), (3, 'hello'), (0, 'a')],
                (1, 'a'), (99, '!')
            );
            ''',
            [[(0, 'a'), (10, 'b'), (3, 'hello'), (0, 'a')]],
        )

    async def test_edgeql_functions_array_replace_04(self):
        await self.assert_query_result(
            r'''
            select array_replace(
                [
                    (a := 0, b := 'a'),
                    (a := 10, b := 'b'),
                    (a := 3, b := 'hello'),
                    (a := 0, b := 'a')
                ],
                (a := 0, b := 'a'), (a := 99, b := '!')
            );
            ''',
            [[
                {"a": 99, "b": "!"},
                {"a": 10, "b": "b"},
                {"a": 3, "b": "hello"},
                {"a": 99, "b": "!"}
            ]],
        )

        await self.assert_query_result(
            r'''
            select array_replace(
                [
                    (a := 0, b := 'a'),
                    (a := 10, b := 'b'),
                    (a := 3, b := 'hello'),
                    (a := 0, b := 'a')
                ],
                (a := 1, b := 'a'), (a := 99, b := '!')
            );
            ''',
            [[
                {"a": 0, "b": "a"},
                {"a": 10, "b": "b"},
                {"a": 3, "b": "hello"},
                {"a": 0, "b": "a"}
            ]],
        )

    async def test_edgeql_functions_enumerate_01(self):
        await self.assert_query_result(
            r'''SELECT [10, 20];''',
            [[10, 20]],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([10,20]));''',
            [[0, 10], [1, 20]],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([10,20])).0 + 100;''',
            [100, 101],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([10,20])).1 + 100;''',
            [110, 120],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([(1, '2')]))''',
            [[0, [1, '2']]],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([(1, '2')])).1.1''',
            ['2'],
        )

    async def test_edgeql_functions_enumerate_02(self):
        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([(x:=1)])).1;''',
            [{"x": 1}],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([(x:=1)])).1.x;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([(x:=(a:=2))])).1;''',
            [{"x": {"a": 2}}],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([(x:=(a:=2))])).1.x;''',
            [{"a": 2}],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(array_unpack([(x:=(a:=2))])).1.x.a;''',
            [2],
        )

    @tb.needs_factoring
    async def test_edgeql_functions_enumerate_03(self):
        await self.assert_query_result(
            r'''SELECT enumerate((SELECT User.name ORDER BY User.name));''',
            [[0, 'Elvis'], [1, 'Yury']],
        )

        await self.assert_query_result(
            r'''SELECT enumerate({'a', 'b', 'c'});''',
            [[0, 'a'], [1, 'b'], [2, 'c']],
        )

        await self.assert_query_result(
            r'''WITH A := {'a', 'b'} SELECT (A, enumerate(A));''',
            [['a', [0, 'a']], ['b', [0, 'b']]],
        )

        await self.assert_query_result(
            r'''SELECT enumerate({(1, 2), (3, 4)});''',
            [[0, [1, 2]], [1, [3, 4]]],
        )

    async def test_edgeql_functions_enumerate_04(self):
        self.assertEqual(
            await self.con.query(
                'select <json>enumerate({(1, 2), (3, 4)})'),
            ['[0, [1, 2]]', '[1, [3, 4]]'])

        self.assertEqual(
            await self.con.query_json(
                'select <json>enumerate({(1, 2), (3, 4)})'),
            '[[0, [1, 2]], [1, [3, 4]]]')

    async def test_edgeql_functions_enumerate_05(self):
        await self.assert_query_result(
            r'''SELECT enumerate(User { name } ORDER BY .name);''',
            [[0, {"name": "Elvis"}],
             [1, {"name": "Yury"}]],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(User ORDER BY .name).1.name;''',
            ["Elvis", "Yury"],
        )

    async def test_edgeql_functions_enumerate_06(self):
        await self.assert_query_result(
            r'''SELECT enumerate(_gen_series(0, 99) FILTER FALSE);''',
            [],
        )

    @tb.needs_factoring
    async def test_edgeql_functions_enumerate_07(self):
        # Check that enumerate of a function works when the tuple type
        # appears in the schema (like tuple<int64, int64> does)
        await self.assert_query_result(
            r'''
            WITH Z := enumerate(array_unpack([10, 20])),
                 Y := enumerate(Z),
            SELECT (Y.1.0, Y.1.1) ORDER BY Y.0;
            ''',
            [[0, 10], [1, 20]]
        )

    async def test_edgeql_functions_enumerate_08(self):
        await self.assert_query_result(
            r'''
            SELECT Issue { te := enumerate(.time_estimate) };
            ''',
            tb.bag(
                [{"te": [0, 3000]}, {"te": None}, {"te": None}, {"te": None}]
            )
        )

        await self.assert_query_result(
            r'''
            SELECT Issue { te := enumerate(.time_estimate UNION 3000) };
            ''',
            tb.bag([
                {"te": [[0, 3000], [1, 3000]]},
                {"te": [[0, 3000]]},
                {"te": [[0, 3000]]},
                {"te": [[0, 3000]]}
            ])
        )

    async def test_edgeql_functions_enumerate_09(self):
        await self.assert_query_result(
            'SELECT enumerate(sum({1,2,3}))',
            [[0, 6]]
        )
        await self.assert_query_result(
            'SELECT enumerate(count(Issue))',
            [[0, 4]]
        )
        await self.assert_query_result(
            '''
            WITH x := (SELECT enumerate(array_agg((select User)))),
            SELECT (x.0, array_unpack(x.1).name)
            ''',
            [[0, 'Elvis'], [0, 'Yury']]
        )

    async def test_edgeql_functions_array_get_01(self):
        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], 2);''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], -2);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], 20);''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], -20);''',
            [],
        )

    async def test_edgeql_functions_array_get_02(self):
        await self.assert_query_result(
            r'''
                SELECT array_get(array_agg(
                    Issue.number ORDER BY Issue.number), 2);
            ''',
            ['3'],
        )

        await self.assert_query_result(
            r'''
                SELECT array_get(array_agg(
                    Issue.number ORDER BY Issue.number), -2);
            ''',
            ['3'],
        )

        await self.assert_query_result(
            r'''SELECT array_get(array_agg(Issue.number), 20);''',
            []
        )

        await self.assert_query_result(
            r'''SELECT array_get(array_agg(Issue.number), -20);''',
            []
        )

    async def test_edgeql_functions_array_get_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function "array_get.+" does not exist'):

            await self.con.query(r'''
                SELECT array_get([1, 2, 3], 2^40);
            ''')

    async def test_edgeql_functions_array_get_04(self):
        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], 0) ?? 42;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], 0, default := -1) ?? 42;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], -2) ?? 42;''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], 20) ?? 42;''',
            [42],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], -20) ?? 42;''',
            [42],
        )

    async def test_edgeql_functions_array_get_05(self):
        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], 1, default := 4200) ?? 42;''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], -2, default := 4200) ?? 42;''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], 20, default := 4200) ?? 42;''',
            [4200],
        )

        await self.assert_query_result(
            r'''SELECT array_get([1, 2, 3], -20, default := 4200) ?? 42;''',
            [4200],
        )

    async def test_edgeql_functions_array_get_06(self):
        await self.assert_query_result(
            r'''SELECT array_get([(20,), (30,)], 0);''',
            [[20]],
        )

        await self.assert_query_result(
            r'''SELECT array_get([(a:=20), (a:=30)], 1);''',
            [{'a': 30}],
        )

        await self.assert_query_result(
            r'''SELECT array_get([(20,), (30,)], 0).0;''',
            [20],
        )

        await self.assert_query_result(
            r'''SELECT array_get([(a:=20), (a:=30)], 1).0;''',
            [30],
        )

        await self.assert_query_result(
            r'''SELECT array_get([(a:=20, b:=1), (a:=30, b:=2)], 0).a;''',
            [20],
        )

        await self.assert_query_result(
            r'''SELECT array_get([(a:=20, b:=1), (a:=30, b:=2)], 1).b;''',
            [2],
        )

    async def test_edgeql_functions_array_get_07(self):
        await self.assert_query_result(
            r'''
                SELECT array_get([Issue.number], 0)
            ''',
            {'1', '2', '3', '4'},
        )

    async def test_edgeql_functions_array_get_08(self):
        await self.assert_query_result(
            r'''
                select array_get(
                    array_agg((select x := {1,2,3} filter x > 0)), 1);
            ''',
            [2],
        )

    async def test_edgeql_functions_array_set_01(self):
        # Positive indexes
        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], 0, 9);''',
            [[9, 2, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], 1, 9);''',
            [[1, 9, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], 2, 9);''',
            [[1, 2, 9, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], 3, 9);''',
            [[1, 2, 3, 9]],
        )

        # Negative indexes
        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], -1, 9);''',
            [[1, 2, 3, 9]],
        )

        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], -2, 9);''',
            [[1, 2, 9, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], -3, 9);''',
            [[1, 9, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_set([1, 2, 3, 4], -4, 9);''',
            [[9, 2, 3, 4]],
        )

        # Size 1 array
        await self.assert_query_result(
            r'''SELECT array_set([1], 0, 9);''',
            [[9]],
        )

        await self.assert_query_result(
            r'''SELECT array_set([1], -1, 9);''',
            [[9]],
        )

    async def test_edgeql_functions_array_set_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index 4 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_set([1, 2, 3, 4], 4, 9);''',
            )

    async def test_edgeql_functions_array_set_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index -5 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_set([1, 2, 3, 4], -5, 9);''',
            )

    async def test_edgeql_functions_array_set_04(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index 1 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_set([1], 1, 9);''',
            )

    async def test_edgeql_functions_array_set_05(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index -2 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_set([1], -2, 9);''',
            )

    async def test_edgeql_functions_array_set_06(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index 0 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_set(<array<int64>>[], 0, 9);''',
            )

    async def test_edgeql_functions_array_set_07(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index -1 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_set(<array<int64>>[], -1, 9);''',
            )

    async def test_edgeql_functions_array_insert_01(self):
        # Positive indexes
        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], 0, 9);''',
            [[9, 1, 2, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], 1, 9);''',
            [[1, 9, 2, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], 2, 9);''',
            [[1, 2, 9, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], 3, 9);''',
            [[1, 2, 3, 9, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], 4, 9);''',
            [[1, 2, 3, 4, 9]],
        )

        # Negative indexes
        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], -1, 9);''',
            [[1, 2, 3, 9, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], -2, 9);''',
            [[1, 2, 9, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], -3, 9);''',
            [[1, 9, 2, 3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1, 2, 3, 4], -4, 9);''',
            [[9, 1, 2, 3, 4]],
        )

        # Size 1 array
        await self.assert_query_result(
            r'''SELECT array_insert([1], 0, 9);''',
            [[9, 1]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1], 1, 9);''',
            [[1, 9]],
        )

        await self.assert_query_result(
            r'''SELECT array_insert([1], -1, 9);''',
            [[9, 1]],
        )

        # Size 0 array
        await self.assert_query_result(
            r'''SELECT array_insert(<array<int64>>[], 0, 9);''',
            [[9]],
        )

    async def test_edgeql_functions_array_insert_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index 5 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_insert([1, 2, 3, 4], 5, 9);''',
            )

    async def test_edgeql_functions_array_insert_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index -5 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_insert([1, 2, 3, 4], -5, 9);''',
            )

    async def test_edgeql_functions_array_insert_04(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index 2 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_insert([1], 2, 9);''',
            )

    async def test_edgeql_functions_array_insert_05(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index -2 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_insert([1], -2, 9);''',
            )

    async def test_edgeql_functions_array_insert_06(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index 1 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_insert(<array<int64>>[], 1, 9);''',
            )

    async def test_edgeql_functions_array_insert_07(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'array index -1 is out of bounds'
        ):
            await self.con.query(
                r'''SELECT array_insert(<array<int64>>[], -1, 9);''',
            )

    @test.xerror(
        "Known collation issue on Heroku Postgres",
        unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    )
    async def test_edgeql_functions_re_match_01(self):
        await self.assert_query_result(
            r'''SELECT re_match('ab', 'AbabaB');''',
            [['ab']],
        )

        await self.assert_query_result(
            r'''SELECT re_match('AB', 'AbabaB');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT re_match('(?i)AB', 'AbabaB');''',
            [['Ab']],
        )

        await self.assert_query_result(
            r'''SELECT re_match('ac', 'AbabaB');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT EXISTS re_match('ac', 'AbabaB');''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT NOT EXISTS re_match('ac', 'AbabaB');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT EXISTS re_match('ab', 'AbabaB');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT NOT EXISTS re_match('ab', 'AbabaB');''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT x := re_match({'(?i)ab', 'a'}, 'AbabaB') ORDER BY x;''',
            [['Ab'], ['a']],
        )

        await self.assert_query_result(
            r'''
                SELECT x := re_match({'(?i)ab', 'a'}, {'AbabaB', 'qwerty'})
                ORDER BY x;
            ''',
            [['Ab'], ['a']],
        )

        await self.assert_query_result(
            r'''
            select re_match(
                r"(foo)?bar",
                'barbar',
            )
            ''',
            [[""]],
        )

    async def test_edgeql_functions_re_match_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT x := re_match('(\\w+)::(Link|Property)',
                                     ObjectType.name)
                ORDER BY x;
            ''',
            [['schema', 'Link'], ['schema', 'Property']],
        )

    async def test_edgeql_functions_re_match_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "invalid regular expression"
        ):
            await self.con.query(r'''
                select re_match('\\', 'asdf')
            ''')

    @test.xerror(
        "Known collation issue on Heroku Postgres",
        unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    )
    async def test_edgeql_functions_re_match_all_01(self):
        await self.assert_query_result(
            r'''SELECT re_match_all('ab', 'AbabaB');''',
            [['ab']],
        )

        await self.assert_query_result(
            r'''SELECT re_match_all('AB', 'AbabaB');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT re_match_all('(?i)AB', 'AbabaB');''',
            [['Ab'], ['ab'], ['aB']],
        )

        await self.assert_query_result(
            r'''SELECT re_match_all('ac', 'AbabaB');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT EXISTS re_match_all('ac', 'AbabaB');''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT NOT EXISTS re_match_all('ac', 'AbabaB');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT EXISTS re_match_all('(?i)ab', 'AbabaB');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT NOT EXISTS re_match_all('(?i)ab', 'AbabaB');''',
            [False],
        )

        await self.assert_query_result(
            r'''
                SELECT x := re_match_all({'(?i)ab', 'a'}, 'AbabaB')
                ORDER BY x;''',
            [['Ab'], ['a'], ['a'], ['aB'], ['ab']],
        )

        await self.assert_query_result(
            r'''
                SELECT x := re_match_all({'(?i)ab', 'a'},
                                         {'AbabaB', 'qwerty'})
                ORDER BY x;
            ''',
            [['Ab'], ['a'], ['a'], ['aB'], ['ab']],
        )

        await self.assert_query_result(
            r'''
            select re_match_all(
                r"(foo)?bar",
                'barbar',
            )
            ''',
            [[""], [""]],
        )

    async def test_edgeql_functions_re_test_01(self):
        await self.assert_query_result(
            r'''SELECT re_test('ac', 'AbabaB');''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT NOT re_test('ac', 'AbabaB');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT re_test(r'(?i)ab', 'AbabaB');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT NOT re_test(r'(?i)ab', 'AbabaB');''',
            [False],
        )

        await self.assert_query_result(
            # the result always exists
            r'''SELECT EXISTS re_test('(?i)ac', 'AbabaB');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT NOT EXISTS re_test('(?i)ac', 'AbabaB');''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT x := re_test({'ab', 'a'}, 'AbabaB') ORDER BY x;''',
            [True, True],
        )

        await self.assert_query_result(
            r'''
                SELECT x := re_test({'ab', 'a'}, {'AbabaB', 'qwerty'})
                ORDER BY x;
            ''',
            [False, False, True, True],
        )

    async def test_edgeql_functions_re_test_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT count(
                    ObjectType FILTER re_test(r'(\W\w)bject$', ObjectType.name)
                ) = 2;
            ''',
            [True],
        )

    async def test_edgeql_functions_re_replace_01(self):
        await self.assert_query_result(
            r'''SELECT re_replace('l', 'L', 'Hello World');''',
            ['HeLlo World'],
        )

        await self.assert_query_result(
            r'''SELECT re_replace('l', 'L', 'Hello World', flags := 'g');''',
            ['HeLLo WorLd'],
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[a-z]', '~', 'Hello World',
                                  flags := 'i');''',
            ['~ello World'],
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[a-z]', '~', 'Hello World',
                                  flags := 'gi');
            ''',
            ['~~~~~ ~~~~~'],
        )

    async def test_edgeql_functions_re_replace_02(self):
        await self.assert_query_result(
            r'''SELECT re_replace('[aeiou]', '~', User.name);''',
            {'Elv~s', 'Y~ry'},
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[aeiou]', '~', User.name,
                                  flags := 'g');
            ''',
            {'Elv~s', 'Y~ry'},
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[aeiou]', '~', User.name,
                                  flags := 'i');
            ''',
            {'~lvis', 'Y~ry'},
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[aeiou]', '~', User.name,
                                  flags := 'gi');
            ''',
            {'~lv~s', 'Y~ry'},
        )

    async def test_edgeql_functions_sum_01(self):
        await self.assert_query_result(
            r'''SELECT sum({1, 2, 3, -4, 5});''',
            [7],
        )

        await self.assert_query_result(
            r'''SELECT sum({0.1, 0.2, 0.3, -0.4, 0.5});''',
            [0.7],
        )

    async def test_edgeql_functions_sum_02(self):
        await self.assert_query_result(
            r'''
                SELECT sum({1, 2, 3, -4.2, 5});
            ''',
            [6.8],
        )

    async def test_edgeql_functions_sum_03(self):
        await self.assert_query_result(
            r'''
                SELECT sum({1.0, 2.0, 3.0, -4.2, 5});
            ''',
            [6.8],
        )

    async def test_edgeql_functions_sum_04(self):
        await self.assert_query_result(
            r'''SELECT sum(<int16>2) IS int64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT sum(<int32>2) IS int64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT sum(<int64>2) IS int64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT sum(<float32>2) IS float32;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT sum(<float64>2) IS float64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT sum(<decimal>2) IS decimal;''',
            [True],
        )

    async def test_edgeql_functions_unix_to_datetime_01(self):
        dt = await self.con.query_single(
            'SELECT <str>to_datetime(1590595184.584);'
        )
        self.assertEqual('2020-05-27T15:59:44.584+00:00', dt)

    async def test_edgeql_functions_unix_to_datetime_02(self):
        dt = await self.con.query_single(
            'SELECT <str>to_datetime(1590595184);'
        )
        self.assertEqual('2020-05-27T15:59:44+00:00', dt)

    async def test_edgeql_functions_unix_to_datetime_03(self):
        dt = await self.con.query_single(
            'SELECT <str>to_datetime(517795200);'
        )
        self.assertEqual('1986-05-30T00:00:00+00:00', dt)

    async def test_edgeql_functions_unix_to_datetime_04(self):
        dt = await self.con.query_single(
            'SELECT <str>to_datetime(517795200.00n);'
        )
        self.assertEqual('1986-05-30T00:00:00+00:00', dt)

    async def test_edgeql_functions_unix_to_datetime_05(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "'std::datetime' value out of range"
        ):
            await self.con.query_single(
                'SELECT to_datetime(999999999999)'
            )

    async def test_edgeql_functions_datetime_current_01(self):
        # make sure that datetime as a str gets serialized to a
        # particular format
        dt = await self.con.query_single('SELECT <str>datetime_current();')
        self.assertRegex(dt, r'\d+-\d+-\d+T\d+:\d+:\d+\.\d+.*')

    async def test_edgeql_functions_datetime_current_02(self):
        batch1 = await self.con.query_json(r'''
            WITH MODULE schema
            SELECT Type {
                dt_t := datetime_of_transaction(),
                dt_s := datetime_of_statement(),
                dt_n := datetime_current(),
            };
        ''')
        batch2 = await self.con.query_json(r'''
            # NOTE: this test assumes that there's at least 1 microsecond
            # time difference between statements
            WITH MODULE schema
            SELECT Type {
                dt_t := datetime_of_transaction(),
                dt_s := datetime_of_statement(),
                dt_n := datetime_current(),
            };
        ''')

        batch1 = json.loads(batch1)
        batch2 = json.loads(batch2)
        batches = batch1 + batch2

        # all of the dt_t should be the same
        set_dt_t = {t['dt_t'] for t in batches}
        self.assertTrue(len(set_dt_t) == 1)

        # all of the dt_s should be the same in each batch
        set_dt_s1 = {t['dt_s'] for t in batch1}
        set_dt_s2 = {t['dt_s'] for t in batch2}
        self.assertTrue(len(set_dt_s1) == 1)
        self.assertTrue(len(set_dt_s1) == 1)

        # the transaction and statement datetimes should be in
        # chronological order
        dt_t = set_dt_t.pop()
        dt_s1 = set_dt_s1.pop()
        dt_s2 = set_dt_s2.pop()
        self.assertTrue(dt_t <= dt_s1 < dt_s2)

        # the first "now" datetime is no earlier than the statement
        # for each batch
        self.assertTrue(dt_s1 <= batch1[0]['dt_n'])
        self.assertTrue(dt_s2 <= batch2[0]['dt_n'])

        # every dt_n is already in chronological order
        self.assertEqual(
            [t['dt_n'] for t in batches],
            sorted([t['dt_n'] for t in batches])
        )
        # the first dt_n is strictly earlier than the last
        self.assertTrue(batches[0]['dt_n'] < batches[-1]['dt_n'])

    async def test_edgeql_functions_datetime_get_01(self):
        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'millennium');
            ''',
            {3},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'century');
            ''',
            {21},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'decade');
            ''',
            {201},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'year');
            ''',
            {2018},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'month');
            ''',
            {5},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'day');
            ''',
            {7},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'hour');
            ''',
            {20},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'minutes');
            ''',
            {1},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'seconds');
            ''',
            {22.306916},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'epochseconds');
            ''',
            {1525723282.306916},
        )

    async def test_edgeql_functions_datetime_get_02(self):
        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <cal::local_datetime>'2018-05-07T15:01:22.306916', 'year');
            ''',
            {2018},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                  <cal::local_datetime>'2018-05-07T15:01:22.306916', 'month');
            ''',
            {5},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <cal::local_datetime>'2018-05-07T15:01:22.306916', 'day');
            ''',
            {7},
        )

        await self.assert_query_result(
            r'''
                SELECT datetime_get(
                    <cal::local_datetime>'2018-05-07T15:01:22.306916', 'hour');
            ''',
            {15},
        )

        await self.assert_query_result(
            r'''SELECT datetime_get(
                <cal::local_datetime>'2018-05-07T15:01:22.306916', 'minutes');
            ''',
            {1},
        )

        await self.assert_query_result(
            r'''SELECT datetime_get(
                <cal::local_datetime>'2018-05-07T15:01:22.306916', 'seconds');
            ''',
            {22.306916},
        )

    async def test_edgeql_functions_datetime_get_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::datetime_get'):
            await self.con.query('''
                SELECT datetime_get(
                    <cal::local_datetime>'2018-05-07T15:01:22.306916',
                    'timezone_hour'
                );
            ''')

    async def test_edgeql_functions_datetime_get_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::datetime_get'):
            await self.con.query('''
                SELECT datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05',
                    'timezone_hour');
            ''')

    async def test_edgeql_functions_datetime_get_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::datetime_get'):
            await self.con.execute(
                r'''
                SELECT <str>datetime_get(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'epoch');
                ''')

    async def test_edgeql_functions_duration_get_01(self):
        await self.assert_query_result(
            r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'hour');
            ''',
            {15},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'minutes');
            ''',
            {1},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'seconds');
            ''',
            {22.306916},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'milliseconds');
            ''',
            {22306.916},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'microseconds');
            ''',
            {22306916},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'totalseconds');
            ''',
            {54082.306916},
        )

    async def test_edgeql_functions_duration_get_02(self):
        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'123 months', 'year');
            ''',
            {10},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'123 months', 'month');
            ''',
            {3},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'45 days', 'day');
            ''',
            {45},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'hour');
            ''',
            {15},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'minutes');
            ''',
            {1},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'seconds');
            ''',
            {22.306916},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'milliseconds'
                );
            ''',
            {22306.916},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'microseconds'
                );
            ''',
            {22306916},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'totalseconds'
                );
            ''',
            {54082.306916},
        )

    async def test_edgeql_functions_duration_get_03(self):
        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::date_duration>'123 months', 'year');
            ''',
            {10},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::date_duration>'123 months', 'month');
            ''',
            {3},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::date_duration>'45 days', 'day');
            ''',
            {45},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::date_duration>'13 months 12 days', 'day');
            ''',
            {12},
        )

        await self.assert_query_result(
            r'''
                select duration_get(
                    <cal::date_duration>'2 days', 'totalseconds'
                );
            ''',
            {2 * 24 * 3600},
        )

    async def test_edgeql_functions_duration_get_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'days');
                ''')

    async def test_edgeql_functions_duration_get_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'epoch');
                ''')

    async def test_edgeql_functions_duration_get_06(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <duration>'15:01:22.306916', 'epochseconds');
                ''')

    async def test_edgeql_functions_duration_get_07(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'epoch'
                );
                ''')

    async def test_edgeql_functions_duration_get_08(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <cal::relative_duration>'15:01:22.306916', 'epochseconds'
                );
                ''')

    async def test_edgeql_functions_duration_get_09(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <cal::date_duration>'1 day', 'hours');
                ''')

    async def test_edgeql_functions_duration_get_10(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <cal::date_duration>'1 day', 'epoch');
                ''')

    async def test_edgeql_functions_duration_get_11(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_get'):
            await self.con.execute(
                r'''
                select duration_get(
                    <cal::date_duration>'1 day', 'epochseconds');
                ''')

    async def test_edgeql_functions_date_get_01(self):
        await self.assert_query_result(
            r'''SELECT cal::date_get(<cal::local_date>'2018-05-07', 'year');
            ''',
            {2018},
        )

        await self.assert_query_result(
            r'''SELECT cal::date_get(<cal::local_date>'2018-05-07', 'month');
            ''',
            {5},
        )

        await self.assert_query_result(
            r'''SELECT cal::date_get(<cal::local_date>'2018-05-07', 'day');
            ''',
            {7},
        )

    async def test_edgeql_functions_date_get_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::date_get'):
            await self.con.execute(
                r'''
                SELECT <str>cal::date_get(
                    <cal::local_date>'2018-05-07', 'epoch');
                ''')

    async def test_edgeql_functions_time_get_01(self):
        await self.assert_query_result(
            r'''SELECT
                    cal::time_get(<cal::local_time>'15:01:22.306916', 'hour')
            ''',
            {15},
        )

        await self.assert_query_result(
            r'''SELECT
                cal::time_get(<cal::local_time>'15:01:22.306916', 'minutes')
            ''',
            {1},
        )

        await self.assert_query_result(
            r'''SELECT
                cal::time_get(<cal::local_time>'15:01:22.306916', 'seconds')
            ''',
            {22.306916},
        )

        await self.assert_query_result(
            r'''SELECT
                cal::time_get(<cal::local_time>'15:01:22.306916',
                              'midnightseconds')
            ''',
            {54082.306916},
        )

    async def test_edgeql_functions_time_get_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::time_get'):
            await self.con.execute(
                r'''
                SELECT <str>cal::time_get(
                    <cal::local_time>'15:01:22.306916', 'epoch');
                ''')

    async def test_edgeql_functions_datetime_trunc_01(self):
        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'years');
            ''',
            {'2018-01-01T00:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'decades');
            ''',
            {'2010-01-01T00:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'centuries');
            ''',
            {'2001-01-01T00:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'quarters');
            ''',
            {'2018-04-01T00:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'months');
            ''',
            {'2018-05-01T00:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'weeks');
            ''',
            {'2018-05-07T00:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'days');
            ''',
            {'2018-05-07T00:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'hours');
            ''',
            {'2018-05-07T20:00:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'minutes');
            ''',
            {'2018-05-07T20:01:00+00:00'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'seconds');
            ''',
            {'2018-05-07T20:01:22+00:00'},
        )

    async def test_edgeql_functions_datetime_trunc_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::datetime_truncate'):
            await self.con.execute(
                r'''
                SELECT <str>datetime_truncate(
                    <datetime>'2018-05-07T15:01:22.306916-05', 'second');
                ''')

    async def test_edgeql_functions_duration_trunc_01(self):
        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'hours');
            ''',
            {'PT15H'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'minutes');
            ''',
            {'PT15H1M'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'seconds');
            ''',
            {'PT15H1M22S'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'milliseconds');
            ''',
            {'PT15H1M22.306S'},
        )

        # Currently no-op but may be useful if precision is improved
        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'microseconds');
            ''',
            {'PT15H1M22.306916S'},
        )

    async def test_edgeql_functions_duration_trunc_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_truncate'):
            await self.con.execute(
                r'''
                SELECT <str>duration_truncate(
                    <duration>'73 hours', 'day');
                ''')

    async def test_edgeql_functions_duration_trunc_03(self):
        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::relative_duration>'P1312Y',
                'centuries'
            );
            ''',
            {'P1300Y'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::relative_duration>'P1312Y',
                'decades'
            );
            ''',
            {'P1310Y'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    cal::duration_normalize_hours(
                        <cal::relative_duration>'PT15000H',
                    )
                ),
                'years'
            );
            ''',
            {'P1Y'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    cal::duration_normalize_hours(
                        <cal::relative_duration>'PT15000H'
                    )
                ),
                'quarters'
            );
            ''',
            {'P1Y6M'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    cal::duration_normalize_hours(
                        <cal::relative_duration>'PT15000H'
                    )
                ),
                'months'
            );
            ''',
            {'P1Y8M'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    cal::duration_normalize_hours(
                        <cal::relative_duration>'PT15000H'
                    )
                ),
                'days'
            );
            ''',
            {'P1Y8M25D'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::relative_duration>'15:01:22.306916', 'hours');
            ''',
            {'PT15H'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::relative_duration>'15:01:22.306916', 'minutes');
            ''',
            {'PT15H1M'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::relative_duration>'15:01:22.306916', 'seconds');
            ''',
            {'PT15H1M22S'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::relative_duration>'15:01:22.306916', 'milliseconds');
            ''',
            {'PT15H1M22.306S'},
        )

        # Currently no-op but may be useful if precision is improved
        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::relative_duration>'15:01:22.306916', 'microseconds');
            ''',
            {'PT15H1M22.306916S'},
        )

    async def test_edgeql_functions_duration_trunc_04(self):
        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::date_duration>'P1312Y',
                'centuries'
            );
            ''',
            {'P1300Y'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <cal::date_duration>'P1312Y',
                'decades'
            );
            ''',
            {'P1310Y'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    <cal::date_duration>'P1312D'
                ),
                'years'
            );
            ''',
            {'P3Y'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    <cal::date_duration>'P1312D'
                ),
                'quarters'
            );
            ''',
            {'P3Y6M'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    <cal::date_duration>'P1312D'
                ),
                'months'
            );
            ''',
            {'P3Y7M'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                cal::duration_normalize_days(
                    <cal::date_duration>'P1312D'
                ),
                'days'
            );
            ''',
            {'P3Y7M22D'},
        )

    async def test_edgeql_functions_duration_trunc_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid unit for std::duration_truncate'):
            await self.con.execute(
                r'''
                SELECT <str>duration_truncate(
                    <cal::date_duration>'42 days', 'hours');
                ''')

    async def test_edgeql_functions_to_datetime_01(self):
        await self.assert_query_result(
            r'''
                SELECT <str>to_datetime(
                    2018, 5, 7, 15, 1, 22.306916, 'EST');
            ''',
            ['2018-05-07T20:01:22.306916+00:00'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>to_datetime(
                    2018, 5, 7, 15, 1, 22.306916, '-5');
            ''',
            ['2018-05-07T20:01:22.306916+00:00'],
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('SELECT to_datetime("2017-10-10", "")')

    async def test_edgeql_functions_to_datetime_02(self):
        await self.assert_query_result(
            r'''
                SELECT <str>to_datetime(
                    cal::to_local_datetime(2018, 5, 7, 15, 1, 22.306916),
                    'EST')
            ''',
            ['2018-05-07T20:01:22.306916+00:00'],
        )

    async def test_edgeql_functions_to_datetime_03(self):
        await self.assert_query_result(
            r'''
                SELECT
                    to_datetime('2019/01/01 00:00:00 0715',
                                'YYYY/MM/DD H24:MI:SS TZHTZM') =
                    <datetime>'2019-01-01T00:00:00+0715';
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT
                    to_datetime('2019/01/01 00:00:00 07TZM',
                                'YYYY/MM/DD H24:MI:SS TZH"TZM"') =
                    <datetime>'2019-01-01T00:00:00+07';
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT
                    to_datetime('2019/01/01 00:00:00 TZH07TZM',
                                'YYYY/MM/DD H24:MI:SS "TZH"TZH"TZM"') =
                    <datetime>'2019-01-01T00:00:00+07';
            ''',
            [True],
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'missing required time zone in format'):
            async with self.con.transaction():
                await self.con.query(r'''
                    SELECT
                        to_datetime('2019/01/01 00:00:00 TZH07',
                                    'YYYY/MM/DD H24:MI:SS "TZH"TZM') =
                        <datetime>'2019-01-01T00:00:00+07';
                ''')

    async def test_edgeql_functions_to_datetime_04(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'missing required time zone in input'):
            async with self.con.transaction():
                await self.con.query(r'''
                    SELECT
                        to_datetime('2019/01/01 00:00:00 0715',
                                    'YYYY/MM/DD H24:MI:SS "NOPE"TZHTZM');
                ''')

    async def test_edgeql_functions_to_datetime_05(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'invalid input syntax'):
            async with self.con.transaction():
                # omitting time zone
                await self.con.query(r'''
                    SELECT
                        to_datetime('2019/01/01 00:00:00');
                ''')

    async def test_edgeql_functions_to_datetime_06(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT to_datetime(10000, 1, 1, 1, 1, 1, 'UTC');
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT to_datetime(0, 1, 1, 1, 1, 1, 'UTC');
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT to_datetime(-1, 1, 1, 1, 1, 1, 'UTC');
            ''')

    async def test_edgeql_functions_to_local_datetime_01(self):
        await self.assert_query_result(
            r'''
                SELECT <str>cal::to_local_datetime(
                    <datetime>'2018-05-07T20:01:22.306916+00:00',
                    'America/Los_Angeles');
            ''',
            ['2018-05-07T13:01:22.306916'],
        )

    async def test_edgeql_functions_to_local_datetime_02(self):
        await self.assert_query_result(
            r'''
              SELECT <str>cal::to_local_datetime(2018, 5, 7, 15, 1, 22.306916);
            ''',
            ['2018-05-07T15:01:22.306916'],
        )

    async def test_edgeql_functions_to_local_datetime_03(self):
        await self.assert_query_result(
            # The time zone is ignored because the format string just
            # specifies arbitrary characters in its place.
            r'''
                SELECT
                    cal::to_local_datetime('2019/01/01 00:00:00 0715',
                                      'YYYY/MM/DD H24:MI:SS "NOTZ"') =
                    <cal::local_datetime>'2019-01-01T00:00:00';
            ''',
            [True],
        )

        await self.assert_query_result(
            # The time zone is ignored because the format string does
            # not expect to parse it.
            r'''
                SELECT
                    cal::to_local_datetime('2019/01/01 00:00:00 0715',
                                      'YYYY/MM/DD H24:MI:SS') =
                    <cal::local_datetime>'2019-01-01T00:00:00';
            ''',
            [True],
        )

    async def test_edgeql_functions_to_local_datetime_04(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'unexpected time zone in format'):
            async with self.con.transaction():
                await self.con.query(
                    r'''
                        SELECT
                          cal::to_local_datetime('2019/01/01 00:00:00 0715',
                                                 'YYYY/MM/DD H24:MI:SS TZH') =
                          <cal::local_datetime>'2019-01-01T00:00:00';
                    ''')

    async def test_edgeql_functions_to_local_datetime_05(self):
        await self.assert_query_result(
            # Make sure that time zone change (while converting
            # `to_local_datetime`) is not leaking.
            r'''
                SELECT (<str><cal::local_datetime>'2019-01-01 00:00:00',
                        <str>cal::to_local_datetime('2019/01/01 00:00:00 0715',
                                                    'YYYY/MM/DD H24:MI:SS'),
                        <str><cal::local_datetime>'2019-02-01 00:00:00');
            ''',
            [['2019-01-01T00:00:00',
              '2019-01-01T00:00:00',
              '2019-02-01T00:00:00']],
        )

    async def test_edgeql_functions_to_local_datetime_06(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'invalid input syntax'):
            async with self.con.transaction():
                # including time zone
                await self.con.query(r'''
                    SELECT
                        cal::to_local_datetime('2019/01/01 00:00:00 0715');
                ''')

    async def test_edgeql_functions_to_local_datetime_07(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT cal::to_local_datetime(10000, 1, 1, 1, 1, 1);
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT cal::to_local_datetime(0, 1, 1, 1, 1, 1);
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT cal::to_local_datetime(-1, 1, 1, 1, 1, 1);
            ''')

    async def test_edgeql_functions_to_local_date_01(self):
        await self.assert_query_result(
            r'''
                SELECT <str>cal::to_local_date(2018, 5, 7);
            ''',
            ['2018-05-07'],
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(
                    'SELECT cal::to_local_date("2017-10-10", "")')

    async def test_edgeql_functions_to_local_date_02(self):
        await self.assert_query_result(
            r'''
                SELECT <str>cal::to_local_date(
                    <datetime>'2018-05-07T20:01:22.306916+00:00',
                    'America/Los_Angeles');
            ''',
            ['2018-05-07'],
        )

    async def test_edgeql_functions_to_local_date_03(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'unexpected time zone in format'):
            async with self.con.transaction():
                await self.con.query(
                    r'''
                        SELECT
                            cal::to_local_date('2019/01/01 00:00:00 0715',
                                               'YYYY/MM/DD H24:MI:SS TZH') =
                            <cal::local_date>'2019-01-01';
                    ''')

    async def test_edgeql_functions_to_local_date_04(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'invalid input syntax'):
            async with self.con.transaction():
                # including too much
                await self.con.query(r'''
                    SELECT
                        cal::to_local_date('2019/01/01 00:00:00 0715');
                ''')

    async def test_edgeql_functions_to_local_date_05(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT cal::to_local_date(10000, 1, 1);
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT cal::to_local_date(0, 1, 1);
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.query(r'''
                SELECT cal::to_local_date(-1, 1, 1);
            ''')

    async def test_edgeql_functions_to_local_time_01(self):
        await self.assert_query_result(
            r'''
                SELECT <str>cal::to_local_time(15, 1, 22.306916);
            ''',
            ['15:01:22.306916'],
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(
                    'SELECT cal::to_local_time("12:00:00", "")')

    async def test_edgeql_functions_to_local_time_02(self):
        await self.assert_query_result(
            r'''
                SELECT <str>cal::to_local_time(
                    <datetime>'2018-05-07T20:01:22.306916+00:00',
                    'America/Los_Angeles');
            ''',
            ['13:01:22.306916'],
        )

    async def test_edgeql_functions_to_local_time_03(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'unexpected time zone in format'):
            async with self.con.transaction():
                await self.con.query(
                    r'''
                        SELECT
                            cal::to_local_time('00:00:00 0715',
                                          'H24:MI:SS TZH') =
                            <cal::local_time>'00:00:00';
                    ''')

    async def test_edgeql_functions_to_local_time_04(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'invalid input syntax'):
            async with self.con.transaction():
                # including time zone
                await self.con.query(r'''
                    SELECT
                        cal::to_local_datetime('00:00:00 0715');
                ''')

    async def test_edgeql_functions_to_local_time_05(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'std::cal::local_time field value out of range'
        ):
            async with self.con.transaction():
                # including time zone
                await self.con.query(r'''
                    SELECT
                        cal::to_local_time('24:00:00');
                ''')

    async def test_edgeql_functions_to_local_time_06(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'std::cal::local_time field value out of range'
        ):
            async with self.con.transaction():
                # including time zone
                await self.con.query(r'''
                    SELECT
                        cal::to_local_time(23, 59, 60);
                ''')

    async def test_edgeql_functions_to_local_time_07(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'std::cal::local_time field value out of range'
        ):
            async with self.con.transaction():
                # including time zone
                await self.con.query(r'''
                    SELECT
                        <cal::local_time>'23:59:59.999999999999';
                ''')

    async def test_edgeql_functions_to_local_time_08(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'std::cal::local_time field value out of range'
        ):
            async with self.con.transaction():
                # including time zone
                await self.con.query(r'''
                    SELECT
                        <cal::local_time><json>'24:00:00';
                ''')

    async def test_edgeql_functions_to_duration_01(self):
        await self.assert_query_result(
            r'''SELECT <str>to_duration(hours:=20);''',
            ['PT20H'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(minutes:=20);''',
            ['PT20M'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(seconds:=20);''',
            ['PT20S'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(seconds:=20.15);''',
            ['PT20.15S'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(microseconds:=100);''',
            ['PT0.0001S'],
        )

    async def test_edgeql_functions_to_duration_02(self):
        await self.assert_query_result(
            r'''SELECT to_duration(hours:=20) > to_duration(minutes:=20);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT to_duration(minutes:=20) > to_duration(seconds:=20);''',
            [True],
        )

    async def test_edgeql_functions_duration_to_seconds(self):
        await self.assert_query_result(
            r'''SELECT duration_to_seconds(<duration>'20 hours');''',
            [72000.0],
        )

        await self.assert_query_result(
            r'''SELECT duration_to_seconds(<duration>'1:02:03.000123');''',
            [3723.000123],
        )

    async def test_edgeql_functions_duration_to_seconds_exact(self):
        # at this value extract(epoch from duration) is imprecise
        await self.assert_query_result(
            r'''SELECT duration_to_seconds(
                <duration>'1801439850 seconds 123456 microseconds');''',
            [1801439850.123456],
        )

    async def test_edgeql_functions_duration_normalize_01(self):
        # 350 days = 350 * 24 * 3600 seconds = 30240000 seconds
        await self.assert_query_result(
            r'''select <cal::relative_duration>'30240000 seconds';''',
            ['PT8400H'],
            [edgedb.RelativeDuration(microseconds=30_240_000_000_000)],
        )

        await self.assert_query_result(
            r'''select cal::duration_normalize_hours(
                <cal::relative_duration>'30240000 seconds');''',
            ['P350D'],
            [edgedb.RelativeDuration(days=350)],
        )

        await self.assert_query_result(
            r'''select cal::duration_normalize_days(
                <cal::relative_duration>'350 days');''',
            ['P11M20D'],
            [edgedb.RelativeDuration(months=11, days=20)],
        )

        await self.assert_query_result(
            r'''select cal::duration_normalize_days(
                    cal::duration_normalize_hours(
                        <cal::relative_duration>'30240000 seconds'));''',
            ['P11M20D'],
            [edgedb.RelativeDuration(months=11, days=20)],
        )

    async def test_edgeql_functions_duration_normalize_02(self):
        # duration_normalize_days has an overloaded version specifically for
        # date_duration, so that the return type is also date_duration and
        # doesn't require a cast.
        await self.assert_query_result(
            r'''select <str>cal::duration_normalize_days(
                <cal::date_duration>'350 days');''',
            ['P11M20D'],
        )

    async def test_edgeql_functions_to_str_01(self):
        # at the very least the cast <str> should be equivalent to
        # a call to to_str() without explicit format for simple scalars
        await self.assert_query_result(
            r'''
                WITH DT := datetime_current()
                # FIXME: the cast has a "T" and the str doesn't for some reason
                SELECT <str>DT = to_str(DT);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
            WITH D := cal::to_local_date(datetime_current(), 'UTC')
            SELECT <str>D = to_str(D);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
            WITH NT := cal::to_local_time(datetime_current(), 'UTC')
            SELECT <str>NT = to_str(NT);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <str>123 = to_str(123);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <str>123.456 = to_str(123.456);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <str>123.456e-20 = to_str(123.456e-20);''',
            [True],
        )

        await self.assert_query_result(
            r'''
            SELECT <str><decimal>'123456789012345678901234567890.1234567890' =
                to_str(123456789012345678901234567890.1234567890n);
            ''',
            [True],
        )

        # Empty format string shouldn't produce an empty set.
        #

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''SELECT to_str(1, "")''')

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''SELECT to_str(1.1, "")''')

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''SELECT to_str(1.1n, "")''')

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(
                    r'''SELECT to_str(to_json('{}'), "")''')

    async def test_edgeql_functions_to_str_02(self):
        await self.assert_query_result(
            r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'YYYY-MM-DD');
            ''',
            {'2018-05-07'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'YYYYBC');
            ''',
            {'2018AD'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'FMDDth "of" FMMonth, YYYY');
            ''',
            {'7th of May, 2018'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'CCth "century"');
            ''',
            {'21st century'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'Y,YYY Month DD Day');
            ''',
            {'2,018 May       07 Monday   '},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'foo');
            ''',
            {'foo'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, ' ');
            ''',
            {' '}
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''
                    WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
                    SELECT to_str(DT, '');
                ''')

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''
                    WITH DT := to_duration(hours:=20)
                    SELECT to_str(DT, '');
                ''')

    async def test_edgeql_functions_to_str_03(self):
        await self.assert_query_result(
            r'''
                WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
                SELECT to_str(DT, 'HH:MI A.M.');
            ''',
            # tests run in UTC time-zone, so 15:01-05 is 20:01 UTC
            {'08:01 P.M.'},
        )

    async def test_edgeql_functions_to_str_04(self):
        await self.assert_query_result(
            r'''
            WITH DT := <cal::local_date>'2018-05-07'
            SELECT to_str(DT, 'YYYY-MM-DD');
            ''',
            {'2018-05-07'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <cal::local_date>'2018-05-07'
            SELECT to_str(DT, 'YYYYBC');
            ''',
            {'2018AD'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <cal::local_date>'2018-05-07'
            SELECT to_str(DT, 'FMDDth "of" FMMonth, YYYY');
            ''',
            {'7th of May, 2018'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <cal::local_date>'2018-05-07'
            SELECT to_str(DT, 'CCth "century"');
            ''',
            {'21st century'},
        )

        await self.assert_query_result(
            r'''
            WITH DT := <cal::local_date>'2018-05-07'
            SELECT to_str(DT, 'Y,YYY Month DD Day');
            ''',
            {'2,018 May       07 Monday   '},
        )

        await self.assert_query_result(
            r'''
            # the format string doesn't have any special characters
            WITH DT := <cal::local_date>'2018-05-07'
            SELECT to_str(DT, 'foo');
            ''',
            {'foo'},
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''
                    WITH DT := <cal::local_time>'12:00:00'
                    SELECT to_str(DT, '');
                ''')

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''
                    WITH DT := <cal::local_date>'2018-05-07'
                    SELECT to_str(DT, '');
                ''')

    async def test_edgeql_functions_to_str_05(self):
        await self.assert_query_result(
            r'''SELECT to_str(123456789, '99');''',
            {' ##'},  # the number is too long for the desired representation
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, '999999999');''',
            {' 123456789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, '999,999,999');''',
            {' 123,456,789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, '999,999,999,999');''',
            {'     123,456,789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'FM999,999,999,999');''',
            {'123,456,789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'S999,999,999,999');''',
            {'    +123,456,789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'SG999,999,999,999');''',
            {'+    123,456,789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'S099,999,999,999');''',
            {'+000,123,456,789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'SG099,999,999,999');''',
            {'+000,123,456,789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'S099999999999');''',
            {'+000123456789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'S990999999999');''',
            {'  +0123456789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123456789, 'FMS990999999999');''',
            {'+0123456789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(-123456789, '999999999PR');''',
            {'<123456789>'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(987654321, 'FM999999999th');''',
            {'987654321st'},
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(r'''SELECT to_str(987654321, '');''',)

    async def test_edgeql_functions_to_str_06(self):
        await self.assert_query_result(
            r'''SELECT to_str(123.456789, '99');''',
            {' ##'},  # the integer part of the number is too long
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789, '999');''',
            {' 123'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789, '999.999');''',
            {' 123.457'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789, '999.999999999');''',
            {' 123.456789000'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789, 'FM999.999999999');''',
            {'123.456789'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789e-20, '999.999999999');''',
            {'    .000000000'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789e-20, 'FM999.999999999');''',
            {'0.'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789e-20, '099.999999990');''',
            {' 000.000000000'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789e-20, 'FM990.099999999');''',
            {'0.0'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789e-20, '0.0999EEEE');''',
            {' 1.2346e-18'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(123.456789e20, '0.0999EEEE');''',
            {' 1.2346e+22'},
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(
                    r'''SELECT to_str(123.456789e20, '');''')

    async def test_edgeql_functions_to_str_07(self):
        await self.assert_query_result(
            r'''SELECT to_str(<cal::local_time>'15:01:22', 'HH:MI A.M.');''',
            {'03:01 P.M.'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(<cal::local_time>'15:01:22', 'HH:MI:SSam.');''',
            {'03:01:22pm.'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(<cal::local_time>'15:01:22', 'HH24:MI');''',
            {'15:01'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(<cal::local_time>'15:01:22', ' ');''',
            {' '},
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query(
                    r'''SELECT to_str(<cal::local_time>'15:01:22', '');''',)

    async def test_edgeql_functions_string_bytes_conversion(self):
        string = "Паляниця"

        await self.assert_query_result(
            r'''
            WITH
                input := <bytes>$input,
                string := to_str(input),
                binary := to_bytes(string),
            SELECT
                binary = input;
            ''',
            {True},
            variables={
                "input": string.encode("utf-8"),
            },
        )

    async def test_edgeql_functions_string_bytes_conversion_error(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'invalid byte sequence for encoding "UTF8": 0x00',
        ):
            await self.con.execute(
                r'''
                SELECT to_str(b'\x00')
                ''',
            )

    async def test_edgeql_functions_int_bytes_conversion_01(self):
        # Make sure we can convert the bytes to ints and back
        for num in range(256):
            byte = num.to_bytes()
            for numbytes in [2, 4, 8]:
                raw = byte * numbytes
                typename = f'int{numbytes * 8}'
                await self.assert_query_result(
                    f'''
                    WITH
                        val_b := <{typename}>$val_b,
                        val_l := <{typename}>$val_l,
                        bin := <bytes>$bin,
                    SELECT (
                        val_b = to_{typename}(bin, Endian.Big),
                        val_l = to_{typename}(bin, Endian.Little),
                        bin = to_bytes(val_b, Endian.Big),
                        bin = to_bytes(val_l, Endian.Little),
                    )
                    ''',
                    {(True, True, True, True)},
                    variables={
                        "val_b": int.from_bytes(raw, 'big', signed=True),
                        "val_l": int.from_bytes(raw, 'little', signed=True),
                        "bin": raw,
                    },
                    msg=f'Failed to convert {raw!r} to int or vice versa'
                )

    async def test_edgeql_functions_int_bytes_conversion_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_int16.*the argument must be exactly 2 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_int16(b'\x01', Endian.Big)
                    ''',
                )

        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_int16.*the argument must be exactly 2 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_int16(
                        to_bytes(<int32>123, Endian.Big),
                        Endian.Big,
                    )
                    ''',
                )

    async def test_edgeql_functions_int_bytes_conversion_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_int32.*the argument must be exactly 4 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_int32(
                        to_bytes(<int16>23, Endian.Big),
                        Endian.Big,
                    )
                    ''',
                )

        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_int32.*the argument must be exactly 4 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_int32(
                        to_bytes(<int64>16908295, Endian.Big),
                        Endian.Big,
                    )
                    ''',
                )

    async def test_edgeql_functions_int_bytes_conversion_04(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_int64.*the argument must be exactly 8 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_int64(
                        to_bytes(<int16>23, Endian.Big),
                        Endian.Big,
                    )
                    ''',
                )

        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_int64.*the argument must be exactly 8 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_int64(
                        b'\x00' ++ to_bytes(62620574343574340, Endian.Big),
                        Endian.Big,
                    )
                    ''',
                )

    async def test_edgeql_functions_uuid_bytes_conversion_01(self):
        uuid_val = uuid.uuid4()

        await self.assert_query_result(
            r'''
            WITH
                uuid_input := <uuid>$uuid_input,
                bin_input := <bytes>$bin_input,
            SELECT (
                bin_input = to_bytes(uuid_input),
                uuid_input = to_uuid(bin_input),
            )
            ''',
            {(True, True)},
            variables={
                "uuid_input": uuid_val,
                "bin_input": uuid_val.bytes,
            },
        )

    async def test_edgeql_functions_uuid_bytes_conversion_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_uuid.*the argument must be exactly 16 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_uuid(to_bytes(uuid_generate_v4())[:10])
                    ''',
                )

        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'to_uuid.*the argument must be exactly 16 bytes long',
        ):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    SELECT to_uuid(b'\xff\xff' ++ to_bytes(uuid_generate_v4()))
                    ''',
                )

    async def test_edgeql_functions_array_join_01(self):
        await self.assert_query_result(
            r'''SELECT array_join(['one', 'two', 'three'], ', ');''',
            ['one, two, three'],
        )

        await self.assert_query_result(
            r'''SELECT array_join(['one', 'two', 'three'], '');''',
            ['onetwothree'],
        )

        await self.assert_query_result(
            r'''SELECT array_join(<array<str>>[], ', ');''',
            [''],
        )

    async def test_edgeql_functions_array_join_02(self):
        await self.assert_query_result(
            r'''SELECT array_join(['one', 'two', 'three'], {', ', '@!'});''',
            {'one, two, three', 'one@!two@!three'},
        )

    async def test_edgeql_functions_array_join_03(self):
        await self.assert_query_result(
            r'''SELECT array_join([b'one', b'two', b'three'], b', ');''',
            [base64.b64encode(b'one, two, three').decode()],
            [b'one, two, three'],
        )

        await self.assert_query_result(
            r'''SELECT array_join([b'one', b'two', b'three'], b'');''',
            [base64.b64encode(b'onetwothree').decode()],
            [b'onetwothree'],
        )

        await self.assert_query_result(
            r'''SELECT array_join(<array<bytes>>[], b', ');''',
            [base64.b64encode(b'').decode()],
            [b''],
        )

    async def test_edgeql_functions_array_join_04(self):
        await self.assert_query_result(
            r'''
            SELECT array_join([b'one', b'two', b'three'], {b', ', b'@!'});
            ''',
            {
                base64.b64encode(b'one, two, three').decode(),
                base64.b64encode(b'one@!two@!three').decode(),
            },
            {b'one, two, three', b'one@!two@!three'},
        )

    async def test_edgeql_functions_str_split_01(self):
        await self.assert_query_result(
            r'''SELECT str_split('one, two, three', ', ');''',
            [['one', 'two', 'three']],
        )

        await self.assert_query_result(
            r'''SELECT str_split('', ', ');''',
            [[]],
        )

        await self.assert_query_result(
            r'''SELECT str_split('foo', ', ');''',
            [['foo']],
        )

        await self.assert_query_result(
            r'''SELECT str_split('foo', '');''',
            [['f', 'o', 'o']],
        )

    async def test_edgeql_functions_to_int_01(self):
        await self.assert_query_result(
            r'''SELECT to_int64(' 123456789', '999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64(' 123,456,789', '999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('     123,456,789', '999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('123,456,789', 'FM999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('    +123,456,789', 'S999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('+    123,456,789', 'SG999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('+000,123,456,789', 'S099,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('+000,123,456,789', 'SG099,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('+000123456789', 'S099999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('  +0123456789', 'S990999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('+0123456789', 'FMS990999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('<123456789>', '999999999PR');''',
            {-123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('987654321st', 'FM999999999th');''',
            {987654321},
        )

        await self.assert_query_result(
            r'''SELECT to_int64('987654321st', <str>$0);''',
            {987654321},
            variables=('FM999999999th',),
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_int64('1', '')''')

    async def test_edgeql_functions_to_int_02(self):
        await self.assert_query_result(
            r'''SELECT to_int32(' 123456789', '999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32(' 123,456,789', '999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('     123,456,789', '999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('123,456,789', 'FM999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('    +123,456,789', 'S999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('+    123,456,789', 'SG999,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('+000,123,456,789', 'S099,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('+000,123,456,789', 'SG099,999,999,999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('+000123456789', 'S099999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('  +0123456789', 'S990999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('+0123456789', 'FMS990999999999');''',
            {123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('<123456789>', '999999999PR');''',
            {-123456789},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('987654321st', 'FM999999999th');''',
            {987654321},
        )

        await self.assert_query_result(
            r'''SELECT to_int32('987654321st', <str>$0);''',
            {987654321},
            variables=('FM999999999th',),
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_int32('1', '')''')

    async def test_edgeql_functions_to_int_03(self):
        await self.assert_query_result(
            r'''SELECT to_int16('12345', '999999999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('12,345', '999,999,999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('     12,345', '999,999,999,999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('12,345', 'FM999,999,999,999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('+12,345', 'S999,999,999,999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('+    12,345', 'SG999,999,999,999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('-000,012,345', 'S099,999,999,999');''',
            {-12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('+000,012,345', 'SG099,999,999,999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('+00012345', 'S099999999999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('  +012345', 'S990999999999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('+012345', 'FMS990999999999');''',
            {12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('<12345>', '999999999PR');''',
            {-12345},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('4321st', 'FM999999999th');''',
            {4321},
        )

        await self.assert_query_result(
            r'''SELECT to_int16('4321st', <str>$0);''',
            {4321},
            variables=('FM999999999th',),
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_int16('1', '')''')

    async def test_edgeql_functions_to_float_01(self):
        await self.assert_query_result(
            r'''SELECT to_float64(' 123', '999');''',
            {123},
        )

        await self.assert_query_result(
            r'''SELECT to_float64('123.457', '999.999');''',
            {123.457},
        )

        await self.assert_query_result(
            r'''SELECT to_float64(' 123.456789000', '999.999999999');''',
            {123.456789},
        )

        await self.assert_query_result(
            r'''SELECT to_float64('123.456789', 'FM999.999999999');''',
            {123.456789},
        )
        await self.assert_query_result(
            r'''SELECT to_float64('123.456789', <str>$0);''',
            {123.456789},
            variables=('FM999.999999999',)
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_float64('1', '')''')

    async def test_edgeql_functions_to_float_02(self):
        await self.assert_query_result(
            r'''SELECT to_float32(' 123', '999');''',
            {123},
        )

        await self.assert_query_result(
            r'''SELECT to_float32('123.457', '999.999');''',
            {123.457},
        )

        await self.assert_query_result(
            r'''SELECT to_float32(' 123.456789000', '999.999999999');''',
            {123.457},
        )

        await self.assert_query_result(
            r'''SELECT to_float32('123.456789', 'FM999.999999999');''',
            {123.457},
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_float32('1', '')''')

    async def test_edgeql_functions_to_bigint_01(self):
        await self.assert_query_result(
            r'''SELECT to_bigint(' 123', '999');''',
            {123},
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_bigint('1', '')''')

    async def test_edgeql_functions_to_bigint_02(self):
        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    'invalid input syntax'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_bigint('1.02')''')

    async def test_edgeql_functions_to_decimal_01(self):
        await self.assert_query_result(
            r'''SELECT to_decimal(' 123', '999');''',
            {123},
        )

        await self.assert_query_result(
            r'''SELECT to_decimal('123.457', '999.999');''',
            exp_result_json={123.457},
            exp_result_binary={decimal.Decimal('123.457')},
        )

        await self.assert_query_result(
            r'''SELECT to_decimal(' 123.456789000', '999.999999999');''',
            exp_result_json={123.456789},
            exp_result_binary={decimal.Decimal('123.456789')},
        )

        await self.assert_query_result(
            r'''SELECT to_decimal('123.456789', 'FM999.999999999');''',
            exp_result_json={123.456789},
            exp_result_binary={decimal.Decimal('123.456789')},
        )

        with self.assertRaisesRegex(edgedb.InvalidValueError,
                                    '"fmt" argument must be'):
            async with self.con.transaction():
                await self.con.query('''SELECT to_decimal('1', '')''')

    async def test_edgeql_functions_to_decimal_02(self):
        await self.assert_query_result(
            r'''
            SELECT to_decimal(
                '123456789123456789123456789.123456789123456789123456789',
                'FM999999999999999999999999999.999999999999999999999999999');
            ''',
            exp_result_json={
                123456789123456789123456789.123456789123456789123456789},
            exp_result_binary={decimal.Decimal(
                '123456789123456789123456789.123456789123456789123456789')},
        )

    async def test_edgeql_functions_len_01(self):
        await self.assert_query_result(
            r'''SELECT len('');''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT len('hello');''',
            [5],
        )

        await self.assert_query_result(
            r'''SELECT __std__::len({'hello', 'world'});''',
            [5, 5]
        )

    async def test_edgeql_functions_len_02(self):
        await self.assert_query_result(
            r'''SELECT len(b'');''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT len(b'hello');''',
            [5],
        )

        await self.assert_query_result(
            r'''SELECT len({b'hello', b'world'});''',
            [5, 5]
        )

    async def test_edgeql_functions_len_03(self):
        await self.assert_query_result(
            r'''SELECT len(<array<str>>[]);''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT len([]);''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT len(['hello']);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT len(['hello', 'world']);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT len([1, 2, 3, 4, 5]);''',
            [5],
        )

        await self.assert_query_result(
            r'''SELECT len({['hello'], ['hello', 'world']});''',
            {1, 2},
        )

    @test.xerror(
        "Known collation issue on Heroku Postgres",
        unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    )
    async def test_edgeql_functions_min_01(self):
        await self.assert_query_result(
            r'''SELECT min(<int64>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT min(4);''',
            [4],
        )

        await self.assert_query_result(
            r'''SELECT min({10, 20, -3, 4});''',
            [-3],
        )

        await self.assert_query_result(
            r'''SELECT min({10, 2.5, -3.1, 4});''',
            [-3.1],
        )

        await self.assert_query_result(
            r'''SELECT min({'10', '20', '-3', '4'});''',
            ['-3'],
        )

        await self.assert_query_result(
            r'''SELECT min({'10', 'hello', 'world', '-3', '4'});''',
            ['-3'],
        )

        await self.assert_query_result(
            r'''SELECT min({'hello', 'world'});''',
            ['hello'],
        )

        await self.assert_query_result(
            r'''SELECT min({[1, 2], [3, 4]});''',
            [[1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT min({[1, 2], [3, 4], <array<int64>>[]});''',
            [[]],
        )

        await self.assert_query_result(
            r'''SELECT min({[1, 2], [1, 0.4]});''',
            [[1, 0.4]],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>min(<datetime>{
                    '2018-05-07T15:01:22.306916-05',
                    '2017-05-07T16:01:22.306916-05',
                    '2017-01-07T11:01:22.306916-05',
                    '2018-01-07T11:12:22.306916-05',
                });
            ''',
            ['2017-01-07T16:01:22.306916+00:00'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>min(<cal::local_datetime>{
                    '2018-05-07T15:01:22.306916',
                    '2017-05-07T16:01:22.306916',
                    '2017-01-07T11:01:22.306916',
                    '2018-01-07T11:12:22.306916',
                });
            ''',
            ['2017-01-07T11:01:22.306916'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>min(<cal::local_date>{
                    '2018-05-07',
                    '2017-05-07',
                    '2017-01-07',
                    '2018-01-07',
                });
            ''',
            ['2017-01-07'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>min(<cal::local_time>{
                    '15:01:22',
                    '16:01:22',
                    '11:01:22',
                    '11:12:22',
                });
            ''',
            ['11:01:22'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>min(<duration>{
                    '15:01:22',
                    '16:01:22',
                    '11:01:22',
                    '11:12:22',
                });
            ''',
            ['PT11H1M22S'],
        )

    async def test_edgeql_functions_min_02(self):
        await self.assert_query_result(
            r'''
                SELECT min(User.name);
            ''',
            ['Elvis'],
        )

        await self.assert_query_result(
            r'''
                SELECT min(Issue.time_estimate);
            ''',
            [3000],
        )

        await self.assert_query_result(
            r'''
                SELECT min(<int64>Issue.number);
            ''',
            [1],
        )

    async def test_edgeql_functions_min_03(self):
        # Objects are valid inputs to "min" and are ordered by their .id.
        await self.assert_query_result(
            r'''
            SELECT min(User).id = min(User.id);
            ''',
            [True],
        )

    async def test_edgeql_functions_max_01(self):
        await self.assert_query_result(
            r'''SELECT max(<int64>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT max(4);''',
            [4],
        )

        await self.assert_query_result(
            r'''SELECT max({10, 20, -3, 4});''',
            [20],
        )

        await self.assert_query_result(
            r'''SELECT max({10, 2.5, -3.1, 4});''',
            [10],
        )

        await self.assert_query_result(
            r'''SELECT max({'10', '20', '-3', '4'});''',
            ['4'],
        )

        await self.assert_query_result(
            r'''SELECT max({'10', 'hello', 'world', '-3', '4'});''',
            ['world'],
        )

        await self.assert_query_result(
            r'''SELECT max({'hello', 'world'});''',
            ['world'],
        )

        await self.assert_query_result(
            r'''SELECT max({[1, 2], [3, 4]});''',
            [[3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT max({[1, 2], [3, 4], <array<int64>>[]});''',
            [[3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT max({[1, 2], [1, 0.4]});''',
            [[1, 2]],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>max(<datetime>{
                    '2018-05-07T15:01:22.306916-05',
                    '2017-05-07T16:01:22.306916-05',
                    '2017-01-07T11:01:22.306916-05',
                    '2018-01-07T11:12:22.306916-05',
                });
            ''',
            ['2018-05-07T20:01:22.306916+00:00'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>max(<cal::local_datetime>{
                    '2018-05-07T15:01:22.306916',
                    '2017-05-07T16:01:22.306916',
                    '2017-01-07T11:01:22.306916',
                    '2018-01-07T11:12:22.306916',
                });
            ''',
            ['2018-05-07T15:01:22.306916'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>max(<cal::local_date>{
                    '2018-05-07',
                    '2017-05-07',
                    '2017-01-07',
                    '2018-01-07',
                });
            ''',
            ['2018-05-07'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>max(<cal::local_time>{
                    '15:01:22',
                    '16:01:22',
                    '11:01:22',
                    '11:12:22',
                });
            ''',
            ['16:01:22'],
        )

        await self.assert_query_result(
            r'''
                SELECT <str>max(<duration>{
                    '15:01:22',
                    '16:01:22',
                    '11:01:22',
                    '11:12:22',
                });
            ''',
            ['PT16H1M22S'],
        )

    async def test_edgeql_functions_max_02(self):
        await self.assert_query_result(
            r'''
                SELECT max(User.name);
            ''',
            ['Yury'],
        )

        await self.assert_query_result(
            r'''
                SELECT max(Issue.time_estimate);
            ''',
            [3000],
        )

        await self.assert_query_result(
            r'''
            SELECT max(<int64>Issue.number);
            ''',
            [4],
        )

    async def test_edgeql_functions_max_03(self):
        # Objects are valid inputs to "max" and are ordered by their .id.
        await self.assert_query_result(
            r'''
            SELECT max(User).id = max(User.id);
            ''',
            [True],
        )

    async def test_edgeql_functions_max_04(self):
        await self.assert_query_result(
            r'''
            select max(array_unpack(array_agg(User))) { name };
            ''',
            [{'name': str}],
        )

    async def test_edgeql_functions_all_01(self):
        await self.assert_query_result(
            r'''SELECT all(<bool>{});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT all({True});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT all({False});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT all({True, False, True, False});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT all({1, 2, 3, 4} > 0);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT all({1, -2, 3, 4} > 0);''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT all({0, -1, -2, -3} > 0);''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT all({1, -2, 3, 4} IN {-2, -1, 0, 1, 2, 3, 4});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT all(<int64>{} IN {-2, -1, 0, 1, 2, 3, 4});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT all({1, -2, 3, 4} IN <int64>{});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT all(<int64>{} IN <int64>{});''',
            [True],
        )

    async def test_edgeql_functions_all_02(self):
        await self.assert_query_result(
            r'''
                SELECT all(len(User.name) = 4);
            ''',
            [False],
        )

        await self.assert_query_result(
            r'''
                SELECT all(
                    (
                        FOR I IN {Issue}
                        UNION EXISTS I.time_estimate
                    )
                );
            ''',
            [False],
        )

        await self.assert_query_result(
            r'''
                SELECT all(Issue.number != '');
                ''',
            [True],
        )

    async def test_edgeql_functions_any_01(self):
        await self.assert_query_result(
            r'''SELECT any(<bool>{});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT any({True});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT any({False});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT any({True, False, True, False});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT any({1, 2, 3, 4} > 0);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT any({1, -2, 3, 4} > 0);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT any({0, -1, -2, -3} > 0);''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT any({1, -2, 3, 4} IN {-2, -1, 0, 1, 2, 3, 4});''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT any(<int64>{} IN {-2, -1, 0, 1, 2, 3, 4});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT any({1, -2, 3, 4} IN <int64>{});''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT any(<int64>{} IN <int64>{});''',
            [False],
        )

    async def test_edgeql_functions_any_02(self):
        await self.assert_query_result(
            r'''
                SELECT any(len(User.name) = 4);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT any(
                    (
                        FOR I IN {Issue}
                        UNION EXISTS I.time_estimate
                    )
                );
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT any(Issue.number != '');
            ''',
            [True],
        )

    async def test_edgeql_functions_any_03(self):
        await self.assert_query_result(
            r'''
                SELECT any(len(User.name) = 4) =
                    NOT all(NOT (len(User.name) = 4));
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT any(
                    (
                        FOR I IN {Issue}
                        UNION EXISTS I.time_estimate
                    )
                ) = NOT all(
                    (
                        FOR I IN {Issue}
                        UNION NOT EXISTS I.time_estimate
                    )
                );
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT any(Issue.number != '') = NOT all(Issue.number = '');
            ''',
            [True],
        )

    async def test_edgeql_functions_round_01(self):
        await self.assert_query_result(
            r'''SELECT round(<float64>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>1);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>1);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>1.2);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>-1.2);''',
            [-1],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>1.2);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>-1.2);''',
            [-1],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>-2.5);''',
            [-2],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>-1.5);''',
            [-2],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>-0.5);''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>0.5);''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>1.5);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>2.5);''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>-2.5);''',
            [-3],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>-1.5);''',
            [-2],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>-0.5);''',
            [-1],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>0.5);''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>1.5);''',
            [2]
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>2.5);''',
            [3]
        )

    async def test_edgeql_functions_round_02(self):
        await self.assert_query_result(
            r'''SELECT round(1) IS int64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT round(<float32>1.2) IS float64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT round(<float64>1.2) IS float64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT round(1.2) IS float64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT round(<bigint>1) IS bigint;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>1.2) IS decimal;''',
            [True],
        )

        # rounding to a specified decimal place is only defined
        # for decimals
        await self.assert_query_result(
            r'''SELECT round(<decimal>1.2, 0) IS decimal;''',
            [True],
        )

    async def test_edgeql_functions_round_03(self):
        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, 10);''',
            [123.456],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, 3);''',
            [123.456],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, 2);''',
            [123.46],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, 1);''',
            [123.5],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, 0);''',
            [123],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, -1);''',
            [120],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, -2);''',
            [100],
        )

        await self.assert_query_result(
            r'''SELECT round(<decimal>123.456, -3);''',
            [0],
        )

    async def test_edgeql_functions_round_04(self):
        await self.assert_query_result(
            r'''
                SELECT _ := round(<int64>Issue.number / 2)
                ORDER BY _;
            ''',
            [0, 1, 2, 2],
        )

        await self.assert_query_result(
            r'''
                SELECT _ := round(<decimal>Issue.number / 2)
                ORDER BY _;
            ''',
            [1, 1, 2, 2],
        )

    async def test_edgeql_functions_contains_01(self):
        await self.assert_query_result(
            r'''SELECT std::contains(<array<int64>>[], {1, 3});''',
            [False, False],
        )

        await self.assert_query_result(
            r'''SELECT contains([1], {1, 3});''',
            [True, False],
        )

        await self.assert_query_result(
            r'''SELECT contains([1, 2], 1);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT contains([1, 2], 3);''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT contains(['a'], <str>{});''',
            [],
        )

    async def test_edgeql_functions_contains_02(self):
        await self.assert_query_result(
            r'''
                WITH x := [3, 1, 2]
                SELECT contains(x, 2);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH x := [3, 1, 2]
                SELECT contains(x, 5);
            ''',
            [False],
        )

        await self.assert_query_result(
            r'''
                WITH x := [3, 1, 2]
                SELECT contains(x, 5);
            ''',
            [False],
        )

    async def test_edgeql_functions_contains_03(self):
        await self.assert_query_result(
            r'''SELECT contains(<str>{}, <str>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT contains(<str>{}, 'a');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', <str>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', '');''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', 'q');''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', 'qwe');''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', 'we');''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', 't');''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', 'a');''',
            {False},
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', 'azerty');''',
            {False},
        )

    async def test_edgeql_functions_contains_04(self):
        await self.assert_query_result(
            r'''SELECT contains(<bytes>{}, <bytes>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT contains(<bytes>{}, b'a');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT contains(b'qwerty', <bytes>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT contains(b'qwerty', b't');''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT contains(b'qwerty', b'a');''',
            {False},
        )

        await self.assert_query_result(
            r'''SELECT contains(b'qwerty', b'azerty');''',
            {False},
        )

    async def test_edgeql_functions_contains_05(self):
        await self.assert_query_result(
            r'''
                SELECT contains(
                    array_agg(User),
                    (SELECT User FILTER .name = 'Elvis')
                )
            ''',
            [True],
        )

    async def test_edgeql_functions_find_01(self):
        await self.assert_query_result(
            r'''SELECT find(<str>{}, <str>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT find(<str>{}, 'a');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', <str>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', '');''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', 'q');''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', 'qwe');''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', 'we');''',
            {1},
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', 't');''',
            {4},
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', 'a');''',
            {-1},
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', 'azerty');''',
            {-1},
        )

    async def test_edgeql_functions_find_02(self):
        await self.assert_query_result(
            r'''SELECT find(<bytes>{}, <bytes>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT find(b'qwerty', b'');''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT find(b'qwerty', b'qwe');''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT find(b'qwerty', b'a');''',
            {-1},
        )

    async def test_edgeql_functions_find_03(self):
        await self.assert_query_result(
            r'''SELECT find(<array<str>>{}, <str>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT find(<array<str>>{}, 'the');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT find(['the', 'quick', 'brown', 'fox'], <str>{});''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT find(<array<str>>[], 'the');''',
            {-1},
        )

        await self.assert_query_result(
            r'''SELECT find(['the', 'quick', 'brown', 'fox'], 'the');''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT find(['the', 'quick', 'brown', 'fox'], 'fox');''',
            {3},
        )

        await self.assert_query_result(
            r'''SELECT find(['the', 'quick', 'brown', 'fox'], 'jumps');''',
            {-1},
        )

        await self.assert_query_result(
            r'''
                SELECT find(['the', 'quick', 'brown', 'fox',
                             'jumps', 'over', 'the', 'lazy', 'dog'],
                            'the');
            ''',
            {0},
        )

        await self.assert_query_result(
            r'''
                SELECT find(['the', 'quick', 'brown', 'fox',
                             'jumps', 'over', 'the', 'lazy', 'dog'],
                            'the', 1);
            ''',
            {6},
        )

    async def test_edgeql_functions_str_case_01(self):
        await self.assert_query_result(
            r'''SELECT str_lower({'HeLlO', 'WoRlD!', 'ПриВет', 'мИр'});''',
            {'hello', 'world!', 'привет', 'мир'},
        )

        await self.assert_query_result(
            r'''SELECT str_upper({'HeLlO', 'WoRlD!'});''',
            {'HELLO', 'WORLD!'},
        )

        await self.assert_query_result(
            r'''SELECT str_title({'HeLlO', 'WoRlD!'});''',
            {'Hello', 'World!'},
        )

        await self.assert_query_result(
            r'''SELECT str_lower('HeLlO WoRlD!');''',
            {'hello world!'},
        )

        await self.assert_query_result(
            r'''SELECT str_upper('HeLlO WoRlD!');''',
            {'HELLO WORLD!'},
        )

        await self.assert_query_result(
            r'''SELECT str_title('HeLlO WoRlD!');''',
            {'Hello World!'},
        )

    async def test_edgeql_functions_str_pad_01(self):
        await self.assert_query_result(
            r'''SELECT str_pad_start('Hello', 20);''',
            {'               Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_start('Hello', 20, '>');''',
            {'>>>>>>>>>>>>>>>Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_start('Hello', 20, '-->');''',
            {'-->-->-->-->-->Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_end('Hello', 20);''',
            {'Hello               '},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_end('Hello', 20, '<');''',
            {'Hello<<<<<<<<<<<<<<<'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_end('Hello', 20, '<--');''',
            {'Hello<--<--<--<--<--'},
        )

        # Call deprecated functions, too.
        await self.assert_query_result(
            r'''SELECT str_lpad('Hello', 20);''',
            {'               Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_rpad('Hello', 20);''',
            {'Hello               '},
        )

    async def test_edgeql_functions_str_pad_02(self):
        await self.assert_query_result(
            r'''SELECT str_pad_start('Hello', 2);''',
            {'He'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_start('Hello', 2, '>');''',
            {'He'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_start('Hello', 2, '-->');''',
            {'He'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_end('Hello', 2);''',
            {'He'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_end('Hello', 2, '<');''',
            {'He'},
        )

        await self.assert_query_result(
            r'''SELECT str_pad_end('Hello', 2, '<--');''',
            {'He'},
        )

    async def test_edgeql_functions_str_pad_03(self):
        await self.assert_query_result(
            r'''
                FOR l IN {0, 2, 10, 20}
                SELECT len(str_pad_start('Hello', l)) = l;
            ''',
            [True, True, True, True],
        )

        await self.assert_query_result(
            r'''
                FOR l IN {0, 2, 10, 20}
                SELECT len(str_pad_end('Hello', l)) = l;
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_str_trim_01(self):
        await self.assert_query_result(
            r'''SELECT str_trim('    Hello    ');''',
            {'Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_trim_start('    Hello    ');''',
            {'Hello    '},
        )

        await self.assert_query_result(
            r'''SELECT str_trim_end('    Hello    ');''',
            {'    Hello'},
        )

        # Call deprecated functions, too.
        await self.assert_query_result(
            r'''SELECT str_ltrim('    Hello    ');''',
            {'Hello    '},
        )

        await self.assert_query_result(
            r'''SELECT str_rtrim('    Hello    ');''',
            {'    Hello'},
        )

    async def test_edgeql_functions_str_trim_02(self):
        await self.assert_query_result(
            r'''SELECT str_trim_start('               Hello', ' <->');''',
            {'Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_trim_start('>>>>>>>>>>>>>>>Hello', ' <->');''',
            {'Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_trim_start('-->-->-->-->-->Hello', ' <->');''',
            {'Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_trim_end('Hello               ', ' <->');''',
            {'Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_trim_end('Hello<<<<<<<<<<<<<<<', ' <->');''',
            {'Hello'},
        )

        await self.assert_query_result(
            r'''SELECT str_trim_end('Hello<--<--<--<--<--', ' <->');''',
            {'Hello'},
        )

        await self.assert_query_result(
            r'''
                SELECT str_trim(
                '-->-->-->-->-->Hello<--<--<--<--<--', ' <->');
            ''',
            {'Hello'},
        )

    async def test_edgeql_functions_str_repeat_01(self):
        await self.assert_query_result(
            r'''SELECT str_repeat('', 1);''',
            {''},
        )

        await self.assert_query_result(
            r'''SELECT str_repeat('', 0);''',
            {''},
        )

        await self.assert_query_result(
            r'''SELECT str_repeat('', -1);''',
            {''},
        )

        await self.assert_query_result(
            r'''SELECT str_repeat('a', 1);''',
            {'a'},
        )

        await self.assert_query_result(
            r'''SELECT str_repeat('aa', 3);''',
            {'aaaaaa'},
        )

        await self.assert_query_result(
            r'''SELECT str_repeat('a', 0);''',
            {''},
        )

        await self.assert_query_result(
            r'''SELECT str_repeat('', -1);''',
            {''},
        )

    async def test_edgeql_functions_str_replace_01(self):
        await self.assert_query_result(
            r'''select str_replace('', '', '');''',
            {''},
        )

        await self.assert_query_result(
            r'''select str_replace('', 'a', 'b');''',
            {''},
        )

        await self.assert_query_result(
            r'''select str_replace('', 'a', '');''',
            {''},
        )

        await self.assert_query_result(
            r'''select str_replace('', '', 'b');''',
            {''},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', '', '');''',
            {'hello world'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', 'a', 'b');''',
            {'hello world'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', 'a', '');''',
            {'hello world'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', '', 'b');''',
            {'hello world'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', 'o', '0');''',
            {'hell0 w0rld'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', 'o', 'LETTER_O');''',
            {'hellLETTER_O wLETTER_Orld'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', 'orl', '');''',
            {'hello wd'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', 'orl', '-');''',
            {'hello w-d'},
        )

        await self.assert_query_result(
            r'''select str_replace('hello world', 'orl', '...');''',
            {'hello w...d'},
        )

    async def test_edgeql_functions_str_reverse_01(self):
        await self.assert_query_result(
            r'''select str_reverse('');''',
            {''},
        )

        await self.assert_query_result(
            r'''select str_reverse('a');''',
            {'a'},
        )

        await self.assert_query_result(
            r'''select str_reverse('aa');''',
            {'aa'},
        )

        await self.assert_query_result(
            r'''select str_reverse('hello');''',
            {'olleh'},
        )

    async def test_edgeql_functions_math_abs_01(self):
        await self.assert_query_result(
            r'''SELECT math::abs(2);''',
            {2},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(-2);''',
            {2},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(2.5);''',
            {2.5},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(-2.5);''',
            {2.5},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(<decimal>2.5);''',
            {2.5},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(<decimal>-2.5);''',
            {2.5},
        )

    async def test_edgeql_functions_math_abs_02(self):
        await self.assert_query_result(
            r'''SELECT math::abs(<int16>2) IS int16;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(<int32>2) IS int32;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(<int64>2) IS int64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(<float32>2) IS float32;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(<float64>2) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::abs(<decimal>2) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_ceil_01(self):
        await self.assert_query_result(
            r'''SELECT math::ceil(2);''',
            {2},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(2.5);''',
            {3},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(-2.5);''',
            {-2},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<decimal>2.5);''',
            {3},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<decimal>-2.5);''',
            {-2},
        )

    async def test_edgeql_functions_math_ceil_02(self):
        await self.assert_query_result(
            r'''SELECT math::ceil(<int16>2) IS int64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<int32>2) IS int64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<int64>2) IS int64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<float32>2.5) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<float64>2.5) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<bigint>2) IS bigint;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::ceil(<decimal>2.5) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_floor_01(self):
        await self.assert_query_result(
            r'''SELECT math::floor(2);''',
            {2},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(2.5);''',
            {2},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(-2.5);''',
            {-3},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<decimal>2.5);''',
            {2},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<decimal>-2.5);''',
            {-3},
        )

    async def test_edgeql_functions_math_floor_02(self):
        await self.assert_query_result(
            r'''SELECT math::floor(<int16>2) IS int64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<int32>2) IS int64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<int64>2) IS int64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<float32>2.5) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<float64>2.5) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<bigint>2) IS bigint;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::floor(<decimal>2.5) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_log_01(self):
        await self.assert_query_result(
            r'''SELECT math::ln({1, 10, 32});''',
            {0, 2.30258509299405, 3.46573590279973},
        )

        await self.assert_query_result(
            r'''SELECT math::lg({1, 10, 32});''',
            {0, 1, 1.50514997831991},
        )

        await self.assert_query_result(
            r'''SELECT math::log(<decimal>{1, 10, 32}, base := <decimal>2);''',
            {0, 3.321928094887362, 5},
        )

    async def test_edgeql_functions_math_log_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            ''
        ):
            await self.con.query('SELECT math::ln(-1)')
        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            ''
        ):
            await self.con.query('SELECT math::lg(-1)')
        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            ''
        ):
            await self.con.query('SELECT math::log(-1, base := 10)')

    async def test_edgeql_function_math_sqrt_01(self):
        await self.assert_query_result(
            r'''SELECT math::sqrt({1, 2, 25});''',
            {1, 1.4142135623730951, 5},
        )
        with self.assertRaises(edgedb.errors.InvalidValueError):
            await self.con.query('SELECT math::sqrt(-1)')

    async def test_edgeql_function_math_sqrt_02(self):
        await self.assert_query_result(
            '''SELECT math::sqrt({1.0, 2.0, 25.0});''',
            {1.0, 1.4142135623730951, 5.0},
        )
        with self.assertRaises(edgedb.errors.InvalidValueError):
            await self.con.query('SELECT math::sqrt(-1.0)')

    async def test_edgeql_function_math_sqrt_03(self):
        await self.assert_query_result(
            '''SELECT math::sqrt({1n, 2n, 25n});''',
            {1, 1.4142135623730951, 5},
        )
        with self.assertRaises(edgedb.errors.InvalidValueError):
            await self.con.query('SELECT math::sqrt(-1n)')

    async def test_edgeql_functions_math_mean_01(self):
        await self.assert_query_result(
            r'''SELECT math::mean(1);''',
            {1.0},
        )

        await self.assert_query_result(
            r'''SELECT math::mean(1.5);''',
            {1.5},
        )

        await self.assert_query_result(
            r'''SELECT math::mean({1, 2, 3});''',
            {2.0},
        )

        await self.assert_query_result(
            r'''SELECT math::mean({1, 2, 3, 4});''',
            {2.5},
        )

        await self.assert_query_result(
            r'''SELECT math::mean({0.1, 0.2, 0.3});''',
            {0.2},
        )

        await self.assert_query_result(
            r'''SELECT math::mean({0.1, 0.2, 0.3, 0.4});''',
            {0.25},
        )

    async def test_edgeql_functions_math_mean_02(self):
        # int16 is implicitly cast in float32, which produces a
        # float64 result
        await self.assert_query_result(
            r'''SELECT math::mean(<int16>2) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::mean(<int32>2) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::mean(<int64>2) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::mean(<float32>2) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::mean(<float64>2) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::mean(<decimal>2) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_mean_03(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    A := {1, 3, 1}
                # the difference between sum and mean * count is due to
                # rounding errors, but it should be small
                SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
            ''',
            {True},
        )

    async def test_edgeql_functions_math_mean_04(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    A := <float64>{1, 3, 1}
                # the difference between sum and mean * count is due to
                # rounding errors, but it should be small
                SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
            ''',
            {True},
        )

    async def test_edgeql_functions_math_mean_05(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    A := len(default::Named.name)
                # the difference between sum and mean * count is due to
                # rounding errors, but it should be small
                SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
            ''',
            {True},
        )

    async def test_edgeql_functions_math_mean_06(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    A := <float64>len(default::Named.name)
                # the difference between sum and mean * count is due to
                # rounding errors, but it should be small
                SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
            ''',
            {True},
        )

    async def test_edgeql_functions_math_mean_07(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    A := {3}
                SELECT mean(A) * count(A);
            ''',
            {3},
        )

    async def test_edgeql_functions_math_mean_08(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {1, 2, 3, 4}
                SELECT mean(X) = sum(X) / count(X);
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {0.1, 0.2, 0.3, 0.4}
                SELECT mean(X) = sum(X) / count(X);
            ''',
            {True},
        )

    async def test_edgeql_functions_math_mean_09(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid input to mean\(\): "
                r"not enough elements in input set"):
            await self.con.query(r'''
                SELECT math::mean(<int64>{});
            ''')

    async def test_edgeql_functions_math_stddev_01(self):
        await self.assert_query_result(
            r'''SELECT math::stddev({1, 1});''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev({1, 1, -1, 1});''',
            {1.0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev({1, 2, 3});''',
            {1.0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev({0.1, 0.1, -0.1, 0.1});''',
            {0.1},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev(<decimal>{0.1, 0.2, 0.3});''',
            {0.1},
        )

    async def test_edgeql_functions_math_stddev_02(self):
        await self.assert_query_result(
            r'''SELECT math::stddev(<int16>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev(<int32>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev(<int64>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev(<float32>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev(<float64>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev(<decimal>{1, 1}) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_stddev_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid input to stddev\(\): not enough "
                r"elements in input set"):
            await self.con.query(r'''
                SELECT math::stddev(<int64>{});
            ''')

    async def test_edgeql_functions_math_stddev_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid input to stddev\(\): not enough "
                r"elements in input set"):
            await self.con.query(r'''
                SELECT math::stddev(1);
            ''')

    async def test_edgeql_functions_math_stddev_pop_01(self):
        await self.assert_query_result(
            r'''SELECT math::stddev_pop(1);''',
            {0.0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop({1, 1, 1});''',
            {0.0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop({1, 2, 1, 2});''',
            {0.5},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop({0.1, 0.1, 0.1});''',
            {0.0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop({0.1, 0.2, 0.1, 0.2});''',
            {0.05},
        )

    async def test_edgeql_functions_math_stddev_pop_02(self):
        await self.assert_query_result(
            r'''SELECT math::stddev_pop(<int16>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop(<int32>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop(<int64>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop(<float32>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop(<float64>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop(<decimal>1) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_stddev_pop_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid input to stddev_pop\(\): not enough "
                r"elements in input set"):
            await self.con.query(r'''
                SELECT math::stddev_pop(<int64>{});
            ''')

    async def test_edgeql_functions_math_var_01(self):
        await self.assert_query_result(
            r'''SELECT math::var({1, 1});''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT math::var({1, 1, -1, 1});''',
            {1.0},
        )

        await self.assert_query_result(
            r'''SELECT math::var({1, 2, 3});''',
            {1.0},
        )

        await self.assert_query_result(
            r'''SELECT math::var({0.1, 0.1, -0.1, 0.1});''',
            {0.01},
        )

        await self.assert_query_result(
            r'''SELECT math::var(<decimal>{0.1, 0.2, 0.3});''',
            {0.01},
        )

    async def test_edgeql_functions_math_var_02(self):
        # int16 is implicitly cast in float32, which produces a
        # float64 result
        await self.assert_query_result(
            r'''SELECT math::var(<int16>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var(<int32>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var(<int64>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var(<float32>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var(<float64>{1, 1}) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var(<decimal>{1, 1}) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_var_03(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {1, 1}
                SELECT var(X) = stddev(X) ^ 2;
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {1, 1, -1, 1}
                SELECT var(X) = stddev(X) ^ 2;
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {1, 2, 3}
                SELECT var(X) = stddev(X) ^ 2;
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {0.1, 0.1, -0.1, 0.1}
                SELECT var(X) = stddev(X) ^ 2;
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := <decimal>{0.1, 0.2, 0.3}
                SELECT var(X) = stddev(X) ^ 2;
            ''',
            {True},
        )

    async def test_edgeql_functions_math_var_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid input to var\(\): not enough "
                r"elements in input set"):
            await self.con.query(r'''
                SELECT math::var(<int64>{});
            ''')

    async def test_edgeql_functions_math_var_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid input to var\(\): not enough "
                r"elements in input set"):
            await self.con.query(r'''
                SELECT math::var(1);
            ''')

    async def test_edgeql_functions_math_var_pop_01(self):
        await self.assert_query_result(
            r'''SELECT math::var_pop(1);''',
            {0.0},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop({1, 1, 1});''',
            {0.0},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop({1, 2, 1, 2});''',
            {0.25},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop({0.1, 0.1, 0.1});''',
            {0.0},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop({0.1, 0.2, 0.1, 0.2});''',
            {0.0025},
        )

    async def test_edgeql_functions_math_var_pop_02(self):
        await self.assert_query_result(
            r'''SELECT math::var_pop(<int16>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop(<int32>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop(<int64>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop(<float32>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop(<float64>1) IS float64;''',
            {True},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop(<decimal>1) IS decimal;''',
            {True},
        )

    async def test_edgeql_functions_math_var_pop_03(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {1, 2, 1, 2}
                SELECT abs(var_pop(X) - stddev_pop(X) ^ 2) < 1.0e-15;
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {0.1, 0.2, 0.1, 0.2}
                SELECT abs(var_pop(X) - stddev_pop(X) ^ 2) < 1.0e-15;
            ''',
            {True},
        )

    async def test_edgeql_functions_math_var_pop_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid input to var_pop\(\): not enough "
                r"elements in input set"):
            await self.con.query(r'''
                SELECT math::var_pop(<int64>{});
            ''')

    async def test_edgeql_functions_math_pi_01(self):
        await self.assert_query_result(
            r'''SELECT math::pi() IS float64;''',
            {True},
        )
        await self.assert_query_result(
            r'''SELECT math::pi();''',
            {math.pi},
            abs_tol=0.0000000005,
        )

    async def test_edgeql_functions_math_acos_01(self):
        await self.assert_query_result(
            r'''SELECT math::acos(-1);''',
            {math.pi},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::acos(-math::sqrt(2) / 2);''',
            {math.pi * 3 / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::acos(-0.0);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::acos(0.0);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::acos(math::sqrt(2) / 2);''',
            {math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''WITH x := math::acos(1) SELECT (x, <str>x);''',
            [(0.0, '0')],
        )

        await self.assert_query_result(
            r'''SELECT <str>math::acos(<float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_acos_02(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::acos(-1.001);
            ''')

    async def test_edgeql_functions_math_acos_03(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::acos(1.001);
            ''')

    async def test_edgeql_functions_math_acos_04(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::acos(<float64>"-inf");
            ''')

    async def test_edgeql_functions_math_acos_05(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::acos(<float64>"inf");
            ''')

    async def test_edgeql_functions_math_asin_01(self):
        await self.assert_query_result(
            r'''SELECT math::asin(-1);''',
            {-math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::asin(-math::sqrt(2) / 2);''',
            {-math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''WITH x := math::asin(-0.0) SELECT (x, <str>x);''',
            [[-0.0, '-0']],
        )
        await self.assert_query_result(
            r'''WITH x := math::asin(0.0) SELECT (x, <str>x);''',
            [[0.0, '0']],
        )
        await self.assert_query_result(
            r'''SELECT math::asin(math::sqrt(2) / 2);''',
            {math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::asin(1);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT <str>math::asin(<float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_asin_02(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::asin(-1.001);
            ''')

    async def test_edgeql_functions_math_asin_03(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::asin(1.001);
            ''')

    async def test_edgeql_functions_math_asin_04(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::asin(<float64>"-inf");
            ''')

    async def test_edgeql_functions_math_asin_05(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::asin(<float64>"inf");
            ''')

    async def test_edgeql_functions_math_atan_01(self):
        await self.assert_query_result(
            r'''SELECT math::atan(<float64>"-inf");''',
            {-math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(-1000000000);''',
            {-math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(-math::sqrt(3));''',
            {-math.pi / 3},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(-1);''',
            {-math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(-1 / math::sqrt(3));''',
            {-math.pi / 6},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''WITH x := math::atan(-0.0) SELECT (x, <str>x);''',
            [[-0.0, '-0']],
        )
        await self.assert_query_result(
            r'''WITH x := math::atan(0.0) SELECT (x, <str>x);''',
            [[0.0, '0']],
        )
        await self.assert_query_result(
            r'''SELECT math::atan(1 / math::sqrt(3));''',
            {math.pi / 6},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(1);''',
            {math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(math::sqrt(3));''',
            {math.pi / 3},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(1000000000);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan(<float64>"inf");''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT <str>math::atan(<float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_atan2_01(self):
        await self.assert_query_result(
            r'''SELECT math::atan2(-0.0, -1);''',
            {-math.pi},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-2, -2);''',
            {-math.pi * 3 / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-3, -0.0);''',
            {-math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-4, 0.0);''',
            {-math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-5, 5);''',
            {-math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''WITH x := math::atan2(-0.0, 6) SELECT (x, <str>x);''',
            [[-0.0, '-0']],
        )
        await self.assert_query_result(
            r'''WITH x := math::atan2(0.0, 6) SELECT (x, <str>x);''',
            [[0.0, '0']],
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(8, 8);''',
            {math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(9, 0.0);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(10, -0.0);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(11, -11);''',
            {math.pi * 3 / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(0.0, -12);''',
            {math.pi},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT math::atan2(-0.0, -0.0);''',
            {-math.pi},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''WITH x := math::atan2(-0.0, 0.0) SELECT (x, <str>x);''',
            [[-0.0, '-0']],
        )
        await self.assert_query_result(
            r'''WITH x := math::atan2(0.0, 0.0) SELECT (x, <str>x);''',
            [[0.0, '0']],
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(0.0, -0.0);''',
            {math.pi},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT math::atan2(-0.0, -<float64>"inf");''',
            {-math.pi},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-<float64>"inf", -<float64>"inf");''',
            {-math.pi * 3 / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-<float64>"inf", -0.0);''',
            {-math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-<float64>"inf", 0.0);''',
            {-math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(-<float64>"inf", <float64>"inf");''',
            {-math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''
            WITH x := math::atan2(-0.0, <float64>"inf")
            SELECT (x, <str>x);
            ''',
            [[-0.0, '-0']],
        )
        await self.assert_query_result(
            r'''
            WITH x := math::atan2(0.0, <float64>"inf")
            SELECT (x, <str>x);
            ''',
            [[0.0, '0']],
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(<float64>"inf", <float64>"inf");''',
            {math.pi / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(<float64>"inf", 0.0);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(<float64>"inf", -0.0);''',
            {math.pi / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(<float64>"inf", -<float64>"inf");''',
            {math.pi * 3 / 4},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::atan2(0.0, -<float64>"inf");''',
            {math.pi},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT <str>math::atan2(<float64>"NaN", 1);''',
            {"NaN"},
        )
        await self.assert_query_result(
            r'''SELECT <str>math::atan2(1, <float64>"NaN");''',
            {"NaN"},
        )
        await self.assert_query_result(
            r'''SELECT <str>math::atan2(<float64>"NaN", <float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_cos_01(self):
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi() * 2);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi() * 7 / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi() * 3 / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi() * 5 / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi());''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi() * 3 / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi() / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-math::pi() / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(-0.0);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(0.0);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi() / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi() / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi() * 3 / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi());''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi() * 5 / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi() * 3 / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi() * 7 / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cos(math::pi() * 2);''',
            {1.0},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT <str>math::cos(<float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_cos_02(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::cos(<float64>"-inf");
            ''')

    async def test_edgeql_functions_math_cos_03(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::cos(<float64>"inf");
            ''')

    async def test_edgeql_functions_math_cot_01(self):
        await self.assert_query_result(
            r'''SELECT math::cot(-math::pi() * 7 / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(-math::pi() * 3 / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(-math::pi() * 5 / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(-math::pi() * 3 / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(-math::pi() / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(-math::pi() / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT <str>math::cot(-0.0);''',
            {"-Infinity"},
        )
        await self.assert_query_result(
            r'''SELECT <str>math::cot(0.0);''',
            {"Infinity"},
        )
        await self.assert_query_result(
            r'''SELECT math::cot(math::pi() / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(math::pi() / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(math::pi() * 3 / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(math::pi() * 5 / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(math::pi() * 3 / 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::cot(math::pi() * 7 / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT <str>math::cot(<float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_cot_02(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::cot(<float64>"-inf");
            ''')

    async def test_edgeql_functions_math_cot_03(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::cot(<float64>"inf");
            ''')

    async def test_edgeql_functions_math_sin_01(self):
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi() * 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi() * 7 / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi() * 3 / 2);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi() * 5 / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi());''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi() * 3 / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi() / 2);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(-math::pi() / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''WITH x := math::sin(-0.0) SELECT (x, <str>x);''',
            [[-0.0, '-0']],
        )
        await self.assert_query_result(
            r'''WITH x := math::sin(0.0) SELECT (x, <str>x);''',
            [[0.0, '0']],
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi() / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi() / 2);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi() * 3 / 4);''',
            {math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi());''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi() * 5 / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi() * 3 / 2);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi() * 7 / 4);''',
            {-math.sqrt(2) / 2},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::sin(math::pi() * 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT <str>math::sin(<float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_sin_02(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::sin(<float64>"-inf");
            ''')

    async def test_edgeql_functions_math_sin_03(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::sin(<float64>"inf");
            ''')

    async def test_edgeql_functions_math_tan_01(self):
        await self.assert_query_result(
            r'''SELECT math::tan(-math::pi() * 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(-math::pi() * 7 / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(-math::pi() * 5 / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(-math::pi());''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(-math::pi() * 3 / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(-math::pi() / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''WITH x := math::tan(-0.0) SELECT (x, <str>x);''',
            [[-0.0, '-0']],
        )
        await self.assert_query_result(
            r'''WITH x := math::tan(0.0) SELECT (x, <str>x);''',
            [[0.0, '0']],
        )
        await self.assert_query_result(
            r'''SELECT math::tan(math::pi() / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(math::pi() * 3 / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(math::pi());''',
            {0.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(math::pi() * 5 / 4);''',
            {1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(math::pi() * 7 / 4);''',
            {-1.0},
            abs_tol=0.0000000005,
        )
        await self.assert_query_result(
            r'''SELECT math::tan(math::pi() * 2);''',
            {0.0},
            abs_tol=0.0000000005,
        )

        await self.assert_query_result(
            r'''SELECT <str>math::tan(<float64>"NaN");''',
            {"NaN"},
        )

    async def test_edgeql_functions_math_tan_02(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::tan(<float64>"-inf");
            ''')

    async def test_edgeql_functions_math_tan_03(self):
        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError,
                r"input is out of range"):
            await self.con.query(r'''
                SELECT math::tan(<float64>"inf");
            ''')

    async def test_edgeql_functions__genseries_01(self):
        await self.assert_query_result(
            r'''
            SELECT _gen_series(1, 10)
            ''',
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        )

        await self.assert_query_result(
            r'''
            SELECT _gen_series(1, 10, 2)
            ''',
            [1, 3, 5, 7, 9]
        )

        await self.assert_query_result(
            r'''
            SELECT _gen_series(1n, 10n)
            ''',
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        )

        await self.assert_query_result(
            r'''
            SELECT _gen_series(1n, 10n, 2n)
            ''',
            [1, 3, 5, 7, 9]
        )

    async def test_edgeql_functions_sequence_next_reset(self):
        await self.con.execute('''
            CREATE SCALAR TYPE my_seq_01 EXTENDING std::sequence;
        ''')

        result = await self.con.query_single('''
            SELECT sequence_next(INTROSPECT my_seq_01)
        ''')

        self.assertEqual(result, 1)

        result = await self.con.query_single('''
            SELECT sequence_next(INTROSPECT my_seq_01)
        ''')

        self.assertEqual(result, 2)

        await self.con.execute('''
            SELECT sequence_reset(INTROSPECT my_seq_01)
        ''')

        result = await self.con.query_single('''
            SELECT sequence_next(INTROSPECT my_seq_01)
        ''')

        self.assertEqual(result, 1)

        await self.con.execute('''
            SELECT sequence_reset(INTROSPECT my_seq_01, 20)
        ''')

        result = await self.con.query_single('''
            SELECT sequence_next(INTROSPECT my_seq_01)
        ''')

        self.assertEqual(result, 21)

    async def test_edgeql_functions__datetime_range_buckets(self):
        await self.assert_query_result(
            '''
            SELECT <tuple<str, str>>std::_datetime_range_buckets(
                <datetime>'2021-01-01T00:00:00Z',
                <datetime>'2021-04-01T00:00:00Z',
                '1 month');
            ''',
            [
                ('2021-01-01T00:00:00+00:00', '2021-02-01T00:00:00+00:00'),
                ('2021-02-01T00:00:00+00:00', '2021-03-01T00:00:00+00:00'),
                ('2021-03-01T00:00:00+00:00', '2021-04-01T00:00:00+00:00'),
            ],
        )

        await self.assert_query_result(
            '''
            SELECT <tuple<str, str>>std::_datetime_range_buckets(
                <datetime>'2021-04-01T00:00:00Z',
                <datetime>'2021-04-01T00:00:00Z',
                '1 month');
            ''',
            [],
        )

        await self.assert_query_result(
            '''
            SELECT <tuple<str, str>>std::_datetime_range_buckets(
                <datetime>'2021-01-01T00:00:00Z',
                <datetime>'2021-04-01T00:00:00Z',
                '1.5 months');
            ''',
            [
                ('2021-01-01T00:00:00+00:00', '2021-02-16T00:00:00+00:00'),
                ('2021-02-16T00:00:00+00:00', '2021-03-31T00:00:00+00:00'),
            ],
        )

    async def test_edgeql_functions_bitwise_01(self):
        await self.assert_query_result(
            r'''select bit_and(<int16>6, <int16>12);''',
            {4},
        )
        await self.assert_query_result(
            r'''select bit_and(<int32>6, <int32>12);''',
            {4},
        )
        await self.assert_query_result(
            r'''select bit_and(<int64>6, <int64>12);''',
            {4},
        )

    async def test_edgeql_functions_bitwise_02(self):
        await self.assert_query_result(
            r'''select bit_or(<int16>6, <int16>12);''',
            {14},
        )
        await self.assert_query_result(
            r'''select bit_or(<int32>6, <int32>12);''',
            {14},
        )
        await self.assert_query_result(
            r'''select bit_or(<int64>6, <int64>12);''',
            {14},
        )

    async def test_edgeql_functions_bitwise_03(self):
        await self.assert_query_result(
            r'''select bit_xor(<int16>6, <int16>12);''',
            {10},
        )
        await self.assert_query_result(
            r'''select bit_xor(<int32>6, <int32>12);''',
            {10},
        )
        await self.assert_query_result(
            r'''select bit_xor(<int64>6, <int64>12);''',
            {10},
        )

    async def test_edgeql_functions_bitwise_04(self):
        # 1111111111111111 corresponds to -1 int16
        # Generally the result of logical NOT is expected to be:
        #   NOT a = (-a - 1)
        # Because the number and its bitwise negation must add up to -1.
        await self.assert_query_result(
            r'''select bit_not(<int16>123);''',
            {-124},
        )
        await self.assert_query_result(
            r'''select bit_not(<int32>123);''',
            {-124},
        )
        await self.assert_query_result(
            r'''select bit_not(<int64>123);''',
            {-124},
        )

    async def test_edgeql_functions_bitwise_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"bit_lshift.*: cannot shift by negative amount"):
            async with self.con.transaction():
                await self.con.query(r'''
                    select bit_lshift(<int16>5, -2);
                ''')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"bit_lshift.*: cannot shift by negative amount"):
            async with self.con.transaction():
                await self.con.query(r'''
                    select bit_lshift(<int32>5, -2);
                ''')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"bit_lshift.*: cannot shift by negative amount"):
            async with self.con.transaction():
                await self.con.query(r'''
                    select bit_lshift(<int64>5, -2);
                ''')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"bit_rshift.*: cannot shift by negative amount"):
            async with self.con.transaction():
                await self.con.query(r'''
                    select bit_rshift(<int16>5, -2);
                ''')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"bit_rshift.*: cannot shift by negative amount"):
            async with self.con.transaction():
                await self.con.query(r'''
                    select bit_rshift(<int32>5, -2);
                ''')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"bit_rshift.*: cannot shift by negative amount"):
            async with self.con.transaction():
                await self.con.query(r'''
                    select bit_rshift(<int64>5, -2);
                ''')

    async def test_edgeql_functions_bitwise_06(self):
        await self.assert_query_result(
            r'''select bit_lshift(<int16>5, 2);''',
            {20},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int16>32767, 15);''',
            {-32768},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int16>32767, 16);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int16>32767, 32);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int16>32767, 40);''',
            {0},
        )

        # Left shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int16>1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_lshift(bit_lshift(val, X.0), X.1) =
                   bit_lshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_07(self):
        await self.assert_query_result(
            r'''select bit_lshift(<int32>5, 2);''',
            {20},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int32>2147483647, 31);''',
            {-2147483648},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int32>2147483647, 32);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int32>2147483647, 40);''',
            {0},
        )

        # Left shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int32>1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_lshift(bit_lshift(val, X.0), X.1) =
                   bit_lshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_08(self):
        await self.assert_query_result(
            r'''select bit_lshift(<int64>5, 2);''',
            {20},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int64>9223372036854775807, 31);''',
            {-2147483648},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int64>9223372036854775807, 63);''',
            {-9223372036854775808},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int64>9223372036854775807, 64);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_lshift(<int64>9223372036854775807, 100);''',
            {0},
        )

        # Left shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int64>1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_lshift(bit_lshift(val, X.0), X.1) =
                   bit_lshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_09(self):
        # Right shift uses "arithemtic bitshift", which preserves the sign
        # bit. This means that right shifting works different for positive and
        # negative values.
        #
        # Positive value right bitshift tests.
        await self.assert_query_result(
            r'''select bit_rshift(<int16>123, 2);''',
            {30},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>32767, 14);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>32767, 15);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>32767, 16);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>32767, 32);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>32767, 40);''',
            {0},
        )

        # Right shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int16>1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_rshift(bit_rshift(val, X.0), X.1) =
                   bit_rshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_10(self):
        # Right shift uses "arithemtic bitshift", which preserves the sign
        # bit. This means that right shifting works different for positive and
        # negative values.
        #
        # Positive value right bitshift tests.
        await self.assert_query_result(
            r'''select bit_rshift(<int32>123, 2);''',
            {30},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>2147483647, 30);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>2147483647, 31);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>2147483647, 32);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>2147483647, 40);''',
            {0},
        )

        # Right shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int32>1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_rshift(bit_rshift(val, X.0), X.1) =
                   bit_rshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_11(self):
        # Right shift uses "arithemtic bitshift", which preserves the sign
        # bit. This means that right shifting works different for positive and
        # negative values.
        #
        # Positive value right bitshift tests.
        await self.assert_query_result(
            r'''select bit_rshift(<int64>123, 2);''',
            {30},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>9223372036854775807, 62);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>9223372036854775807, 63);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>9223372036854775807, 64);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>9223372036854775807, 90);''',
            {0},
        )

        # Right shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int64>1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_rshift(bit_rshift(val, X.0), X.1) =
                   bit_rshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_12(self):
        # Right shift uses "arithemtic bitshift", which preserves the sign
        # bit. This means that right shifting works different for positive and
        # negative values.
        #
        # Negative value right bitshift tests.
        await self.assert_query_result(
            r'''select bit_rshift(<int16>-123, 2);''',
            {-31},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>-32768, 14);''',
            {-2},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>-32768, 15);''',
            {-1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>-32768, 16);''',
            {-1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>-32768, 32);''',
            {-1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int16>-32768, 40);''',
            {-1},
        )

        # Right shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int16>-1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_rshift(bit_rshift(val, X.0), X.1) =
                   bit_rshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_13(self):
        # Right shift uses "arithemtic bitshift", which preserves the sign
        # bit. This means that right shifting works different for positive and
        # negative values.
        #
        # Negative value right bitshift tests.
        await self.assert_query_result(
            r'''select bit_rshift(<int32>-123, 2);''',
            {-31},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>-2147483648, 30);''',
            {-2},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>-2147483648, 31);''',
            {-1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>-2147483648, 32);''',
            {-1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int32>-2147483648, 40);''',
            {-1},
        )

        # Right shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int32>-1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_rshift(bit_rshift(val, X.0), X.1) =
                   bit_rshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_14(self):
        # Right shift uses "arithemtic bitshift", which preserves the sign
        # bit. This means that right shifting works different for positive and
        # negative values.
        #
        # Negative value right bitshift tests.
        await self.assert_query_result(
            r'''select bit_rshift(<int64>-123, 2);''',
            {-31},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>-9223372036854775808, 62);''',
            {-2},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>-9223372036854775808, 63);''',
            {-1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>-9223372036854775808, 64);''',
            {-1},
        )
        await self.assert_query_result(
            r'''select bit_rshift(<int64>-9223372036854775808, 90);''',
            {-1},
        )

        # Right shift by A bits and then B bits, should be same as bitshifting
        # by A + B bits.
        await self.assert_query_result(
            r'''
            with
                val := <int64>-1234,
            for X in {(2, 2), (10, 10), (20, 20), (40, 40)}
            select bit_rshift(bit_rshift(val, X.0), X.1) =
                   bit_rshift(val, X.0 + X.1);
            ''',
            [True, True, True, True],
        )

    async def test_edgeql_functions_bitwise_15(self):
        # bit_count counts the number of bits

        # bit_count(0)
        await self.assert_query_result(
            r'''select bit_count(<int16>0);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_count(<int32>0);''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_count(<int64>0);''',
            {0},
        )

        # bit_count(1)
        await self.assert_query_result(
            r'''select bit_count(<int16>1);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_count(<int32>1);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_count(<int64>1);''',
            {1},
        )

        # bit_count(255)
        await self.assert_query_result(
            r'''select bit_count(<int16>255);''',
            {8},
        )
        await self.assert_query_result(
            r'''select bit_count(<int32>255);''',
            {8},
        )
        await self.assert_query_result(
            r'''select bit_count(<int64>255);''',
            {8},
        )

        # bit_count(256)
        await self.assert_query_result(
            r'''select bit_count(<int16>256);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_count(<int32>256);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_count(<int64>256);''',
            {1},
        )

        # bit_count(max)
        await self.assert_query_result(
            r'''select bit_count(<int16>32767);''',
            {15},
        )
        await self.assert_query_result(
            r'''select bit_count(<int32>2147483647);''',
            {31},
        )
        await self.assert_query_result(
            r'''select bit_count(<int64>9223372036854775807);''',
            {63},
        )

        # bit_count(min)
        await self.assert_query_result(
            r'''select bit_count(<int16>-32768);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_count(<int32>-2147483648);''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_count(<int64>-9223372036854775808);''',
            {1},
        )

        # bit_count(-1)
        await self.assert_query_result(
            r'''select bit_count(<int16>-1);''',
            {16},
        )
        await self.assert_query_result(
            r'''select bit_count(<int32>-1);''',
            {32},
        )
        await self.assert_query_result(
            r'''select bit_count(<int64>-1);''',
            {64},
        )

        # bit_count(bytes)
        await self.assert_query_result(
            r'''select bit_count(b'');''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_count(b'\x00');''',
            {0},
        )
        await self.assert_query_result(
            r'''select bit_count(b'\x01');''',
            {1},
        )
        await self.assert_query_result(
            r'''select bit_count(b'\xff');''',
            {8},
        )
        await self.assert_query_result(
            r'''select bit_count(b'\x01\x01');''',
            {2},
        )
        await self.assert_query_result(
            r'''select bit_count(b'\xff\xff');''',
            {16},
        )
        await self.assert_query_result(
            r'''select bit_count(b'\x01\x01\x01\x01');''',
            {4},
        )
        await self.assert_query_result(
            r'''select bit_count(b'\xff\xff\xff\xff');''',
            {32},
        )

    async def test_edgeql_functions_range_contains_01(self):
        # Test `contains` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select contains(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>2, <{st}>4));''',
                [True],
            )

            await self.assert_query_result(
                f'''select contains(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>2, <{st}>7));''',
                [False],
            )

            await self.assert_query_result(
                f'''select contains(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>-2, <{st}>4));''',
                [False],
            )

            await self.assert_query_result(
                f'''select contains(
                        range(<{st}>1),
                        range(<{st}>2, <{st}>7));''',
                [True],
            )

            await self.assert_query_result(
                f'''select contains(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>2));''',
                [False],
            )

    async def test_edgeql_functions_range_contains_02(self):
        # Test `contains` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select contains(range(<{st}>1, <{st}>5), <{st}>2);''',
                [True],
            )

            await self.assert_query_result(
                f'''select contains(range(<{st}>1, <{st}>5), <{st}>5);''',
                [False],
            )

            await self.assert_query_result(
                f'''select contains(range(<{st}>1, <{st}>5), <{st}>15);''',
                [False],
            )

            await self.assert_query_result(
                f'''select contains(range(<{st}>1), <{st}>15);''',
                [True],
            )

            await self.assert_query_result(
                f'''select contains(range(<{st}>1), <{st}>0);''',
                [False],
            )

    async def test_edgeql_functions_range_contains_03(self):
        # Test `contains` for datetime ranges.
        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                    range(<datetime>'2022-06-02T00:00:00Z',
                          <datetime>'2022-06-04T00:00:00Z'));''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                    range(<datetime>'2022-06-02T00:00:00Z',
                          <datetime>'2022-06-07T00:00:00Z'));''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                    range(<datetime>'2022-05-29T00:00:00Z',
                          <datetime>'2022-06-04T00:00:00Z'));''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z'),
                    range(<datetime>'2022-06-02T00:00:00Z',
                          <datetime>'2022-06-07T00:00:00Z'));''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                    range(<datetime>'2022-06-02T00:00:00Z'));''',
            [False],
        )

    async def test_edgeql_functions_range_contains_04(self):
        # Test `contains` for datetime ranges.
        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                    <datetime>'2022-06-02T00:00:00Z');''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                    <datetime>'2022-06-05T00:00:00Z');''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                    <datetime>'2022-06-15T00:00:00Z');''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z'),
                    <datetime>'2022-06-15T00:00:00Z');''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<datetime>'2022-06-01T00:00:00Z'),
                    <datetime>'2022-05-31T23:59:59Z');''',
            [False],
        )

    async def test_edgeql_functions_range_contains_05(self):
        # Test `contains` for local_datetime ranges.
        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                    range(<cal::local_datetime>'2022-06-02T00:00:00',
                          <cal::local_datetime>'2022-06-04T00:00:00'));''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                    range(<cal::local_datetime>'2022-06-02T00:00:00',
                          <cal::local_datetime>'2022-06-07T00:00:00'));''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                    range(<cal::local_datetime>'2022-05-29T00:00:00',
                          <cal::local_datetime>'2022-06-04T00:00:00'));''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00'),
                    range(<cal::local_datetime>'2022-06-02T00:00:00',
                          <cal::local_datetime>'2022-06-07T00:00:00'));''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                    range(<cal::local_datetime>'2022-06-02T00:00:00'));''',
            [False],
        )

    async def test_edgeql_functions_range_contains_06(self):
        # Test `contains` for local_datetime ranges.
        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                    <cal::local_datetime>'2022-06-02T00:00:00');''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                    <cal::local_datetime>'2022-06-05T00:00:00');''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                    <cal::local_datetime>'2022-06-15T00:00:00');''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00'),
                    <cal::local_datetime>'2022-06-15T00:00:00');''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_datetime>'2022-06-01T00:00:00'),
                    <cal::local_datetime>'2022-05-31T23:59:59');''',
            [False],
        )

    async def test_edgeql_functions_range_contains_07(self):
        # Test `contains` for local_date ranges.
        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                    range(<cal::local_date>'2022-06-02',
                          <cal::local_date>'2022-06-04'));''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                    range(<cal::local_date>'2022-06-02',
                          <cal::local_date>'2022-06-07'));''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                    range(<cal::local_date>'2022-05-29',
                          <cal::local_date>'2022-06-04'));''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01'),
                    range(<cal::local_date>'2022-06-02',
                          <cal::local_date>'2022-06-07'));''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                    range(<cal::local_date>'2022-06-02'));''',
            [False],
        )

    async def test_edgeql_functions_range_contains_08(self):
        # Test `contains` for local_date ranges.
        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                    <cal::local_date>'2022-06-02');''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                    <cal::local_date>'2022-06-05');''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                    <cal::local_date>'2022-06-15');''',
            [False],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01'),
                    <cal::local_date>'2022-06-15');''',
            [True],
        )

        await self.assert_query_result(
            r'''select contains(
                    range(<cal::local_date>'2022-06-01'),
                    <cal::local_date>'2022-05-31');''',
            [False],
        )

    async def test_edgeql_functions_range_contains_09(self):
        # Test `contains` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select contains(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        multirange([
                            range(<{st}>1, <{st}>2),
                            range(<{st}>8, <{st}>10),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select contains(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        range(<{st}>8),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select contains(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        <{st}>3,
                    )
                ''',
                [True],
            )

    async def test_edgeql_functions_range_contains_10(self):
        # Test `contains` for datetime multiranges.
        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-10T00:00:00Z'),
                        range(<datetime>'2022-06-12T00:00:00Z',
                              <datetime>'2022-06-17T00:00:00Z'),
                        range(<datetime>'2022-06-20T00:00:00Z'),
                    ]),
                    multirange([
                        range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-05T00:00:00Z'),
                        range(<datetime>'2022-06-21T00:00:00Z',
                              <datetime>'2022-06-22T00:00:00Z'),
                    ]),
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-10T00:00:00Z'),
                        range(<datetime>'2022-06-12T00:00:00Z',
                              <datetime>'2022-06-17T00:00:00Z'),
                        range(<datetime>'2022-06-20T00:00:00Z'),
                    ]),
                    range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-05T00:00:00Z'),
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-10T00:00:00Z'),
                        range(<datetime>'2022-06-12T00:00:00Z',
                              <datetime>'2022-06-17T00:00:00Z'),
                        range(<datetime>'2022-06-20T00:00:00Z'),
                    ]),
                    <datetime>'2022-06-05T00:00:00Z',
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-10T00:00:00'),
                        range(<cal::local_datetime>'2022-06-12T00:00:00',
                              <cal::local_datetime>'2022-06-17T00:00:00'),
                        range(<cal::local_datetime>'2022-06-20T00:00:00'),
                    ]),
                    multirange([
                        range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-05T00:00:00'),
                        range(<cal::local_datetime>'2022-06-21T00:00:00',
                              <cal::local_datetime>'2022-06-22T00:00:00')
                    ]),
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-10T00:00:00'),
                        range(<cal::local_datetime>'2022-06-12T00:00:00',
                              <cal::local_datetime>'2022-06-17T00:00:00'),
                        range(<cal::local_datetime>'2022-06-20T00:00:00'),
                    ]),
                    range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-05T00:00:00'),
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-10T00:00:00'),
                        range(<cal::local_datetime>'2022-06-12T00:00:00',
                              <cal::local_datetime>'2022-06-17T00:00:00'),
                        range(<cal::local_datetime>'2022-06-20T00:00:00'),
                    ]),
                    <cal::local_datetime>'2022-06-05T00:00:00',
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-10'),
                        range(<cal::local_date>'2022-06-12',
                              <cal::local_date>'2022-06-17'),
                        range(<cal::local_date>'2022-06-20'),
                    ]),
                    multirange([
                        range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-05'),
                        range(<cal::local_date>'2022-06-21',
                              <cal::local_date>'2022-06-22')
                    ]),
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-10'),
                        range(<cal::local_date>'2022-06-12',
                              <cal::local_date>'2022-06-17'),
                        range(<cal::local_date>'2022-06-20'),
                    ]),
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-05'),
                )
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''select contains(
                    multirange([
                        range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-10'),
                        range(<cal::local_date>'2022-06-12',
                              <cal::local_date>'2022-06-17'),
                        range(<cal::local_date>'2022-06-20'),
                    ]),
                    <cal::local_date>'2022-06-05',
                )
            ''',
            [True],
        )

    async def test_edgeql_functions_range_overlaps_01(self):
        # Test `overlaps` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select overlaps(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>2, <{st}>4));''',
                [True],
            )

            await self.assert_query_result(
                f'''select overlaps(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>5, <{st}>7));''',
                [False],
            )

            await self.assert_query_result(
                f'''select overlaps(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>2, <{st}>7));''',
                [True],
            )

            await self.assert_query_result(
                f'''select overlaps(
                        range(<{st}>1),
                        range(<{st}>2, <{st}>7));''',
                [True],
            )

            await self.assert_query_result(
                f'''select overlaps(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>2));''',
                [True],
            )

            await self.assert_query_result(
                f'''select overlaps(
                        range(<{st}>{{}}, <{st}>5),
                        range(<{st}>2));''',
                [True],
            )

    async def test_edgeql_functions_range_overlaps_02(self):
        # Test `overlaps` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select overlaps(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        multirange([
                            range(<{st}>0, <{st}>2),
                            range(<{st}>5, <{st}>6),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select overlaps(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        range(<{st}>8),
                    )
                ''',
                [True],
            )

    async def test_edgeql_functions_range_adjacent_01(self):
        # Test `adjacent` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select adjacent(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>5, <{st}>6));''',
                [True],
            )

            await self.assert_query_result(
                f'''select adjacent(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>4, <{st}>6));''',
                [False],
            )

            await self.assert_query_result(
                f'''select adjacent(
                        range(<{st}>1),
                        range(<{st}>0, <{st}>1));''',
                [True],
            )

            await self.assert_query_result(
                f'''select adjacent(
                        range(<{st}>{{}}, <{st}>1),
                        range(<{st}>1));''',
                [True],
            )

    async def test_edgeql_functions_range_adjacent_02(self):
        # Test `adjacent` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select adjacent(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>0, <{st}>1),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select adjacent(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        range(<{st}>{{}}, <{st}>1),
                    )
                ''',
                [True],
            )

    async def test_edgeql_functions_range_strictly_below_01(self):
        # Test `strictly_below` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select strictly_below(
                        range(<{st}>1, <{st}>4),
                        range(<{st}>4, <{st}>5));''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_below(
                        range(<{st}>1, <{st}>4),
                        range(<{st}>1, <{st}>5));''',
                [False],
            )

            await self.assert_query_result(
                f'''select strictly_below(
                        range(<{st}>2, <{st}>3),
                        range(<{st}>10));''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_below(
                        range(<{st}>1),
                        range(<{st}>{{}}, <{st}>10));''',
                [False],
            )

    async def test_edgeql_functions_range_strictly_below_02(self):
        # Test `strictly_below` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select strictly_below(
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>1, <{st}>5),
                        ]),
                        multirange([
                            range(<{st}>6, <{st}>9),
                            range(<{st}>20),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_below(
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>1, <{st}>5),
                        ]),
                        multirange([
                            range(<{st}>2, <{st}>9),
                            range(<{st}>20),
                        ]),
                    )
                ''',
                [False],
            )

            await self.assert_query_result(
                f'''select strictly_below(
                        range(<{st}>{{}}, <{st}>3),
                        multirange([
                            range(<{st}>3, <{st}>4),
                            range(<{st}>7),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_below(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        range(<{st}>10),
                    )
                ''',
                [False],
            )

    async def test_edgeql_functions_range_strictly_above_01(self):
        # Test `strictly_above` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select strictly_above(
                        range(<{st}>4, <{st}>5),
                        range(<{st}>1, <{st}>3));''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_above(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>1, <{st}>3));''',
                [False],
            )

            await self.assert_query_result(
                f'''select strictly_above(
                        range(<{st}>5),
                        range(<{st}>2, <{st}>3));''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_above(
                        range(<{st}>{{}}, <{st}>10),
                        range(<{st}>1));''',
                [False],
            )

    async def test_edgeql_functions_range_strictly_above_02(self):
        # Test `strictly_above` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select strictly_above(
                        multirange([
                            range(<{st}>3, <{st}>4),
                            range(<{st}>7),
                        ]),
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>1, <{st}>3),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_above(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>1, <{st}>3),
                        ]),
                    )
                ''',
                [False],
            )

            await self.assert_query_result(
                f'''select strictly_above(
                        multirange([
                            range(<{st}>3, <{st}>4),
                            range(<{st}>7),
                        ]),
                        range(<{st}>{{}}, <{st}>1),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select strictly_above(
                        range(<{st}>{{}}, <{st}>10),
                        multirange([
                            range(<{st}>3, <{st}>4),
                            range(<{st}>7),
                        ]),
                    )
                ''',
                [False],
            )

    async def test_edgeql_functions_range_bounded_above_01(self):
        # Test `bounded_above` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select bounded_above(
                        range(<{st}>1, <{st}>4),
                        range(<{st}>4, <{st}>5));''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_above(
                        range(<{st}>1, <{st}>5),
                        range(<{st}>2, <{st}>4));''',
                [False],
            )

            await self.assert_query_result(
                f'''select bounded_above(
                        range(<{st}>2, <{st}>3),
                        range(<{st}>10));''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_above(
                        range(<{st}>1),
                        range(<{st}>{{}}, <{st}>10));''',
                [False],
            )

    async def test_edgeql_functions_range_bounded_above_02(self):
        # Test `bounded_above` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select bounded_above(
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>1, <{st}>5),
                        ]),
                        multirange([
                            range(<{st}>6, <{st}>9),
                            range(<{st}>20),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_above(
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>20),
                        ]),
                        multirange([
                            range(<{st}>1, <{st}>3),
                            range(<{st}>6, <{st}>9),
                        ]),
                    )
                ''',
                [False],
            )

            await self.assert_query_result(
                f'''select bounded_above(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        range(<{st}>10),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_above(
                        range(<{st}>{{}}, <{st}>10),
                        multirange([
                            range(<{st}>3, <{st}>4),
                            range(<{st}>7, <{st}>9),
                        ]),
                    )
                ''',
                [False],
            )

    async def test_edgeql_functions_range_bounded_below_01(self):
        # Test `bounded_below` for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select bounded_below(
                        range(<{st}>1, <{st}>4),
                        range(<{st}>1, <{st}>5));''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_below(
                        range(<{st}>1, <{st}>4),
                        range(<{st}>4, <{st}>5));''',
                [False],
            )

            await self.assert_query_result(
                f'''select bounded_below(
                        range(<{st}>2, <{st}>3),
                        range(<{st}>1));''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_below(
                        range(<{st}>{{}}, <{st}>3),
                        range(<{st}>1));''',
                [False],
            )

    async def test_edgeql_functions_range_bounded_below_02(self):
        # Test `bounded_below` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select bounded_below(
                        multirange([
                            range(<{st}>1, <{st}>2),
                            range(<{st}>4, <{st}>7),
                        ]),
                        multirange([
                            range(<{st}>0, <{st}>9),
                            range(<{st}>20),
                        ]),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_below(
                        multirange([
                            range(<{st}>-10, <{st}>-2),
                            range(<{st}>1, <{st}>5),
                        ]),
                        multirange([
                            range(<{st}>2, <{st}>9),
                            range(<{st}>20),
                        ]),
                    )
                ''',
                [False],
            )

            await self.assert_query_result(
                f'''select bounded_below(
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                        range(<{st}>1),
                    )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''select bounded_below(
                        range(<{st}>{{}}, <{st}>3),
                        multirange([
                            range(<{st}>1, <{st}>4),
                            range(<{st}>7),
                        ]),
                    )
                ''',
                [False],
            )

    async def test_edgeql_functions_range_unpack_01(self):
        # Test `range_unpack` for numeric ranges.
        for st in ['int32', 'int64']:
            await self.assert_query_result(
                f'select range_unpack(range(<{st}>1, <{st}>10));',
                [1, 2, 3, 4, 5, 6, 7, 8, 9],
            )

        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'select range_unpack(range(<{st}>1, <{st}>10), <{st}>3);',
                [1, 4, 7],
            )

    async def test_edgeql_functions_range_unpack_02(self):
        # Test `range_unpack` for numeric ranges with both inclusive
        # boundaries.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    select range_unpack(
                        range(<{st}>1, <{st}>10,
                              inc_lower := true,
                              inc_upper := true),
                        <{st}>3
                    );
                ''',
                [1, 4, 7, 10],
            )

    async def test_edgeql_functions_range_unpack_03(self):
        # Test `range_unpack` for numeric ranges with both exclusive
        # boundaries.
        for st in ['int32', 'int64']:
            await self.assert_query_result(
                f'''
                    select range_unpack(
                        range(<{st}>1, <{st}>11,
                              inc_lower := false,
                              inc_upper := false),
                        <{st}>3
                    );
                ''',
                [2, 5, 8],
            )

        for st in ['float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    select range_unpack(
                        range(<{st}>1, <{st}>10,
                              inc_lower := false,
                              inc_upper := false),
                        <{st}>3
                    );
                ''',
                [4, 7],
            )

    async def test_edgeql_functions_range_unpack_04(self):
        # Test `range_unpack` for date/time.
        await self.assert_query_result(
            f'''select <str>range_unpack(
                    range(<datetime>'2022-06-01T07:00:00Z',
                          <datetime>'2022-06-10T07:00:00Z'),
                    <duration>'36:00:00');''',
            [
                '2022-06-01T07:00:00+00:00',
                '2022-06-02T19:00:00+00:00',
                '2022-06-04T07:00:00+00:00',
                '2022-06-05T19:00:00+00:00',
                '2022-06-07T07:00:00+00:00',
                '2022-06-08T19:00:00+00:00',
            ],
        )

        await self.assert_query_result(
            f'''select <str>range_unpack(
                    range(<cal::local_datetime>'2022-06-01T07:00:00',
                          <cal::local_datetime>'2022-06-10T07:00:00'),
                    <cal::relative_duration>'36:00:00');''',
            [
                '2022-06-01T07:00:00',
                '2022-06-02T19:00:00',
                '2022-06-04T07:00:00',
                '2022-06-05T19:00:00',
                '2022-06-07T07:00:00',
                '2022-06-08T19:00:00',
            ],
        )

        await self.assert_query_result(
            f'''select <str>range_unpack(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-10'));''',
            [
                '2022-06-01',
                '2022-06-02',
                '2022-06-03',
                '2022-06-04',
                '2022-06-05',
                '2022-06-06',
                '2022-06-07',
                '2022-06-08',
                '2022-06-09',
            ],
        )

        await self.assert_query_result(
            f'''select <str>range_unpack(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2023-06-10'),
                    <cal::date_duration>'P1M1D');''',
            [
                '2022-06-01',
                '2022-07-02',
                '2022-08-03',
                '2022-09-04',
                '2022-10-05',
                '2022-11-06',
                '2022-12-07',
                '2023-01-08',
                '2023-02-09',
                '2023-03-10',
                '2023-04-11',
                '2023-05-12',
            ],
        )

    async def test_edgeql_functions_range_unpack_05(self):
        # Test `range_unpack` for date/time with non-standard boundaries
        # inclusion.
        await self.assert_query_result(
            f'''select <str>range_unpack(
                    range(<datetime>'2022-06-01T07:00:00Z',
                          <datetime>'2022-06-10T07:00:00Z',
                          inc_lower := false,
                          inc_upper := true),
                    <duration>'36:00:00');''',
            [
                '2022-06-02T19:00:00+00:00',
                '2022-06-04T07:00:00+00:00',
                '2022-06-05T19:00:00+00:00',
                '2022-06-07T07:00:00+00:00',
                '2022-06-08T19:00:00+00:00',
                '2022-06-10T07:00:00+00:00',
            ],
        )

        await self.assert_query_result(
            f'''select <str>range_unpack(
                    range(<cal::local_datetime>'2022-06-01T07:00:00',
                          <cal::local_datetime>'2022-06-10T07:00:00',
                          inc_lower := false,
                          inc_upper := true),
                    <cal::relative_duration>'36:00:00');''',
            [
                '2022-06-02T19:00:00',
                '2022-06-04T07:00:00',
                '2022-06-05T19:00:00',
                '2022-06-07T07:00:00',
                '2022-06-08T19:00:00',
                '2022-06-10T07:00:00',
            ],
        )

        await self.assert_query_result(
            f'''select <str>range_unpack(
                    range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2023-05-13',
                          inc_lower := false,
                          inc_upper := true),
                    <cal::date_duration>'P1M1D');''',
            [
                '2022-06-02',
                '2022-07-03',
                '2022-08-04',
                '2022-09-05',
                '2022-10-06',
                '2022-11-07',
                '2022-12-08',
                '2023-01-09',
                '2023-02-10',
                '2023-03-11',
                '2023-04-12',
                '2023-05-13',
            ],
        )

    async def test_edgeql_functions_range_unpack_06(self):
        # Test `range_unpack` of empty ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                select range_unpack(
                    range(<{st}>{{}}, empty := true), <{st}>1);
                ''',
                [],
            )

        await self.assert_query_result(
            r'''
            select range_unpack(
                range(<datetime>{}, empty := true), <duration>'36:00:00');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            select range_unpack(
                range(<cal::local_datetime>{}, empty := true),
                <cal::relative_duration>'36:00:00');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            select range_unpack(
                range(<cal::local_date>{}, empty := true));
            ''',
            [],
        )

    async def test_edgeql_functions_range_unpack_07(self):
        # Test errors for `range_unpack`.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                "cannot unpack an unbounded range",
            ):
                await self.con.execute(f"""
                    select range_unpack(
                        range(<{st}>5), <{st}>1);
                """)

            async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                "cannot unpack an unbounded range",
            ):
                await self.con.execute(f"""
                    select range_unpack(
                        range(<{st}>{{}}, <{st}>5), <{st}>1);
                """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "cannot unpack an unbounded range",
        ):
            await self.con.execute(r"""
                select range_unpack(
                    range(<datetime>'2022-06-01T07:00:00Z'),
                    <duration>'36:00:00');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "cannot unpack an unbounded range",
        ):
            await self.con.execute(r"""
                select range_unpack(
                    range(<datetime>{}, <datetime>'2022-06-01T07:00:00Z'),
                    <duration>'36:00:00');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "cannot unpack an unbounded range",
        ):
            await self.con.execute(r"""
                select range_unpack(
                    range(<cal::local_datetime>'2022-06-01T07:00:00'),
                    <cal::relative_duration>'36:00:00');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "cannot unpack an unbounded range",
        ):
            await self.con.execute(r"""
                select range_unpack(
                    range(<cal::local_datetime>{},
                          <cal::local_datetime>'2022-06-01T07:00:00'),
                    <cal::relative_duration>'36:00:00');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "cannot unpack an unbounded range",
        ):
            await self.con.execute(r"""
                select range_unpack(
                    range(<cal::local_date>'2022-06-01'));
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "cannot unpack an unbounded range",
        ):
            await self.con.execute(r"""
                select range_unpack(
                    range(<cal::local_date>{},
                          <cal::local_date>'2022-06-01'));
            """)

    async def test_edgeql_functions_multirange_unpack_01(self):
        # Test `multirange_unpack` for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''select multirange_unpack(
                        multirange([
                            range(<{st}>4, <{st}>8),
                            range(<{st}>0, <{st}>2),
                            range(<{st}>10),
                        ]),
                    )
                ''',
                [
                    {
                        "lower": 0,
                        "inc_lower": True,
                        "upper": 2,
                        "inc_upper": False,
                    },
                    {
                        "lower": 4,
                        "inc_lower": True,
                        "upper": 8,
                        "inc_upper": False,
                    },
                    {
                        "lower": 10,
                        "inc_lower": True,
                        "upper": None,
                        "inc_upper": False,
                    },
                ],
                json_only=True,
            )

    async def test_edgeql_functions_encoding_base64_fuzz(self):
        for _ in range(10):
            value = random.randbytes(random.randrange(0, 1000))
            await self.assert_query_result(
                r"""
                WITH
                    MODULE std::enc,
                    value := <bytes>$value,
                    standard_encoded := base64_encode(
                        value),
                    standard_decoded := base64_decode(
                        standard_encoded),
                    standard_unpadded_encoded := base64_encode(
                        value,
                        padding := false),
                    standard_unpadded_decoded := base64_decode(
                        standard_unpadded_encoded,
                        padding := false),
                    urlsafe_encoded := base64_encode(
                        value,
                        alphabet := Base64Alphabet.urlsafe),
                    urlsafe_decoded := base64_decode(
                        urlsafe_encoded,
                        alphabet := Base64Alphabet.urlsafe),
                    urlsafe_unpadded_encoded := base64_encode(
                        value,
                        alphabet := Base64Alphabet.urlsafe,
                        padding := false),
                    urlsafe_unpadded_decoded := base64_decode(
                        urlsafe_unpadded_encoded,
                        alphabet := Base64Alphabet.urlsafe,
                        padding := false),
                SELECT {
                    standard_encoded :=
                        standard_encoded,
                    standard_crosscheck :=
                        standard_decoded = value,
                    standard_unpadded_encoded :=
                        standard_unpadded_encoded,
                    standard_unpadded_crosscheck :=
                        standard_unpadded_decoded = value,
                    urlsafe_encoded :=
                        urlsafe_encoded,
                    urlsafe_crosscheck :=
                        urlsafe_decoded = value,
                    urlsafe_unpadded_encoded :=
                        urlsafe_unpadded_encoded,
                    urlsafe_unpadded_crosscheck :=
                        urlsafe_unpadded_decoded = value,
                }
                """,
                [{
                    "standard_encoded":
                        base64.b64encode(value)
                              .decode("utf-8"),
                    "standard_crosscheck": True,
                    "standard_unpadded_encoded":
                        base64.b64encode(value)
                              .decode("utf-8").rstrip('='),
                    "standard_unpadded_crosscheck": True,
                    "urlsafe_encoded":
                        base64.urlsafe_b64encode(value)
                              .decode("utf-8"),
                    "urlsafe_crosscheck": True,
                    "urlsafe_unpadded_encoded":
                        base64.urlsafe_b64encode(value)
                              .decode("utf-8").rstrip('='),
                    "urlsafe_unpadded_crosscheck": True,
                }],
                variables={
                    "value": value,
                },
            )

    async def test_edgeql_functions_encoding_base64_bad(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r'invalid symbol "~" found while decoding base64 sequence',
        ):
            await self.con.execute(
                'select std::enc::base64_decode("~")'
            )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r'invalid base64 end sequence',
        ):
            await self.con.execute(
                'select std::enc::base64_decode("AA")'
            )

    async def test_edgeql_call_type_as_function_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidReferenceError,
            "does not exist",
            _hint="did you mean to cast to 'str'?",
        ):
            await self.con.execute(f"""
                select str(1);
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidReferenceError,
            "does not exist",
            _hint="did you mean to cast to 'int32'?",
        ):
            await self.con.execute(f"""
                select int32(1);
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidReferenceError,
            "does not exist",
            _hint="did you mean to cast to 'std::cal::local_date'?",
        ):
            await self.con.execute(f"""
                select cal::local_date(1);
            """)

    async def test_edgeql_functions_complex_types_01(self):
        await self.con.execute('''
            create function foo(x: File | URL) -> File | URL using (
                x
            );
        ''')
        await self.assert_query_result(
            'select foo(<File>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<URL>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<File | URL>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select File)).name',
            ['screenshot.png'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select URL)).name',
            ['edgedb.com'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {File, URL})).name',
            ['edgedb.com', 'screenshot.png'],
            sort=True,
        )

    async def test_edgeql_functions_complex_types_02(self):
        await self.con.execute('''
            create function foo(x: str) -> optional File | URL using (
                select {File, URL} filter .name = x limit 1
            );
        ''')
        await self.assert_query_result(
            'select foo(<str>{})',
            [],
        )
        await self.assert_query_result(
            'select foo("haha")',
            [],
        )
        await self.assert_query_result(
            'select foo("screenshot.png").name',
            ['screenshot.png'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo("edgedb.com").name',
            ['edgedb.com'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo({"edgedb.com", "screenshot.png"}).name',
            ['edgedb.com', 'screenshot.png'],
            sort=True,
        )

    async def test_edgeql_functions_complex_types_03(self):
        await self.con.execute('''
            create function foo(x: File | URL) -> str using (
                x.name
            );
        ''')
        await self.assert_query_result(
            'select foo(<File>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<URL>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<File | URL>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select File))',
            ['screenshot.png'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select URL))',
            ['edgedb.com'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {File, URL}))',
            ['edgedb.com', 'screenshot.png'],
            sort=True,
        )

    async def test_edgeql_functions_complex_types_04(self):
        await self.con.execute('''
            create function foo(x: File | URL) -> str using (
                if x is URL
                then assert_exists(x[is URL]).address
                else '~/' ++ x.name
            );
        ''')
        await self.assert_query_result(
            'select foo(<File>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<URL>{})',
            [],
        )
        await self.assert_query_result(
            'select foo(<File | URL>{})',
            [],
        )
        await self.assert_query_result(
            'select foo((select File))',
            ['~/screenshot.png'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select URL))',
            ['https://edgedb.com'],
            sort=True,
        )
        await self.assert_query_result(
            'select foo((select {File, URL}))',
            ['https://edgedb.com', '~/screenshot.png'],
            sort=True,
        )
