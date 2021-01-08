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


import decimal
import json
import os.path

import edgedb

from edb.testbase import server as tb


class TestEdgeQLFunctions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.edgeql')

    async def test_edgeql_functions_count_01(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
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
                    MODULE test,
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
                    MODULE test,
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
            [{}],
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
                WITH MODULE test
                SELECT array_agg(
                    [<str>Issue.number, Issue.status.name]
                    ORDER BY Issue.number);
            """)

    async def test_edgeql_functions_array_agg_11(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH
                    MODULE test
                SELECT
                    array_agg(User{name} ORDER BY User.name);
            ''',
            [[{'name': 'Elvis'}, {'name': 'Yury'}]]
        )

        result = await self.con.query(r'''
            WITH
                MODULE test
            SELECT
                array_agg(User{name} ORDER BY User.name);
        ''')

        self.assertEqual(result[0][0].name, 'Elvis')
        self.assertEqual(result[0][1].name, 'Yury')

    async def test_edgeql_functions_array_agg_13(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test
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
                WITH MODULE test
                SELECT array_agg(array_agg(User.name));
            ''')

    async def test_edgeql_functions_array_agg_15(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
                SELECT array_unpack(array_agg(Issue.number));
            ''',
            {'1', '2', '3', '4'},
        )

    async def test_edgeql_functions_array_unpack_04(self):
        await self.assert_query_result(
            r'''
                # array_agg and array_unpack are inverses of each other
                WITH MODULE test
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

    async def test_edgeql_functions_enumerate_03(self):
        await self.con.execute('SET MODULE test')

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
            r'''SELECT enumerate(test::User { name } ORDER BY .name);''',
            [[0, {"name": "Elvis"}],
             [1, {"name": "Yury"}]],
        )

        await self.assert_query_result(
            r'''SELECT enumerate(test::User ORDER BY .name).1.name;''',
            ["Elvis", "Yury"],
        )

    async def test_edgeql_functions_enumerate_06(self):
        await self.assert_query_result(
            r'''SELECT enumerate(_gen_series(0, 99) FILTER FALSE);''',
            [],
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
        await self.con.execute('SET MODULE test')

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

    async def test_edgeql_functions_re_match_all_02(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE schema,
                    C2 := ScalarType
                SELECT
                    count(re_match_all('(\\w+)', ScalarType.name)) =
                    2 * count(C2);
            ''',
            [True],
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
            r'''SELECT re_replace('[aeiou]', '~', test::User.name);''',
            {'Elv~s', 'Y~ry'},
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[aeiou]', '~', test::User.name,
                                  flags := 'g');
            ''',
            {'Elv~s', 'Y~ry'},
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[aeiou]', '~', test::User.name,
                                  flags := 'i');
            ''',
            {'~lvis', 'Y~ry'},
        )

        await self.assert_query_result(
            r'''
                SELECT re_replace('[aeiou]', '~', test::User.name,
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
        dt = await self.con.query_one(
            'SELECT <str>to_datetime(1590595184.584);'
        )
        self.assertEqual('2020-05-27T15:59:44.584+00:00', dt)

    async def test_edgeql_functions_unix_to_datetime_02(self):
        dt = await self.con.query_one(
            'SELECT <str>to_datetime(1590595184);'
        )
        self.assertEqual('2020-05-27T15:59:44+00:00', dt)

    async def test_edgeql_functions_unix_to_datetime_03(self):
        dt = await self.con.query_one(
            'SELECT <str>to_datetime(517795200);'
        )
        self.assertEqual('1986-05-30T00:00:00+00:00', dt)

    async def test_edgeql_functions_unix_to_datetime_04(self):
        dt = await self.con.query_one(
            'SELECT <str>to_datetime(517795200.00n);'
        )
        self.assertEqual('1986-05-30T00:00:00+00:00', dt)

    async def test_edgeql_functions_datetime_current_01(self):
        # make sure that datetime as a str gets serialized to a
        # particular format
        dt = await self.con.query_one('SELECT <str>datetime_current();')
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
            {'15:00:00'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'minutes');
            ''',
            {'15:01:00'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'seconds');
            ''',
            {'15:01:22'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'milliseconds');
            ''',
            {'15:01:22.306'},
        )

        # Currently no-op but may be useful if precision is improved
        await self.assert_query_result(
            r'''
            SELECT <str>duration_truncate(
                <duration>'15:01:22.306916', 'microseconds');
            ''',
            {'15:01:22.306916'},
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

    async def test_edgeql_functions_to_local_datetime_01(self):
        await self.assert_query_result(
            r'''
                SELECT <str>cal::to_local_datetime(
                    <datetime>'2018-05-07T20:01:22.306916+00:00',
                    'US/Pacific');
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
                    'US/Pacific');
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
                        cal::to_local_datetime('2019/01/01 00:00:00 0715');
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
                    'US/Pacific');
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

    async def test_edgeql_functions_to_duration_01(self):
        await self.assert_query_result(
            r'''SELECT <str>to_duration(hours:=20);''',
            ['20:00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(minutes:=20);''',
            ['00:20:00'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(seconds:=20);''',
            ['00:00:20'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(seconds:=20.15);''',
            ['00:00:20.15'],
        )

        await self.assert_query_result(
            r'''SELECT <str>to_duration(microseconds:=100);''',
            ['00:00:00.0001'],
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
            SELECT to_str(DT, 'FMDDth of FMMonth, YYYY');
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
            SELECT to_str(DT, 'FMDDth of FMMonth, YYYY');
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
                                    'invalid syntax'):
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
            ['11:01:22'],
        )

    async def test_edgeql_functions_min_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT min(User.name);
            ''',
            ['Elvis'],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT min(Issue.time_estimate);
            ''',
            [3000],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT min(<int64>Issue.number);
            ''',
            [{}],
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
            ['16:01:22'],
        )

    async def test_edgeql_functions_max_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT max(User.name);
            ''',
            ['Yury'],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT max(Issue.time_estimate);
            ''',
            [3000],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT max(<int64>Issue.number);
            ''',
            [4],
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
                WITH MODULE test
                SELECT all(len(User.name) = 4);
            ''',
            [False],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
                SELECT any(len(User.name) = 4);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
                SELECT any(Issue.number != '');
            ''',
            [True],
        )

    async def test_edgeql_functions_any_03(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT any(len(User.name) = 4) =
                    NOT all(NOT (len(User.name) = 4));
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
                SELECT _ := round(<int64>Issue.number / 2)
                ORDER BY _;
            ''',
            [0, 1, 2, 2],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
            {},
        )

        await self.assert_query_result(
            r'''SELECT contains(<str>{}, 'a');''',
            {},
        )

        await self.assert_query_result(
            r'''SELECT contains('qwerty', <str>{});''',
            {},
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
            {},
        )

        await self.assert_query_result(
            r'''SELECT contains(<bytes>{}, b'a');''',
            {},
        )

        await self.assert_query_result(
            r'''SELECT contains(b'qwerty', <bytes>{});''',
            {},
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

    async def test_edgeql_functions_find_01(self):
        await self.assert_query_result(
            r'''SELECT find(<str>{}, <str>{});''',
            {},
        )

        await self.assert_query_result(
            r'''SELECT find(<str>{}, 'a');''',
            {},
        )

        await self.assert_query_result(
            r'''SELECT find('qwerty', <str>{});''',
            {},
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
            {},
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
            {},
        )

        await self.assert_query_result(
            r'''SELECT find(<array<str>>{}, 'the');''',
            {},
        )

        await self.assert_query_result(
            r'''SELECT find(['the', 'quick', 'brown', 'fox'], <str>{});''',
            {},
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
                WITH l := {0, 2, 10, 20}
                SELECT len(str_pad_start('Hello', l)) = l;
            ''',
            [True, True, True, True],
        )

        await self.assert_query_result(
            r'''
                WITH l := {0, 2, 10, 20}
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
                    A := len(test::Named.name)
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
                    A := <float64>len(test::Named.name)
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
            {0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop({1, 1, 1});''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop({1, 2, 1, 2});''',
            {0.5},
        )

        await self.assert_query_result(
            r'''SELECT math::stddev_pop({0.1, 0.1, 0.1});''',
            {0},
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
            {0},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop({1, 1, 1});''',
            {0},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop({1, 2, 1, 2});''',
            {0.25},
        )

        await self.assert_query_result(
            r'''SELECT math::var_pop({0.1, 0.1, 0.1});''',
            {0},
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
                SELECT var_pop(X) = stddev_pop(X) ^ 2;
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE math,
                    X := {0.1, 0.2, 0.1, 0.2}
                SELECT var_pop(X) = stddev_pop(X) ^ 2;
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
