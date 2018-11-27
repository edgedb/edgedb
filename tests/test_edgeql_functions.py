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


import os.path

import edgedb

from edb.server import _testbase as tb
from edb.tools import test


class TestEdgeQLFunctions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.eql')

    async def test_edgeql_functions_array_contains_01(self):
        await self.assert_query_result(r'''
            SELECT std::array_contains(<array<int64>>[], {1, 3});
            SELECT array_contains([1], {1, 3});
            SELECT array_contains([1, 2], 1);
            SELECT array_contains([1, 2], 3);
            SELECT array_contains(['a'], <std::str>{});
        ''', [
            [False, False],
            [True, False],
            [True],
            [False],
            [],
        ])

    async def test_edgeql_functions_array_contains_02(self):
        await self.assert_query_result('''
            WITH x := [3, 1, 2]
            SELECT array_contains(x, 2);

            WITH x := [3, 1, 2]
            SELECT array_contains(x, 5);

            WITH x := [3, 1, 2]
            SELECT array_contains(x, 5);
        ''', [
            [True],
            [False],
            [False],
        ])

    async def test_edgeql_functions_count_01(self):
        await self.assert_query_result(r"""
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
        """, [
            [True]
        ])

    async def test_edgeql_functions_count_02(self):
        await self.assert_query_result(r"""
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
        """, [
            [True]
        ])

    async def test_edgeql_functions_count_03(self):
        await self.assert_query_result(r"""
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
        """, [
            [True]
        ])

    async def test_edgeql_functions_array_agg_01(self):
        res = await self.query('''
            SELECT array_agg({1, 2, 3});
            SELECT array_agg({3, 2, 3});
            SELECT array_agg({3, 3, 2});
        ''')
        self.assert_data_shape(res, [
            [[1, 2, 3]],
            [[3, 2, 3]],
            [[3, 3, 2]],
        ])

    async def test_edgeql_functions_array_agg_02(self):
        res = await self.query('''
            SELECT array_agg({1, 2, 3})[0];
            SELECT array_agg({3, 2, 3})[1];
            SELECT array_agg({3, 3, 2})[-1];
        ''')
        self.assert_data_shape(res, [
            [1],
            [2],
            [2],
        ])

    async def test_edgeql_functions_array_agg_03(self):
        await self.assert_query_result('''
            WITH x := {3, 1, 2}
            SELECT array_agg(x ORDER BY x);

            WITH x := {3, 1, 2}
            SELECT array_agg(x ORDER BY x) = [1, 2, 3];
        ''', [
            [[1, 2, 3]],
            [True],
        ])

    async def test_edgeql_functions_array_agg_04(self):
        await self.assert_query_result('''
            WITH x := {3, 1, 2}
            SELECT array_contains(array_agg(x ORDER BY x), 2);

            WITH x := {3, 1, 2}
            SELECT array_contains(array_agg(x ORDER BY x), 5);

            WITH x := {3, 1, 2}
            SELECT array_contains(array_agg(x ORDER BY x), 5);
        ''', [
            [True],
            [False],
            [False],
        ])

    async def test_edgeql_functions_array_agg_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not determine expression type'):

            await self.query("""
                SELECT array_agg({});
            """)

    async def test_edgeql_functions_array_agg_06(self):
        await self.assert_query_result('''
            SELECT array_agg(<int64>{});
            SELECT array_agg(DISTINCT <int64>{});
        ''', [
            [
                []
            ],
            [
                []
            ],
        ])

    async def test_edgeql_functions_array_agg_07(self):
        await self.assert_query_result('''
            SELECT array_agg((SELECT schema::ObjectType FILTER False));
            SELECT array_agg(
                (SELECT schema::ObjectType
                 FILTER <str>schema::ObjectType.id = '~')
            );
        ''', [
            [
                []
            ],
            [
                []
            ],
        ])

    async def test_edgeql_functions_array_agg_08(self):
        await self.assert_query_result('''
            WITH x := <int64>{}
            SELECT array_agg(x);

            WITH x := (SELECT schema::ObjectType FILTER False)
            SELECT array_agg(x);

            WITH x := (
                SELECT schema::ObjectType
                FILTER <str>schema::ObjectType.id = '~'
            )
            SELECT array_agg(x);
        ''', [
            [
                []
            ],
            [
                []
            ],
            [
                []
            ],
        ])

    async def test_edgeql_functions_array_agg_09(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT
                ObjectType {
                    l := array_agg(
                        ObjectType.properties.name
                        FILTER
                            ObjectType.properties.name IN {
                                'std::id',
                                'schema::name'
                            }
                        ORDER BY ObjectType.properties.name ASC
                    )
                }
            FILTER
                ObjectType.name = 'schema::Object';
        """, [
            [{
                'l': ['schema::name', 'std::id']
            }]
        ])

    async def test_edgeql_functions_array_agg_10(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.query(r"""
                WITH MODULE test
                SELECT array_agg(
                    [<str>Issue.number, Issue.status.name]
                    ORDER BY Issue.number);
            """)

    async def test_edgeql_functions_array_agg_11(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT array_agg(
                (<str>Issue.number, Issue.status.name)
                ORDER BY Issue.number
            )[1];
        """, [
            [['2', 'Open']]
        ])

    async def test_edgeql_functions_array_agg_12(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test
            SELECT
                array_agg(User{name} ORDER BY User.name);
        ''', [
            [[{'name': 'Elvis'}, {'name': 'Yury'}]]
        ])

    async def test_edgeql_functions_array_agg_13(self):
        await self.assert_query_result(r'''
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
        ''', [
            [
                {'number': '1', 'watchers_array': [{'name': 'Yury'}]},
                {'number': '2', 'watchers_array': [{'name': 'Elvis'}]},
                {'number': '3', 'watchers_array': [{'name': 'Elvis'}]}
            ]
        ])

    async def test_edgeql_functions_array_agg_14(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.query(r'''
                WITH MODULE test
                SELECT array_agg(array_agg(User.name));
            ''')

    async def test_edgeql_functions_array_agg_15(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT array_agg(
                ([([User.name],)],) ORDER BY User.name
            );
        ''', [
            [       # result set
                [   # array_agg
                    [[[['Elvis']]]], [[[['Yury']]]],
                ]
            ]
        ])

    async def test_edgeql_functions_array_agg_16(self):
        await self.assert_query_result(r'''
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
        ''', [
            [       # result set
                [   # outer array_agg
                    [[[['Elvis', 'Yury']]]]
                ]
            ]
        ])

    async def test_edgeql_functions_array_unpack_01(self):
        await self.assert_query_result(r'''
            SELECT [1, 2];
            SELECT array_unpack([1, 2]);
            SELECT array_unpack([10, 20]) - 1;
        ''', [
            [[1, 2]],
            [1, 2],
            [9, 19],
        ])

    async def test_edgeql_functions_array_unpack_02(self):
        await self.assert_query_result(r'''
            # array_agg and array_unpack are inverses of each other
            SELECT array_agg(array_unpack([1, 2, 3])) = [1, 2, 3];
            SELECT array_unpack(array_agg({1, 2, 3}));
        ''', [
            [True],
            {1, 2, 3},
        ])

    async def test_edgeql_functions_array_unpack_03(self):
        await self.assert_query_result(r'''
            # array_agg and array_unpack are inverses of each other
            WITH MODULE test
            SELECT array_unpack(array_agg(Issue.number));
        ''', [
            {'1', '2', '3', '4'},
        ])

    async def test_edgeql_functions_array_unpack_04(self):
        await self.assert_sorted_query_result(r'''
            # array_agg and array_unpack are inverses of each other
            WITH MODULE test
            SELECT array_unpack(array_agg(Issue)){number};
        ''', lambda x: x['number'], [
            [
                {'number': '1'},
                {'number': '2'},
                {'number': '3'},
                {'number': '4'},
            ],
        ])

    async def test_edgeql_functions_array_enumerate_01(self):
        await self.assert_query_result(r'''
            SELECT [10, 20];
            SELECT array_enumerate([10,20]);
            SELECT array_enumerate([10,20]).1 + 100;
            SELECT array_enumerate([10,20]).index + 100;
        ''', [
            [[10, 20]],
            [{"element": 10, "index": 0}, {"element": 20, "index": 1}],
            [100, 101],
            [100, 101],
        ])

    async def test_edgeql_functions_array_enumerate_02(self):
        await self.assert_query_result(r'''
            SELECT array_enumerate([10,20]).0 + 100;
            SELECT array_enumerate([10,20]).element + 1000;
        ''', [
            [110, 120],
            [1010, 1020],
        ])

    @test.xfail('''
        Every single line of this test produces the same error:

        UnknownEdgeDBError: a column definition list is required for
        functions returning "record"
    ''')
    async def test_edgeql_functions_array_enumerate_03(self):
        await self.assert_query_result(r'''
            SELECT array_enumerate([(x:=1)]).0;
            SELECT array_enumerate([(x:=1)]).0.x;

            SELECT array_enumerate([(x:=(a:=2))]).0;
            SELECT array_enumerate([(x:=(a:=2))]).0.x;

            SELECT array_enumerate([(x:=(a:=2))]).0.x.a;
        ''', [
            [{"x": 1}],
            [1],

            [{"x": {"a": 2}}],
            [{"a": 2}],

            [2],
        ])

    async def test_edgeql_functions_array_get_01(self):
        await self.assert_query_result(r'''
            SELECT array_get([1, 2, 3], 2);
            SELECT array_get([1, 2, 3], -2);
            SELECT array_get([1, 2, 3], 20);
            SELECT array_get([1, 2, 3], -20);
        ''', [
            [3],
            [2],
            [],
            [],
        ])

    async def test_edgeql_functions_array_get_02(self):
        await self.assert_query_result(r'''
            SET MODULE test;

            SELECT array_get(array_agg(Issue.number ORDER BY Issue.number), 2);
            SELECT array_get(array_agg(
                Issue.number ORDER BY Issue.number), -2);
            SELECT array_get(array_agg(Issue.number), 20);
            SELECT array_get(array_agg(Issue.number), -20);
        ''', [
            None,
            ['3'],
            ['3'],
            [],
            [],
        ])

    async def test_edgeql_functions_array_get_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant array_get'):

            await self.query(r'''
                SELECT array_get([1, 2, 3], 2^40);
            ''')

    async def test_edgeql_functions_array_get_04(self):
        await self.assert_query_result(r'''
            SELECT array_get([1, 2, 3], 0) ?? 42;
            SELECT array_get([1, 2, 3], 0, default := -1) ?? 42;
            SELECT array_get([1, 2, 3], -2) ?? 42;
            SELECT array_get([1, 2, 3], 20) ?? 42;
            SELECT array_get([1, 2, 3], -20) ?? 42;
        ''', [
            [1],
            [1],
            [2],
            [42],
            [42],
        ])

    async def test_edgeql_functions_array_get_05(self):
        await self.assert_query_result(r'''
            SELECT array_get([1, 2, 3], 1, default := 4200) ?? 42;
            SELECT array_get([1, 2, 3], -2, default := 4200) ?? 42;
            SELECT array_get([1, 2, 3], 20, default := 4200) ?? 42;
            SELECT array_get([1, 2, 3], -20, default := 4200) ?? 42;
        ''', [
            [2],
            [2],
            [4200],
            [4200],
        ])

    async def test_edgeql_functions_array_get_06(self):
        await self.assert_query_result(r'''
            SELECT array_get([(20,), (30,)], 0);
            SELECT array_get([(a:=20), (a:=30)], 1);

            SELECT array_get([(20,), (30,)], 0).0;
            SELECT array_get([(a:=20), (a:=30)], 1).0;

            SELECT array_get([(a:=20, b:=1), (a:=30, b:=2)], 0).a;
            SELECT array_get([(a:=20, b:=1), (a:=30, b:=2)], 1).b;
        ''', [
            [[20]],
            [{'a': 30}],

            [20],
            [30],

            [20],
            [2],
        ])

    async def test_edgeql_functions_re_match_01(self):
        await self.assert_query_result(r'''
            SELECT re_match('ab', 'AbabaB');
            SELECT re_match('AB', 'AbabaB');
            SELECT re_match('(?i)AB', 'AbabaB');
            SELECT re_match('ac', 'AbabaB');

            SELECT EXISTS re_match('ac', 'AbabaB');
            SELECT NOT EXISTS re_match('ac', 'AbabaB');

            SELECT EXISTS re_match('ab', 'AbabaB');
            SELECT NOT EXISTS re_match('ab', 'AbabaB');

            SELECT x := re_match({'(?i)ab', 'a'}, 'AbabaB') ORDER BY x;
            SELECT x := re_match({'(?i)ab', 'a'}, {'AbabaB', 'qwerty'})
                ORDER BY x;
        ''', [
            [['ab']],
            [],
            [['Ab']],
            [],

            [False],
            [True],

            [True],
            [False],

            [['Ab'], ['a']],
            [['Ab'], ['a']],
        ])

    async def test_edgeql_functions_re_match_02(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT x := re_match('(\\w+)::(Link|Property)', ObjectType.name)
            ORDER BY x;
        ''', [
            [['schema', 'Link'], ['schema', 'Property']],
        ])

    async def test_edgeql_functions_re_match_all_01(self):
        await self.assert_query_result(r'''
            SELECT re_match_all('ab', 'AbabaB');
            SELECT re_match_all('AB', 'AbabaB');
            SELECT re_match_all('(?i)AB', 'AbabaB');
            SELECT re_match_all('ac', 'AbabaB');

            SELECT EXISTS re_match_all('ac', 'AbabaB');
            SELECT NOT EXISTS re_match_all('ac', 'AbabaB');

            SELECT EXISTS re_match_all('(?i)ab', 'AbabaB');
            SELECT NOT EXISTS re_match_all('(?i)ab', 'AbabaB');

            SELECT x := re_match_all({'(?i)ab', 'a'}, 'AbabaB') ORDER BY x;
            SELECT x := re_match_all({'(?i)ab', 'a'}, {'AbabaB', 'qwerty'})
                ORDER BY x;
        ''', [
            [['ab']],
            [],
            [['Ab'], ['ab'], ['aB']],
            [],

            [False],
            [True],

            [True],
            [False],

            [['Ab'], ['a'], ['a'], ['aB'], ['ab']],
            [['Ab'], ['a'], ['a'], ['aB'], ['ab']],
        ])

    async def test_edgeql_functions_re_match_all_02(self):
        await self.assert_query_result(r'''
            WITH
                MODULE schema,
                C2 := ObjectType
            SELECT
                count(re_match_all('(\\w+)', ObjectType.name)) =
                2 * count(C2);
        ''', [
            [True],
        ])

    async def test_edgeql_functions_re_test_01(self):
        await self.assert_query_result(r'''
            SELECT re_test('ac', 'AbabaB');
            SELECT NOT re_test('ac', 'AbabaB');

            SELECT re_test(r'(?i)ab', 'AbabaB');
            SELECT NOT re_test(r'(?i)ab', 'AbabaB');

            # the result always exists
            SELECT EXISTS re_test('(?i)ac', 'AbabaB');
            SELECT NOT EXISTS re_test('(?i)ac', 'AbabaB');

            SELECT x := re_test({'ab', 'a'}, 'AbabaB') ORDER BY x;
            SELECT x := re_test({'ab', 'a'}, {'AbabaB', 'qwerty'}) ORDER BY x;
        ''', [
            [False],
            [True],

            [True],
            [False],

            [True],
            [False],

            [True, True],
            [False, False, True, True],
        ])

    async def test_edgeql_functions_re_test_02(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT count(
                ObjectType FILTER re_test(r'(\W\w)bject$', ObjectType.name)
            ) = 2;
        ''', [
            [True],
        ])

    async def test_edgeql_functions_re_replace_01(self):
        await self.assert_query_result(r'''
            SELECT re_replace('l', 'L', 'Hello World');
            SELECT re_replace('l', 'L', 'Hello World', flags := 'g');
            SELECT re_replace('[a-z]', '~', 'Hello World', flags := 'i');
            SELECT re_replace('[a-z]', '~', 'Hello World', flags := 'gi');
        ''', [
            ['HeLlo World'],
            ['HeLLo WorLd'],
            ['~ello World'],
            ['~~~~~ ~~~~~'],
        ])

    async def test_edgeql_functions_re_replace_02(self):
        await self.assert_query_result(r'''
            SELECT re_replace('[aeiou]', '~', test::User.name);
            SELECT re_replace('[aeiou]', '~', test::User.name, flags := 'g');
            SELECT re_replace('[aeiou]', '~', test::User.name, flags := 'i');
            SELECT re_replace('[aeiou]', '~', test::User.name, flags := 'gi');
        ''', [
            {'Elv~s', 'Y~ry'},
            {'Elv~s', 'Y~ry'},
            {'~lvis', 'Y~ry'},
            {'~lv~s', 'Y~ry'},
        ])

    async def test_edgeql_functions_sum_01(self):
        await self.assert_query_result(r'''
            SELECT sum({1, 2, 3, -4, 5});
            SELECT sum({0.1, 0.2, 0.3, -0.4, 0.5});
        ''', [
            [7],
            [0.7],
        ])

    async def test_edgeql_functions_sum_02(self):
        await self.assert_query_result(r'''
            SELECT sum({1, 2, 3, -4.2, 5});
        ''', [
            [6.8],
        ])

    async def test_edgeql_functions_sum_03(self):
        await self.assert_query_result(r'''
            SELECT sum({1.0, 2.0, 3.0, -4.2, 5});
        ''', [
            [6.8],
        ])

    async def test_edgeql_functions_sum_04(self):
        await self.assert_query_result(r'''
            SELECT sum(<int16>2) IS int64;
            SELECT sum(<int32>2) IS int64;
            SELECT sum(<int64>2) IS int64;
            SELECT sum(<float32>2) IS float32;
            SELECT sum(<float64>2) IS float64;
            SELECT sum(<decimal>2) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    async def test_edgeql_functions_datetime_current_01(self):
        dt = (await self.query('SELECT datetime_current();'))[0][0]
        self.assertRegex(dt, r'\d+-\d+-\d+T\d+:\d+:\d+\.\d+.*')

    async def test_edgeql_functions_datetime_current_02(self):
        res = await self.query(r'''
            WITH MODULE schema
            SELECT Type {
                dt_t := datetime_of_transaction(),
                dt_s := datetime_of_statement(),
                dt_n := datetime_current(),
            };
            # NOTE: this test assumes that there's at least 1 microsecond
            # time difference between statements
            WITH MODULE schema
            SELECT Type {
                dt_t := datetime_of_transaction(),
                dt_s := datetime_of_statement(),
                dt_n := datetime_current(),
            };
        ''')

        batch1 = res[0]
        batch2 = res[1]
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
        await self.assert_query_result(r'''
            SELECT datetime_get(
                <datetime>'2018-05-07T15:01:22.306916-05', 'year');
            SELECT datetime_get(
                <datetime>'2018-05-07T15:01:22.306916-05', 'month');
            SELECT datetime_get(
                <datetime>'2018-05-07T15:01:22.306916-05', 'day');
            SELECT datetime_get(
                <datetime>'2018-05-07T15:01:22.306916-05', 'hour');
            SELECT datetime_get(
                <datetime>'2018-05-07T15:01:22.306916-05', 'minute');
            SELECT datetime_get(
                <datetime>'2018-05-07T15:01:22.306916-05', 'second');
            SELECT datetime_get(
                <datetime>'2018-05-07T15:01:22.306916-05', 'timezone_hour');
        ''', [
            {2018},
            {5},
            {7},
            {20},
            {1},
            {22.306916},
            {0},
        ])

    async def test_edgeql_functions_datetime_get_02(self):
        await self.assert_query_result(r'''
            SELECT datetime_get(
                <naive_datetime>'2018-05-07T15:01:22.306916', 'year');
            SELECT datetime_get(
                <naive_datetime>'2018-05-07T15:01:22.306916', 'month');
            SELECT datetime_get(
                <naive_datetime>'2018-05-07T15:01:22.306916', 'day');
            SELECT datetime_get(
                <naive_datetime>'2018-05-07T15:01:22.306916', 'hour');
            SELECT datetime_get(
                <naive_datetime>'2018-05-07T15:01:22.306916', 'minute');
            SELECT datetime_get(
                <naive_datetime>'2018-05-07T15:01:22.306916', 'second');
        ''', [
            {2018},
            {5},
            {7},
            {15},
            {1},
            {22.306916},
        ])

    async def test_edgeql_functions_datetime_get_03(self):
        with self.assertRaisesRegex(
            edgedb.InternalServerError,
                'timestamp units "timezone_hour" not supported'):
            await self.query('''
                SELECT datetime_get(
                    <naive_datetime>'2018-05-07T15:01:22.306916',
                    'timezone_hour'
                );
            ''')

    async def test_edgeql_functions_date_get_01(self):
        await self.assert_query_result(r'''
            SELECT date_get(<naive_date>'2018-05-07', 'year');
            SELECT date_get(<naive_date>'2018-05-07', 'month');
            SELECT date_get(<naive_date>'2018-05-07', 'day');
        ''', [
            {2018},
            {5},
            {7},
        ])

    async def test_edgeql_functions_time_get_01(self):
        await self.assert_query_result(r'''
            SELECT time_get(<naive_time>'15:01:22.306916', 'hour');
            SELECT time_get(<naive_time>'15:01:22.306916', 'minute');
            SELECT time_get(<naive_time>'15:01:22.306916', 'second');
        ''', [
            {15},
            {1},
            {22.306916},
        ])

    async def test_edgeql_functions_timedelta_get_01(self):
        await self.assert_query_result(r'''
            SELECT timedelta_get(<timedelta>'15:01:22.306916', 'hour');
            SELECT timedelta_get(<timedelta>'15:01:22.306916', 'minute');
            SELECT timedelta_get(<timedelta>'15:01:22.306916', 'second');

            SELECT timedelta_get(<timedelta>'3 days 15:01:22', 'day');
        ''', [
            {15},
            {1},
            {22.306916},
            {3},
        ])

    async def test_edgeql_functions_to_datetime_01(self):
        await self.assert_query_result(r'''
            SELECT <str>to_datetime(2018, 5, 7, 15, 1, 22.306916);
            SELECT <str>to_datetime(2018, 5, 7, 15, 1, 22.306916, 'EST');
            SELECT <str>to_datetime(2018, 5, 7, 15, 1, 22.306916, '-5');
        ''', [
            ['2018-05-07T15:01:22.306916+00:00'],
            ['2018-05-07T20:01:22.306916+00:00'],
            ['2018-05-07T20:01:22.306916+00:00'],
        ])

    async def test_edgeql_functions_to_naive_datetime_01(self):
        await self.assert_query_result(r'''
            SELECT <str>to_naive_datetime(2018, 5, 7, 15, 1, 22.306916);
        ''', [
            ['2018-05-07T15:01:22.306916'],
        ])

    async def test_edgeql_functions_to_naive_date_01(self):
        await self.assert_query_result(r'''
            SELECT <str>to_naive_date(2018, 5, 7);
        ''', [
            ['2018-05-07'],
        ])

    async def test_edgeql_functions_to_naive_time_01(self):
        await self.assert_query_result(r'''
            SELECT <str>to_naive_time(15, 1, 22.306916);
        ''', [
            ['15:01:22.306916'],
        ])

    async def test_edgeql_functions_to_timedelta_01(self):
        await self.assert_query_result(r'''
            SELECT <str>to_timedelta(years:=20);
            SELECT <str>to_timedelta(months:=20);
            SELECT <str>to_timedelta(weeks:=20);
            SELECT <str>to_timedelta(days:=20);
            SELECT <str>to_timedelta(hours:=20);
            SELECT <str>to_timedelta(mins:=20);
            SELECT <str>to_timedelta(secs:=20);
        ''', [
            ['20 years'],
            ['1 year 8 mons'],
            ['140 days'],
            ['20 days'],
            ['20:00:00'],
            ['00:20:00'],
            ['00:00:20'],
        ])

    async def test_edgeql_functions_to_timedelta_02(self):
        await self.assert_query_result(r'''
            SELECT to_timedelta(years:=20) > to_timedelta(months:=20);
            SELECT to_timedelta(months:=20) > to_timedelta(weeks:=20);
            SELECT to_timedelta(weeks:=20) > to_timedelta(days:=20);
            SELECT to_timedelta(days:=20) > to_timedelta(hours:=20);
            SELECT to_timedelta(hours:=20) > to_timedelta(mins:=20);
            SELECT to_timedelta(mins:=20) > to_timedelta(secs:=20);
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_functions_to_str_01(self):
        # at the very least the cast <str> should be equivalent to
        # a call to to_str() without explicit format for simple scalars
        await self.assert_query_result(r'''
            WITH DT := datetime_current()
            # FIXME: the cast has a "T" and the str doesn't for some reason
            SELECT <str>DT = to_str(DT);

            WITH D := <naive_date>datetime_current()
            SELECT <str>D = to_str(D);

            WITH NT := <naive_time>datetime_current()
            SELECT <str>NT = to_str(NT);

            SELECT <str>123 = to_str(123);
            SELECT <str>123.456 = to_str(123.456);
            SELECT <str>123.456e-20 = to_str(123.456e-20);
            # an unambiguous way to define a decimal is by casting
            # from a string
            SELECT <str><decimal>'123456789012345678901234567890.1234567890' =
                to_str(<decimal>'123456789012345678901234567890.1234567890');
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_functions_to_str_02(self):
        await self.assert_query_result(r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'YYYY-MM-DD');

            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'YYYYBC');

            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'FMDDth of FMMonth, YYYY');

            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'CCth "century"');

            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'Y,YYY Month DD Day');

            # the format string doesn't have any special characters
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'foo');

            # the format string doesn't have any special characters
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, '');
        ''', [
            {'2018-05-07'},
            {'2018AD'},
            {'7th of May, 2018'},
            {'21st century'},
            {'2,018 May       07 Monday   '},
            {'foo'},
            {},
        ])

    async def test_edgeql_functions_to_str_03(self):
        await self.assert_query_result(r'''
            WITH DT := <datetime>'2018-05-07 15:01:22.306916-05'
            SELECT to_str(DT, 'HH:MI A.M.');
        ''', [
            # tests run in UTC time-zone, so 15:01-05 is 20:01 UTC
            {'08:01 P.M.'},
        ])

    async def test_edgeql_functions_to_str_04(self):
        await self.assert_query_result(r'''
            WITH DT := <naive_date>'2018-05-07'
            SELECT to_str(DT, 'YYYY-MM-DD');

            WITH DT := <naive_date>'2018-05-07'
            SELECT to_str(DT, 'YYYYBC');

            WITH DT := <naive_date>'2018-05-07'
            SELECT to_str(DT, 'FMDDth of FMMonth, YYYY');

            WITH DT := <naive_date>'2018-05-07'
            SELECT to_str(DT, 'CCth "century"');

            WITH DT := <naive_date>'2018-05-07'
            SELECT to_str(DT, 'Y,YYY Month DD Day');

            # the format string doesn't have any special characters
            WITH DT := <naive_date>'2018-05-07'
            SELECT to_str(DT, 'foo');

            # the format string doesn't have any special characters
            WITH DT := <naive_date>'2018-05-07'
            SELECT to_str(DT, '');
        ''', [
            {'2018-05-07'},
            {'2018AD'},
            {'7th of May, 2018'},
            {'21st century'},
            {'2,018 May       07 Monday   '},
            {'foo'},
            {},
        ])

    async def test_edgeql_functions_to_str_05(self):
        await self.assert_query_result(r'''
            SELECT to_str(123456789, '99');
            SELECT to_str(123456789, '999999999');
            SELECT to_str(123456789, '999,999,999');
            SELECT to_str(123456789, '999,999,999,999');
            SELECT to_str(123456789, 'FM999,999,999,999');

            SELECT to_str(123456789, 'S999,999,999,999');
            SELECT to_str(123456789, 'SG999,999,999,999');
            SELECT to_str(123456789, 'S099,999,999,999');
            SELECT to_str(123456789, 'SG099,999,999,999');

            SELECT to_str(123456789, 'S099999999999');
            SELECT to_str(123456789, 'S990999999999');
            SELECT to_str(123456789, 'FMS990999999999');

            SELECT to_str(-123456789, '999999999PR');

            SELECT to_str(987654321, 'FM999999999th');
        ''', [
            {' ##'},  # the number is too long for the desired representation
            {' 123456789'},
            {' 123,456,789'},
            {'     123,456,789'},
            {'123,456,789'},
            {'    +123,456,789'},
            {'+    123,456,789'},
            {'+000,123,456,789'},
            {'+000,123,456,789'},
            {'+000123456789'},
            {'  +0123456789'},
            {'+0123456789'},
            {'<123456789>'},
            {'987654321st'},
        ])

    async def test_edgeql_functions_to_str_06(self):
        await self.assert_query_result(r'''
            SELECT to_str(123.456789, '99');
            SELECT to_str(123.456789, '999');
            SELECT to_str(123.456789, '999.999');
            SELECT to_str(123.456789, '999.999999999');

            SELECT to_str(123.456789, 'FM999.999999999');
            SELECT to_str(123.456789e-20, '999.999999999');
            SELECT to_str(123.456789e-20, 'FM999.999999999');
            SELECT to_str(123.456789e-20, '099.999999990');
            SELECT to_str(123.456789e-20, 'FM990.099999999');

            SELECT to_str(123.456789e-20, '0.0999EEEE');
            SELECT to_str(123.456789e20, '0.0999EEEE');
        ''', [
            {' ##'},  # the integer part of the number is too long
            {' 123'},
            {' 123.457'},
            {' 123.456789000'},
            {'123.456789'},
            {'    .000000000'},
            {'0.'},
            {' 000.000000000'},
            {'0.0'},
            {' 1.2346e-18'},
            {' 1.2346e+22'},
        ])

    async def test_edgeql_functions_to_str_07(self):
        await self.assert_query_result(r'''
            SELECT to_str(<naive_time>'15:01:22', 'HH:MI A.M.');
            SELECT to_str(<naive_time>'15:01:22', 'HH:MI:SSam.');
            SELECT to_str(<naive_time>'15:01:22', 'HH24:MI');
        ''', [
            {'03:01 P.M.'},
            {'03:01:22pm.'},
            {'15:01'},
        ])

    async def test_edgeql_functions_to_int_01(self):
        await self.assert_query_result(r'''
            SELECT to_int64(' 123456789', '999999999');
            SELECT to_int64(' 123,456,789', '999,999,999');
            SELECT to_int64('     123,456,789', '999,999,999,999');
            SELECT to_int64('123,456,789', 'FM999,999,999,999');
            SELECT to_int64('    +123,456,789', 'S999,999,999,999');
            SELECT to_int64('+    123,456,789', 'SG999,999,999,999');
            SELECT to_int64('+000,123,456,789', 'S099,999,999,999');
            SELECT to_int64('+000,123,456,789', 'SG099,999,999,999');
            SELECT to_int64('+000123456789', 'S099999999999');
            SELECT to_int64('  +0123456789', 'S990999999999');
            SELECT to_int64('+0123456789', 'FMS990999999999');
            SELECT to_int64('<123456789>', '999999999PR');
            SELECT to_int64('987654321st', 'FM999999999th');
        ''', [
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {-123456789},
            {987654321},
        ])

    async def test_edgeql_functions_to_int_02(self):
        await self.assert_query_result(r'''
            SELECT to_int32(' 123456789', '999999999');
            SELECT to_int32(' 123,456,789', '999,999,999');
            SELECT to_int32('     123,456,789', '999,999,999,999');
            SELECT to_int32('123,456,789', 'FM999,999,999,999');
            SELECT to_int32('    +123,456,789', 'S999,999,999,999');
            SELECT to_int32('+    123,456,789', 'SG999,999,999,999');
            SELECT to_int32('+000,123,456,789', 'S099,999,999,999');
            SELECT to_int32('+000,123,456,789', 'SG099,999,999,999');
            SELECT to_int32('+000123456789', 'S099999999999');
            SELECT to_int32('  +0123456789', 'S990999999999');
            SELECT to_int32('+0123456789', 'FMS990999999999');
            SELECT to_int32('<123456789>', '999999999PR');
            SELECT to_int32('987654321st', 'FM999999999th');
        ''', [
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {123456789},
            {-123456789},
            {987654321},
        ])

    async def test_edgeql_functions_to_int_03(self):
        await self.assert_query_result(r'''
            SELECT to_int16('12345', '999999999');
            SELECT to_int16('12,345', '999,999,999');
            SELECT to_int16('     12,345', '999,999,999,999');
            SELECT to_int16('12,345', 'FM999,999,999,999');
            SELECT to_int16('+12,345', 'S999,999,999,999');
            SELECT to_int16('+    12,345', 'SG999,999,999,999');
            SELECT to_int16('-000,012,345', 'S099,999,999,999');
            SELECT to_int16('+000,012,345', 'SG099,999,999,999');
            SELECT to_int16('+00012345', 'S099999999999');
            SELECT to_int16('  +012345', 'S990999999999');
            SELECT to_int16('+012345', 'FMS990999999999');
            SELECT to_int16('<12345>', '999999999PR');
            SELECT to_int16('4321st', 'FM999999999th');
        ''', [
            {12345},
            {12345},
            {12345},
            {12345},
            {12345},
            {12345},
            {-12345},
            {12345},
            {12345},
            {12345},
            {12345},
            {-12345},
            {4321},
        ])

    async def test_edgeql_functions_to_float_01(self):
        await self.assert_query_result(r'''
            SELECT to_float64(' 123', '999');
            SELECT to_float64('123.457', '999.999');
            SELECT to_float64(' 123.456789000', '999.999999999');
            SELECT to_float64('123.456789', 'FM999.999999999');
        ''', [
            {123},
            {123.457},
            {123.456789},
            {123.456789},
        ])

    async def test_edgeql_functions_to_float_02(self):
        await self.assert_query_result(r'''
            SELECT to_float32(' 123', '999');
            SELECT to_float32('123.457', '999.999');
            SELECT to_float32(' 123.456789000', '999.999999999');
            SELECT to_float32('123.456789', 'FM999.999999999');
        ''', [
            {123},
            {123.457},
            {123.457},
            {123.457},
        ])

    async def test_edgeql_functions_to_decimal_01(self):
        await self.assert_query_result(r'''
            SELECT to_decimal(' 123', '999');
            SELECT to_decimal('123.457', '999.999');
            SELECT to_decimal(' 123.456789000', '999.999999999');
            SELECT to_decimal('123.456789', 'FM999.999999999');
        ''', [
            {123},
            {123.457},
            {123.456789},
            {123.456789},
        ])

    async def test_edgeql_functions_to_decimal_02(self):
        await self.assert_query_result(r'''
            SELECT to_decimal(
                '123456789123456789123456789.123456789123456789123456789',
                'FM999999999999999999999999999.999999999999999999999999999');
        ''', [
            {123456789123456789123456789.123456789123456789123456789},
        ])

    async def test_edgeql_functions_len_01(self):
        await self.assert_query_result(r'''
            SELECT len('');
            SELECT len('hello');
            SELECT len({'hello', 'world'});
        ''', [
            [0],
            [5],
            [5, 5]
        ])

    async def test_edgeql_functions_len_02(self):
        await self.assert_query_result(r'''
            SELECT len(b'');
            SELECT len(b'hello');
            SELECT len({b'hello', b'world'});
        ''', [
            [0],
            [5],
            [5, 5]
        ])

    async def test_edgeql_functions_len_03(self):
        await self.assert_query_result(r'''
            SELECT len(<array<str>>[]);
            SELECT len(['hello']);
            SELECT len(['hello', 'world']);
            SELECT len([1, 2, 3, 4, 5]);
            SELECT len({['hello'], ['hello', 'world']});
        ''', [
            [0],
            [1],
            [2],
            [5],
            {1, 2},
        ])

    async def test_edgeql_functions_min_01(self):
        await self.assert_query_result(r'''
            # numbers
            SELECT min(<int64>{});
            SELECT min(4);
            SELECT min({10, 20, -3, 4});
            SELECT min({10, 2.5, -3.1, 4});

            # strings
            SELECT min({'10', '20', '-3', '4'});
            SELECT min({'10', 'hello', 'world', '-3', '4'});
            SELECT min({'hello', 'world'});

            # arrays
            SELECT min({[1, 2], [3, 4]});
            SELECT min({[1, 2], [3, 4], <array<int64>>[]});
            SELECT min({[1, 2], [1, 0.4]});

            # date and time
            SELECT <str>min(<datetime>{
                '2018-05-07T15:01:22.306916-05',
                '2017-05-07T16:01:22.306916-05',
                '2017-01-07T11:01:22.306916-05',
                '2018-01-07T11:12:22.306916-05',
            });
            SELECT <str>min(<naive_datetime>{
                '2018-05-07T15:01:22.306916',
                '2017-05-07T16:01:22.306916',
                '2017-01-07T11:01:22.306916',
                '2018-01-07T11:12:22.306916',
            });
            SELECT <str>min(<naive_date>{
                '2018-05-07',
                '2017-05-07',
                '2017-01-07',
                '2018-01-07',
            });
            SELECT <str>min(<naive_time>{
                '15:01:22',
                '16:01:22',
                '11:01:22',
                '11:12:22',
            });
            SELECT <str>min(<timedelta>{
                '15:01:22',
                '16:01:22',
                '11:01:22',
                '11:12:22',
            });
        ''', [
            [],
            [4],
            [-3],
            [-3.1],

            ['-3'],
            ['-3'],
            ['hello'],

            [[1, 2]],
            [[]],
            [[1, 0.4]],

            ['2017-01-07T16:01:22.306916+00:00'],
            ['2017-01-07T11:01:22.306916'],
            ['2017-01-07'],
            ['11:01:22'],
            ['11:01:22'],
        ])

    async def test_edgeql_functions_min_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT min(User.name);

            WITH MODULE test
            SELECT min(Issue.time_estimate);

            WITH MODULE test
            SELECT min(<int64>Issue.number);
        ''', [
            ['Elvis'],
            [3000],
            [1],
        ])

    async def test_edgeql_functions_max_01(self):
        await self.assert_query_result(r'''
            # numbers
            SELECT max(<int64>{});
            SELECT max(4);
            SELECT max({10, 20, -3, 4});
            SELECT max({10, 2.5, -3.1, 4});

            # strings
            SELECT max({'10', '20', '-3', '4'});
            SELECT max({'10', 'hello', 'world', '-3', '4'});
            SELECT max({'hello', 'world'});

            # arrays
            SELECT max({[1, 2], [3, 4]});
            SELECT max({[1, 2], [3, 4], <array<int64>>[]});
            SELECT max({[1, 2], [1, 0.4]});

            # date and time
            SELECT <str>max(<datetime>{
                '2018-05-07T15:01:22.306916-05',
                '2017-05-07T16:01:22.306916-05',
                '2017-01-07T11:01:22.306916-05',
                '2018-01-07T11:12:22.306916-05',
            });
            SELECT <str>max(<naive_datetime>{
                '2018-05-07T15:01:22.306916',
                '2017-05-07T16:01:22.306916',
                '2017-01-07T11:01:22.306916',
                '2018-01-07T11:12:22.306916',
            });
            SELECT <str>max(<naive_date>{
                '2018-05-07',
                '2017-05-07',
                '2017-01-07',
                '2018-01-07',
            });
            SELECT <str>max(<naive_time>{
                '15:01:22',
                '16:01:22',
                '11:01:22',
                '11:12:22',
            });
            SELECT <str>max(<timedelta>{
                '15:01:22',
                '16:01:22',
                '11:01:22',
                '11:12:22',
            });
        ''', [
            [],
            [4],
            [20],
            [10],

            ['4'],
            ['world'],
            ['world'],

            [[3, 4]],
            [[3, 4]],
            [[1, 2]],

            ['2018-05-07T20:01:22.306916+00:00'],
            ['2018-05-07T15:01:22.306916'],
            ['2018-05-07'],
            ['16:01:22'],
            ['16:01:22'],
        ])

    async def test_edgeql_functions_max_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT max(User.name);

            WITH MODULE test
            SELECT max(Issue.time_estimate);

            WITH MODULE test
            SELECT max(<int64>Issue.number);
        ''', [
            ['Yury'],
            [3000],
            [4],
        ])

    async def test_edgeql_functions_all_01(self):
        await self.assert_query_result(r'''
            SELECT all(<bool>{});
            SELECT all({True});
            SELECT all({False});
            SELECT all({True, False, True, False});

            SELECT all({1, 2, 3, 4} > 0);
            SELECT all({1, -2, 3, 4} > 0);
            SELECT all({0, -1, -2, -3} > 0);
            # subset evaluation
            SELECT all({1, -2, 3, 4} IN {-2, -1, 0, 1, 2, 3, 4});
            SELECT all(<int64>{} IN {-2, -1, 0, 1, 2, 3, 4});
            SELECT all({1, -2, 3, 4} IN <int64>{});
            SELECT all(<int64>{} IN <int64>{});
        ''', [
            [True],
            [True],
            [False],
            [False],

            [True],
            [False],
            [False],

            [True],
            [True],
            [False],
            [True],
        ])

    async def test_edgeql_functions_all_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT all(len(User.name) = 4);

            WITH MODULE test
            SELECT all(
                (
                    FOR I IN {Issue}
                    UNION EXISTS I.time_estimate
                )
            );

            WITH MODULE test
            SELECT all(Issue.number != '');
        ''', [
            [False],
            [False],
            [True],
        ])

    async def test_edgeql_functions_any_01(self):
        await self.assert_query_result(r'''
            SELECT any(<bool>{});
            SELECT any({True});
            SELECT any({False});
            SELECT any({True, False, True, False});

            SELECT any({1, 2, 3, 4} > 0);
            SELECT any({1, -2, 3, 4} > 0);
            SELECT any({0, -1, -2, -3} > 0);
            # subset evaluation
            SELECT any({1, -2, 3, 4} IN {-2, -1, 0, 1, 2, 3, 4});
            SELECT any(<int64>{} IN {-2, -1, 0, 1, 2, 3, 4});
            SELECT any({1, -2, 3, 4} IN <int64>{});
            SELECT any(<int64>{} IN <int64>{});
        ''', [
            [False],
            [True],
            [False],
            [True],

            [True],
            [True],
            [False],

            [True],
            [False],
            [False],
            [False],
        ])

    async def test_edgeql_functions_any_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT any(len(User.name) = 4);

            WITH MODULE test
            SELECT any(
                (
                    FOR I IN {Issue}
                    UNION EXISTS I.time_estimate
                )
            );

            WITH MODULE test
            SELECT any(Issue.number != '');
        ''', [
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_functions_any_03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT any(len(User.name) = 4) = NOT all(NOT (len(User.name) = 4));

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

            WITH MODULE test
            SELECT any(Issue.number != '') = NOT all(Issue.number = '');
        ''', [
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_functions_round_01(self):
        await self.assert_query_result(r'''
            # trivial
            SELECT round(<float64>{});
            SELECT round(<float64>1);
            SELECT round(<decimal>1);
            SELECT round(<float64>1.2);
            SELECT round(<float64>-1.2);
            SELECT round(<decimal>1.2);
            SELECT round(<decimal>-1.2);
            # float tie is rounded towards even
            SELECT round(<float64>-2.5);
            SELECT round(<float64>-1.5);
            SELECT round(<float64>-0.5);
            SELECT round(<float64>0.5);
            SELECT round(<float64>1.5);
            SELECT round(<float64>2.5);
            # decimal tie is rounded away from 0
            SELECT round(<decimal>-2.5);
            SELECT round(<decimal>-1.5);
            SELECT round(<decimal>-0.5);
            SELECT round(<decimal>0.5);
            SELECT round(<decimal>1.5);
            SELECT round(<decimal>2.5);
        ''', [
            [],
            [1],
            [1],
            [1],
            [-1],
            [1],
            [-1],

            [-2],
            [-2],
            [0],
            [0],
            [2],
            [2],

            [-3],
            [-2],
            [-1],
            [1],
            [2],
            [3],
        ])

    async def test_edgeql_functions_round_02(self):
        await self.assert_query_result(r'''
            SELECT round(<float32>1.2) IS float64;
            SELECT round(<float64>1.2) IS float64;
            SELECT round(1.2) IS float64;
            SELECT round(<decimal>1.2) IS decimal;
            # rounding to a specified decimal place is only defined
            # for decimals
            SELECT round(<decimal>1.2, 0) IS decimal;
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_functions_round_03(self):
        await self.assert_query_result(r'''
            SELECT round(<decimal>123.456, 10);
            SELECT round(<decimal>123.456, 3);
            SELECT round(<decimal>123.456, 2);
            SELECT round(<decimal>123.456, 1);
            SELECT round(<decimal>123.456, 0);
            SELECT round(<decimal>123.456, -1);
            SELECT round(<decimal>123.456, -2);
            SELECT round(<decimal>123.456, -3);
        ''', [
            [123.456],
            [123.456],
            [123.46],
            [123.5],
            [123],
            [120],
            [100],
            [0],
        ])

    async def test_edgeql_functions_round_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT _ := round(<int64>Issue.number / 2)
            ORDER BY _;

            WITH MODULE test
            SELECT _ := round(<decimal>Issue.number / 2)
            ORDER BY _;
        ''', [
            [0, 1, 2, 2],
            [1, 1, 2, 2],
        ])

    async def test_edgeql_functions_find_01(self):
        await self.assert_query_result(r'''
            SELECT find(<str>{}, <str>{});
            SELECT find(<str>{}, 'a');
            SELECT find('qwerty', <str>{});
            SELECT find('qwerty', '');
            SELECT find('qwerty', 'q');
            SELECT find('qwerty', 'qwe');
            SELECT find('qwerty', 'we');
            SELECT find('qwerty', 't');
            SELECT find('qwerty', 'a');
            SELECT find('qwerty', 'azerty');
        ''', [
            {},
            {},
            {},
            {0},
            {0},
            {0},
            {1},
            {4},
            {-1},
            {-1},
        ])

    async def test_edgeql_functions_find_02(self):
        await self.assert_query_result(r'''
            SELECT find(<bytes>{}, <bytes>{});
            SELECT find(<bytes>{}, b'a');
            SELECT find(b'qwerty', <bytes>{});
            SELECT find(b'qwerty', b'');
            SELECT find(b'qwerty', b'q');
            SELECT find(b'qwerty', b'qwe');
            SELECT find(b'qwerty', b'we');
            SELECT find(b'qwerty', b't');
            SELECT find(b'qwerty', b'a');
            SELECT find(b'qwerty', b'azerty');
        ''', [
            {},
            {},
            {},
            {0},
            {0},
            {0},
            {1},
            {4},
            {-1},
            {-1},
        ])

    async def test_edgeql_functions_find_03(self):
        await self.assert_query_result(r'''
            SELECT find(<array<str>>{}, <str>{});
            SELECT find(<array<str>>{}, 'the');
            SELECT find(['the', 'quick', 'brown', 'fox'], <str>{});

            SELECT find(<array<str>>[], 'the');
            SELECT find(['the', 'quick', 'brown', 'fox'], 'the');
            SELECT find(['the', 'quick', 'brown', 'fox'], 'fox');
            SELECT find(['the', 'quick', 'brown', 'fox'], 'jumps');

            SELECT find(['the', 'quick', 'brown', 'fox',
                         'jumps', 'over', 'the', 'lazy', 'dog'],
                        'the');
            SELECT find(['the', 'quick', 'brown', 'fox',
                         'jumps', 'over', 'the', 'lazy', 'dog'],
                        'the', 1);
        ''', [
            {},
            {},
            {},

            {-1},
            {0},
            {3},
            {-1},

            {0},
            {6},
        ])

    async def test_edgeql_functions_str_case_01(self):
        await self.assert_query_result(r'''
            SELECT str_lower({'HeLlO', 'WoRlD!'});
            SELECT str_upper({'HeLlO', 'WoRlD!'});
            SELECT str_title({'HeLlO', 'WoRlD!'});
            SELECT str_lower('HeLlO WoRlD!');
            SELECT str_upper('HeLlO WoRlD!');
            SELECT str_title('HeLlO WoRlD!');
        ''', [
            {'hello', 'world!'},
            {'HELLO', 'WORLD!'},
            {'Hello', 'World!'},
            {'hello world!'},
            {'HELLO WORLD!'},
            {'Hello World!'},
        ])

    async def test_edgeql_functions_str_pad_01(self):
        await self.assert_query_result(r'''
            SELECT str_lpad('Hello', 20);
            SELECT str_lpad('Hello', 20, '>');
            SELECT str_lpad('Hello', 20, '-->');
            SELECT str_rpad('Hello', 20);
            SELECT str_rpad('Hello', 20, '<');
            SELECT str_rpad('Hello', 20, '<--');
        ''', [
            {'               Hello'},
            {'>>>>>>>>>>>>>>>Hello'},
            {'-->-->-->-->-->Hello'},
            {'Hello               '},
            {'Hello<<<<<<<<<<<<<<<'},
            {'Hello<--<--<--<--<--'},
        ])

    async def test_edgeql_functions_str_pad_02(self):
        await self.assert_query_result(r'''
            SELECT str_lpad('Hello', 2);
            SELECT str_lpad('Hello', 2, '>');
            SELECT str_lpad('Hello', 2, '-->');
            SELECT str_rpad('Hello', 2);
            SELECT str_rpad('Hello', 2, '<');
            SELECT str_rpad('Hello', 2, '<--');
        ''', [
            {'He'},
            {'He'},
            {'He'},
            {'He'},
            {'He'},
            {'He'},
        ])

    async def test_edgeql_functions_str_pad_03(self):
        await self.assert_query_result(r'''
            WITH l := {0, 2, 10, 20}
            SELECT len(str_lpad('Hello', l)) = l;

            WITH l := {0, 2, 10, 20}
            SELECT len(str_rpad('Hello', l)) = l;
        ''', [
            [True, True, True, True],
            [True, True, True, True],
        ])

    async def test_edgeql_functions_str_trim_01(self):
        await self.assert_query_result(r'''
            SELECT str_trim('    Hello    ');
            SELECT str_ltrim('    Hello    ');
            SELECT str_rtrim('    Hello    ');
        ''', [
            {'Hello'},
            {'Hello    '},
            {'    Hello'},
        ])

    async def test_edgeql_functions_str_trim_02(self):
        await self.assert_query_result(r'''
            SELECT str_ltrim('               Hello', ' <->');
            SELECT str_ltrim('>>>>>>>>>>>>>>>Hello', ' <->');
            SELECT str_ltrim('-->-->-->-->-->Hello', ' <->');
            SELECT str_rtrim('Hello               ', ' <->');
            SELECT str_rtrim('Hello<<<<<<<<<<<<<<<', ' <->');
            SELECT str_rtrim('Hello<--<--<--<--<--', ' <->');
            SELECT str_trim('-->-->-->-->-->Hello<--<--<--<--<--', ' <->');
        ''', [
            {'Hello'},
            {'Hello'},
            {'Hello'},
            {'Hello'},
            {'Hello'},
            {'Hello'},
            {'Hello'},
        ])

    async def test_edgeql_functions_str_trim_03(self):
        await self.assert_query_result(r'''
            SELECT str_trim(str_lpad('Hello', 20), ' <->');
            SELECT str_trim(str_lpad('Hello', 20, '>'), ' <->');
            SELECT str_trim(str_lpad('Hello', 20, '-->'), ' <->');
            SELECT str_trim(str_rpad('Hello', 20), ' <->');
            SELECT str_trim(str_rpad('Hello', 20, '<'), ' <->');
            SELECT str_trim(str_rpad('Hello', 20, '<--'), ' <->');
        ''', [
            {'Hello'},
            {'Hello'},
            {'Hello'},
            {'Hello'},
            {'Hello'},
            {'Hello'},
        ])

    async def test_edgeql_functions_math_abs_01(self):
        await self.assert_query_result(r'''
            SELECT math::abs(2);
            SELECT math::abs(-2);
            SELECT math::abs(2.5);
            SELECT math::abs(-2.5);
            SELECT math::abs(<decimal>2.5);
            SELECT math::abs(<decimal>-2.5);
        ''', [
            {2},
            {2},
            {2.5},
            {2.5},
            {2.5},
            {2.5},
        ])

    async def test_edgeql_functions_math_abs_02(self):
        await self.assert_query_result(r'''
            SELECT math::abs(<int16>2) IS int16;
            SELECT math::abs(<int32>2) IS int32;
            SELECT math::abs(<int64>2) IS int64;
            SELECT math::abs(<float32>2) IS float32;
            SELECT math::abs(<float64>2) IS float64;
            SELECT math::abs(<decimal>2) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    async def test_edgeql_functions_math_ceil_01(self):
        await self.assert_query_result(r'''
            SELECT math::ceil(2);
            SELECT math::ceil(2.5);
            SELECT math::ceil(-2.5);
            SELECT math::ceil(<decimal>2.5);
            SELECT math::ceil(<decimal>-2.5);
        ''', [
            {2},
            {3},
            {-2},
            {3},
            {-2},
        ])

    async def test_edgeql_functions_math_ceil_02(self):
        await self.assert_query_result(r'''
            SELECT math::ceil(<int16>2) IS float64;
            SELECT math::ceil(<int32>2) IS float64;
            SELECT math::ceil(<int64>2) IS float64;
            SELECT math::ceil(<float32>2.5) IS float64;
            SELECT math::ceil(<float64>2.5) IS float64;
            SELECT math::ceil(<decimal>2.5) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    async def test_edgeql_functions_math_floor_01(self):
        await self.assert_query_result(r'''
            SELECT math::floor(2);
            SELECT math::floor(2.5);
            SELECT math::floor(-2.5);
            SELECT math::floor(<decimal>2.5);
            SELECT math::floor(<decimal>-2.5);
        ''', [
            {2},
            {2},
            {-3},
            {2},
            {-3},
        ])

    async def test_edgeql_functions_math_floor_02(self):
        await self.assert_query_result(r'''
            SELECT math::floor(<int16>2) IS float64;
            SELECT math::floor(<int32>2) IS float64;
            SELECT math::floor(<int64>2) IS float64;
            SELECT math::floor(<float32>2.5) IS float64;
            SELECT math::floor(<float64>2.5) IS float64;
            SELECT math::floor(<decimal>2.5) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    async def test_edgeql_functions_math_log_01(self):
        await self.assert_query_result(r'''
            SELECT math::ln({1, 10, 32});
            SELECT math::lg({1, 10, 32});
            SELECT math::log(<decimal>{1, 10, 32}, base := <decimal>2);
        ''', [
            {0, 2.30258509299405, 3.46573590279973},
            {0, 1, 1.50514997831991},
            {0, 3.321928094887362, 5},
        ])

    async def test_edgeql_functions_math_log_02(self):
        await self.assert_query_result(r'''
            SELECT math::ln(<int16>2) IS float64;
            SELECT math::ln(<int32>2) IS float64;
            SELECT math::ln(<int64>2) IS float64;
            SELECT math::ln(<float32>2) IS float64;
            SELECT math::ln(<float64>2) IS float64;
            SELECT math::ln(<decimal>2) IS decimal;

            SELECT math::lg(<int16>2) IS float64;
            SELECT math::lg(<int32>2) IS float64;
            SELECT math::lg(<int64>2) IS float64;
            SELECT math::lg(<float32>2) IS float64;
            SELECT math::lg(<float64>2) IS float64;
            SELECT math::lg(<decimal>2) IS decimal;

            SELECT math::log(<decimal>2, base := <decimal>1.3) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},

            {True},
            {True},
            {True},
            {True},
            {True},
            {True},

            {True},
        ])

    async def test_edgeql_functions_math_mean_01(self):
        await self.assert_query_result(r'''
            SELECT math::mean(1);
            SELECT math::mean(1.5);
            SELECT math::mean({1, 2, 3});
            SELECT math::mean({1, 2, 3, 4});
            SELECT math::mean({0.1, 0.2, 0.3});
            SELECT math::mean({0.1, 0.2, 0.3, 0.4});
            SELECT math::mean({1, 99999999999999999999999999});
        ''', [
            {1.0},
            {1.5},
            {2.0},
            {2.5},
            {0.2},
            {0.25},
            {50000000000000000000000000},
        ])

    async def test_edgeql_functions_math_mean_02(self):
        await self.assert_query_result(r'''
            # int16 is implicitly cast in float32, which produces a
            # float64 result
            SELECT math::mean(<int16>2) IS float64;
            SELECT math::mean(<int32>2) IS float64;
            SELECT math::mean(<int64>2) IS float64;
            SELECT math::mean(<float32>2) IS float64;
            SELECT math::mean(<float64>2) IS float64;
            SELECT math::mean(<decimal>2) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    async def test_edgeql_functions_math_mean_03(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                A := {1, 3, 1}
            # the difference between sum and mean * count is due to
            # rounding errors, but it should be small
            SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
        ''', [
            {True},
        ])

    async def test_edgeql_functions_math_mean_04(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                A := <float64>{1, 3, 1}
            # the difference between sum and mean * count is due to
            # rounding errors, but it should be small
            SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
        ''', [
            {True},
        ])

    async def test_edgeql_functions_math_mean_05(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                A := len(test::Named.name)
            # the difference between sum and mean * count is due to
            # rounding errors, but it should be small
            SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
        ''', [
            {True},
        ])

    async def test_edgeql_functions_math_mean_06(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                A := <float64>len(test::Named.name)
            # the difference between sum and mean * count is due to
            # rounding errors, but it should be small
            SELECT abs(sum(A) - count(A) * mean(A)) < 1e-10;
        ''', [
            {True},
        ])

    async def test_edgeql_functions_math_mean_07(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                A := {3}
            SELECT mean(A) * count(A);
        ''', [
            {3},
        ])

    async def test_edgeql_functions_math_mean_08(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                X := {1, 2, 3, 4}
            SELECT mean(X) = sum(X) / count(X);

            WITH
                MODULE math,
                X := {0.1, 0.2, 0.3, 0.4}
            SELECT mean(X) = sum(X) / count(X);
        ''', [
            {True},
            {True},
        ])

    @test.not_implemented(
        "We don't yet validate that the return cardinality is 1 here")
    async def test_edgeql_functions_math_mean_09(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"mean in undefined for an empty set"):
            await self.query(r'''
                SELECT math::mean(<int64>{});
            ''')

    async def test_edgeql_functions_math_stddev_01(self):
        await self.assert_query_result(r'''
            SELECT math::stddev({1, 1});
            SELECT math::stddev({1, 1, -1, 1});
            SELECT math::stddev({1, 2, 3});
            SELECT math::stddev({0.1, 0.1, -0.1, 0.1});
            SELECT math::stddev(<decimal>{0.1, 0.2, 0.3});
        ''', [
            {0},
            {1.0},
            {1.0},
            {0.1},
            {0.1},
        ])

    async def test_edgeql_functions_math_stddev_02(self):
        await self.assert_query_result(r'''
            SELECT math::stddev(<int16>{1, 1}) IS float64;
            SELECT math::stddev(<int32>{1, 1}) IS float64;
            SELECT math::stddev(<int64>{1, 1}) IS float64;
            SELECT math::stddev(<float32>{1, 1}) IS float64;
            SELECT math::stddev(<float64>{1, 1}) IS float64;
            SELECT math::stddev(<decimal>{1, 1}) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    @test.not_implemented('''
        We don't yet validate that the return cardinality is 1 here.
        Standard deviation is not defined for sets with fewer than 2 elements.
    ''')
    async def test_edgeql_functions_math_stddev_03(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"stddev in undefined for input set.+< 2"):
            await self.query(r'''
                SELECT math::stddev(<int64>{});
            ''')

    @test.not_implemented('''
        We don't yet validate that the return cardinality is 1 here.
        Standard deviation is not defined for sets with fewer than 2 elements.
    ''')
    async def test_edgeql_functions_math_stddev_04(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"stddev in undefined for input set.+< 2"):
            await self.query(r'''
                SELECT math::stddev(1);
            ''')

    async def test_edgeql_functions_math_stddev_pop_01(self):
        await self.assert_query_result(r'''
            SELECT math::stddev_pop(1);
            SELECT math::stddev_pop({1, 1, 1});
            SELECT math::stddev_pop({1, 2, 1, 2});
            SELECT math::stddev_pop({0.1, 0.1, 0.1});
            SELECT math::stddev_pop({0.1, 0.2, 0.1, 0.2});
        ''', [
            {0},
            {0},
            {0.5},
            {0},
            {0.05},
        ])

    async def test_edgeql_functions_math_stddev_pop_02(self):
        await self.assert_query_result(r'''
            SELECT math::stddev_pop(<int16>1) IS float64;
            SELECT math::stddev_pop(<int32>1) IS float64;
            SELECT math::stddev_pop(<int64>1) IS float64;
            SELECT math::stddev_pop(<float32>1) IS float64;
            SELECT math::stddev_pop(<float64>1) IS float64;
            SELECT math::stddev_pop(<decimal>1) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    @test.not_implemented('''
        We don't yet validate that the return cardinality is 1 here.
        Population standard deviation is not defined for an empty set.
    ''')
    async def test_edgeql_functions_math_stddev_pop_04(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"stddev_pop in undefined for an empty set"):
            await self.query(r'''
                SELECT math::stddev_pop(<int64>{});
            ''')

    async def test_edgeql_functions_math_var_01(self):
        await self.assert_query_result(r'''
            SELECT math::var({1, 1});
            SELECT math::var({1, 1, -1, 1});
            SELECT math::var({1, 2, 3});
            SELECT math::var({0.1, 0.1, -0.1, 0.1});
            SELECT math::var(<decimal>{0.1, 0.2, 0.3});
        ''', [
            {0},
            {1.0},
            {1.0},
            {0.01},
            {0.01},
        ])

    async def test_edgeql_functions_math_var_02(self):
        await self.assert_query_result(r'''
            # int16 is implicitly cast in float32, which produces a
            # float64 result
            SELECT math::var(<int16>{1, 1}) IS float64;
            SELECT math::var(<int32>{1, 1}) IS float64;
            SELECT math::var(<int64>{1, 1}) IS float64;
            SELECT math::var(<float32>{1, 1}) IS float64;
            SELECT math::var(<float64>{1, 1}) IS float64;
            SELECT math::var(<decimal>{1, 1}) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    async def test_edgeql_functions_math_var_03(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                X := {1, 1}
            SELECT var(X) = stddev(X) ^ 2;

            WITH
                MODULE math,
                X := {1, 1, -1, 1}
            SELECT var(X) = stddev(X) ^ 2;

            WITH
                MODULE math,
                X := {1, 2, 3}
            SELECT var(X) = stddev(X) ^ 2;

            WITH
                MODULE math,
                X := {0.1, 0.1, -0.1, 0.1}
            SELECT var(X) = stddev(X) ^ 2;

            WITH
                MODULE math,
                X := <decimal>{0.1, 0.2, 0.3}
            SELECT var(X) = stddev(X) ^ 2;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    @test.not_implemented('''
        We don't yet validate that the return cardinality is 1 here.
        Variance is not defined for sets with fewer than 2 elements.
    ''')
    async def test_edgeql_functions_math_var_04(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"var in undefined for input set.+< 2"):
            await self.query(r'''
                SELECT math::var(<int64>{});
            ''')

    @test.not_implemented('''
        We don't yet validate that the return cardinality is 1 here.
        Variance is not defined for sets with fewer than 2 elements.
    ''')
    async def test_edgeql_functions_math_var_05(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"var in undefined for input set.+< 2"):
            await self.query(r'''
                SELECT math::var(1);
            ''')

    async def test_edgeql_functions_math_var_pop_01(self):
        await self.assert_query_result(r'''
            SELECT math::var_pop(1);
            SELECT math::var_pop({1, 1, 1});
            SELECT math::var_pop({1, 2, 1, 2});
            SELECT math::var_pop({0.1, 0.1, 0.1});
            SELECT math::var_pop({0.1, 0.2, 0.1, 0.2});
        ''', [
            {0},
            {0},
            {0.25},
            {0},
            {0.0025},
        ])

    async def test_edgeql_functions_math_var_pop_02(self):
        await self.assert_query_result(r'''
            SELECT math::var_pop(<int16>1) IS float64;
            SELECT math::var_pop(<int32>1) IS float64;
            SELECT math::var_pop(<int64>1) IS float64;
            SELECT math::var_pop(<float32>1) IS float64;
            SELECT math::var_pop(<float64>1) IS float64;
            SELECT math::var_pop(<decimal>1) IS decimal;
        ''', [
            {True},
            {True},
            {True},
            {True},
            {True},
            {True},
        ])

    async def test_edgeql_functions_math_var_pop_03(self):
        await self.assert_query_result(r'''
            WITH
                MODULE math,
                X := {1, 2, 1, 2}
            SELECT var_pop(X) = stddev_pop(X) ^ 2;

            WITH
                MODULE math,
                X := {0.1, 0.2, 0.1, 0.2}
            SELECT var_pop(X) = stddev_pop(X) ^ 2;
        ''', [
            {True},
            {True},
        ])

    @test.not_implemented('''
        We don't yet validate that the return cardinality is 1 here.
        Population variance is not defined for an empty set.
    ''')
    async def test_edgeql_functions_math_var_pop_04(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"var_pop in undefined for an empty set"):
            await self.query(r'''
                SELECT math::var_pop(<int64>{});
            ''')
