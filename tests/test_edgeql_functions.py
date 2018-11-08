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
import unittest  # NOQA

from edb.client import exceptions as exc
from edb.server import _testbase as tb


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
        res = await self.con.execute('''
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
        res = await self.con.execute('''
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
                exc.EdgeQLError,
                r'could not determine expression type'):

            await self.con.execute("""
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
                exc.SchemaError,
                r"nested arrays are not supported"):
            await self.con.execute(r"""
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
                exc.SchemaError,
                r"nested arrays are not supported"):
            await self.con.execute(r'''
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

    @unittest.expectedFailure
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
                exc.EdgeQLError,
                r'could not find a function variant array_get'):

            await self.con.execute(r'''
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

    async def test_edgeql_functions_datetime_current_01(self):
        dt = (await self.con.execute('SELECT datetime_current();'))[0][0]
        self.assertRegex(dt, r'\d+-\d+-\d+T\d+:\d+:\d+\.\d+.*')

    async def test_edgeql_functions_datetime_current_02(self):
        res = await self.con.execute(r'''
            START TRANSACTION;

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

            COMMIT;
        ''')

        batch1 = res[1]
        batch2 = res[2]
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
            exc.UnknownEdgeDBError,
                'timestamp units "timezone_hour" not supported'):
            await self.con.execute('''
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
