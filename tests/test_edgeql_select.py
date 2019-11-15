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

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLSelect(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.edgeql')

    async def test_edgeql_select_unique_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue.watchers.<owner[IS Issue] {
                    name
                } ORDER BY .name;
            ''',
            [{
                'name': 'Improve EdgeDB repl output rendering.',
            }, {
                'name': 'Regression.',
            }, {
                'name': 'Release EdgeDB',
            }, {
                'name': 'Repl tweak.',
            }]
        )

    async def test_edgeql_select_unique_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue.owner{name}
            ORDER BY Issue.owner.name;
            ''',
            [
                {'name': 'Elvis'}, {'name': 'Yury'},
            ]
        )

    async def test_edgeql_select_computable_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    aliased_number := Issue.number,
                    total_time_spent := (
                        SELECT sum(Issue.time_spent_log.spent_time)
                    )
                }
            FILTER
                Issue.number = '1';
            ''',
            [{
                'number': '1',
                'aliased_number': '1',
                'total_time_spent': 50000
            }]
        )

    async def test_edgeql_select_computable_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    total_time_spent := (
                        SELECT sum(Issue.time_spent_log.spent_time)
                    )
                }
            FILTER
                Issue.number = '1';
            ''',
            [{
                'number': '1',
                'total_time_spent': 50000
            }]
        )

    async def test_edgeql_select_computable_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    shortest_own_text := (
                        SELECT
                            Text {
                                body
                            }
                        FILTER
                            Text[IS Owned].owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT 1
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Rewriting everything.',
                },
            }]
        )

    async def test_edgeql_select_computable_04(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                # we aren't referencing User in any way, so this works
                # best as a subquery, rather than inline computable
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT 1
                )
            SELECT
                User {
                    name,
                    shortest_text := sub {
                        body
                    }
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        )

    async def test_edgeql_select_computable_05(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                # we aren't referencing User in any way, so this works
                # best as a subquery, than inline computable
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                User {
                    name,
                    shortest_own_text := (
                        SELECT
                            Text {body}
                        FILTER
                            Text[IS Owned].owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT
                            1
                    ),
                    shortest_text := sub {
                        body
                    },
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Rewriting everything.',
                },
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        )

    async def test_edgeql_select_computable_06(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    shortest_text := (
                        SELECT
                            Text {body}
                        # a clause that references User and is always true
                        FILTER
                            User IS User
                        ORDER
                            BY len(Text.body) ASC
                        LIMIT 1
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        )

    async def test_edgeql_select_computable_07(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    # ad-hoc computable with many results
                    special_texts := (
                        SELECT Text {body}
                        FILTER Text[IS Owned].owner != User
                        ORDER BY len(Text.body) DESC
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'special_texts': [
                    {'body': 'We need to be able to render data in '
                             'tabular format.'},
                    {'body': 'Minor lexer tweaks.'}
                ],
            }]
        )

    async def test_edgeql_select_computable_08(self):
        await self.assert_query_result(
            r"""
            # get a user + the latest issue (regardless of owner), which has
            # the same number of characters in the status as the user's name
            WITH MODULE test
            SELECT User{
                name,
                special_issue := (
                    SELECT Issue {
                        name,
                        number,
                        owner: {
                            name
                        },
                        status: {
                            name
                        }
                    }
                    FILTER len(Issue.status.name) = len(User.name)
                    ORDER BY Issue.number DESC
                    LIMIT 1
                )
            }
            ORDER BY User.name;
            """,
            [
                {
                    'name': 'Elvis',
                    'special_issue': None
                }, {
                    'name': 'Yury',
                    'special_issue': {
                        'name': 'Improve EdgeDB repl output rendering.',
                        'owner': {'name': 'Yury'},
                        'status': {'name': 'Open'},
                        'number': '2'
                    },
                }
            ],
        )

    async def test_edgeql_select_computable_09(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Text{
                body,
                name := Text[IS Issue].name IF Text IS Issue      ELSE
                        'log'                IF Text IS LogEntry   ELSE
                        'comment'            IF Text IS Comment    ELSE
                        'unknown'
            }
            ORDER BY Text.body;
            """,
            [
                {'body': 'EdgeDB needs to happen soon.',
                 'name': 'comment'},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'Rewriting everything.',
                 'name': 'log'},
                {'body': 'We need to be able to render data in '
                         'tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ],
        )

    async def test_edgeql_select_computable_10(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue{
                name,
                number,
                # use shorthand with some simple operations
                foo := <int64>Issue.number + 10,
            }
            FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'number': '1',
                'foo': 11,
            }],
        )

    async def test_edgeql_select_computable_11(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub.body;
            ''',
            ['Minor lexer tweaks.']
        )

    async def test_edgeql_select_computable_12(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub.__type__.name;
            ''',
            ['test::Issue']
        )

    async def test_edgeql_select_computable_13(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub[IS Issue].number;
            ''',
            ['3']
        )

    async def test_edgeql_select_computable_14(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue{
                name,
                number,
                # Explicit cardinality override
                multi foo := <int64>Issue.number + 10,
                }
                FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'number': '1',
                'foo': [11],
            }],
        )

    async def test_edgeql_select_computable_15(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly more than one element returned by an expression "
                r"for a computable property 'foo' declared as 'single'",
                _position=199):
            await self.con.fetchall("""\
                WITH MODULE test
                SELECT Issue{
                    name,
                    number,
                    # Explicit erroneous cardinality override
                    single foo := {1, 2}
                }
                FILTER Issue.number = '1';
            """)

    async def test_edgeql_select_computable_16(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue{
                name,
                number,
                single foo := <int64>{},
                single bar := 11,
            }
            FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'number': '1',
                'foo': None,
                'bar': 11,
            }]
        )

    async def test_edgeql_select_computable_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly more than one element returned by an expression "
                r"for a computable property 'foo' declared as 'single'",
                _position=248):
            await self.con.fetchall("""\
                WITH
                    MODULE test,
                    V := (SELECT Issue {
                        foo := {1, 2}
                    } FILTER .number = '1')
                SELECT
                    V {
                        single foo := .foo
                    };
            """)

    async def test_edgeql_select_match_01(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%Edge%'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '2'}],
        )

    async def test_edgeql_select_match_02(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%Edge%'
            ORDER BY Issue.number;
            """,
            [{'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_match_03(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%re%'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

    async def test_edgeql_select_match_04(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%re%'
            ORDER BY Issue.number;
            """,
            [],
        )

    async def test_edgeql_select_match_07(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                re_test('ed', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                re_test('eD', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Initial public release of EdgeDB.'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                re_test(r'ed([S\s]|$)', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}]
        )

    async def test_edgeql_select_match_08(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                re_test('(?i)ed', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                re_test('(?i)eD', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                re_test(r'(?i)ed([S\s]|$)', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

    async def test_edgeql_select_type_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    __type__: {
                        name
                    }
                }
            FILTER
                Issue.number = '1';
            ''',
            [{
                'number': '1',
                '__type__': {'name': 'test::Issue'},
            }],
        )

    async def test_edgeql_select_type_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT User.__type__.name LIMIT 1;
            ''',
            ['test::User']
        )

    async def test_edgeql_select_type_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'invalid property reference'):
            await self.con.fetchall(r'''
                WITH MODULE test
                SELECT User.name.__type__.name LIMIT 1;
            ''')

    async def test_edgeql_select_type_04(self):
        # Make sure that the __type__ attribute gets the same object
        # as a direct schema::ObjectType query. As long as this is true,
        # we can test the schema separately without any other data.
        res = await self.con.fetchone(r'''
            WITH MODULE test
            SELECT User {
                __type__: {
                    name,
                    id,
                }
            } LIMIT 1;
        ''')

        await self.assert_query_result(
            r'''
            WITH MODULE schema
            SELECT `ObjectType` {
                name,
                id,
            } FILTER `ObjectType`.name = 'test::User';
            ''',
            [{
                'name': res.__type__.name,
                'id': str(res.__type__.id),
            }]
        )

    async def test_edgeql_select_type_05(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT User.__type__ { name };
            ''',
            [{
                'name': 'test::User'
            }]
        )

    @test.not_implemented('recursive queries are not implemented')
    async def test_edgeql_select_recursive_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    related_to: {
                        number,
                    },
                }
            FILTER
                Issue.number = '2';
            ''',
            [{
                'number': '3',
                'related_to': [{
                    'number': '2',
                }]
            }],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    related_to *1
                }
            FILTER
                Issue.number = '2';
            ''',
            [{
                'number': '3',
                'related_to': [{
                    'number': '2',
                }]
            }],
        )

    async def test_edgeql_select_limit_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT 3;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2 LIMIT 3;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_limit_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 1 + 1;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT 6 // 2;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 4 - 2 LIMIT 5 * 2 - 7;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_limit_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET (SELECT count(Status));
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT (SELECT count(Status) + 1);
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET (SELECT count(Status))
            LIMIT (SELECT count(Priority) + 1);
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_limit_04(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    owner_of := (
                        SELECT User.<owner[IS Issue] {
                            number
                        } ORDER BY .number
                        LIMIT 1
                    )
                }
            ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': {'number': '1'},
                },
                {
                    'name': 'Yury',
                    'owner_of': {'number': '2'},
                }
            ]
        )

    async def test_edgeql_select_limit_05(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    owner_of := (
                        SELECT User.<owner[IS Issue] {
                            number
                        } ORDER BY .number
                        LIMIT len(User.name) - 3
                    )
                }
            ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': [{'number': '1'}, {'number': '4'}],
                },
                {
                    'name': 'Yury',
                    'owner_of': [{'number': '2'}],
                }
            ]
        )

    async def test_edgeql_select_limit_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            await self.con.fetchall("""
                WITH MODULE test
                SELECT
                    User { name }
                LIMIT <int64>User.<owner[IS Issue].number;
            """)

    async def test_edgeql_select_limit_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            await self.con.fetchall("""
                WITH MODULE test
                SELECT
                    User { name }
                OFFSET <int64>User.<owner[IS Issue].number;
            """)

    async def test_edgeql_select_polymorphic_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Text {body}
            ORDER BY Text.body;
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render data '
                         'in tabular format.'}
            ],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Text {
                    [IS Issue].name,
                    body,
                }
            ORDER BY Text.body;
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.',
                 'name': None},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'Rewriting everything.',
                 'name': None},
                {'body': 'We need to be able to render data in '
                         'tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ]
        )

    async def test_edgeql_select_polymorphic_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT User{
                name,
                owner_of := User.<owner[IS LogEntry] {
                    body
                },
            } FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'owner_of': [
                    {'body': 'Rewriting everything.'}
                ],
            }],
        )

    async def test_edgeql_select_polymorphic_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT User{
                name,
                owner_of := (
                    SELECT User.<owner[IS Issue] {
                        number
                    } FILTER <int64>(.number) < 3
                ),
            } FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'owner_of': [
                    {'number': '1'},
                ],
            }],
        )

    async def test_edgeql_select_polymorphic_04(self):
        # Since using a polymorphic shape element means that sometimes
        # that element may be empty, it is prohibited to access
        # protected property such as `id` on it as that would be
        # equivalent to re-writing it.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot access id on a polymorphic shape element'):
            await self.con.fetchall(r'''
                WITH MODULE test
                SELECT User {
                    [IS Named].id,
                };
            ''')

    async def test_edgeql_select_polymorphic_05(self):
        # Since using a polymorphic shape element means that sometimes
        # that element may be empty, it is prohibited to access
        # protected link such as `__type__` on it as that would be
        # equivalent to re-writing it.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot access __type__ on a polymorphic shape element'):
            await self.con.fetchall(r'''
                WITH MODULE test
                SELECT User {
                    [IS Named].__type__: {
                        name
                    },
                };
            ''')

    async def test_edgeql_select_polymorphic_06(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Object[IS Status].name;
            ''',
            {
                'Closed',
                'Open',
            },
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Object[IS Priority].name;
            ''',
            {
                'High',
                'Low',
            },
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Object[IS Status].name ?? Object[IS Priority].name;
            ''',
            {
                'Closed',
                'High',
                'Low',
                'Open',
            },
        )

    @test.not_implemented('type expressions are not implemented')
    async def test_edgeql_select_polymorphic_07(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Object[IS Status | Priority].name;
            # the above should be equivalent to this:
            # SELECT Object[IS Status].name ?? Object[IS Priority].name;
            ''',
            {
                'Closed',
                'High',
                'Low',
                'Open',
            },
        )

    @test.not_implemented('type expressions are not implemented')
    async def test_edgeql_select_polymorphic_08(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Object {
                [IS Status | Priority].name,
            } ORDER BY .name;
            ''',
            [
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': 'Closed'},
                {'name': 'High'},
                {'name': 'Low'},
                {'name': 'Open'}
            ],
        )

        await self.assert_query_result(
            r'''
            # the above should be equivalent to this:
            WITH MODULE test
            SELECT Object {
                name := Object[IS Status].name ?? Object[IS Priority].name,
            } ORDER BY .name;
            ''',
            [
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': 'Closed'},
                {'name': 'High'},
                {'name': 'Low'},
                {'name': 'Open'}
            ],
        )

    async def test_edgeql_select_polymorphic_09(self):
        # Test simultaneous type indirection on source and target
        # of a shape element.
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Named {
                name,
                [IS Issue].references: File {
                    name
                }
            }
            FILTER .name ILIKE '%edgedb%'
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'Improve EdgeDB repl output rendering.',
                    'references': [{'name': 'screenshot.png'}],
                },
                {
                    'name': 'Release EdgeDB',
                    'references': [],
                },
                {
                    'name': 'edgedb.com',
                    'references': [],
                },
            ],
        )

    async def test_edgeql_select_polymorphic_10(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                count(Object[IS Named][IS Text])
                != count(Object[IS Text]);
            ''',
            [True]
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                count(User.<owner[IS Named][IS Text])
                != count(User.<owner[IS Text]);
            ''',
            [True]
        )

    async def test_edgeql_select_view_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{
                number,
                related_to: {
                    number
                } FILTER Issue.related_to.owner = Issue.owner,
            } ORDER BY Issue.number;
            ''',
            [
                {
                    'number': '1',
                    'related_to': []
                },
                {
                    'number': '2',
                    'related_to': []
                },
                {
                    'number': '3',
                    'related_to': [
                        {'number': '2'}
                    ]
                },
                {
                    'number': '4',
                    'related_to': []
                }
            ],
        )

    async def test_edgeql_select_view_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT User{
                name,
                owner_of := (
                    SELECT User.<owner[IS Issue] {
                        number
                    } FILTER EXISTS .related_to
                ),
            } ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': [{
                        'number': '4'
                    }]
                }, {
                    'name': 'Yury',
                    'owner_of': [{
                        'number': '3'
                    }]
                }
            ],
        )

    async def test_edgeql_select_view_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT User{
                name,
                owner_of := (
                    SELECT User.<owner[IS Issue] {
                        number
                    } ORDER BY .number DESC
                ),
            } ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': [{
                        'number': '4'
                    }, {
                        'number': '1'
                    }]
                }, {
                    'name': 'Yury',
                    'owner_of': [{
                        'number': '3'
                    }, {
                        'number': '2'
                    }]
                }
            ],
        )

    async def test_edgeql_select_view_04(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                L := LogEntry   # there happens to only be 1 entry
            SELECT
                # define a view that assigns a log to every Issue
                Issue {
                    tsl := (Issue.time_spent_log ?? L)
                }.tsl {
                    body
                };
            """,
            [
                # no duplicates are possible, because the expression
                # is a path pointing to an object
                {'body': 'Rewriting everything.'},
            ],
        )

    async def test_edgeql_select_view_05(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue.owner {
                name,
                # this path extends `Issue.owner` from top scope
                foo := Issue.owner.<owner[IS Issue]{
                    number,
                    # this path *also* extends `Issue.owner` from top scope
                    bar := Issue.owner.name
                }
            };
            """,
            [
                {
                    'name': 'Elvis',
                    'foo': [
                        {'bar': 'Elvis', 'number': '1'},
                        {'bar': 'Elvis', 'number': '4'}
                    ],
                },
                {
                    'name': 'Yury',
                    'foo': [
                        {'bar': 'Yury', 'number': '2'},
                        {'bar': 'Yury', 'number': '3'}
                    ],
                },
            ],
        )

    async def test_edgeql_select_view_06(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT User {
                name,
                foo := (
                    SELECT (
                        SELECT Status
                        FILTER Status.name = 'Open'
                    ).name
                )
            } FILTER User.name = 'Elvis';
            """,
            [
                {
                    'name': 'Elvis',
                    'foo': 'Open',
                },
            ],
        )

    async def test_edgeql_select_view_07(self):
        await self.assert_query_result(
            r"""
            # semantically identical to the previous test
            WITH MODULE test
            SELECT User {
                name,
                foo := {
                    (
                        SELECT Status
                        FILTER Status.name = 'Open'
                    ).name
                }
            } FILTER User.name = 'Elvis';
            # FIXME: please also fix the error message to be less
            # arcane with some sort of reference to where things go
            # wrong in the query
            """,
            [
                {
                    'name': 'Elvis',
                    'foo': 'Open',
                },
            ],
        )

    async def test_edgeql_select_view_08(self):
        await self.assert_query_result(
            r"""
            # semantically similar to previous test, but involving
            # schema (since schema often has special handling)
            WITH MODULE test
            SELECT User {
                name,
                foo := {
                    (
                        SELECT schema::ObjectType
                        FILTER schema::ObjectType.name = 'test::User'
                    ).name
                }
            } FILTER User.name = 'Elvis';
            """,
            [
                {
                    'name': 'Elvis',
                    'foo': ['test::User'],
                },
            ],
        )

    async def test_edgeql_select_instance_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Text {body}
            FILTER Text IS Comment
            ORDER BY Text.body;
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
        )

    async def test_edgeql_select_instance_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Text {body}
            FILTER Text IS NOT Comment | Issue
            ORDER BY Text.body;
            ''',
            [
                {'body': 'Rewriting everything.'},
            ],
        )

    async def test_edgeql_select_instance_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Text {body}
            FILTER Text IS Issue AND Text[IS Issue].number = '1'
            ORDER BY Text.body;
            ''',
            [
                {'body': 'Initial public release of EdgeDB.'},
            ],
        )

    async def test_edgeql_select_setops_01(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                (Issue UNION Comment) {
                    [IS Issue].name,  # name is not in the duck type
                    body  # body should appear in the duck type
                };
            """,
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'We need to be able to render data '
                         'in tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ],
            sort=lambda x: x['body']
        )

    async def test_edgeql_select_setops_02(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                Obj := (SELECT Issue UNION Comment)
            SELECT Obj {
                [IS Issue].name,
                [IS Text].body
            };
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'We need to be able to render '
                         'data in tabular format.'}
            ],
            sort=lambda x: x['body']
        )

        await self.assert_query_result(
            r'''
            # XXX: I think we should be able to drop [IS Text] from
            # the query below.
            WITH
                MODULE test,
                Obj := (SELECT Issue UNION Comment)
            SELECT Obj[IS Text] { id, body }
            ORDER BY Obj[IS Text].body;
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'We need to be able to render '
                         'data in tabular format.'}
            ],
        )

    async def test_edgeql_select_setops_03(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue {
                number,
                # open := 'yes' IF Issue.status.name = 'Open' ELSE 'no'
                # equivalent to
                open := (SELECT (
                    (SELECT 'yes' FILTER Issue.status.name = 'Open')
                    UNION
                    (SELECT 'no' FILTER NOT Issue.status.name = 'Open')
                ) LIMIT 1)
            }
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'open': 'yes',
            }, {
                'number': '2',
                'open': 'yes',
            }, {
                'number': '3',
                'open': 'no',
            }, {
                'number': '4',
                'open': 'no',
            }],
        )

    async def test_edgeql_select_setops_04(self):
        await self.assert_query_result(
            r"""
            # equivalent to ?=
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                # Issue.priority.name ?= 'High'
                # equivalent to this via an if/else translation
                (SELECT Issue.priority.name = 'High'
                 FILTER EXISTS Issue.priority.name)
                UNION
                (SELECT EXISTS Issue.priority.name = TRUE
                 FILTER NOT EXISTS Issue.priority.name)
            ORDER BY Issue.number;
            """,
            [{'number': '2'}],
        )

    async def test_edgeql_select_setops_05(self):
        await self.assert_query_result(
            r"""
            # using DISTINCT on a UNION with overlapping sets of Objects
            WITH MODULE test
            SELECT _ := (
                DISTINCT ((
                    # Issue 1, 4
                    (SELECT User
                     FILTER User.name = 'Elvis').<owner[IS Issue]
                ) UNION (
                    # Issue 1
                    (SELECT User
                     FILTER User.name = 'Yury').<watchers[IS Issue]
                ) UNION (
                    # Issue 1, 4
                    SELECT Issue
                    FILTER NOT EXISTS Issue.priority
                ))
            ) { number }
            ORDER BY _.number;
            """,
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_setops_06(self):
        await self.assert_query_result(
            r"""
            # using DISTINCT on a UNION with overlapping sets of Objects
            WITH MODULE test
            SELECT _ := count(DISTINCT ((
                # Issue 1, 4
                (SELECT User
                 FILTER User.name = 'Elvis').<owner[IS Issue]
            ) UNION (
                # Issue 1
                (SELECT User
                 FILTER User.name = 'Yury').<watchers[IS Issue]
            ) UNION (
                # Issue 1, 4
                SELECT Issue
                FILTER NOT EXISTS Issue.priority
            )));
            """,
            [2],
        )

    async def test_edgeql_select_setops_07(self):
        await self.assert_query_result(
            r"""
            # using UNION with overlapping sets of Objects
            WITH MODULE test
            SELECT _ := {  # equivalent to UNION for Objects
                # Issue 1, 4
                (
                    SELECT Issue
                    FILTER Issue.owner.name = 'Elvis'
                ), (
                    SELECT Issue
                    FILTER Issue.number = '1'
                )
            } { number }
            ORDER BY _.number;
            """,
            [{'number': '1'}, {'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_setops_08(self):
        await self.assert_query_result(
            r"""
            # using implicit nested UNION with overlapping sets of Objects
            WITH MODULE test
            SELECT _ := {  # equivalent to UNION for Objects
                # Issue 1, 4
                (
                    SELECT Issue
                    FILTER Issue.owner.name = 'Elvis'
                ),
                {
                    (
                        # Issue 1, 4
                        (
                            SELECT User
                            FILTER User.name = 'Elvis'
                        ).<owner[IS Issue]
                    ) UNION (
                        # Issue 1
                        (
                            SELECT User
                            FILTER User.name = 'Yury'
                        ).<watchers[IS Issue]
                    ),
                    (
                        # Issue 1, 4
                        SELECT Issue
                        FILTER NOT EXISTS Issue.priority
                    )
                },
                (
                    SELECT Issue FILTER Issue.number = '1'
                )
            } { number }
            ORDER BY _.number;
            """,
            [
                {'number': '1'}, {'number': '1'}, {'number': '1'},
                {'number': '1'}, {'number': '1'},
                {'number': '4'}, {'number': '4'}, {'number': '4'},
            ],
        )

    async def test_edgeql_select_setops_09(self):
        await self.assert_query_result(
            r"""
            # same as above but with a DISTINCT
            WITH MODULE test
            SELECT _ := (DISTINCT {  # equivalent to UNION for Objects
                # Issue 1, 4
                (
                    SELECT Issue
                    FILTER Issue.owner.name = 'Elvis'
                ),
                {
                    (
                        # Issue 1, 4
                        (
                            SELECT User
                            FILTER User.name = 'Elvis'
                        ).<owner[IS Issue]
                    ) UNION (
                        # Issue 1
                        (
                            SELECT User
                            FILTER User.name = 'Yury'
                        ).<watchers[IS Issue]
                    ),
                    (
                        # Issue 1, 4
                        SELECT Issue
                        FILTER NOT EXISTS Issue.priority
                    )
                },
                (
                    SELECT Issue
                    FILTER Issue.number = '1'
                )
            }) { number }
            ORDER BY _.number;
            """,
            [
                {'number': '1'}, {'number': '4'},
            ],
        )

    async def test_edgeql_select_setops_10(self):
        await self.assert_query_result(
            r"""
            # using UNION in a FILTER
            WITH MODULE test
            SELECT _ := User{name}
            FILTER (
                (
                    SELECT User.<owner[IS Issue]
                ) UNION (
                    # this part should guarantee the filter is always true
                    SELECT Issue
                    FILTER Issue.number = '1'
                )
            ).number = '1'
            ORDER BY _.name;
            """,
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        )

    async def test_edgeql_select_setops_11(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                L := LogEntry  # there happens to only be 1 entry
            SELECT
                (Issue.time_spent_log UNION L) {
                    body
                };
            """,
            [
                # duplicates are allowed in a plain UNION
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
            ],
        )

    async def test_edgeql_select_setops_12(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                L := LogEntry  # there happens to only be 1 entry
            SELECT
                (DISTINCT (Issue.time_spent_log UNION L)) {
                    body
                };
            """,
            [
                # no duplicates are allowed due to DISTINCT
                {'body': 'Rewriting everything.'},
            ],
        )

    async def test_edgeql_select_setops_13(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                L := LogEntry  # there happens to only be 1 entry
            SELECT
                (Issue.time_spent_log UNION L, Issue).0 {
                    body
                };
            """,
            [
                # not only do we expect duplicates, but we actually
                # expect 5 entries here:
                # - 1 for the actual `time_spent_log' links from Issue
                # - 4 from the UNION for each Issue.time_spent_log
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
            ],
        )

    async def test_edgeql_select_setops_14(self):
        await self.assert_query_result(
            r"""
            # The computable in the view is omitted from the duck type
            # of the UNION because ultimately it's the duck type of
            # the operands, which are both Issue with the real
            # property 'number'.
            WITH MODULE test
            SELECT {
                Issue{number := 'foo'}, Issue
            }.number;
            """,
            ['1', '1', '2', '2', '3', '3', '4', '4'],
            sort=True
        )

    async def test_edgeql_select_setops_15(self):
        await self.assert_query_result(
            r"""
            # The computable in the view is omitted from the duck type
            # of the UNION because ultimately it's the duck type of
            # the operands, which are both Issue with the real
            # property 'number'.
            WITH
                MODULE test,
                I := Issue{number := 'foo'}
            SELECT {I, Issue}.number;
            """,
            ['1', '1', '2', '2', '3', '3', '4', '4'],
            sort=True
        )

    async def test_edgeql_select_setops_16(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"has no link or property 'number'"):
            await self.con.fetchall(r"""
                # Named doesn't have a property number.
                WITH MODULE test
                SELECT Issue[IS Named].number;
            """)

    async def test_edgeql_select_setops_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"has no link or property 'number'"):
            await self.con.fetchall(r"""
                # UNION between Issue and empty set Named should be
                # duck-typed to be effectively equivalent to Issue[IS Named].
                WITH MODULE test
                SELECT (Issue UNION <Named>{}).number;
            """)

    async def test_edgeql_select_setops_18(self):
        await self.assert_query_result(
            r"""
            # UNION between Issue and empty set Named should be
            # duck-typed to be effectively equivalent to Issue[IS Named].
            WITH MODULE test
            SELECT (Issue UNION <Named>{}).name;
            """,
            {
                'Release EdgeDB',
                'Improve EdgeDB repl output rendering.',
                'Repl tweak.',
                'Regression.',
            },
        )

    async def test_edgeql_select_setops_19(self):
        await self.assert_query_result(
            r"""
            # UNION between Issue and empty set Issue should be
            # duck-typed to be effectively equivalent to Issue[IS
            # Issue], which is just an Issue.
            WITH MODULE test
            SELECT (Issue UNION <Issue>{}).name;
            """,
            {
                'Release EdgeDB',
                'Improve EdgeDB repl output rendering.',
                'Repl tweak.',
                'Regression.',
            },
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (Issue UNION <Issue>{}).number;
            """,
            {'1', '2', '3', '4'},
        )

    async def test_edgeql_select_order_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC EMPTY LAST THEN Issue.name;
            ''',
            [
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
            ],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC EMPTY FIRST THEN Issue.name;
            ''',
            [
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
            ]
        )

    async def test_edgeql_select_order_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Text {body}
            ORDER BY len(Text.body) DESC;
            ''',
            [
                {'body': 'We need to be able to render '
                         'data in tabular format.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Minor lexer tweaks.'}
            ]
        )

    async def test_edgeql_select_order_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT User {name}
            ORDER BY (
                SELECT sum(<int64>User.<watchers.number)
            );
            ''',
            [
                {'name': 'Yury'},
                {'name': 'Elvis'},
            ]
        )

    async def test_edgeql_select_order_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            await self.con.fetchall("""
                WITH MODULE test
                SELECT
                    User { name }
                ORDER BY User.<owner[IS Issue].number;
            """)

    async def test_edgeql_select_where_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment with non-empty body
            FILTER Issue.owner.<owner[IS Comment].body != ''
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_where_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment to it
            FILTER Issue.owner.<owner[IS Comment].issue = Issue;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_where_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{
                name,
                number,
                owner: {
                    name
                },
                status: {
                    name
                }
            } FILTER len(Issue.status.name) = 4
            ORDER BY Issue.number;
            ''',
            [{
                'owner': {'name': 'Elvis'},
                'status': {'name': 'Open'},
                'name': 'Release EdgeDB',
                'number': '1'
            }, {
                'owner': {'name': 'Yury'},
                'status': {'name': 'Open'},
                'name': 'Improve EdgeDB repl output rendering.',
                'number': '2'
            }],
        )

    async def test_edgeql_select_func_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT std::len(User.name) ORDER BY User.name;
            ''',
            [5, 4],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT std::sum(<std::int64>Issue.number);
            ''',
            [10]
        )

    async def test_edgeql_select_func_05(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::concat1(VARIADIC s: anytype) -> std::str
                FROM SQL FUNCTION 'concat';
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {
                params: {
                    num,
                    kind,
                    type: {
                        name
                    }
                }
            } FILTER schema::Function.name = 'test::concat1';
            ''',
            [{'params': [
                {
                    'num': 0,
                    'kind': 'VARIADIC',
                    'type': {
                        'name': 'array'
                    }
                }
            ]}]
        )

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'could not find a function variant'):
            async with self.con.transaction():
                await self.con.fetchall(
                    "SELECT test::concat1('aaa', 'bbb', 2);")

        await self.con.execute(r'''
            DROP FUNCTION test::concat1(VARIADIC s: anytype);
        ''')

    async def test_edgeql_select_func_06(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::concat2(VARIADIC s: std::str) -> std::str
                FROM SQL FUNCTION 'concat';
        ''')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'could not find a function'):
            await self.con.execute(r'SELECT test::concat2(123);')

    async def test_edgeql_select_func_07(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::concat3(sep: OPTIONAL std::str,
                                          VARIADIC s: std::str)
                    -> std::str
                FROM EdgeQL $$
                    # poor man concat
                    SELECT (array_get(s, 0) ?? '') ++
                           (sep ?? '::') ++
                           (array_get(s, 1) ?? '')
                $$;
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {
                params: {
                    num,
                    name,
                    kind,
                    type: {
                        name,
                        [IS schema::Array].element_type: {
                            name
                        }
                    },
                    typemod
                } ORDER BY .num ASC,
                return_type: {
                    name
                },
                return_typemod
            } FILTER schema::Function.name = 'test::concat3';
            ''',
            [{
                'params': [
                    {
                        'num': 0,
                        'name': 'sep',
                        'kind': 'POSITIONAL',
                        'type': {
                            'name': 'std::str',
                            'element_type': None
                        },
                        'typemod': 'OPTIONAL'
                    },
                    {
                        'num': 1,
                        'name': 's',
                        'kind': 'VARIADIC',
                        'type': {
                            'name': 'array',
                            'element_type': {'name': 'std::str'}
                        },
                        'typemod': 'SINGLETON'
                    }
                ],
                'return_type': {
                    'name': 'std::str'
                },
                'return_typemod': 'SINGLETON'
            }]
        )

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'could not find a function'):
            async with self.con.transaction():
                await self.con.fetchall(r'SELECT test::concat3(123);')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'could not find a function'):
            async with self.con.transaction():
                await self.con.fetchall(r'SELECT test::concat3("a", 123);')

        await self.assert_query_result(
            r'''
            SELECT test::concat3('|', '1', '2');
            ''',
            ['1|2'],
        )

        await self.con.execute(r'''
            DROP FUNCTION test::concat3(sep: std::str, VARIADIC s: std::str);
        ''')

    async def test_edgeql_select_exists_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS Issue.time_estimate
            ORDER BY
                Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                EXISTS Issue.time_estimate
            ORDER BY
                Issue.number;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_exists_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS (Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_exists_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS (SELECT Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_exists_04(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                EXISTS (Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_exists_05(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_exists_06(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_exists_07(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_exists_08(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_exists_09(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_exists_10(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_exists_11(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_exists_12(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_exists_13(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment
            FILTER EXISTS Issue.owner.<owner[IS Comment]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_exists_14(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment to it
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        Comment.issue = Issue
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_exists_15(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment, but not to the
            # issue itself
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        Comment.issue != Issue
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '4'}],
        )

    async def test_edgeql_select_exists_16(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment, but not to the
            # issue itself
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        Comment.issue.id != Issue.id
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '4'}],
        )

    async def test_edgeql_select_exists_17(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment, but not to the
            # issue itself
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        NOT Comment.issue = Issue
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '4'}],
        )

    async def test_edgeql_select_exists_18(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT EXISTS (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            );
            ''',
            [True],
        )

    async def test_edgeql_select_coalesce_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{
                kind := Issue.priority.name ?? Issue.status.name
            }
            ORDER BY Issue.number;
            ''',
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        )

    async def test_edgeql_select_coalesce_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\?\?' cannot.*'std::str' and 'std::int64'"):

            await self.con.execute(r'''
                WITH MODULE test
                SELECT Issue{
                    kind := Issue.priority.name ?? 1
                };
            ''')

    async def test_edgeql_select_coalesce_03(self):
        issues_h = await self.con.fetchall(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
            ORDER BY Issue.number;
        ''')

        issues_n = await self.con.fetchall(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority
            ORDER BY Issue.number;
        ''')

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name ?? 'High' = 'High'
            ORDER BY
                Issue.priority.name EMPTY LAST THEN Issue.number;
            ''',
            [{'number': o.number} for o in [*issues_h, *issues_n]]
        )

    async def test_edgeql_select_equivalence_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue {
                number,
                h1 := Issue.priority.name = 'High',
                h2 := Issue.priority.name ?= 'High',
                l1 := Issue.priority.name != 'High',
                l2 := Issue.priority.name ?!= 'High'
            }
            ORDER BY Issue.number;
            ''',
            [{
                'number': '1',
                'h1': None,
                'h2': False,
                'l1': None,
                'l2': True,
            }, {
                'number': '2',
                'h1': True,
                'h2': True,
                'l1': False,
                'l2': False,
            }, {
                'number': '3',
                'h1': False,
                'h2': False,
                'l1': True,
                'l2': True,
            }, {
                'number': '4',
                'h1': None,
                'h2': False,
                'l1': None,
                'l2': True,
            }],
        )

    async def test_edgeql_select_equivalence_02(self):
        await self.assert_query_result(
            r'''
            # get Issues such that there's another Issue with
            # equivalent priority
            WITH
                MODULE test,
                I2 := Issue
            SELECT Issue {number}
            FILTER
                I2 != Issue
                AND
                I2.priority.name ?= Issue.priority.name
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_equivalence_03(self):
        await self.assert_query_result(
            r'''
            # get Issues with priority equivalent to empty
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                Issue.priority.name ?= <str>{}
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_equivalence_04(self):
        await self.assert_query_result(
            r'''
            # get Issues with priority equivalent to empty
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                NOT Issue.priority.name ?!= <str>{}
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_as_01(self):
        # NOTE: for the expected ordering of Text see instance04 test
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT (SELECT T := Text[IS Issue] ORDER BY T.body).number;
            ''',
            ['4', '1', '3', '2'],
        )

    async def test_edgeql_select_as_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT (
                SELECT T := Text[IS Issue]
                FILTER T.body LIKE '%EdgeDB%'
                ORDER BY T.name
            ).name;
            ''',
            ['Release EdgeDB']
        )

    async def test_edgeql_select_and_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [],
        )

    async def test_edgeql_select_and_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [],
        )

    async def test_edgeql_select_and_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '4'}],
        )

    async def test_edgeql_select_and_04(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '4'}],
        )

    async def test_edgeql_select_and_05(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_and_06(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    async def test_edgeql_select_and_07(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_and_08(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_or_01(self):
        issues_h = await self.con.fetchall(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
            ORDER BY Issue.number;
        ''')

        issues_l = await self.con.fetchall(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'Low'
            ORDER BY Issue.number;
        ''')

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
            ORDER BY Issue.priority.name THEN Issue.number;
            ''',
            [{'number': o.number} for o in [*issues_h, *issues_l]]
        )

    async def test_edgeql_select_or_04(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            # it so happens that all low priority issues are also closed
            [{'number': '2'}, {'number': '3'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name IN {'High', 'Low'}
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_or_05(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r'''
            # should be identical
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_or_06(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_or_07(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_or_08(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

    async def test_edgeql_select_or_09(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

    async def test_edgeql_select_or_10(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_or_11(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_or_12(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_or_13(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_or_14(self):
        await self.assert_query_result(
            r'''
            # Find Issues that have status 'Closed' or number 2 or 3
            #
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.status.name = 'Closed'
                OR
                Issue.number = '2'
                OR
                Issue.number = '3'
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_or_15(self):
        await self.assert_query_result(
            r'''
            # Find Issues that have status 'Closed' or number 2 or 3
            #
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                (
                    # Issues 2, 3, 4 satisfy this subclause
                    Issue.status.name = 'Closed'
                    OR
                    Issue.number = '2'
                    OR
                    Issue.number = '3'
                ) AND (
                    # Issues 1, 2, 3 satisfy this subclause
                    Issue.name ILIKE '%edgedb%'
                    OR
                    Issue.priority.name = 'Low'
                )
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_not_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER Issue.priority.name != 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

    async def test_edgeql_select_not_02(self):
        # testing double negation
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER NOT NOT NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER NOT NOT Issue.priority.name != 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

    async def test_edgeql_select_not_03(self):
        # test that: a OR b = NOT( NOT a AND NOT b)
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT (
                    NOT Issue.priority.name = 'High'
                    AND
                    NOT Issue.status.name = 'Closed'
                )
            ORDER BY Issue.number;
            ''',
            # this is the result from or04
            #
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_empty_01(self):
        await self.assert_query_result(
            r"""
            # This is not the same as checking that number does not EXIST.
            # Any binary operator with one operand as empty results in an
            # empty result, because the cross product of anything with an
            # empty set is empty.
            SELECT test::Issue.number = <str>{};
            """,
            [],
        )

    async def test_edgeql_select_empty_02(self):
        await self.assert_query_result(
            r"""
            # Test short-circuiting operations with empty
            SELECT test::Issue.number = '1' OR <bool>{};
            """,
            [],
        )

        await self.assert_query_result(
            r"""
            SELECT test::Issue.number = 'X' OR <bool>{};
            """,
            [],
        )

        await self.assert_query_result(
            r"""
            SELECT test::Issue.number = '1' AND <bool>{};
            """,
            [],
        )

        await self.assert_query_result(
            r"""
            SELECT test::Issue.number = 'X' AND <bool>{};
            """,
            [],
        )

    async def test_edgeql_select_empty_03(self):
        await self.assert_query_result(
            r"""
            # Test short-circuiting operations with empty
            SELECT count(test::Issue.number = '1' OR <bool>{});
            """,
            [0],
        )

        await self.assert_query_result(
            r"""
            SELECT count(test::Issue.number = 'X' OR <bool>{});
            """,
            [0],
        )

        await self.assert_query_result(
            r"""
            SELECT count(test::Issue.number = '1' AND <bool>{});
            """,
            [0],
        )

        await self.assert_query_result(
            r"""
            SELECT count(test::Issue.number = 'X' AND <bool>{});
            """,
            [0],
        )

    async def test_edgeql_select_empty_04(self):
        await self.assert_query_result(
            r"""
            # Perfectly legal way to mask 'name' with empty set of
            # some arbitrary type.
            WITH MODULE test
            SELECT Issue {
                number,
                name := <int64>{}
            } ORDER BY .number;
            """,
            [
                {'number': '1', 'name': None},
                {'number': '2', 'name': None},
                {'number': '3', 'name': None},
                {'number': '4', 'name': None},
            ],
        )

    async def test_edgeql_select_empty_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'expression returns value of indeterminate type'):
            await self.con.fetchall(r"""
                WITH MODULE test
                SELECT Issue {
                    number,
                    # the empty set is of an unspecified type
                    name := {}
                } ORDER BY .number;
                """)

    async def test_edgeql_select_cross_01(self):
        await self.assert_query_result(
            r"""
            # the cross product of status and priority names
            WITH MODULE test
            SELECT Status.name ++ Priority.name
            ORDER BY Status.name THEN Priority.name;
            """,
            ['ClosedHigh', 'ClosedLow', 'OpenHigh', 'OpenLow'],
        )

    async def test_edgeql_select_cross_02(self):
        await self.assert_query_result(
            r"""
            # status and priority name for each issue
            WITH MODULE test
            SELECT Issue.status.name ++ Issue.priority.name
            ORDER BY Issue.number;
            """,
            ['OpenHigh', 'ClosedLow'],
        )

    async def test_edgeql_select_cross_03(self):
        await self.assert_query_result(
            r"""
            # cross-product of all user names and issue numbers
            WITH MODULE test
            SELECT User.name ++ Issue.number
            ORDER BY User.name THEN Issue.number;
            """,
            ['Elvis1', 'Elvis2', 'Elvis3', 'Elvis4',
             'Yury1', 'Yury2', 'Yury3', 'Yury4'],
        )

    async def test_edgeql_select_cross_04(self):
        await self.assert_query_result(
            r"""
            # concatenate the user name with every issue number that user has
            WITH MODULE test
            SELECT User.name ++ User.<owner[IS Issue].number
            ORDER BY User.name THEN User.<owner[IS Issue].number;
            """,
            ['Elvis1', 'Elvis4', 'Yury2', 'Yury3'],
        )

    async def test_edgeql_select_cross05(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            # tuples will not exist for the Issue without watchers
            SELECT _ := (Issue.owner.name, Issue.watchers.name)
            ORDER BY _;
            """,
            [['Elvis', 'Yury'], ['Yury', 'Elvis'], ['Yury', 'Elvis']],
        )

    async def test_edgeql_select_cross06(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            # tuples will not exist for the Issue without watchers
            SELECT _ := Issue.owner.name ++ Issue.watchers.name
            ORDER BY _;
            """,
            ['ElvisYury', 'YuryElvis', 'YuryElvis'],
        )

    async def test_edgeql_select_cross_07(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT _ := count(Issue.owner.name ++ Issue.watchers.name);
            """,
            [3],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT _ := count(DISTINCT (
                Issue.owner.name ++ Issue.watchers.name));
            """,
            [2],
        )

    async def test_edgeql_select_cross08(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT _ := Issue.owner.name ++ <str>count(Issue.watchers.name)
            ORDER BY _;
            """,
            ['Elvis0', 'Elvis1', 'Yury1', 'Yury1'],
        )

    async def test_edgeql_select_cross_09(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT _ := count(
                Issue.owner.name ++ <str>count(Issue.watchers.name));
            """,
            [4],
        )

    async def test_edgeql_select_cross_10(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                # this select shows all the relevant data for next tests
                x := (SELECT Issue {
                    name := Issue.owner.name,
                    w := count(Issue.watchers.name),
                })
            SELECT count(x.name ++ <str>x.w);
            """,
            [4],
        )

    async def test_edgeql_select_cross_11(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT count(
                Issue.owner.name ++
                <str>count(Issue.watchers) ++
                <str>Issue.time_estimate ?? '0'
            );
            """,
            [4],
        )

    async def test_edgeql_select_cross_12(self):
        # Same as cross11, but without coalescing the time_estimate,
        # which should collapse the counted set to a single element.
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT count(
                Issue.owner.name ++
                <str>count(Issue.watchers) ++
                <str>Issue.time_estimate
            );
            """,
            [{}],
        )

    async def test_edgeql_select_cross_13(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT count(count( Issue.watchers));
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT count(
                (Issue, count(Issue.watchers))
            );
            """,
            [4],
        )

    async def test_edgeql_select_subqueries_01(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                Issue2 := Issue
            # this is string concatenation, not integer arithmetic
            SELECT Issue.number ++ Issue2.number
            ORDER BY Issue.number ++ Issue2.number;
            """,
            ['{}{}'.format(a, b) for a in range(1, 5) for b in range(1, 5)],
        )

    async def test_edgeql_select_subqueries_02(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.number IN {'2', '3', '4'}
                AND
                EXISTS (
                    # due to common prefix, the Issue referred to here is
                    # the same Issue as in the LHS of AND, therefore
                    # this condition can never be true
                    SELECT Issue FILTER Issue.number IN {'1', '6'}
                );
            """,
            [],
        )

    async def test_edgeql_select_subqueries_03(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                sub := (
                    SELECT Issue FILTER Issue.number IN {'1', '6'}
                )
            SELECT Issue{number}
            FILTER
                Issue.number IN {'2', '3', '4'}
                AND
                EXISTS (
                    (SELECT sub FILTER sub = Issue)
                );
            """,
            [],
        )

    async def test_edgeql_select_subqueries_04(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                sub := (
                    SELECT
                        Issue
                    FILTER
                        Issue.number IN {'1', '6'}
                )
            SELECT
                Issue{number}
            FILTER
                Issue.number IN {'2', '3', '4'}
                AND
                EXISTS sub
            ORDER BY
                Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_select_subqueries_05(self):
        await self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue with the same priority
            WITH
                MODULE test,
                Issue2 := (SELECT Issue)
            SELECT
                Issue {
                    number
                }
            FILTER
                Issue != Issue2
                AND
                # NOTE: this condition is false when one of the sides is empty
                Issue.priority = Issue2.priority
            ORDER BY
                Issue.number;
            """,
            [],
        )

    async def test_edgeql_select_subqueries_06(self):
        await self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue with the same priority (even if the "same" means empty)
            WITH
                MODULE test,
                Issue2 := Issue
            SELECT
                Issue {
                    number
                }
            FILTER
                Issue != Issue2 AND Issue.priority ?= Issue2.priority
            ORDER BY
                Issue.number;
            """,
            [{'number': '1'}, {'number': '4'}],
        )

    async def test_edgeql_select_subqueries_07(self):
        await self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue watched by the same user as this one
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.watchers
                AND
                EXISTS (
                    (SELECT
                        User
                     FILTER
                        User = Issue.watchers AND
                        User.<watchers != Issue
                    ).<watchers
                )
            ORDER BY
                Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_subqueries_08(self):
        await self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue watched by the same user as this one
            WITH
                MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.watchers
                AND
                EXISTS (
                    SELECT Text
                    FILTER
                        Text IS Issue
                        AND
                        Text[IS Issue].watchers = Issue.watchers
                        AND
                        Text != Issue
                )
            ORDER BY
                Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}],
        )

    async def test_edgeql_select_subqueries_09(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue.number ++ (SELECT Issue.number);
            """,
            ['11', '22', '33', '44'],
            sort=True
        )

    async def test_edgeql_select_subqueries_10(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                sub := (SELECT Issue.number)
            SELECT
                Issue.number ++ sub;
            """,
            ['11', '12', '13', '14', '21', '22', '23', '24',
             '31', '32', '33', '34', '41', '42', '43', '44'],
            sort=True
        )

    async def test_edgeql_select_subqueries_11(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Text{
                [IS Issue].number,
                body_length := len(Text.body)
            } ORDER BY len(Text.body);
            """,
            [
                {'number': '3', 'body_length': 19},
                {'number': None, 'body_length': 21},
                {'number': None, 'body_length': 28},
                {'number': '1', 'body_length': 33},
                {'number': '4', 'body_length': 41},
                {'number': '2', 'body_length': 52},
            ],
        )

        await self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # Text item of similar body length (+/-5 characters)
            WITH MODULE test
            SELECT Issue{
                number,
            }
            FILTER
                EXISTS (
                    SELECT Text
                    FILTER
                        Text != Issue
                        AND
                        (len(Text.body) - len(Issue.body)) ^ 2 <= 25
                )
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '3'}],
        )

    async def test_edgeql_select_subqueries_12(self):
        await self.assert_query_result(
            r"""
            # same as above, but also include the body_length computable
            WITH MODULE test
            SELECT Issue{
                number,
                body_length := len(Issue.body)
            }
            FILTER
                EXISTS (
                    SELECT Text
                    FILTER
                        Text != Issue
                        AND
                        (len(Text.body) - len(Issue.body)) ^ 2 <= 25
                )
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'body_length': 33,
            }, {
                'number': '3',
                'body_length': 19,
            }],
        )

    async def test_edgeql_select_subqueries_13(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT User{name}
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = User
                );
            """,
            [{'name': 'Elvis'}],
        )

    async def test_edgeql_select_subqueries_14(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT User{name}
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = User
                # adding a required link to an EXISTS should not alter
                # the result
                ).owner;
            """,
            [{'name': 'Elvis'}],
        )

    async def test_edgeql_select_subqueries_15(self):
        await self.assert_query_result(
            r"""
            # Find all issues such that there's at least one more
            # issue watched by the same user as this one, this user
            # must have at least one Comment.
            WITH MODULE test
            SELECT Issue {
                number
            }
            FILTER
                EXISTS Issue.watchers AND
                EXISTS (
                    SELECT
                        User
                    FILTER
                        # The User is among the watchers of this Issue
                        User = Issue.watchers AND
                        # and they also watch some other Issue other than this
                        User.<watchers[IS Issue] != Issue AND
                        # and they also have at least one comment
                        EXISTS (
                            SELECT Comment FILTER Comment.owner = User
                        )
                )
            ORDER BY
                Issue.number;
            """,
            [
                {'number': '2'},
                {'number': '3'}
            ],
        )

    async def test_edgeql_select_subqueries_16(self):
        await self.assert_query_result(
            r"""
            # testing IN and a subquery
            WITH MODULE test
            SELECT Comment{body}
            FILTER
                Comment.owner IN (
                    SELECT User
                    FILTER
                        User.name = 'Elvis'
                );
            """,
            [{'body': 'EdgeDB needs to happen soon.'}],
        )

    async def test_edgeql_select_subqueries_17(self):
        await self.assert_query_result(
            r"""
            # get a comment whose owner is part of the users who own Issue "1"
            WITH MODULE test
            SELECT Comment{body}
            FILTER
                Comment.owner IN (
                    SELECT User
                    FILTER
                        User.<owner IN (
                            SELECT Issue
                            FILTER
                                Issue.number = '1'
                        )
                );
            """,
            [{'body': 'EdgeDB needs to happen soon.'}],
        )

    async def test_edgeql_select_subqueries_18(self):
        await self.assert_query_result(
            r"""
            # here, DETACHED doesn't do anything special, because the
            # symbol U2 is reused on both sides of '+'
            WITH
                MODULE test,
                U2 := DETACHED User
            SELECT U2.name ++ U2.name;
            """,
            {'ElvisElvis', 'YuryYury'},
        )

        await self.assert_query_result(
            r"""
            # DETACHED is reused on both sides of '+' directly
            WITH MODULE test
            SELECT (DETACHED User).name ++ (DETACHED User).name;
            """,
            {'ElvisElvis', 'ElvisYury', 'YuryElvis', 'YuryYury'},
        )

    async def test_edgeql_select_view_indirection_01(self):
        await self.assert_query_result(
            r"""
            # Direct reference to a computable element in a subquery
            WITH MODULE test
            SELECT
                (
                    SELECT User {
                        num_issues := count(User.<owner[IS Issue])
                    } FILTER .name = 'Elvis'
                ).num_issues;
            """,
            [2],
        )

    async def test_edgeql_select_view_indirection_02(self):
        await self.assert_query_result(
            r"""
            # Reference to a computable element in a subquery
            # defined as an inline view.
            WITH MODULE test,
                U := (
                    SELECT User {
                        num_issues := count(User.<owner[IS Issue])
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.num_issues;
            """,
            [2],
        )

    async def test_edgeql_select_view_indirection_03(self):
        await self.assert_query_result(
            r"""
            # Reference a computed object set in a view.
            WITH MODULE test,
                U := (
                    WITH U2 := User
                    SELECT User {
                        friend := (
                            SELECT U2 FILTER U2.name = 'Yury'
                        )
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.friend.name;
            """,
            ['Yury'],
        )

    async def test_edgeql_select_view_indirection_04(self):
        result = await self.con.fetchall(r"""
            # Reference a constant expression in a view.
            WITH MODULE test,
                U := (
                    SELECT User {
                        issues := (
                            SELECT Issue {
                                foo := 1 + random()
                            } FILTER Issue.owner = User
                        )
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.issues.foo;
            """)

        self.assertEqual(len(result), 2)

    async def test_edgeql_select_view_indirection_05(self):
        await self.assert_query_result(
            r"""
            # Reference multiple views.
            WITH MODULE test,
                U := (
                    SELECT User FILTER User.name = 'Elvis'
                ),
                I := (
                    SELECT Issue FILTER Issue.number = '1'
                )
            SELECT
                I.owner = U;
            """,
            [True],
        )

    async def test_edgeql_select_view_indirection_06(self):
        await self.assert_query_result(
            r"""
            # Reference another view from a view.
            WITH MODULE test,
                U := (
                    SELECT User FILTER User.name = 'Elvis'
                ),
                I := (
                    SELECT Issue FILTER Issue.owner = U
                )
            SELECT
                I.number
            ORDER BY
                I.number;
            """,
            ['1', '4'],
        )

    async def test_edgeql_select_view_indirection_07(self):
        await self.assert_query_result(
            r"""
            # A combination of the above two.
            WITH MODULE test,
                U := (
                    SELECT User FILTER User.name = 'Elvis'
                ),
                I := (
                    SELECT Issue FILTER Issue.owner = U
                )
            SELECT
                I
            FILTER
                I.owner != U
            ORDER BY
                I.number;
            """,
            [],
        )

    async def test_edgeql_select_view_indirection_08(self):
        await self.assert_query_result(
            r"""
            # A slightly more complex view.
             WITH MODULE test,
                 U := (
                     WITH U2 := User
                     SELECT User {
                         friends := (
                             SELECT U2 { foo := U2.name ++ '!' }
                             FILTER U2.name = 'Yury'
                         )
                     } FILTER .name = 'Elvis'
                 )
             SELECT
                 U {
                     my_issues := (
                        SELECT U.<owner[IS Issue].number
                        ORDER BY U.<owner[IS Issue].number),
                     friends_issues := (
                        SELECT U.friends.<owner[IS Issue].number
                        ORDER BY U.friends.<owner[IS Issue].number),
                     friends_foos := (
                        SELECT U.friends.foo
                        ORDER BY U.friends.foo)
                 };
            """,
            [{
                'my_issues': ['1', '4'],
                'friends_foos': 'Yury!',
                'friends_issues': ['2', '3']
            }]
        )

    async def test_edgeql_select_view_indirection_09(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                sub := (
                    SELECT
                        Text {
                            foo := Text.body ++ '!'
                        }
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT 1
                )
            SELECT
                User {
                    name,
                    shortest_text_shape := sub {
                        body,
                        foo
                    }
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text_shape': {
                    'body': 'Minor lexer tweaks.',
                    'foo': 'Minor lexer tweaks.!',
                },
            }]
        )

    async def test_edgeql_select_view_indirection_10(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                sub := (
                    SELECT
                        Text {
                            foo := Text.body ++ '!'
                        }
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT 1
                )
            SELECT
                User {
                    name,
                    shortest_text_foo := sub.foo
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text_foo': 'Minor lexer tweaks.!'
            }]
        )

    async def test_edgeql_select_view_indirection_11(self):
        await self.assert_query_result(
            r'''
            WITH
                MODULE test,
                Developers := (
                    SELECT
                        User {
                            open_issues := (
                                SELECT
                                    Issue {
                                        spent_time := (
                                            SELECT
                                                sum(Issue.time_spent_log
                                                         .spent_time)
                                        )
                                    }
                                FILTER
                                    Issue.owner = User
                            )
                        }
                    FILTER
                        User.name IN {'Elvis', 'Yury'}
                )
            SELECT
                Developers {
                    name,
                    open_issues: {
                        number,
                        spent_time
                    } ORDER BY .number
                }
            ORDER BY
                Developers.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'open_issues': [
                        {'number': '1', 'spent_time': 50000},
                        {'number': '4', 'spent_time': 0},
                    ]
                },
                {
                    'name': 'Yury',
                    'open_issues': [
                        {'number': '2', 'spent_time': 0},
                        {'number': '3', 'spent_time': 0}
                    ]
                }
            ]
        )

    async def test_edgeql_select_slice_01(self):
        await self.assert_query_result(
            r"""
            # full name of the Issue is 'Release EdgeDB'
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2];
            """,
            ['l'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[-2];
            """,
            ['D'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2:4];
            """,
            ['le'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2:];
            """,
            ['lease EdgeDB'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[:2];
            """,
            ['Re'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2:-1];
            """,
            ['lease EdgeD'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[-2:];
            """,
            ['DB'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[:-2];
            """,
            ['Release Edge'],
        )

    async def test_edgeql_select_slice_02(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name;
            """,
            ['test::Issue'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2];
            """,
            ['s'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[-2];
            """,
            ['u'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2:4];
            """,
            ['st'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2:];
            """,
            ['st::Issue'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[:2];
            """,
            ['te'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2:-1];
            """,
            ['st::Issu'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[-2:];
            """,
            ['ue'],
        )

        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[:-2];
            """,
            ['test::Iss'],
        )

    async def test_edgeql_select_slice_03(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue{
                name,
                type_name := Issue.__type__.name,
                a := Issue.name[2],
                b := Issue.name[2:-1],
                c := Issue.__type__.name[2:-1],
            }
            FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'type_name': 'test::Issue',
                'a': 'l',
                'b': 'lease EdgeD',
                'c': 'st::Issu',
            }],
        )

    async def test_edgeql_select_tuple_01(self):
        await self.assert_query_result(
            r"""
            # get tuples (status, number of issues)
            WITH MODULE test
            SELECT (Status.name, count(Status.<status))
            ORDER BY Status.name;
            """,
            [['Closed', 2], ['Open', 2]]
        )

    async def test_edgeql_select_tuple_02(self):
        await self.assert_query_result(
            r"""
            # nested tuples
            WITH MODULE test
            SELECT
                _ := (
                    User.name, (
                        User.<owner[IS Issue].status.name,
                        count(User.<owner[IS Issue])
                    )
                )
                # A tuple is essentially an identity function within our
                # set operation semantics, so here we're selecting a cross
                # product of all user names with user owned issue statuses.
                #
            ORDER BY _.0 THEN _.1;
            """,
            [
                ['Elvis', ['Closed', 1]],
                ['Elvis', ['Open', 1]],
                ['Yury', ['Closed', 1]],
                ['Yury', ['Open', 1]],
            ]
        )

    async def test_edgeql_select_tuple_03(self):
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                _ := {('Elvis',), ('Yury',)}
            SELECT
                User {
                    name
                }
            FILTER
                User.name = _.0
            ORDER BY
                User.name;
            """,
            [
                {'name': 'Elvis'},
                {'name': 'Yury'},
            ]
        )

    async def test_edgeql_select_tuple_04(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT
                User {
                    t := {(1, 2), (3, 4)}
                }
            FILTER
                User.name = 'Elvis'
            ORDER BY
                User.name;
            """,
            [
                {'t': [[1, 2], [3, 4]]},
            ]
        )

    async def test_edgeql_select_tuple_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (
                    statuses := count(Status),
                    issues := count(Issue),
                );
            """,
            [{'statuses': 2, 'issues': 4}],
        )

    async def test_edgeql_select_tuple_06(self):
        # Tuple in a common set expr.
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                counts := (SELECT (
                    statuses := count(Status),
                    issues := count(Issue),
                ))
            SELECT
                counts.statuses + counts.issues;
            """,
            [6],
        )

    async def test_edgeql_select_tuple_07(self):
        # Object in a tuple.
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                criteria := (SELECT (
                    user := (SELECT User FILTER User.name = 'Yury'),
                    status := (SELECT Status FILTER Status.name = 'Open'),
                ))
            SELECT (
                SELECT
                    Issue
                FILTER
                    Issue.owner = criteria.user
                    AND Issue.status = criteria.status
            ).number;
            """,
            ['2'],
        )

    async def test_edgeql_select_tuple_08(self):
        # Object in a tuple returned directly.
        await self.assert_query_result(
            r"""
            WITH
                MODULE test
            SELECT
                (
                    user := (SELECT User{name} FILTER User.name = 'Yury')
                );
            """,
            [{
                'user': {
                    'name': 'Yury'
                }
            }],
        )

    async def test_edgeql_select_tuple_09(self):
        # Object in a tuple referred to directly.
        await self.assert_query_result(
            r"""
            WITH
                MODULE test
            SELECT
                (
                    user := (SELECT User{name} FILTER User.name = 'Yury')
                ).user.name;
            """,
            ['Yury'],
        )

    async def test_edgeql_select_tuple_10(self):
        # Tuple comparison
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                U1 := User,
                U2 := User
            SELECT
                (user := (SELECT U1{name} FILTER U1.name = 'Yury'))
                    =
                (user := (SELECT U2{name} FILTER U2.name = 'Yury'));
            """,
            [True],
        )

        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                U1 := User,
                U2 := User
            SELECT
                (user := (SELECT U1{name} FILTER U1.name = 'Yury'))
                    =
                (user := (SELECT U2{name} FILTER U2.name = 'Elvis'));

            """,
            [False],
        )

    async def test_edgeql_select_linkproperty_01(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT User.todo@rank + <int64>User.todo.number
            ORDER BY User.todo.number;
            """,
            [43, 44, 45, 46]
        )

    async def test_edgeql_select_linkproperty_02(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue.<todo@rank + <int64>Issue.number
            ORDER BY Issue.number;
            """,
            [43, 44, 45, 46]
        )

    async def test_edgeql_select_linkproperty_03(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT User {
                name,
                todo: {
                    number,
                    @rank
                } ORDER BY User.todo.number
            }
            ORDER BY User.name;
            """,
            [{
                'name': 'Elvis',
                'todo': [{
                    'number': '1',
                    '@rank': 42,
                }, {
                    'number': '2',
                    '@rank': 42,
                }]
            }, {
                'name': 'Yury',
                'todo': [{
                    'number': '3',
                    '@rank': 42,
                }, {
                    'number': '4',
                    '@rank': 42,
                }]
            }],
        )

    async def test_edgeql_select_if_else_01(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue {
                number,
                open := 'yes' IF Issue.status.name = 'Open' ELSE 'no'
            }
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'open': 'yes',
            }, {
                'number': '2',
                'open': 'yes',
            }, {
                'number': '3',
                'open': 'no',
            }, {
                'number': '4',
                'open': 'no',
            }],
        )

    async def test_edgeql_select_if_else_02(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue {
                number,
                # foo is 'bar' for Issue number 1 and status name for the rest
                foo := 'bar' IF Issue.number = '1' ELSE Issue.status.name
            }
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'foo': 'bar',
            }, {
                'number': '2',
                'foo': 'Open',
            }, {
                'number': '3',
                'foo': 'Closed',
            }, {
                'number': '4',
                'foo': 'Closed',
            }],
        )

    async def test_edgeql_select_if_else_03(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'operator.*IF.*cannot be applied'):

            await self.con.execute(r"""
                WITH MODULE test
                SELECT Issue {
                    foo := 'bar' IF Issue.number = '1' ELSE 123
                };
                """)

    async def test_edgeql_select_if_else_04(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue{
                kind := (Issue.priority.name
                         IF EXISTS Issue.priority.name
                         ELSE Issue.status.name)
            }
            ORDER BY Issue.number;
            """,
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        )

        await self.assert_query_result(
            r"""
            # Above IF is equivalent to ??,
            WITH MODULE test
            SELECT Issue{
                kind := Issue.priority.name ?? Issue.status.name
            }
            ORDER BY Issue.number;
            """,
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        )

    async def test_edgeql_select_if_else_05(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                Issue.priority.name = 'High'
                    IF EXISTS Issue.priority.name AND EXISTS 'High'
                    ELSE EXISTS Issue.priority.name = EXISTS 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}],
        )

        await self.assert_query_result(
            r"""
            # Above IF is equivalent to ?=,
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                Issue.priority.name ?= 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}],
        )

    async def test_edgeql_select_if_else_06(self):
        await self.assert_query_result(
            r"""
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                Issue.priority.name != 'High'
                    IF EXISTS Issue.priority.name AND EXISTS 'High'
                    ELSE EXISTS Issue.priority.name != EXISTS 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

        await self.assert_query_result(
            r"""
            # Above IF is equivalent to !?=,
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                Issue.priority.name ?!= 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

    async def test_edgeql_partial_01(self):
        await self.assert_query_result(
            '''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                .number = '1';
            ''',
            [{
                'number': '1'
            }]
        )

    async def test_edgeql_partial_02(self):
        await self.assert_query_result(
            '''
            WITH MODULE test
            SELECT
                Issue.watchers {
                    name
                }
            FILTER
                .name = 'Yury';
            ''',
            [{
                'name': 'Yury'
            }]
        )

    async def test_edgeql_partial_03(self):
        await self.assert_query_result(
            '''
            WITH MODULE test
            SELECT Issue {
                number,
                watchers: {
                    name,
                    name_upper := str_upper(.name)
                } FILTER .name = 'Yury'
            } FILTER .status.name = 'Open' AND .owner.name = 'Elvis';
            ''',
            [{
                'number': '1',
                'watchers': [{
                    'name': 'Yury',
                    'name_upper': 'YURY',
                }]
            }]
        )

    async def test_edgeql_partial_04(self):
        await self.assert_query_result(
            '''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .number > '1'
              ORDER BY .number DESC;
            ''',
            [
                {'number': '4'},
                {'number': '3'},
                {'number': '2'},
            ]
        )

    async def test_edgeql_partial_05(self):
        await self.assert_query_result('''
            WITH
                MODULE test
            SELECT
                Issue{
                    sub := (SELECT .number)
                }
            FILTER .number = '1';
        ''', [
            {'sub': '1'},
        ])

    async def test_edgeql_partial_06(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'invalid property reference on a '
                                    'primitive type expression'):
            await self.con.execute('''
                WITH MODULE test
                SELECT Issue.number FILTER .number > '1';
            ''')

    async def test_edgeql_virtual_target_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER EXISTS (.references)
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2'
            }],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .references[IS URL].address = 'https://edgedb.com'
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2'
            }],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .references[IS Named].name = 'screenshot.png'
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2'
            }],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue {
                number,
                references: Named {
                    __type__: {
                        name
                    },

                    name
                } ORDER BY .name
            } FILTER EXISTS (.references)
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2',
                'references': [
                    {
                        'name': 'edgedb.com',
                        '__type__': {
                            'name': 'test::URL'
                        }
                    },
                    {
                        'name': 'screenshot.png',
                        '__type__': {
                            'name': 'test::File'
                        }
                    }
                ]
            }]
        )

    async def test_edgeql_select_for_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT Issue := (
                FOR x IN {1, 4}
                UNION (
                    SELECT Issue {
                        name
                    }
                    FILTER
                        .number = <str>x
                )
            )
            ORDER BY
                .number;
            ''',
            [
                {'name': 'Release EdgeDB'},
                {'name': 'Regression.'},
            ]
        )

    async def test_edgeql_select_for_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT I := (
                FOR x IN {1, 3, 4}
                UNION (
                    SELECT Issue {
                        name,
                        number,
                    }
                    FILTER
                        .number > <str>x
                )
            )
            ORDER BY .number;
            ''',
            [
                {
                    'name': 'Improve EdgeDB repl output rendering.',
                    'number': '2'
                },
                {
                    'name': 'Repl tweak.',
                    'number': '3'
                },
                {
                    'name': 'Regression.',
                    'number': '4'
                },
                {
                    'name': 'Regression.',
                    'number': '4'
                },
            ]
        )

    async def test_edgeql_select_for_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            FOR x IN {1, 3, 4}
            UNION (
                SELECT Issue {
                    name,
                    number,
                }
                FILTER
                    Issue.number > <str>x
                ORDER BY
                    Issue.number
                LIMIT 2
            );
            ''',
            [
                {
                    'name': 'Improve EdgeDB repl output rendering.',
                    'number': '2'
                },
                {
                    'name': 'Repl tweak.',
                    'number': '3'
                },
                {
                    'name': 'Regression.',
                    'number': '4'
                },
            ],
            sort=lambda x: x['number'],
        )

    async def test_edgeql_select_json_01(self):
        await self.assert_query_result(
            r'''
            # cast an ad-hoc view into a set of json
            WITH MODULE test
            SELECT (
                SELECT <json>Issue {
                    number,
                    time_estimate
                } FILTER Issue.number = '1'
            ) = to_json('{"number": "1", "time_estimate": 3000}');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT (
                SELECT <json>Issue {
                    number,
                    time_estimate
                } FILTER Issue.number = '2'
            ) = to_json('{"number": "2", "time_estimate": null}');
            ''',
            [True],
        )

    async def test_edgeql_select_bad_reference_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"object type or view 'Usr' does not exist",
                _hint="did you mean one of these: User, URL?"):

            await self.con.fetchall("""
                WITH MODULE test
                SELECT Usr;
            """)

    async def test_edgeql_select_bad_reference_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'test::User' has no link or property 'nam'",
                _hint="did you mean 'name'?"):

            await self.con.fetchall("""
                WITH MODULE test
                SELECT User.nam;
            """)

    async def test_edgeql_select_precedence_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'index indirection cannot.*int64.*'):

            await self.con.fetchall("""
                # index access is higher precedence than cast
                SELECT <str>1[0];
            """)

    async def test_edgeql_select_precedence_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'index indirection cannot.*int64.*'):

            await self.con.fetchall("""
                WITH MODULE test
                # index access is higher precedence than cast
                SELECT <str>Issue.time_estimate[0];
            """)

    async def test_edgeql_select_precedence_03(self):
        await self.assert_query_result(
            r'''
            SELECT (<str>1)[0];
            ''',
            ['1'],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT (<str>Issue.time_estimate)[0];
            ''',
            ['3'],
        )

    async def test_edgeql_select_precedence_04(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT EXISTS Issue{number};
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT EXISTS Issue;
            ''',
            [True],
        )

    async def test_edgeql_select_precedence_05(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT EXISTS Issue{number};
            ''',
            [True],
        )

    async def test_edgeql_select_is_01(self):
        await self.assert_query_result(
            r'''SELECT 5 IS int64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS anyint;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS anyreal;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS anyscalar;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS int16;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS float64;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS anyfloat;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS str;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5 IS Object;''',
            [False],
        )

    async def test_edgeql_select_is_02(self):
        await self.assert_query_result(
            r'''SELECT 5.5 IS int64;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS anyint;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS anyreal;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS anyscalar;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS int16;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS float64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS anyfloat;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS str;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 IS Object;''',
            [False],
        )

    async def test_edgeql_select_is_03(self):
        await self.con.execute('SET MODULE test;')

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS int64 LIMIT 1;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyint LIMIT 1;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyreal LIMIT 1;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyscalar LIMIT 1;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS int16 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS float64 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyfloat LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS str LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.time_estimate IS Object LIMIT 1;''',
            [False],
        )

    async def test_edgeql_select_is_04(self):
        await self.con.execute('SET MODULE test;')

        await self.assert_query_result(
            r'''SELECT Issue.number IS int64 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS anyint LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS anyreal LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS anyscalar LIMIT 1;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS int16 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS float64 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS anyfloat LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS str LIMIT 1;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT Issue.number IS Object LIMIT 1;''',
            [False],
        )

    async def test_edgeql_select_is_05(self):
        await self.con.execute('SET MODULE test;')

        await self.assert_query_result(
            r'''SELECT Issue.status IS int64 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS anyint LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS anyreal LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS anyscalar LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS int16 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS float64 LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS anyfloat LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS str LIMIT 1;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT Issue.status IS Object LIMIT 1;''',
            [True],
        )

    async def test_edgeql_select_is_06(self):
        await self.assert_query_result(
            r'''
            SELECT 5 IS anytype;
            ''',
            [True]
        )

    async def test_edgeql_select_is_07(self):
        await self.assert_query_result(
            r'''
            SELECT 5 IS anyint;
            ''',
            [True]
        )

    async def test_edgeql_select_is_08(self):
        await self.assert_query_result(
            r'''
            SELECT 5.5 IS anyfloat;
            ''',
            [True]
        )

    async def test_edgeql_select_is_09(self):
        await self.assert_query_result(
            r'''
            SELECT test::Issue.time_estimate IS anytype LIMIT 1;
            ''',
            [True]
        )

    async def test_edgeql_select_is_10(self):
        await self.assert_query_result(
            r'''
            SELECT [5] IS (array<anytype>);
            ''',
            [True]
        )

    async def test_edgeql_select_is_11(self):
        await self.assert_query_result(
            r'''
            SELECT (5, 'hello') IS (tuple<anytype, str>);
            ''',
            [True]
        )

    async def test_edgeql_select_is_12(self):
        await self.assert_query_result(
            r'''
            SELECT [5] IS (array<int64>);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
            SELECT (5, 'hello') IS (tuple<int64, str>);
            ''',
            [True],
        )

    @test.xfail('IS is broken for runtime type checks of object collections')
    async def test_edgeql_select_is_13(self):
        await self.assert_query_result(
            r'''
            WITH MODULE test
            SELECT
                NOT all([Text] IS (array<Issue>))
                AND any([Text] IS (array<Issue>));
            ''',
            [True],
        )
