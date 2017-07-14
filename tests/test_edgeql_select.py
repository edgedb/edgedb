##
# Copyright (c) 2012-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.server import _testbase as tb
from edgedb.client import exceptions as exc


class TestEdgeQLSelect(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.eql')

    async def test_edgeql_select_unique01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT
                Issue.watchers.<owner[IS Issue] {
                    name
                } ORDER BY .name;
        ''', [
            [{
                'name': 'Improve EdgeDB repl output rendering.',
            }, {
                'name': 'Regression.',
            }, {
                'name': 'Release EdgeDB',
            }, {
                'name': 'Repl tweak.',
            }]
        ])

    async def test_edgeql_select_computable01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    aliased_number := Issue.number,
                    total_time_spent := (
                        SELECT SINGLETON
                            sum(ALL Issue.time_spent_log.spent_time)
                    )
                }
            FILTER
                Issue.number = '1';
        ''', [
            [{
                'number': '1',
                'aliased_number': '1',
                'total_time_spent': 50000
            }]
        ])

    async def test_edgeql_select_computable02(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    total_time_spent := (
                        SELECT SINGLETON
                            sum(ALL Issue.time_spent_log.spent_time)
                    )
                }
            FILTER
                Issue.number = '1';
        ''', [
            [{
                'number': '1',
                'total_time_spent': 50000
            }]
        ])

    async def test_edgeql_select_computable03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    shortest_own_text := (
                        SELECT SINGLETON
                            Text {
                                body
                            }
                        FILTER
                            Text[IS Owned].owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT
                            1
                    ),
                }
            FILTER User.name = 'Elvis';
        ''', [
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Rewriting everything.',
                },
            }]
        ])

    async def test_edgeql_select_computable04(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                # we aren't referencing User in any way, so this works
                # best as a subquery, rather than inline computable
                sub := (
                    SELECT SINGLETON
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                User {
                    name,
                    shortest_text := sub {
                        body
                    }
                }
            FILTER User.name = 'Elvis';
        ''', [
            [{
                'name': 'Elvis',
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        ])

    async def test_edgeql_select_computable05(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                # we aren't referencing User in any way, so this works
                # best as a subquery, than inline computable
                sub := (
                    SELECT SINGLETON
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
                        SELECT SINGLETON
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
        ''', [
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Rewriting everything.',
                },
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        ])

    async def test_edgeql_select_computable06(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    shortest_text := (
                        SELECT SINGLETON
                            Text {body}
                        # a clause that references User and is always true
                        FILTER
                            User IS User
                        ORDER
                            BY len(Text.body) ASC
                        LIMIT
                            1
                    ),
                }
            FILTER User.name = 'Elvis';
        ''', [
            [{
                'name': 'Elvis',
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        ])

    async def test_edgeql_select_computable07(self):
        await self.assert_query_result(r'''
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
        ''', [
            [{
                'name': 'Elvis',
                'special_texts': [
                    {'body': 'We need to be able to render data in '
                             'tabular format.'},
                    {'body': 'Minor lexer tweaks.'}
                ],
            }]
        ])

    async def test_edgeql_select_computable08(self):
        await self.assert_query_result(r"""
            # get a user + the latest issue (regardless of owner), which has
            # the same number of characters in the status as the user's name
            WITH MODULE test
            SELECT User{
                name,
                special_issue := (
                    SELECT SINGLETON Issue {
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
            """, [
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
        ])

    async def test_edgeql_select_computable09(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Text{
                body,
                name := Text[IS Issue].name IF Text IS Issue      ELSE
                        'log'                IF Text IS LogEntry   ELSE
                        'comment'            IF Text IS Comment    ELSE
                        'unknown'
            }
            ORDER BY Text.body;
            """, [
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
        ])

    async def test_edgeql_select_computable10(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue{
                name,
                number,
                # use shorthand with some simple operations
                foo := <int>Issue.number + 10,
            }
            FILTER Issue.number = '1';
            """, [
            [{
                'name': 'Release EdgeDB',
                'number': '1',
                'foo': 11,
            }],
        ])

    async def test_edgeql_select_computable11(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                sub := (
                    SELECT SINGLETON
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub.body;
        ''', [
            ['Minor lexer tweaks.']
        ])

    async def test_edgeql_select_computable12(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                sub := (
                    SELECT SINGLETON
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub.__class__.name;
        ''', [
            ['test::Issue']
        ])

    async def test_edgeql_select_computable13(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                sub := (
                    SELECT SINGLETON
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub[IS Issue].number;
        ''', [
            ['3']
        ])

    async def test_edgeql_select_match01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%Edge%'
            ORDER BY Issue.number;
        """, [
            [],
            [{'number': '1'}],
            [{'number': '1'}, {'number': '2'}],
        ])

    async def test_edgeql_select_match02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%Edge%'
            ORDER BY Issue.number;
        """, [
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [{'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_match03(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%re%'
            ORDER BY Issue.number;
        """, [
            [{'number': '1'}],
            [{'number': '1'}],
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        ])

    async def test_edgeql_select_match04(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%re%'
            ORDER BY Issue.number;
        """, [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [],
        ])

    async def test_edgeql_select_match07(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                Text.body ~ 'ed'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                Text.body ~ 'eD'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                Text.body ~ 'ed([S\s]|$)'
            ORDER BY Text.body;
        """, [
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Initial public release of EdgeDB.'}],
            [{'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}]
        ])

    async def test_edgeql_select_match08(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                Text.body ~* 'ed'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                Text.body ~* 'eD'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            FILTER
                Text.body ~* 'ed([S\s]|$)'
            ORDER BY Text.body;
        """, [
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        ])

    async def test_edgeql_select_type01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    __class__: {
                        name
                    }
                }
            FILTER
                Issue.number = '1';
        ''', [
            [{
                'number': '1',
                '__class__': {'name': 'test::Issue'},
            }],
        ])

    async def test_edgeql_select_type02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User.__class__.name LIMIT 1;
        ''', [
            ['test::User']
        ])

    async def test_edgeql_select_type03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User.name.__class__.name LIMIT 1;
        ''', [
            ['std::str']
        ])

    async def test_edgeql_select_type04(self):
        # Make sure that the __class__ attribute gets the same object
        # as a direct schema::Concept query. As long as this is true,
        # we can test the schema separately without any other data.
        res1 = await self.con.execute(r'''
            WITH MODULE test
            SELECT User {
                __class__: {
                    name,
                    id,
                }
            } LIMIT 1;
        ''')

        res2 = await self.con.execute(r'''
            WITH MODULE schema
            SELECT `Concept` {
                name,
                id,
            } FILTER `Concept`.name = 'test::User';
        ''')

        self.assert_data_shape(res1[0][0]['__class__'], res2[0][0])

    # Recursion isn't working properly yet
    @unittest.expectedFailure
    async def test_edgeql_select_recursive01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    <related_to: {
                        number,
                    },
                }
            FILTER
                Issue.number = '2';

            WITH MODULE test
            SELECT
                Issue {
                    number,
                    <related_to *1
                }
            FILTER
                Issue.number = '2';
        ''', [
            [{
                'number': '2',
                'related_to': [{
                    'number': '3',
                }]
            }],
            [{
                'number': '2',
                'related_to': [{
                    'number': '3',
                }]
            }],
        ])

    async def test_edgeql_select_limit01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2;

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT 3;

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2 LIMIT 3;
        ''', [
            [{'number': '3'}, {'number': '4'}],
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
            [{'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_limit02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 1 + 1;

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT 6 / 2;

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 4 - 2 LIMIT 5 * 2 - 7;
        ''', [
            [{'number': '3'}, {'number': '4'}],
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
            [{'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_limit03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET (SELECT count(ALL Status));

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT (SELECT count(ALL Status) + 1);

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET (SELECT count(ALL Status))
            LIMIT (SELECT count(ALL Priority) + 1);
        ''', [
            [{'number': '3'}, {'number': '4'}],
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
            [{'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_limit04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    <owner: Issue{
                        number
                    } ORDER BY User.<owner[IS Issue].number
                      LIMIT 1
                }
            ORDER BY User.name;
        ''', [
            [{
                'name': 'Elvis',
                'owner': [{'number': '1'}],
            }, {
                'name': 'Yury',
                'owner': [{'number': '2'}],
            }]
        ])

    async def test_edgeql_select_limit05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                User {
                    name,
                    <owner: Issue{
                        number
                    } ORDER BY User.<owner[IS Issue].number
                      LIMIT len(User.name) - 3
                }
            ORDER BY User.name;
        ''', [
            [{
                'name': 'Elvis',
                'owner': [{'number': '1'}, {'number': '4'}],
            }, {
                'name': 'Yury',
                'owner': [{'number': '2'}],
            }]
        ])

    async def test_edgeql_select_limit06(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            await self.con.execute("""
                WITH MODULE test
                SELECT
                    User { name }
                LIMIT User.<owner[IS Issue].number;
            """)

    async def test_edgeql_select_limit07(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            await self.con.execute("""
                WITH MODULE test
                SELECT
                    User { name }
                OFFSET User.<owner[IS Issue].number;
            """)

    async def test_edgeql_select_specialized01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Text {body}
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {
                    Issue.name,
                    body,
                }
            ORDER BY Text.body;
        ''', [
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render data '
                         'in tabular format.'}
            ],
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
        ])

    async def test_edgeql_select_specialized02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User{
                name,
                <owner: LogEntry {
                    body
                },
            } FILTER User.name = 'Elvis';
        ''', [
            [{
                'name': 'Elvis',
                'owner': [
                    {'body': 'Rewriting everything.'}
                ],
            }],
        ])

    async def test_edgeql_select_specialized03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User{
                name,
                <owner: Issue {
                    number
                } FILTER <int>(User.<owner[IS Issue].number) < 3,
            } FILTER User.name = 'Elvis';
        ''', [
            [{
                'name': 'Elvis',
                'owner': [
                    {'number': '1'},
                ],
            }],
        ])

    async def test_edgeql_select_shape01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{
                number,
                related_to: {
                    number
                } FILTER Issue.related_to.owner = Issue.owner,
            } ORDER BY Issue.number;
        ''', [
            [{
                'number': '1',
                'related_to': None
            }, {
                'number': '2',
                'related_to': None
            }, {
                'number': '3',
                'related_to': [
                    {'number': '2'}
                ]
            }, {
                'number': '4',
                'related_to': None
            }],
        ])

    async def test_edgeql_select_shape02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User{
                name,
                <owner: Issue {
                    number
                } FILTER EXISTS User.<owner[IS Issue].related_to,
            } ORDER BY User.name;
        ''', [
            [{
                'name': 'Elvis',
                'owner': [{
                    'number': '4'
                }]
            }, {
                'name': 'Yury',
                'owner': [{
                    'number': '3'
                }]
            }],
        ])

    async def test_edgeql_select_shape03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User{
                name,
                <owner: Issue {
                    number
                } ORDER BY .number DESC,
            } ORDER BY User.name;
        ''', [
            [{
                'name': 'Elvis',
                'owner': [{
                    'number': '4'
                }, {
                    'number': '1'
                }]
            }, {
                'name': 'Yury',
                'owner': [{
                    'number': '3'
                }, {
                    'number': '2'
                }]
            }],
        ])

    async def test_edgeql_select_instance01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Text {body}
            FILTER Text IS Comment
            ORDER BY Text.body;
        ''', [
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
        ])

    async def test_edgeql_select_instance02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Text {body}
            FILTER Text IS NOT (Comment, Issue)
            ORDER BY Text.body;
        ''', [
            [
                {'body': 'Rewriting everything.'},
            ],
        ])

    async def test_edgeql_select_instance03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Text {body}
            FILTER Text IS Issue AND Text[IS Issue].number = '1'
            ORDER BY Text.body;
        ''', [
            [
                {'body': 'Initial public release of EdgeDB.'},
            ],
        ])

    async def test_edgeql_select_setops01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                (Issue UNION Comment) {
                    Issue.name,
                    Text.body
                };
        ''')

        # sorting manually to test basic functionality first
        for r in res:
            r.sort(key=lambda x: x['body'])

        self.assert_data_shape(res, [
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
        ])

    async def test_edgeql_select_setops02(self):
        res = await self.con.execute(r'''
            WITH
                MODULE test,
                Obj := (SELECT Issue UNION Comment)
            SELECT Obj {
                Issue.name,
                Text.body
            };

            WITH
                MODULE test,
                Obj := (SELECT Issue UNION Comment)
            SELECT Obj[IS Text] { body }
            ORDER BY Obj[IS Text].body;
        ''')

        self.assert_data_shape(res, [
            res[0],
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'We need to be able to render '
                         'data in tabular format.'}
            ],
        ])

    async def test_edgeql_select_setops03(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue {
                number,
                # open := 'yes' IF Issue.status.name = 'Open' ELSE 'no'
                # equivalent to
                open := (SELECT SINGLETON
                    (SELECT 'yes' FILTER Issue.status.name = 'Open')
                    UNION
                    (SELECT 'no' FILTER NOT Issue.status.name = 'Open')
                )
            }
            ORDER BY Issue.number;
            """, [
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
        ])

    async def test_edgeql_select_setops04(self):
        await self.assert_query_result(r"""
            # equivalent to ?=
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                # Issue.priority.name ?= 'High'
                # equivalent to this via an if/else translation
                #
                (SELECT Issue.priority.name = 'High'
                 FILTER EXISTS Issue.priority.name)
                UNION
                (SELECT EXISTS Issue.priority.name = TRUE
                 FILTER NOT EXISTS Issue.priority.name)
            ORDER BY Issue.number;
        """, [
            [{'number': '2'}],
        ])

    async def test_edgeql_select_order01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC EMPTY LAST THEN Issue.name;

            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC EMPTY FIRST THEN Issue.name;
        ''', [
            [
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
            ],
            [
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
            ]
        ])

    async def test_edgeql_select_order02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Text {body}
            ORDER BY len(Text.body) DESC;
        ''', [[
            {'body': 'We need to be able to render data in tabular format.'},
            {'body': 'Fix regression introduced by lexer tweak.'},
            {'body': 'Initial public release of EdgeDB.'},
            {'body': 'EdgeDB needs to happen soon.'},
            {'body': 'Rewriting everything.'},
            {'body': 'Minor lexer tweaks.'}
        ]])

    async def test_edgeql_select_order03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {name}
            ORDER BY (
                SELECT sum(ALL <int>User.<watchers.number)
            );
        ''', [
            [
                {'name': 'Yury'},
                {'name': 'Elvis'},
            ]
        ])

    async def test_edgeql_select_order04(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            await self.con.execute("""
                WITH MODULE test
                SELECT
                    User { name }
                ORDER BY User.<owner[IS Issue].number;
            """)

    async def test_edgeql_select_where01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment with non-empty body
            FILTER Issue.owner.<owner[IS Comment].body != ''
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_where02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment to it
            FILTER Issue.owner.<owner[IS Comment].issue = Issue;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_where03(self):
        await self.assert_query_result(r'''
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
            ''', [
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
        ])

    async def test_edgeql_select_func01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT std::len(User.name) ORDER BY User.name;

            WITH MODULE test
            SELECT std::sum(ALL <std::int>Issue.number);
        ''', [
            [5, 4],
            [10]
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_func02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT std::lower(string := User.name) ORDER BY User.name;
        ''', [
            ['elvis', 'yury'],
        ])

    async def test_edgeql_select_func05(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::concat1(*std::any) RETURNING std::str
                FROM SQL FUNCTION 'concat';
        ''')

        await self.assert_query_result(r'''
            SELECT schema::Function {
                params: {
                    num,
                    variadic,
                    type: {
                        name
                    }
                }
            } FILTER schema::Function.name = 'test::concat1';
        ''', [
            [{'params': [
                {
                    'num': 1,
                    'variadic': True,
                    'type': {
                        'name': 'std::any'
                    }
                }
            ]}]
        ])

        await self.assert_query_result(r'''
            SELECT test::concat1('aaa');
            SELECT test::concat1('aaa', 'bbb');
            SELECT test::concat1('aaa', 'bbb', 22);
        ''', [
            ['aaa'],
            ['aaabbb'],
            ['aaabbb22'],
        ])

        await self.con.execute(r'''
            DROP FUNCTION test::concat1(*std::any);
        ''')

    async def test_edgeql_select_func06(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::concat2(*std::str) RETURNING std::str
                FROM SQL FUNCTION 'concat';
        ''')

        with self.assertRaisesRegex(exc.EdgeQLError,
                                    'could not find a function'):
            await self.con.execute(r'SELECT test::concat2(123);')

    async def test_edgeql_select_func07(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::concat3($sep: std::str, *std::str)
                RETURNING std::str
                FROM SQL FUNCTION 'concat_ws';
        ''')

        await self.assert_query_result(r'''
            SELECT schema::Function {
                params: {
                    num,
                    name,
                    variadic,
                    type: {
                        name
                    }
                } ORDER BY schema::Function.params.num ASC
            } FILTER schema::Function.name = 'test::concat3';
        ''', [
            [{'params': [
                {
                    'num': 1,
                    'name': 'sep',
                    'variadic': False,
                    'type': {
                        'name': 'std::str'
                    }
                },
                {
                    'num': 2,
                    'name': None,
                    'variadic': True,
                    'type': {
                        'name': 'std::str'
                    }
                }
            ]}]
        ])

        with self.assertRaisesRegex(exc.EdgeQLError,
                                    'could not find a function'):
            await self.con.execute(r'SELECT test::concat3(123);')

        with self.assertRaisesRegex(exc.EdgeQLError,
                                    'could not find a function'):
            await self.con.execute(r'SELECT test::concat3("a", 123);')

        await self.assert_query_result(r'''
            SELECT test::concat3('|', '1');
            SELECT test::concat3('+', '1', '2');
        ''', [
            ['1'],
            ['1+2'],
        ])

        await self.con.execute(r'''
            DROP FUNCTION test::concat3($sep: std::str, *std::str);
        ''')

    async def test_edgeql_select_func08(self):
        await self.assert_query_result(r'''
            SELECT len('111');
            SELECT len(<std::bytes>'abcdef');
            SELECT len([1, 2, 3, 4]);
        ''', [
            [3],
            [6],
            [4],
        ])

        time = (await self.con.execute('SELECT std::current_time();'))[0][0]
        self.assertRegex(time, r'\d+:\d+:\d+.*')

        date = (await self.con.execute('SELECT std::current_date();'))[0][0]
        self.assertRegex(date, r'\d+-\d+-\d+')

    async def test_edgeql_select_func09(self):
        await self.con.execute('''
            CREATE FUNCTION test::my_edgeql_func1(std::str)
                RETURNING std::str
                FROM EdgeQL $$
                    SELECT 'str=' + $1
                $$;
        ''')

        await self.assert_query_result(r'''
            SELECT test::my_edgeql_func1('111');
        ''', [
            ['str=111'],
        ])

        await self.con.execute('''
            DROP FUNCTION test::my_edgeql_func1(std::str);
        ''')

    async def test_edgeql_select_exists01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS Issue.time_estimate
            ORDER BY
                Issue.number;

            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                EXISTS Issue.time_estimate
            ORDER BY
                Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [{'number': '1'}],
        ])

    async def test_edgeql_select_exists02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS (Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS (SELECT Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                EXISTS (Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_exists05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_exists06(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using concepts
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_exists07(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_exists08(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using concepts
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_exists09(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists10(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using concepts
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists11(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists12(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using concepts
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists13(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment
            FILTER EXISTS Issue.owner.<owner[IS Comment]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists14(self):
        await self.assert_query_result(r'''
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
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_exists15(self):
        await self.assert_query_result(r'''
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
        ''', [
            [{'number': '4'}],
        ])

    async def test_edgeql_select_exists16(self):
        await self.assert_query_result(r'''
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
        ''', [
            [{'number': '4'}],
        ])

    async def test_edgeql_select_exists17(self):
        await self.assert_query_result(r'''
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
        ''', [
            [{'number': '4'}],
        ])

    async def test_edgeql_select_exists18(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT EXISTS Issue
            FILTER Issue.status.name = 'Open';
        ''', [
            [True, True],
        ])

    async def test_edgeql_select_exists19(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT EXISTS (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            );
        ''', [
            [True],
        ])

    async def test_edgeql_select_exists20(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT EXISTS Issue
            FILTER Issue.status.name = 'Open'
            ORDER BY Issue.number;
        ''', [
            [True, True],
        ])

    async def test_edgeql_select_coalesce01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{
                kind := Issue.priority.name ?? Issue.status.name
            }
            ORDER BY Issue.number;
        ''', [
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        ])

    async def test_edgeql_select_coalesce02(self):
        with self.assertRaisesRegex(exc.EdgeQLError,
                                    'coalescing .* operands of related types'):

            await self.con.execute(r'''
                WITH MODULE test
                SELECT Issue{
                    kind := Issue.priority.name ?? Issue.number
                };
            ''')

    async def test_edgeql_select_coalesce03(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority
            ORDER BY Issue.number;
        ''')

        issues_h, issues_n = res

        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name ?? 'High' = 'High'
            ORDER BY
                Issue.priority.name EMPTY LAST THEN Issue.number;
        ''')

        self.assert_data_shape(res, [
            issues_h + issues_n
        ])

    async def test_edgeql_select_equivalence01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                number,
                h1 := Issue.priority.name = 'High',
                h2 := Issue.priority.name ?= 'High',
                l1 := Issue.priority.name != 'High',
                l2 := Issue.priority.name ?!= 'High'
            }
            ORDER BY Issue.number;
        ''', [
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
        ])

    async def test_edgeql_select_equivalence02(self):
        await self.assert_query_result(r'''
            # get Issues such that there's another Issue with
            # equivalent priority
            #
            WITH
                MODULE test,
                I2 := Issue
            SELECT Issue {number}
            FILTER
                I2 != Issue
                AND
                I2.priority.name ?= Issue.priority.name
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_equivalence03(self):
        await self.assert_query_result(r'''
            # get Issues with priority equivalent to empty
            #
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                Issue.priority.name ?= <str>{}
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_equivalence04(self):
        await self.assert_query_result(r'''
            # get Issues with priority equivalent to empty
            #
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                NOT Issue.priority.name ?!= <str>{}
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_as01(self):
        # NOTE: for the expected ordering of Text see instance04 test
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Text[IS Issue].number
            ORDER BY Text.body;
        ''', [
            ['4', '1', '3', '2'],
        ])

    async def test_edgeql_select_as02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Text[IS Issue].name
            FILTER Text.body LIKE '%EdgeDB%'
            ORDER BY Text[IS Issue].name;
        ''', [
            ['Release EdgeDB']
        ])

    async def test_edgeql_select_and01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [],
        ])

    async def test_edgeql_select_and02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [],
        ])

    async def test_edgeql_select_and03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '4'}],
        ])

    async def test_edgeql_select_and04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '4'}],
        ])

    async def test_edgeql_select_and05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_and06(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_and07(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_and08(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_or01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'Low'
            ORDER BY Issue.number;
        ''')

        issues_h, issues_l = res

        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
            ORDER BY Issue.priority.name THEN Issue.number;
        ''')

        self.assert_data_shape(res, [
            issues_h + issues_l,
        ])

    async def test_edgeql_select_or04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            FILTER
                Issue.priority.name IN {'High', 'Low'}
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}],
            # it so happens that all low priority issues are also closed
            [{'number': '2'}, {'number': '3'}],
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_or05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;

            # should be identical
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_or06(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_or07(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_or08(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        ])

    async def test_edgeql_select_or09(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        ])

    async def test_edgeql_select_or10(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_or11(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_or12(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_or13(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_or14(self):
        await self.assert_query_result(r'''
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
        ''', [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_or15(self):
        await self.assert_query_result(r'''
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
        ''', [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_not01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            FILTER Issue.priority.name != 'High'
            ORDER BY Issue.number;
       ''', [
            [{'number': '3'}],
            [{'number': '3'}],
        ])

    async def test_edgeql_select_not02(self):
        # testing double negation
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER NOT NOT NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            FILTER NOT NOT Issue.priority.name != 'High'
            ORDER BY Issue.number;
       ''', [
            [{'number': '3'}],
            [{'number': '3'}],
        ])

    async def test_edgeql_select_not03(self):
        # test that: a OR b = NOT( NOT a AND NOT b)
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                NOT (
                    NOT Issue.priority.name = 'High'
                    AND
                    NOT Issue.status.name = 'Closed'
                )
            ORDER BY Issue.number;
       ''', [
            # this is the result from or04
            #
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_empty01(self):
        await self.assert_query_result(r"""
            # This is not the same as checking that number does not EXIST.
            # Any binary operator with one operand as empty results in an
            # empty result, because the cross product of anything with an
            # empty set is empty.
            #
            SELECT test::Issue.number = <str>{};
            """, [
            [],
        ])

    async def test_edgeql_select_empty02(self):
        await self.assert_query_result(r"""
            # Test short-circuiting operations with empty
            #
            SELECT test::Issue.number = '1' OR <bool>{};
            SELECT test::Issue.number = 'X' OR <bool>{};
            SELECT test::Issue.number = '1' AND <bool>{};
            SELECT test::Issue.number = 'X' AND <bool>{};
            """, [
            [],
            [],
            [],
            [],
        ])

    async def test_edgeql_select_empty03(self):
        await self.assert_query_result(r"""
            # Test short-circuiting operations with empty
            #
            SELECT count(ALL test::Issue.number = '1' OR <bool>{});
            SELECT count(ALL test::Issue.number = 'X' OR <bool>{});
            SELECT count(ALL test::Issue.number = '1' AND <bool>{});
            SELECT count(ALL test::Issue.number = 'X' AND <bool>{});
            """, [
            [0],
            [0],
            [0],
            [0],
        ])

    async def test_edgeql_select_cross01(self):
        await self.assert_query_result(r"""
            # the cross product of status and priority names
            WITH MODULE test
            SELECT Status.name + Priority.name
            ORDER BY Status.name THEN Priority.name;
            """, [
            ['ClosedHigh', 'ClosedLow', 'OpenHigh', 'OpenLow'],
        ])

    async def test_edgeql_select_cross02(self):
        await self.assert_query_result(r"""
            # status and priority name for each issue
            WITH MODULE test
            SELECT Issue.status.name + Issue.priority.name
            ORDER BY Issue.number;
            """, [
            ['OpenHigh', 'ClosedLow'],
        ])

    async def test_edgeql_select_cross03(self):
        await self.assert_query_result(r"""
            # cross-product of all user names and issue numbers
            WITH MODULE test
            SELECT User.name + Issue.number
            ORDER BY User.name THEN Issue.number;
            """, [
            ['Elvis1', 'Elvis2', 'Elvis3', 'Elvis4',
             'Yury1', 'Yury2', 'Yury3', 'Yury4'],
        ])

    async def test_edgeql_select_cross04(self):
        await self.assert_query_result(r"""
            # concatenate the user name with every issue number that user has
            WITH MODULE test
            SELECT User.name + User.<owner[IS Issue].number
            ORDER BY User.name THEN User.<owner[IS Issue].number;
            """, [
            ['Elvis1', 'Elvis4', 'Yury2', 'Yury3'],
        ])

    async def test_edgeql_select_cross05(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            # tuples will not exist for the Issue without watchers
            SELECT _ := (Issue.owner.name, Issue.watchers.name)
            ORDER BY _;
            """, [
            [['Elvis', 'Yury'], ['Yury', 'Elvis'], ['Yury', 'Elvis']],
        ])

    async def test_edgeql_select_cross06(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            # tuples will not exist for the Issue without watchers
            SELECT _ := Issue.owner.name + Issue.watchers.name
            ORDER BY _;
            """, [
            ['ElvisYury', 'YuryElvis', 'YuryElvis'],
        ])

    async def test_edgeql_select_cross07(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT _ := count(ALL Issue.owner.name + Issue.watchers.name);

            WITH MODULE test
            SELECT _ := count(DISTINCT Issue.owner.name + Issue.watchers.name);
            """, [
            [3],
            [2],
        ])

    async def test_edgeql_select_cross08(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT _ := Issue.owner.name + <str>count(ALL Issue.watchers.name)
            ORDER BY _;
            """, [
            ['Elvis0', 'Elvis1', 'Yury1', 'Yury1'],
        ])

    async def test_edgeql_select_cross09(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT _ := count(ALL
                Issue.owner.name + <str>count(ALL Issue.watchers.name));
            """, [
            [4],
        ])

    async def test_edgeql_select_cross10(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                # this select shows all the relevant data for next tests
                x := (SELECT Issue {
                    name := Issue.owner.name,
                    w := count(ALL Issue.watchers.name),
                })
            SELECT count(ALL x.name + <str>x.w);
            """, [
            [4],
        ])

    async def test_edgeql_select_cross11(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT count(ALL
                Issue.owner.name +
                <str>count(ALL Issue.watchers) +
                <str>Issue.time_estimate ?? '0'
            );
            """, [
            [4],
        ])

    async def test_edgeql_select_cross12(self):
        # Same as cross11, but without coalescing the time_estimate,
        # which should collapse the counted set to a single element.
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT count(ALL
                Issue.owner.name +
                <str>count(ALL Issue.watchers) +
                <str>Issue.time_estimate
            );
            """, [
            [1],
        ])

    async def test_edgeql_select_cross13(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT count(ALL count(ALL Issue.watchers));

            WITH MODULE test
            SELECT count(ALL
                (Issue, count(ALL Issue.watchers))
            );
            """, [
            [1],
            [4],
        ])

    async def test_edgeql_select_subqueries01(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                Issue2 := (SELECT Issue)
            # this is string concatenation, not integer arithmetic
            SELECT Issue.number + Issue2.number
            ORDER BY Issue.number + Issue2.number;
            """, [
            ['{}{}'.format(a, b) for a in range(1, 5) for b in range(1, 5)],
        ])

    async def test_edgeql_select_subqueries02(self):
        await self.assert_query_result(r"""
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
            """, [
            [],
        ])

    async def test_edgeql_select_subqueries03(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                sub := (SELECT Issue FILTER Issue.number IN {'1', '6'})
            SELECT Issue{number}
            FILTER
                Issue.number IN {'2', '3', '4'}
                AND
                EXISTS (
                    (SELECT sub FILTER sub = Issue)
                );
            """, [
            [],
        ])

    async def test_edgeql_select_subqueries04(self):
        await self.assert_query_result(r"""
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
                EXISTS sub;
            """, [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_subqueries05(self):
        await self.assert_query_result(r"""
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
            """, [
            [],
        ])

    async def test_edgeql_select_subqueries06(self):
        await self.assert_query_result(r"""
            # find all issues such that there's at least one more
            # issue with the same priority (even if the "same" means empty)
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
                (
                    Issue.priority = Issue2.priority
                        IF EXISTS Issue.priority AND EXISTS Issue2.priority
                    ELSE
                        EXISTS Issue.priority = EXISTS Issue2.priority
                );
            """, [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_subqueries07(self):
        await self.assert_query_result(r"""
            # find all issues such that there's at least one more
            # issue watched by the same user as this one
            WITH MODULE test
            SELECT Issue{number}
            FILTER
                EXISTS Issue.watchers
                AND
                EXISTS (
                    SELECT User.<watchers
                    FILTER
                        User = Issue.watchers
                        AND
                        User.<watchers != Issue
                );
            """, [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_subqueries08(self):
        await self.assert_query_result(r"""
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
                );
            """, [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_subqueries09(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            SELECT Issue.number + (SELECT Issue.number);
        """)

        for r in res:
            r.sort()

        self.assert_data_shape(res, [
            ['11', '22', '33', '44'],
        ])

    async def test_edgeql_select_subqueries10(self):
        res = await self.con.execute(r"""
            WITH
                MODULE test,
                sub := (SELECT Issue.number)
            SELECT
                Issue.number + sub;
        """)

        for r in res:
            r.sort()

        self.assert_data_shape(res, [
            ['11', '12', '13', '14', '21', '22', '23', '24',
             '31', '32', '33', '34', '41', '42', '43', '44']
        ])

    async def test_edgeql_select_subqueries11(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Text{
                Issue.number,
                body_length := len(Text.body)
            } ORDER BY len(Text.body);

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
            """, [
            [
                {'number': '3', 'body_length': 19},
                {'number': None, 'body_length': 21},
                {'number': None, 'body_length': 28},
                {'number': '1', 'body_length': 33},
                {'number': '4', 'body_length': 41},
                {'number': '2', 'body_length': 52},
            ],
            [{'number': '1'}, {'number': '3'}],
        ])

    async def test_edgeql_select_subqueries12(self):
        await self.assert_query_result(r"""
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
            """, [
            [{
                'number': '1',
                'body_length': 33,
            }, {
                'number': '3',
                'body_length': 19,
            }],
        ])

    async def test_edgeql_select_subqueries13(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT User{name}
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = User
                );
            """, [
            [{'name': 'Elvis'}],
        ])

    async def test_edgeql_select_subqueries14(self):
        await self.assert_query_result(r"""
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
                        User.<watchers
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
            """, [
            [
                {'number': '2'},
                {'number': '3'}
            ],
        ])

    async def test_edgeql_select_subqueries15(self):
        await self.assert_query_result(r"""
            # testing IN and a subquery
            WITH MODULE test
            SELECT Comment{body}
            FILTER
                Comment.owner IN (
                    SELECT User
                    FILTER
                        User.name = 'Elvis'
                );
            """, [
            [{'body': 'EdgeDB needs to happen soon.'}],
        ])

    async def test_edgeql_select_subqueries16(self):
        await self.assert_query_result(r"""
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
            """, [
            [{'body': 'EdgeDB needs to happen soon.'}],
        ])

    async def test_edgeql_select_view_indirection01(self):
        await self.assert_query_result(r"""
            # Direct reference to a computable element in a subquery
            WITH MODULE test
            SELECT
                (
                    SELECT User {
                        num_issues := count(ALL User.<owner[IS Issue])
                    } FILTER .name = 'Elvis'
                ).num_issues;
            """, [
            [2],
        ])

    async def test_edgeql_select_view_indirection02(self):
        await self.assert_query_result(r"""
            # Reference to a computable element in a subquery
            # defined as an inline view.
            WITH MODULE test,
                U := (
                    SELECT User {
                        num_issues := count(ALL User.<owner[IS Issue])
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.num_issues;
            """, [
            [2],
        ])

    async def test_edgeql_select_view_indirection03(self):
        await self.assert_query_result(r"""
            # Reference a computed object set in a view.
            WITH MODULE test,
                U := (
                    WITH U2 := (SELECT User)
                    SELECT User {
                        friend := (
                            SELECT SINGLETON U2 FILTER U2.name = 'Yury'
                        )
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.friend.name;
            """, [
            ['Yury'],
        ])

    async def test_edgeql_select_view_indirection04(self):
        result = await self.con.execute(r"""
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

        self.assertEqual(len(result[0]), 2)

    async def test_edgeql_select_view_indirection05(self):
        await self.assert_query_result(r"""
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
            """, [
            [True],
        ])

    async def test_edgeql_select_view_indirection06(self):
        await self.assert_query_result(r"""
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
            """, [
            ['1', '4'],
        ])

    async def test_edgeql_select_view_indirection07(self):
        await self.assert_query_result(r"""
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
            """, [
            [],
        ])

    async def test_edgeql_select_view_indirection08(self):
        await self.assert_query_result(r"""
            # A slightly more complex view.
             WITH MODULE test,
                 U := (
                     WITH U2 := (SELECT User)
                     SELECT User {
                         friends := (
                             SELECT U2 { foo := U2.name + '!' }
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
            """, [

            [{
                'my_issues': ['1', '4'],
                'friends_foos': ['Yury!'],
                'friends_issues': ['2', '3']
            }]
        ])

    async def test_edgeql_select_view_indirection09(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                sub := (
                    SELECT SINGLETON
                        Text {
                            foo := Text.body + '!'
                        }
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
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
        ''', [
            [{
                'name': 'Elvis',
                'shortest_text_shape': {
                    'body': 'Minor lexer tweaks.',
                    'foo': 'Minor lexer tweaks.!',
                },
            }]
        ])

    async def test_edgeql_select_view_indirection10(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                sub := (
                    SELECT SINGLETON
                        Text {
                            foo := Text.body + '!'
                        }
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                User {
                    name,
                    shortest_text_foo := sub.foo
                }
            FILTER User.name = 'Elvis';
        ''', [
            [{
                'name': 'Elvis',
                'shortest_text_foo': 'Minor lexer tweaks.!'
            }]
        ])

    async def test_edgeql_select_view_indirection11(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                Developers := (
                    SELECT
                        User {
                            open_issues := (
                                SELECT
                                    Issue {
                                        spent_time := (
                                            SELECT SINGLETON
                                                sum(ALL Issue.time_spent_log
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

        ''', [
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
        ])

    async def test_edgeql_select_slice01(self):
        await self.assert_query_result(r"""
            # full name of the Issue is 'Release EdgeDB'
            WITH MODULE test
            SELECT Issue.name[2]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[-2]
            FILTER Issue.number = '1';

            WITH MODULE test
            SELECT Issue.name[2:4]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[2:]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[:2]
            FILTER Issue.number = '1';

            WITH MODULE test
            SELECT Issue.name[2:-1]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[-2:]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[:-2]
            FILTER Issue.number = '1';
            """, [
            ['l'],
            ['D'],

            ['le'],
            ['lease EdgeDB'],
            ['Re'],

            ['lease EdgeD'],
            ['DB'],
            ['Release Edge'],
        ])

    async def test_edgeql_select_slice02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue.__class__.name
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[2]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[-2]
            FILTER Issue.number = '1';

            WITH MODULE test
            SELECT Issue.__class__.name[2:4]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[2:]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[:2]
            FILTER Issue.number = '1';

            WITH MODULE test
            SELECT Issue.__class__.name[2:-1]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[-2:]
            FILTER Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[:-2]
            FILTER Issue.number = '1';
        """, [
            ['test::Issue'],
            ['s'],
            ['u'],

            ['st'],
            ['st::Issue'],
            ['te'],

            ['st::Issu'],
            ['ue'],
            ['test::Iss'],
        ])

    async def test_edgeql_select_slice03(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue{
                name,
                type_name := Issue.__class__.name,
                a := Issue.name[2],
                b := Issue.name[2:-1],
                c := Issue.__class__.name[2:-1],
            }
            FILTER Issue.number = '1';
        """, [
            [{
                'name': 'Release EdgeDB',
                'type_name': 'test::Issue',
                'a': 'l',
                'b': 'lease EdgeD',
                'c': 'st::Issu',
            }],
        ])

    async def test_edgeql_select_tuple01(self):
        await self.assert_query_result(r"""
            # get tuples (status, number of issues)
            WITH MODULE test
            SELECT (Status.name, count(ALL Status.<status))
            ORDER BY Status.name;
            """, [
            [['Closed', 2], ['Open', 2]]
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_select_tuple02(self):
        await self.assert_query_result(r"""
            # nested tuples
            WITH MODULE test
            SELECT
                _ := (
                    User.name, (
                        User.<owner[IS Issue].status.name,
                        count(ALL User.<owner[IS Issue])
                    )
                )
                # A tuple is essentially an identity function within our
                # set operation semantics, so here we're selecting a cross
                # product of all user names with user owned issue statuses.
                #
            ORDER BY _.0 THEN _.1;
            """, [[
            ['Elvis', ['Closed', 1]],
            ['Elvis', ['Open', 1]],
            ['Yury', ['Closed', 1]],
            ['Yury', ['Open', 1]],
        ]])

    async def test_edgeql_select_tuple03(self):
        await self.assert_query_result(r"""
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
            """, [[
            {'name': 'Elvis'},
            {'name': 'Yury'},
        ]])

    async def test_edgeql_select_tuple04(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                User {
                    t := {(1, 2), (3, 4)}
                }
            FILTER
                User.name = 'Elvis'
            ORDER BY
                User.name;
            """, [[
            {'t': [[1, 2], [3, 4]]},
        ]])

    async def test_edgeql_select_struct01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT (
                statuses := count(ALL Status),
                issues := count(ALL Issue),
            );
            """, [
            [{'statuses': 2, 'issues': 4}],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_select_struct02(self):
        # Struct in a common set expr.
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                counts := (SELECT (
                    statuses := count(ALL Status),
                    issues := count(ALL Issue),
                ))
            SELECT
                counts.statuses + counts.issues;
            """, [
            [6],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_select_struct03(self):
        # Object in a struct.
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                criteria := (SELECT (
                    user := (SELECT SINGLETON
                             User FILTER User.name = 'Yury'),
                    status := (SELECT SINGLETON
                               Status FILTER Status.name = 'Open'),
                ))
            SELECT
                Issue.number
            FILTER
                Issue.owner = criteria.user
                AND Issue.status = criteria.status;
            """, [
            ['2'],
        ])

    async def test_edgeql_select_struct04(self):
        # Object in a struct returned directly.
        await self.assert_query_result(r"""
            WITH
                MODULE test
            SELECT
                (
                    user := (SELECT SINGLETON User{name}
                             FILTER User.name = 'Yury')
                );
            """, [
            [{
                'user': {
                    'name': 'Yury'
                }
            }],
        ])

    async def test_edgeql_select_struct05(self):
        # Object in a struct referred to directly.
        await self.assert_query_result(r"""
            WITH
                MODULE test
            SELECT
                (
                    user := (SELECT SINGLETON
                             User{name} FILTER User.name = 'Yury')
                ).user.name;
            """, [
            ['Yury'],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_select_struct06(self):
        # Struct comparison
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                U1 := User,
                U2 := User
            SELECT
                (user := (SELECT SINGLETON
                          U1{name} FILTER U1.name = 'Yury'))
                    =
                (user := (SELECT SINGLETON
                          U2{name} FILTER U2.name = 'Yury'));

            WITH
                MODULE test,
                U1 := User,
                U2 := User
            SELECT
                (user := (SELECT SINGLETON
                          U1{name} FILTER U1.name = 'Yury'))
                    =
                (user := (SELECT SINGLETON
                          U2{name} FILTER U2.name = 'Elvis'));

            WITH
                MODULE test,
                U1 := User,
                U2 := User
            SELECT
                (
                    user := (SELECT SINGLETON
                             U1{name} FILTER U1.name = 'Yury'),
                    spam := 'ham',
                )
                    =
                (
                    user := (SELECT SINGLETON
                             U2{name} FILTER U2.name = 'Yury'),
                );

            """, [
            [True],
            [False],
            [False],
        ])

    async def test_edgeql_select_linkproperty01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT User.todo@rank + <int>User.todo.number
            ORDER BY User.todo.number;
            """, [
            [43, 44, 45, 46]
        ])

    async def test_edgeql_select_linkproperty02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue.<todo@rank + <int>Issue.number
            ORDER BY Issue.number;
            """, [
            [43, 44, 45, 46]
        ])

    async def test_edgeql_select_linkproperty03(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT User {
                name,
                todo: {
                    number,
                    @rank
                } ORDER BY User.todo.number
            }
            ORDER BY User.name;
            """, [
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
        ])

    async def test_edgeql_select_if_else01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue {
                number,
                open := 'yes' IF Issue.status.name = 'Open' ELSE 'no'
            }
            ORDER BY Issue.number;
            """, [
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
        ])

    async def test_edgeql_select_if_else02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue {
                number,
                # foo is 'bar' for Issue number 1 and status name for the rest
                foo := 'bar' IF Issue.number = '1' ELSE Issue.status.name
            }
            ORDER BY Issue.number;
            """, [
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
        ])

    async def test_edgeql_select_if_else03(self):
        with self.assertRaisesRegex(exc.EdgeQLError,
                                    r'if/else clauses .* related types'):

            await self.con.execute(r"""
                WITH MODULE test
                SELECT Issue {
                    foo := 'bar' IF Issue.number = '1' ELSE 123
                };
                """)

    async def test_edgeql_select_if_else04(self):
        await self.assert_query_result(r"""
            # equivalent to coalesce
            WITH MODULE test
            SELECT Issue{
                # kind := Issue.priority.name ?? Issue.status.name
                kind := (Issue.priority.name
                         IF EXISTS Issue.priority.name
                         ELSE Issue.status.name)
            }
            ORDER BY Issue.number;
        """, [
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        ])

    async def test_edgeql_select_if_else05(self):
        await self.assert_query_result(r"""
            # equivalent to ?=
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                # Issue.priority.name ?= 'High'
                Issue.priority.name = 'High'
                    IF EXISTS Issue.priority.name AND EXISTS 'High'
                    ELSE EXISTS Issue.priority.name = EXISTS 'High'
            ORDER BY Issue.number;
        """, [
            [{'number': '2'}],
        ])

    async def test_edgeql_select_if_else06(self):
        await self.assert_query_result(r"""
            # equivalent to ?!=
            WITH MODULE test
            SELECT Issue {number}
            FILTER
                # Issue.priority.name ?!= 'High'
                Issue.priority.name != 'High'
                    IF EXISTS Issue.priority.name AND EXISTS 'High'
                    ELSE EXISTS Issue.priority.name != EXISTS 'High'
            ORDER BY Issue.number;
        """, [
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_partial_01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            FILTER
                .number = '1';
        ''', [
            [{
                'number': '1'
            }]
        ])

    async def test_edgeql_partial_02(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT
                Issue.watchers {
                    name
                }
            FILTER
                .name = 'Yury';
        ''', [
            [{
                'name': 'Yury'
            }]
        ])

    async def test_edgeql_partial_03(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
                watchers: {
                    name
                } FILTER .name = 'Yury'
            } FILTER .status.name = 'Open' AND .owner.name = 'Elvis';
        ''', [
            [{
                'number': '1',
                'watchers': [{
                    'name': 'Yury'
                }]
            }]
        ])

    async def test_edgeql_partial_04(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .number > '1'
              ORDER BY .number DESC;
        ''', [[
            {'number': '4'},
            {'number': '3'},
            {'number': '2'},
        ]])

    async def test_edgeql_partial_05(self):
        with self.assertRaisesRegex(exc.EdgeQLError,
                                    'could not resolve partial path'):
            await self.con.execute('''
                WITH MODULE test
                SELECT Issue.number FILTER .number > '1';
            ''')

    async def test_edgeql_partial_06(self):
        with self.assertRaisesRegex(exc.EdgeQLError,
                                    'could not resolve partial path'):
            await self.con.execute('''
                WITH
                    MODULE test
                SELECT
                    Issue{
                        sub := (SELECT .name)
                    }
                FILTER .number = '1';
            ''')

    async def test_edgeql_virtual_target_01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER EXISTS (.references)
              ORDER BY .number DESC;

            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .references[IS URL].address = 'https://edgedb.com'
              ORDER BY .number DESC;

            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .references[IS Named].name = 'screenshot.png'
              ORDER BY .number DESC;

            WITH MODULE test
            SELECT Issue {
                number,
                references: Named {
                    __class__: {
                        name
                    },

                    name
                } ORDER BY .name
            } FILTER EXISTS (.references)
              ORDER BY .number DESC;
        ''', [
            [{
                'number': '2'
            }],
            [{
                'number': '2'
            }],
            [{
                'number': '2'
            }],
            [{
                'number': '2',
                'references': [
                    {
                        'name': 'edgedb.com',
                        '__class__': {
                            'name': 'test::URL'
                        }
                    },
                    {
                        'name': 'screenshot.png',
                        '__class__': {
                            'name': 'test::File'
                        }
                    }
                ]
            }]
        ])

    async def test_edgeql_uniqueness01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue.owner{name}
            ORDER BY Issue.owner.name;
        ''', [[
            {'name': 'Elvis'}, {'name': 'Yury'},
        ]])

    @tb.expected_optimizer_failure
    async def test_edgeql_select_for01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR x IN {1, 4}
            SELECT Issue {
                name
            }
            FILTER
                Issue.number = <str>x
            ORDER BY
                Issue.number;
        ''', [[
            {'name': 'Release EdgeDB'},
            {'name': 'Regression.'},
        ]])
