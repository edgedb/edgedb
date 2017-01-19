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
                          'queries.eschema')

    SETUP = r"""
        WITH MODULE test
        INSERT Priority {
            name := 'High'
        };

        WITH MODULE test
        INSERT Priority {
            name := 'Low'
        };

        WITH MODULE test
        INSERT Status {
            name := 'Open'
        };

        WITH MODULE test
        INSERT Status {
            name := 'Closed'
        };


        WITH MODULE test
        INSERT User {
            name := 'Elvis'
        };

        WITH MODULE test
        INSERT User {
            name := 'Yury'
        };


        WITH MODULE test
        INSERT LogEntry {
            owner := (SELECT User WHERE User.name = 'Elvis'),
            spent_time := 50000,
            body := 'Rewriting everything.'
        };

        WITH MODULE test
        INSERT Issue {
            number := '1',
            name := 'Release EdgeDB',
            body := 'Initial public release of EdgeDB.',
            owner := (SELECT User WHERE User.name = 'Elvis'),
            watchers := (SELECT User WHERE User.name = 'Yury'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            time_spent_log := (SELECT LogEntry),
            time_estimate := 3000
        };

        WITH MODULE test
        INSERT Comment {
            body := 'EdgeDB needs to happen soon.',
            owner := (SELECT User WHERE User.name = 'Elvis'),
            issue := (SELECT Issue WHERE Issue.number = '1')
        };


        WITH MODULE test
        INSERT Issue {
            number := '2',
            name := 'Improve EdgeDB repl output rendering.',
            body := 'We need to be able to render data in tabular format.',
            owner := (SELECT User WHERE User.name = 'Yury'),
            watchers := (SELECT User WHERE User.name = 'Elvis'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            priority := (SELECT Priority WHERE Priority.name = 'High')
        };

        WITH
            MODULE test,
            I := (SELECT Issue)
        INSERT Issue {
            number := '3',
            name := 'Repl tweak.',
            body := 'Minor lexer tweaks.',
            owner := (SELECT User WHERE User.name = 'Yury'),
            watchers := (SELECT User WHERE User.name = 'Elvis'),
            status := (SELECT Status WHERE Status.name = 'Closed'),
            related_to := (
                SELECT I WHERE I.number = '2'
            ),
            priority := (SELECT Priority WHERE Priority.name = 'Low')
        };

        WITH
            MODULE test,
            I := (SELECT Issue)
        INSERT Issue {
            number := '4',
            name := 'Regression.',
            body := 'Fix regression introduced by lexer tweak.',
            owner := (SELECT User WHERE User.name = 'Elvis'),
            status := (SELECT Status WHERE Status.name = 'Closed'),
            related_to := (
                SELECT I WHERE I.number = '3'
            )
        };

        # NOTE: UPDATE Users for testing the link properties
        #
        WITH MODULE test
        UPDATE User {
            todo := (SELECT Issue WHERE Issue.number in ('1', '2'))
        } WHERE User.name = 'Elvis';
        WITH MODULE test
        UPDATE User {
            todo := (SELECT Issue WHERE Issue.number in ('3', '4'))
        } WHERE User.name = 'Yury';
    """

    async def test_edgeql_select_computable01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    aliased_number := Issue.number,
                    total_time_spent := (
                        SELECT SINGLETON
                            sum(Issue.time_spent_log.spent_time)
                    )
                }
            WHERE
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
                            sum(Issue.time_spent_log.spent_time)
                    )
                }
            WHERE
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
                        WHERE
                            (Text AS Owned).owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT
                            1
                    ),
                }
            WHERE User.name = 'Elvis';
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
            WHERE User.name = 'Elvis';
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
                        WHERE
                            (Text AS Owned).owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT
                            1
                    ),
                    shortest_text := sub {
                        body
                    },
                }
            WHERE User.name = 'Elvis';
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
                        WHERE
                            User IS User
                        ORDER
                            BY len(Text.body) ASC
                        LIMIT
                            1
                    ),
                }
            WHERE User.name = 'Elvis';
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
                        WHERE (Text AS Owned).owner != User
                        ORDER BY len(Text.body) DESC
                    ),
                }
            WHERE User.name = 'Elvis';
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
                    WHERE len(Issue.status.name) = len(User.name)
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
                name := (Text AS Issue).name IF Text IS Issue      ELSE
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
            WHERE Issue.number = '1';
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
                (sub AS Issue).number;
        ''', [
            ['3']
        ])

    async def test_edgeql_select_match01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name LIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name LIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
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
            WHERE
                Issue.name NOT LIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT LIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
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
            WHERE
                Issue.name ILIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name ILIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
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
            WHERE
                Issue.name NOT ILIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT ILIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT ILIKE '%re%'
            ORDER BY Issue.number;
        """, [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [],
        ])

    async def test_edgeql_select_match05(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name @@ 'edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.body @@ 'need'
            ORDER BY Issue.number;
        """, [
            [{'number': '1'}, {'number': '2'}],
            [{'number': '2'}],
        ])

    async def test_edgeql_select_match06(self):
        await self.assert_query_result(r"""
            # XXX: @@ used with a query operand, as opposed to a literal
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name @@ to_tsquery('edgedb & repl')
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name @@ to_tsquery('edgedb | repl')
            ORDER BY Issue.number;
        """, [
            [{'number': '2'}],
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_match07(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~ 'ed'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~ 'eD'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
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
            WHERE
                Text.body ~* 'ed'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~* 'eD'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
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
            WHERE
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
            } WHERE `Concept`.name = 'test::User';
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
            WHERE
                Issue.number = '2';

            WITH MODULE test
            SELECT
                Issue {
                    number,
                    <related_to *1
                }
            WHERE
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
            OFFSET (SELECT count(Status));

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT (SELECT count(Status) + 1);

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET (SELECT count(Status)) LIMIT (SELECT count(Priority) + 1);
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
                    } ORDER BY User.<owner[TO Issue].number
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
                    } ORDER BY User.<owner[TO Issue].number
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
            } WHERE User.name = 'Elvis';
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
                } WHERE <int>(User.<owner[TO Issue].number) < 3,
            } WHERE User.name = 'Elvis';
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
                } WHERE Issue.related_to.owner = Issue.owner,
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
                } WHERE EXISTS User.<owner[TO Issue].related_to,
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
                } ORDER BY User.<owner[TO Issue].number DESC,
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
            WHERE Text IS Comment
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
            WHERE Text IS NOT (Comment, Issue)
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
            WHERE Text IS Issue AND (Text AS Issue).number = '1'
            ORDER BY Text.body;
        ''', [
            [
                {'body': 'Initial public release of EdgeDB.'},
            ],
        ])

    async def test_edgeql_select_combined01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {name, body}
            UNION
            SELECT
                Comment {body};

            WITH MODULE test
            SELECT
                Text {body}
            INTERSECT
            SELECT
                Comment {body};

            WITH MODULE test
            SELECT
                Text {body}
            EXCEPT
            SELECT
                Comment {body};
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
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
            [
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render '
                         'data in tabular format.'}
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_combined02(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {name, body}
            UNION
            SELECT
                Comment {body}
            ORDER BY (Object AS Text).body;

            WITH MODULE test
            SELECT
                Text {body}
            INTERSECT
            SELECT
                Comment {body}
            ORDER BY (Object AS Text).body;

            WITH MODULE test
            SELECT
                Text {body}
            EXCEPT
            SELECT
                Comment {body}
            ORDER BY (Object AS Text).body;
        ''')

        self.assert_data_shape(res, [
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'We need to be able to render '
                         'data in tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ],
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
            [
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render'
                         'data in tabular format.'}
            ],
        ])

    async def test_edgeql_select_order01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC NULLS LAST THEN Issue.name;

            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC NULLS FIRST THEN Issue.name;
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
                SELECT sum(<int>User.<watchers.number)
            );
        ''', [
            [
                {'name': 'Yury'},
                {'name': 'Elvis'},
            ]
        ])

    async def test_edgeql_select_where01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment with non-empty body
            WHERE Issue.owner.<owner[TO Comment].body != ''
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_where02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment to it
            WHERE Issue.owner.<owner[TO Comment].issue = Issue;
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
            } WHERE len(Issue.status.name) = 4
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
            SELECT std::sum(<std::int>Issue.number);
        ''', [
            [5, 4],
            [10]
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_func02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT std::lower(string:=User.name) ORDER BY User.name;
        ''', [
            ['elvis', 'yury'],
        ])

    async def test_edgeql_select_func03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT std::count(User.<owner.id)
            GROUP BY User.name ORDER BY User.name;
        ''', [
            [4, 2],
        ])

    async def test_edgeql_select_func04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT sum(<int>Issue.number)
            WHERE EXISTS Issue.watchers.name
            GROUP BY Issue.watchers.name
            ORDER BY Issue.watchers.name;
        ''', [
            [5, 1],
        ])

    async def test_edgeql_select_func05(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::concat1(*std::any) RETURNING std::str
                FROM SQL FUNCTION 'concat';
        ''')

        await self.assert_query_result(r'''
            SELECT schema::Function {
                params: {
                    @paramnum,
                    @paramvariadic
                }
            } WHERE schema::Function.name = 'test::concat1';
        ''', [
            [{'params': [
                {
                    '@paramnum': 1,
                    '@paramvariadic': True
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
                    @paramnum,
                    @paramname,
                    @paramvariadic
                } ORDER BY schema::Function.params@paramnum ASC
            } WHERE schema::Function.name = 'test::concat3';
        ''', [
            [{'params': [
                {
                    '@paramnum': 1,
                    '@paramname': 'sep',
                    '@paramvariadic': False
                },
                {
                    '@paramnum': 2,
                    '@paramname': None,
                    '@paramvariadic': True
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

    async def test_edgeql_select_exists01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
                NOT EXISTS Issue.time_estimate
            ORDER BY
                Issue.number;

            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
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
            WHERE
                NOT EXISTS (Issue.<issue[TO Comment])
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
            WHERE
                NOT EXISTS (SELECT Issue.<issue[TO Comment])
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
            WHERE
                EXISTS (Issue.<issue[TO Comment])
            ORDER BY
                Issue.number;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_exists05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
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
            WHERE
                EXISTS Issue.priority.id        # has Priority [2, 3]
            ORDER BY Issue.number;
        ''', [
            [{'number': '2'}, {'number': '3'}],
        ])

    async def test_edgeql_select_exists07(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
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
            WHERE
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}],
        ])

    async def test_edgeql_select_exists09(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
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
            WHERE
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists11(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
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
            WHERE
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
            WHERE EXISTS Issue.owner.<owner[TO Comment]
            ORDER BY Issue.number;
        ''', [
            [{'number': '1'}, {'number': '4'}],
        ])

    async def test_edgeql_select_exists14(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            # issue where the owner also has a comment to it
            WHERE
                EXISTS (
                    SELECT Comment
                    WHERE
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
            WHERE
                EXISTS (
                    SELECT Comment
                    WHERE
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
            WHERE
                EXISTS (
                    SELECT Comment
                    WHERE
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
            WHERE
                EXISTS (
                    SELECT Comment
                    WHERE
                        Comment.owner = Issue.owner
                        AND
                        NOT Comment.issue = Issue
                )
            ORDER BY
                Issue.number;
        ''', [
            [{'number': '4'}],
        ])

    async def test_edgeql_select_as01(self):
        # NOTE: for the expected ordering of Text see instance04 test
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT (Text AS Issue).number
            ORDER BY Text.body;
        ''', [
            ['4', '1', '3', '2'],
        ])

    async def test_edgeql_select_as02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT (Text AS Issue).name
            WHERE Text.body @@ 'EdgeDB'
            ORDER BY (Text AS Issue).name;
        ''', [
            ['Release EdgeDB']
        ])

    async def test_edgeql_select_and01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
                Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'Low'
            ORDER BY Issue.number;
        ''')

        issues_h, issues_l = res

        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
            ORDER BY Issue.priority.name THEN Issue.number;
        ''')

        self.assert_data_shape(res, [
            issues_h + issues_l,
        ])

    async def test_edgeql_select_or02(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE
                NOT EXISTS Issue.priority
            ORDER BY Issue.number;
        ''')

        issues_h, issues_n = res

        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
                OR
                NOT EXISTS Issue.priority.name
            ORDER BY Issue.priority.name NULLS LAST THEN Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
                OR
                NOT EXISTS Issue.priority.id
            ORDER BY Issue.priority.name NULLS LAST THEN Issue.number;
        ''')

        self.assert_data_shape(res, [
            issues_h + issues_n,
            issues_h + issues_n,
        ])

    async def test_edgeql_select_or03(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE
                NOT EXISTS Issue.priority
            ORDER BY Issue.number;
        ''')

        issues_h, issues_n = res

        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
                OR
                NOT EXISTS Issue.priority
            ORDER BY Issue.priority.name NULLS LAST THEN Issue.number;
        ''')

        self.assert_data_shape(res, [
            issues_h + issues_n,
        ])

    async def test_edgeql_select_or04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE
                Issue.priority.name IN ('High', 'Low')
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
            WHERE
                NOT EXISTS Issue.priority.id
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;

            # should be identical
            WITH MODULE test
            SELECT Issue{number}
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE
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
            WHERE NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE Issue.priority.name != 'High'
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
            WHERE NOT NOT NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT Issue{number}
            WHERE NOT NOT Issue.priority.name != 'High'
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
            WHERE
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

    async def test_edgeql_select_null01(self):
        await self.assert_query_result(r"""
            SELECT test::Issue.number = NULL;
            """, [
            [None, None, None, None],
        ])

    async def test_edgeql_select_null02(self):
        await self.assert_query_result(r"""
            # the WHERE clause is always NULL, so it can never be true
            WITH MODULE test
            SELECT Issue{number}
            WHERE Issue.number = NULL;

            WITH MODULE test
            SELECT Issue{number}
            WHERE Issue.priority = NULL;

            WITH MODULE test
            SELECT Issue{number}
            WHERE Issue.priority.name = NULL;
            """, [
            [],
            [],
            [],
        ])

    async def _test_edgeql_select_cross01(self):
        await self.assert_query_result(r"""
            # the cross product of status and priority names
            WITH MODULE test
            SELECT Status.name + Priority.name
            ORDER BY Status.name THEN Priority.name;
            """, [
            ['{}{}'.format(a, b) for a in ('Closed', 'Open')
             for b in ('High', 'Low')],
        ])

    async def _test_edgeql_select_cross02(self):
        await self.assert_query_result(r"""
            # status and priority name for each issue
            WITH MODULE test
            SELECT Issue.status.name + Issue.priority.name
            ORDER BY Issue.number;
            """, [
            [None, 'OpenHigh', 'ClosedLow', None],
        ])

    async def _test_edgeql_select_cross03(self):
        await self.assert_query_result(r"""
            # cross-product of all user names and issue numbers
            WITH MODULE test
            SELECT User.name + Issue.number
            ORDER BY User.name THEN Issue.number;
            """, [
            ['{}{}'.format(a, b) for a in ('Elvis', 'Yury')
             for b in range(1, 5)],
        ])

    async def test_edgeql_select_cross04(self):
        await self.assert_query_result(r"""
            # concatenate the user name with every issue number that user has
            WITH MODULE test
            SELECT User.name + User.<owner[TO Issue].number
            ORDER BY User.name THEN User.<owner[TO Issue].number;
            """, [
            ['Elvis1', 'Elvis4', 'Yury2', 'Yury3'],
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
            WHERE
                Issue.number IN ('2', '3', '4')
                AND
                EXISTS (
                    # due to common prefix, the Issue referred to here is
                    # the same Issue as in the LHS of AND, therefore
                    # this condition can never be true
                    SELECT Issue WHERE Issue.number IN ('1', '6')
                );
            """, [
            [],
        ])

    async def test_edgeql_select_subqueries03(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                sub := (SELECT Issue WHERE Issue.number IN ('1', '6'))
            SELECT Issue{number}
            WHERE
                Issue.number IN ('2', '3', '4')
                AND
                EXISTS (
                    (SELECT sub WHERE sub = Issue)
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
                    WHERE
                        Issue.number IN ('1', '6')
                )
            SELECT
                Issue{number}
            WHERE
                Issue.number IN ('2', '3', '4')
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
            WHERE
                Issue != Issue2
                AND
                # NOTE: this condition is false when one of the sides is NULL
                Issue.priority = Issue2.priority
            ORDER BY
                Issue.number;
            """, [
            [],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_subqueries06(self):
        await self.assert_query_result(r"""
            # find all issues such that there's at least one more
            # issue with the same priority (even if the "same" means NULL)
            WITH
                MODULE test,
                Issue2:= (SELECT Issue)
            SELECT Issue{number}
            WHERE
                Issue != Issue2
                AND
                (
                    Issue.priority = Issue2.priority
                    OR

                    NOT EXISTS Issue.priority.id
                    AND
                    NOT EXISTS Issue2.priority.id
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
            WHERE
                EXISTS Issue.watchers
                AND
                EXISTS (
                    SELECT User.<watchers
                    WHERE
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
            WHERE
                EXISTS Issue.watchers
                AND
                EXISTS (
                    SELECT Text
                    WHERE
                        Text IS Issue
                        AND
                        (Text AS Issue).watchers = Issue.watchers
                        AND
                        Text != Issue
                );
            """, [
            [{'number': '2'}, {'number': '3'}],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_subqueries09(self):
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError,
                r'unexpected binop operands'):
            # XXX: I'm not actually sure that this should work, much less
            # what the result should be
            await self.assert_query_result(r"""
                WITH MODULE test
                SELECT Issue.number + (SELECT Issue.number);
                """, [
                ['1"1"', '2"2"', '3"3"', '4"4"'],
            ])

    @unittest.expectedFailure
    async def test_edgeql_select_subqueries10(self):
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError,
                r'unexpected binop operands'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    sub:= (SELECT Issue.number)
                SELECT Issue.number + sub;
                """)

    async def test_edgeql_select_subqueries11(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Text{
                Issue.number,
                body_length:= len(Text.body)
            } ORDER BY len(Text.body);

            # find all issues such that there's at least one more
            # Text item of similar body length (+/-5 characters)
            WITH MODULE test
            SELECT Issue{
                number,
            }
            WHERE
                EXISTS (
                    SELECT Text
                    WHERE
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
                body_length:= len(Issue.body)
            }
            WHERE
                EXISTS (
                    SELECT Text
                    WHERE
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
            WHERE
                EXISTS (
                    SELECT Comment
                    WHERE
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
            WHERE
                EXISTS Issue.watchers AND
                EXISTS (
                    SELECT
                        User.<watchers
                    WHERE
                        # The User is among the watchers of this Issue
                        User = Issue.watchers AND
                        # and they also watch some other Issue other than this
                        User.<watchers[TO Issue] != Issue AND
                        # and they also have at least one comment
                        EXISTS (
                            SELECT Comment WHERE Comment.owner = User
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
            WHERE
                Comment.owner IN (
                    SELECT User
                    WHERE
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
            WHERE
                Comment.owner IN (
                    SELECT User
                    WHERE
                        User.<owner IN (
                            SELECT Issue
                            WHERE
                                Issue.number = '1'
                        )
                );
            """, [
            [{'body': 'EdgeDB needs to happen soon.'}],
        ])

    async def test_edgeql_select_slice01(self):
        await self.assert_query_result(r"""
            # full name of the Issue is 'Release EdgeDB'
            WITH MODULE test
            SELECT Issue.name[2]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[-2]
            WHERE Issue.number = '1';

            WITH MODULE test
            SELECT Issue.name[2:4]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[2:]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[:2]
            WHERE Issue.number = '1';

            WITH MODULE test
            SELECT Issue.name[2:-1]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[-2:]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.name[:-2]
            WHERE Issue.number = '1';
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
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[2]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[-2]
            WHERE Issue.number = '1';

            WITH MODULE test
            SELECT Issue.__class__.name[2:4]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[2:]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[:2]
            WHERE Issue.number = '1';

            WITH MODULE test
            SELECT Issue.__class__.name[2:-1]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[-2:]
            WHERE Issue.number = '1';
            WITH MODULE test
            SELECT Issue.__class__.name[:-2]
            WHERE Issue.number = '1';
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
            WHERE Issue.number = '1';
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
            SELECT (Status.name, count(Status.<status))
            GROUP BY Status.name
            ORDER BY Status.name;
            """, [
            [['Closed', 2], ['Open', 2]]
        ])

    async def test_edgeql_select_tuple02(self):
        await self.assert_query_result(r"""
            # nested tuples
            WITH MODULE test
            SELECT
                (
                    User.name, (
                        User.<owner[TO Issue].status.name,
                        count(User.<owner[TO Issue])
                    )
                )
            GROUP BY User.name, User.<owner[TO Issue].status.name
            ORDER BY User.name THEN User.<owner[TO Issue].status.name;
            """, [[
            ['Elvis', ['Closed', 1]],
            ['Elvis', ['Open', 1]],
            ['Yury', ['Closed', 1]],
            ['Yury', ['Open', 1]],
        ]])

    @unittest.expectedFailure
    async def test_edgeql_select_anon01(self):
        await self.assert_query_result(r"""
            # get shapes {'status': ..., 'count': ...}
            WITH MODULE test
            SELECT {
                status := Status.name,
                count := count(Status.<status),
            }
            GROUP BY Status.name
            ORDER BY Status.name;
            """, [
            {'status': 'Closed', 'count': 2},
            {'status': 'Open', 'count': 2}
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_anon02(self):
        await self.assert_query_result(r"""
            # nested shapes
            WITH MODULE test
            SELECT
                {
                    name := User.name,
                    issue := (SELECT {
                        status := User.<owner[TO Issue].status.name,
                        count := count(User.<owner[TO Issue]),
                    } GROUP BY User.<owner[TO Issue].status.name)
                }
            GROUP BY User.name
            ORDER BY User.name THEN User.<owner[TO Issue].status.name;
            """, [
            {
                'name': 'Elvis',
                'issues': {
                    'status': 'Closed',
                    'count': 1,
                }
            }, {
                'name': 'Elvis',
                'issues': {
                    'status': 'Open',
                    'count': 1,
                }
            }, {
                'name': 'Yury',
                'issues': {
                    'status': 'Closed',
                    'count': 1,
                }
            }, {
                'name': 'Yury',
                'issues': {
                    'status': 'Open',
                    'count': 1,
                }
            }
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

    async def test_edgeql_agg_01(self):
        await self.assert_query_result(r"""
            SELECT array_agg(
                schema::Concept.links.name
                WHERE
                    schema::Concept.links.name IN (
                        'std::id',
                        'schema::name'
                    )
                ORDER BY schema::Concept.links.name ASC)
            WHERE
                schema::Concept.name = 'schema::PrimaryClass';
        """, [
            [['schema::name', 'std::id']]
        ])

    async def test_edgeql_agg_02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT array_agg(
                [Issue.number, Issue.status.name]
                ORDER BY Issue.number);
        """, [
            [[['1', 'Open'], ['2', 'Open'], ['3', 'Closed'], ['4', 'Closed']]]
        ])

    async def test_edgeql_partial_01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
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
            WHERE
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
                } WHERE .name = 'Yury'
            } WHERE .status.name = 'Open' AND .owner.name = 'Elvis';
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
            } WHERE .number > '1'
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
                SELECT Issue.number WHERE .number > '1';
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
                WHERE .number = '1';
            ''')
