##
# Copyright (c) 2012-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.server import _testbase as tb


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
        INSERT URL {
            name := 'edgedb.com',
            address := 'https://edgedb.com'
        };

        WITH MODULE test
        INSERT File {
            name := 'screenshot.png'
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
            priority := (SELECT Priority WHERE Priority.name = 'High'),
            references :=
                (SELECT URL WHERE URL.address = 'https://edgedb.com')
                UNION
                (SELECT File WHERE File.name = 'screenshot.png')
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

    async def test_edgeql_select_group_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                User
            BY
                User.name
            RETURNING
                std::count(User.<owner.id)
            ORDER BY
                User.name;
        ''', [
            [4, 2],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_struct99(self):
        await self.assert_query_result(r"""
            # nested structs
            # XXX: the below doesn't work yet due to the unhandled
            # conflict between the GROUP BY and path matching.
            WITH MODULE test
            SELECT
                {
                    name := User.name,
                    issues := (
                        SELECT {
                            status := User.<owner[IS Issue].status.name,
                            count := count(User.<owner[IS Issue]),
                        }
                        GROUP BY
                            User.<owner[IS Issue].status.name
                        ORDER BY
                            User.<owner[IS Issue].status.name
                    )
                }
            ORDER BY User.name;
            """, [
            {
                'name': 'Elvis',
                'issues': [{
                    'status': 'Closed',
                    'count': 1,
                }, {
                    'status': 'Open',
                    'count': 1,
                }]
            },
            {
                'name': 'Yury',
                'issues': [{
                    'status': 'Closed',
                    'count': 1,
                }, {
                    'status': 'Open',
                    'count': 1,
                }]
            },
        ])

    async def test_edgeql_agg_01(self):
        await self.assert_query_result(r"""
            SELECT
                schema::Concept {
                    l := array_agg(
                        schema::Concept.links.name
                        WHERE
                            schema::Concept.links.name IN (
                                'std::id',
                                'schema::name'
                            )
                        ORDER BY schema::Concept.links.name ASC
                    )
                }
            WHERE
                schema::Concept.name = 'schema::PrimaryClass';
        """, [
            [{
                'l': ['schema::name', 'std::id']
            }]
        ])

    async def test_edgeql_agg_02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT array_agg(
                [<str>Issue.number, Issue.status.name]
                ORDER BY Issue.number);
        """, [
            [[['1', 'Open'], ['2', 'Open'], ['3', 'Closed'], ['4', 'Closed']]]
        ])
