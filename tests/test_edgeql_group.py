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
            owner := (SELECT User FILTER User.name = 'Elvis'),
            spent_time := 50000,
            body := 'Rewriting everything.'
        };

        WITH MODULE test
        INSERT Issue {
            number := '1',
            name := 'Release EdgeDB',
            body := 'Initial public release of EdgeDB.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            watchers := (SELECT User FILTER User.name = 'Yury'),
            status := (SELECT Status FILTER Status.name = 'Open'),
            time_spent_log := (SELECT LogEntry),
            time_estimate := 3000
        };

        WITH MODULE test
        INSERT Comment {
            body := 'EdgeDB needs to happen soon.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            issue := (SELECT Issue FILTER Issue.number = '1')
        };


        WITH MODULE test
        INSERT Issue {
            number := '2',
            name := 'Improve EdgeDB repl output rendering.',
            body := 'We need to be able to render data in tabular format.',
            owner := (SELECT User FILTER User.name = 'Yury'),
            watchers := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Open'),
            priority := (SELECT Priority FILTER Priority.name = 'High'),
            references :=
                (SELECT URL FILTER URL.address = 'https://edgedb.com')
                UNION
                (SELECT File FILTER File.name = 'screenshot.png')
        };

        WITH
            MODULE test,
            I := (SELECT Issue)
        INSERT Issue {
            number := '3',
            name := 'Repl tweak.',
            body := 'Minor lexer tweaks.',
            owner := (SELECT User FILTER User.name = 'Yury'),
            watchers := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Closed'),
            related_to := (
                SELECT I FILTER I.number = '2'
            ),
            priority := (SELECT Priority FILTER Priority.name = 'Low')
        };

        WITH
            MODULE test,
            I := (SELECT Issue)
        INSERT Issue {
            number := '4',
            name := 'Regression.',
            body := 'Fix regression introduced by lexer tweak.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Closed'),
            related_to := (
                SELECT I FILTER I.number = '3'
            )
        };

        # NOTE: UPDATE Users for testing the link properties
        #
        WITH MODULE test
        UPDATE User
        FILTER User.name = 'Elvis'
        SET {
            todo := (SELECT Issue FILTER Issue.number in ('1', '2'))
        };

        WITH MODULE test
        UPDATE User
        FILTER User.name = 'Yury'
        SET {
            todo := (SELECT Issue FILTER Issue.number in ('3', '4'))
        };
    """

    async def test_edgeql_group_simple01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                User
            BY
                User.name
            SELECT
                count(ALL User.<owner)
            ORDER BY
                User.name;
        ''', [
            [4, 2],
        ])

    async def test_edgeql_group_simple02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.time_estimate
            SELECT
                # count using link 'id'
                count(ALL Issue.id)
            ORDER BY
                Issue.time_estimate EMPTY FIRST;
        ''', [
            [3, 1],
        ])

    async def test_edgeql_group_simple03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.time_estimate
            SELECT
                # count Issue directly
                count(ALL Issue)
            ORDER BY
                Issue.time_estimate EMPTY FIRST;
        ''', [
            [3, 1],
        ])

    async def test_edgeql_group_simple04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.time_estimate
            SELECT
                # count Issue statuses, which should be same as counting
                # Issues, since the status link is *1
                count(ALL Issue.status.id)
            ORDER BY
                Issue.time_estimate EMPTY FIRST;
        ''', [
            [3, 1],
        ])

    async def test_edgeql_group_simple05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.time_estimate
            SELECT
                # unusual qualifier for 'count'
                count(DISTINCT Issue.status.id)
            ORDER BY
                Issue.time_estimate EMPTY FIRST;
        ''', [
            [2, 1],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_alias01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.time_estimate
            SELECT _ := (
                count := count(ALL Issue.status.id),
                te := Issue.time_estimate > 0
            ) ORDER BY
                _.te EMPTY FIRST;
        ''', [
            [{'count': 3, 'te': None}, {'count': 1, 'te': True}],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_nested01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                R := {
                    name := User.name,
                    issues := (
                        GROUP
                            User.<owner[IS Issue]
                        BY
                            User.<owner[IS Issue].status.name
                        SELECT (
                            status := User.<owner[IS Issue].status.name,
                            count := count(ALL User.<owner[IS Issue]),
                        )
                        ORDER BY
                            User.<owner[IS Issue].status.name
                    )
                }
            ORDER BY R.name;
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

    async def test_edgeql_group_agg01(self):
        await self.assert_query_result(r"""
            SELECT
                schema::Concept {
                    l := array_agg(
                        ALL
                        schema::Concept.links.name
                        FILTER
                            schema::Concept.links.name IN (
                                'std::id',
                                'schema::name'
                            )
                        ORDER BY schema::Concept.links.name ASC
                    )
                }
            FILTER
                schema::Concept.name = 'schema::PrimaryClass';
        """, [
            [{
                'l': ['schema::name', 'std::id']
            }]
        ])

    async def test_edgeql_group_agg02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT array_agg(
                ALL
                [<str>Issue.number, Issue.status.name]
                ORDER BY Issue.number);
        """, [
            [[['1', 'Open'], ['2', 'Open'], ['3', 'Closed'], ['4', 'Closed']]]
        ])

    async def test_edgeql_group_agg03(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name
            SELECT (
                sum := sum(ALL <int>Issue.number),
                status := Issue.status.name,
            ) ORDER BY Issue.status.name;
        """, [
            [{
                'status': 'Closed',
                'sum': 7,
            }, {
                'status': 'Open',
                'sum': 3,
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_agg04(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name
            SELECT
                _ := (
                    sum := sum(ALL <int>Issue.number),
                    status := Issue.status.name,
                )
            FILTER
                _.sum > 5
            ORDER BY
                Issue.status.name;
        """, [
            [{
                'status': 'Closed',
                'sum': 7,
            }],
        ])

    async def test_edgeql_group_returning01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.time_estimate
            SELECT
                # since we're returning the same element for all of
                # the groups the expected resulting SET should only
                # have one element
                42;
        ''', [
            [42],
        ])

    async def test_edgeql_group_by_tuple01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                Issue.time_estimate
            SELECT _ := (
                sum := sum(ALL <int>Issue.number),
                status := Issue.status.name,
                time_estimate := Issue.time_estimate
            ) ORDER BY Issue.status.name
                THEN Issue.time_estimate;
        """, [
            [{
                'status': 'Closed', 'sum': 7, 'time_estimate': None
            }, {
                'status': 'Open', 'sum': 2, 'time_estimate': None
            }, {
                'status': 'Open', 'sum': 1, 'time_estimate': 3000
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_tuple02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                Issue.time_estimate
            SELECT (
                sum := sum(ALL <int>Issue.number),
                status := Issue.status.name,
                time_estimate := Issue.time_estimate
            ) ORDER BY Issue.status.name
                # ordering condition derived from one of the GROUP BY
                # expressions
                THEN Issue.time_estimate > 0;
        """, [
            [{
                'status': 'Closed', 'sum': 7, 'time_estimate': None
            }, {
                'status': 'Open', 'sum': 2, 'time_estimate': None
            }, {
                'status': 'Open', 'sum': 1, 'time_estimate': 3000
            }],
        ])

    async def test_edgeql_group_by_tuple03(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                Issue.time_estimate
            SELECT (
                # array_agg with ordering instead of sum
                numbers := array_agg(
                    ALL <int>Issue.number ORDER BY Issue.number),
                status := Issue.status.name,
                time_estimate := Issue.time_estimate
            ) ORDER BY Issue.status.name
                THEN Issue.time_estimate;
        """, [
            [{
                'status': 'Closed',
                'time_estimate': None,
                'numbers': [3, 4],
            }, {
                'status': 'Open',
                'time_estimate': None,
                'numbers': [2],
            }, {
                'status': 'Open',
                'time_estimate': 3000,
                'numbers': [1],
            }],
        ])

    async def test_edgeql_group_by_tuple04(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                Issue.time_estimate
            SELECT (
                # array_agg with ordering instead of sum
                numbers := array_agg(ALL Issue.number ORDER BY Issue.number),
                status := Issue.status.name,
                time_estimate := Issue.time_estimate
            ) ORDER BY Issue.status.name
                THEN Issue.time_estimate;
        """, [
            [{
                'status': 'Closed',
                'time_estimate': None,
                'numbers': ['3', '4'],
            }, {
                'status': 'Open',
                'time_estimate': None,
                'numbers': ['2'],
            }, {
                'status': 'Open',
                'time_estimate': 3000,
                'numbers': ['1'],
            }],
        ])

    async def test_edgeql_group_by_tuple05(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                Issue.time_estimate
            SELECT (
                # a couple of array_agg
                numbers := array_agg(
                    ALL <int>Issue.number ORDER BY Issue.number),
                watchers := array_agg(
                    ALL <str>Issue.watchers.name ORDER BY Issue.watchers.name),
                status := Issue.status.name,
                time_estimate := Issue.time_estimate
            ) ORDER BY Issue.status.name
                THEN Issue.time_estimate;
        """, [
            [{
                'status': 'Closed',
                'time_estimate': None,
                'numbers': [3, 4],
                'watchers': ['Elvis'],
            }, {
                'status': 'Open',
                'time_estimate': None,
                'numbers': [2],
                'watchers': ['Elvis'],
            }, {
                'status': 'Open',
                'time_estimate': 3000,
                'numbers': [1],
                'watchers': ['Yury'],
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_tuple06(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                # group by non-atomic expression
                <int>Issue.number < 4
            SELECT (
                numbers := array_agg(
                    ALL Issue.number ORDER BY Issue.number),
                # watchers will sometimes be EMPTY resulting in []
                watchers := array_agg(
                    ALL Issue.watchers.name ORDER BY Issue.watchers.name),
                status := Issue.status.name,
            ) ORDER BY Issue.status.name
                THEN <int>Issue.number < 4;
        """, [
            [{
                'status': 'Closed',
                'numbers': ['4'],
                'watchers': []
            }, {
                'status': 'Closed',
                'numbers': ['3'],
                'watchers': ['Elvis']
            }, {
                'status': 'Open',
                'numbers': ['1', '2'],
                'watchers': ['Elvis', 'Yury']
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_tuple07(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                # group by non-atomic expression
                <int>Issue.number < 4
            SELECT (
                numbers := array_agg(
                    ALL <int>Issue.number ORDER BY Issue.number),
                watchers := count(DISTINCT Issue.watchers),
                status := Issue.status.name,
            ) ORDER BY Issue.status.name
                # should work because count evaluates to a SINGLETON
                THEN count(DISTINCT Issue);
        """, [
            [{
                'status': 'Closed',
                'numbers': [4],
                'watchers': 0
            }, {
                'status': 'Closed',
                'numbers': [3],
                'watchers': 1
            }, {
                'status': 'Open',
                'numbers': [1, 2],
                'watchers': 2
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_tuple08(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                Issue.owner.id
            SELECT (
                numbers := array_agg(
                    ALL <int>Issue.number ORDER BY Issue.number),
                status := Issue.status.name,
            ) ORDER BY Issue.status.name
                # should work because owner.name and owner.id are 1-1
                THEN Issue.owner.name;
        """, [
            [{
                'status': 'Closed',
                'numbers': [4],
            }, {
                'status': 'Closed',
                'numbers': [3],
            }, {
                'status': 'Open',
                'numbers': [1],
            }, {
                'status': 'Open',
                'numbers': [2],
            }],
        ])
