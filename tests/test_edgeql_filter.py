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
    """The test DB is designed to test certain non-trivial FILTER clauses.
    """

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
        INSERT User {
            name := 'Victor'
        };


        WITH MODULE test
        INSERT Issue {
            number := '1',
            name := 'Implicit path existence',
            body := 'Any expression involving paths also implies paths exist.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Closed'),
            time_estimate := 9001,
        };

        WITH MODULE test
        INSERT Issue {
            number := '2',
            name := 'NOT EXISTS problem',
            body := 'Implicit path existence does not apply to NOT EXISTS.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Open'),
            due_date := <datetime>'2020/01/15',
        };

        WITH MODULE test
        INSERT Issue {
            number := '3',
            name := 'EdgeQL to SQL translator',
            body := 'Rewrite and refactor translation to SQL.',
            owner := (SELECT User FILTER User.name = 'Yury'),
            status := (SELECT Status FILTER Status.name = 'Open'),
            time_estimate := 9999,
            due_date := <datetime>'2020/01/15',
        };

        WITH MODULE test
        INSERT Issue {
            number := '4',
            name := 'Translator optimization',
            body := 'At some point SQL translations should be optimized.',
            owner := (SELECT User FILTER User.name = 'Yury'),
            status := (SELECT Status FILTER Status.name = 'Open'),
        };
    """

    async def test_edgeql_filter_two_atomic_conditions01(self):
        await self.assert_query_result(r'''
            # Find Users who own at least one Issue with simultaneously
            # time_estimate > 9000 and due_date on 2020/01/15.
            #
            WITH MODULE test
            SELECT User{name}
            FILTER
                User.<owner[IS Issue].time_estimate > 9000
                AND
                User.<owner[IS Issue].due_date = <datetime>'2020/01/15'
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])

    async def test_edgeql_filter_two_atomic_conditions02(self):
        await self.assert_query_result(r'''
            # NOTE: semantically same as and01, but using OR
            # Find Users who own at least one Issue with simultaneously
            # time_estimate > 9000 and due_date on 2020/01/15.
            #
            WITH MODULE test
            SELECT User{name}
            FILTER
                NOT (
                    NOT (
                        EXISTS User.<owner[IS Issue].time_estimate
                        AND
                        User.<owner[IS Issue].time_estimate > 9000
                    )
                    OR
                    NOT (
                        EXISTS User.<owner[IS Issue].due_date
                        AND
                        User.<owner[IS Issue].due_date = <datetime>'2020/01/15'
                    )
                )
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])

    async def test_edgeql_filter_two_atomic_conditions03(self):
        await self.assert_query_result(r'''
            # NOTE: same as above, but more human-like
            # Find Users who own at least one Issue with simultaneously
            # time_estimate > 9000 and due_date on 2020/01/15.
            #
            WITH MODULE test
            SELECT User{name}
            FILTER
                NOT (
                    NOT EXISTS User.<owner[IS Issue].time_estimate
                    OR
                    NOT EXISTS User.<owner[IS Issue].due_date
                    OR
                    User.<owner[IS Issue].time_estimate <= 9000
                    OR
                    User.<owner[IS Issue].due_date != <datetime>'2020/01/15'
                )
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])

    async def test_edgeql_filter_two_atomic_conditions04(self):
        await self.assert_query_result(r'''
            # NOTE: semantically same as and01, but using OR,
            #       separate roots and explicit joining
            #
            # Find Users who own at least one Issue with simultaneously
            # time_estimate > 9000 and due_date on 2020/01/15.
            #
            WITH
                MODULE test,
                U2 := User
            SELECT User{name}
            FILTER
                NOT (
                    NOT EXISTS (User.<owner[IS Issue].time_estimate > 9000)
                    OR
                    NOT EXISTS (U2.<owner[IS Issue].due_date =
                        <datetime>'2020/01/15')
                )
                AND
                # making sure it's the same Issue in both sub-clauses
                User.<owner[IS Issue] = U2.<owner[IS Issue]
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])

    async def test_edgeql_filter_not_exists01(self):
        await self.assert_query_result(r'''
            # Find Users who do not have any Issues with time_estimate
            #
            WITH MODULE test
            SELECT User{name}
            FILTER
                NOT EXISTS User.<owner[IS Issue].time_estimate
            ORDER BY User.name;
        ''', [
            # Only such user is Victor, who has no Issues at all.
            [{'name': 'Victor'}],
        ])

    async def test_edgeql_filter_not_exists02(self):
        await self.assert_query_result(r'''
            # Find Users who have at least one Issue without time_estimates
            #
            WITH MODULE test
            SELECT Issue.owner{name}
            FILTER
                NOT EXISTS Issue.time_estimate
            ORDER BY Issue.owner.name;
        ''', [
            # Elvis and Yury have Issues without time_estimate.
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        ])

    async def test_edgeql_filter_not_exists03(self):
        await self.assert_query_result(r'''
            # NOTE: same as above, but starting with User
            #
            # Find Users who have at least one Issue without time_estimates
            #
            WITH MODULE test
            SELECT User{name}
            FILTER
                NOT EXISTS User.<owner[IS Issue].time_estimate
                AND
                EXISTS User.<owner[IS Issue]
            ORDER BY User.name;
        ''', [
            # Elvis and Yury have Issues without time_estimate.
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        ])

    async def test_edgeql_filter_not_exists04(self):
        await self.assert_query_result(r'''
            # NOTE: same as above, but with separate roots and
            # explicit path joining
            #
            # Find Users who have at least one Issue without time_estimates
            #
            WITH
                MODULE test,
                U2 := User
            SELECT User{name}
            FILTER
                EXISTS User.<owner[IS Issue]
                AND
                NOT EXISTS U2.<owner[IS Issue].time_estimate
                AND
                User.<owner[IS Issue] = U2.<owner[IS Issue]
            ORDER BY User.name;
        ''', [
            # Elvis and Yury have Issues without time_estimate.
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        ])

    async def test_edgeql_filter_two_atomic_exists01(self):
        await self.assert_query_result(r'''
            # NOTE: very similar to two_atomic_conditions, same
            #       expected results
            #
            # Find Users who own at least one Issue with simultaneously
            # having a time_estimate and a due_date.
            #
            WITH MODULE test
            SELECT User{name}
            FILTER
                EXISTS User.<owner[IS Issue].time_estimate
                AND
                EXISTS User.<owner[IS Issue].due_date
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])

    async def test_edgeql_filter_two_atomic_exists02(self):
        await self.assert_query_result(r'''
            # NOTE: same as above, but using OR
            #
            # Find Users who own at least one Issue with simultaneously
            # time_estimate > 9000 and due_date on 2020/01/15.
            #
            WITH MODULE test
            SELECT User{name}
            FILTER
                NOT (
                    NOT EXISTS User.<owner[IS Issue].time_estimate
                    OR
                    NOT EXISTS User.<owner[IS Issue].due_date
                )
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])

    async def test_edgeql_filter_two_atomic_exists03(self):
        await self.assert_query_result(r'''
            # NOTE: same as above, but using OR,
            #       separate roots and explicit joining
            #
            # Find Users who own at least one Issue with simultaneously
            # time_estimate > 9000 and due_date on 2020/01/15.
            #
            WITH
                MODULE test,
                U2 := User
            SELECT User{name}
            FILTER
                NOT (
                    NOT EXISTS User.<owner[IS Issue].time_estimate
                    OR
                    NOT EXISTS U2.<owner[IS Issue].due_date
                )
                AND
                # making sure it's the same Issue in both sub-clauses
                User.<owner[IS Issue] = U2.<owner[IS Issue]
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])

    @unittest.expectedFailure
    async def test_edgeql_filter_two_atomic_exists04(self):
        await self.assert_query_result(r'''
            # NOTE: same as above, but using OR,
            #       explicit sub-query and explicit joining
            #
            # Find Users who own at least one Issue with simultaneously
            # time_estimate > 9000 and due_date on 2020/01/15.
            #
            WITH
                MODULE test,
                U2 := User
            SELECT User{name}
            FILTER
                NOT (
                    NOT EXISTS User.<owner[IS Issue].time_estimate
                    OR
                    NOT EXISTS (
                        SELECT U2.<owner[IS Issue].due_date
                        FILTER User.<owner[IS Issue] = U2.<owner[IS Issue]
                    )
                )
            ORDER BY User.name;
        ''', [
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        ])
