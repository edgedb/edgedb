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

from edb.testbase import server as tb


class TestEdgeQLFilter(tb.QueryTestCase):
    """The test DB is designed to test certain non-trivial FILTER clauses.
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_filter_setup.edgeql')

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_two_scalar_conditions01(self):
        await self.assert_query_result(
            r'''
                # Find Users who own at least one Issue with simultaneously
                # time_estimate > 9000 and due_date on 2020/01/15.
                SELECT User{name}
                FILTER
                    User.<owner[IS Issue].time_estimate > 9000
                    AND
                    User.<owner[IS Issue].due_date =
                        <datetime>'2020-01-15T00:00:00+00:00'
                ORDER BY User.name;
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    async def test_edgeql_filter_two_scalar_conditions02(self):
        await self.assert_query_result(
            r'''
                # NOTE: semantically same as and01, but using OR
                # Find Users who own at least one Issue with simultaneously
                # time_estimate > 9000 and due_date on 2020/01/15.
                SELECT User{name}
                FILTER
                    EXISTS (
                        SELECT
                            I := User.<owner[IS Issue]
                        FILTER
                            NOT (
                                NOT (
                                    EXISTS I.time_estimate AND
                                    I.time_estimate > 9000
                                ) OR
                                NOT (
                                    EXISTS I.due_date
                                    AND I.due_date =
                                        <datetime>'2020-01-15T00:00:00+00:00'
                                )
                            )
                    )
                ORDER BY User.name;
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    async def test_edgeql_filter_two_scalar_conditions03(self):
        await self.assert_query_result(
            r'''
                # NOTE: same as above, but more human-like
                # Find Users who own at least one Issue with simultaneously
                # time_estimate > 9000 and due_date on 2020/01/15.
                SELECT User{name}
                FILTER
                    EXISTS (
                        SELECT
                            I := User.<owner[IS Issue]
                        FILTER
                            NOT (
                                NOT EXISTS I.time_estimate OR
                                NOT EXISTS I.due_date OR
                                I.time_estimate <= 9000 OR
                                I.due_date !=
                                    <datetime>'2020-01-15T00:00:00+00:00'
                            )
                    )
                ORDER BY User.name;
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_two_scalar_conditions04(self):
        await self.assert_query_result(
            r'''
                # NOTE: semantically same as and01, but using OR,
                #       separate roots and explicit joining
                #
                # Find Users who own at least one Issue with simultaneously
                # time_estimate > 9000 and due_date on 2020/01/15.
                WITH
                    U2 := User
                SELECT User{name}
                FILTER
                    NOT (
                        NOT EXISTS (User.<owner[IS Issue].time_estimate > 9000)
                        OR
                        NOT EXISTS (U2.<owner[IS Issue].due_date =
                            <datetime>'2020-01-15T00:00:00+00:00')
                    )
                    AND
                    # making sure it's the same Issue in both sub-clauses
                    User.<owner[IS Issue] = U2.<owner[IS Issue]
                ORDER BY User.name;
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    async def test_edgeql_filter_not_exists01(self):
        await self.assert_query_result(
            r'''
                # Find Users who do not have any Issues with time_estimate
                SELECT User{name}
                FILTER
                    NOT EXISTS User.<owner[IS Issue].time_estimate
                ORDER BY User.name;
            ''',
            # Only such user is Victor, who has no Issues at all.
            [{'name': 'Victor'}],
        )

    async def test_edgeql_filter_not_exists02(self):
        await self.assert_query_result(
            r'''
                # Find Users who have at least one Issue without time_estimates
                SELECT (
                    SELECT Issue
                    FILTER
                        NOT EXISTS Issue.time_estimate
                ).owner{name} ORDER BY .name;
            ''',
            # Elvis and Yury have Issues without time_estimate.
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_not_exists03(self):
        await self.assert_query_result(
            r'''
                # NOTE: same as above, but starting with User
                #
                # Find Users who have at least one Issue without time_estimates
                SELECT User{name}
                FILTER
                    EXISTS (
                        SELECT I := User.<owner[IS Issue]
                        FILTER NOT EXISTS I.time_estimate
                    )
                ORDER BY User.name;
            ''',
            # Elvis and Yury have Issues without time_estimate.
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_not_exists04(self):
        await self.assert_query_result(
            r'''
                # NOTE: same as above, but with separate roots and
                # explicit path joining
                #
                # Find Users who have at least one Issue without time_estimates
                WITH
                    U2 := User
                SELECT User{name}
                FILTER
                    EXISTS User.<owner[IS Issue]
                    AND
                    NOT EXISTS U2.<owner[IS Issue].time_estimate
                    AND
                    User.<owner[IS Issue] = U2.<owner[IS Issue]
                ORDER BY User.name;
            ''',
            # Elvis and Yury have Issues without time_estimate.
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        )

    async def test_edgeql_filter_two_scalar_exists01(self):
        await self.assert_query_result(
            r'''
                # NOTE: very similar to two_scalar_conditions, same
                #       expected results
                #
                # Find Users who own at least one Issue with simultaneously
                # having a time_estimate and a due_date.
                SELECT User{name}
                FILTER
                    EXISTS (
                        SELECT
                            I := User.<owner[IS Issue]
                        FILTER
                            EXISTS I.time_estimate AND EXISTS I.due_date
                    )
                ORDER BY User.name;
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    async def test_edgeql_filter_two_scalar_exists02(self):
        await self.assert_query_result(
            r'''
                # NOTE: same as above, but using OR
                #
                # Find Users who own at least one Issue with simultaneously
                # time_estimate > 9000 and due_date on 2020/01/15.
                SELECT User{name}
                FILTER
                    EXISTS (
                        SELECT
                            I := User.<owner[IS Issue]
                        FILTER
                            NOT (
                                NOT EXISTS I.time_estimate OR
                                NOT EXISTS I.due_date
                            )
                    )
                ORDER BY User.name;
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_two_scalar_exists03(self):
        await self.assert_query_result(
            r'''
                # NOTE: same as above, but using OR,
                #       separate roots and explicit joining
                #
                # Find Users who own at least one Issue with simultaneously
                # time_estimate > 9000 and due_date on 2020/01/15.
                WITH
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
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    async def test_edgeql_filter_two_scalar_exists04(self):
        await self.assert_query_result(
            r'''
                # NOTE: same as above, but using OR,
                #       explicit sub-query and explicit joining
                #
                # Find Users who own at least one Issue with simultaneously
                # time_estimate > 9000 and due_date on 2020/01/15.
                WITH
                    U2 := User
                SELECT User{name}
                FILTER
                    EXISTS (
                        SELECT
                            I := User.<owner[IS Issue]
                        FILTER
                            NOT (
                                NOT EXISTS I.time_estimate OR
                                NOT EXISTS (
                                    (SELECT U2.<owner[IS Issue]
                                     FILTER I = U2.<owner[IS Issue]).due_date
                                )
                            )
                    )
                ORDER BY User.name;
            ''',
            # Only one Issue satisfies this and its owner is Yury.
            [{'name': 'Yury'}],
        )

    async def test_edgeql_filter_short_form01(self):
        await self.assert_query_result(
            r'''
                SELECT Status{name}
                FILTER .name = 'Open';
            ''',
            [{'name': 'Open'}],
        )

    async def test_edgeql_filter_short_form02(self):
        await self.assert_query_result(
            r'''
                # test that shape spec is not necessary to use short form
                # in the filter
                SELECT Status
                FILTER .name = 'Open';
            ''',
            [{}],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_flow01(self):
        await self.assert_query_result(
            r'''
                SELECT Issue.number
                FILTER TRUE
                ORDER BY Issue.number;
            ''',
            ['1', '2', '3', '4'],
        )

        await self.assert_query_result(
            r'''
                SELECT Issue.number
                # obviously irrelevant filter, simply equivalent to TRUE
                FILTER Status.name = 'Closed'
                ORDER BY Issue.number;
            ''',
            ['1', '2', '3', '4'],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_flow02(self):
        await self.assert_query_result(
            r'''
                SELECT Issue.number
                FILTER FALSE
                ORDER BY Issue.number;
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                SELECT Issue.number
                # obviously irrelevant filter, simply equivalent to FALSE
                FILTER Status.name = 'XXX'
                ORDER BY Issue.number;
            ''',
            [],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_flow03(self):
        await self.assert_query_result(
            r'''
                # base line for a cross product
                SELECT _ := Issue.number ++ Status.name
                ORDER BY _;
            ''',
            [
                '1Closed', '1Open',
                '2Closed', '2Open',
                '3Closed', '3Open',
                '4Closed', '4Open',
            ]
        )

        await self.assert_query_result(
            r'''
                # interaction of filter and cross product
                SELECT _ := (
                        SELECT Issue
                        FILTER Issue.owner.name = 'Elvis'
                    ).number ++ Status.name
                ORDER BY _;
            ''',
            ['1Closed', '1Open', '2Closed', '2Open'],
        )

        await self.assert_query_result(
            r'''
                SELECT _ := (
                        SELECT Issue
                        FILTER Issue.owner.name = 'Elvis'
                    ).number ++ Status.name
                FILTER
                    # this FILTER is legal, but irrelevant, the same way as
                    # SELECT
                    #    Issue.number + Status.name FILTER Status.name = 'Open'
                    Status.name = 'Open'
                ORDER BY _;
            ''',
            ['1Closed', '1Open', '2Closed', '2Open'],
        )

    async def test_edgeql_filter_empty01(self):
        await self.assert_query_result(
            r"""
                # the FILTER clause is always empty, so it can never be true
                SELECT Issue{number}
                FILTER {};
            """,
            [],
        )

    async def test_edgeql_filter_empty02(self):
        await self.assert_query_result(
            r"""
                # the FILTER clause evaluates to empty, so it can never be true
                SELECT Issue{number}
                FILTER Issue.number = <str>{};
            """,
            [],
        )

        await self.assert_query_result(
            r"""
                SELECT Issue{number}
                FILTER Issue.priority = <Object>{};
            """,
            [],
        )

        await self.assert_query_result(
            r"""
                SELECT Issue{number}
                FILTER Issue.priority.name = <str>{};
            """,
            [],
        )

    async def test_edgeql_filter_aggregate01(self):
        await self.assert_query_result(
            r'''
                SELECT count(Issue);
            ''',
            [4],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_filter_aggregate04(self):
        await self.assert_query_result(
            r'''
                SELECT count(Issue)
                # this filter is not related to the aggregate and is allowed
                #
                FILTER Status.name = 'Open';
            ''',
            [4],
        )

        await self.assert_query_result(
            r'''
                SELECT count(Issue)
                # this filter is conceptually equivalent to the above
                FILTER TRUE;
            ''',
            [4],
        )

    async def test_edgeql_filter_aggregate05(self):
        await self.assert_query_result(
            r'''
                WITH
                    I := (SELECT Issue FILTER Issue.status.name = 'Open')
                SELECT count(I);
            ''',
            [3],
        )

    async def test_edgeql_filter_aggregate06(self):
        await self.assert_query_result(
            r'''
                # regardless of what count evaluates to, FILTER clause is
                # impossible to fulfill, so the result is empty
                SELECT count(Issue)
                FILTER FALSE;
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                SELECT count(Issue)
                FILTER {};
            ''',
            [],
        )
