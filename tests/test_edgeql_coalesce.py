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

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLCoalesce(tb.QueryTestCase):
    """The test DB is designed to test various coalescing operations.
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_coalesce_setup.edgeql')

    async def test_edgeql_coalesce_scalar_01(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    time_estimate := Issue.time_estimate ?? -1
                };
            ''',
            [
                {'time_estimate': -1},
                {'time_estimate': -1},
                {'time_estimate': -1},
                {'time_estimate': 60},
                {'time_estimate': 90},
                {'time_estimate': 90},
            ],
            sort=lambda x: x['time_estimate']
        )

    async def test_edgeql_coalesce_scalar_02(self):
        await self.assert_query_result(
            r'''
                SELECT (Issue.number, Issue.time_estimate ?? -1)
                ORDER BY Issue.number;
            ''',
            [
                ['1', 60],
                ['2', 90],
                ['3', 90],
                ['4', -1],
                ['5', -1],
                ['6', -1],
            ]
        )

    async def test_edgeql_coalesce_scalar_03(self):
        await self.assert_query_result(
            r'''
                # Only values present in the graph will be selected.
                # There is at least one value there.
                # Therefore, the second argument to ?? will not be returned.
                SELECT Issue.time_estimate ?? -1;
            ''',
            [
                60,
                90,
                90,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_scalar_04(self):
        await self.assert_query_result(
            r'''
                # No open issue has a time_estimate, so the first argument
                # to ?? is an empty set.
                # Therefore, the second argument to ?? will be returned.
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?? -1;
            ''',
            [
                -1,
            ]
        )

    async def test_edgeql_coalesce_scalar_05(self):
        await self.assert_query_result(
            r'''
                WITH
                    I := (SELECT Issue
                          FILTER Issue.status.name = 'Open')
                # No open issue has a time_estimate, so the first argument
                # to ?? is an empty set.
                # Therefore, the second argument to ?? will be returned.
                SELECT I.time_estimate ?? -1;
            ''',
            [
                -1
            ]
        )

    async def test_edgeql_coalesce_scalar_06(self):
        # The result is either an empty set if at least one
        # estimate exists, or `-1` if no estimates exist.
        # Our database contains one estimate.
        await self.assert_query_result(
            r"""
                SELECT Issue.time_estimate ?? -1
                FILTER NOT EXISTS Issue.time_estimate;
            """,
            []
        )

    async def test_edgeql_coalesce_scalar_07(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    number,
                    has_estimate := Issue.time_estimate ?!= <int64>{}
                };
            ''',
            [
                {'number': '1', 'has_estimate': True},
                {'number': '2', 'has_estimate': True},
                {'number': '3', 'has_estimate': True},
                {'number': '4', 'has_estimate': False},
                {'number': '5', 'has_estimate': False},
                {'number': '6', 'has_estimate': False},
            ],
            sort=lambda x: x['number']
        )

    async def test_edgeql_coalesce_scalar_08(self):
        await self.assert_query_result(
            r'''
                SELECT (Issue.number, Issue.time_estimate ?= 60)
                ORDER BY Issue.number;
            ''',
            [
                ['1', True],
                ['2', False],
                ['3', False],
                ['4', False],
                ['5', False],
                ['6', False],
            ]
        )

    async def test_edgeql_coalesce_scalar_09(self):
        await self.assert_query_result(
            r'''
                # Only values present in the graph will be selected.
                SELECT Issue.time_estimate ?= 60;
            ''',
            [
                False, False, True,
            ],
            sort=True
        )

        await self.assert_query_result(
            r'''
                SELECT Issue.time_estimate ?= <int64>{};
            ''',
            [
                False, False, False,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_scalar_10(self):
        await self.assert_query_result(
            r'''
                # No open issue has a time_estimate, so the first argument
                # to ?= is an empty set.
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?= <int64>{};
            ''',
            [
                True,
            ]
        )

    async def test_edgeql_coalesce_scalar_11(self):
        await self.assert_query_result(
            r'''
                # No open issue has a time_estimate, so the first argument
                # to ?!= is an empty set.
                WITH
                    I := (SELECT Issue
                          FILTER Issue.status.name = 'Open')
                SELECT I.time_estimate ?!= <int64>{};
            ''',
            [
                False
            ]
        )

        await self.assert_query_result(
            r'''
                WITH
                    I := (SELECT Issue
                          FILTER Issue.status.name = 'Open')
                SELECT I.time_estimate ?!= 60;
            ''',
            [
                True
            ]
        )

    async def test_edgeql_coalesce_scalar_12(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    number,
                    time_estimate,
                    related_to: {time_estimate},
                }
                ORDER BY Issue.number;
            ''',
            [
                {'number': '1', 'related_to': [], 'time_estimate': 60},
                {'number': '2', 'related_to': [], 'time_estimate': 90},
                {'number': '3', 'related_to': [], 'time_estimate': 90},
                {'number': '4', 'related_to': [], 'time_estimate': None},
                {
                    'number': '5',
                    'related_to': [{'time_estimate': 60}],
                    'time_estimate': None,
                },
                {
                    'number': '6',
                    'related_to': [{'time_estimate': 90}],
                    'time_estimate': None,
                },
            ]
        )

        await self.assert_query_result(
            r'''
                # now test a combination of several coalescing operators
                SELECT
                    Issue.time_estimate ??
                    Issue.related_to.time_estimate ?=
                        <int64>Issue.number * 12
                ORDER BY Issue.number;
            ''',
            [
                False, False, False, False, True, False,
            ]
        )

    async def test_edgeql_coalesce_set_01(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    comp_time_estimate := Issue.time_estimate ?? {-1, -2}
                };
            ''',
            [
                {'comp_time_estimate': [-1, -2]},
                {'comp_time_estimate': [-1, -2]},
                {'comp_time_estimate': [-1, -2]},
                {'comp_time_estimate': [60]},
                {'comp_time_estimate': [90]},
                {'comp_time_estimate': [90]},
            ],
            sort=lambda x: x['comp_time_estimate']
        )

    async def test_edgeql_coalesce_set_02(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    multi te := (
                        SELECT Issue.time_estimate ?? {-1, -2}
                    )
                };
            ''',
            [
                {'te': [-1, -2]},
                {'te': [-1, -2]},
                {'te': [-1, -2]},
                {'te': [60]},
                {'te': [90]},
                {'te': [90]},
            ],
            sort=lambda x: x['te']
        )

    async def test_edgeql_coalesce_set_03(self):
        await self.assert_query_result(
            r'''
                SELECT _ := (Issue.number, Issue.time_estimate ?? {-1, -2})
                ORDER BY _;
            ''',
            [
                ['1', 60],
                ['2', 90],
                ['3', 90],
                ['4', -2],
                ['4', -1],
                ['5', -2],
                ['5', -1],
                ['6', -2],
                ['6', -1],
            ],
        )

    async def test_edgeql_coalesce_set_04(self):
        await self.assert_query_result(
            r'''
                # Only values present in the graph will be selected.
                # There is at least one value there.
                # Therefore, the second argument to ?? will not be returned.
                SELECT Issue.time_estimate ?? {-1, -2};
            ''',
            [
                60,
                90,
                90,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_set_05(self):
        await self.assert_query_result(
            r'''
                # No open issue has a time_estimate, so the first argument
                # to ?? is an empty set.
                # Therefore, the second argument to ?? will be returned.
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?? {-1, -2};
            ''',
            {
                -1, -2,
            },
        )

    async def test_edgeql_coalesce_set_06(self):
        await self.assert_query_result(
            r'''
                WITH
                    I := (SELECT Issue
                          FILTER Issue.status.name = 'Open')
                # No open issue has a time_estimate, so the first argument
                # to ?? is an empty set.
                # Therefore, the second argument to ?? will be returned.
                SELECT I.time_estimate ?? {-1, -2};
            ''',
            {
                -1, -2,
            },
        )

    async def test_edgeql_coalesce_set_07(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    number,
                    te := Issue.time_estimate ?= {60, 30}
                };
            ''',
            [
                {'number': '1', 'te': {True, False}},
                {'number': '2', 'te': [False, False]},
                {'number': '3', 'te': [False, False]},
                {'number': '4', 'te': [False, False]},
                {'number': '5', 'te': [False, False]},
                {'number': '6', 'te': [False, False]},
            ],
            sort=lambda x: x['number']
        )

    async def test_edgeql_coalesce_set_08(self):
        await self.assert_query_result(
            r'''
                SELECT _ := (Issue.number, Issue.time_estimate ?= {60, 90})
                ORDER BY _;
            ''',
            [
                ['1', False],
                ['1', True],
                ['2', False],
                ['2', True],
                ['3', False],
                ['3', True],
                ['4', False],
                ['4', False],
                ['5', False],
                ['5', False],
                ['6', False],
                ['6', False],
            ],
        )

    async def test_edgeql_coalesce_set_09(self):
        await self.assert_query_result(
            r'''
                # Only values present in the graph will be selected.
                SELECT Issue.time_estimate ?= {60, 30};
            ''',
            [
                False,
                False,
                False,
                False,
                False,
                True,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_set_10(self):
        await self.assert_query_result(
            r'''
                # No open issue has a time_estimate, so the first argument
                # to ?!= is an empty set.
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?!= {-1, -2};
            ''',
            [
                True, True,
            ],
        )

    async def test_edgeql_coalesce_set_11(self):
        await self.assert_query_result(
            r'''
                # No open issue has a time_estimate, so the first argument
                # to ?= is an empty set.
                WITH
                    I := (SELECT Issue
                          FILTER Issue.status.name = 'Open')
                SELECT I.time_estimate ?= {-1, -2};
            ''',
            [
                False, False,
            ],
        )

    async def test_edgeql_coalesce_dependent_01(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    # for every issue, there's a unique derived "default"
                    # to use  with ??
                    time_estimate :=
                        Issue.time_estimate ?? -<int64>Issue.number
                } ORDER BY Issue.time_estimate;
            ''',
            [
                {'time_estimate': -6},
                {'time_estimate': -5},
                {'time_estimate': -4},
                {'time_estimate': 60},
                {'time_estimate': 90},
                {'time_estimate': 90},
            ],
        )

    async def test_edgeql_coalesce_dependent_02(self):
        await self.assert_query_result(
            r'''
                # for every issue, there's a unique derived "default" to use
                # with ??
                SELECT (Issue.number,
                        Issue.time_estimate ?? -<int64>Issue.number)
                ORDER BY Issue.number;
            ''',
            [
                ['1', 60],
                ['2', 90],
                ['3', 90],
                ['4', -4],
                ['5', -5],
                ['6', -6],
            ],
        )

    async def test_edgeql_coalesce_dependent_03(self):
        await self.assert_query_result(
            r'''
                # ?? is OPTIONAL w.r.t. first argument, so it behaves like
                # an element-wise function. Therefore, the longest common
                # prefix `Issue` is factored out and the expression is
                # evaluated for every Issue.
                SELECT Issue.time_estimate ?? -<int64>Issue.number;
            ''',
            [
                -6, -5, -4, 60, 90, 90,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_04(self):
        await self.assert_query_result(
            r'''
                # Since ?? is OPTIONAL over it's first argument,
                # the expression is evaluated for all six issues.
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?? -<int64>Issue.number;
            ''',
            [
                -6, -5, -4, -3, -2, -1
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_05(self):
        await self.assert_query_result(
            r'''
                # Unlike the above test, we refer to the
                # same "open" subset of issues on both
                # sides of ??, so the result set contains
                # only three elements.
                WITH
                    I := (SELECT Issue
                          FILTER Issue.status.name = 'Open')
                SELECT I.time_estimate ?? -<int64>I.number;
            ''',
            [
                -6, -5, -4,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_06(self):
        await self.assert_query_result(
            r'''
                WITH
                    I2 := Issue
                # ?? is OPTIONAL w.r.t. first argument, so it behaves like
                # an element-wise function. However, since there is no
                # common prefix, the expression gets evaluated ONLY for
                # existing values of `Issue.time_estimate`.
                SELECT Issue.time_estimate ?? -<int64>I2.number;
            ''',
            [
                60, 90, 90,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_07(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?? -<int64>Issue.number;
            ''',
            [
                -6, -5, -4, -3, -2, -1,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_08(self):
        await self.assert_query_result(
            r'''
                # On one hand the right operand of ?? is not independent
                # of the left. On the other hand, it is constructed in
                # such a way as to be equivalent to literal `-1` for the
                # case when its value is important.
                #
                # LCP is `Issue.time_estimate`, so this should not
                # actually be evaluated for every `Issue`, but for every
                # `Issue.time_estimate`.
                SELECT Issue.time_estimate ?? {Issue.time_estimate, -1};
            ''',
            [
                60, 90, 90,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_09(self):
        await self.assert_query_result(
            r'''
                # `Issue` on both sides is behind a fence, so the left-hand
                # expression is an empty set, and the result is a union
                # of all existing time estimates and -1.
                SELECT _ := (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?? {Issue.time_estimate, -1}
                ORDER BY _;
            ''',
            [
                -1, 60, 90, 90
            ],
        )

    async def test_edgeql_coalesce_dependent_10(self):
        await self.assert_query_result(
            r'''
                WITH
                    I := (
                        SELECT Issue
                        FILTER Issue.status.name = 'Open'
                    )
                # `I.time_estimate` is now a LCP
                SELECT I.time_estimate ?? {I.time_estimate, -1};
            ''',
            [
                -1,
            ],
        )

    async def test_edgeql_coalesce_dependent_11(self):
        await self.assert_query_result(
            r'''
                SELECT Issue {
                    number,
                    foo := Issue.time_estimate ?= <int64>Issue.number * 30
                } ORDER BY Issue.number;
            ''',
            [
                {'number': '1', 'foo': False},
                {'number': '2', 'foo': False},
                {'number': '3', 'foo': True},
                {'number': '4', 'foo': False},
                {'number': '5', 'foo': False},
                {'number': '6', 'foo': False},
            ],
        )

    async def test_edgeql_coalesce_dependent_12(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    Issue.number,
                    Issue.time_estimate ?!= <int64>Issue.number * 30
                )
                ORDER BY Issue.number;
            ''',
            [
                ['1', True],
                ['2', True],
                ['3', False],
                ['4', True],
                ['5', True],
                ['6', True],
            ],
        )

    async def test_edgeql_coalesce_dependent_13(self):
        await self.assert_query_result(
            r'''
                # ?= is OPTIONAL w.r.t. both arguments, so it behaves like
                # an element-wise function. Therefore, the longest common
                # prefix `Issue` is factored out and the expression is
                # evaluated for every Issue.
                SELECT Issue.time_estimate ?= <int64>Issue.number * 30;
            ''',
            [
                False,
                False,
                False,
                False,
                False,
                True,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_14(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?= <int64>Issue.number;
            ''',
            [
                False, False, False, False, False, False
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_15(self):
        await self.assert_query_result(
            r'''
                WITH
                    I := (SELECT Issue
                          FILTER Issue.status.name = 'Open')
                # Same as dependent_13, but only 'Open' issues
                # being considered.
                SELECT I.time_estimate ?!= I.time_spent_log.spent_time;
            ''',
            [
                False, False, False,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_16(self):
        await self.assert_query_result(
            r'''
                WITH
                    I2 := Issue
                # ?= is OPTIONAL w.r.t. both arguments, so it behaves like
                # an element-wise function. However, since there is no
                # common prefix, the expression gets evaluated ONLY for
                # existing values of `Issue.time_estimate`, so the cardinality
                # of the result set is 18 (3 * 6).
                SELECT Issue.time_estimate ?= <int64>I2.number * 30;
            ''',
            [
                False, False, False,
                False, False, False,
                False, False, False,
                False, False, False,
                False, False, False,
                True, True, True,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_17(self):
        await self.assert_query_result(
            r'''
                WITH
                    I2 := Issue
                # ?!= is OPTIONAL w.r.t. both arguments, so it behaves like
                # an element-wise function. However, since there is no
                # common prefix, the expression gets evaluated ONLY for
                # existing values of `Issue.time_estimate`, where
                # `Issue.status` is 'Open', which happens to be an empty set,
                # but ?!= is OPTIONAL, so the cardinality of the result set is
                # 1 * |I.number| == 6.
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?!= <int64>I2.number * 30;
            ''',
            [
                True, True, True,
                True, True, True,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_18(self):
        await self.assert_query_result(
            r'''
                # LCP is `Issue.time_estimate`, so this should not
                # actually be evaluated for every `Issue`, but for every
                # `Issue.time_estimate`.
                SELECT Issue.time_estimate ?= Issue.time_estimate * 2;
            ''',
            [
                False, False, False,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_19(self):
        await self.assert_query_result(
            r'''
                # `Issue` is now a LCP and the overall expression will be
                # evaluated for every `Issue`.
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_estimate ?= Issue.time_estimate * 2;
            ''',
            [
                False, False, False, True, True, True,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_dependent_20(self):
        await self.assert_query_result(
            r'''
                WITH
                    I := (
                        SELECT Issue
                        FILTER Issue.status.name = 'Open'
                    )
                # `I.time_estimate` is now a LCP
                SELECT I.time_estimate ?= I.time_estimate * 2;
            ''',
            [
                True,
            ],
        )

    async def test_edgeql_coalesce_dependent_21(self):
        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X[IS Priority].name ?? X[IS Status].name;
            ''',
            {'High', 'Low', 'Open', 'Closed'},
        )

    async def test_edgeql_coalesce_dependent_22(self):
        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X[IS Priority].name[0] ?? X[IS Status].name;
            ''',
            {'H', 'L', 'Open', 'Closed'},
        )

        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X[IS Priority].name ?? X[IS Status].name[0];
            ''',
            {'High', 'Low', 'O', 'C'},
        )

        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X[IS Priority].name[0] ?? X[IS Status].name[0];
            ''',
            {'H', 'L', 'O', 'C'},
        )

    async def test_edgeql_coalesce_dependent_23(self):
        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X {
                    foo := X[IS Priority].name ?? X[IS Status].name
                };
            ''',
            [
                {'foo': 'Closed'},
                {'foo': 'High'},
                {'foo': 'Low'},
                {'foo': 'Open'}
            ],
            sort=lambda x: x['foo']
        )

        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X {
                    foo := X[IS Priority].name[0] ?? X[IS Status].name
                };
            ''',
            [
                {'foo': 'Closed'},
                {'foo': 'H'},
                {'foo': 'L'},
                {'foo': 'Open'}
            ],
            sort=lambda x: x['foo']
        )

        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X {
                    foo := X[IS Priority].name ?? X[IS Status].name[0]
                };
            ''',
            [
                {'foo': 'C'},
                {'foo': 'High'},
                {'foo': 'Low'},
                {'foo': 'O'}
            ],
            sort=lambda x: x['foo']
        )

        await self.assert_query_result(
            r'''
                WITH
                    X := {Priority, Status}
                SELECT X {
                    foo := X[IS Priority].name[0] ?? X[IS Status].name[0]
                };
            ''',
            [
                {'foo': 'C'},
                {'foo': 'H'},
                {'foo': 'L'},
                {'foo': 'O'}
            ],
            sort=lambda x: x['foo']
        )

    async def test_edgeql_coalesce_object_01(self):
        await self.assert_query_result(
            r'''
                WITH
                    DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
                SELECT Issue {
                    number,
                    time_spent_log := (
                        SELECT x := (Issue.time_spent_log ?? DUMMY) {
                            id,
                            spent_time
                        }
                        ORDER BY x.spent_time
                    )
                } ORDER BY Issue.number;
            ''',
            [
                {
                    'number': '1',
                    'time_spent_log': [{
                        'spent_time': 60,
                    }],
                }, {
                    'number': '2',
                    'time_spent_log': [{
                        'spent_time': 90,
                    }],
                }, {
                    'number': '3',
                    'time_spent_log': [{
                        'spent_time': 30,
                    }, {
                        'spent_time': 60,
                    }],
                }, {
                    'number': '4',
                    'time_spent_log': [{
                        'spent_time': -1,
                    }],
                }, {
                    'number': '5',
                    'time_spent_log': [{
                        'spent_time': -1,
                    }],
                }, {
                    'number': '6',
                    'time_spent_log': [{
                        'spent_time': -1,
                    }],
                },
            ],
        )

    async def test_edgeql_coalesce_object_02(self):
        await self.assert_query_result(
            r'''
                WITH
                    DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
                SELECT x := (
                    Issue.number,
                    (Issue.time_spent_log ?? DUMMY).spent_time
                ) ORDER BY x.0 THEN x.1;
            ''',
            [
                ['1', 60],
                ['2', 90],
                ['3', 30],
                ['3', 60],
                ['4', -1],
                ['5', -1],
                ['6', -1],
            ],
        )

    async def test_edgeql_coalesce_object_03(self):
        await self.assert_query_result(
            r'''
                WITH
                    DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
                SELECT x := (Issue.time_spent_log ?? DUMMY) {
                    spent_time
                }
                ORDER BY x.spent_time;
            ''',
            [
                {'spent_time': 30},
                {'spent_time': 60},
                {'spent_time': 60},
                {'spent_time': 90},
            ],
            sort=lambda x: x['spent_time']
        )

    async def test_edgeql_coalesce_object_04(self):
        await self.assert_query_result(
            r'''
                WITH
                    DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
                SELECT (
                    (SELECT Issue
                     FILTER Issue.status.name = 'Open').time_spent_log
                    ??
                    DUMMY
                ) {
                    id,
                    spent_time
                };
            ''',
            [
                {'spent_time': -1},
            ],
        )

    async def test_edgeql_coalesce_object_05(self):
        await self.assert_query_result(
            r'''
                WITH
                    DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy'),
                    I := (
                        SELECT Issue
                        FILTER Issue.status.name = 'Open'
                    )
                SELECT (I.time_spent_log ?? DUMMY) {
                    id,
                    spent_time
                };
            ''',
            [
                {'spent_time': -1},
            ],
        )

    async def test_edgeql_coalesce_object_06(self):
        await self.assert_query_result(
            r'''
                WITH
                    LOG1 := (SELECT LogEntry FILTER LogEntry.body = 'Log1')
                SELECT Issue {
                    number,
                    log1 := Issue.time_spent_log ?= LOG1
                } ORDER BY Issue.number;
            ''',
            [
                {
                    'number': '1',
                    'log1': [True],
                }, {
                    'number': '2',
                    'log1': [False],
                }, {
                    'number': '3',
                    'log1': [False, False]
                }, {
                    'number': '4',
                    'log1': [False],
                }, {
                    'number': '5',
                    'log1': [False],
                }, {
                    'number': '6',
                    'log1': [False],
                },
            ],
        )

    async def test_edgeql_coalesce_object_07(self):
        await self.assert_query_result(
            r'''
                WITH
                    LOG1 := (SELECT LogEntry FILTER LogEntry.body = 'Log1')
                SELECT (
                    Issue.number, Issue.time_spent_log ?= LOG1
                ) ORDER BY Issue.number;
            ''',
            [
                ['1', True],
                ['2', False],
                ['3', False],
                ['3', False],
                ['4', False],
                ['5', False],
                ['6', False],
            ],
        )

    async def test_edgeql_coalesce_object_08(self):
        await self.assert_query_result(
            r'''
                WITH
                    LOG1 := (SELECT LogEntry FILTER LogEntry.body = 'Log1')
                SELECT Issue.time_spent_log ?!= LOG1;
            ''',
            [
                False,
                True,
                True,
                True,
            ],
            sort=True
        )

    async def test_edgeql_coalesce_object_09(self):
        await self.assert_query_result(
            r'''
                WITH
                    DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
                SELECT (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                ).time_spent_log ?= DUMMY;
            ''',
            [
                False,
            ],
        )

    async def test_edgeql_coalesce_object_10(self):
        await self.assert_query_result(
            r'''
                WITH
                    DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy'),
                    I := (
                        SELECT Issue
                        FILTER Issue.status.name = 'Open'
                    )
                SELECT I.time_spent_log ?!= DUMMY;
            ''',
            [
                True,
            ],
        )

    async def test_edgeql_coalesce_object_11(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (
                        (SELECT Issue FILTER .number = '1')
                        ??
                        (SELECT Issue FILTER .number = '2')
                    ) {
                        number
                    }
            ''',
            [{
                'number': '1',
            }]
        )

    async def test_edgeql_coalesce_object_12(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (
                        (SELECT Issue FILTER .number = '100')
                        ??
                        (SELECT Issue FILTER .number = '2')
                    ) {
                        number
                    }
            ''',
            [{
                'number': '2',
            }]
        )

    async def test_edgeql_coalesce_wrapping_optional(self):
        await self.con.execute(
            r'''
                CREATE FUNCTION optfunc(
                        a: std::str, b: OPTIONAL std::str) -> std::str
                    USING EdgeQL $$
                        SELECT b IF a = 'foo' ELSE a
                    $$;
            '''
        )

        await self.assert_query_result(
            r'''
                SELECT optfunc('foo', <str>{}) ?? 'N/A';
            ''',
            ['N/A'],
        )
        await self.assert_query_result(
            r'''
                SELECT optfunc('foo', 'b') ?? 'N/A';
            ''',
            ['b'],
        )
        await self.assert_query_result(
            r'''
                SELECT optfunc('a', <str>{}) ?? 'N/A';
            ''',
            ['a'],
        )

    async def test_edgeql_coalesce_set_of_01(self):
        await self.assert_query_result(
            r'''
                SELECT <str>Publication.id ?? <str>count(Publication)
            ''',
            ['0'],
        )

    async def test_edgeql_coalesce_set_of_02(self):
        await self.assert_query_result(
            r'''
                SELECT Publication.title ?? <str>count(Publication)
            ''',
            ['0'],
        )

    async def test_edgeql_coalesce_set_of_03(self):
        await self.assert_query_result(
            r'''
                SELECT <str>Publication.id ?= <str>count(Publication)
            ''',
            [False],
        )

    async def test_edgeql_coalesce_set_of_04(self):
        await self.assert_query_result(
            r'''
                SELECT Publication.title ?= <str>count(Publication)
            ''',
            [False],
        )

    async def test_edgeql_coalesce_set_of_05(self):
        await self.assert_query_result(
            r'''
                SELECT (Publication.title ?? <str>count(Publication))
                       ?? Publication.title
            ''',
            ['0'],
        )

    async def test_edgeql_coalesce_set_of_06(self):
        await self.assert_query_result(
            r'''
                SELECT (Publication.title ?= <str>count(Publication),
                        Publication)
            ''',
            [],
        )

    async def test_edgeql_coalesce_set_of_07(self):
        await self.assert_query_result(
            r'''
                SELECT (Publication.title ?= '0',
                        (Publication.title ?? <str>count(Publication)));
            ''',
            [[False, '0']],
        )

    async def test_edgeql_coalesce_set_of_08(self):
        await self.assert_query_result(
            r'''
                SELECT ("1" if Publication.title ?= "foo" else "2") ++
                       (Publication.title ?? <str>count(Publication))
            ''',
            ['20'],
        )

    async def test_edgeql_coalesce_set_of_09(self):
        await self.assert_query_result(
            r'''
                SELECT (Publication.title ?= "Foo", Publication.title ?= "bar")
            ''',
            [[False, False]],
        )

    async def test_edgeql_coalesce_set_of_10(self):
        await self.assert_query_result(
            r'''
                SELECT (Publication.title++Publication.title ?= "Foo",
                        Publication.title ?= "bar")
            ''',
            [[False, False]],
        )

    async def test_edgeql_coalesce_set_of_11(self):
        await self.assert_query_result(
            r'''
                SELECT (Publication.title ?= "", count(Publication))
            ''',
            [[False, 0]],
        )

    async def test_edgeql_coalesce_set_of_12(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    Publication ?= Publication,
                    (Publication.title++Publication.title
                       ?= Publication.title) ?=
                    (Publication ?!= Publication)
                )
            ''',
            [[True, False]]
        )

    async def test_edgeql_coalesce_set_of_13(self):
        await self.assert_query_result(
            r'''
                SELECT (Publication ?= Publication, Publication)
            ''',
            [],
        )

    async def test_edgeql_coalesce_set_of_nonempty_01(self):
        await self.con.execute(
            '''INSERT Publication { title := "1" }''')
        await self.con.execute(
            '''INSERT Publication { title := "asdf" }''')

        await self.assert_query_result(
            r'''
                SELECT Publication.title ?= <str>count(Publication)
            ''',
            [True, False],
        )

    @test.xfail('''
        This test produces a bogus NULL object, when it should
        return an empty set.

        I think it is a issue in relgen.
    ''')
    async def test_edgeql_coalesce_self_01(self):
        # This is busted!
        await self.assert_query_result(
            r'''
                SELECT Publication ?? Publication
            ''',
            [],
        )
