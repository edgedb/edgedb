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

from edb.server import _testbase as tb


class TestEdgeQLCoalesce(tb.QueryTestCase):
    """The test DB is designed to test various coalescing operations.
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_coalesce_setup.eql')

    async def test_edgeql_coalesce_scalar_01(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                time_estimate := Issue.time_estimate ?? -1
            };
        ''', lambda x: x['time_estimate'], [
            [
                {'time_estimate': -1},
                {'time_estimate': -1},
                {'time_estimate': -1},
                {'time_estimate': 60},
                {'time_estimate': 90},
                {'time_estimate': 90},
            ],
        ])

    async def test_edgeql_coalesce_scalar_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT (Issue.number, Issue.time_estimate ?? -1)
            ORDER BY Issue.number;
        ''', [
            [
                ['1', 60],
                ['2', 90],
                ['3', 90],
                ['4', -1],
                ['5', -1],
                ['6', -1],
            ],
        ])

    async def test_edgeql_coalesce_scalar_03(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # Only values present in the graph will be selected.
            # There is at least one value there.
            # Therefore, the second argument to ?? will not be returned.
            SELECT Issue.time_estimate ?? -1;
        ''', lambda x: x, [
            [
                60,
                90,
                90,
            ],
        ])

    async def test_edgeql_coalesce_scalar_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            # No open issue has a time_estimate, so the first argument
            # to ?? is an empty set.
            # Therefore, the second argument to ?? will be returned.
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?? -1;
        ''', [
            [
                -1,
            ],
        ])

    async def test_edgeql_coalesce_scalar_05(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                I := (SELECT Issue
                      FILTER Issue.status.name = 'Open')
            # No open issue has a time_estimate, so the first argument
            # to ?? is an empty set.
            # Therefore, the second argument to ?? will be returned.
            SELECT I.time_estimate ?? -1;
        ''', [
            [
                -1
            ],
        ])

    async def test_edgeql_coalesce_scalar_06(self):
        # The result is either an empty set if at least one
        # estimate exists, or `-1` if no estimates exist.
        # Our database contains one estimate.
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT Issue.time_estimate ?? -1
            FILTER NOT EXISTS Issue.time_estimate;
        """, [
            []
        ])

    async def test_edgeql_coalesce_scalar_07(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                number,
                has_estimate := Issue.time_estimate ?!= <int64>{}
            };
        ''', lambda x: x['number'], [
            [
                {'number': '1', 'has_estimate': True},
                {'number': '2', 'has_estimate': True},
                {'number': '3', 'has_estimate': True},
                {'number': '4', 'has_estimate': False},
                {'number': '5', 'has_estimate': False},
                {'number': '6', 'has_estimate': False},
            ],
        ])

    async def test_edgeql_coalesce_scalar_08(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT (Issue.number, Issue.time_estimate ?= 60)
            ORDER BY Issue.number;
        ''', [
            [
                ['1', True],
                ['2', False],
                ['3', False],
                ['4', False],
                ['5', False],
                ['6', False],
            ],
        ])

    async def test_edgeql_coalesce_scalar_09(self):
        await self.assert_sorted_query_result(r'''
            # Only values present in the graph will be selected.
            WITH MODULE test
            SELECT Issue.time_estimate ?= 60;

            WITH MODULE test
            SELECT Issue.time_estimate ?= <int64>{};
        ''', lambda x: x, [
            [
                False, False, True,
            ], [
                False, False, False,
            ]
        ])

    async def test_edgeql_coalesce_scalar_10(self):
        await self.assert_query_result(r'''
            # No open issue has a time_estimate, so the first argument
            # to ?= is an empty set.
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?= <int64>{};
        ''', [
            [
                True,
            ],
        ])

    async def test_edgeql_coalesce_scalar_11(self):
        await self.assert_query_result(r'''
            # No open issue has a time_estimate, so the first argument
            # to ?!= is an empty set.
            WITH
                MODULE test,
                I := (SELECT Issue
                      FILTER Issue.status.name = 'Open')
            SELECT I.time_estimate ?!= <int64>{};

            WITH
                MODULE test,
                I := (SELECT Issue
                      FILTER Issue.status.name = 'Open')
            SELECT I.time_estimate ?!= 60;
        ''', [
            [
                False
            ], [
                True
            ]
        ])

    async def test_edgeql_coalesce_scalar_12(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                number,
                time_estimate,
                related_to: {time_estimate},
            }
            ORDER BY Issue.number;

            # now test a combination of several coalescing operators
            WITH MODULE test
            SELECT
                Issue.time_estimate ??
                Issue.related_to.time_estimate ?=
                    <int64>Issue.number * 12
            ORDER BY Issue.number;
        ''', [
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
            ],

            [
                False, False, False, False, True, False,
            ],
        ])

    async def test_edgeql_coalesce_set_01(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                time_estimate := Issue.time_estimate ?? {-1, -2}
            };
        ''', lambda x: x['time_estimate'], [
            [
                {'time_estimate': [-1, -2]},
                {'time_estimate': [-1, -2]},
                {'time_estimate': [-1, -2]},
                {'time_estimate': [60]},
                {'time_estimate': [90]},
                {'time_estimate': [90]},
            ],
        ])

    async def test_edgeql_coalesce_set_02(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                multi te := (
                    SELECT Issue.time_estimate ?? {-1, -2}
                )
            };
        ''', lambda x: x['te'], [
            [
                {'te': [-1, -2]},
                {'te': [-1, -2]},
                {'te': [-1, -2]},
                {'te': [60]},
                {'te': [90]},
                {'te': [90]},
            ],
        ])

    async def test_edgeql_coalesce_set_03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT _ := (Issue.number, Issue.time_estimate ?? {-1, -2})
            ORDER BY _;
        ''', [
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
        ])

    async def test_edgeql_coalesce_set_04(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # Only values present in the graph will be selected.
            # There is at least one value there.
            # Therefore, the second argument to ?? will not be returned.
            SELECT Issue.time_estimate ?? {-1, -2};
        ''', lambda x: x, [
            [
                60,
                90,
                90,
            ],
        ])

    async def test_edgeql_coalesce_set_05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            # No open issue has a time_estimate, so the first argument
            # to ?? is an empty set.
            # Therefore, the second argument to ?? will be returned.
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?? {-1, -2};
        ''', [
            {
                -1, -2,
            },
        ])

    async def test_edgeql_coalesce_set_06(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                I := (SELECT Issue
                      FILTER Issue.status.name = 'Open')
            # No open issue has a time_estimate, so the first argument
            # to ?? is an empty set.
            # Therefore, the second argument to ?? will be returned.
            SELECT I.time_estimate ?? {-1, -2};
        ''', [
            {
                -1, -2,
            },
        ])

    async def test_edgeql_coalesce_set_07(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                number,
                te := Issue.time_estimate ?= {60, 30}
            };
        ''', lambda x: x['number'], [
            [
                {'number': '1', 'te': {True, False}},
                {'number': '2', 'te': [False, False]},
                {'number': '3', 'te': [False, False]},
                {'number': '4', 'te': [False, False]},
                {'number': '5', 'te': [False, False]},
                {'number': '6', 'te': [False, False]},
            ],
        ])

    async def test_edgeql_coalesce_set_08(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT _ := (Issue.number, Issue.time_estimate ?= {60, 90})
            ORDER BY _;
        ''', [
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
        ])

    async def test_edgeql_coalesce_set_09(self):
        await self.assert_sorted_query_result(r'''
            # Only values present in the graph will be selected.
            WITH MODULE test
            SELECT Issue.time_estimate ?= {60, 30};
        ''', lambda x: x, [
            [
                False,
                False,
                False,
                False,
                False,
                True,
            ],
        ])

    async def test_edgeql_coalesce_set_10(self):
        await self.assert_query_result(r'''
            # No open issue has a time_estimate, so the first argument
            # to ?!= is an empty set.
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?!= {-1, -2};
        ''', [
            [
                True, True,
            ],
        ])

    async def test_edgeql_coalesce_set_11(self):
        await self.assert_query_result(r'''
            # No open issue has a time_estimate, so the first argument
            # to ?= is an empty set.
            WITH
                MODULE test,
                I := (SELECT Issue
                      FILTER Issue.status.name = 'Open')
            SELECT I.time_estimate ?= {-1, -2};
        ''', [
            [
                False, False,
            ],
        ])

    async def test_edgeql_coalesce_dependent_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                # for every issue, there's a unique derived "default" to use
                # with ??
                time_estimate := Issue.time_estimate ?? -<int64>Issue.number
            } ORDER BY Issue.time_estimate;
        ''', [
            [
                {'time_estimate': -6},
                {'time_estimate': -5},
                {'time_estimate': -4},
                {'time_estimate': 60},
                {'time_estimate': 90},
                {'time_estimate': 90},
            ],
        ])

    async def test_edgeql_coalesce_dependent_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            # for every issue, there's a unique derived "default" to use
            # with ??
            SELECT (Issue.number, Issue.time_estimate ?? -<int64>Issue.number)
            ORDER BY Issue.number;
        ''', [
            [
                ['1', 60],
                ['2', 90],
                ['3', 90],
                ['4', -4],
                ['5', -5],
                ['6', -6],
            ],
        ])

    async def test_edgeql_coalesce_dependent_03(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # ?? is OPTIONAL w.r.t. first argument, so it behaves like
            # an element-wise function. Therefore, the longest common
            # prefix `Issue` is factored out and the expression is
            # evaluated for every Issue.
            SELECT Issue.time_estimate ?? -<int64>Issue.number;
        ''', lambda x: x, [
            [
                -6, -5, -4, 60, 90, 90,
            ],
        ])

    async def test_edgeql_coalesce_dependent_04(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # Since ?? is OPTIONAL over it's first argument,
            # the expression is evaluated for all six issues.
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?? -<int64>Issue.number;
        ''', lambda x: x, [
            [
                -6, -5, -4, -3, -2, -1
            ],
        ])

    async def test_edgeql_coalesce_dependent_05(self):
        await self.assert_sorted_query_result(r'''
            # Unlike the above test, we refer to the
            # same "open" subset of issues on both
            # sides of ??, so the result set contains
            # only three elements.
            WITH
                MODULE test,
                I := (SELECT Issue
                      FILTER Issue.status.name = 'Open')
            SELECT I.time_estimate ?? -<int64>I.number;
        ''', lambda x: x, [
            [
                -6, -5, -4,
            ],
        ])

    async def test_edgeql_coalesce_dependent_06(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                I2 := Issue
            # ?? is OPTIONAL w.r.t. first argument, so it behaves like
            # an element-wise function. However, since there is no
            # common prefix, the expression gets evaluated ONLY for
            # existing values of `Issue.time_estimate`.
            SELECT Issue.time_estimate ?? -<int64>I2.number;
        ''', lambda x: x, [
            [
                60, 90, 90,
            ],
        ])

    async def test_edgeql_coalesce_dependent_07(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?? -<int64>Issue.number;
        ''', lambda x: x, [
            [
                -6, -5, -4, -3, -2, -1,
            ],
        ])

    async def test_edgeql_coalesce_dependent_08(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # On one hand the right operand of ?? is not independent
            # of the left. On the other hand, it is constructed in
            # such a way as to be equivalent to literal `-1` for the
            # case when its value is important.
            #
            # LCP is `Issue.time_estimate`, so this should not
            # actually be evaluated for every `Issue`, but for every
            # `Issue.time_estimate`.
            SELECT Issue.time_estimate ?? {Issue.time_estimate, -1};
        ''', lambda x: x, [
            [
                60, 90, 90,
            ],
        ])

    async def test_edgeql_coalesce_dependent_09(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            # `Issue` on both sides is behind a fence, so the left-hand
            # expression is an empty set, and the result is a union
            # of all existing time estimates and -1.
            SELECT _ := (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?? {Issue.time_estimate, -1}
            ORDER BY _;
        ''', [
            [
                -1, 60, 90, 90
            ],
        ])

    async def test_edgeql_coalesce_dependent_10(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                I := (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                )
            # `I.time_estimate` is now a LCP
            SELECT I.time_estimate ?? {I.time_estimate, -1};
        ''', [
            [
                -1,
            ],
        ])

    async def test_edgeql_coalesce_dependent_11(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Issue {
                number,
                foo := Issue.time_estimate ?= <int64>Issue.number * 30
            } ORDER BY Issue.number;
        ''', [
            [
                {'number': '1', 'foo': False},
                {'number': '2', 'foo': False},
                {'number': '3', 'foo': True},
                {'number': '4', 'foo': False},
                {'number': '5', 'foo': False},
                {'number': '6', 'foo': False},
            ],
        ])

    async def test_edgeql_coalesce_dependent_12(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT (
                Issue.number,
                Issue.time_estimate ?!= <int64>Issue.number * 30
            )
            ORDER BY Issue.number;
        ''', [
            [
                ['1', True],
                ['2', True],
                ['3', False],
                ['4', True],
                ['5', True],
                ['6', True],
            ],
        ])

    async def test_edgeql_coalesce_dependent_13(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # ?= is OPTIONAL w.r.t. both arguments, so it behaves like
            # an element-wise function. Therefore, the longest common
            # prefix `Issue` is factored out and the expression is
            # evaluated for every Issue.
            SELECT Issue.time_estimate ?= <int64>Issue.number * 30;
        ''', lambda x: x, [
            [
                False,
                False,
                False,
                False,
                False,
                True,
            ],
        ])

    async def test_edgeql_coalesce_dependent_14(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?= <int64>Issue.number;
        ''', lambda x: x, [
            [
                False, False, False, False, False, False
            ],
        ])

    async def test_edgeql_coalesce_dependent_15(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                I := (SELECT Issue
                      FILTER Issue.status.name = 'Open')
            # Same as dependent_13, but only 'Open' issues being considered.
            SELECT I.time_estimate ?!= I.time_spent_log.spent_time;
        ''', lambda x: x, [
            [
                False, False, False,
            ],
        ])

    async def test_edgeql_coalesce_dependent_16(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                I2 := Issue
            # ?= is OPTIONAL w.r.t. both arguments, so it behaves like
            # an element-wise function. However, since there is no
            # common prefix, the expression gets evaluated ONLY for
            # existing values of `Issue.time_estimate`, so the cardinality
            # of the result set is 18 (3 * 6).
            SELECT Issue.time_estimate ?= <int64>I2.number * 30;
        ''', lambda x: x, [
            [
                False, False, False,
                False, False, False,
                False, False, False,
                False, False, False,
                False, False, False,
                True, True, True,
            ],
        ])

    async def test_edgeql_coalesce_dependent_17(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
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
        ''', lambda x: x, [
            [
                True, True, True,
                True, True, True,
            ],
        ])

    async def test_edgeql_coalesce_dependent_18(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # LCP is `Issue.time_estimate`, so this should not
            # actually be evaluated for every `Issue`, but for every
            # `Issue.time_estimate`.
            SELECT Issue.time_estimate ?= Issue.time_estimate * 2;
        ''', lambda x: x, [
            [
                False, False, False,
            ],
        ])

    async def test_edgeql_coalesce_dependent_19(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            # `Issue` is now a LCP and the overall expression will be
            # evaluated for every `Issue`.
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_estimate ?= Issue.time_estimate * 2;
        ''', lambda x: x, [
            [
                False, False, False, True, True, True,
            ],
        ])

    async def test_edgeql_coalesce_dependent_20(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                I := (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                )
            # `I.time_estimate` is now a LCP
            SELECT I.time_estimate ?= I.time_estimate * 2;
        ''', [
            [
                True,
            ],
        ])

    async def test_edgeql_coalesce_dependent_21(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X[IS Priority].name ?? X[IS Status].name;
        ''', [
            {'High', 'Low', 'Open', 'Closed'},
        ])

    async def test_edgeql_coalesce_dependent_22(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X[IS Priority].name[0] ?? X[IS Status].name;

            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X[IS Priority].name ?? X[IS Status].name[0];

            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X[IS Priority].name[0] ?? X[IS Status].name[0];
        ''', [
            {'H', 'L', 'Open', 'Closed'},
            {'High', 'Low', 'O', 'C'},
            {'H', 'L', 'O', 'C'},
        ])

    async def test_edgeql_coalesce_dependent_23(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X {
                foo := X[IS Priority].name ?? X[IS Status].name
            };

            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X {
                foo := X[IS Priority].name[0] ?? X[IS Status].name
            };

            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X {
                foo := X[IS Priority].name ?? X[IS Status].name[0]
            };

            WITH
                MODULE test,
                X := {Priority, Status}
            SELECT X {
                foo := X[IS Priority].name[0] ?? X[IS Status].name[0]
            };
        ''', lambda x: x['foo'], [
            [
                {'foo': 'Closed'},
                {'foo': 'High'},
                {'foo': 'Low'},
                {'foo': 'Open'}
            ],
            [
                {'foo': 'Closed'},
                {'foo': 'H'},
                {'foo': 'L'},
                {'foo': 'Open'}
            ],
            [
                {'foo': 'C'},
                {'foo': 'High'},
                {'foo': 'Low'},
                {'foo': 'O'}
            ],
            [
                {'foo': 'C'},
                {'foo': 'H'},
                {'foo': 'L'},
                {'foo': 'O'}
            ],
        ])

    async def test_edgeql_coalesce_object_01(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
            SELECT Issue {
                number,
                time_spent_log := (
                    SELECT x := (Issue.time_spent_log ?? DUMMY) {
                        spent_time
                    }
                    ORDER BY x.spent_time
                )
            } ORDER BY Issue.number;
        ''', [
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
        ])

    async def test_edgeql_coalesce_object_02(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
            SELECT x := (
                Issue.number,
                (Issue.time_spent_log ?? DUMMY).spent_time
            ) ORDER BY x.0 THEN x.1;
        ''', [
            [
                ['1', 60],
                ['2', 90],
                ['3', 30],
                ['3', 60],
                ['4', -1],
                ['5', -1],
                ['6', -1],
            ],
        ])

    async def test_edgeql_coalesce_object_03(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
            SELECT x := (Issue.time_spent_log ?? DUMMY) {
                spent_time
            }
            ORDER BY x.spent_time;
        ''', lambda x: x['spent_time'], [
            [
                {'spent_time': 30},
                {'spent_time': 60},
                {'spent_time': 60},
                {'spent_time': 90},
            ],
        ])

    async def test_edgeql_coalesce_object_04(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
            SELECT (
                (SELECT Issue
                 FILTER Issue.status.name = 'Open').time_spent_log
                ??
                DUMMY
            ) {
                spent_time
            };
        ''', [
            [
                {'spent_time': -1},
            ],
        ])

    async def test_edgeql_coalesce_object_05(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy'),
                I := (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                )
            SELECT (I.time_spent_log ?? DUMMY) {
                spent_time
            };
        ''', [
            [
                {'spent_time': -1},
            ],
        ])

    async def test_edgeql_coalesce_object_06(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                LOG1 := (SELECT LogEntry FILTER LogEntry.body = 'Log1')
            SELECT Issue {
                number,
                log1 := Issue.time_spent_log ?= LOG1
            } ORDER BY Issue.number;
        ''', [
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
        ])

    async def test_edgeql_coalesce_object_07(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                LOG1 := (SELECT LogEntry FILTER LogEntry.body = 'Log1')
            SELECT (
                Issue.number, Issue.time_spent_log ?= LOG1
            ) ORDER BY Issue.number;
        ''', [
            [
                ['1', True],
                ['2', False],
                ['3', False],
                ['3', False],
                ['4', False],
                ['5', False],
                ['6', False],
            ],
        ])

    async def test_edgeql_coalesce_object_08(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                LOG1 := (SELECT LogEntry FILTER LogEntry.body = 'Log1')
            SELECT Issue.time_spent_log ?!= LOG1;
        ''', lambda x: x, [
            [
                False,
                True,
                True,
                True,
            ],
        ])

    async def test_edgeql_coalesce_object_09(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy')
            SELECT (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            ).time_spent_log ?= DUMMY;
        ''', [
            [
                False,
            ],
        ])

    async def test_edgeql_coalesce_object_10(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                DUMMY := (SELECT LogEntry FILTER LogEntry.body = 'Dummy'),
                I := (
                    SELECT Issue
                    FILTER Issue.status.name = 'Open'
                )
            SELECT I.time_spent_log ?!= DUMMY;
        ''', [
            [
                True,
            ],
        ])

    async def test_edgeql_coalesce_wrapping_optional(self):
        await self.query(r'''
            CREATE FUNCTION test::optfunc(
                    a: std::str, b: OPTIONAL std::str) -> std::str
                FROM EdgeQL $$
                    SELECT b IF a = 'foo' ELSE a
                $$;
        ''')

        await self.assert_query_result(r'''
            SELECT test::optfunc('foo', 'b') ?? 'N/A';
            SELECT test::optfunc('a', <str>{}) ?? 'N/A';
        ''', [
            ['b'],
            ['a'],
        ])
