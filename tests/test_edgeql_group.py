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
from edb.tools import test


@test.not_implemented('GROUP statement is not yet implemented')
class TestEdgeQLGroup(tb.QueryTestCase):
    '''These tests are focused on using GROUP statement.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SCHEMA_CARDS = os.path.join(os.path.dirname(__file__), 'schemas',
                                'cards.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'groups_setup.edgeql')

    async def test_edgeql_group_simple_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP User
                USING _ := User.name
                BY _
                INTO User
                UNION count(User.<owner);
            ''',
            {4, 2},
        )

    async def test_edgeql_group_simple_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue := Issue
                # time_estimate is {} on some Issues,
                # but that's a valid grouping
                USING _ := Issue.time_estimate
                BY _
                INTO Issue
                # count using a property
                UNION count(Issue.id);
            ''',
            {3, 1},
        )

    async def test_edgeql_group_simple_03(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING _ := Issue.time_estimate
                BY _
                INTO Issue
                # count Issue directly
                UNION count(Issue);
            ''',
            {3, 1},
        )

    async def test_edgeql_group_simple_04(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING _ := Issue.time_estimate
                BY _
                INTO Issue
                # count Issue name, which should be same as counting
                # Issues, since the name property is *1
                UNION count(Issue.name);
            ''',
            {3, 1},
        )

    async def test_edgeql_group_simple_05(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING _ := Issue.time_estimate
                BY _
                INTO Issue
                # count Issue statuses, which is not the same as counting
                # Issues, since multiple Issues can point to the same Status
                UNION count(Issue.status);
            ''',
            {2, 1},
        )

    async def test_edgeql_group_simple_06(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING _ := Issue.time_estimate
                BY _
                INTO Issue
                # unusual qualifier for 'count', but should be the same as
                # counting the statuses directly
                UNION count(DISTINCT Issue.status.id);
            ''',
            {2, 1},
        )

    async def test_edgeql_group_by_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING B :=  Issue.status.name
                BY B
                INTO Issue
                UNION (
                    sum := sum(<int64>Issue.number),
                    status := B,
                ) ORDER BY B;
            """,
            [
                {
                    'status': 'Closed',
                    'sum': 7,
                },
                {
                    'status': 'Open',
                    'sum': 3,
                }
            ],
        )

    async def test_edgeql_group_by_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING B :=  Issue.status.name
                BY B
                INTO Issue
                UNION _ := (
                    sum := sum(<int64>Issue.number),
                    status := B,
                )
                FILTER
                    _.sum > 5
                ORDER BY
                    B;
            """,
            [{
                'status': 'Closed',
                'sum': 7,
            }],
        )

    async def test_edgeql_group_result_alias_01(self):
        await self.assert_query_result(
            r'''
                # re-use the same "_" alias in nested scope
                WITH MODULE test
                GROUP Issue
                USING _ :=  Issue.time_estimate
                BY _
                INTO Issue
                UNION _ := (
                    count := count(Issue.status.id),
                    te := array_agg(DISTINCT Issue.time_estimate > 0),
                ) ORDER BY
                    _.te;
            ''',
            [{'count': 3, 'te': []}, {'count': 1, 'te': [True]}]
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING _ :=  Issue.time_estimate
                BY _
                INTO Issue
                UNION _ := (
                    count := count(Issue.status.id),
                    te := array_agg(DISTINCT Issue.time_estimate > 0),
                ) ORDER BY
                    _.te DESC;
            ''',
            [{'count': 1, 'te': [True]}, {'count': 3, 'te': []}],
        )

    async def test_edgeql_group_result_alias_02(self):
        await self.assert_query_result(
            r'''
                # re-use the same "_" alias in nested scope
                WITH MODULE test
                GROUP Issue
                USING _ :=  Issue.time_estimate
                BY _
                INTO Issue
                UNION _ := (
                    count := count(Issue.status.id),
                    # confusing, but legal usage of '_' to refer to BY
                    # (this is comparable to SELECT Issue := count(Issue))
                    te := array_agg(_ > 0),
                ) ORDER BY
                    _.te DESC;
            ''',
            [{'count': 1, 'te': [True]}, {'count': 3, 'te': []}],
        )

    async def test_edgeql_group_nested_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    R := (
                        name := User.name,
                        issues := array_agg(
                            (
                                GROUP UserIssue := User.<owner[IS Issue]
                                USING B :=  UserIssue.status.name
                                BY B
                                INTO UserIssue
                                UNION (
                                    status := B,
                                    count := count(UserIssue),
                                )
                                ORDER BY
                                    B
                            )
                        )
                    )
                ORDER BY R.name;
            """,
            [
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
            ]
        )

    async def test_edgeql_group_returning_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING _ :=  Issue.time_estimate
                BY _
                INTO Issue
                # The issues should be partitioned into 2 sub-sets by
                # Issue.time_estimate (with values {} and 3000). Therefore
                # we expect 2 results combined via UNION.
                UNION 42;
            ''',
            [42, 42],
        )

    async def test_edgeql_group_returning_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING B := Issue.time_estimate
                BY B
                INTO Issue
                # No reason to restrict the above example to doing a
                # UNION of singletons.
                UNION _ := {42, count(Issue)}
                ORDER BY _;
            ''',
            [1, 3, 42, 42],
        )

    async def test_edgeql_group_returning_03(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING B := Issue.status
                BY B
                INTO Issue
                # The result should be a set of status objects,
                # augmented with Issue.numbers corresponding to the
                # status.
                UNION B {
                    name,
                    nums := Issue.number
                }
                ORDER BY B.name;
            ''',
            [
                {
                    'name': 'Closed',
                    'nums': {'3', '4'},
                },
                {
                    'name': 'Open',
                    'nums': {'1', '2'},
                }
            ],
        )

    async def test_edgeql_group_returning_04(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                GROUP Issue
                USING _ := Issue.status
                BY _
                INTO Issue
                # Identical to the previous example, but expressed
                # slightly differently.
                UNION (
                    SELECT
                        Status {
                            name,
                            nums := Issue.number
                        }
                    # all issues in this subset will have the same
                    # status, so this FILTER is valid, although not
                    # necessarily optimal
                    FILTER Status = Issue.status
                )
                ORDER BY Status.name;
            ''',
            [
                {
                    'name': 'Closed',
                    'nums': {'3', '4'},
                },
                {
                    'name': 'Open',
                    'nums': {'1', '2'},
                }
            ],
        )

    async def test_edgeql_group_returning_05(self):
        await self.assert_query_result(
            r'''
                # a trivial group that is actually not doing anything
                # different from a plain SELECT
                WITH MODULE cards
                GROUP Card
                USING _ :=  Card.element
                BY _
                INTO Card
                UNION Card.name
                ORDER BY
                    Card.name;
            ''',
            [
                'Bog monster',
                'Djinn',
                'Dragon',
                'Dwarf',
                'Giant eagle',
                'Giant turtle',
                'Golem',
                'Imp',
                'Sprite',
            ],
        )

    async def test_edgeql_group_returning_06(self):
        await self.assert_query_result(
            r'''
                # a trivial group that is actually not doing anything
                # different from a plain SELECT
                WITH MODULE cards
                GROUP Card
                USING _ :=  Card.element
                BY _
                INTO Card
                UNION Card {name}
                ORDER BY
                    Card.name;
            ''',
            [
                {'name': 'Bog monster'},
                {'name': 'Djinn'},
                {'name': 'Dragon'},
                {'name': 'Dwarf'},
                {'name': 'Giant eagle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
                {'name': 'Imp'},
                {'name': 'Sprite'},
            ],
        )

    async def test_edgeql_group_returning_07(self):
        await self.assert_query_result(
            r'''
                # Nominate a leader in each group from among the group.
                #
                # The below is a very long and explicit way of forming a
                # set of "leaders" and associated "members" for each
                # element.
                WITH
                    MODULE cards,
                    C2 := Card,
                    ELEMENTAL := (
                        # group cards into arrays by element
                        GROUP Card
                        USING _ :=  Card.element
                        BY _
                        INTO Card
                        UNION array_agg(Card)
                    )
                SELECT _ := (
                    FOR X IN {ELEMENTAL}
                    # for each "elemental" array select a "leader"
                    UNION (
                        # the selection of leader is nested and has its
                        # own application of ORDER BY and LIMIT clauses
                        SELECT Card {
                                element,
                                name,
                                cost,
                                members := (
                                    # just unpacking the elemental cards
                                    # with a particular ordering and
                                    # specific links included in the final
                                    # result
                                    SELECT C2{name, cost}
                                    FILTER contains(X, C2)
                                    ORDER BY C2.cost
                                )
                            }
                        # the leader is selected from among the elemental array
                        FILTER contains(X, Card)
                        # the leader is defined as the one with the
                        # highest cost
                        ORDER BY Card.cost DESC
                        LIMIT 1
                    )
                )
                ORDER BY _.element;
            ''',
            [
                {
                    'cost': 4,
                    'name': 'Djinn',
                    'element': 'Air',
                    'members': [
                        {'cost': 1, 'name': 'Sprite'},
                        {'cost': 2, 'name': 'Giant eagle'},
                        {'cost': 4, 'name': 'Djinn'},
                    ],
                },
                {
                    'cost': 3,
                    'name': 'Golem',
                    'element': 'Earth',
                    'members': [
                        {'cost': 1, 'name': 'Dwarf'},
                        {'cost': 3, 'name': 'Golem'},
                    ],
                },
                {
                    'cost': 5,
                    'name': 'Dragon',
                    'element': 'Fire',
                    'members': [
                        {'cost': 1, 'name': 'Imp'},
                        {'cost': 5, 'name': 'Dragon'},
                    ],
                },
                {
                    'cost': 3,
                    'name': 'Giant turtle',
                    'element': 'Water',
                    'members': [
                        {'cost': 2, 'name': 'Bog monster'},
                        {'cost': 3, 'name': 'Giant turtle'},
                    ],
                }
            ]
        )

    async def test_edgeql_group_returning_08(self):
        await self.assert_query_result(
            r'''
                # Nominate a leader in each group from among the group.
                #
                # Same as previous test, but with all of the shape spec
                # factored out tot he outermost SELECT.
                WITH
                    MODULE cards,
                    ELEMENTAL := (
                        GROUP Card
                        USING _ :=  Card.element
                        BY _
                        INTO Card
                        UNION array_agg(Card)
                    )
                SELECT _ := (
                    FOR X IN {ELEMENTAL}
                    UNION (
                        SELECT Card {
                                element,
                                name,
                                cost,
                                members := (
                                    SELECT C2 := array_unpack(X)
                                    ORDER BY C2.cost
                                )
                            }
                        FILTER contains(X, Card)
                        ORDER BY Card.cost DESC
                        LIMIT 1
                    )
                ) {
                    # the entire shape spec of the result is now here
                    element,
                    name,
                    cost,
                    members: {
                        name,
                        cost
                    }
                }
                ORDER BY _.element;
            ''',
            [
                {
                    'cost': 4,
                    'name': 'Djinn',
                    'element': 'Air',
                    'members': [
                        {'cost': 1, 'name': 'Sprite'},
                        {'cost': 2, 'name': 'Giant eagle'},
                        {'cost': 4, 'name': 'Djinn'},
                    ],
                },
                {
                    'cost': 3,
                    'name': 'Golem',
                    'element': 'Earth',
                    'members': [
                        {'cost': 1, 'name': 'Dwarf'},
                        {'cost': 3, 'name': 'Golem'},
                    ],
                },
                {
                    'cost': 5,
                    'name': 'Dragon',
                    'element': 'Fire',
                    'members': [
                        {'cost': 1, 'name': 'Imp'},
                        {'cost': 5, 'name': 'Dragon'},
                    ],
                },
                {
                    'cost': 3,
                    'name': 'Giant turtle',
                    'element': 'Water',
                    'members': [
                        {'cost': 2, 'name': 'Bog monster'},
                        {'cost': 3, 'name': 'Giant turtle'},
                    ],
                }
            ]
        )

    async def test_edgeql_group_returning_09(self):
        await self.assert_query_result(
            r'''
                # Nominate a leader in each group from among the group.
                #
                # Same as previous tests, but refactored to take full
                # advantage of GROUP semantics and BY aliasing.
                WITH
                    MODULE cards,
                    C2 := Card
                GROUP Card
                USING Element :=
                        # partition cards by element
                        Card.element
                BY Element
                INTO Card
                UNION (
                    # for every partition, compute the "leader"
                    SELECT C2 {
                        element,
                        name,
                        cost,
                        members := (
                            # all members of the particular elemental
                            # partition
                            SELECT Card{name, cost}
                            ORDER BY Card.cost
                        )
                    }
                    # the leader is a member of its elemental group
                    FILTER
                        C2 IN Card
                    # the leader is simply the one with the highest cost
                    ORDER BY
                        C2.cost DESC
                    LIMIT 1
                )
                # Ordering by the expression used to partition the
                # original set. This happens to be unambiguous since we
                # compute a singleton (LIMIT 1) set for each partition.
                ORDER BY Element;
            ''',
            [
                {
                    'cost': 4,
                    'name': 'Djinn',
                    'element': 'Air',
                    'members': [
                        {'cost': 1, 'name': 'Sprite'},
                        {'cost': 2, 'name': 'Giant eagle'},
                        {'cost': 4, 'name': 'Djinn'},
                    ],
                },
                {
                    'cost': 3,
                    'name': 'Golem',
                    'element': 'Earth',
                    'members': [
                        {'cost': 1, 'name': 'Dwarf'},
                        {'cost': 3, 'name': 'Golem'},
                    ],
                },
                {
                    'cost': 5,
                    'name': 'Dragon',
                    'element': 'Fire',
                    'members': [
                        {'cost': 1, 'name': 'Imp'},
                        {'cost': 5, 'name': 'Dragon'},
                    ],
                },
                {
                    'cost': 3,
                    'name': 'Giant turtle',
                    'element': 'Water',
                    'members': [
                        {'cost': 2, 'name': 'Bog monster'},
                        {'cost': 3, 'name': 'Giant turtle'},
                    ],
                }
            ]
        )

    async def test_edgeql_group_by_tuple_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING B := (Issue.status.name, Issue.time_estimate)
                # This tuple will be {} for Issues lacking
                # time_estimate. So effectively we're expecting only 2
                # subsets, grouped by:
                # - {}
                # - ('Open', 3000)
                BY B
                INTO Issue
                UNION _ := (
                    sum := sum(<int64>Issue.number),
                    # don't forget to coalesce the {} or else the whole
                    # tuple will collapse
                    status := B.0 ?? '',
                    time_estimate := B.1 ?? 0
                ) ORDER BY B;
            """,
            [
                {
                    'status': '', 'sum': 9, 'time_estimate': 0
                },
                {
                    'status': 'Open', 'sum': 1, 'time_estimate': 3000
                }
            ],
        )

    async def test_edgeql_group_by_multiple_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING
                    Stat := Issue.status.name,
                    Est := Issue.time_estimate
                # Unlike the tuple example, these grouping sets
                # generate more combinations:
                # - 'Closed', {}
                # - 'Open', {}
                # - 'Open', 3000
                BY Stat, Est
                INTO Issue
                UNION _ := (
                    sum := sum(<int64>Issue.number),
                    # Stat is never {}, so coalescing is not needed
                    status := Stat,
                    # only this one needs to be coalesced
                    time_estimate := Est ?? 0
                ) ORDER BY _;
            """,
            [
                {
                    'status': 'Closed', 'sum': 7, 'time_estimate': 0,
                },
                {
                    'status': 'Open', 'sum': 2, 'time_estimate': 0,
                },
                {
                    'status': 'Open', 'sum': 1, 'time_estimate': 3000,
                }
            ],
        )

    async def test_edgeql_group_by_multiple_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING
                    Stat := Issue.status.name,
                    Est := Issue.time_estimate
                BY Stat, Est
                INTO Issue
                UNION (
                    sum := sum(<int64>Issue.number),
                    status := Stat,
                    time_estimate := Est ?? 0
                )
                # ordering condition derived from the grouping parameters
                ORDER BY Stat
                    THEN Est > 0 EMPTY FIRST;
            """,
            [
                {
                    'status': 'Closed', 'sum': 7, 'time_estimate': 0
                },
                {
                    'status': 'Open', 'sum': 2, 'time_estimate': 0
                },
                {
                    'status': 'Open', 'sum': 1, 'time_estimate': 3000
                }
            ],
        )

    async def test_edgeql_group_by_multiple_03(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING
                    Stat := Issue.status.name,
                    Est := Issue.time_estimate
                BY Stat, Est
                INTO Issue
                UNION (
                    # array_agg with ordering instead of sum
                    numbers := array_agg(
                        <int64>Issue.number ORDER BY Issue.number),
                    status := Stat,
                    time_estimate := Est ?? 0
                ) ORDER BY Stat THEN Est;
            """,
            [
                {
                    'status': 'Closed',
                    'time_estimate': 0,
                    'numbers': [3, 4],
                },
                {
                    'status': 'Open',
                    'time_estimate': 0,
                    'numbers': [2],
                },
                {
                    'status': 'Open',
                    'time_estimate': 3000,
                    'numbers': [1],
                }
            ],
        )

    async def test_edgeql_group_by_multiple_04(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING
                    Stat := Issue.status.name,
                    Est := Issue.time_estimate
                BY Stat, Est
                INTO Issue
                UNION (
                    # a couple of array_agg
                    numbers := array_agg(
                        <int64>Issue.number ORDER BY Issue.number),
                    watchers := array_agg(
                        <str>Issue.watchers.name ORDER BY Issue.watchers.name),
                    status := Stat,
                    time_estimate := Est ?? 0
                ) ORDER BY Stat THEN Est;
            """,
            [
                {
                    'status': 'Closed',
                    'time_estimate': 0,
                    'numbers': [3, 4],
                    'watchers': ['Elvis'],
                },
                {
                    'status': 'Open',
                    'time_estimate': 0,
                    'numbers': [2],
                    'watchers': ['Elvis'],
                },
                {
                    'status': 'Open',
                    'time_estimate': 3000,
                    'numbers': [1],
                    'watchers': ['Yury'],
                }
            ],
        )

    async def test_edgeql_group_by_multiple_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP
                    # define a computable in the GROUP expr
                    Issue := Issue {
                        less_than_four := <int64>Issue.number < 4
                    }
                USING
                    Stat := Issue.status.name,
                    # group by computable
                    X := Issue.less_than_four
                BY Stat, X
                INTO Issue
                UNION (
                    numbers := array_agg(
                        Issue.number ORDER BY Issue.number),
                    # watchers will sometimes be empty resulting in []
                    watchers := array_agg(
                        Issue.watchers.name ORDER BY Issue.watchers.name),
                    status := Stat,
                ) ORDER BY Stat THEN X;
            """,
            [
                {
                    'status': 'Closed',
                    'numbers': ['4'],
                    'watchers': []
                },
                {
                    'status': 'Closed',
                    'numbers': ['3'],
                    'watchers': ['Elvis']
                },
                {
                    'status': 'Open',
                    'numbers': ['1', '2'],
                    'watchers': ['Elvis', 'Yury']
                }
            ],
        )

    async def test_edgeql_group_by_multiple_06(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                GROUP Issue
                USING
                    Stat := Issue.status.name,
                    # group by some non-trivial expression
                    X := <int64>Issue.number < 4
                BY Stat, X
                INTO Issue
                UNION I := (
                    numbers := array_agg(
                        <int64>Issue.number ORDER BY Issue.number),
                    watchers := count(DISTINCT Issue.watchers),
                    status := Stat,
                ) ORDER BY
                    # used a mixture of different aliases in ORDER BY
                    Stat
                    THEN I.watchers
                    # should work because count evaluates to a singleton
                    THEN count(DISTINCT Issue);
            """,
            [
                {
                    'status': 'Closed',
                    'numbers': [4],
                    'watchers': 0
                },
                {
                    'status': 'Closed',
                    'numbers': [3],
                    'watchers': 1
                },
                {
                    'status': 'Open',
                    'numbers': [1, 2],
                    'watchers': 2
                }
            ],
        )

    async def test_edgeql_group_by_multiple_07(self):
        await self.assert_query_result(
            r"""
                WITH MODULE cards
                GROUP C := Card
                USING x := C.cost
                BY x
                INTO C
                UNION (
                    array_agg(C.name ORDER BY C.name),
                    # At this point C is a subset of Card. So the below
                    # expression should be the size of the subset in
                    # percent.
                    100 * count(C) / count(Card)
                ) ORDER BY x;
            """,
            [
                [['Dwarf', 'Imp', 'Sprite'], 33],
                [['Bog monster', 'Giant eagle'], 22],
                [['Giant turtle', 'Golem'], 22],
                [['Djinn'], 11],
                [['Dragon'], 11]
            ]
        )

    async def test_edgeql_group_linkproperty_simple_01(self):
        await self.assert_query_result(
            r"""
                # group by link property
                WITH MODULE cards
                GROUP Card
                USING B := Card.<deck[IS User]@count
                BY B
                INTO Card
                UNION _ := (
                    cards := array_agg(
                        DISTINCT Card.name ORDER BY Card.name),
                    count := B,
                ) ORDER BY _.count;
            """,
            [
                {
                    'cards': ['Bog monster', 'Djinn', 'Dragon', 'Giant eagle',
                              'Giant turtle', 'Golem'],
                    'count': 1
                },
                {
                    'cards': ['Dragon', 'Giant turtle', 'Golem', 'Imp'],
                    'count': 2
                },
                {
                    'cards': ['Bog monster', 'Dwarf', 'Giant eagle',
                              'Giant turtle', 'Golem'],
                    'count': 3
                },
                {
                    'cards': ['Dwarf', 'Sprite'],
                    'count': 4
                },
            ],
        )

    async def test_edgeql_group_linkproperty_simple_02(self):
        await self.assert_query_result(
            r"""
                # use link property inside a group aggregate
                WITH MODULE cards
                GROUP Card
                USING El :=
                        Card.element
                BY El
                INTO Card
                UNION _ := (
                    cards := array_agg(
                        DISTINCT Card.name ORDER BY Card.name),
                    element := El,
                    count := sum(Card.<deck[IS User]@count),
                ) ORDER BY _.count;
            """,
            [
                {
                    'element': 'Fire',
                    'cards': ['Dragon', 'Imp'],
                    'count': 5,
                },
                {
                    'element': 'Earth',
                    'cards': ['Dwarf', 'Golem'],
                    'count': 13,
                },
                {
                    'element': 'Air',
                    'cards': ['Djinn', 'Giant eagle', 'Sprite'],
                    'count': 14,
                },
                {
                    'element': 'Water',
                    'cards': ['Bog monster', 'Giant turtle'],
                    'count': 19,
                },
            ]
        )

    async def test_edgeql_group_linkproperty_simple_03(self):
        await self.assert_query_result(
            r"""
                # group by link property
                WITH MODULE cards
                GROUP User
                # get the nickname that this user from Alice (if any)
                USING B := (
                    SELECT User.<friends[IS User]@nickname
                    FILTER User.<friends[IS User].name = 'Alice'
                )
                BY B
                INTO F
                UNION _ := (
                    nickname := B,
                    # the tuple without nickname will be missing from the
                    # final result
                    name := array_agg(F.name)
                ) ORDER BY _.nickname;
            """,
            [
                {'name': ['Carol'], 'nickname': 'Firefighter'},
                {'name': ['Dave'], 'nickname': 'Grumpy'},
                {'name': ['Bob'], 'nickname': 'Swampy'},
            ]
        )

    async def test_edgeql_group_linkproperty_nested_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE cards
                SELECT User {
                    name,
                    # total card count across the deck
                    total := sum(User.deck@count),
                    # group each deck by elements, adding up the counts
                    elements := (
                        WITH
                            # need an intermediate representation of
                            # user's deck to be able to associate the
                            # count link property with the specific
                            U := User {
                                deck: {
                                    count := User.deck@count
                                }
                            }
                        # grouping the cards now
                        GROUP U.deck
                        USING B :=  U.deck.element
                        BY B
                        INTO D
                        UNION _ := (
                            name := B,
                            count := sum(D.count),
                        )
                        ORDER BY _.name
                    )
                } ORDER BY User.name;
            """,
            [
                {
                    'name': 'Alice',
                    'total': 10,
                    'elements': [
                        {'name': 'Fire', 'count': 4},
                        {'name': 'Water', 'count': 6}
                    ]
                },
                {
                    'name': 'Bob',
                    'total': 12,
                    'elements': [
                        {'name': 'Earth', 'count': 6},
                        {'name': 'Water', 'count': 6}
                    ]
                },
                {
                    'name': 'Carol',
                    'total': 19,
                    'elements': [
                        {'name': 'Air', 'count': 8},
                        {'name': 'Earth', 'count': 6},
                        {'name': 'Water', 'count': 5}
                    ]
                },
                {
                    'name': 'Dave',
                    'total': 10,
                    'elements': [
                        {'name': 'Air', 'count': 6},
                        {'name': 'Earth', 'count': 1},
                        {'name': 'Fire', 'count': 1},
                        {'name': 'Water', 'count': 2}
                    ]
                }
            ]
        )

    async def test_edgeql_group_linkproperty_multiple_01(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE cards
                    U := (
                        SELECT User {
                            deck: {
                                count := User.deck@count
                            }
                        } FILTER User.name = 'Dave'
                    )
                GROUP
                    U.deck
                USING
                    El := U.deck.element,
                    Count := U.deck.count
                BY El, Count
                INTO D
                UNION _ := (
                    cards := array_agg(D.name ORDER BY D.name),
                    element := El,
                    count := Count,
                ) ORDER BY El THEN Count;
            """,
            [
                # compare to test_edgeql_props_basic01
                {
                    'element': 'Air',
                    'count': 1,
                    'cards': ['Djinn', 'Giant eagle'],
                },
                {
                    'element': 'Air',
                    'count': 4,
                    'cards': ['Sprite'],
                },
                {
                    'element': 'Earth',
                    'count': 1,
                    'cards': ['Golem'],
                },
                {
                    'element': 'Fire',
                    'count': 1,
                    'cards': ['Dragon'],
                },
                {
                    'element': 'Water',
                    'count': 1,
                    'cards': ['Bog monster', 'Giant turtle'],
                },
            ]
        )

    async def test_edgeql_group_scalar_01(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    I := <int64>Issue.number
                GROUP I
                USING _ :=  I % 2 = 0
                BY _
                INTO I
                UNION _r := (
                    values := array_agg(I ORDER BY I)
                ) ORDER BY _r.values;
            """,
            [
                {'values': [1, 3]},
                {'values': [2, 4]}
            ]
        )
