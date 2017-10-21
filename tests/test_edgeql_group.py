##
# Copyright (c) 2012-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest  # NOQA

from edgedb.server import _testbase as tb


class TestEdgeQLGroup(tb.QueryTestCase):
    '''These tests are focused on using GROUP with a FOR statement.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SCHEMA_CARDS = os.path.join(os.path.dirname(__file__), 'schemas',
                                'cards.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'groups_setup.eql')

    @tb.expected_optimizer_failure
    async def test_edgeql_group_simple_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR User, _ IN (
                GROUP User BY User.name
            )
            UNION OF count(User.<owner);
        ''', [
            {4, 2},
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_simple_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            # count using link 'id'
            UNION OF count(Issue.id);
        ''', [
            {3, 1},
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_simple_03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            # count Issue directly
            UNION OF count(Issue);
        ''', [
            {3, 1},
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_simple_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            # count Issue statuses, which should be same as counting
            # Issues, since the status link is *1
            UNION OF count(Issue.status.id);
        ''', [
            {3, 1},
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_simple_05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            # unusual qualifier for 'count'
            UNION OF count(DISTINCT Issue.status.id);
        ''', [
            {2, 1},
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, B IN (
                GROUP Issue
                BY Issue.status.name
            )
            UNION OF (
                sum := sum(<int>Issue.number),
                status := B,
            ) ORDER BY B;
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
    async def test_edgeql_group_by_02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, B IN (
                GROUP Issue
                BY Issue.status.name
            )
            UNION OF _ := (
                sum := sum(<int>Issue.number),
                status := B,
            )
            FILTER
                _.sum > 5
            ORDER BY
                B;
        """, [
            [{
                'status': 'Closed',
                'sum': 7,
            }],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_result_alias_01(self):
        await self.assert_query_result(r'''
            # re-use the same "_" alias in nested scope
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            UNION OF _ := (
                count := count(Issue.status.id),
                te := array_agg(DISTINCT Issue.time_estimate > 0),
            ) ORDER BY
                _.te;

            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            UNION OF _ := (
                count := count(Issue.status.id),
                te := array_agg(DISTINCT Issue.time_estimate > 0),
            ) ORDER BY
                _.te DESC;
        ''', [
            [{'count': 3, 'te': []}, {'count': 1, 'te': [True]}],
            [{'count': 1, 'te': [True]}, {'count': 3, 'te': []}],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_result_alias_02(self):
        await self.assert_query_result(r'''
            # re-use the same "_" alias in nested scope
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            UNION OF _ := (
                count := count(Issue.status.id),
                te := array_agg(_ > 0),
            ) ORDER BY
                _.te DESC;
        ''', [
            [{'count': 1, 'te': [True]}, {'count': 3, 'te': []}],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_nested_01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                R := (
                    name := User.name,
                    issues := array_agg(
                        (
                            FOR UserIssue, B IN (
                                GROUP User.<owner[IS Issue]
                                BY User.<owner[IS Issue].status.name
                            )
                            UNION OF (
                                status := B,
                                count := count(UserIssue),
                            )
                            ORDER BY
                                B
                        )
                    )
                )
            ORDER BY R.name;
            """, [[
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
        ]])

    @unittest.expectedFailure
    async def test_edgeql_group_returning_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            # The issues should be partitioned into 2 sub-sets by
            # Issue.time_estimate (with values {} and 3000). Therefore
            # we expect 2 results combined via UNION ALL.
            UNION OF 42;
        ''', [
            [42, 42],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_returning_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, B IN (
                GROUP Issue
                BY Issue.time_estimate
            )
            # No reason to restrict the above example to doing a
            # UNION ALL of singletons.
            UNION OF _ := {42, count(Issue)}
            ORDER BY _;
        ''', [
            [1, 3, 42, 42],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_returning_03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, B IN (
                GROUP Issue
                BY Issue.status
            )
            # The result should be a set of status objects,
            # augmented with Issue.numbers corresponding to the
            # status.
            UNION OF B {
                name,
                nums := Issue.number
            }
            ORDER BY B.name;
        ''', [
            [{
                'name': 'Closed',
                'nums': {'3', '4'},
            }, {
                'name': 'Open',
                'nums': {'1', '2'},
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_returning_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR Issue, _ IN (
                GROUP Issue
                BY Issue.status
            )
            # Identical to the previous example, but expressed
            # slightly differently.
            UNION OF (
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
        ''', [
            [{
                'name': 'Closed',
                'nums': {'3', '4'},
            }, {
                'name': 'Open',
                'nums': {'1', '2'},
            }],
        ])

    async def test_edgeql_group_returning_05(self):
        await self.assert_query_result(r'''
            # a trivial group that is actually not doing anything
            # different from a plain SELECT
            WITH MODULE cards
            FOR Card, _ IN (
                GROUP Card
                BY Card.element
            )
            UNION OF Card.name
            ORDER BY
                Card.name;
        ''', [
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
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_returning_06(self):
        await self.assert_query_result(r'''
            # a trivial group that is actually not doing anything
            # different from a plain SELECT
            WITH MODULE cards
            FOR Card, _ IN (
                GROUP Card
                BY Card.element
            )
            UNION OF Card {name}
            ORDER BY
                Card.name;
        ''', [
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
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_returning_07(self):
        await self.assert_query_result(r'''
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
                    FOR Card, _ IN (
                        GROUP Card
                        BY Card.element
                    )
                    UNION OF array_agg(Card)
                )
            SELECT _ := (
                FOR X IN ELEMENTAL
                # for each "elemental" array select a "leader"
                UNION OF (
                    # the selection of leader is nested to
                    # disambiguate the application of ORDER BY and
                    # LIMIT clauses
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
                                FILTER array_contains(X, C2)
                                ORDER BY C2.cost
                            )
                        }
                    # the leader is selected from among the elemental array
                    FILTER array_contains(X, Card)
                    # the leader is defined as the one with the highest cost
                    ORDER BY Card.cost DESC
                    LIMIT 1
                )
            )
            ORDER BY _.element;
        ''', [
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
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_returning_08(self):
        await self.assert_query_result(r'''
            # Nominate a leader in each group from among the group.
            #
            # Same as previous test, but with all of the shape spec
            # factored out tot he outermost SELECT.
            WITH
                MODULE cards,
                C2 := Card,
                ELEMENTAL := (
                    FOR Card, _ IN (
                        GROUP Card
                        BY Card.element
                    )
                    UNION OF array_agg(Card)
                )
            SELECT _ := (
                FOR X IN ELEMENTAL
                SELECT (
                    SELECT Card {
                            element,
                            name,
                            cost,
                            members := (
                                SELECT C2
                                FILTER array_contains(X, C2)
                                ORDER BY C2.cost
                            )
                        }
                    FILTER array_contains(X, Card)
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
                } # XXX: is the order actually preserved here according
                  #      to our semantics?
            }
            ORDER BY _.element;
        ''', [
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
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_returning_09(self):
        await self.assert_query_result(r'''
            # Nominate a leader in each group from among the group.
            #
            # Same as previous tests, but refactored to take full
            # advantage of GROUP semantics and BY aliasing.
            WITH
                MODULE cards,
                C2 := Card
            FOR Card, Element IN (
                GROUP Card
                BY
                    # partition cards by element
                    Card.element
            )
            UNION OF (
                # for every partition, compute the "leader"
                SELECT C2 {
                    element,
                    name,
                    cost,
                    members := (
                        # all members of the particular elemental
                        # partition
                        SELECT Card{name, cost}
                        ORDER BY C2.cost
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
        ''', [
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
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_tuple_01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, B IN (
                GROUP Issue
                # This tuple will be {} for Issues lacking
                # time_estimate. So effectively we're expecting only 2
                # subsets, grouped by:
                # - {}
                # - ('Open', 3000)
                UNION OF BY (Issue.status.name, Issue.time_estimate)
            )
            _ := (
                sum := sum(<int>Issue.number),
                # don't forget to coalesce the {} or else the whole
                # tuple will collapse
                status := B.0 ?? '',
                time_estimate := B.1 ?? 0
            ) ORDER BY B;
        """, [
            [{
                'status': '', 'sum': 9, 'time_estimate': 0
            }, {
                'status': 'Open', 'sum': 1, 'time_estimate': 3000
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_multiple_01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, Stat, Est IN (
                GROUP Issue
                # Unlike the tuple example, these grouping sets
                # generate more combinations:
                # - 'Closed', {}
                # - 'Open', {}
                # - 'Open', 3000
                BY Issue.status.name, Issue.time_estimate
            )
            UNION OF _ := (
                sum := sum(<int>Issue.number),
                # Stat is never {}, so coalescing is not needed
                status := Stat,
                # only this one needs to be coalesced
                time_estimate := Est ?? 0
            ) ORDER BY _;
        """, [
            [{
                'status': 'Closed', 'sum': 7, 'time_estimate': 0,
            }, {
                'status': 'Open', 'sum': 2, 'time_estimate': 0,
            }, {
                'status': 'Open', 'sum': 1, 'time_estimate': 3000,
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_multiple_02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, Stat, Est IN (
                GROUP Issue
                BY Issue.status.name, Issue.time_estimate
            )
            UNION OF (
                sum := sum(<int>Issue.number),
                status := Stat,
                time_estimate := Est ?? 0
            )
            # ordering condition derived from the grouping parameters
            ORDER BY Stat
                THEN Est > 0 EMPTY FIRST;
        """, [
            [{
                'status': 'Closed', 'sum': 7, 'time_estimate': 0
            }, {
                'status': 'Open', 'sum': 2, 'time_estimate': 0
            }, {
                'status': 'Open', 'sum': 1, 'time_estimate': 3000
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_multiple_03(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, Stat, Est IN (
                GROUP Issue
                BY Issue.status.name, Issue.time_estimate
            )
            UNION OF (
                # array_agg with ordering instead of sum
                numbers := array_agg(
                    <int>Issue.number ORDER BY Issue.number),
                status := Stat,
                time_estimate := Est ?? 0
            ) ORDER BY Stat THEN Est;
        """, [
            [{
                'status': 'Closed',
                'time_estimate': 0,
                'numbers': [3, 4],
            }, {
                'status': 'Open',
                'time_estimate': 0,
                'numbers': [2],
            }, {
                'status': 'Open',
                'time_estimate': 3000,
                'numbers': [1],
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_by_multiple_04(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, Stat, Est IN (
                GROUP Issue
                BY Issue.status.name, Issue.time_estimate
            )
            UNION OF (
                # a couple of array_agg
                numbers := array_agg(
                    <int>Issue.number ORDER BY Issue.number),
                watchers := array_agg(
                    <str>Issue.watchers.name ORDER BY Issue.watchers.name),
                status := Stat,
                time_estimate := Est ?? 0
            ) ORDER BY Stat THEN Est;
        """, [
            [{
                'status': 'Closed',
                'time_estimate': 0,
                'numbers': [3, 4],
                'watchers': ['Elvis'],
            }, {
                'status': 'Open',
                'time_estimate': 0,
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
    async def test_edgeql_group_by_multiple_05(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, Stat, X IN (
                GROUP
                    # define a computable in the GROUP expr
                    Issue {
                        less_than_four := <int>Issue.number < 4
                    }
                BY
                    Issue.status.name,
                    # group by computed link
                    Issue.less_than_four
            )
            UNION OF (
                numbers := array_agg(
                    Issue.number ORDER BY Issue.number),
                # watchers will sometimes be empty resulting in []
                watchers := array_agg(
                    Issue.watchers.name ORDER BY Issue.watchers.name),
                status := Stat,
            ) ORDER BY Stat THEN X;
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
    async def test_edgeql_group_by_multiple_06(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            FOR Issue, Stat, X IN (
                GROUP
                    Issue
                BY
                    Issue.status.name,
                    # group by non-atomic expression
                    <int>Issue.number < 4
            )
            UNION OF I := (
                numbers := array_agg(
                    <int>Issue.number ORDER BY Issue.number),
                watchers := count(DISTINCT Issue.watchers),
                status := Stat,
            ) ORDER BY
                # used a mixture of different aliases in ORDER BY
                Stat
                THEN I.watchers
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
    async def test_edgeql_group_by_multiple_07(self):
        await self.assert_query_result(r"""
            WITH MODULE cards
            FOR C, x IN (
                GROUP Card
                BY Card.cost
            )
            UNION OF (
                array_agg(C.name ORDER BY C.name),
                # At this point C is a subset of Card. So the below
                # expression should be the size of the subset in
                # percent.
                100 * count(C) / count(Card)
            ) ORDER BY x;
        """, [
            [['Dwarf', 'Imp', 'Sprite'], 33],
            [['Bog monster', 'Giant eagle'], 22],
            [['Giant turtle', 'Golem'], 22],
            [['Djinn'], 11],
            [['Dragon'], 11]
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_linkproperty_simple_01(self):
        await self.assert_query_result(r"""
            # group by link property
            WITH MODULE cards
            FOR Card, B IN (
                GROUP
                    Card
                BY
                    Card.<deck@count
            )
            UNION OF _ := (
                cards := array_agg(
                    DISTINCT Card.name ORDER BY Card.name),
                count := B,
            ) ORDER BY _.count;
        """, [
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
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_linkproperty_simple_02(self):
        await self.assert_query_result(r"""
            # use link property inside a group aggregate
            WITH MODULE cards
            FOR Card, El IN (
                GROUP
                    Card
                BY
                    Card.element
            )
            UNION OF _ := (
                cards := array_agg(
                    DISTINCT Card.name ORDER BY Card.name),
                element := El,
                count := sum(Card.<deck@count),
            ) ORDER BY _.count;
        """, [

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

        ])

    @unittest.expectedFailure
    async def test_edgeql_group_linkproperty_simple_03(self):
        await self.assert_query_result(r"""
            # group by link property
            WITH MODULE cards
            FOR F, B IN (
                GROUP
                    (SELECT User FILTER User.name = 'Alice').friends
                BY
                    User.friends@nickname
            )
            UNION OF _ := (
                nickname := B,
                # using array agg here because it cannot be proven
                # that when grouped by nickname, friends are unique
                name := array_agg(DISTINCT User.friends.name)
            ) ORDER BY _.nickname;
        """, [
            [
                {'name': ['Carol'], 'nickname': 'Firefighter'},
                {'name': ['Dave'], 'nickname': 'Grumpy'},
                {'name': ['Bob'], 'nickname': 'Swampy'},
            ]
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_simple_04(self):
        await self.assert_query_result(r"""
            # NOTE: should be the same as above because we happen to
            # have unique nicknames for friends
            WITH MODULE cards
            FOR F, B IN (
                GROUP
                    (SELECT User FILTER User.name = 'Alice').friends
                BY
                    User.friends@nickname
            )
            UNION OF _ := (
                nickname := User.friends@nickname,
                # using array agg here because it cannot be proven
                # that when grouped by nickname, friends are unique
                name := array_agg(User.friends.name)
            ) ORDER BY _.nickname;
        """, [
            [
                {'name': ['Carol'], 'nickname': 'Firefighter'},
                {'name': ['Dave'], 'nickname': 'Grumpy'},
                {'name': ['Bob'], 'nickname': 'Swampy'},
            ]
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_nested_01(self):
        await self.assert_query_result(r"""
            WITH MODULE cards
            SELECT User {
                name,
                # total card count across the deck
                total := sum(User.deck@count),
                # group each deck by elements, adding up the counts
                elements := (
                    FOR D, B IN (
                        GROUP User.deck
                        BY User.deck.element
                    )
                    UNION OF _ := (
                        name := User.deck.element,
                        count := sum(User.deck@count),
                    )
                    ORDER BY _.name
                )
            } ORDER BY User.name;
        """, [
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
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_nested_02(self):
        await self.assert_query_result(r"""
            # similar to nested01, but with the root grouped by @nickname
            WITH MODULE cards
            FOR F, _0 IN (
                GROUP
                    (SELECT User FILTER User.name = 'Alice').friends
                BY
                    User.friends@nickname
            )
            UNION OF _af := (
                name := array_agg(DISTINCT User.friends.name),
                nickname := User.friends@nickname,

                # total card count across the deck
                total := sum(User.friends.deck@count),
                # group each deck by elements, adding up the counts
                elements := array_agg( (
                    FOR D, _1 IN (
                        GROUP User.friends.deck
                        BY User.friends.deck.element
                    )
                    UNION OF _ := (
                        name := User.friends.deck.element,
                        count := sum(User.friends.deck@count),
                    )
                    ORDER BY _.name
                ))
            ) ORDER BY _af.name;
        """, [
            [
                {
                    'name': ['Bob'],
                    'nickname': 'Swampy',
                    'total': 12,
                    'elements': [
                        {'name': 'Earth', 'count': 6},
                        {'name': 'Water', 'count': 6}
                    ]
                },
                {
                    'name': ['Carol'],
                    'nickname': 'Firefighter',
                    'total': 19,
                    'elements': [
                        {'name': 'Air', 'count': 8},
                        {'name': 'Earth', 'count': 6},
                        {'name': 'Water', 'count': 5}
                    ]
                },
                {
                    'name': ['Dave'],
                    'nickname': 'Grumpy',
                    'total': 10,
                    'elements': [
                        {'name': 'Air', 'count': 6},
                        {'name': 'Earth', 'count': 1},
                        {'name': 'Fire', 'count': 1},
                        {'name': 'Water', 'count': 2}
                    ]
                }
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_group_linkproperty_multiple_01(self):
        await self.assert_query_result(r"""
            WITH MODULE cards
            FOR D, El, Count IN (
                GROUP
                    (SELECT User FILTER User.name = 'Dave').deck
                BY
                    User.deck.element, User.deck@count
            )
            UNION OF _ := (
                cards := array_agg(
                    User.deck.name ORDER BY User.deck.name),
                element := El,
                count := Count,
            ) ORDER BY El THEN Count;
        """, [
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
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_atom_01(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                I := <int>Issue.number
            FOR I, _ IN (
                GROUP I
                BY I % 2 = 0
            )
            UNION OF _r := (
                values := array_agg(I ORDER BY I)
            ) ORDER BY _r.values;
        """, [
            [
                {'values': [1, 3]},
                {'values': [2, 4]}
            ]
        ])
