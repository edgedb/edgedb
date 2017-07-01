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
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SCHEMA_CARDS = os.path.join(os.path.dirname(__file__), 'schemas',
                                'cards.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'groups_setup.eql')

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_expr_01(self):
        await self.assert_query_result(r'''
            GROUP
                x := {(1, 2), (3, 4), (4, 2)}
            BY
                x.1
            SELECT
                (x.1, array_agg(ALL x.0))
            ORDER BY
                x.1;
        ''', [
            [[2, [1, 4]], [4, [3]]],
        ])

    @tb.expected_optimizer_failure
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

            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.time_estimate
            SELECT _ := (
                count := count(ALL Issue.status.id),
                te := Issue.time_estimate > 0
            ) ORDER BY
                _.te EMPTY LAST;
        ''', [
            [{'count': 3, 'te': None}, {'count': 1, 'te': True}],
            [{'count': 1, 'te': True}, {'count': 3, 'te': None}],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_nested01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            SELECT
                R := (
                    name := User.name,
                    issues := array_agg(ALL (
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
                    ))
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

    async def test_edgeql_group_agg01(self):
        await self.assert_query_result(r"""
            SELECT
                schema::Concept {
                    l := array_agg(
                        ALL
                        schema::Concept.links.name
                        FILTER
                            schema::Concept.links.name IN [
                                'std::id',
                                'schema::name'
                            ]
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    async def test_edgeql_group_agg05(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                x := (
                    # User is simply employed as an object to be augmented
                    #
                    SELECT User {
                        count := 4,
                        all_issues := Issue
                    } FILTER .name = 'Elvis'
                )
            SELECT x.count = count(ALL x.all_issues);
        """, [
            [True]
        ])

    async def test_edgeql_group_agg06(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                x := (
                    # User is simply employed as an object to be augmented
                    #
                    SELECT User {
                        count := count(ALL Issue),
                        all_issues := Issue
                    } FILTER .name = 'Elvis'
                )
            SELECT x.count = count(ALL x.all_issues);
        """, [
            [True]
        ])

    async def test_edgeql_group_agg07(self):
        await self.assert_query_result(r"""
            WITH
                MODULE test,
                x := (
                    # User is simply employed as an object to be augmented
                    #
                    SELECT User {
                        count := count(ALL <int>Issue.number),
                        all_issues := <int>Issue.number
                    } FILTER .name = 'Elvis'
                )
            SELECT x.count = count(ALL x.all_issues);
        """, [
            [True]
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_by_tuple06(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue {
                    less_than_four := <int>Issue.number < 4
                }
            BY
                Issue.status.name,
                # group by computed link
                Issue.less_than_four
            SELECT (
                numbers := array_agg(
                    ALL Issue.number ORDER BY Issue.number),
                # watchers will sometimes be empty resulting in []
                watchers := array_agg(
                    ALL Issue.watchers.name ORDER BY Issue.watchers.name),
                status := Issue.status.name,
            ) ORDER BY
                Issue.status.name
                THEN Issue.less_than_four;
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_by_tuple07(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                # group by non-atomic expression
                <int>Issue.number < 4
            SELECT _ := (
                numbers := array_agg(
                    ALL <int>Issue.number ORDER BY Issue.number),
                watchers := count(DISTINCT Issue.watchers),
                status := Issue.status.name,
            ) ORDER BY
                Issue.status.name
                THEN _.watchers
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_by_tuple08(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                Issue
            BY
                Issue.status.name,
                Issue.owner
            SELECT (
                numbers := array_agg(
                    ALL <int>Issue.number ORDER BY Issue.number),
                status := Issue.status.name,
            ) ORDER BY Issue.status.name
                # should work because owner.name is *-1
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_simple01(self):
        await self.assert_query_result(r"""
            # group by link property
            #
            WITH MODULE cards
            GROUP
                Card
            BY
                Card.<deck@count
            SELECT _ := (
                cards := array_agg(
                    DISTINCT Card.name ORDER BY Card.name),
                count := Card.<deck@count,
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_simple02(self):
        await self.assert_query_result(r"""
            # use link property inside a group aggregate
            #
            WITH MODULE cards
            GROUP
                Card
            BY
                Card.element
            SELECT _ := (
                cards := array_agg(
                    DISTINCT Card.name ORDER BY Card.name),
                element := Card.element,
                count := sum(ALL Card.<deck@count),
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_simple03(self):
        await self.assert_query_result(r"""
            # group by link property
            #
            WITH MODULE cards
            GROUP
                (SELECT User FILTER User.name = 'Alice').friends
            BY
                User.friends@nickname
            SELECT _ := (
                nickname := User.friends@nickname,
                # using array agg here because it cannot be proven
                # that when grouped by nickname, friends are unique
                #
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
    async def test_edgeql_group_linkproperty_simple04(self):
        await self.assert_query_result(r"""
            # NOTE: should be the same as above because we happen to
            # have unique nicknames for friends
            #
            WITH MODULE cards
            GROUP
                (SELECT User FILTER User.name = 'Alice').friends
            BY
                User.friends@nickname
            SELECT _ := (
                nickname := User.friends@nickname,
                # using array agg here because it cannot be proven
                # that when grouped by nickname, friends are unique
                #
                name := array_agg(ALL User.friends.name)
            ) ORDER BY _.nickname;
        """, [
            [
                {'name': ['Carol'], 'nickname': 'Firefighter'},
                {'name': ['Dave'], 'nickname': 'Grumpy'},
                {'name': ['Bob'], 'nickname': 'Swampy'},
            ]
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_nested01(self):
        await self.assert_query_result(r"""
            WITH MODULE cards
            SELECT User {
                name,
                # total card count across the deck
                #
                total := sum(ALL User.deck@count),
                # group each deck by elements, adding up the counts
                #
                elements := (
                    GROUP User.deck
                    BY User.deck.element
                    SELECT _ := (
                        name := User.deck.element,
                        count := sum(ALL User.deck@count),
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
    async def test_edgeql_group_linkproperty_nested02(self):
        await self.assert_query_result(r"""
            # similar to nested01, but with the root grouped by @nickname
            #
            WITH MODULE cards
            GROUP
                (SELECT User FILTER User.name = 'Alice').friends
            BY
                User.friends@nickname
            SELECT _af := (
                name := array_agg(DISTINCT User.friends.name),
                nickname := User.friends@nickname,

                # total card count across the deck
                #
                total := sum(ALL User.friends.deck@count),
                # group each deck by elements, adding up the counts
                #
                elements := array_agg(ALL (
                    GROUP User.friends.deck
                    BY User.friends.deck.element
                    SELECT _ := (
                        name := User.friends.deck.element,
                        count := sum(ALL User.friends.deck@count),
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

    @tb.expected_optimizer_failure
    async def test_edgeql_group_linkproperty_tuple01(self):
        await self.assert_query_result(r"""
            WITH MODULE cards
            GROUP
                (SELECT User FILTER User.name = 'Dave').deck
            BY
                User.deck.element, User.deck@count
            SELECT _ := (
                cards := array_agg(
                    ALL User.deck.name ORDER BY User.deck.name),
                element := User.deck.element,
                count := User.deck@count,
            ) ORDER BY _.element ASC THEN _.count ASC;
        """, [
            [
                # compare to test_edgeql_props_basic01
                #
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
            WITH MODULE test
            GROUP
                I := {1, 2, 3, 4}
            BY
                I % 2 = 0
            SELECT _ := (
                values := array_agg(ALL I ORDER BY I)
            ) ORDER BY _.values;
        """, [
            [
                {'values': [1, 3]},
                {'values': [2, 4]}
            ]
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_group_atom_02(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            GROUP
                I := <int>Issue.number
            BY
                I % 2 = 0
            SELECT _ := (
                values := array_agg(ALL I ORDER BY I)
            ) ORDER BY _.values;
        """, [
            [
                {'values': [1, 3]},
                {'values': [2, 4]}
            ]
        ])
