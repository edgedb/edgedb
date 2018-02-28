##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest  # NOQA

from edgedb.server import _testbase as tb


class TestEdgeQLLinkproperties(tb.QueryTestCase):
    '''The scope is to test link properties.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.eql')

    async def test_edgeql_props_basic_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {
                name,
                deck: {
                    name,
                    element,
                    cost,
                    @count
                } ORDER BY @count DESC THEN .name ASC
            } ORDER BY .name;
        ''', [
            [
                {
                    'name': 'Alice',
                    'deck': [
                        {
                            'cost': 2,
                            'name': 'Bog monster',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 5,
                            'name': 'Dragon',
                            '@count': 2,
                            'element': 'Fire'
                        },
                        {
                            'cost': 1,
                            'name': 'Imp',
                            '@count': 2,
                            'element': 'Fire'
                        },
                    ],
                },
                {
                    'name': 'Bob',
                    'deck': [
                        {
                            'cost': 2,
                            'name': 'Bog monster',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 1,
                            'name': 'Dwarf',
                            '@count': 3,
                            'element': 'Earth'
                        },
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 3,
                            'name': 'Golem',
                            '@count': 3,
                            'element': 'Earth'
                        },
                    ],
                },
                {
                    'name': 'Carol',
                    'deck': [
                        {
                            'cost': 1,
                            'name': 'Dwarf',
                            '@count': 4,
                            'element': 'Earth'
                        },
                        {
                            'cost': 1,
                            'name': 'Sprite',
                            '@count': 4,
                            'element': 'Air'
                        },
                        {
                            'cost': 2,
                            'name': 'Bog monster',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 2,
                            'name': 'Giant eagle',
                            '@count': 3,
                            'element': 'Air'
                        },
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 2,
                            'element': 'Water'
                        },
                        {
                            'cost': 3,
                            'name': 'Golem',
                            '@count': 2,
                            'element': 'Earth'
                        },
                        {
                            'cost': 4,
                            'name': 'Djinn',
                            '@count': 1,
                            'element': 'Air'
                        },
                    ],
                },
                {
                    'name': 'Dave',
                    'deck': [
                        {
                            'cost': 1,
                            'name': 'Sprite',
                            '@count': 4,
                            'element': 'Air'
                        },
                        {
                            'cost': 2,
                            'name': 'Bog monster',
                            '@count': 1,
                            'element': 'Water'
                        },
                        {
                            'cost': 4,
                            'name': 'Djinn',
                            '@count': 1,
                            'element': 'Air'
                        },
                        {
                            'cost': 5,
                            'name': 'Dragon',
                            '@count': 1,
                            'element': 'Fire'
                        },
                        {
                            'cost': 2,
                            'name': 'Giant eagle',
                            '@count': 1,
                            'element': 'Air'
                        },
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 1,
                            'element': 'Water'
                        },
                        {
                            'cost': 3,
                            'name': 'Golem',
                            '@count': 1,
                            'element': 'Earth'
                        },
                    ],
                }
            ]
        ])

    async def test_edgeql_props_basic_02(self):
        await self.assert_query_result(r'''
            # get users and only cards that have the same count and
            # cost in the decks
            WITH MODULE test
            SELECT User {
                name,
                deck: {
                    name,
                    element,
                    cost,
                    @count
                } FILTER .cost = @count
                  ORDER BY @count DESC THEN .name ASC
            } ORDER BY .name;
        ''', [
            [
                {
                    'name': 'Alice',
                    'deck': [
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 3,
                            'element': 'Water'
                        },
                    ],
                },
                {
                    'name': 'Bob',
                    'deck': [
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 3,
                            'name': 'Golem',
                            '@count': 3,
                            'element': 'Earth'
                        },
                    ],
                },
                {
                    'name': 'Carol',
                    'deck': None,
                },
                {
                    'name': 'Dave',
                    'deck': None,
                }
            ]
        ])

    async def test_edgeql_props_basic_03(self):
        await self.assert_query_result(r'''
            # get only users who have the same count and cost in the decks
            WITH MODULE test
            SELECT User {
                name,
                deck: {
                    name,
                    element,
                    cost,
                    @count
                } ORDER BY @count DESC THEN .name ASC
            } FILTER .deck.cost = .deck@count
              ORDER BY .name;
        ''', [
            [
                {
                    'name': 'Alice',
                    'deck': [
                        {
                            'cost': 2,
                            'name': 'Bog monster',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 5,
                            'name': 'Dragon',
                            '@count': 2,
                            'element': 'Fire'
                        },
                        {
                            'cost': 1,
                            'name': 'Imp',
                            '@count': 2,
                            'element': 'Fire'
                        },
                    ],
                },
                {
                    'name': 'Bob',
                    'deck': [
                        {
                            'cost': 2,
                            'name': 'Bog monster',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 1,
                            'name': 'Dwarf',
                            '@count': 3,
                            'element': 'Earth'
                        },
                        {
                            'cost': 3,
                            'name': 'Giant turtle',
                            '@count': 3,
                            'element': 'Water'
                        },
                        {
                            'cost': 3,
                            'name': 'Golem',
                            '@count': 3,
                            'element': 'Earth'
                        },
                    ],
                },
            ]
        ])

    async def test_edgeql_props_basic_04(self):
        await self.assert_query_result(r'''
            # get all cards that match their cost to the count in at
            # least some deck
            WITH MODULE test
            SELECT Card {
                name,
                element,
                cost
            }
            FILTER
                .cost = .<deck@count
            ORDER BY .name;
        ''', [
            [
                {
                    'cost': 3,
                    'name': 'Giant turtle',
                    'element': 'Water'
                },
                {
                    'cost': 3,
                    'name': 'Golem',
                    'element': 'Earth'
                },
            ]
        ])

    async def test_edgeql_props_basic_05(self):
        await self.assert_query_result(r'''
            # get all the friends of Alice and their nicknames
            WITH MODULE test
            SELECT User {
                name,
                friends: {
                    name,
                    @nickname,
                } ORDER BY .name,
            }
            FILTER .name = 'Alice';
        ''', [
            [
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Bob', '@nickname': 'Swampy'},
                        {'name': 'Carol', '@nickname': 'Firefighter'},
                        {'name': 'Dave', '@nickname': 'Grumpy'},
                    ]
                }
            ]
        ])

    async def test_edgeql_props_cross_01(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            WITH MODULE test
            SELECT Card {
                name,
            }
            FILTER .cost = .<deck@count
            ORDER BY .name;
        ''', [
            [
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        ])

    async def test_edgeql_props_cross_02(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            WITH MODULE test
            SELECT Card {
                name,
                same := EXISTS (
                    SELECT User
                    FILTER
                        Card.cost = User.deck@count AND
                        Card = User.deck
                )
            }
            ORDER BY .name;
        ''', [
            [
                {'name': 'Bog monster', 'same': False},
                {'name': 'Djinn', 'same': False},
                {'name': 'Dragon', 'same': False},
                {'name': 'Dwarf', 'same': False},
                {'name': 'Giant eagle', 'same': False},
                {'name': 'Giant turtle', 'same': True},
                {'name': 'Golem', 'same': True},
                {'name': 'Imp', 'same': False},
                {'name': 'Sprite', 'same': False},
            ]
        ])

    async def test_edgeql_props_cross_03(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            WITH MODULE test
            SELECT Card {
                name,
                same := EXISTS (
                    SELECT
                        User
                    FILTER
                        Card.cost = User.deck@count AND
                        Card = User.deck
                )
            }
            ORDER BY .name;
        ''', [
            [
                {'name': 'Bog monster', 'same': False},
                {'name': 'Djinn', 'same': False},
                {'name': 'Dragon', 'same': False},
                {'name': 'Dwarf', 'same': False},
                {'name': 'Giant eagle', 'same': False},
                {'name': 'Giant turtle', 'same': True},
                {'name': 'Golem', 'same': True},
                {'name': 'Imp', 'same': False},
                {'name': 'Sprite', 'same': False},
            ]
        ])

    async def test_edgeql_props_cross_04(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            WITH MODULE test
            SELECT Card {
                name,
                same := (
                    WITH CARDINALITY '1'
                    SELECT _ := Card.cost = Card.<deck@count
                    ORDER BY _ DESC LIMIT 1
                )
            }
            ORDER BY .name;
        ''', [
            [
                {'name': 'Bog monster', 'same': False},
                {'name': 'Djinn', 'same': False},
                {'name': 'Dragon', 'same': False},
                {'name': 'Dwarf', 'same': False},
                {'name': 'Giant eagle', 'same': False},
                {'name': 'Giant turtle', 'same': True},
                {'name': 'Golem', 'same': True},
                {'name': 'Imp', 'same': False},
                {'name': 'Sprite', 'same': False},
            ]
        ])

    async def test_edgeql_props_implication_01(self):
        await self.assert_query_result(r'''
            # count of 1 in at least some deck implies 'Fire'
            WITH MODULE test
            SELECT Card {
                name,
                element,
                count := (SELECT _ := Card.<deck@count ORDER BY _),
                expr := (
                    SELECT _ := NOT EXISTS (SELECT Card
                                            FILTER Card.<deck@count = 1) OR
                                Card.element = 'Fire'
                    ORDER BY _ DESC LIMIT 1
                )
            }
            ORDER BY .name;
        ''', [
            [
                {
                    'expr': False,
                    'name': 'Bog monster',
                    'count': [1, 3, 3, 3],
                    'element': 'Water',
                },
                {
                    'expr': False,
                    'name': 'Djinn',
                    'count': [1, 1],
                    'element': 'Air',
                },
                {
                    'expr': True,
                    'name': 'Dragon',
                    'count': [1, 2],
                    'element': 'Fire',
                },
                {
                    'expr': True,
                    'name': 'Dwarf',
                    'count': [3, 4],
                    'element': 'Earth',
                },
                {
                    'expr': False,
                    'name': 'Giant eagle',
                    'count': [1, 3],
                    'element': 'Air',
                },
                {
                    'expr': False,
                    'name': 'Giant turtle',
                    'count': [1, 2, 3, 3],
                    'element': 'Water',
                },
                {
                    'expr': False,
                    'name': 'Golem',
                    'count': [1, 2, 3],
                    'element': 'Earth',
                },
                {
                    'expr': True,
                    'name': 'Imp',
                    'count': [2],
                    'element': 'Fire',
                },
                {
                    'expr': True,
                    'name': 'Sprite',
                    'count': [4, 4],
                    'element': 'Air',
                },
            ]
        ])

    async def test_edgeql_props_implication_02(self):
        await self.assert_query_result(r'''
            # FILTER by NOT (count of 1 implies 'Fire') in at least some deck
            WITH MODULE test
            SELECT Card {
                name,
            }
            FILTER NOT (NOT .<deck@count = 1 OR .element = 'Fire')
            ORDER BY .name;
        ''', [
            [
                # all of these have count of 1 in some deck and are not 'Fire'
                {'name': 'Bog monster'},
                {'name': 'Djinn'},
                {'name': 'Giant eagle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        ])

    async def test_edgeql_props_implication_03(self):
        await self.assert_query_result(r'''
            # same as above, refactored
            WITH MODULE test
            SELECT Card {
                name,
            }
            FILTER .<deck@count = 1 AND .element != 'Fire'
            ORDER BY .name;
        ''', [
            [
                # all of these have count of 1 and are not 'Fire' in some deck
                {'name': 'Bog monster'},
                {'name': 'Djinn'},
                {'name': 'Giant eagle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        ])

    async def test_edgeql_props_implication_04(self):
        await self.assert_query_result(r'''
            # count of 1 implies 'Fire' in the deck of Dave
            WITH MODULE test
            SELECT User {
                name,
                deck: {
                    name,
                    element,
                    @count,
                    expr :=
                        NOT User.deck@count = 1 OR User.deck.element = 'Fire'

                }
            }
            FILTER .name = 'Dave';
        ''', [
            [
                {
                    'name': 'Dave',
                    'deck': [
                        {
                            'name': 'Dragon',
                            'expr': True,
                            '@count': 1,
                            'element': 'Fire',
                        },
                        {
                            'name': 'Bog monster',
                            'expr': False,
                            '@count': 1,
                            'element': 'Water',
                        },
                        {
                            'name': 'Giant turtle',
                            'expr': False,
                            '@count': 1,
                            'element': 'Water',
                        },
                        {
                            'name': 'Golem',
                            'expr': False,
                            '@count': 1,
                            'element': 'Earth',
                        },
                        {
                            'name': 'Sprite',
                            'expr': True,
                            '@count': 4,
                            'element': 'Air',
                        },
                        {
                            'name': 'Giant eagle',
                            'expr': False,
                            '@count': 1,
                            'element': 'Air',
                        },
                        {
                            'name': 'Djinn',
                            'expr': False,
                            '@count': 1,
                            'element': 'Air',
                        },
                    ],
                }
            ]
        ])

    async def test_edgeql_props_setops_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT DISTINCT User.deck@count;

            WITH MODULE test
            SELECT DISTINCT (
                SELECT User.deck@count FILTER User.deck.element = 'Fire'
            );

            WITH MODULE test
            SELECT DISTINCT (
                SELECT User.deck@count FILTER User.deck.element = 'Water'
            );

            WITH MODULE test
            SELECT DISTINCT (
                SELECT (SELECT Card FILTER Card.element = 'Water').<deck@count
            );
        ''', [
            {1, 2, 3, 4},
            {1, 2},
            {1, 2, 3},
            {1, 2, 3},
        ])

    async def test_edgeql_props_setops_02(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                C := DETACHED (
                    SELECT User FILTER User.name = 'Carol').deck.name,
                D := DETACHED (
                    SELECT User FILTER User.name = 'Dave').deck.name
            SELECT _ := C UNION D
            ORDER BY _;

            WITH
                MODULE test,
                C := DETACHED (
                    SELECT User FILTER User.name = 'Carol').deck.name,
                D := DETACHED (
                    SELECT User FILTER User.name = 'Dave').deck.name
            SELECT _ := DISTINCT (C UNION D)
            ORDER BY _;
        ''', [
            [
                'Bog monster',
                'Bog monster',
                'Djinn',
                'Djinn',
                'Dragon',
                'Dwarf',
                'Giant eagle',
                'Giant eagle',
                'Giant turtle',
                'Giant turtle',
                'Golem',
                'Golem',
                'Sprite',
                'Sprite'
            ],
            [
                'Bog monster',
                'Djinn',
                'Dragon',
                'Dwarf',
                'Giant eagle',
                'Giant turtle',
                'Golem',
                'Sprite'
            ],
        ])

    async def test_edgeql_props_setops_03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT _ := {
                # this is equivalent to UNION
                User.name,
                User.friends@nickname,
                {'Foo', 'Bob'}
            }
            ORDER BY _;

            WITH MODULE test
            SELECT _ := DISTINCT {
                User.name,
                User.friends@nickname,
                {'Foo', 'Bob'}
            }
            ORDER BY _;
        ''', [
            [
                'Alice', 'Bob', 'Bob', 'Carol', 'Dave', 'Firefighter',
                'Foo', 'Grumpy', 'Swampy'
            ],
            [
                'Alice', 'Bob', 'Carol', 'Dave', 'Firefighter', 'Foo',
                'Grumpy', 'Swampy',
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_props_setops_04(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                x := DISTINCT (
                    (
                        SELECT User FILTER User.name = 'Alice'
                    ).deck@count
                )
                # `x` is the set of distinct values of card counts in
                # the deck of Alice, namely: {2, 3}
            SELECT _ := (x, User.name)
            # we only expect tuples for User.name 'Alice', because for
            # everyone else `x` will be {} due to the unsatisfied
            # FILTER
            ORDER BY _;
        ''', [
            [2, 'Alice'],
            [3, 'Alice'],
        ])

    @unittest.expectedFailure
    async def test_edgeql_props_setops_05(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                x := DISTINCT (
                    (
                        SELECT User FILTER User.name = 'Alice'
                    ).deck@count
                )
                # `x` is the set of distinct values of card counts in
                # the deck of Alice, namely: {2, 3}
            SELECT _ := (DETACHED x, User.name)
            # DETACHED x results in a cross product:
            # {2, 3} X {'Alice', 'Bob', 'Carol', 'Dave'}
            ORDER BY _;
        ''', [
            [2, 'Alice'],
            [3, 'Alice'],
            [2, 'Bob'],
            [3, 'Bob'],
            [2, 'Carol'],
            [3, 'Carol'],
            [2, 'Dave'],
            [3, 'Dave'],
        ])

    async def test_edgeql_props_agg_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT sum(User.deck@count);

            WITH MODULE test
            SELECT _ := (sum(User.deck@count), User.name)
            ORDER BY _;
        ''', [
            [51],
            [
                [10, 'Alice'], [10, 'Dave'], [12, 'Bob'], [19, 'Carol'],
            ],
        ])
