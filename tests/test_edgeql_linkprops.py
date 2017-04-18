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
                          'linkprops.eschema')

    SETUP = r"""
        # create some cards
        WITH MODULE test
        INSERT Card {
            name := 'Imp',
            element := 'Fire',
            cost := 1
        };

        WITH MODULE test
        INSERT Card {
            name := 'Dragon',
            element := 'Fire',
            cost := 5
        };

        WITH MODULE test
        INSERT Card {
            name := 'Bog monster',
            element := 'Water',
            cost := 2
        };

        WITH MODULE test
        INSERT Card {
            name := 'Giant turtle',
            element := 'Water',
            cost := 3
        };

        WITH MODULE test
        INSERT Card {
            name := 'Dwarf',
            element := 'Earth',
            cost := 1
        };

        WITH MODULE test
        INSERT Card {
            name := 'Golem',
            element := 'Earth',
            cost := 3
        };

        WITH MODULE test
        INSERT Card {
            name := 'Sprite',
            element := 'Air',
            cost := 1
        };

        WITH MODULE test
        INSERT Card {
            name := 'Giant eagle',
            element := 'Air',
            cost := 2
        };

        WITH MODULE test
        INSERT Card {
            name := 'Djinn',
            element := 'Air',
            cost := 4
        };

        # create players & decks
        WITH MODULE test
        INSERT User {
            name := 'Alice',
            deck := (
                SELECT Card {@count := len(Card.element) - 2}
                FILTER .element IN ['Fire', 'Water']
            )
        };

        WITH MODULE test
        INSERT User {
            name := 'Bob',
            deck := (
                SELECT Card {@count := 3} FILTER .element IN ['Earth', 'Water']
            )
        };

        WITH MODULE test
        INSERT User {
            name := 'Carol',
            deck := (
                SELECT Card {@count := 5 - Card.cost} FILTER .element != 'Fire'
            )
        };

        WITH MODULE test
        INSERT User {
            name := 'Dave',
            deck := (
                SELECT Card {@count := 4 IF Card.cost = 1 ELSE 1}
                FILTER .element = 'Air' OR .cost != 1
            )
        };

        # update friends list
        WITH
            MODULE test,
            U2 := User
        UPDATE User
        FILTER User.name = 'Alice'
        SET {
            friends := (
                SELECT U2 {
                    @nickname :=
                        'Swampy'        IF U2.name = 'Bob' ELSE
                        'Firefighter'   IF U2.name = 'Carol' ELSE
                        'Grumpy'
                } FILTER U2.name IN ['Bob', 'Carol', 'Dave']
            )
        };

        WITH
            MODULE test,
            U2 := User
        UPDATE User
        FILTER User.name = 'Dave'
        SET {
            friends := (
                SELECT U2 FILTER U2.name = 'Bob'
            )
        };
    """

    async def test_edgeql_props_basic01(self):
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

    async def test_edgeql_props_basic02(self):
        await self.assert_query_result(r'''
            # get users and only cards that have the same count and
            # cost in the decks
            #
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

    async def test_edgeql_props_basic03(self):
        await self.assert_query_result(r'''
            # get only users who have the same count and cost in the decks
            #
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

    async def test_edgeql_props_basic04(self):
        await self.assert_query_result(r'''
            # get all cards that match their cost to the count in at
            # least some deck
            #
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

    async def test_edgeql_props_basic05(self):
        await self.assert_query_result(r'''
            # get all the friends of Alice and their nicknames
            #
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

    async def test_edgeql_props_cross01(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            #
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

    @unittest.expectedFailure
    async def test_edgeql_props_cross02(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            #
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

    @unittest.expectedFailure
    async def test_edgeql_props_cross03(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            #
            WITH MODULE test
            SELECT Card {
                name,
                same := (
                    SELECT EXISTS User
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

    async def test_edgeql_props_cross04(self):
        await self.assert_query_result(r'''
            # get cards that have the same count in some deck as their cost
            #
            WITH MODULE test
            SELECT Card {
                name,
                same := (
                    SELECT SINGLETON _ := Card.cost = Card.<deck@count
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

    async def test_edgeql_props_implication01(self):
        await self.assert_query_result(r'''
            # count of 1 in at least some deck implies 'Fire'
            #
            WITH MODULE test
            SELECT Card {
                name,
                element,
                count := (SELECT _ := Card.<deck@count ORDER BY _),
                expr := (
                    SELECT _ := NOT Card.<deck@count = 1 OR
                                Card.element = 'Fire'
                    ORDER BY _ DESC LIMIT 1
                )
            }
            ORDER BY .name;
        ''', [
            [
                {
                    'expr': [True],
                    'name': 'Bog monster',
                    'count': [1, 3],
                    'element': 'Water',
                },
                {
                    'expr': [False],
                    'name': 'Djinn',
                    'count': [1],
                    'element': 'Air',
                },
                {
                    'expr': [True],
                    'name': 'Dragon',
                    'count': [1, 2],
                    'element': 'Fire',
                },
                {
                    'expr': [True],
                    'name': 'Dwarf',
                    'count': [3, 4],
                    'element': 'Earth',
                },
                {
                    'expr': [True],
                    'name': 'Giant eagle',
                    'count': [1, 3],
                    'element': 'Air',
                },
                {
                    'expr': [True],
                    'name': 'Giant turtle',
                    'count': [1, 2, 3],
                    'element': 'Water',
                },
                {
                    'expr': [True],
                    'name': 'Golem',
                    'count': [1, 2, 3],
                    'element': 'Earth',
                },
                {
                    'expr': [True],
                    'name': 'Imp',
                    'count': [2],
                    'element': 'Fire',
                },
                {
                    'expr': [True],
                    'name': 'Sprite',
                    'count': [4],
                    'element': 'Air',
                },
            ]
        ])

    async def test_edgeql_props_implication02(self):
        await self.assert_query_result(r'''
            # FILTER by NOT (count of 1 implies 'Fire') in at least some deck
            #
            WITH MODULE test
            SELECT Card {
                name,
            }
            FILTER NOT (NOT .<deck@count = 1 OR .element = 'Fire')
            ORDER BY .name;
        ''', [
            [
                # all of these have count of 1 in some deck and are not 'Fire'
                #
                {'name': 'Bog monster'},
                {'name': 'Djinn'},
                {'name': 'Giant eagle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        ])

    async def test_edgeql_props_implication03(self):
        await self.assert_query_result(r'''
            # same as above, refactored
            #
            WITH MODULE test
            SELECT Card {
                name,
            }
            FILTER .<deck@count = 1 AND .element != 'Fire'
            ORDER BY .name;
        ''', [
            [
                # all of these have count of 1 and are not 'Fire' in some deck
                #
                {'name': 'Bog monster'},
                {'name': 'Djinn'},
                {'name': 'Giant eagle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        ])

    async def test_edgeql_props_implication04(self):
        await self.assert_query_result(r'''
            # count of 1 implies 'Fire' in the deck of Dave
            #
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
