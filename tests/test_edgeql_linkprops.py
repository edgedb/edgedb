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
