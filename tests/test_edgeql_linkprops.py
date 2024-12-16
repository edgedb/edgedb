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

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLLinkproperties(tb.QueryTestCase):
    '''The scope is to test link properties.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.edgeql')

    async def test_edgeql_props_basic_01(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    name,
                    deck: {
                        name,
                        element,
                        cost,
                        @count
                    } ORDER BY @count DESC THEN .name ASC
                } ORDER BY .name;
            ''',
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
        )

    async def test_edgeql_props_basic_02(self):
        await self.assert_query_result(
            r'''
                # get users and only cards that have the same count and
                # cost in the decks
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
            ''',
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
                    'deck': [],
                },
                {
                    'name': 'Dave',
                    'deck': [],
                }
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_props_basic_03(self):
        await self.assert_query_result(
            r'''
                # get only users who have the same count and cost in the decks
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
            ''',
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
        )

    async def test_edgeql_props_basic_04(self):
        await self.assert_query_result(
            r'''
                # get all cards that match their cost to the count in at
                # least some deck
                SELECT Card {
                    name,
                    element,
                    cost
                }
                FILTER
                    .cost IN .<deck[IS User]@count
                ORDER BY .name;
            ''',
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
        )

    async def test_edgeql_props_basic_05(self):
        await self.assert_query_result(
            r'''
                # get all the friends of Alice and their nicknames
                SELECT User {
                    name,
                    friends: {
                        name,
                        @nickname,
                    } ORDER BY .name,
                }
                FILTER .name = 'Alice';
            ''',
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
        )

    async def test_edgeql_props_basic_06(self):
        await self.assert_query_result(
            r'''
                SELECT User.avatar@text;
            ''',
            {
                'Best', 'Wow',
            }
        )

    async def test_edgeql_props_basic_07(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    avatar: {
                        @text
                    }
                } FILTER EXISTS .avatar@text
                ORDER BY .name;
            ''',
            [
                {'avatar': {'@text': 'Best'}},
                {'avatar': {'@text': 'Wow'}},
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_props_cross_01(self):
        await self.assert_query_result(
            r'''
                # get cards that have the same count in some deck as their cost
                SELECT Card {
                    name,
                }
                FILTER .cost = .<deck[IS User]@count
                ORDER BY .name;
            ''',
            [
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_props_cross_02(self):
        await self.assert_query_result(
            r'''
                # get cards that have the same count in some deck as their cost
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
            ''',
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
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_props_cross_03(self):
        await self.assert_query_result(
            r'''
                # get cards that have the same count in some deck as their cost
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
            ''',
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
        )

    async def test_edgeql_props_cross_04(self):
        await self.assert_query_result(
            r'''
                # get cards that have the same count in some deck as their cost
                SELECT Card {
                    name,
                    same := (
                        SELECT _ := Card.cost = Card.<deck[IS User]@count
                        ORDER BY _ DESC LIMIT 1
                    )
                }
                ORDER BY .name;
            ''',
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
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_props_implication_01(self):
        await self.assert_query_result(
            r'''
                # count of 1 in at least some deck implies 'Fire'
                SELECT Card {
                    name,
                    element,
                    count := (
                        SELECT _ := Card.<deck[IS User]@count ORDER BY _
                    ),
                    expr := (
                        SELECT
                            _ := NOT EXISTS (
                                SELECT Card
                                FILTER Card.<deck[IS User]@count = 1
                            ) OR Card.element = 'Fire'
                        ORDER BY _ DESC LIMIT 1
                    )
                }
                ORDER BY .name;
            ''',
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
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_props_implication_02(self):
        await self.assert_query_result(
            r'''
                # FILTER by NOT (count of 1 implies 'Fire')
                # in at least some deck
                SELECT Card {
                    name,
                }
                FILTER NOT (NOT .<deck[IS User]@count = 1 OR .element = 'Fire')
                ORDER BY .name;
            ''',
            [
                # all of these have count of 1 in some deck and are not 'Fire'
                {'name': 'Bog monster'},
                {'name': 'Djinn'},
                {'name': 'Giant eagle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_props_implication_03(self):
        await self.assert_query_result(
            r'''
                # same as above, refactored
                SELECT Card {
                    name,
                }
                FILTER .<deck[IS User]@count = 1 AND .element != 'Fire'
                ORDER BY .name;
            ''',
            [
                # all of these have count of 1 and are not 'Fire' in some deck
                {'name': 'Bog monster'},
                {'name': 'Djinn'},
                {'name': 'Giant eagle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
            ]
        )

    async def test_edgeql_props_implication_04(self):
        await self.assert_query_result(
            r'''
                # count of 1 implies 'Fire' in the deck of Dave
                SELECT User {
                    name,
                    deck: {
                        name,
                        element,
                        @count,
                        expr :=
                            NOT User.deck@count = 1 OR
                                User.deck.element = 'Fire'
                    }
                }
                FILTER .name = 'Dave';
            ''',
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
        )

    async def test_edgeql_props_setops_01(self):
        await self.assert_query_result(
            r'''
                SELECT DISTINCT User.deck@count;
            ''',
            {1, 2, 3, 4},
        )

        await self.assert_query_result(
            r'''
                SELECT User.deck@count FILTER User.deck.element = 'Fire'
            ''',
            tb.bag([1, 2, 2]),
        )

        await self.assert_query_result(
            r'''
                SELECT DISTINCT (
                    SELECT User.deck@count FILTER User.deck.element = 'Fire'
                );
            ''',
            {1, 2},
        )

        await self.assert_query_result(
            r'''
                SELECT DISTINCT (
                    SELECT User.deck@count FILTER User.deck.element = 'Water'
                );
            ''',
            {1, 2, 3},
        )

        await self.assert_query_result(
            r'''
                SELECT DISTINCT (
                    SELECT (
                        SELECT Card FILTER Card.element = 'Water'
                    ).<deck[IS User]@count
            );
            ''',
            {1, 2, 3},
        )

    async def test_edgeql_props_setops_02(self):
        await self.assert_query_result(
            r'''
                WITH
                    C := (
                        SELECT User FILTER User.name = 'Carol').deck.name,
                    D := (
                        SELECT User FILTER User.name = 'Dave').deck.name
                SELECT _ := C UNION D
                ORDER BY _;
            ''',
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
        )

        await self.assert_query_result(
            r'''
                WITH
                    C := (
                        SELECT User FILTER User.name = 'Carol').deck.name,
                    D := (
                        SELECT User FILTER User.name = 'Dave').deck.name
                SELECT _ := DISTINCT (C UNION D)
                ORDER BY _;
            ''',
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
        )

    async def test_edgeql_props_setops_03(self):
        await self.assert_query_result(
            r'''
                SELECT _ := {
                    # this is equivalent to UNION
                    User.name,
                    User.friends@nickname,
                    {'Foo', 'Bob'}
                }
                ORDER BY _;
            ''',
            [
                'Alice', 'Bob', 'Bob', 'Carol', 'Dave', 'Firefighter',
                'Foo', 'Grumpy', 'Swampy'
            ],
        )

        await self.assert_query_result(
            r'''
                SELECT _ := DISTINCT {
                    User.name,
                    User.friends@nickname,
                    {'Foo', 'Bob'}
                }
                ORDER BY _;
            ''',
            [
                'Alice', 'Bob', 'Carol', 'Dave', 'Firefighter', 'Foo',
                'Grumpy', 'Swampy',
            ],
        )

    async def test_edgeql_props_setops_04(self):
        await self.assert_query_result(
            r'''
                WITH
                    A := (SELECT User FILTER User.name = 'Alice')
                    # the set of distinct values of card counts in
                    # the deck of Alice is {2, 3}
                SELECT _ := (DISTINCT A.deck@count, A.name)
                ORDER BY _;
            ''',
            [
                [2, 'Alice'],
                [3, 'Alice'],
            ]
        )

    async def test_edgeql_props_setops_05(self):
        await self.assert_query_result(
            r'''
                SELECT DISTINCT
                        (
                            SELECT User FILTER User.name = 'Alice'
                        ).deck@count;
            ''',
            {2, 3},
        )

    async def test_edgeql_props_computable_01(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    name,
                    my_deck := (SELECT Card { @foo := Card.name }
                                FILTER .name = 'Djinn')
                }
                FILTER User.name = 'Alice';
            ''',
            [{
                'name': 'Alice',
                'my_deck': {
                    '@foo': 'Djinn'
                }
            }],
        )

    async def test_edgeql_props_computable_02(self):
        await self.assert_query_result(
            r'''
                WITH
                    MyUser := (
                        SELECT
                            User {
                                my_deck := (SELECT Card { @foo := Card.name }
                                            FILTER .name = 'Djinn')
                            }
                        FILTER User.name = 'Alice'
                    )
                SELECT MyUser {
                    name,
                    my_deck: {
                        @foo
                    }
                };
            ''',
            [{
                'name': 'Alice',
                'my_deck': {
                    '@foo': 'Djinn'
                }
            }],
        )

    async def test_edgeql_props_abbrev(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    name,
                    my_deck := (SELECT .deck {
                        name,
                        num_cards := @count
                    } ORDER BY .name)
                } FILTER .name = 'Alice';
            ''',
            [{
                'name': 'Alice',
                'my_deck': [
                    {"name": "Bog monster", "num_cards": 3},
                    {"name": "Dragon", "num_cards": 2},
                    {"name": "Giant turtle", "num_cards": 3},
                    {"name": "Imp", "num_cards": 2},
                ],
            }],
        )

    async def test_edgeql_props_agg_01(self):
        await self.assert_query_result(
            r'''
                SELECT sum(User.deck@count);
            ''',
            [51],
        )

        await self.assert_query_result(
            r'''
                SELECT _ := (sum(User.deck@count), User.name)
                ORDER BY _;
            ''',
            [
                [10, 'Alice'], [10, 'Dave'], [12, 'Bob'], [19, 'Carol'],
            ],
        )

    async def test_edgeql_props_link_shadow_01(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    name,
                    deck := (SELECT x := User.deck
                             ORDER BY x.name ASC
                             LIMIT 2) {
                                 name
                             }
                } ORDER BY .name;
            ''',
            [
                {
                    "name": "Alice",
                    "deck": [{"name": "Bog monster"}, {"name": "Dragon"}],
                },
                {
                    "name": "Bob",
                    "deck": [{"name": "Bog monster"}, {"name": "Dwarf"}],
                },
                {
                    "name": "Carol",
                    "deck": [{"name": "Bog monster"}, {"name": "Djinn"}],
                },
                {
                    "name": "Dave",
                    "deck": [{"name": "Bog monster"}, {"name": "Djinn"}],
                },
            ]
        )

    async def test_edgeql_props_link_shadow_02(self):
        await self.assert_query_result(
            r'''
                WITH
                    AliasedUser := User {
                        name,
                        deck := (SELECT User.deck ORDER BY .name LIMIT 2)
                    }
                SELECT
                    AliasedUser {
                        name,
                        deck: {
                            @count
                        }
                    }
                ORDER BY .name;
            ''',
            [
                {"deck": [{"@count": 3}, {"@count": 2}], "name": "Alice"},
                {"deck": [{"@count": 3}, {"@count": 3}], "name": "Bob"},
                {"deck": [{"@count": 3}, {"@count": 1}], "name": "Carol"},
                {"deck": [{"@count": 1}, {"@count": 1}], "name": "Dave"},
            ]
        )

    async def test_edgeql_props_link_computed_01(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    name,
                    deck: {name, @total_cost} ORDER BY .name
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    "name": "Alice",
                    "deck": [
                        {"@total_cost": 6, "name": "Bog monster"},
                        {"@total_cost": 10, "name": "Dragon"},
                        {"@total_cost": 9, "name": "Giant turtle"},
                        {"@total_cost": 2, "name": "Imp"}
                    ],
                }
            ],
        )

    async def test_edgeql_props_link_computed_02(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    name,
                    avatar: { @tag },
                }
                FILTER .name IN {'Alice', 'Bob'} ORDER BY .name;
            ''',
            [
                {"name": "Alice", "avatar": {"@tag": "Dragon-Best"}},
                {"name": "Bob", "avatar": None}
            ],
        )

    async def test_edgeql_props_link_union_01(self):
        await self.con.execute(r'''
            CREATE TYPE Tgt;
            CREATE TYPE Tgt2 EXTENDING Tgt;
            CREATE TYPE Bar {
                CREATE LINK l -> Tgt {
                    CREATE PROPERTY x -> str;
                };
            };
            CREATE TYPE Foo {
                CREATE LINK l -> Tgt {
                    CREATE PROPERTY x -> str;
                };
            };
            CREATE TYPE Baz {
                CREATE LINK fubar -> (Bar | Foo);
            };

            INSERT Baz {
                fubar := (INSERT Bar {
                    l := (INSERT Tgt2 { @x := "test" })
                })
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Baz.fubar.l@x;
            ''',
            ["test"],
        )

        await self.assert_query_result(
            r'''
                SELECT Baz.fubar.l[IS Tgt2]@x;
            ''',
            ["test"],
        )

        await self.assert_query_result(
            r'''
                SELECT (Foo UNION Bar).l@x;
            ''',
            ["test"],
        )

    async def test_edgeql_props_link_union_02(self):
        await self.con.execute(r'''
            CREATE TYPE Tgt;
            CREATE TYPE Tgt2 EXTENDING Tgt;
            CREATE TYPE Bar {
                CREATE MULTI LINK l -> Tgt {
                    CREATE PROPERTY x -> str;
                };
            };
            CREATE TYPE Foo {
                CREATE MULTI LINK l -> Tgt {
                    CREATE PROPERTY x -> str;
                };
            };
            CREATE TYPE Baz {
                CREATE LINK fubar -> (Bar | Foo);
            };

            INSERT Baz {
                fubar := (INSERT Bar {
                    l := (INSERT Tgt2 { @x := "test" })
                })
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Baz.fubar.l@x;
            ''',
            ["test"],
        )

        await self.assert_query_result(
            r'''
                SELECT Baz.fubar.l[IS Tgt2]@x;
            ''',
            ["test"],
        )

        await self.assert_query_result(
            r'''
                SELECT (Foo UNION Bar).l@x;
            ''',
            ["test"],
        )

    async def test_edgeql_props_link_union_03(self):
        await self.con.execute(r'''
            CREATE TYPE Tgt;
            CREATE TYPE Tgt2 EXTENDING Tgt;
            CREATE TYPE Bar {
                CREATE LINK l -> Tgt {
                    CREATE PROPERTY x -> str;
                };
            };
            CREATE TYPE Foo {
                CREATE MULTI LINK l -> Tgt {
                    CREATE PROPERTY x -> str;
                };
            };
            CREATE TYPE Baz {
                CREATE LINK fubar -> (Bar | Foo);
            };

            INSERT Baz {
                fubar := (INSERT Bar {
                    l := (INSERT Tgt2 { @x := "test" })
                })
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Baz.fubar.l@x;
            ''',
            ["test"],
        )

        await self.assert_query_result(
            r'''
                SELECT Baz.fubar.l[IS Tgt2]@x;
            ''',
            ["test"],
        )

        await self.assert_query_result(
            r'''
                SELECT (Foo UNION Bar).l@x;
            ''',
            ["test"],
        )

    async def test_edgeql_props_back_01(self):
        await self.assert_query_result(
            """
            with X1 := (Card { z := (.<deck[IS User], .<deck[IS User]@count)}),
                 X2 := X1 { owners2 := assert_distinct(
                     .z.0 { count := X1.z.1 }) },
            select X2 { name, owners2: {name, count} order BY .name }
            filter .name = 'Dwarf';
            """,
            [
                {
                    "name": "Dwarf",
                    "owners2": [
                        {"count": 3, "name": "Bob"},
                        {"count": 4, "name": "Carol"}
                    ]
                }
            ],
        )

    async def test_edgeql_props_back_02(self):
        await self.assert_query_result(
            r'''
            select Card { name, z := .<deck[IS User] { name, @count }}
            filter .name = 'Dragon';
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"@count": 2, "name": "Alice"},
                        {"@count": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_back_03(self):
        await self.assert_query_result(
            r'''
            select Card { name, z := .<deck[IS User] { name, x := @count }}
            filter .name = 'Dragon';
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"x": 2, "name": "Alice"},
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_back_04(self):
        await self.assert_query_result(
            r'''
            select assert_exists((
                select Card { name, z := .<deck[IS User] { name, @count }}
                filter .name = 'Dragon'
            ));
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"@count": 2, "name": "Alice"},
                        {"@count": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_back_05(self):
        await self.assert_query_result(
            r'''
            select assert_exists((
                select Card { name, z := .<deck[IS User] { name, x := @count }}
                filter .name = 'Dragon'
            ));
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"x": 2, "name": "Alice"},
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_back_06(self):
        # This should not work
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"has no property 'count'"):

            await self.con.query(
                r'''
                    select Card { name, z := .<deck { @count }}
                    filter .name = 'Dragon'
                '''
            )

    @test.xfail('We are too permissive with intersections on supertypes')
    async def test_edgeql_props_back_07(self):
        # This should not work
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"has no property 'count'"):

            await self.con.query(
                r'''
                    select Card { name, z := .<deck[IS Object] { @count }}
                    filter .name = 'Dragon'
                '''
            )

    async def test_edgeql_props_back_08(self):
        await self.assert_query_result(
            r'''
            select Card { name, z := .<deck[IS Bot] { name, x := @count }}
            filter .name = 'Dragon';
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    @test.xerror('Stack overflow!')
    async def test_edgeql_props_back_09(self):
        await self.assert_query_result(
            r'''
            select assert_exists((
                select Card { name, z := .<deck[IS User] {
                  name, @count := @count }}
                filter .name = 'Dragon'
            ));
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"x": 2, "name": "Alice"},
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_schema_back_00(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"has no property 'total_cost'"):

            await self.con.query(
                r'''
                    select (Card.name, Card.owners@total_cost)
                '''
            )

    async def test_edgeql_props_schema_back_01(self):
        await self.assert_query_result(
            r'''
                select (Card.name, Card.owners.name, Card.owners@count)
                filter Card.name = 'Dragon'
                order by Card.owners.name
            ''',
            [["Dragon", "Alice", 2], ["Dragon", "Dave", 1]],
        )

    async def test_edgeql_props_schema_back_02(self):
        await self.assert_query_result(
            r'''
            select Card { name, z := .owners { name, @count }}
            filter .name = 'Dragon';
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"@count": 2, "name": "Alice"},
                        {"@count": 1, "name": "Dave"},
                    ])
                }
            ]
        )
        await self.assert_query_result(
            r'''
            select Card { name, owners: { name, @count }}
            filter .name = 'Dragon';
            ''',
            [
                {
                    "name": "Dragon",
                    "owners": tb.bag([
                        {"@count": 2, "name": "Alice"},
                        {"@count": 1, "name": "Dave"},
                    ])
                }
            ]
        )

        await self.assert_query_result(
            r'''
            select SpecialCard { name, owners: { name, @count }}
            filter .name = 'Djinn';
            ''',
            [
                {
                    "name": "Djinn",
                    "owners": tb.bag([
                        {"@count": 1, "name": "Carol"},
                        {"@count": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_schema_back_03(self):
        await self.assert_query_result(
            r'''
            select Card { name, z := .owners { name, x := @count }}
            filter .name = 'Dragon';
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"x": 2, "name": "Alice"},
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

        await self.assert_query_result(
            r'''
            select Card { name, owners: { name, x := @count }}
            filter .name = 'Dragon';
            ''',
            [
                {
                    "name": "Dragon",
                    "owners": tb.bag([
                        {"x": 2, "name": "Alice"},
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_schema_back_04(self):
        await self.assert_query_result(
            r'''
            select assert_exists((
                select Card { name, z := .owners { name, @count }}
                filter .name = 'Dragon'
            ));
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"@count": 2, "name": "Alice"},
                        {"@count": 1, "name": "Dave"},
                    ])
                }
            ]
        )

        await self.assert_query_result(
            r'''
            select assert_exists((
                select Card { name, owners: { name, @count }}
                filter .name = 'Dragon'
            ));
            ''',
            [
                {
                    "name": "Dragon",
                    "owners": tb.bag([
                        {"@count": 2, "name": "Alice"},
                        {"@count": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_schema_back_05(self):
        await self.assert_query_result(
            r'''
            select assert_exists((
                select Card { name, z := .owners { name, x := @count }}
                filter .name = 'Dragon'
            ));
            ''',
            [
                {
                    "name": "Dragon",
                    "z": tb.bag([
                        {"x": 2, "name": "Alice"},
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

        await self.assert_query_result(
            r'''
            select assert_exists((
                select Card { name, owners: { name, x := @count }}
                filter .name = 'Dragon'
            ));
            ''',
            [
                {
                    "name": "Dragon",
                    "owners": tb.bag([
                        {"x": 2, "name": "Alice"},
                        {"x": 1, "name": "Dave"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_intersect_01(self):
        await self.assert_query_result(
            r'''
            select Named {
               [IS User].deck:{name, @count}
            } filter .name = 'Alice';
            ''',
            [
                {
                    "deck": tb.bag([
                        {"@count": 2, "name": "Imp"},
                        {"@count": 2, "name": "Dragon"},
                        {"@count": 3, "name": "Bog monster"},
                        {"@count": 3, "name": "Giant turtle"},
                    ])
                }
            ]
        )

    async def test_edgeql_props_bogus_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"implicit reference to an object changes the "
                r"interpretation of it elsewhere in the query"):

            await self.con.query(
                r'''
                    select (
                      select User
                    ).deck {
                      linkprop := @count
                    };
                '''
            )

    async def test_edgeql_props_modification_01(self):
        await self.con.execute(r'''
            CREATE TYPE Tgt;
            CREATE TYPE Src {
                CREATE LINK l -> Tgt {
                    CREATE PROPERTY x -> str;
                };
            };
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"link 'l' of object type 'default::Src' has no property 'y'",
                _hint="did you mean 'x'?"):
            await self.con.query(
                r'''
                    insert Src { l := assert_single(Tgt { @y := "..." }) };
                '''
            )

    async def test_edgeql_props_tuples_01(self):
        await self.con.execute(r'''
            create type Org;
            create type Foo {
                create multi link orgs -> Org {
                    create property roles -> tuple<role1: bool, role2: bool>;
                }
            };
            insert Org;
            insert Foo { orgs := (select Org {
                @roles := (role1 := true, role2 := false) }) };
        ''')

        await self.assert_query_result(
            '''
            select Foo.orgs@roles.role1;
            ''',
            [True],
        )

    async def test_edgeql_pure_computed_linkprops_01(self):
        await self.con.execute(r'''
            CREATE TYPE default::Test3 {
                CREATE PROPERTY name: std::str {
                    SET default := 'test3';
                };
            };
            CREATE TYPE default::Test4 {
                CREATE LINK test3ref: default::Test3 {
                    CREATE PROPERTY note := (.name);
                };
                CREATE PROPERTY name: std::str {
                    SET default := 'test4';
                };
            };
            insert Test3;
        ''')

        await self.assert_query_result(
            '''
            insert Test4 { test3ref := (select Test3 limit 1)};
            ''',
            [{}],
        )

    async def test_edgeql_props_target_06(self):
        # This should not work
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"@target may only be used in index and constraint definitions"
        ):
            await self.con.query(
                r'''
                SELECT schema::ObjectType {
                  name,
                  is_abstract,
                  bases: {
                    name,
                  } ORDER BY @index ASC,
                  pointers: {
                    cardinality,
                    required,
                    name,
                    target: {
                      name,
                    },
                    kind := 'link' IF @target IS schema::Link ELSE 'property'
                  },
                } FILTER NOT .is_compound_type;
                '''
            )
