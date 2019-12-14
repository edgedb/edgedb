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


class TestEdgeQLLinkproperties(tb.QueryTestCase):
    '''The scope is to test link properties.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.edgeql')

    async def test_edgeql_props_basic_01(self):
        await self.assert_query_result(
            r'''
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

    async def test_edgeql_props_basic_03(self):
        await self.assert_query_result(
            r'''
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
                WITH MODULE test
                SELECT Card {
                    name,
                    element,
                    cost
                }
                FILTER
                    .cost = .<deck[IS User]@count
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
                WITH MODULE test
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
                SELECT test::User.avatar@text;
            ''',
            [
                'Best'
            ]
        )

    async def test_edgeql_props_basic_07(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    avatar: {
                        @text
                    }
                } FILTER EXISTS .avatar@text;
            ''',
            [
                {'avatar': {'@text': 'Best'}},
            ]
        )

    async def test_edgeql_props_cross_01(self):
        await self.assert_query_result(
            r'''
                # get cards that have the same count in some deck as their cost
                WITH MODULE test
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

    async def test_edgeql_props_cross_02(self):
        await self.assert_query_result(
            r'''
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

    async def test_edgeql_props_cross_03(self):
        await self.assert_query_result(
            r'''
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
                WITH MODULE test
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

    async def test_edgeql_props_implication_01(self):
        await self.assert_query_result(
            r'''
                # count of 1 in at least some deck implies 'Fire'
                WITH MODULE test
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

    async def test_edgeql_props_implication_02(self):
        await self.assert_query_result(
            r'''
                # FILTER by NOT (count of 1 implies 'Fire')
                # in at least some deck
                WITH MODULE test
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

    async def test_edgeql_props_implication_03(self):
        await self.assert_query_result(
            r'''
                # same as above, refactored
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
                SELECT DISTINCT User.deck@count;
            ''',
            {1, 2, 3, 4},
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DISTINCT (
                    SELECT User.deck@count FILTER User.deck.element = 'Fire'
                );
            ''',
            {1, 2},
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DISTINCT (
                    SELECT User.deck@count FILTER User.deck.element = 'Water'
                );
            ''',
            {1, 2, 3},
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                    MODULE test,
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
                    MODULE test,
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
                WITH MODULE test
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
                WITH MODULE test
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
                    MODULE test,
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
                WITH
                    MODULE test
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
                WITH MODULE test
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
                    MODULE test,
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
                WITH
                    MODULE test
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
                WITH MODULE test
                SELECT sum(User.deck@count);
            ''',
            [51],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
                SELECT User {
                    name,
                    deck := (SELECT x := User.deck.name
                             ORDER BY x ASC
                             LIMIT 2)
                } ORDER BY .name;
            ''',
            [
                {"deck": ["Bog monster", "Dragon"], "name": "Alice"},
                {"deck": ["Bog monster", "Dwarf"], "name": "Bob"},
                {"deck": ["Bog monster", "Djinn"], "name": "Carol"},
                {"deck": ["Bog monster", "Djinn"], "name": "Dave"}
            ]
        )

    async def test_edgeql_props_link_shadow_02(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
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
