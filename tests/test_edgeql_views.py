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


import json
import os.path

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLViews(tb.QueryTestCase):
    '''The scope is to test views.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    SETUP = [os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_setup.edgeql'),
             os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_views_setup.edgeql')]

    async def test_edgeql_views_basic_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT AirCard {
                    name,
                    owners: {
                        name
                    } ORDER BY .name
                } ORDER BY AirCard.name;
            ''',
            [
                {
                    'name': 'Djinn',
                    'owners': [{'name': 'Carol'}, {'name': 'Dave'}]
                },
                {
                    'name': 'Giant eagle',
                    'owners': [{'name': 'Carol'}, {'name': 'Dave'}]
                },
                {
                    'name': 'Sprite',
                    'owners': [{'name': 'Carol'}, {'name': 'Dave'}]
                }
            ],
        )

    async def test_edgeql_views_basic_02(self):
        await self.con.execute('''
            CREATE VIEW test::expert_map := (
                SELECT {
                    ('Alice', 'pro'),
                    ('Bob', 'noob'),
                    ('Carol', 'noob'),
                    ('Dave', 'casual'),
                }
            );
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT expert_map
                ORDER BY expert_map;
            ''',
            [
                ['Alice', 'pro'],
                ['Bob', 'noob'],
                ['Carol', 'noob'],
                ['Dave', 'casual'],
            ],
        )

        await self.con.execute('''
            DROP VIEW test::expert_map;
        ''')

    async def test_edgeql_views_basic_03(self):
        await self.con.execute('''
            CREATE VIEW test::scores := (
                SELECT {
                    (name := 'Alice', score := 100, games := 10),
                    (name := 'Bob', score := 11, games := 2),
                    (name := 'Carol', score := 31, games := 5),
                    (name := 'Dave', score := 78, games := 10),
                }
            );
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT scores ORDER BY scores.name;
            ''',
            [
                {'name': 'Alice', 'score': 100, 'games': 10},
                {'name': 'Bob', 'score': 11, 'games': 2},
                {'name': 'Carol', 'score': 31, 'games': 5},
                {'name': 'Dave', 'score': 78, 'games': 10},
            ],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT <tuple<str, int64, int64>>scores
                ORDER BY scores.name;
            ''',
            [
                ['Alice', 100, 10],
                ['Bob', 11, 2],
                ['Carol', 31, 5],
                ['Dave', 78, 10],
            ],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT <tuple<name: str, points: int64, plays: int64>>scores
                ORDER BY scores.name;
            ''',
            [
                {'name': 'Alice', 'points': 100, 'plays': 10},
                {'name': 'Bob', 'points': 11, 'plays': 2},
                {'name': 'Carol', 'points': 31, 'plays': 5},
                {'name': 'Dave', 'points': 78, 'plays': 10},
            ],
        )

        await self.con.execute('''
            DROP VIEW test::scores;
        ''')

    async def test_edgeql_views_basic_04(self):
        await self.con.execute('''
            CREATE VIEW test::levels := {'pro', 'casual', 'noob'};
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test SELECT levels;
            ''',
            {'pro', 'casual', 'noob'},
        )

    async def test_edgeql_views_create_01(self):
        await self.con.execute(r'''
            CREATE VIEW test::DCard := (
                WITH MODULE test
                SELECT Card {
                    # This is an identical computable to the one
                    # present in the type, but it must be legal to
                    # override the link with any compatible
                    # expression.
                    owners := (
                        SELECT Card.<deck[IS User] {
                            name_upper := str_upper(.name)
                        }
                    )
                } FILTER Card.name LIKE 'D%'
            );
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DCard {
                    name,
                    owners: {
                        name_upper,
                    } ORDER BY .name
                } ORDER BY DCard.name;
            ''',
            [
                {
                    'name': 'Djinn',
                    'owners': [{'name_upper': 'CAROL'},
                               {'name_upper': 'DAVE'}],
                },
                {
                    'name': 'Dragon',
                    'owners': [{'name_upper': 'ALICE'},
                               {'name_upper': 'DAVE'}],
                },
                {
                    'name': 'Dwarf',
                    'owners': [{'name_upper': 'BOB'},
                               {'name_upper': 'CAROL'}],
                }
            ],
        )

        await self.con.execute('DROP VIEW test::DCard;')

        # Check that we can recreate the view.
        await self.con.execute(r'''
            CREATE VIEW test::DCard := (
                WITH MODULE test
                SELECT Card {
                    owners := (
                        SELECT Card.<deck[IS User] {
                            name_upper := str_upper(.name)
                        }
                    )
                } FILTER Card.name LIKE 'D%'
            );
        ''')

        await self.assert_query_result(
            r'''
                WITH
                    MODULE schema,
                    DCardT := (SELECT ObjectType
                               FILTER .name = 'test::DCard'),
                    DCardOwners := (SELECT DCardT.links
                                    FILTER .name = 'owners')
                SELECT
                    DCardOwners {
                        target: ObjectType {
                            name,
                            pointers: {
                                name
                            } FILTER .name = 'name_upper'
                        }
                    }
            ''',
            [{
                'target': {
                    'name': 'test::__DCard__owners',
                    'pointers': [
                        {
                            'name': 'name_upper',
                        }
                    ]
                }
            }]
        )

    async def test_edgeql_views_filter_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT FireCard {name}
                FILTER FireCard = DaveCard
                ORDER BY FireCard.name;
            ''',
            [{'name': 'Dragon'}],
        )

    async def test_edgeql_views_filter02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT AirCard {name}
                FILTER AirCard NOT IN (SELECT Card FILTER Card.name LIKE 'D%')
                ORDER BY AirCard.name;
            ''',
            [
                {'name': 'Giant eagle'},
                {'name': 'Sprite'},
            ],
        )

    async def test_edgeql_computable_link_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT Card {
                    owners: {
                        name
                    } ORDER BY .name
                }
                FILTER .name = 'Djinn';
            ''',
            [{
                'owners': [
                    {'name': 'Carol'},
                    {'name': 'Dave'}
                ]
            }]
        )

    async def test_edgeql_computable_link_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    deck_cost
                }
                ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'deck_cost': 11
                },
                {
                    'name': 'Bob',
                    'deck_cost': 9
                },
                {
                    'name': 'Carol',
                    'deck_cost': 16
                },
                {
                    'name': 'Dave',
                    'deck_cost': 20
                }
            ]
        )

    async def test_edgeql_computable_aliased_link_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT AliasedFriends {
                    my_name,
                    my_friends: {
                        @nickname
                    } ORDER BY .name
                }
                FILTER .name = 'Alice';
            ''',
            [{
                'my_name': 'Alice',
                'my_friends': [
                    {
                        '@nickname': 'Swampy'
                    },
                    {
                        '@nickname': 'Firefighter'
                    },
                    {
                        '@nickname': 'Grumpy'
                    },
                ]
            }]
        )

    async def test_edgeql_computable_nested_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT Card {
                    name,
                    owned := (
                        WITH O := Card.<deck[IS User]
                        SELECT O {
                            name,
                            # simple computable
                            fr0 := count(O.friends),
                            # computable with a view defined
                            fr1 := (WITH F := O.friends SELECT count(F)),
                        }
                        ORDER BY .name
                    )
                } FILTER .name = 'Giant turtle';
            ''',
            [{
                'name': 'Giant turtle',
                'owned': [
                    {'fr0': 3, 'fr1': 3, 'name': 'Alice'},
                    {'fr0': 0, 'fr1': 0, 'name': 'Bob'},
                    {'fr0': 0, 'fr1': 0, 'name': 'Carol'},
                    {'fr0': 1, 'fr1': 1, 'name': 'Dave'},
                ]
            }]
        )

    async def test_edgeql_view_shape_propagation_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT _ := {
                    (SELECT User FILTER .name = 'Alice').deck,
                    (SELECT User FILTER .name = 'Bob').deck
                } {name}
                ORDER BY _.name;
            ''',
            [
                {'name': 'Bog monster'},
                {'name': 'Bog monster'},
                {'name': 'Dragon'},
                {'name': 'Dwarf'},
                {'name': 'Giant turtle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
                {'name': 'Imp'},
            ],
        )

    async def test_edgeql_view_shape_propagation_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                # the view should be propagated through _ := DISTINCT since it
                # maps `any` to `any`
                SELECT _ := DISTINCT {
                    (SELECT User FILTER .name = 'Alice').deck,
                    (SELECT User FILTER .name = 'Bob').deck
                } {name}
                ORDER BY _.name;
            ''',
            [
                {'name': 'Bog monster'},
                {'name': 'Dragon'},
                {'name': 'Dwarf'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
                {'name': 'Imp'},
            ],
        )

    async def test_edgeql_view_shape_propagation_03(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                # the view should be propagated through _ := DETACHED
                SELECT _ := DETACHED {
                    (SELECT User FILTER .name = 'Alice').deck,
                    (SELECT User FILTER .name = 'Bob').deck
                } {name}
                ORDER BY _.name;
            ''',
            [
                {'name': 'Bog monster'},
                {'name': 'Bog monster'},
                {'name': 'Dragon'},
                {'name': 'Dwarf'},
                {'name': 'Giant turtle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
                {'name': 'Imp'},
            ],
        )

    async def test_edgeql_view_shape_propagation_04(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                # the view should be propagated through _ := DETACHED
                SELECT _ := DETACHED ({
                    (SELECT User FILTER .name = 'Alice').deck,
                    (SELECT User FILTER .name = 'Bob').deck
                } {name})
                ORDER BY _.name;
            ''',
            [
                {'name': 'Bog monster'},
                {'name': 'Bog monster'},
                {'name': 'Dragon'},
                {'name': 'Dwarf'},
                {'name': 'Giant turtle'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
                {'name': 'Imp'},
            ],
        )

    async def test_edgeql_views_if_else_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    _ := 'yes' IF Card.cost > 4 ELSE 'no'
                ORDER BY _;
            """,
            ['no', 'no', 'no', 'no', 'no', 'no', 'no', 'no', 'yes'],
        )

    async def test_edgeql_views_if_else_02(self):
        await self.assert_query_result(
            r"""
                # working with singletons
                WITH MODULE test
                SELECT
                    _ := 'ok' IF User.deck_cost < 19 ELSE User.deck.name
                ORDER BY _;
            """,
            [
                'Bog monster',
                'Djinn',
                'Dragon',
                'Giant eagle',
                'Giant turtle',
                'Golem',
                'Sprite',
                'ok',
                'ok',
                'ok',
            ],
        )

        await self.assert_query_result(
            r"""
                # either result is a set, but the condition is a singleton
                WITH MODULE test
                SELECT
                    _ := User.deck.element IF User.deck_cost < 19
                         ELSE User.deck.name
                ORDER BY _;
            """,
            [
                'Air',
                'Air',
                'Air',
                'Bog monster',
                'Djinn',
                'Dragon',
                'Earth',
                'Earth',
                'Earth',
                'Earth',
                'Fire',
                'Fire',
                'Giant eagle',
                'Giant turtle',
                'Golem',
                'Sprite',
                'Water',
                'Water',
                'Water',
                'Water',
                'Water',
                'Water',
            ],
        )

    async def test_edgeql_views_if_else_03(self):
        res = [
            ['Air', 'Air', 'Air', 'Earth', 'Earth', 'Fire', 'Fire', 'Water',
             'Water'],
            ['1', '1', '1', '2', '2', '3', '3', '4', '5'],
            [False, False, False, True, True],
        ]

        await self.assert_query_result(
            r"""
                # get the data that this test relies upon in a format
                # that's easy to analyze
                WITH MODULE test
                SELECT _ := User.deck.element
                ORDER BY _;
            """,
            res[0]
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT _ := <str>User.deck.cost
                ORDER BY _;
            """,
            res[1]
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT _ := {User.name[0] = 'A', EXISTS User.friends}
                ORDER BY _;
            """,
            res[2]
        )

        await self.assert_query_result(
            r"""
                # results and conditions are sets
                WITH MODULE test
                SELECT _ :=
                    User.deck.element
                    # because the elements of {} are treated as SET OF,
                    # all of the paths in this expression are independent sets
                    IF {User.name[0] = 'A', EXISTS User.friends} ELSE
                    <str>User.deck.cost
                ORDER BY _;
            """,
            sorted(res[1] + res[1] + res[1] + res[0] + res[0]),
        )

    async def test_edgeql_views_if_else_04(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    1   IF User.name[0] = 'A' ELSE
                    10  IF User.name[0] = 'B' ELSE
                    100 IF User.name[0] = 'C' ELSE
                    0;
            """,
            {1, 10, 100, 0},
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (
                    User.name,
                    sum(
                        1   IF User.friends.name[0] = 'A' ELSE
                        10  IF User.friends.name[0] = 'B' ELSE
                        100 IF User.friends.name[0] = 'C' ELSE
                        0
                    ),
                ) ORDER BY .0;
            """,
            [['Alice', 110], ['Bob', 0], ['Carol', 0], ['Dave', 10]],
        )

    async def test_edgeql_views_if_else_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    (Card.name, 'yes' IF Card.cost > 4 ELSE 'no')
                ORDER BY .0;
            """,
            [
                ['Bog monster', 'no'],
                ['Djinn', 'no'],
                ['Dragon', 'yes'],
                ['Dwarf', 'no'],
                ['Giant eagle', 'no'],
                ['Giant turtle', 'no'],
                ['Golem', 'no'],
                ['Imp', 'no'],
                ['Sprite', 'no'],
            ],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    (Card.name, 'yes') IF Card.cost > 4 ELSE (Card.name, 'no')
                ORDER BY .0;
            """,
            [
                ['Bog monster', 'no'],
                ['Djinn', 'no'],
                ['Dragon', 'yes'],
                ['Dwarf', 'no'],
                ['Giant eagle', 'no'],
                ['Giant turtle', 'no'],
                ['Golem', 'no'],
                ['Imp', 'no'],
                ['Sprite', 'no'],
            ],
        )

    async def test_edgeql_views_nested_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT AwardView {
                    name,
                    winner: {
                        name
                    }
                } ORDER BY .name;
            """,
            [
                {'name': '1st', 'winner': {'name': 'Alice'}},
                {'name': '2nd', 'winner': {'name': 'Alice'}},
                {'name': '3rd', 'winner': {'name': 'Bob'}},
            ],
        )

    async def test_edgeql_views_nested_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT stdgraphql::Query {
                    foo := (
                        SELECT AwardView {
                            name,
                            winner: {
                                name
                            }
                        } ORDER BY .name
                    )
                };
            """,
            [
                {
                    'foo': [
                        {'name': '1st', 'winner': {'name': 'Alice'}},
                        {'name': '2nd', 'winner': {'name': 'Alice'}},
                        {'name': '3rd', 'winner': {'name': 'Bob'}},
                    ]
                }
            ],
        )

    async def test_edgeql_views_deep_01(self):
        # fetch the result we will compare to
        res = await self.con.fetchall_json(r"""
            WITH MODULE test
            SELECT AwardView {
                winner: {
                    deck: {
                        owners
                    }
                }
            }
            FILTER .name = '1st'
            LIMIT 1;
        """)
        res = json.loads(res)

        # fetch the same data via a different view, that should be
        # functionally identical
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT AwardView2 {
                    winner: {
                        deck: {
                            owners
                        }
                    }
                }
                FILTER .name = '1st';
            """,
            res
        )

    async def test_edgeql_views_clauses_01(self):
        # fetch the result we will compare to
        res = await self.con.fetchall_json(r"""
            WITH MODULE test
            SELECT User {
                deck: {
                    id
                } ORDER BY User.deck.cost DESC
                  LIMIT 1,
            }
            FILTER .name = 'Alice';
        """)
        res = json.loads(res)

        # fetch the same data via a view, that should be
        # functionally identical
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT UserView {
                    deck,
                }
                FILTER .name = 'Alice';
            """,
            res
        )

    async def test_edgeql_views_limit_01(self):
        # Test interaction of views and the LIMIT clause
        await self.con.execute("""
            WITH MODULE test
            CREATE VIEW FirstUser := (
                SELECT User {
                    name_upper := str_upper(User.name)
                }
                ORDER BY .name
                LIMIT 1
            );
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT FirstUser {
                    name_upper,
                }
            """,
            [
                {
                    'name_upper': 'ALICE',
                },
            ],
        )

    async def test_edgeql_views_ignore_alias(self):
        await self.con.execute('''
            SET MODULE test;

            CREATE VIEW UserView2 := (
                SELECT User {
                    deck: {
                        id
                    } ORDER BY User.deck.cost DESC
                    LIMIT 1,
                }
            );
        ''')

        # Explicitly reset the default module alias to test
        # that views don't care.
        await self.con.execute('''
            SET MODULE std;
        ''')

        await self.assert_query_result(
            r"""
                SELECT test::UserView2 {
                    deck,
                }
                FILTER .name = 'Alice';
            """,
            [{
                'deck': [
                    {}
                ]
            }]
        )

    async def test_edgeql_views_esdl_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT WaterOrEarthCard {
                    name,
                    owned_by_alice,
                }
                FILTER .name ILIKE {'%turtle%', 'dwarf'}
                ORDER BY .name;
            """,
            [
                {
                    'name': 'Dwarf',
                    'owned_by_alice': True,
                },
                {
                    'name': 'Giant turtle',
                    'owned_by_alice': True,
                },
            ]
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT EarthOrFireCard {
                    name,
                }
                FILTER .name = {'Imp', 'Dwarf'}
                ORDER BY .name;
            """,
            [
                {
                    'name': 'Dwarf'
                },
                {
                    'name': 'Imp'
                },
            ]
        )

    async def test_edgeql_views_collection_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT SpecialCardView {
                    name,
                    el_cost,
                };
            """,
            [
                {
                    'name': 'Djinn',
                    'el_cost': ['Air', 4],
                },
            ]
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: missing FROM-clause entry
        for table "SpecialCard~8"

        See `test_edgeql_views_collection_04` or
        `test_edgeql_views_collection_05` for minimal setup.
    ''')
    async def test_edgeql_views_collection_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT SpecialCardView.el_cost;
            """,
            [
                ['Air', 4],
            ]
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: missing FROM-clause entry
        for table "SpecialCard~8"

        See `test_edgeql_views_collection_04` or
        `test_edgeql_views_collection_05` for minimal setup.
    ''')
    async def test_edgeql_views_collection_03(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    X := SpecialCard {
                        el_cost := (.element, .cost)
                    }
                SELECT X.el_cost;
            """,
            [
                ['Air', 4],
            ]
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: missing FROM-clause entry
        for table "SpecialCard~4"
    ''')
    async def test_edgeql_views_collection_04(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (
                    SpecialCard {
                        el_cost := (.element,)
                    }
                ).el_cost;
            """,
            [
                ['Air'],
            ]
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: missing FROM-clause entry
        for table "SpecialCard~4"
    ''')
    async def test_edgeql_views_collection_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (
                    SpecialCard {
                        el_cost := [.element]
                    }
                ).el_cost;
            """,
            [
                ['Air'],
            ]
        )
