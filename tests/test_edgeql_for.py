#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


class TestEdgeQLFor(tb.QueryTestCase):
    '''These tests are focused on using FOR statement.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.edgeql')

    async def test_edgeql_for_cross_01(self):
        cards = ['Bog monster', 'Djinn', 'Dragon', 'Dwarf', 'Giant eagle',
                 'Giant turtle', 'Golem', 'Imp', 'Sprite']
        await self.assert_query_result(
            r'''
                FOR C IN Card
                # C and Card are not related here
                UNION (C.name, Card.name);
            ''',
            [[a, b] for a in cards for b in cards],
            sort=True
        )

    async def test_edgeql_for_cross_02(self):
        await self.assert_query_result(
            r'''
                FOR C IN Card
                # C and Card are not related here, so count(Card) should be 9
                UNION (C.name, count(Card));
            ''',
            [
                ['Bog monster', 9],
                ['Djinn', 9],
                ['Dragon', 9],
                ['Dwarf', 9],
                ['Giant eagle', 9],
                ['Giant turtle', 9],
                ['Golem', 9],
                ['Imp', 9],
                ['Sprite', 9],
            ],
            sort=True
        )

    async def test_edgeql_for_cross_03(self):
        await self.assert_query_result(
            r'''
                FOR Card IN Card
                # Card is shadowed here
                UNION (Card.name, count(Card));
            ''',
            [
                ['Bog monster', 1],
                ['Djinn', 1],
                ['Dragon', 1],
                ['Dwarf', 1],
                ['Giant eagle', 1],
                ['Giant turtle', 1],
                ['Golem', 1],
                ['Imp', 1],
                ['Sprite', 1],
            ],
            sort=True
        )

    async def test_edgeql_for_cross_04(self):
        await self.assert_query_result(
            r'''
                FOR C IN Card
                # C and Card are not related here, so count(Card) should be 9
                UNION (count(C), count(Card));
            ''',
            [
                [1, 9],
            ] * 9,
        )

    async def test_edgeql_for_mix_01(self):
        await self.assert_query_result(
            r'''
                FOR X IN {Card.name, User.name}
                UNION X;
            ''',
            {
                'Alice',
                'Bob',
                'Bog monster',
                'Carol',
                'Dave',
                'Djinn',
                'Dragon',
                'Dwarf',
                'Giant eagle',
                'Giant turtle',
                'Golem',
                'Imp',
                'Sprite',
            }
        )

    async def test_edgeql_for_mix_02(self):
        await self.assert_query_result(
            r'''
                FOR X IN {Card.name, User.name}
                # both Card and User should be independent of X
                UNION (X, count(Card), count(User));
            ''',
            [
                ['Alice', 9, 4],
                ['Bob', 9, 4],
                ['Bog monster', 9, 4],
                ['Carol', 9, 4],
                ['Dave', 9, 4],
                ['Djinn', 9, 4],
                ['Dragon', 9, 4],
                ['Dwarf', 9, 4],
                ['Giant eagle', 9, 4],
                ['Giant turtle', 9, 4],
                ['Golem', 9, 4],
                ['Imp', 9, 4],
                ['Sprite', 9, 4],
            ],
            sort=True
        )

    async def test_edgeql_for_mix_03(self):
        await self.assert_query_result(
            r'''
                # should be the same result as above
                FOR X IN {Card.name, User.name}
                UNION (X, count(Card FILTER TRUE), count(User FILTER TRUE));
            ''',
            [
                ['Alice', 9, 4],
                ['Bob', 9, 4],
                ['Bog monster', 9, 4],
                ['Carol', 9, 4],
                ['Dave', 9, 4],
                ['Djinn', 9, 4],
                ['Dragon', 9, 4],
                ['Dwarf', 9, 4],
                ['Giant eagle', 9, 4],
                ['Giant turtle', 9, 4],
                ['Golem', 9, 4],
                ['Imp', 9, 4],
                ['Sprite', 9, 4],
            ],
            sort=True
        )

    async def test_edgeql_for_mix_04(self):
        await self.assert_query_result(
            r'''
                FOR X IN {Card.name, User.name}
                # this should be just [3] for each name (9 + 4 of names)
                UNION count(User.friends);
            ''',
            [3] * 13
        )

    async def test_edgeql_for_limit_01(self):
        await self.assert_query_result(
            r'''
                SELECT X := (
                    FOR X IN {User.name}
                    UNION X
                )
                ORDER BY X
                OFFSET 2
                LIMIT 1
            ''',
            {
                'Carol',
            }
        )

    async def test_edgeql_for_implicit_limit_01(self):
        await self.assert_query_result(
            r'''
                select sum((
                  for i in range_unpack(range(0, 10000)) union
                    1
                ));
            ''',
            [10000],
            implicit_limit=100,
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_for_filter_02(self):
        await self.assert_query_result(
            r'''
                SELECT X := (
                    FOR X IN {Card.name}
                    UNION X
                )
                # this FILTER should have no impact
                FILTER Card.element = 'Air';
            ''',
            {
                'Bog monster',
                'Djinn',
                'Dragon',
                'Dwarf',
                'Giant eagle',
                'Giant turtle',
                'Golem',
                'Imp',
                'Sprite',
            }
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_for_filter_03(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    # get a combination of names from different object types
                    FOR X IN {Card.name, User.name}
                    UNION X
                )
                # this FILTER should have no impact
                FILTER Card.element = 'Air';
            ''',
            {
                'Alice',
                'Bob',
                'Bog monster',
                'Carol',
                'Dave',
                'Djinn',
                'Dragon',
                'Dwarf',
                'Giant eagle',
                'Giant turtle',
                'Golem',
                'Imp',
                'Sprite',
            }
        )

    async def test_edgeql_for_in_computable_01(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := assert_distinct((
                        FOR letter IN {'I', 'B'}
                        UNION (
                            SELECT User.deck {
                                name,
                                # just define an ad-hoc link prop
                                @letter := letter
                            }
                            FILTER User.deck.name[0] = letter
                        )
                    ))
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': tb.bag([
                        {'name': 'Bog monster', '@letter': 'B'},
                        {'name': 'Imp', '@letter': 'I'},
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_computable_02(self):
        await self.con.execute(
            """
            UPDATE User
            FILTER .name = "Alice"
            SET {
                deck += {
                    (INSERT Card {
                        name := "Ice Elemental",
                        element := "Water",
                        cost := 10,
                    }),
                    (INSERT Card {
                        name := "Basilisk",
                        element := "Earth",
                        cost := 20,
                    }),
                }
            }
            """
        )

        await self.assert_query_result(
            r"""
                SELECT User {
                    select_deck := (
                        SELECT DISTINCT((
                            FOR letter IN {'I', 'B'}
                            UNION (
                                FOR cost IN {1, 2, 10, 20}
                                UNION (
                                    SELECT User.deck {
                                        name,
                                        letter := letter ++ <str>cost
                                    }
                                    FILTER
                                        .name[0] = letter AND .cost = cost
                                )
                            )
                        ))
                        ORDER BY .name THEN .letter
                    )
                } FILTER .name = 'Alice';
            """,
            [
                {
                    'select_deck': [
                        {'name': 'Basilisk', 'letter': 'B20'},
                        {'name': 'Bog monster', 'letter': 'B2'},
                        {'name': 'Ice Elemental', 'letter': 'I10'},
                        {'name': 'Imp', 'letter': 'I1'},
                    ]
                }
            ]
        )

    async def test_edgeql_for_in_computable_02b(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := ((
                        WITH cards := (
                            FOR letter IN {'I', 'B'}
                            FOR copy IN {'1', '2'}
                            SELECT User.deck {
                                name,
                                letter := letter ++ copy
                            }
                            FILTER User.deck.name[0] = letter
                        )
                        SELECT cards ORDER BY .name THEN .letter
                    ),)
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': [
                        [{}], [{}], [{}], [{}],
                    ]
                }
            ]
        )

    async def test_edgeql_for_in_computable_02c(self):
        await self.assert_query_result(
            r"""
                SELECT User {
                    select_deck := DISTINCT (
                        FOR v IN { ("Imp", 1), ("Dragon", 2) }
                        UNION (
                            SELECT Card {
                                name,
                                count := <int64>v.1
                            }
                            FILTER .name = <str>v.0
                        )
                    )
                } FILTER .name = 'Alice'
            """,
            [{
                "select_deck": [
                    {"name": "Imp", "count": 1},
                    {"name": "Dragon", "count": 2},
                ],
            }],
        )

    async def test_edgeql_for_in_computable_02d(self):
        await self.con.execute(
            """
            UPDATE User
            FILTER .name = "Alice"
            SET {
                deck += {
                    (INSERT Card {
                        name := "Ice Elemental",
                        element := "Water",
                        cost := 10,
                    }),
                    (INSERT Card {
                        name := "Basilisk",
                        element := "Earth",
                        cost := 20,
                    }),
                }
            }
            """
        )

        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := assert_distinct((
                        WITH cards := (
                            FOR letter IN {'I', 'B'}
                            UNION (
                                FOR cost IN {1, 2, 10, 20}
                                UNION (
                                    SELECT User.deck {
                                        name,
                                        letter := letter ++ <str>cost
                                    }
                                    FILTER
                                        .name[0] = letter AND .cost = cost
                                )
                            )
                        )
                        SELECT cards {name, letter} ORDER BY .name THEN .letter
                    ))
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': [
                        {'name': 'Basilisk', 'letter': 'B20'},
                        {'name': 'Bog monster', 'letter': 'B2'},
                        {'name': 'Ice Elemental', 'letter': 'I10'},
                        {'name': 'Imp', 'letter': 'I1'},
                    ]
                }
            ]
        )

    async def test_edgeql_for_in_computable_02e(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := ((
                        WITH cards := (
                            FOR letter IN {'I', 'B'}
                            UNION (
                                FOR copy IN {'1', '2'}
                                UNION (
                                    SELECT User.deck {
                                        name,
                                        letter := letter ++ copy
                                    }
                                    FILTER User.deck.name[0] = letter
                                )
                            )
                        )
                        SELECT cards {name, letter} ORDER BY .name THEN .letter
                    ),)
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': [
                        [{'name': 'Bog monster', 'letter': 'B1'}],
                        [{'name': 'Bog monster', 'letter': 'B2'}],
                        [{'name': 'Imp', 'letter': 'I1'}],
                        [{'name': 'Imp', 'letter': 'I2'}],
                    ]
                }
            ]
        )

    @test.xerror('deeply nested linkprop hoisting is currently broken')
    async def test_edgeql_for_in_computable_03(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := (
                        SELECT _ := (
                            FOR letter IN {'I', 'B'}
                            UNION (
                                SELECT User.deck {
                                    name,
                                    @letter := letter
                                }
                                FILTER User.deck.name[0] = letter
                            )
                        ) {
                            name,
                            # redefine the _letter as a link prop
                            @letter := ._letter
                        }
                        ORDER BY _.name
                    )
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': [
                        {'name': 'Bog monster', '@letter': 'B'},
                        {'name': 'Imp', '@letter': 'I'},
                    ]
                }
            ]
        )

    @test.xerror('''
        See comment on why this test doesn't contain a FOR.

        The result is *almost* correct, but oddly @letter is not a
        singleton, even though it's equal to a tuple element, which
        should be a singleton by definition.

        See `test_edgeql_scope_tuple_13` for a shorter version of the
        same issue.
    ''')
    async def test_edgeql_for_in_computable_04(self):
        # This is trying to compute the same result as in previous
        # example, but without using a FOR. Instead relying on
        # property of tuples and cross-products to get the same
        # result.
        #
        # In principle FOR x := <expr0> UNION <expr1> {<shape>} can be
        # conceptually refactored as:
        # WITH
        #   el0 := <expr0>,
        #   tup := (el0, expr1)
        # SELECT tup.1 {<shape>};
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := (
                        WITH
                            Deck := User.deck,
                            letter := {'I', 'B'},
                            tup := (
                                SELECT (
                                    letter,
                                    (
                                        SELECT Deck
                                        FILTER Deck.name[0] = letter
                                    )
                                )
                            )
                        SELECT _ := tup.1 {
                            name,
                            # redefine the _letter as a link prop
                            @letter := tup.0
                        }
                        ORDER BY _.name
                    )
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': [
                        {'name': 'Bog monster', '@letter': 'B'},
                        {'name': 'Imp', '@letter': 'I'},
                    ]
                }
            ]
        )

    async def test_edgeql_for_in_computable_05(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := (
                        FOR letter IN {'X'}
                        UNION (
                            (SELECT .deck.name)
                        )
                    )
                } FILTER .name = 'Alice';
            ''',
            [{"select_deck":
              tb.bag(["Bog monster", "Dragon", "Giant turtle", "Imp"])}],
        )

        # This one caused a totally nonsense type error.
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := (
                        FOR letter IN 'X'
                        UNION (
                            ((SELECT .deck).name)
                        )
                    )
                } FILTER .name = 'Alice';
            ''',
            [{"select_deck":
              tb.bag(["Bog monster", "Dragon", "Giant turtle", "Imp"])}],
        )

    async def test_edgeql_for_in_computable_06(self):
        await self.assert_query_result(
            r'''
            SELECT User {
                select_deck := assert_distinct((
                    WITH ps := (FOR x IN {"!", "?"} UNION (x)),
                    FOR letter IN {'I', 'B'}
                    UNION (
                        SELECT .deck {
                            name,
                            letter := letter ++ "!" ++ ps,
                        }
                        FILTER User.deck.name[0] = letter
                    )
                ))
            } FILTER .name = 'Alice';
            ''',
            [
                {
                    "select_deck": tb.bag([
                        {"letter": {"B!!", "B!?"}, "name": "Bog monster"},
                        {"letter": {"I!!", "I!?"}, "name": "Imp"},
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_computable_07(self):
        await self.assert_query_result(
            r'''
            SELECT User {
                select_deck := assert_distinct((
                    WITH ps := (FOR x IN {"!", "?"} UNION (
                        SELECT { z := x }).z),
                    FOR letter IN {'I', 'B'}
                    UNION (
                        SELECT .deck {
                            name,
                            letter := letter ++ "!" ++ ps,
                        }
                        FILTER User.deck.name[0] = letter
                    )
                ))
            } FILTER .name = 'Alice';
            ''',
            [
                {
                    "select_deck": tb.bag([
                        {"letter": ["B!!", "B!?"], "name": "Bog monster"},
                        {"letter": ["I!!", "I!?"], "name": "Imp"},
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_computable_08(self):
        await self.assert_query_result(
            r'''
            SELECT User {
                select_deck := assert_distinct((
                    WITH ps := (FOR x in {"!", "?"} UNION (x++""))
                    FOR letter IN {'I', 'B'}
                    UNION (
                        SELECT .deck {
                            name,
                            letter := letter ++ "!" ++ ps,
                            correlated := (ps, ps),
                            uncorrelated := ((SELECT ps), (SELECT ps)),
                        }
                        FILTER User.deck.name[0] = letter
                    )
                ))
            } FILTER .name = 'Alice';
            ''',
            [
                {
                    "select_deck": tb.bag([
                        {
                            "name": "Bog monster",
                            "letter": {"B!!", "B!?"},
                            "correlated": {("!", "!"), ("?", "?")},
                            "uncorrelated": {("!", "!"), ("!", "?"),
                                             ("?", "!"), ("?", "?")}
                        },
                        {
                            "name": "Imp",
                            "letter": {"I!!", "I!?"},
                            "correlated": {("!", "!"), ("?", "?")},
                            "uncorrelated": {("!", "!"), ("!", "?"),
                                             ("?", "!"), ("?", "?")}
                        },
                    ])
                }
            ],
        )

    @test.xerror("'letter' does not exist")
    async def test_edgeql_for_in_computable_09(self):
        # This is basically test_edgeql_for_in_computable_01 but with
        # a WITH binding in front of the whole shape
        await self.assert_query_result(
            r'''
                WITH
                    U := (
                        SELECT User {
                            select_deck := (
                                FOR letter IN {'I', 'B'}
                                UNION (
                                    SELECT User.deck {
                                        name,
                                        # just define an ad-hoc link prop
                                        @letter := letter
                                    }
                                    FILTER User.deck.name[0] = letter
                                )
                            )
                        } FILTER .name = 'Alice'
                   ),
                SELECT U { name, select_deck: { name, @letter } };
            ''',
            [
                {
                    'select_deck': tb.bag([
                        {'name': 'Bog monster', '@letter': 'B'},
                        {'name': 'Imp', '@letter': 'I'},
                    ])
                }
            ],
        )

    @test.xerror("""
        This outputs ["I", "B"] as letter for both objects.
    """)
    async def test_edgeql_for_in_computable_10(self):
        # This is basically test_edgeql_for_in_computable_01 but with
        # a WITH binding inside the computable and no link prop

        # If we just drop the WITH Z binding, we get
        # `letter does not exist`.
        # If we replace the WITH Z part with an extra SELECT,
        # we get the same buggy behavior.
        await self.assert_query_result(
            r'''
            SELECT (SELECT User {
                select_deck := (
                    WITH Z := (
                        FOR letter IN {'I', 'B'}
                        UNION (
                            SELECT .deck {
                                name,
                                # just define an ad-hoc link prop
                                letter := letter
                            }
                            FILTER User.deck.name[0] = letter
                        )
                    ),
                    SELECT assert_distinct(Z)
                )
            } FILTER .name = 'Alice') { select_deck: {name, letter} };
            ''',
            [
                {
                    'select_deck': tb.bag([
                        {'name': 'Bog monster', 'letter': 'B'},
                        {'name': 'Imp', 'letter': 'I'},
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_computable_11(self):
        await self.assert_query_result(
            r"""
            SELECT
                User {
                    select_deck := DISTINCT (
                        FOR name IN {'Imp', 'Imp'}
                        UNION (
                            SELECT Card {name}
                            FILTER .name = name
                        )
                    )
                }
            FILTER
                .name = 'Alice'
            """,
            [
                {
                    'select_deck': [{
                        'name': 'Imp',
                    }],
                },
            ],
        )

    async def test_edgeql_for_in_computable_12(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := (assert_exists((
                        FOR letter IN {'I', 'B'}
                        UNION (
                            SELECT User.deck {
                                name,
                                letter := letter
                            }
                            FILTER User.deck.name[0] = letter
                        )
                    )),)
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': tb.bag([
                        [{'name': 'Bog monster', 'letter': 'B'}],
                        [{'name': 'Imp', 'letter': 'I'}],
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_computable_13(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    multi select_deck := assert_single((
                        FOR letter IN {'I', 'Z'}
                        UNION (
                            SELECT User.deck {
                                name,
                                letter := letter
                            }
                            FILTER User.deck.name[0] = letter
                        )
                    ))
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': [
                        {'name': 'Imp', 'letter': 'I'},
                    ]
                }
            ],
        )

    async def test_edgeql_for_in_computable_14(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := DISTINCT assert_exists((
                        FOR letter IN {'I', 'B'}
                        UNION (
                            SELECT User.deck {
                                name,
                                letter := letter
                            }
                            FILTER User.deck.name[0] = letter
                        )
                    ))
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': tb.bag([
                        {'name': 'Bog monster', 'letter': 'B'},
                        {'name': 'Imp', 'letter': 'I'},
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_computable_15(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := assert_distinct(assert_exists((
                        FOR letter IN {'I', 'B'}
                        UNION (
                            SELECT User.deck {
                                name,
                                letter := letter
                            }
                            FILTER User.deck.name[0] = letter
                        )
                    )))
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': tb.bag([
                        {'name': 'Bog monster', 'letter': 'B'},
                        {'name': 'Imp', 'letter': 'I'},
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_computable_16(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    select_deck := assert_exists(assert_distinct((
                        FOR letter IN {'I', 'B'}
                        UNION (
                            SELECT User.deck {
                                name,
                                letter := letter
                            }
                            FILTER User.deck.name[0] = letter
                        )
                    )))
                } FILTER .name = 'Alice';
            ''',
            [
                {
                    'select_deck': tb.bag([
                        {'name': 'Bog monster', 'letter': 'B'},
                        {'name': 'Imp', 'letter': 'I'},
                    ])
                }
            ],
        )

    async def test_edgeql_for_in_function_01(self):
        await self.assert_query_result(
            r'''
                SELECT array_unpack([(
                    FOR letter IN {'I', 'Z'}
                    UNION (
                        SELECT Card {name, letter := letter}
                        FILTER .name[0] = letter
                    )
                )]);
            ''',
            [{"letter": "I", "name": "Imp"}],
        )

    async def test_edgeql_for_in_function_02(self):
        await self.assert_query_result(
            r'''
                SELECT enumerate((
                    FOR letter IN {'I', 'Z'}
                    UNION (
                        SELECT Card {name, letter := letter}
                        FILTER .name[0] = letter
                    )
                )).1;
            ''',
            [{"letter": "I", "name": "Imp"}],
        )

    async def test_edgeql_for_in_function_03(self):
        await self.assert_query_result(
            r'''
                SELECT DISTINCT assert_exists((
                    FOR letter IN {'I', 'Z'}
                    UNION (
                        SELECT Card {name, letter := letter}
                        FILTER .name[0] = letter
                    )
                ));
            ''',
            [{"letter": "I", "name": "Imp"}],
        )

    async def test_edgeql_for_and_computable_05(self):
        await self.assert_query_result(
            r'''
                WITH X := (SELECT (FOR x IN {1,2} UNION (
                    SELECT User { m := x }))),
                SELECT count(X.m);
            ''',
            [8],
        )

    async def test_edgeql_for_correlated_01(self):
        await self.assert_query_result(
            r'''
                SELECT count((
                    WITH X := {1, 2}
                    SELECT (X, (FOR x in {X} UNION (SELECT x)))
                ));
            ''',
            [2],
        )

        await self.assert_query_result(
            r'''
                SELECT count((
                    WITH X := {1, 2}
                    SELECT ((FOR x in {X} UNION (SELECT x)), X)
                ));
            ''',
            [2],
        )

    async def test_edgeql_for_correlated_02(self):
        await self.assert_query_result(
            r'''
                SELECT count((Card.name,
                              (FOR x in {Card} UNION (SELECT x.name)),
                ));
            ''',
            [9],
        )

    async def test_edgeql_for_correlated_03(self):
        await self.assert_query_result(
            r'''
                SELECT count(((FOR x in {Card} UNION (SELECT x.name)),
                               Card.name,
                ));
            ''',
            [9],
        )

    async def test_edgeql_for_empty_01(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            "FOR statement has iterator of indeterminate type",
        ):
            await self.con.execute("""
                SELECT (FOR x in {} UNION ());
            """)

    async def test_edgeql_for_empty_02(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            "FOR statement has iterator of indeterminate type",
        ):
            await self.con.execute("""
                WITH s := {} SELECT (FOR x in {s} UNION ());
            """)

    async def test_edgeql_for_fake_group_01a(self):
        await self.assert_query_result(
            r'''
            with GR := (
                for x in {'Earth', 'Water'} union { key := {element := x} }
            )
            select GR {
              key: {element},
            }
            order by .key.element;
            ''',
            [{"key": {"element": "Earth"}}, {"key": {"element": "Water"}}]
        )

    async def test_edgeql_for_fake_group_01b(self):
        await self.assert_query_result(
            r'''
            with GR := (
                for x in {'Earth', 'Water'} union {
                    key := {element := x},
                    elements := (select Card filter .element = x),
                }
            )
            select GR {
              key: {element},
            }
            order by .key.element;
            ''',
            [{"key": {"element": "Earth"}}, {"key": {"element": "Water"}}]
        )

    # XXX: This is *wrong*, I think
    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_for_fake_group_01c(self):
        await self.assert_query_result(
            r'''
            with GR := (
                for x in {'Earth', 'Water'} union {
                    key := {element := x},
                    elements := (select Card filter .element = x),
                }
            )
            select GR {
              key: {element},
              elements: {name},
            }
            order by .key.element;
            ''',
            [
                {
                    "key": {"element": "Earth"},
                    "elements": tb.bag([{"name": "Dwarf"}, {"name": "Golem"}]),
                },
                {
                    "key": {"element": "Water"},
                    "elements": tb.bag([
                        {"name": "Bog monster"}, {"name": "Giant turtle"},
                    ]),
                },
            ]
        )

    async def test_edgeql_for_fake_group_02(self):
        await self.assert_query_result(
            r'''
            with GR := (for x in {'Earth', 'Water'} union {key := x})
            select GR { key }
            order by .key;
            ''',
            [{"key": "Earth"}, {"key": "Water"}]
        )

    async def test_edgeql_for_tuple_optional_01(self):
        await self.assert_query_result(
            r'''
                for user in User union (
                  ((select (1,) filter false) ?? (2,)).0
                );
            ''',
            [2, 2, 2, 2],
        )

        await self.assert_query_result(
            r'''
                for user in User union (
                  ((select (1,) filter user.name = 'Alice') ?? (2,)).0
                );
            ''',
            tb.bag([1, 2, 2, 2]),
        )

    async def test_edgeql_for_optional_01(self):
        # Lol FOR OPTIONAL doesn't work for object-type iterators
        # but it does work for 1-ary tuples
        await self.assert_query_result(
            r'''
                for optional x in
                    ((select User filter .name = 'George'),)
                union x.0.deck_cost ?? 0;
            ''',
            [0],
        )

        await self.assert_query_result(
            r'''
                for optional x in
                    ((select User filter .name = 'George'),)
                union x.0
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                for optional x in
                    ((select User filter .name = 'George'),)
                union x
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                for optional x in
                    ((select User filter .name = 'Alice'),)
                union x.0.deck_cost ?? 0;
            ''',
            [11],
        )

        await self.assert_query_result(
            r'''
                for optional x in
                    ((select User filter .name = 'George'),)
                union (insert Award { name := "Participation" })
            ''',
            [{}],
        )

        await self.assert_query_result(
            r'''
                for optional x in (<bool>{})
                union (insert Award { name := "Participation!" })
            ''',
            [{}],
        )

        await self.assert_query_result(
            r'''
                for user in (select User filter .name = 'Alice') union (
                  for optional x in (<Card>{},) union (
                    1
                  )
                );
            ''',
            [1],
        )

        await self.assert_query_result(
            r'''
                for user in (select User filter .name = 'Alice') union (
                  for optional x in (<Card>{},) union (
                    user.name
                  )
                );
            ''',
            ['Alice'],
        )

        await self.assert_query_result(
            r'''
                for user in (select User filter .name = 'Alice') union (
                  for optional x in (<Card>{},) union (
                    user.name ++ (x.0.name ?? "!")
                  )
                );
            ''',
            ['Alice!'],
        )

    async def test_edgeql_for_optional_02(self):
        await self.assert_query_result(
            r'''
                for optional x in
                    (select User filter .name = 'George')
                union x.deck_cost ?? 0;
            ''',
            [0],
        )

        await self.assert_query_result(
            r'''
                for optional x in
                    (select User filter .name = 'Alice')
                union x.deck_cost ?? 0;
            ''',
            [11],
        )

        await self.assert_query_result(
            r'''
                for optional x in
                    (select User filter .name = 'George')
                union (insert Award { name := "Participation" })
            ''',
            [{}],
        )

    async def test_edgeql_for_optional_03(self):
        Q = '''
        for dummy in "1"
        for optional x in (delete Card filter .name = 'Yolanda Swaggins')
        select x.cost ?? 420;
        '''

        await self.assert_query_result(
            Q,
            [420],
        )

    async def test_edgeql_for_lprop_01(self):
        await self.assert_query_result(
            '''
            SELECT User {
                cards := (
                    SELECT (FOR d IN .deck SELECT (d.name, d@count))
                    ORDER BY .0
                ),
            }
            filter .name = 'Carol';
            ''',
            [
                {
                    "cards": [
                        ["Bog monster", 3],
                        ["Djinn", 1],
                        ["Dwarf", 4],
                        ["Giant eagle", 3],
                        ["Giant turtle", 2],
                        ["Golem", 2],
                        ["Sprite", 4]
                    ]
                }
            ]
        )

        await self.assert_query_result(
            '''
            SELECT User {
                cards := (
                    SELECT (FOR d IN .deck[is SpecialCard]
                            SELECT (d.name, d@count))
                    ORDER BY .0
                ),
            }
            filter .name = 'Carol';
            ''',
            [
                {
                    "cards": [
                        ["Djinn", 1],
                    ]
                }
            ]
        )

    async def test_edgeql_for_lprop_02(self):
        await self.assert_query_result(
            '''
            SELECT Card {
                users := (
                    SELECT (FOR u IN .<deck[is User] SELECT (u.name, u@count))
                    ORDER BY .0
                ),
            }
            filter .name = 'Dragon'
            ''',
            [{"users": [["Alice", 2], ["Dave", 1]]}],
        )

        await self.assert_query_result(
            '''
            SELECT Card {
                users := (
                    SELECT (FOR u IN .owners SELECT (u.name, u@count))
                    ORDER BY .0
                ),
            }
            filter .name = 'Dragon'
            ''',
            [{"users": [["Alice", 2], ["Dave", 1]]}],
        )

    async def test_edgeql_for_lprop_03(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            "",
        ):
            await self.con.query('''
                FOR d IN User.deck SELECT (d.name, d@count);
            ''')
