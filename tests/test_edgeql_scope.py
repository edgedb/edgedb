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

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLScope(tb.QueryTestCase):
    '''This tests the scoping rules.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.edgeql')

    async def test_edgeql_scope_sort_01(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    A := {1, 2},
                    U := (SELECT User FILTER User.name IN {'Alice', 'Bob'})
                SELECT _ := (U{name}, A)
                # specifically test the ORDER clause
                ORDER BY _.1 THEN _.0.name DESC;
            ''',
            [
                [{'name': 'Bob'}, 1],
                [{'name': 'Alice'}, 1],
                [{'name': 'Bob'}, 2],
                [{'name': 'Alice'}, 2],
            ]
        )

    async def test_edgeql_scope_tuple_01(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    A := {1, 2}
                SELECT _ := (User{name, a := A}, A)
                ORDER BY _.1 THEN _.0.name;
            ''',
            [
                [{'a': 1, 'name': 'Alice'}, 1],
                [{'a': 1, 'name': 'Bob'}, 1],
                [{'a': 1, 'name': 'Carol'}, 1],
                [{'a': 1, 'name': 'Dave'}, 1],
                [{'a': 2, 'name': 'Alice'}, 2],
                [{'a': 2, 'name': 'Bob'}, 2],
                [{'a': 2, 'name': 'Carol'}, 2],
                [{'a': 2, 'name': 'Dave'}, 2],
            ]
        )

    async def test_edgeql_scope_tuple_02(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    A := {1, 2}
                SELECT _ := (A, User{name, a := A})
                ORDER BY _.0 THEN _.1.name;
            ''',
            [
                [1, {'a': 1, 'name': 'Alice'}],
                [1, {'a': 1, 'name': 'Bob'}],
                [1, {'a': 1, 'name': 'Carol'}],
                [1, {'a': 1, 'name': 'Dave'}],
                [2, {'a': 2, 'name': 'Alice'}],
                [2, {'a': 2, 'name': 'Bob'}],
                [2, {'a': 2, 'name': 'Carol'}],
                [2, {'a': 2, 'name': 'Dave'}],
            ]
        )

    async def test_edgeql_scope_tuple_03(self):
        # get the User names and ids
        res = await self.con.fetchall(r'''
            WITH MODULE test
            SELECT User {
                name,
                id
            }
            ORDER BY User.name;
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT _ := (User { name }, User { id })
                ORDER BY _.0.name;
            ''',
            [
                [{'name': user.name}, {'id': str(user.id)}]
                for user in res
            ]
        )

    async def test_edgeql_scope_tuple_04(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT _ := (
                    # User.friends is a common path, so it refers to the
                    # SAME object in both tuple elements. In particular
                    # that means that in the User shape there will always
                    # be a single object appearing in friends link
                    # (although it's a ** link).
                    User {
                        name,
                        friends: {
                            @nickname
                        }
                    },
                    User.friends {name}
                )
                ORDER BY _.0.name THEN _.1.name;
            ''',
            [
                [
                    {
                        'name': 'Alice',
                        'friends': [{'@nickname': 'Swampy'}],
                    },
                    {
                        'name': 'Bob',
                    },
                ],
                [
                    {
                        'name': 'Alice',
                        'friends': [{'@nickname': 'Firefighter'}],
                    },
                    {
                        'name': 'Carol',
                    },
                ],
                [
                    {
                        'name': 'Alice',
                        'friends': [{'@nickname': 'Grumpy'}],
                    },
                    {
                        'name': 'Dave',
                    },
                ],
                [
                    {
                        'name': 'Dave',
                        'friends': [{'@nickname': None}],
                    },
                    {
                        'name': 'Bob',
                    },
                ],
            ]
        )

    async def test_edgeql_scope_tuple_05(self):
        await self.assert_query_result(
            r'''
                # Same as above, but with a computable instead of real
                # "friends"
                WITH MODULE test
                SELECT _ := (
                    User {
                        name,
                        fr := User.friends {
                            @nickname
                        }
                    },
                    User.friends {name}
                )
                ORDER BY _.0.name THEN _.1.name;
            ''',
            [
                [
                    {
                        'name': 'Alice',
                        'fr': {'@nickname': 'Swampy'},
                    },
                    {
                        'name': 'Bob',
                    },
                ],
                [
                    {
                        'name': 'Alice',
                        'fr': {'@nickname': 'Firefighter'},
                    },
                    {
                        'name': 'Carol',
                    },
                ],
                [
                    {
                        'name': 'Alice',
                        'fr': {'@nickname': 'Grumpy'},
                    },
                    {
                        'name': 'Dave',
                    },
                ],
                [
                    {
                        'name': 'Dave',
                        'fr': {'@nickname': None},
                    },
                    {
                        'name': 'Bob',
                    },
                ],
            ]
        )

    async def test_edgeql_scope_tuple_06(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    U2 := User
                SELECT x := (
                    User {name, foo := U2 {name}},
                    U2 { name }
                )
                FILTER x.1.name = 'Alice'
                ORDER BY x.0.name THEN x.1.name;
            ''',
            [
                [
                    {
                        'name': 'Alice',
                        'foo': {'name': 'Alice'},
                    },
                    {
                        'name': 'Alice',
                    },
                ],
                [
                    {
                        'name': 'Bob',
                        'foo': {'name': 'Alice'},
                    },
                    {
                        'name': 'Alice',
                    },
                ],
                [
                    {
                        'name': 'Carol',
                        'foo': {'name': 'Alice'},
                    },
                    {
                        'name': 'Alice',
                    },
                ],
                [
                    {
                        'name': 'Dave',
                        'foo': {'name': 'Alice'},
                    },
                    {
                        'name': 'Alice',
                    },
                ],
            ]
        )

    async def test_edgeql_scope_tuple_07(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    foo := (
                        # this is the same as enclosing User
                        WITH U2 := User
                        SELECT U2 {name} ORDER BY U2.name
                    )
                }
                ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'foo': {'name': 'Alice'},
                },
                {
                    'name': 'Bob',
                    'foo': {'name': 'Bob'},
                },
                {
                    'name': 'Carol',
                    'foo': {'name': 'Carol'},
                },
                {
                    'name': 'Dave',
                    'foo': {'name': 'Dave'},
                },
            ]
        )

    async def test_edgeql_scope_tuple_08(self):
        await self.assert_query_result(
            r'''
                # compare to test_edgeql_scope_filter_03 to see how it
                # works out without tuples
                WITH
                    MODULE test,
                    U2 := User
                SELECT (
                    User {
                        name,
                        friends_of_others := (
                            SELECT U2.friends {name}
                            FILTER
                                # not me
                                U2.friends != User
                                AND
                                # not one of my friends
                                U2.friends NOT IN User.friends
                            ORDER BY U2.friends.name
                        )
                    },
                    U2.friends {
                        name
                    }
                )
                FILTER U2.friends.name = 'Bob'
                ORDER BY User.name THEN U2.friends.name;
            ''',
            [
                [
                    {
                        'name': 'Alice',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    {
                        'name': 'Bob',
                    }
                ],
                [
                    {
                        'name': 'Bob',
                        'friends_of_others': None,  # this is Bob
                    },
                    {
                        'name': 'Bob',
                    }
                ],
                [
                    {
                        'name': 'Carol',
                        'friends_of_others': {'name': 'Bob'},
                    },
                    {
                        'name': 'Bob',
                    }
                ],
                [
                    {
                        'name': 'Dave',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    {
                        'name': 'Bob',
                    }
                ],
            ]
        )

    async def test_edgeql_scope_tuple_09(self):
        await self.assert_query_result(
            r'''
                # compare to test_edgeql_scope_filter_03 to see how it
                # works out without tuples
                WITH
                    MODULE test,
                    U2 := User
                SELECT _ := (
                    User {
                        name,
                        friends_of_others := (
                            # similar to previous test, but refactored
                            WITH F := (
                                SELECT U2.friends
                                FILTER
                                    # not me
                                    U2.friends != User
                                    AND
                                    # not one of my friends
                                    U2.friends NOT IN User.friends
                            )
                            SELECT F {name}
                            ORDER BY F.name
                        )
                    },
                    U2.friends {
                        name
                    }
                )
                FILTER _.1.name = 'Bob'
                ORDER BY _.0.name THEN _.1;
            ''',
            [
                [
                    {
                        'name': 'Alice',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    {
                        'name': 'Bob',
                    },
                ],
                [
                    {
                        'name': 'Bob',
                        'friends_of_others': None,  # this is Bob
                    },
                    {
                        'name': 'Bob',
                    },
                ],
                [
                    {
                        'name': 'Carol',
                        'friends_of_others': {'name': 'Bob'},
                    },
                    {
                        'name': 'Bob',
                    },
                ],
                [
                    {
                        'name': 'Dave',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    {
                        'name': 'Bob',
                    },
                ],
            ]
        )

    async def test_edgeql_scope_tuple_10(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (User.name, User.deck_cost, count(User.deck),
                        User.deck_cost / count(User.deck))
                ORDER BY User.name;
            ''',
            [
                ['Alice', 11, 4, 2.75],
                ['Bob', 9, 4, 2.25],
                ['Carol', 16, 7, 2.28571428571429],
                ['Dave', 20, 7, 2.85714285714286],
            ],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                # in the below expression User.friends is the
                # longest common prefix, so we know that for
                # each friend, the average cost will be
                # calculated.
                SELECT User.friends.deck_cost / count(User.friends.deck)
                ORDER BY User.friends.name;
            ''',
            [
                2.25,                # Bob (friend of Alice and Dave)
                2.28571428571429,    # Carol
                2.85714285714286,    # Dave
            ],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                # in the below expression User.friends is the
                # longest common prefix, so we know that for
                # each friend, the average cost will be
                # calculated.
                SELECT User.friends.deck_cost / count(User.friends.deck)
                FILTER User.friends.name = 'Bob';
            ''',
            [2.25],
        )

    async def test_edgeql_scope_tuple_11(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT x := (
                    Card {
                        name,
                        percent_cost := (
                            SELECT <int64>(100 * Card.cost /
                                           Card.<deck.deck_cost)
                        ),
                    },
                    Card.<deck { name }
                )
                ORDER BY x.1.name THEN x.0.name;
            ''',
            [
                [{'name': 'Bog monster', 'percent_cost': 18},
                 {'name': 'Alice'}],
                [{'name': 'Dragon', 'percent_cost': 45},
                 {'name': 'Alice'}],
                [{'name': 'Giant turtle', 'percent_cost': 27},
                 {'name': 'Alice'}],
                [{'name': 'Imp', 'percent_cost': 9},
                 {'name': 'Alice'}],

                [{'name': 'Bog monster', 'percent_cost': 22},
                 {'name': 'Bob'}],
                [{'name': 'Dwarf', 'percent_cost': 11},
                 {'name': 'Bob'}],
                [{'name': 'Giant turtle', 'percent_cost': 33},
                 {'name': 'Bob'}],
                [{'name': 'Golem', 'percent_cost': 33},
                 {'name': 'Bob'}],

                [{'name': 'Bog monster', 'percent_cost': 12},
                 {'name': 'Carol'}],
                [{'name': 'Djinn', 'percent_cost': 25},
                 {'name': 'Carol'}],
                [{'name': 'Dwarf', 'percent_cost': 6},
                 {'name': 'Carol'}],
                [{'name': 'Giant eagle', 'percent_cost': 12},
                 {'name': 'Carol'}],
                [{'name': 'Giant turtle', 'percent_cost': 19},
                 {'name': 'Carol'}],
                [{'name': 'Golem', 'percent_cost': 19},
                 {'name': 'Carol'}],
                [{'name': 'Sprite', 'percent_cost': 6},
                 {'name': 'Carol'}],

                [{'name': 'Bog monster', 'percent_cost': 10},
                 {'name': 'Dave'}],
                [{'name': 'Djinn', 'percent_cost': 20},
                 {'name': 'Dave'}],
                [{'name': 'Dragon', 'percent_cost': 25},
                 {'name': 'Dave'}],
                [{'name': 'Giant eagle', 'percent_cost': 10},
                 {'name': 'Dave'}],
                [{'name': 'Giant turtle', 'percent_cost': 15},
                 {'name': 'Dave'}],
                [{'name': 'Golem', 'percent_cost': 15},
                 {'name': 'Dave'}],
                [{'name': 'Sprite', 'percent_cost': 5},
                 {'name': 'Dave'}],
            ]
        )

    async def test_edgeql_scope_tuple_12(self):
        await self.assert_query_result(
            r'''
                # this is similar to test_edgeql_scope_tuple_04
                WITH MODULE test
                SELECT _ := (
                    # User.friends is a common path, so it refers to the
                    # SAME object in both tuple elements. In particular
                    # that means that in the User shape there will always
                    # be a single object appearing in friends link
                    # (although it's a ** link).
                    User {
                        name,
                        friends: {
                            name
                        }
                    },
                    User.friends@nickname
                )
                ORDER BY _.0.name THEN _.1;
            ''',
            [
                # the only User who has nicknames for friends is Alice
                [
                    {'name': 'Alice', 'friends': [{'name': 'Carol'}]},
                    'Firefighter'
                ],
                [
                    {'name': 'Alice', 'friends': [{'name': 'Dave'}]},
                    'Grumpy'
                ],
                [
                    {'name': 'Alice', 'friends': [{'name': 'Bob'}]},
                    'Swampy'
                ],
            ]
        )

    @test.xfail('''
        The result is *almost* correct, but oddly "l" is not a
        singleton, even though it's equal to a tuple element, which
        should be a singleton by definition.
    ''')
    async def test_edgeql_scope_tuple_13(self):
        # Test that the tuple elements are interpreted as singletons.
        await self.assert_query_result(
            r"""
            WITH
                MODULE test,
                letter := {'A', 'B'},
                tup := (
                    letter,
                    (
                        SELECT User
                        FILTER .name[0] = letter
                    )
                )
            SELECT _ := tup.1 {
                name,
                l := tup.0,
            }
            ORDER BY .name;
            """,
            [
                {'name': 'Alice', 'l': 'A'},
                {'name': 'Bob', 'l': 'B'}
            ]
        )

    async def test_edgeql_scope_filter_01(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    U2 := User
                SELECT User {
                    name,
                    foo := (SELECT U2 {name} ORDER BY U2.name)
                }
                # the FILTER clause is irrelevant because it's in a
                # parallel scope to the other mentions of U2
                FILTER U2.name = 'Alice'
                ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Bob',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Carol',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Dave',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
            ]
        )

    async def test_edgeql_scope_filter_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User.friends {name}
                FILTER User.friends NOT IN <Object>{}
                ORDER BY User.friends.name;
            ''',
            [
                {'name': 'Bob'},
                {'name': 'Carol'},
                {'name': 'Dave'},
            ]
        )

    async def test_edgeql_scope_filter_03(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    U2 := User
                SELECT User {
                    name,
                    friends_of_others := (
                        SELECT U2.friends {name}
                        FILTER
                            # not me
                            U2.friends != User
                            AND
                            # not one of my friends
                            U2.friends NOT IN User.friends
                        ORDER BY U2.friends.name
                    )
                }
                ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'friends_of_others': [],
                },
                {
                    'name': 'Bob',
                    'friends_of_others': [
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Carol',
                    'friends_of_others': [
                        {'name': 'Bob'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Dave',
                    'friends_of_others': [
                        {'name': 'Carol'},
                    ],
                }
            ]
        )

    async def test_edgeql_scope_filter_04(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    friends: {
                        name
                    } ORDER BY User.friends.name
                }
                FILTER User.friends.name = 'Carol';
            ''',
            [
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
            ]
        )

    async def test_edgeql_scope_filter_05(self):
        await self.assert_query_result(
            r'''
                # User.name is wrapped into a SELECT, so it's a SET OF
                # w.r.t FILTER
                WITH MODULE test
                SELECT (SELECT User.name)
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    async def test_edgeql_scope_filter_06(self):
        await self.assert_query_result(
            r'''
                # User is wrapped into a SELECT, so it's a SET OF
                # w.r.t FILTER
                WITH MODULE test
                SELECT (SELECT User).name
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    async def test_edgeql_scope_filter_07(self):
        await self.assert_query_result(
            r'''
                # User.name is a SET OF argument of ??, so it's unaffected
                # by the FILTER
                WITH MODULE test
                SELECT (<str>{} ?? User.name)
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    async def test_edgeql_scope_filter_08(self):
        await self.assert_query_result(
            r'''
                # User is a SET OF argument of ??, so it's unaffected
                # by the FILTER
                WITH MODULE test
                SELECT (<User>{} ?? User).name
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    async def test_edgeql_scope_order_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    friends: {
                        name
                    } ORDER BY User.friends.name
                }
                ORDER BY (
                    (SELECT User.friends
                     FILTER User.friends@nickname = 'Firefighter'
                     LIMIT 1).name
                ) EMPTY FIRST
                THEN User.name;
            ''',
            [
                {
                    'name': 'Bob',
                    'friends': [],
                },
                {
                    'name': 'Carol',
                    'friends': [],
                },
                {
                    'name': 'Dave',
                    'friends': [
                        {'name': 'Bob'},
                    ],
                },
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                }
            ]
        )

    # NOTE: LIMIT tests are largely identical to OFFSET tests, any
    # time there is a new OFFSET test, there should be a corresponding
    # LIMIT one.
    async def test_edgeql_scope_offset_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    friends: {
                        name
                    } ORDER BY User.friends.name
                }
                ORDER BY User.name
                # the OFFSET clause is in a sibling scope to SELECT, so
                # the User.friends are completely independent in them.
                OFFSET (# NOTE: effectively it's OFFSET 2
                        #
                        # Select the average card value (rounded to an
                        # int) for the user who is someone's friend AND
                        # nicknamed 'Firefighter':
                        # - the user happens to be Carol
                        # - her average deck cost is 2
                        #   (see test_edgeql_scope_tuple_08)
                        WITH
                            F := (
                                SELECT User
                                FILTER User.<friends@nickname = 'Firefighter'
                            )
                        SELECT
                            # cardinality should be inferable here:
                            # - deck_cost is a computable based on sum
                            # - count also has cardinality 1 of the return set
                            <int64>(F.deck_cost / count(F.deck))
                        LIMIT 1
                    );
            ''',
            [
                {
                    'name': 'Carol',
                    'friends': []
                },
                {
                    'name': 'Dave',
                    'friends': [
                        {'name': 'Bob'},
                    ],
                },
            ]
        )

    async def test_edgeql_scope_offset_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    friends: {
                        name
                    }  # User.friends is scoped from the enclosing shape
                    ORDER BY User.friends.name
                    OFFSET (count(User.friends) - 1)
                            IF EXISTS User.friends ELSE 0
                    # the above is equivalent to getting the last friend,
                    # ordered by name
                }
                ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Bob',
                    'friends': []
                },
                {
                    'name': 'Carol',
                    'friends': []
                },
                {
                    'name': 'Dave',
                    'friends': [
                        {'name': 'Bob'},
                    ],
                },
            ]
        )

    async def test_edgeql_scope_limit_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    friends: {
                        name
                    } ORDER BY User.friends.name
                }
                ORDER BY User.name
                # the LIMIT clause is in a sibling scope to SELECT, so
                # the User.<friends are completely independent in them.
                LIMIT ( # NOTE: effectively it's LIMIT 2
                        #
                        # Select the average card value (rounded to an
                        # int) for the user who is someone's friend AND
                        # nicknamed 'Firefighter':
                        # - the user happens to be Carol
                        # - her average deck cost is 2
                        #   (see test_edgeql_scope_tuple_08)
                        WITH
                            F := (
                                SELECT User
                                FILTER User.<friends@nickname = 'Firefighter'
                            )
                        SELECT
                            # cardinality should be inferable here:
                            # - deck_cost is a computable based on sum
                            # - count also has cardinality 1 of the return set
                            <int64>(F.deck_cost / count(F.deck))
                        LIMIT 1
                    );
            ''',
            [
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Bob',
                    'friends': []
                },
            ]
        )

    async def test_edgeql_scope_limit_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT User {
                    name,
                    friends: {
                        name,
                        name_upper := str_upper(User.friends.name),
                    }  # User.friends is scoped from the enclosing shape
                    ORDER BY User.friends.name
                    LIMIT (count(User.friends) - 1)
                           IF EXISTS User.friends ELSE 0
                    # the above is equivalent to getting the all except
                    # last friend, ordered by name
                }
                ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Bob', 'name_upper': 'BOB'},
                        {'name': 'Carol', 'name_upper': 'CAROL'},
                    ],
                },
                {
                    'name': 'Bob',
                    'friends': [],
                },
                {
                    'name': 'Carol',
                    'friends': [],
                },
                {
                    'name': 'Dave',
                    'friends': [],
                },
            ]
        )

    async def test_edgeql_scope_nested_01(self):
        await self.assert_query_result(
            r'''
                # control query Q1
                WITH MODULE test
                SELECT Card.element ++ ' ' ++ Card.name
                FILTER Card.name > Card.element
                ORDER BY Card.name;
            ''',
            ['Air Djinn', 'Air Giant eagle', 'Earth Golem', 'Fire Imp',
             'Air Sprite']
        )

    async def test_edgeql_scope_nested_02(self):
        await self.assert_query_result(
            r'''
                # Semantically this is same as control query Q1, with lots
                # of nested views. SELECT sets up A to be the longest
                # common prefix to be iterated over, so the rest of views
                # work out because they all refer to a singleton with same
                # value as A.
                WITH
                    MODULE test,
                    A := Card
                SELECT
                    A.element ++ ' ' ++ (WITH B := A SELECT B).name
                FILTER (
                    WITH C := A
                    SELECT (
                        WITH D := C
                        SELECT D.name
                    ) > C.element
                )
                ORDER BY
                    (WITH E := A SELECT E.name);
            ''',
            ['Air Djinn', 'Air Giant eagle', 'Earth Golem', 'Fire Imp',
             'Air Sprite'],
        )

    async def test_edgeql_scope_nested_03(self):
        await self.assert_query_result(
            r'''
                # semantically same as control query Q1, with lots of
                # nested views
                WITH
                    MODULE test,
                    A := Card
                SELECT
                    A.element ++ ' ' ++ (WITH B := A SELECT B).name
                FILTER (
                    WITH C := A
                    SELECT (
                        WITH D := A
                        SELECT D.name
                    ) > C.element
                )
                ORDER BY
                    (WITH E := A SELECT E.name);
            ''',
            ['Air Djinn', 'Air Giant eagle', 'Earth Golem', 'Fire Imp',
             'Air Sprite'],
        )

    async def test_edgeql_scope_nested_05(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT
                    Card {
                        foo := Card.element ++ <str>count(Card.name)
                    }
                FILTER
                    Card.name > Card.element
                ORDER BY
                    Card.name;
            ''',
            [
                {'foo': 'Air1'},
                {'foo': 'Air1'},
                {'foo': 'Earth1'},
                {'foo': 'Fire1'},
                {'foo': 'Air1'},
            ]
        )

    async def test_edgeql_scope_nested_06(self):
        await self.assert_query_result(
            r'''
                # control query Q2
                WITH MODULE test
                # combination of element + SET OF with a common prefix
                SELECT Card.name ++ <str>count(Card.owners)
                FILTER
                    # some element filters
                    Card.name < Card.element
                    AND
                    # a SET OF filter that shares a prefix with SELECT SET
                    # OF, but is actually independent
                    count(Card.owners.friends) > 2
                ORDER BY Card.name;
            ''',
            ['Bog monster4', 'Dragon2', 'Giant turtle4']
        )

    async def test_edgeql_scope_nested_07(self):
        await self.assert_query_result(
            r'''
                # semantically same as control query Q2, with lots of
                # nested aliases
                WITH
                    MODULE test,
                    A := Card
                SELECT
                    A.name ++ (WITH B := A SELECT <str>count(B.owners))
                FILTER (
                    WITH C := A
                    SELECT (
                        WITH D := C
                        SELECT D.name
                    ) < C.element
                    AND
                    (
                        WITH E := A
                        SELECT count((WITH F := E SELECT F.owners.friends)) > 2
                    )
                )
                ORDER BY
                    (WITH E := A SELECT E.name);
            ''',
            ['Bog monster4', 'Dragon2', 'Giant turtle4']
        )

    async def test_edgeql_scope_nested_08(self):
        await self.assert_query_result(
            r'''
                # semantically same as control query Q2, with lots of
                # nested aliases, all referring to the top level alias
                WITH
                    MODULE test,
                    A := Card
                SELECT
                    A.name ++ (WITH B := A SELECT <str>count(B.owners))
                FILTER (
                    WITH C := A
                    SELECT (
                        WITH D := A
                        SELECT D.name
                    ) < C.element
                    AND
                    (
                        SELECT count((WITH F := A SELECT F.owners.friends)) > 2
                    )
                )
                ORDER BY
                    (WITH E := A SELECT E.name);
            ''',
            ['Bog monster4', 'Dragon2', 'Giant turtle4']
        )

    async def test_edgeql_scope_nested_09(self):
        await self.assert_query_result(
            r'''
                # control query Q3
                WITH MODULE test
                SELECT Card.name ++ <str>count(Card.owners);
            ''',
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'}
        )

    async def test_edgeql_scope_nested_11(self):
        await self.assert_query_result(
            r'''
                # semantically same as control query Q3, except that some
                # aliases are introduced
                WITH MODULE test
                SELECT Card.name ++
                       <str>count((WITH A := Card SELECT A).owners);
            ''',
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT Card.name ++
                       <str>count((WITH A := Card SELECT A.owners));
            ''',
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT <str>count((WITH A := Card SELECT A.owners)) ++
                       Card.name;
            ''',
            {'1Imp', '2Dragon', '4Bog monster', '4Giant turtle', '2Dwarf',
             '3Golem', '2Sprite', '2Giant eagle', '2Djinn'},
        )

    async def test_edgeql_scope_nested_12(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test
                SELECT Card {
                    name,
                    owner := (
                        SELECT User {
                            # masking a real `name` link
                            name := 'Elvis'
                        }
                        # this filter should be impossible with the new `name`
                        FILTER User.name = 'Alice'
                    )
                }
                FILTER Card.name = 'Dragon';
            ''',
            [{'name': 'Dragon', 'owner': []}]
        )

    async def test_edgeql_scope_detached_01(self):
        names = {'Alice', 'Bob', 'Carol', 'Dave'}

        await self.assert_query_result(
            r"""
                # U2 is a combination of DETACHED and non-DETACHED expression
                WITH
                    MODULE test,
                    U2 := User.name ++ DETACHED User.name
                SELECT U2 ++ U2;
            """,
            {u + u for u in
                (a + b
                    for a in names
                    for b in names)},
        )

        await self.assert_query_result(
            r"""
                # DETACHED is reused directly
                WITH MODULE test
                SELECT User.name ++ DETACHED User.name ++
                       User.name ++ DETACHED User.name;
            """,
            {a + b + a + c
                for a in names
                for b in names
                for c in names},
        )

    async def test_edgeql_scope_detached_02(self):
        # calculate some useful base expression
        names = await self.con.fetchall(r"""
            WITH MODULE test
            SELECT User.name ++ <str>count(User.deck);
        """)

        await self.assert_query_result(
            r"""
                # Let's say we need a tournament where everybody will play
                # with everybody twice.
                WITH
                    MODULE test,
                    # calculate some expression ("full" name)
                    U0 := User.name ++ <str>count(User.deck),
                    # make a copy of U0 so that we can do cross product
                    U1 := U0
                SELECT U0 ++ ' vs ' ++ U1
                # get rid of players matching themselves
                FILTER U0 != U1;
            """,
            {f'{a} vs {b}' for a in names for b in names if a != b},
        )

    async def test_edgeql_scope_detached_03(self):
        names = {'Alice', 'Bob', 'Carol', 'Dave'}

        # No good narrative here, just a bigger cross-product
        # computed in straight-forward and alternative ways.
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    # make 3 copies of User.name
                    U0 := DETACHED User.name,
                    U1 := DETACHED User.name,
                    U2 := DETACHED User.name
                SELECT User.name ++ U0 ++ U1 ++ U2;
            """,
            {a + b + c + d
                for a in names
                for b in names
                for c in names
                for d in names},
        )

        await self.assert_query_result(
            r"""
                # same thing, but building it up differently
                WITH
                    MODULE test,
                    # calculate some expression ("full" name)
                    U0 := User.name,
                    # make that expression DETACHED so that we can do
                    # cross product
                    U1 := U0,
                    # cross product of players
                    U2 := U0 ++ U1,
                    # a copy of the players cross product
                    U3 := U2
                # compute what is effectively a cross product of a cross
                # product of names (expecting 256 results)
                SELECT U2 ++ U3;
            """,
            {a + b + c + d
                for a in names
                for b in names
                for c in names
                for d in names},
        )

    async def test_edgeql_scope_detached_04(self):
        # Natural, but incorrect way of getting a bunch of friends
        # filtered by @nickname.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'User' changes the interpretation of 'User'"):
            async with self.con.transaction():
                await self.con.fetchall(r"""
                    WITH MODULE test
                    SELECT User.friends
                    FILTER User.friends@nickname = 'Firefighter';
                """)

        # The above query is illegal, but the reason why may be
        # more obvious with the equivalent query below.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'User' changes the interpretation of 'User'"):
            await self.con.fetchall(r"""
                WITH MODULE test
                SELECT User.friends
                FILTER (
                    # create an independent link target set
                    WITH F := DETACHED User.friends
                    # explicitly connect it back to our User
                    SELECT F
                    FILTER F.<friends = User
                ).friends@nickname = 'Firefighter';
                """)

    async def test_edgeql_scope_detached_05(self):
        await self.assert_query_result(
            r"""
                # Natural syntax for filtering friends based on nickname:
                WITH MODULE test
                SELECT User {
                    name,
                    friends: {
                        name
                    } FILTER @nickname = 'Firefighter'
                }
                ORDER BY .name;
            """,
            [
                {'name': 'Alice', 'friends': [{'name': 'Carol'}]},
                {'name': 'Bob', 'friends': []},
                {'name': 'Carol', 'friends': []},
                {'name': 'Dave', 'friends': []},
            ],
        )

        await self.assert_query_result(
            r"""
                # Alternative natural syntax for filtering friends based
                # on nickname:
                WITH MODULE test
                SELECT User {
                    name,
                    fr := (
                        SELECT User.friends {
                            name
                        }
                        FILTER @nickname = 'Firefighter'
                    )
                }
                ORDER BY .name;
            """,
            [
                {'name': 'Alice', 'fr': [{'name': 'Carol'}]},
                {'name': 'Bob', 'fr': []},
                {'name': 'Carol', 'fr': []},
                {'name': 'Dave', 'fr': []},
            ],
        )

        await self.assert_query_result(
            r"""
                # The above query is legal, but the reason why may be more
                # obvious with the equivalent query below.
                WITH MODULE test
                SELECT User {
                    name,
                    fr := (
                        WITH F0 := (
                            WITH F1 := DETACHED User.friends
                            SELECT F1
                            # explicitly connect it back to our User
                            FILTER .<friends = User
                        )
                        SELECT F0 {name}
                        FILTER .<friends@nickname = 'Firefighter'
                    )
                }
                ORDER BY .name;
            """,
            [
                {'name': 'Alice', 'fr': [{'name': 'Carol'}]},
                {'name': 'Bob', 'fr': []},
                {'name': 'Carol', 'fr': []},
                {'name': 'Dave', 'fr': []},
            ],
        )

    async def test_edgeql_scope_detached_06(self):
        # this is very similar to test_edgeql_scope_filter_01
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    U2 := DETACHED User
                SELECT User {
                    name,
                    foo := (SELECT U2 {name} ORDER BY U2.name)
                }
                # the FILTER clause is irrelevant because it's in a
                # parallel scope to the other mentions of U2
                FILTER U2.name = 'Alice'
                ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Bob',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Carol',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Dave',
                    'foo': [
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ],
                },
            ]
        )

    async def test_edgeql_scope_detached_07(self):
        # compare detached to regular expression
        res = await self.con.fetchall_json(r'''
            WITH MODULE test
            SELECT User {
                name,
                fire_deck := (
                    SELECT User.deck {name, element}
                    FILTER .element = 'Fire'
                    ORDER BY .name
                )
            };
        ''')
        res = json.loads(res)
        res.sort(key=lambda x: x['name'])

        await self.assert_query_result(
            r'''
                # adding a top-level DETACHED should not change anything at all
                WITH MODULE test
                SELECT DETACHED User {
                    name,
                    fire_deck := (
                        SELECT User.deck {name, element}
                        FILTER .element = 'Fire'
                        ORDER BY .name
                    )
                };
            ''',
            res,
            sort=lambda x: x['name']
        )

    async def test_edgeql_scope_detached_08(self):
        res = await self.con.fetchall_json(r'''
            WITH MODULE test
            SELECT User {
                name,
                fire_deck := (
                    SELECT User.deck {name, element}
                    FILTER .element = 'Fire'
                    ORDER BY .name
                ).name
            };
        ''')
        res = json.loads(res)
        res.sort(key=lambda x: x['name'])

        await self.assert_query_result(
            r'''
                # adding a top-level DETACHED should not change anything at all
                WITH MODULE test
                SELECT DETACHED User {
                    name,
                    fire_deck := (
                        SELECT User.deck {name, element}
                        FILTER .element = 'Fire'
                        ORDER BY .name
                    ).name
                };
            ''',
            res,
            sort=lambda x: x['name']
        )

    async def test_edgeql_scope_detached_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'only singletons are allowed'):

            async with self.con.transaction():
                await self.con.execute(r"""
                    WITH MODULE test
                    SELECT DETACHED User {name}
                    # a subtle error
                    ORDER BY User.name;
                """)

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DETACHED User {name}
                # correct usage
                ORDER BY .name;
            ''',
            [
                {'name': 'Alice'},
                {'name': 'Bob'},
                {'name': 'Carol'},
                {'name': 'Dave'},
            ]
        )

    async def test_edgeql_scope_detached_10(self):
        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    Card := (SELECT Card FILTER .name = 'Bog monster')
                # The contents of the shape will be detached, thus
                # the `Card` mentioned in the shape will be referring to
                # the set of all issues and not the one defined in the
                # WITH clause.
                SELECT
                    _ := (
                        Card,
                        DETACHED (User {
                            name,
                            fire_cards := (
                                SELECT User.deck {
                                    name,
                                    element,
                                }
                                FILTER User.deck IN Card
                                ORDER BY .name
                            ),
                        }),
                    ).1
                ORDER BY _.name;
            ''',
            [
                {
                    'name': 'Alice',
                    'fire_cards': [
                        {'name': 'Bog monster', 'element': 'Water'},
                        {'name': 'Dragon', 'element': 'Fire'},
                        {'name': 'Giant turtle', 'element': 'Water'},
                        {'name': 'Imp', 'element': 'Fire'},
                    ],
                },
                {
                    'name': 'Bob',
                    'fire_cards': [
                        {'name': 'Bog monster', 'element': 'Water'},
                        {'name': 'Dwarf', 'element': 'Earth'},
                        {'name': 'Giant turtle', 'element': 'Water'},
                        {'name': 'Golem', 'element': 'Earth'},
                    ],
                },
                {
                    'name': 'Carol',
                    'fire_cards': [
                        {'name': 'Bog monster', 'element': 'Water'},
                        {'name': 'Djinn', 'element': 'Air'},
                        {'name': 'Dwarf', 'element': 'Earth'},
                        {'name': 'Giant eagle', 'element': 'Air'},
                        {'name': 'Giant turtle', 'element': 'Water'},
                        {'name': 'Golem', 'element': 'Earth'},
                        {'name': 'Sprite', 'element': 'Air'},
                    ],
                },
                {
                    'name': 'Dave',
                    'fire_cards': [
                        {'name': 'Bog monster', 'element': 'Water'},
                        {'name': 'Djinn', 'element': 'Air'},
                        {'name': 'Dragon', 'element': 'Fire'},
                        {'name': 'Giant eagle', 'element': 'Air'},
                        {'name': 'Giant turtle', 'element': 'Water'},
                        {'name': 'Golem', 'element': 'Earth'},
                        {'name': 'Sprite', 'element': 'Air'},
                    ],
                },
            ],
        )

    async def test_edgeql_scope_union_01(self):
        await self.assert_query_result(
            r'''
                # UNION and `{...}` should create SET OF scoped operands,
                # therefore `count` should operate on the entire set
                WITH MODULE test
                SELECT len(User.name) UNION count(User);
            ''',
            [3, 4, 4, 5, 5],
            sort=True
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT {len(User.name), count(User)};
            ''',
            [3, 4, 4, 5, 5],
            sort=True
        )

    async def test_edgeql_scope_union_02(self):
        await self.assert_query_result(
            r'''
                # UNION and `{...}` should create SET OF scoped operands,
                # therefore FILTER should not be effective
                WITH MODULE test
                SELECT len(User.name)
                FILTER User.name > 'C';
            ''',
            [4, 5],
            sort=True,
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT {len(User.name)}
                FILTER User.name > 'C';
            ''',
            [3, 4, 5, 5],
            sort=True,
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT {len(User.name), count(User)}
                FILTER User.name > 'C';
            ''',
            [3, 4, 4, 5, 5],
            sort=True,
        )

    async def test_edgeql_scope_computables_01(self):
        # Test that expressions in schema link computables
        # do not leak out into the query.
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test
                SELECT x := (User.name, User.deck.name, User.deck_cost)
                FILTER x.0 = 'Alice'
                ORDER BY x.1;
            """,
            [
                ['Alice', 'Bog monster', 11],
                ['Alice', 'Dragon', 11],
                ['Alice', 'Giant turtle', 11],
                ['Alice', 'Imp', 11],
            ]
        )

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test
                SELECT x := (User.name, User.deck.name, sum(User.deck.cost))
                FILTER x.0 = 'Alice'
                ORDER BY x.1;
            """,
            [
                ['Alice', 'Bog monster', 2],
                ['Alice', 'Dragon', 5],
                ['Alice', 'Giant turtle', 3],
                ['Alice', 'Imp', 1],
            ]
        )

    async def test_edgeql_scope_computables_02(self):
        # Test that expressions in view link computables
        # do not leak out into the query.
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test
                SELECT Card {
                    name,
                    alice := (SELECT User FILTER User.name = 'Alice')
                } FILTER Card.alice != User AND Card.name = 'Bog monster';
            """,
            [
                {'name': 'Bog monster'}
            ]
        )

    async def test_edgeql_scope_computables_03(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test
                SELECT User {
                    name,
                    # a sub-shape with a computable property is ordered
                    deck: {
                        name,
                        elemental_cost,
                    } ORDER BY .name
                } FILTER .name = 'Alice';
            """,
            [
                {
                    'name': 'Alice',
                    'deck': [
                        {'name': 'Bog monster', 'elemental_cost': '2 Water'},
                        {'name': 'Dragon', 'elemental_cost': '5 Fire'},
                        {'name': 'Giant turtle', 'elemental_cost': '3 Water'},
                        {'name': 'Imp', 'elemental_cost': '1 Fire'},
                    ],
                }
            ]
        )

    async def test_edgeql_scope_computables_04(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test
                SELECT User {
                    name,
                    # a sub-shape with a computable link is ordered
                    deck: {
                        name,
                        owners: {
                            name
                        },
                    } ORDER BY .name
                } FILTER .name = 'Alice';
            """,
            [
                {
                    'name': 'Alice',
                    'deck': [
                        {
                            'name': 'Bog monster',
                            'owners': [{'name': 'Alice'}, {'name': 'Bob'},
                                       {'name': 'Carol'}, {'name': 'Dave'}],
                        },
                        {
                            'name': 'Dragon',
                            'owners': [{'name': 'Alice'}, {'name': 'Dave'}]},
                        {
                            'name': 'Giant turtle',
                            'owners': [{'name': 'Alice'}, {'name': 'Bob'},
                                       {'name': 'Carol'}, {'name': 'Dave'}],
                        },
                        {
                            'name': 'Imp',
                            'owners': [{'name': 'Alice'}],
                        },
                    ],
                }
            ]
        )

    async def test_edgeql_scope_computables_05(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test
                SELECT User {
                    name,
                    # a sub-shape with a computable derived from a
                    # computable link is ordered
                    deck: {
                        name,
                        o_name := User.deck.owners.name,
                    } ORDER BY .name
                } FILTER .name = 'Alice';
            """,
            [
                {
                    'name': 'Alice',
                    'deck': [
                        {
                            'name': 'Bog monster',
                            'o_name': {'Alice', 'Bob', 'Carol', 'Dave'},
                        },
                        {
                            'name': 'Dragon',
                            'o_name': {'Alice', 'Dave'},
                        },
                        {
                            'name': 'Giant turtle',
                            'o_name': {'Alice', 'Bob', 'Carol', 'Dave'},
                        },
                        {
                            'name': 'Imp',
                            'o_name': {'Alice'},
                        },
                    ],
                }
            ]
        )

    async def test_edgeql_scope_computables_06(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test
                SELECT User {
                    name,
                    # a sub-shape with some arbitrary computable link
                    multi x := (
                        SELECT Card { name }
                        FILTER .elemental_cost = '1 Fire'
                    )
                } FILTER .name = 'Alice';
            """,
            [
                {
                    'name': 'Alice',
                    'x': [
                        {
                            'name': 'Imp',
                        },
                    ],
                }
            ]
        )

    async def test_edgeql_scope_with_01(self):
        # Test that same symbol can be re-used in WITH block.
        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    User := User,
                    User := User,
                    User := User
                SELECT User.name;
            """,
            {'Alice', 'Bob', 'Carol', 'Dave'},
        )

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    User := Card,
                    User := User
                # this is a Card.name now
                SELECT User.name;
            """,
            {'Imp', 'Dragon', 'Bog monster', 'Giant turtle', 'Dwarf', 'Golem',
             'Sprite', 'Giant eagle', 'Djinn'},
        )

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    User := User,
                    User := User.deck,
                    User := User.element,
                    User := User
                # this is a User.deck.element now
                SELECT DISTINCT User;
            """,
            {'Fire', 'Water', 'Earth', 'Air'},
        )

    async def test_edgeql_scope_unused_with_def_01(self):

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"unused view definition: 'foo'",
                _position=48):
            await self.con.fetchall("""\
                WITH
                    foo := 1
                SELECT 1;
            """)

    async def test_edgeql_scope_unused_with_def_02(self):

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"unused view definition: 'foo'",
                _position=48):
            await self.con.fetchall("""\
                WITH
                    foo := 1
                SELECT (
                    WITH foo := 2
                    SELECT foo
                )
            """)
