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

    async def test_edgeql_scope_sort_01a(self):
        await self.assert_query_result(
            r'''
                WITH
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

    async def test_edgeql_scope_sort_01b(self):
        # Make sure it works when we need to eta-expand it also
        await self.assert_query_result(
            r'''
            SELECT assert_exists((
                WITH
                    A := {1, 2},
                    U := (SELECT User FILTER User.name IN {'Alice', 'Bob'})
                SELECT _ := (U{name}, A)
                # specifically test the ORDER clause
                ORDER BY _.1 THEN _.0.name DESC
            ));
            ''',
            [
                [{'name': 'Bob'}, 1],
                [{'name': 'Alice'}, 1],
                [{'name': 'Bob'}, 2],
                [{'name': 'Alice'}, 2],
            ]
        )

    async def test_edgeql_scope_sort_01c(self):
        # Make sure it works when we need to eta-expand it after
        # array_agg()ing it
        await self.assert_query_result(
            r'''
            SELECT assert_exists(array_agg((
                WITH
                    A := {1, 2},
                    U := (SELECT User FILTER User.name IN {'Alice', 'Bob'})
                SELECT _ := (U{name}, A)
                # specifically test the ORDER clause
                ORDER BY _.1 THEN _.0.name DESC
            )));
            ''',
            [
                [
                    [{'name': 'Bob'}, 1],
                    [{'name': 'Alice'}, 1],
                    [{'name': 'Bob'}, 2],
                    [{'name': 'Alice'}, 2],
                ]
            ]
        )

    async def test_edgeql_scope_tuple_01(self):
        await self.assert_query_result(
            r'''
                WITH
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
        res = await self.con.query(r'''
            SELECT User {
                name,
                id
            }
            ORDER BY User.name;
        ''')

        await self.assert_query_result(
            r'''
                SELECT _ := (User { name }, User { id })
                ORDER BY _.0.name;
            ''',
            [
                [{'name': user.name}, {'id': str(user.id)}]
                for user in res
            ]
        )

    async def test_edgeql_scope_tuple_04a(self):
        query = r'''
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
        '''

        res = [
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

        await self.assert_query_result(query, res)
        await self.assert_query_result(query, res, implicit_limit=100)

    async def test_edgeql_scope_tuple_04b(self):
        query = r'''
            SELECT _ := (
                User.friends {name},
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
            )
            ORDER BY _.1.name THEN _.0.name;
        '''
        res = [
            [
                {
                    'name': 'Bob',
                },
                {
                    'name': 'Alice',
                    'friends': [{'@nickname': 'Swampy'}],
                },
            ],
            [
                {
                    'name': 'Carol',
                },
                {
                    'name': 'Alice',
                    'friends': [{'@nickname': 'Firefighter'}],
                },
            ],
            [
                {
                    'name': 'Dave',
                },
                {
                    'name': 'Alice',
                    'friends': [{'@nickname': 'Grumpy'}],
                },
            ],
            [
                {
                    'name': 'Bob',
                },
                {
                    'name': 'Dave',
                    'friends': [{'@nickname': None}],
                },
            ],
        ]

        await self.assert_query_result(query, res)
        await self.assert_query_result(query, res, implicit_limit=100)

    async def test_edgeql_scope_tuple_04c(self):
        query = r'''
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
                    # We filter out one of the friends but it ought to still
                    # show up in the correlated set
                    FILTER @nickname ?!= "Firefighter"
                },
                User.friends {name}
            )
            ORDER BY _.0.name THEN _.1.name;
        '''

        res = [
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
                    'friends': [],
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

        await self.assert_query_result(query, res)
        await self.assert_query_result(query, res, implicit_limit=100)

    async def test_edgeql_scope_tuple_04d(self):
        query = r'''
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
                    } LIMIT 10
                },
                User.friends {name}
            )
            ORDER BY _.0.name THEN _.1.name;
        '''

        res = [
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
                    'name': 'Dave', 'friends': [{'@nickname': None}],
                },
                {
                    'name': 'Bob',
                },
            ],
        ]

        await self.assert_query_result(query, res)
        await self.assert_query_result(query, res, implicit_limit=100)

    async def test_edgeql_scope_tuple_04e(self):
        # Basically the same as the above sequence of things, but
        # with a computable link
        query = r'''
            SELECT _ := (
                Card {
                    name,
                    owners: {
                        name
                    }
                },
                Card.owners {name}
            )
            FILTER _.0.name = 'Sprite'
            ORDER BY _.0.name THEN _.1.name;
        '''

        res = [
            [{"name": "Sprite",
              "owners": [{"name": "Carol"}]}, {"name": "Carol"}],
            [{"name": "Sprite",
              "owners": [{"name": "Dave"}]}, {"name": "Dave"}]
        ]

        await self.assert_query_result(query, res)
        await self.assert_query_result(query, res, implicit_limit=100)

    async def test_edgeql_scope_tuple_04f(self):
        # Similar to above tests, but forcing use of eta-expansion
        query = r'''
            SELECT _ := [(
                User.friends {name},
                User {
                    name,
                    friends: {
                        @nickname
                    }
                },
            )][0]
            ORDER BY _.1.name THEN _.0.name;
        '''
        res = [
            [
                {
                    'name': 'Bob',
                },
                {
                    'name': 'Alice',
                    'friends': [{'@nickname': 'Swampy'}],
                },
            ],
            [
                {
                    'name': 'Carol',
                },
                {
                    'name': 'Alice',
                    'friends': [{'@nickname': 'Firefighter'}],
                },
            ],
            [
                {
                    'name': 'Dave',
                },
                {
                    'name': 'Alice',
                    'friends': [{'@nickname': 'Grumpy'}],
                },
            ],
            [
                {
                    'name': 'Bob',
                },
                {
                    'name': 'Dave',
                    'friends': [{'@nickname': None}],
                },
            ],
        ]

        await self.assert_query_result(query, res)
        await self.assert_query_result(query, res, implicit_limit=100)

    async def test_edgeql_scope_tuple_05a(self):
        await self.assert_query_result(
            r'''
                # Same as above, but with a computable instead of real
                # "friends"
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

    async def test_edgeql_scope_tuple_05b(self):
        # Similar to above tests, but forcing use of eta-expansion
        await self.assert_query_result(
            r'''
                # Same as above, but with a computable instead of real
                # "friends"
                SELECT _ := [(
                    User {
                        name,
                        fr := User.friends {
                            @nickname
                        }
                    },
                    User.friends {name}
                )][0]
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
                SELECT x := (
                    Card {
                        name,
                        percent_cost := (
                            SELECT <int64>(100 * Card.cost /
                                           Card.<deck[IS User].deck_cost)
                        ),
                    },
                    Card.<deck[IS User] { name }
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

    async def test_edgeql_scope_tuple_13(self):
        # Test that the tuple elements are interpreted as singletons.
        await self.assert_query_result(
            r"""
            WITH
                letter := {'A', 'B'},
                tup := (
                    letter,
                    (
                        SELECT User
                        FILTER .name[0] = letter
                    )
                )
            SELECT tup.1 {
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

        await self.assert_query_result(
            r"""
            WITH
                letter := {'A', 'B'},
                tup := (
                    (
                        letter,
                        (
                            SELECT User
                            FILTER .name[0] = letter
                        ),
                    ),
                    'foo',
                )
            SELECT tup.0.1 {
                name,
                l := tup.0.0,
            }
            ORDER BY .name;
            """,
            [
                {'name': 'Alice', 'l': 'A'},
                {'name': 'Bob', 'l': 'B'}
            ]
        )

    async def test_edgeql_scope_tuple_14(self):
        # Test that the tuple elements are interpreted as singletons.

        await self.assert_query_result(
            r"""
            WITH
                letter := {'A', 'B'},
                tup := (
                    letter,
                    (
                        SELECT User
                        FILTER .name[0] = letter
                    )
                ),
                result := tup.1 {
                    l := tup.0
                },
            SELECT result {
                name,
                l,
            }
            ORDER BY .name;
            """,
            [
                {'name': 'Alice', 'l': 'A'},
                {'name': 'Bob', 'l': 'B'}
            ]
        )

    async def test_edgeql_scope_tuple_15(self):
        res = await self.con.query(r"""
            SELECT ((SELECT User {deck}), User.deck);
        """)

        # The deck shape ought to contain just the correlated element
        for row in res:
            self.assertEqual(len(row[0].deck), 1)
            self.assertEqual(row[0].deck[0].id, row[1].id)

    async def test_edgeql_scope_tuple_16(self):
        await self.assert_query_result(
            r"""
            with z := User, select ({z}.name, count(z));
            """,
            tb.bag(
                [["Alice", 4], ["Bob", 4], ["Carol", 4], ["Dave", 4]]
            ),
        )

    async def test_edgeql_scope_binding_01(self):
        await self.assert_query_result(
            r"""
            WITH
                L := (FOR name in {'Alice', 'Bob'} UNION (
                    SELECT User
                    FILTER .name = name
                )),
            SELECT _ := ((SELECT L.name), (SELECT L.name))
            ORDER BY _;
            """,
            [
                ['Alice', 'Alice'],
                ['Alice', 'Bob'],
                ['Bob', 'Alice'],
                ['Bob', 'Bob'],
            ]
        )

    async def test_edgeql_scope_binding_02a(self):
        await self.assert_query_result(
            r"""
            WITH
                name := {'Alice', 'Bob'},
                L := (name, (
                    SELECT User
                    FILTER .name = name
                )),
            SELECT _ := ((SELECT L.1.name), (SELECT L.1.name))
            ORDER BY _;
            """,
            [
                ['Alice', 'Alice'],
                ['Alice', 'Bob'],
                ['Bob', 'Alice'],
                ['Bob', 'Bob'],
            ]
        )

    async def test_edgeql_scope_binding_02b(self):
        await self.assert_query_result(
            r"""
            WITH
                name := {'Alice', 'Bob'},
                L := ((
                    SELECT User
                    FILTER .name = name
                ), name),
            SELECT _ := ((SELECT L.0.name), (SELECT L.0.name))
            ORDER BY _;
            """,
            [
                ['Alice', 'Alice'],
                ['Alice', 'Bob'],
                ['Bob', 'Alice'],
                ['Bob', 'Bob'],
            ]
        )

    async def test_edgeql_scope_binding_03(self):
        await self.assert_query_result(
            r"""
            WITH
                name := {'Alice', 'Bob'},
                L := (name, (
                    SELECT User
                    FILTER .name = name
                )).1,
            SELECT _ := ((SELECT L.name), (SELECT L.name))
            ORDER BY _;
            """,
            [
                ['Alice', 'Alice'],
                ['Alice', 'Bob'],
                ['Bob', 'Alice'],
                ['Bob', 'Bob'],
            ]
        )

    async def test_edgeql_scope_binding_04(self):
        await self.assert_query_result(
            r"""
            WITH Y := (FOR x IN {1, 2} UNION (x + 1)),
            SELECT _ := ((SELECT Y), (SELECT Y))
            ORDER BY _;
            """,
            [[2, 2], [2, 3], [3, 2], [3, 3]]
        )

    async def test_edgeql_scope_binding_05(self):
        await self.assert_query_result(
            r"""
            WITH X := {1, 2},
                 Y := (X, X+1).1,
            SELECT _ := ((SELECT Y), (SELECT Y))
            ORDER BY _;
            """,
            [[2, 2], [2, 3], [3, 2], [3, 3]]
        )

    async def test_edgeql_scope_binding_06(self):
        await self.assert_query_result(
            r"""
            SELECT {
                lol := (
                    WITH L := (FOR name in {'Alice', 'Bob'} UNION (
                        SELECT User
                        FILTER .name = name
                    )),
                    SELECT _ := ((SELECT L.name), (SELECT L.name))
                    ORDER BY _
                )
            };
            """,
            [
                {
                    "lol": [
                        ["Alice", "Alice"],
                        ["Alice", "Bob"],
                        ["Bob", "Alice"],
                        ["Bob", "Bob"]
                    ]
                }
            ]
        )

    async def test_edgeql_scope_binding_07(self):
        await self.assert_query_result(
            r"""
            SELECT {
                lol := (
                    WITH Y := (FOR x IN {1, 2} UNION (x + 1)),
                    SELECT _ := ((SELECT Y), (SELECT Y))
                    ORDER BY _
                )
            };
            """,
            [{"lol": [[2, 2], [2, 3], [3, 2], [3, 3]]}]
        )

    async def test_edgeql_scope_with_subquery_01(self):
        await self.assert_query_result(
            r"""
                SELECT count((
                    Card.name,
                    (WITH X := (SELECT Card) SELECT X.name),
                ));
            """,
            [9],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_filter_01(self):
        await self.assert_query_result(
            r'''
                WITH
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_filter_04(self):
        await self.assert_query_result(
            r'''
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_filter_05(self):
        await self.assert_query_result(
            r'''
                # User.name is wrapped into a SELECT, so it's a SET OF
                # w.r.t FILTER
                SELECT (SELECT User.name)
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_filter_06(self):
        await self.assert_query_result(
            r'''
                # User is wrapped into a SELECT, so it's a SET OF
                # w.r.t FILTER
                SELECT (SELECT User).name
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_filter_07(self):
        await self.assert_query_result(
            r'''
                # User.name is a SET OF argument of ??, so it's unaffected
                # by the FILTER
                SELECT (<str>{} ?? User.name)
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_filter_08(self):
        await self.assert_query_result(
            r'''
                # User is a SET OF argument of ??, so it's unaffected
                # by the FILTER
                SELECT (<User>{} ?? User).name
                FILTER User.name = 'Alice';
            ''',
            {'Alice', 'Bob', 'Carol', 'Dave'}
        )

    async def test_edgeql_scope_order_01(self):
        await self.assert_query_result(
            r'''
                SELECT User {
                    name,
                    friends: {
                        name
                    } ORDER BY User.friends.name
                }
                ORDER BY (
                    assert_single((
                        SELECT User.friends FILTER @nickname = 'Firefighter'
                    )).name
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
    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_offset_01(self):
        await self.assert_query_result(
            r'''
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
                                FILTER
                                    User.<friends[IS User]@nickname
                                    = 'Firefighter'
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_limit_01(self):
        await self.assert_query_result(
            r'''
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
                                FILTER
                                    User.<friends[IS User]@nickname
                                    = 'Firefighter'
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
                # of nested shapes. SELECT sets up A to be the longest
                # common prefix to be iterated over, so the rest of shapes
                # work out because they all refer to a singleton with same
                # value as A.
                WITH
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
                # nested shapes
                WITH
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
                SELECT Card.name ++
                       <str>count((WITH A := Card SELECT A).owners);
            ''',
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
        )

        await self.assert_query_result(
            r'''
                SELECT Card.name ++
                       <str>count((WITH A := Card SELECT A.owners));
            ''',
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
        )

        await self.assert_query_result(
            r'''
                SELECT <str>count((WITH A := Card SELECT A.owners)) ++
                       Card.name;
            ''',
            {'1Imp', '2Dragon', '4Bog monster', '4Giant turtle', '2Dwarf',
             '3Golem', '2Sprite', '2Giant eagle', '2Djinn'},
        )

        await self.assert_query_result(
            r'''
                # semantically same as control query Q3, except that some
                # aliases are introduced
                SELECT (Card.name,
                        count((WITH A := Card SELECT A).owners));
            ''',
            [["Bog monster", 4], ["Djinn", 2], ["Dragon", 2], ["Dwarf", 2],
             ["Giant eagle", 2], ["Giant turtle", 4], ["Golem", 3],
             ["Imp", 1], ["Sprite", 2]],
            sort=True,
        )

    async def test_edgeql_scope_nested_12(self):
        await self.assert_query_result(
            r'''
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
        names = await self.con.query(r"""
            SELECT User.name ++ <str>count(User.deck);
        """)

        await self.assert_query_result(
            r"""
                # Let's say we need a tournament where everybody will play
                # with everybody twice.
                WITH
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
                await self.con.query(r"""
                    SELECT User.friends
                    FILTER User.friends@nickname = 'Firefighter';
                """)

        # The above query is illegal, but the reason why may be
        # more obvious with the equivalent query below.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'User' changes the interpretation of 'User'"):
            await self.con.query(r"""
                SELECT User.friends
                FILTER (
                    # create an independent link target set
                    WITH F := DETACHED User.friends
                    # explicitly connect it back to our User
                    SELECT F
                    FILTER F.<friends = User
                ).friends@nickname = 'Firefighter';
                """)

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_detached_05(self):
        await self.assert_query_result(
            r"""
                # Natural syntax for filtering friends based on nickname:
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
                        FILTER .<friends[IS User]@nickname = 'Firefighter'
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_detached_06(self):
        # this is very similar to test_edgeql_scope_filter_01
        await self.assert_query_result(
            r'''
                WITH
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
        res = await self.con.query_json(r'''
            SELECT User {
                name,
                fire_deck := (
                    SELECT .deck {name, element}
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
                SELECT DETACHED User {
                    name,
                    fire_deck := (
                        SELECT .deck {name, element}
                        FILTER .element = 'Fire'
                        ORDER BY .name
                    )
                };
            ''',
            res,
            sort=lambda x: x['name']
        )

    async def test_edgeql_scope_detached_08(self):
        res = await self.con.query_json(r'''
            SELECT User {
                name,
                fire_deck := (
                    SELECT .deck {name, element}
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
                SELECT DETACHED User {
                    name,
                    fire_deck := (
                        SELECT .deck {name, element}
                        FILTER .element = 'Fire'
                        ORDER BY .name
                    ).name
                };
            ''',
            res,
            sort=lambda x: x['name']
        )

        await self.assert_query_result(
            r'''
                # adding a top-level DETACHED should not change anything at all
                SELECT DETACHED User {
                    name,
                    fire_deck := (
                        SELECT .deck {name, element}
                        FILTER .element = 'Fire'
                        ORDER BY .name
                    ).name
                };
            ''',
            res,
            implicit_limit=100,
            sort=lambda x: x['name']
        )

    async def test_edgeql_scope_detached_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'only singletons are allowed'):

            async with self.con.transaction():
                await self.con.execute(r"""
                    SELECT DETACHED User {name}
                    # a subtle error
                    ORDER BY User.name;
                """)

        await self.assert_query_result(
            r'''
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

    async def test_edgeql_scope_detached_11(self):
        await self.assert_query_result(
            r"""
            SELECT _ := (User.name, { x := User.name }) ORDER BY _;
            """,
            [
                ["Alice", {"x": "Alice"}],
                ["Bob", {"x": "Bob"}],
                ["Carol", {"x": "Carol"}],
                ["Dave", {"x": "Dave"}],
            ]
        )

    async def test_edgeql_scope_detached_12(self):
        await self.assert_query_result(
            r"""
            SELECT DETACHED (User { name2 := User.name }) ORDER BY .name;
            """,
            [
                {"name2": "Alice"},
                {"name2": "Bob"},
                {"name2": "Carol"},
                {"name2": "Dave"},
            ]
        )

    async def test_edgeql_scope_detached_13(self):
        # Detached but using a partial path should still give singletons
        await self.assert_query_result(
            r"""
            SELECT (DETACHED User) { name2 := .name } ORDER BY .name;
            """,
            [
                {"name2": "Alice"},
                {"name2": "Bob"},
                {"name2": "Carol"},
                {"name2": "Dave"},
            ]
        )

    async def test_edgeql_scope_detached_14(self):
        # Detached reference to User should be an unrelated one
        await self.assert_query_result(
            r"""
            SELECT (DETACHED User) { names := User.name }
            """,
            [
                {"names": {"Alice", "Bob", "Carol", "Dave"}},
                {"names": {"Alice", "Bob", "Carol", "Dave"}},
                {"names": {"Alice", "Bob", "Carol", "Dave"}},
                {"names": {"Alice", "Bob", "Carol", "Dave"}},
            ]
        )

    async def test_edgeql_scope_union_01(self):
        await self.assert_query_result(
            r'''
                # UNION and `{...}` should create SET OF scoped operands,
                # therefore `count` should operate on the entire set
                SELECT len(User.name) UNION count(User);
            ''',
            [3, 4, 4, 5, 5],
            sort=True
        )

        await self.assert_query_result(
            r'''
                SELECT {len(User.name), count(User)};
            ''',
            [3, 4, 4, 5, 5],
            sort=True
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_union_02(self):
        await self.assert_query_result(
            r'''
                # UNION and `{...}` should create SET OF scoped operands,
                # therefore FILTER should not be effective
                SELECT len(User.name)
                FILTER User.name > 'C';
            ''',
            [4, 5],
            sort=True,
        )

        await self.assert_query_result(
            r'''
                SELECT {len(User.name)}
                FILTER User.name > 'C';
            ''',
            [3, 4, 5, 5],
            sort=True,
        )

        await self.assert_query_result(
            r'''
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_computables_02(self):
        # Test that expressions in link computables
        # of the type variant do not leak out into the query.
        await self.assert_query_result(
            r"""
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
                SELECT User {
                    name,
                    # a sub-shape with a computable link is ordered
                    deck: {
                        name,
                        owners: {
                            name
                        } ORDER BY .name,
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

    async def test_edgeql_scope_computables_07a(self):
        await self.assert_query_result(
            r"""
                WITH U := User { cards := .deck },
                SELECT count((U.cards.name, U.cards.cost));
            """,
            [9],
        )

    async def test_edgeql_scope_computables_07b(self):
        await self.assert_query_result(
            r"""
                WITH U := User { cards := Card },
                SELECT count((U.cards.name, U.cards.cost));
            """,
            [9],
        )

    async def test_edgeql_scope_computables_07c(self):
        await self.assert_query_result(
            r"""
                WITH U := (SELECT User { cards := Card }
                           FILTER .name = "Phil"),
                SELECT count((U.cards.name, U.cards.cost));
            """,
            [0],
        )

    async def test_edgeql_scope_computables_08(self):
        await self.assert_query_result(
            r"""
                SELECT count((Card.owners.name, Card.owners.deck_cost));
            """,
            [4],
        )

    async def test_edgeql_scope_computables_09a(self):
        await self.assert_query_result(
            r"""
                WITH U := User {
                        unowned := (SELECT Card FILTER Card NOT IN User.deck)
                    },
                SELECT _ := U.unowned.name ORDER BY _;
            """,
            [
                'Djinn', 'Dragon', 'Dwarf', 'Giant eagle',
                'Golem', 'Imp', 'Sprite',
            ],
        )

    async def test_edgeql_scope_computables_09b(self):
        await self.assert_query_result(
            r"""
                WITH U := (SELECT User {
                        unowned := (SELECT Card FILTER Card NOT IN User.deck)
                    } FILTER .name IN {'Carol', 'Dave'}),
                SELECT _ := U.unowned.name ORDER BY _;
            """,
            [
                'Dragon', 'Dwarf', 'Imp',
            ],
        )

    async def test_edgeql_scope_computables_09c(self):
        await self.assert_query_result(
            r"""
                WITH U := (SELECT User {
                        unowned := (SELECT Card FILTER Card NOT IN User.deck)
                    } FILTER .name IN {'Carol', 'Dave'}),
                SELECT _ := (U.unowned.name, U.unowned.cost) ORDER BY _;
            """,
            [
                ['Dragon', 5], ['Dwarf', 1], ['Imp', 1],
            ],
        )

    async def test_edgeql_scope_computables_11a(self):
        await self.assert_query_result(
            r"""
                WITH U := (SELECT User {
                        deck: {name, a := Award},
                    }),
                SELECT count((U.deck.a.name, U.deck.a.id, U.deck.name));
            """,
            [27],
            implicit_limit=100,
        )

    async def test_edgeql_scope_computables_11b(self):
        await self.assert_query_result(
            r"""
                WITH U := (SELECT User {
                        cards := .deck {name, a := Award},
                    }),
                SELECT count((U.cards.a.name, U.cards.a.id, U.cards.name));
            """,
            [27],
        )

    async def test_edgeql_scope_computables_11c(self):
        # ... make sure we output legit objects in this case
        await self.assert_query_result(
            r"""
                WITH U := (SELECT User {
                        cards := .deck {name, a := Award},
                    }),
                SELECT (U.cards.a.name, U.cards.a.id, U.cards) LIMIT 1;
            """,
            [
                [str, str, {"id": str}],
            ],
        )

    async def test_edgeql_scope_computables_12(self):
        await self.assert_query_result(
            r"""
                WITH rows := Named,
                SELECT rows {
                    name, owner_count := count([IS Card].owners)
                } FILTER .name IN {'1st', 'Alice', 'Dwarf'} ORDER BY .name;
            """,
            [
                {"name": "1st", "owner_count": 0},
                {"name": "Alice", "owner_count": 0},
                {"name": "Dwarf", "owner_count": 2}
            ]
        )

        await self.assert_query_result(
            r"""
                SELECT (Named { name }) {
                    name, owner_count := count([IS Card].owners)
                } FILTER .name IN {'1st', 'Alice', 'Dwarf'} ORDER BY .name;
            """,
            [
                {"name": "1st", "owner_count": 0},
                {"name": "Alice", "owner_count": 0},
                {"name": "Dwarf", "owner_count": 2}
            ]
        )

    async def test_edgeql_scope_computables_13(self):
        await self.assert_query_result(
            r"""
                SELECT User {
                    title := (SELECT _ := User.name)
                }
                FILTER .title = 'Alice';
            """,
            [
                {"title": "Alice"},
            ]
        )

    async def test_edgeql_scope_with_01(self):
        # Test that same symbol can be re-used in WITH block.
        await self.assert_query_result(
            r"""
                WITH
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
                    User := User,
                    User := User.deck,
                    User := User.element,
                    User := User
                # this is a User.deck.element now
                SELECT DISTINCT User;
            """,
            {'Fire', 'Water', 'Earth', 'Air'},
        )

    async def test_edgeql_scope_with_02(self):
        # Test a WITH binding that depends on a previous one is still
        # independent
        await self.assert_query_result(
            r"""
                WITH
                    X := {1, 2},
                    Y := X + 1,
                SELECT _ := (X, Y) ORDER BY _;
            """,
            [[1, 2], [1, 3], [2, 2], [2, 3]]
        )

    async def test_edgeql_scope_with_03(self):
        # Test that a WITH binding used in a computable doesn't have its
        # reference to that type captured
        await self.assert_query_result(
            r"""
                WITH
                    a := count({Card.name})
                SELECT Card {name, a := a} FILTER .name = 'Imp';
            """,
            [{"name": "Imp", "a": 9}],
        )

    async def test_edgeql_scope_unused_with_def_01(self):

        await self.assert_query_result(
            """
                WITH foo := 1
                SELECT 1;
            """,
            [1]
        )

    async def test_edgeql_scope_nested_computable_01(self):
        # This is a test for a bug where the outside filter would get
        # messed up when there was a clause on a nested shape element
        # but not one on the enclosing shape element.
        #
        # So we only test that the top-level filter does the right thing,
        # since adding an ORDER BY on the todo would fail to test the
        # bug that inspired this.
        await self.assert_query_result(
            """
                SELECT User {
                    name,
                    deck: {
                        name,
                        awards: { name } ORDER BY .name
                    }
                }
                FILTER EXISTS (User.deck.awards)
                ORDER BY .name;
            """,
            [
                {'name': 'Alice'},
                {'name': 'Carol'},
                {'name': 'Dave'},
            ],
        )

    async def test_edgeql_scope_nested_computable_02(self):
        await self.assert_query_result(
            """
                SELECT User {
                    name,
                }
                FILTER EXISTS (User.deck.good_awards)
                ORDER BY .name;
            """,
            [
                {'name': 'Alice'},
                {'name': 'Dave'},
            ],
        )

    async def test_edgeql_scope_link_narrow_card_01(self):
        await self.assert_query_result(
            """
                SELECT User {
                    name,
                    specials := .deck[IS SpecialCard].name
                } ORDER BY .name;
            """,
            [
                {"name": "Alice", "specials": []},
                {"name": "Bob", "specials": []},
                {"name": "Carol", "specials": ["Djinn"]},
                {"name": "Dave", "specials": ["Djinn"]}
            ],
        )

    async def test_edgeql_scope_link_narrow_computable_01(self):
        await self.assert_query_result(
            """
                SELECT Card {
                    owners[IS Bot]: {name}
                } FILTER .name = 'Sprite'
            """,
            [
                {"owners": [{"name": "Dave"}]},
            ],
        )

    async def test_edgeql_scope_branch_01(self):
        await self.assert_query_result(
            """
                SELECT count(((SELECT User), ((User),).0));
            """,
            [4],
        )

    async def test_edgeql_scope_branch_02(self):
        await self.assert_query_result(
            """
                SELECT count((
                    (SELECT User.name),
                    ((SELECT User.name) ++ (User.name),).0,
                 ));
            """,
            [4],
        )

    async def test_edgeql_scope_branch_03(self):
        await self.assert_query_result(
            """
                SELECT count((
                    (SELECT User.name),
                    ((SELECT User.name) ++ (User.name)) ?? "uhoh",
                 ));
            """,
            [4],
        )

    async def test_edgeql_scope_computable_factoring_01(self):
        await self.assert_query_result(
            """
                WITH U := (
                        SELECT User {
                            cards := (
                                SELECT .deck {
                                    foo := .name
                                }
                            )
                        } FILTER .name = 'Dave'
                    )
                SELECT
                    count(((SELECT U.cards.foo), (SELECT U.cards.foo)));
            """,
            [49],
        )

    async def test_edgeql_scope_computable_factoring_02(self):
        await self.assert_query_result(
            """
                WITH U := (
                        SELECT User {
                            cards := (
                                SELECT .deck {
                                    foo := .name
                                }
                            )
                        } FILTER .name = 'Dave'
                    )
                SELECT
                    count(((SELECT U.cards.foo),
                          ((SELECT U.cards.foo), (U.cards.foo))))
            """,
            [7],
        )

    async def test_edgeql_scope_computable_factoring_03(self):
        await self.assert_query_result(
            """
                WITH U := (
                        SELECT User {
                            cards := (
                                SELECT .deck {
                                    foo := .name
                                }
                            )
                        } FILTER .name = 'Dave'
                    )
                SELECT
                    count(((SELECT U.cards.foo),
                          (((SELECT U.cards.foo), (U.cards.foo)),).0))
            """,
            [7],
        )

    async def test_edgeql_scope_3x_nested_materialized_01(self):
        # Having a doubly nested thing needing materialization
        # caused trouble previously.
        await self.assert_query_result(
            """
                SELECT User {
                    name,
                    avatar: {
                        name,
                        awards: {
                            name,
                            nonce := random(),
                        },
                    }
                }
                FILTER EXISTS User.avatar.awards AND User.name = 'Alice';
            """,
            [
                {
                    "avatar": {
                        "awards": tb.bag([{"name": "1st"}, {"name": "3rd"}]),
                        "name": "Dragon",
                    },
                    "name": "Alice"
                }
            ]
        )

    async def test_edgeql_scope_3x_nested_materialized_02(self):
        # Having a doubly nested thing needing materialization
        # caused trouble previously.
        await self.assert_query_result(
            """
                SELECT User {
                    name,
                    avatar: {
                        name,
                        awd := (SELECT .awards {
                            name,
                            nonce := random(),
                        } FILTER .name = '1st'),
                    }
                }
                FILTER EXISTS User.avatar.awd AND User.name = 'Alice';
            """,
            [
                {
                    "avatar": {"awd": {"name": "1st"}, "name": "Dragon"},
                    "name": "Alice"
                }
            ]
        )

    async def test_edgeql_scope_source_rebind_01(self):
        await self.assert_query_result(
            """
                WITH
                U := (SELECT User { tag := User.name }),
                A := (SELECT U FILTER .name = 'Alice'),
                SELECT A.tag;
            """,
            ["Alice"],
        )

    async def test_edgeql_scope_source_rebind_02a(self):
        await self.assert_query_result(
            """
                WITH
                U := (SELECT User { tag := (
                    SELECT User.name FILTER random() > 0) }),
                A := (SELECT U FILTER .name = 'Alice'),
                SELECT A.tag;
            """,
            ["Alice"],
        )

    @test.xerror("can't find materialized set")
    async def test_edgeql_scope_source_rebind_02b(self):
        await self.assert_query_result(
            """
                WITH
                U := (SELECT User { tag := (
                    SELECT User.name FILTER random() > 0) }),
                A := (SELECT U FILTER .name = 'Alice'),
                SELECT (A,).0.tag;
            """,
            ["Alice"],
        )

    async def test_edgeql_scope_source_rebind_03a(self):
        await self.assert_query_result(
            """
                WITH
                U := (SELECT User {
                    cards := (SELECT .deck FILTER random() > 0) }),
                A := (SELECT U FILTER .name = 'Alice')
                SELECT A {cards: {name}};
            """,
            [
                {
                    "cards": tb.bag([
                        {"name": "Imp"},
                        {"name": "Dragon"},
                        {"name": "Bog monster"},
                        {"name": "Giant turtle"}
                    ]),
                }
            ]
        )

    async def test_edgeql_scope_source_rebind_03b(self):
        await self.con.execute('''
            alter type User create access policy test
            allow all using (true)
        ''')

        await self.assert_query_result(
            """
                WITH
                U := (SELECT User {
                    cards := (SELECT .deck FILTER random() > 0) }),
                A := (SELECT U FILTER .name = 'Alice')
                SELECT A {cards: {name}};
            """,
            [
                {
                    "cards": tb.bag([
                        {"name": "Imp"},
                        {"name": "Dragon"},
                        {"name": "Bog monster"},
                        {"name": "Giant turtle"}
                    ]),
                }
            ]
        )

    async def test_edgeql_scope_source_rebind_04(self):
        await self.assert_query_result(
            """
                WITH
                U := (for c in {'A', 'B', 'C', 'D'} union (
                    SELECT User { name, single tag := c }
                    FILTER .name[0] = c and random() > 0)),
                A := (SELECT U {name} FILTER .name IN {'Alice', 'Bob'})
                     {name },
                SELECT A { name, tag };
            """,
            tb.bag([
                {"name": "Alice", "tag": "A"},
                {"name": "Bob", "tag": "B"},
            ])
        )

    async def test_edgeql_scope_source_rebind_05(self):
        await self.assert_query_result(
            """
                WITH
                U := (SELECT User {
                    cards := (SELECT .deck FILTER random() > 0
                              ORDER BY .name LIMIT 1) }),
                A := (SELECT U FILTER .name = 'Alice')
                SELECT A {cards: {name}};
            """,
            [
                {
                    "cards": {"name": "Bog monster"}
                }
            ]
        )

    async def test_edgeql_scope_ref_outer_01(self):
        await self.assert_query_result(
            """
                SELECT User {
                    cards := (SELECT (SELECT _ := .deck {
                        tag := .name ++ " - " ++ User.name,
                    }
                    ) ORDER BY .name)
                } FILTER .name = 'Alice'
            """,
            [
                {
                    "cards": [
                        {"tag": "Bog monster - Alice"},
                        {"tag": "Dragon - Alice"},
                        {"tag": "Giant turtle - Alice"},
                        {"tag": "Imp - Alice"}
                    ]
                }
            ]
        )

    async def test_edgeql_scope_ref_outer_02a(self):
        await self.assert_query_result(
            """
                SELECT User {
                    cards := (SELECT _ := .deck {
                        multi tag := User.name,
                    })
                } FILTER .name = 'Alice' AND EXISTS .cards;
            """,
            [{
                "cards": [
                    {"tag": ["Alice"]},
                    {"tag": ["Alice"]},
                    {"tag": ["Alice"]},
                    {"tag": ["Alice"]}
                ]
            }],
        )

    async def test_edgeql_scope_ref_outer_02b(self):
        await self.assert_query_result(
            """
                SELECT (for u IN User UNION u {
                    cards := (SELECT _ := .deck {
                        multi tag := u.name,
                    })
                }) FILTER .name = 'Alice' AND EXISTS .cards;
            """,
            [{
                "cards": [
                    {"tag": ["Alice"]},
                    {"tag": ["Alice"]},
                    {"tag": ["Alice"]},
                    {"tag": ["Alice"]}
                ]
            }],
        )

    async def test_edgeql_scope_ref_outer_03(self):
        await self.assert_query_result(
            """
                WITH A := (SELECT User {
                    cards := .deck {
                        name,
                        multi tag := User.name ++ " - " ++ .name,
                    }
                } FILTER .name = 'Alice'),
                SELECT _ := A.cards.tag ORDER BY _;
            """,
            [
                "Alice - Bog monster",
                "Alice - Dragon",
                "Alice - Giant turtle",
                "Alice - Imp"
            ]
        )

        await self.assert_query_result(
            """
                WITH A := (SELECT AliasedFriends {
                    cards := .deck {
                        name,
                        multi tag := AliasedFriends.name ++ " - " ++ .name,
                    }
                } FILTER .name = 'Alice'),
                SELECT _ := A.cards.tag ORDER BY _;
            """,
            [
                "Alice - Bog monster",
                "Alice - Dragon",
                "Alice - Giant turtle",
                "Alice - Imp"
            ]
        )

        await self.assert_query_result(
            """
                WITH A := (SELECT AliasedFriends {
                    cards := .deck {
                        name,
                        multi tag := (
                            SELECT _ := AliasedFriends.name ++ " - " ++ .name),
                    }
                } FILTER .name = 'Alice'),
                SELECT _ := A.cards.tag ORDER BY _;
            """,
            [
                "Alice - Bog monster",
                "Alice - Dragon",
                "Alice - Giant turtle",
                "Alice - Imp"
            ]
        )

    async def test_edgeql_scope_ref_outer_04(self):
        await self.assert_query_result(
            """
                WITH
                U := (
                    SELECT User {
                        cards := .deck {
                            name,
                            multi tag := User.name ++ " - " ++ .name,
                        }
                    }),
                A := (SELECT U FILTER .name = 'Alice'),
                SELECT _ := A.cards.tag ORDER BY _;
            """,
            [
                "Alice - Bog monster",
                "Alice - Dragon",
                "Alice - Giant turtle",
                "Alice - Imp"
            ]
        )

    async def test_edgeql_scope_ref_outer_05a(self):
        await self.assert_query_result(
            """
                WITH
                U := (
                    SELECT User {
                        cards := .deck {
                            name,
                            tag := User.name ++ " - " ++ .name,
                        }
                    }),
                A := (SELECT U FILTER .name = 'Alice'),
                B := (SELECT U FILTER .name = 'Bob'),
                SELECT { a := A.cards.tag, b := B.cards.tag };
            """,
            [
                {
                    "a": {
                        "Alice - Imp",
                        "Alice - Dragon",
                        "Alice - Bog monster",
                        "Alice - Giant turtle",
                    },
                    "b": {
                        "Bob - Bog monster",
                        "Bob - Giant turtle",
                        "Bob - Dwarf",
                        "Bob - Golem",
                    }
                }
            ]
        )

    @test.xfail("gives every user name in the output")
    async def test_edgeql_scope_ref_outer_05b(self):
        # I was trying to do something I wasn't sure of, and I tried
        # to write this variant of outer_05a to investigate.
        #
        # But then it turns out this was already broken.
        await self.assert_query_result(
            """
                WITH
                U := (
                    SELECT User {
                        cards := .deck {
                            name,
                            tag := User.name ++ " - " ++ .name,
                        }
                    }),
                A := (SELECT U FILTER .name = 'Alice'),
                B := (SELECT U FILTER .name = 'Bob'),
                SELECT { a := (A.cards,).0.tag, b := B.cards.tag };
            """,
            [
                {
                    "a": {
                        "Alice - Imp",
                        "Alice - Dragon",
                        "Alice - Bog monster",
                        "Alice - Giant turtle",
                    },
                    "b": {
                        "Bob - Bog monster",
                        "Bob - Giant turtle",
                        "Bob - Dwarf",
                        "Bob - Golem",
                    }
                }
            ]
        )

    async def test_edgeql_scope_ref_outer_06a(self):
        await self.assert_query_result(
            """
                WITH
                U := (
                    SELECT User {
                        cards := .deck {
                            name,
                            tag := User.name ++ " - " ++ .name,
                        }
                    }),
                A := (SELECT U FILTER .name = 'Alice'),
                B := (SELECT U FILTER .name = 'Bob'),
                Bc := B.cards,
                SELECT { a := A.cards.tag, b := Bc.tag };
            """,
            [
                {
                    "a": {
                        "Alice - Imp",
                        "Alice - Dragon",
                        "Alice - Bog monster",
                        "Alice - Giant turtle",
                    },
                    "b": {
                        "Bob - Bog monster",
                        "Bob - Giant turtle",
                        "Bob - Dwarf",
                        "Bob - Golem",
                    }
                }
            ]
        )

    @test.xerror("can't find materialized set")
    async def test_edgeql_scope_ref_outer_06b(self):
        # Same as above, basically, but with an extra shape on Bc
        # that causes trouble.
        await self.assert_query_result(
            """
                WITH
                U := (
                    SELECT User {
                        cards := .deck {
                            name,
                            tag := User.name ++ " - " ++ .name,
                        }
                    }),
                A := (SELECT U FILTER .name = 'Alice'),
                B := (SELECT U FILTER .name = 'Bob'),
                Bc := B.cards { tag2 := .tag ++ "!" },
                SELECT { a := A.cards.tag, b := Bc.tag2 };
            """,
            [
                {
                    "a": {
                        "Alice - Imp",
                        "Alice - Dragon",
                        "Alice - Bog monster",
                        "Alice - Giant turtle",
                    },
                    "b": {
                        "Bob - Bog monster",
                        "Bob - Giant turtle",
                        "Bob - Dwarf",
                        "Bob - Golem",
                    }
                }
            ]
        )

    async def test_edgeql_scope_ref_outer_07(self):
        baseline = await self.con.query(r'''
            WITH A := (SELECT User {
                cards := .deck {
                    name,
                    multi tag := User.name ++ " - " ++ .name,
                }
            }),
            FOR x IN A UNION (x.cards.tag);
        ''')
        self.assertEqual(len(baseline), 22)

        # A.cards gets semi-joined, so we should only get one row per card,
        # and the semantics don't tell us which it should be.
        res = await self.con.query(r'''
            WITH A := (SELECT User {
                cards := .deck {
                    name,
                    multi tag := User.name ++ " - " ++ .name,
                }
            }),
            SELECT A.cards.tag;
        ''')
        self.assertEqual(len(res), 9)
        self.assertTrue(set(res).issubset(baseline))

    async def test_edgeql_scope_ref_outer_08(self):
        await self.assert_query_result(
            """
            SELECT User { avatar := .avatar {
                tag := User.name ++ ' - ' ++ .name
            } }
            ORDER BY .avatar.tag
            """,
            [
                {"avatar": None},
                {"avatar": None},
                {"avatar": {"tag": "Alice - Dragon"}},
                {"avatar": {"tag": "Dave - Djinn"}}
            ]
        )

    async def test_edgeql_scope_ref_outer_09(self):
        await self.assert_query_result(
            """
            SELECT User { avatar := .avatar {
                tag := User.name ++ ' - ' ++ .name
            } }
            FILTER .avatar.tag != 'Dave - Djinn'
            """,
            [
                {"avatar": {"tag": "Alice - Dragon"}},
            ]
        )

    async def test_edgeql_scope_ref_side_01(self):
        await self.assert_query_result(
            """
                SELECT (
                    SELECT (
                        User,
                        (SELECT Card { name, user := User.name }
                         FILTER Card.name[0] = User.name[0]),
                    )
                ).1 { name, user } ORDER BY .name;
            """,
            [
                {"name": "Bog monster", "user": "Bob"},
                {"name": "Djinn", "user": "Dave"},
                {"name": "Dragon", "user": "Dave"},
                {"name": "Dwarf", "user": "Dave"},
            ]
        )

    async def test_edgeql_scope_ref_side_02a(self):
        await self.assert_query_result(
            """
                SELECT (
                    SELECT (
                        User,
                        (SELECT Card { name, user := (SELECT _ := User.name) }
                         FILTER Card.name[0] = User.name[0]),
                    )
                ).1 { name, user } ORDER BY .name;
            """,
            [
                {"name": "Bog monster", "user": "Bob"},
                {"name": "Djinn", "user": "Dave"},
                {"name": "Dragon", "user": "Dave"},
                {"name": "Dwarf", "user": "Dave"},
            ]
        )

    async def test_edgeql_scope_ref_side_02b(self):
        await self.assert_query_result(
            """
                SELECT (
                    SELECT (
                        (SELECT Card { name, user := (SELECT _ := User.name) }
                         FILTER Card.name[0] = User.name[0]),
                        User,
                    )
                ).0 { name, user } ORDER BY .name;
            """,
            [
                {"name": "Bog monster", "user": "Bob"},
                {"name": "Djinn", "user": "Dave"},
                {"name": "Dragon", "user": "Dave"},
                {"name": "Dwarf", "user": "Dave"},
            ]
        )

    async def test_edgeql_scope_tuple_correlate_01(self):
        await self.assert_query_result(
            """
            SELECT _ := (User {friends: {name}}, User.friends.name ?? 'n/a')
            ORDER BY _.1;
            """,
            [
                [{"friends": [{"name": "Bob"}]}, "Bob"],
                [{"friends": [{"name": "Bob"}]}, "Bob"],
                [{"friends": [{"name": "Carol"}]}, "Carol"],
                [{"friends": [{"name": "Dave"}]}, "Dave"],
                [{"friends": []}, "n/a"],
                [{"friends": []}, "n/a"]
            ]
        )

    async def test_edgeql_scope_tuple_correlate_02(self):
        await self.assert_query_result(
            """
            SELECT _ := (User {z := .friends {name}},
                         User.friends.name ?? 'n/a')
            ORDER BY _.1;
            """,
            [
                [{"z": {"name": "Bob"}}, "Bob"],
                [{"z": {"name": "Bob"}}, "Bob"],
                [{"z": {"name": "Carol"}}, "Carol"],
                [{"z": {"name": "Dave"}}, "Dave"],
                [{"z": None}, "n/a"],
                [{"z": None}, "n/a"]
            ]
        )

    async def test_edgeql_scope_tuple_correlate_03(self):
        await self.assert_query_result(
            """
                WITH X := (User, User.friends)
                SELECT count(X.0.friends.name ++ X.1.name);
            """,
            [10]
        )

    async def test_edgeql_scope_tuple_correlate_04(self):
        # The friends in the first element of the tuple should correlate
        # with User.friends on the right, which means that when it is
        # accessed, it should only have the one element.
        await self.assert_query_result(
            """
                WITH X := (User { friends }, User.friends)
                SELECT count(X.0.friends.name ++ X.1.name);
            """,
            [4]
        )

    async def test_edgeql_scope_tuple_correlate_05(self):
        await self.assert_query_result(
            """
                WITH X := (User {friends}, User.friends.name ?? 'n/a')
                SELECT _ := (X.0 {friends: {name}}, X.1) ORDER BY _.1;
            """,
            [
                [{"friends": [{"name": "Bob"}]}, "Bob"],
                [{"friends": [{"name": "Bob"}]}, "Bob"],
                [{"friends": [{"name": "Carol"}]}, "Carol"],
                [{"friends": [{"name": "Dave"}]}, "Dave"],
                [{"friends": []}, "n/a"],
                [{"friends": []}, "n/a"],
            ]
        )

    async def test_edgeql_select_outer_rebind_01(self):
        await self.assert_query_result(
            r'''
            select User {
              deck := (
                with
                  U := (
                    select User.deck {
                      el := User.deck.element
                    }
                  )
                select U {
                  name,
                  el2 := U.el
                } order by .name
              )
            } filter .name = 'Alice';
            ''',
            [
                {
                    "deck": [
                        {"el2": "Water", "name": "Bog monster"},
                        {"el2": "Fire", "name": "Dragon"},
                        {"el2": "Water", "name": "Giant turtle"},
                        {"el2": "Fire", "name": "Imp"}
                    ]
                }
            ]
        )

    async def test_edgeql_select_outer_rebind_02a(self):
        await self.assert_query_result(
            r'''
            select Card {
              name,
              owners := (
                with
                  U := (
                    select Card.owners {
                      n := Card.owners.name
                    }
                  )
                select U {
                  n
                } order by .name
              )
            } FILTER .name = 'Djinn';
            ''',
            [{"name": "Djinn", "owners": [{"n": "Carol"}, {"n": "Dave"}]}]
        )

    async def test_edgeql_select_outer_rebind_02b(self):
        await self.assert_query_result(
            r'''
            select Card {
              name,
              foo := (
                with
                  U := (
                    select Card.<deck[IS User] {
                      n := Card.<deck[IS User].name
                    }
                  )
                select U {
                  n
                } order by .name
              )
            } FILTER .name = 'Djinn';
            ''',
            [{"name": "Djinn", "foo": [{"n": "Carol"}, {"n": "Dave"}]}]
        )

    async def test_edgeql_select_outer_rebind_03(self):
        await self.assert_query_result(
            r'''
            select User {
              deck := (
                with
                  U := (
                    select User.deck {
                      cnt := User.deck@count
                    }
                  )
                select U {
                  name,
                  cnt2 := U.cnt
                } order by .name
              )
            } filter .name = 'Alice';
            ''',
            [
                {
                    "deck": [
                        {"cnt2": 3, "name": "Bog monster"},
                        {"cnt2": 2, "name": "Dragon"},
                        {"cnt2": 3, "name": "Giant turtle"},
                        {"cnt2": 2, "name": "Imp"}
                    ]
                }
            ]
        )

    async def test_edgeql_select_outer_rebind_04(self):
        await self.assert_query_result(
            r'''
            select User {
              avatar := (
                with
                  U := (
                    select User.avatar {
                      t := User.avatar@text,
                      retag := User.avatar@tag,
                    }
                  )
                select U {
                  name,
                  t2 := U.t,
                  retag,
                }
              )
            } order by .name
            ''',
            [
                {"avatar": {
                    "name": "Dragon", "retag": "Dragon-Best", "t2": "Best"}},
                {"avatar": None},
                {"avatar": None},
                {"avatar": {
                    "name": "Djinn", "retag": "Djinn-Wow", "t2": "Wow"}}
            ]
        )

    async def test_edgeql_select_outer_rebind_05(self):
        await self.assert_query_result(
            r'''
            SELECT User {
              name,
              avatar := (
                WITH
                  nemesis := User.avatar,
                  nemesis_mod := (FOR nem IN {nemesis} UNION (
                    WITH
                      name_len := std::len(nem.name)
                    SELECT nem {
                      name_len := name_len
                    }
                  ))
                SELECT nemesis_mod {
                  name,
                  single nameLen := nemesis_mod.name_len,
                  single nameLen2 := nemesis_mod.name_len
                }
              )
            }
            FILTER .name = 'Alice';
            ''',
            [
                {
                    "avatar": {"name": "Dragon", "nameLen": 6, "nameLen2": 6},
                    "name": "Alice"
                }
            ]
        )

    async def test_edgeql_select_outer_rebind_06(self):
        # hm, not really an outer rebind anymore!
        await self.assert_query_result(
            r'''
            for User in (select User filter .name = 'Alice') union (
              (
                WITH
                  nemesis := User.avatar,
                  nemesis_mod := (FOR nem IN {nemesis} UNION (
                    WITH
                      name_len := std::len(nem.name)
                    SELECT nem {
                      name_len := name_len
                    }
                  ))
                SELECT nemesis_mod {
                  name,
                  single nameLen := nemesis_mod.name_len,
                  single nameLen2 := nemesis_mod.name_len
                }
              )
            );
            ''',
            [{"name": "Dragon", "nameLen": 6, "nameLen2": 6}]
        )

    async def test_edgeql_select_outer_rebind_07a(self):
        await self.assert_query_result(
            r'''
                SELECT assert_single((
                  SELECT User {
                  deck := (
                    WITH
                      User_deck := User.deck
                    SELECT User_deck {
                      awards := (
                        User_deck.awards { name }
                      )
                    }
                  )
                }) filter .name ILIKE 'Alice%');
            ''',
            tb.bag([
                {
                    "deck": tb.bag([
                        {"awards": [{"name": "2nd"}]},
                        {"awards": tb.bag([{"name": "1st"}, {"name": "3rd"}])},
                        {"awards": []},
                        {"awards": []}
                    ])
                }
            ])
        )

    async def test_edgeql_select_outer_rebind_07b(self):
        await self.assert_query_result(
            r'''
                SELECT assert_exists((
                  SELECT User {
                  deck := (
                    WITH
                      User_deck := User.deck
                    SELECT User_deck {
                      awards := (
                        User_deck.awards { name }
                      )
                    }
                  )
                }) filter .name ILIKE 'Alice%');
            ''',
            tb.bag([
                {
                    "deck": tb.bag([
                        {"awards": [{"name": "2nd"}]},
                        {"awards": tb.bag([{"name": "1st"}, {"name": "3rd"}])},
                        {"awards": []},
                        {"awards": []}
                    ])
                }
            ])
        )

    async def test_edgeql_scope_linkprop_rebinding_01(self):
        # This is a lot like code the querybuilder generates
        # See issue #4961
        await self.assert_query_result(
            r'''
            select assert_exists((WITH
              __user := DETACHED User
            SELECT __user {
              deck := (
                WITH
                  __user2 := (
                    SELECT __user.deck {
                      __linkprop_count := __user.deck@count
                    }
                  )
                SELECT __user2 {
                  single @count := __user2.__linkprop_count
                }
              )
            } filter .name = 'Alice'));
            ''',
            [
                {
                    "deck": tb.bag([
                        {"@count": 2},
                        {"@count": 2},
                        {"@count": 3},
                        {"@count": 3},
                    ])
                }
            ]
        )

    async def test_edgeql_scope_for_with_computable_01(self):
        await self.assert_query_result(
            r'''
            with props := (
              for h in User union (
                select h {namelen := len(h.name)}
              )
            )
            select props {
              name,
              namelen
            };
            ''',
            tb.bag([
                {"name": "Alice", "namelen": 5},
                {"name": "Bob", "namelen": 3},
                {"name": "Carol", "namelen": 5},
                {"name": "Dave", "namelen": 4}
            ])
        )

    async def test_edgeql_scope_for_with_computable_02(self):
        await self.assert_query_result(
            r'''
            with props := (
              for h in User union (
                with g := h, select g {namelen := len(g.name)}
              )
            )
            select props {
              name,
              namelen
            };
            ''',
            tb.bag([
                {"name": "Alice", "namelen": 5},
                {"name": "Bob", "namelen": 3},
                {"name": "Carol", "namelen": 5},
                {"name": "Dave", "namelen": 4}
            ])
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_shape_intersection_semijoin_01(self):
        await self.assert_query_result(
            r'''
                select User { name } filter [is Bot].deck.name = 'Dragon'
            ''',
            [
                {"name": "Dave"}
            ]
        )

    async def test_edgeql_computable_join_01(self):
        await self.assert_query_result(
            r'''
            select Card {
                multi w := (
                    select .awards { name }
                    filter .name = Card.best_award.name
                )
            }
            filter .name = 'Dragon';
            ''',
            [{"w": [{"name": "1st"}]}]
        )

    async def test_edgeql_scope_intersection_semijoin_01(self):
        await self.assert_query_result(
            r'''
                select count(Named[IS User].deck);
            ''',
            [9],
        )

    async def test_edgeql_scope_union_backlink_01(self):
        await self.assert_query_result(
            r'''
                select {Card {name}, Card{element}} {name, owners: {name}}
                filter .name = 'Djinn';
            ''',
            [
                {"name": "Djinn", "owners": tb.bag([
                    {"name": "Carol"}, {"name": "Dave"}])},
                {"name": "Djinn", "owners": tb.bag([
                    {"name": "Carol"}, {"name": "Dave"}])},
            ],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_scope_schema_computed_01(self):
        await self.con.execute('''
            alter type User
            create link lcards := (
                select Card filter Card.name[0] = User.name[0]);
        ''')

        await self.assert_query_result(
            r'''
                with U := User,
                select U { name } filter exists .lcards;
            ''',
            tb.bag([
                {"name": "Bob"},
                {"name": "Dave"},
            ]),
        )

        await self.assert_query_result(
            r'''
                select Bot { lcards: {name} }
            ''',
            [
                {
                    "lcards": tb.bag([
                        {"name": "Dragon"},
                        {"name": "Dwarf"},
                        {"name": "Djinn"}
                    ])
                }
            ]
        )

    async def test_edgeql_scope_schema_computed_02(self):
        await self.con.execute('''
            alter type Named
            create property foo := count(User)
        ''')

        # Make sure that 'User' doesn't get captured when evaluating
        # the schema computed here.
        await self.assert_query_result(
            r'''
                select User { foo }
            ''',
            [{'foo': 4}, {'foo': 4}, {'foo': 4}, {'foo': 4}]
        )

    async def test_edgeql_scope_linkprop_assert_01(self):
        await self.assert_query_result(
            r'''
            select User {
              cards := assert_exists(User.deck {name, c := User.deck@count})
            }
            filter .name = 'Alice';
            ''',
            [
                {
                    "cards": tb.bag([
                        {"c": 2, "name": "Imp"},
                        {"c": 2, "name": "Dragon"},
                        {"c": 3, "name": "Bog monster"},
                        {"c": 3, "name": "Giant turtle"}
                    ])
                }
            ],
        )

        await self.assert_query_result(
            r'''
            select User {
              cards := assert_exists(User.deck {name, @c := User.deck@count})
            }
            filter .name = 'Alice';
            ''',
            [
                {
                    "cards": tb.bag([
                        {"@c": 2, "name": "Imp"},
                        {"@c": 2, "name": "Dragon"},
                        {"@c": 3, "name": "Bog monster"},
                        {"@c": 3, "name": "Giant turtle"}
                    ])
                }
            ],
        )

        # Query builder style
        await self.assert_query_result(
            r'''
            WITH U := DETACHED User
            SELECT U {
              deck := assert_exists((
                WITH
                  Q := (
                    SELECT U.deck {
                      __count := U.deck@count
                    }
                  )
                SELECT Q {
                  name,
                  single @count := Q.__count
                }
              ))
            } filter .name = 'Alice';
            ''',
            [
                {
                    "deck": tb.bag([
                        {"@count": 2, "name": "Imp"},
                        {"@count": 2, "name": "Dragon"},
                        {"@count": 3, "name": "Bog monster"},
                        {"@count": 3, "name": "Giant turtle"}
                    ])
                }
            ],
        )

    async def test_edgeql_scope_linkprop_assert_02(self):
        await self.assert_query_result(
            '''
            SELECT User {
                cards := assert_exists(.deck {name, @count})
            }
            filter .name = 'Alice';
            ''',
            [
                {
                    "cards": tb.bag([
                        {"@count": 2, "name": "Imp"},
                        {"@count": 2, "name": "Dragon"},
                        {"@count": 3, "name": "Bog monster"},
                        {"@count": 3, "name": "Giant turtle"}
                    ])
                }
            ],
        )

    async def test_edgeql_scope_linkprop_assert_03(self):
        await self.assert_query_result(
            r'''
            SELECT User {
                cards := assert_exists(User.deck {name, c := @count})
            }
            filter .name = 'Alice';
            ''',
            [
                {
                    "cards": tb.bag([
                        {"c": 2, "name": "Imp"},
                        {"c": 2, "name": "Dragon"},
                        {"c": 3, "name": "Bog monster"},
                        {"c": 3, "name": "Giant turtle"}
                    ])
                }
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT User {
                cards := assert_exists(.deck {name, c := @count})
            }
            filter .name = 'Alice';
            ''',
            [
                {
                    "cards": tb.bag([
                        {"c": 2, "name": "Imp"},
                        {"c": 2, "name": "Dragon"},
                        {"c": 3, "name": "Bog monster"},
                        {"c": 3, "name": "Giant turtle"}
                    ])
                }
            ],
        )

    async def test_edgeql_scope_filter_qeq_01(self):
        await self.assert_query_result(
            r'''
            select User filter .avatar ?= <Card>{} and .name = 'Bob';
            ''',
            [
                {},
            ],
        )

        await self.assert_query_result(
            r'''
            select User filter .name = 'Bob' and .avatar ?= <Card>{}
            ''',
            [
                {},
            ],
        )

    @test.xerror("Issue #6059 (non-group generalization)")
    async def test_edgeql_scope_mat_issue_6059(self):
        await self.assert_query_result(
            r'''
            with
              groups := (
                for k in {'Earth', 'Air', 'Fire', 'Water'} union {
                    elements := (select Card filter .element = k),
                    r := random(),
                }
              ),
            select groups {
              keyCard := (
                select .elements { id }
                limit 1
              ),
            }
            order by .keyCard.cost
            ''',
            [{"keyCard": {}}] * 4,
        )

    @test.xerror("Issue #6060 (non-group generalization)")
    async def test_edgeql_scope_mat_issue_6060(self):
        await self.assert_query_result(
            r'''
            with
              groups := (
                for k in {'Earth', 'Air', 'Fire', 'Water'} union {
                    elements := (select Card filter .element = k),
                    r := random(),
                }
              ),
              submissions := (
                groups {
                  minCost := min(.elements.cost)
                }
              )
            select submissions {
              minCost
            }
            order by .minCost;
            ''',
            [{"minCost": 1}, {"minCost": 1}, {"minCost": 1}, {"minCost": 2}],
        )

    async def test_edgeql_scope_implicit_limit_01(self):
        # implicit limit should interact correctly with offset
        await self.assert_query_result(
            r'''
                select Card { name } order by .name offset 3
            ''',
            [
                {"name": "Dwarf"},
                {"name": "Giant eagle"},
                {"name": "Giant turtle"},
                {"name": "Golem"},
            ],
            implicit_limit=4,
        )

        await self.assert_query_result(
            r'''
            with W := Card
            select W { name } order by .name offset 3;
            ''',
            [
                {"name": "Dwarf"},
                {"name": "Giant eagle"},
                {"name": "Giant turtle"},
                {"name": "Golem"},
            ],
            implicit_limit=4,
        )

        await self.assert_query_result(
            r'''
                select Card { name } order by .name offset 3 limit 2
            ''',
            [
                {"name": "Dwarf"},
                {"name": "Giant eagle"},
            ],
            implicit_limit=4,
        )

        await self.assert_query_result(
            r'''
            select User { deck: {name} order by .name offset 3 }
            filter .name = 'Carol';
            ''',
            [
                {
                    "deck": [
                        {"name": "Giant eagle"},
                        {"name": "Giant turtle"},
                        {"name": "Golem"},
                    ]
                }
            ],
            implicit_limit=3,
        )

    async def test_edgeql_scope_implicit_limit_02(self):
        # explicit limit shouldn't override implicit
        await self.assert_query_result(
            r'''
            select User { deck: {name} order by .name offset 3 limit 100}
            filter .name = 'Carol';
            ''',
            [
                {
                    "deck": [
                        {"name": "Giant eagle"},
                        {"name": "Giant turtle"},
                        {"name": "Golem"},
                    ]
                }
            ],
            implicit_limit=3,
        )

        await self.assert_query_result(
            r'''
            select User { cards := (
                select .deck {name} order by .name offset 3 limit 100)}
            filter .name = 'Carol';
            ''',
            [
                {
                    "cards": [
                        {"name": "Giant eagle"},
                        {"name": "Giant turtle"},
                        {"name": "Golem"},
                    ]
                }
            ],
            implicit_limit=3,
        )

        await self.assert_query_result(
            r'''
                select Card { name } order by .name offset 3 limit 100
            ''',
            [
                {"name": "Dwarf"},
                {"name": "Giant eagle"},
                {"name": "Giant turtle"},
                {"name": "Golem"},
            ],
            implicit_limit=4,
        )

        await self.assert_query_result(
            r'''
                select Card { name } order by .name offset 3 limit 1
            ''',
            [
                {"name": "Dwarf"},
            ],
            implicit_limit=4,
        )
