##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest  # NOQA

from edgedb.server import _testbase as tb


class TestEdgeQLScope(tb.QueryTestCase):
    '''This tests the scoping rules.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.eql')

    # TODO: Ideally the tests here should use ORDER BY to sort
    # results, but that is contingent on (at least some) other tests
    # passing:
    #
    # - test_edgeql_scope_sort_01
    # - test_edgeql_expr_tuple_12-13
    # - test_edgeql_expr_tuple_indirection_06+
    #
    # Once these work we can drop the special
    # assert_sorted_query_result in favor of assert_query_result
    # and the commented ORDER BY clause may be uncommented for some tests.

    @unittest.expectedFailure
    async def test_edgeql_scope_sort_01(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                A := {1, 2},
                U := (SELECT User FILTER User.name IN {'Alice', 'Bob'})
            SELECT _ := (U{name}, A)
            # specifically test the ORDER clause
            ORDER BY _.1 THEN _.0.name DESC;
        ''', [
            [
                [{'name': 'Bob'}, 1],
                [{'name': 'Alice'}, 1],
                [{'name': 'Bob'}, 2],
                [{'name': 'Alice'}, 2],
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_tuple_01(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                A := {1, 2}
            SELECT _ := (User{name, a := A}, A)
            # ORDER BY _.0.name THEN _.1
            ;
        ''', lambda x: (x[0]['name'], x[1]), [
            [
                [{'a': [1], 'name': 'Alice'}, 1],
                [{'a': [1], 'name': 'Bob'}, 1],
                [{'a': [1], 'name': 'Carol'}, 1],
                [{'a': [1], 'name': 'Dave'}, 1],
                [{'a': [2], 'name': 'Alice'}, 2],
                [{'a': [2], 'name': 'Bob'}, 2],
                [{'a': [2], 'name': 'Carol'}, 2],
                [{'a': [2], 'name': 'Dave'}, 2],
            ]
        ])

    async def test_edgeql_scope_tuple_02(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                A := {1, 2}
            SELECT _ := (A, User{name, a := A})
            # ORDER BY _.0.name THEN _.1
            ;
        ''', lambda x: (x[0], x[1]['name']), [
            [
                [1, {'a': [1], 'name': 'Alice'}],
                [1, {'a': [1], 'name': 'Bob'}],
                [1, {'a': [1], 'name': 'Carol'}],
                [1, {'a': [1], 'name': 'Dave'}],
                [2, {'a': [2], 'name': 'Alice'}],
                [2, {'a': [2], 'name': 'Bob'}],
                [2, {'a': [2], 'name': 'Carol'}],
                [2, {'a': [2], 'name': 'Dave'}],
            ]
        ])

    async def test_edgeql_scope_tuple_03(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT _ := (
                # User.friends is a common path, so it refers to the
                # SAME object in both tuple elements. In particular
                # that means that in the User shape there will always
                # be a single object appearing in friends link
                # (although it's a ** link).
                User {name, friends: {@nickname}},
                User.friends.name
            )
            # ORDER BY _.0.name THEN _.1
            ;
        ''', lambda x: (x[0]['name'], x[1]), [
            [
                [
                    {
                        'name': 'Alice',
                        'friends': [{'@nickname': 'Swampy'}],
                    },
                    'Bob',
                ],
                [
                    {
                        'name': 'Alice',
                        'friends': [{'@nickname': 'Firefighter'}],
                    },
                    'Carol',
                ],
                [
                    {
                        'name': 'Alice',
                        'friends': [{'@nickname': 'Grumpy'}],
                    },
                    'Dave',
                ],
                [
                    {
                        'name': 'Dave',
                        'friends': [{'@nickname': None}],
                    },
                    'Bob',
                ]
            ]
        ])

    async def test_edgeql_scope_tuple_04(self):
        await self.assert_sorted_query_result(r'''
            WITH
                MODULE test,
                U2 := User
            SELECT (
                User {name, foo := U2 {name}},
                U2.name
            )
            FILTER U2.name = 'Alice';
            # ORDER BY _.0.name THEN _.1
            ;
        ''', lambda x: (x[0]['name'], x[1]), [
            [
                [
                    {
                        'name': 'Alice',
                        'foo': [
                            {'name': 'Alice'},
                        ],
                    },
                    'Alice'
                ],
                [
                    {
                        'name': 'Bob',
                        'foo': [
                            {'name': 'Alice'},
                        ],
                    },
                    'Alice'
                ],
                [
                    {
                        'name': 'Carol',
                        'foo': [
                            {'name': 'Alice'},
                        ],
                    },
                    'Alice'
                ],
                [
                    {
                        'name': 'Dave',
                        'foo': [
                            {'name': 'Alice'},
                        ],
                    },
                    'Alice'
                ],
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_tuple_05(self):
        await self.assert_query_result(r'''
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
        ''', [
            [
                {
                    'name': 'Alice',
                    'foo': [
                        {'name': 'Alice'},
                    ],
                },
                {
                    'name': 'Bob',
                    'foo': [
                        {'name': 'Bob'},
                    ],
                },
                {
                    'name': 'Carol',
                    'foo': [
                        {'name': 'Carol'},
                    ],
                },
                {
                    'name': 'Dave',
                    'foo': [
                        {'name': 'Dave'},
                    ],
                },
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_tuple_06(self):
        await self.assert_sorted_query_result(r'''
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
                U2.friends.name
            )
            FILTER U2.friends.name = 'Bob'
            # ORDER BY _.0.name THEN _.1
            ;
        ''', lambda x: (x[0]['name'], x[1]), [
            [
                [
                    {
                        'name': 'Alice',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    'Bob',
                ],
                [
                    {
                        'name': 'Bob',
                        'friends_of_others': None,  # this is Bob
                    },
                    'Bob',
                ],
                [
                    {
                        'name': 'Carol',
                        'friends_of_others': [
                            {'name': 'Bob'},
                        ],
                    },
                    'Bob',
                ],
                [
                    {
                        'name': 'Dave',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    'Bob',
                ],
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_tuple_07(self):
        await self.assert_sorted_query_result(r'''
            # compare to test_edgeql_scope_filter_03 to see how it
            # works out without tuples
            WITH
                MODULE test,
                U2 := User
            SELECT (
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
                U2.friends.name
            )
            FILTER U2.friends.name = 'Bob'
            # ORDER BY _.0.name THEN _.1
            ;
        ''', lambda x: (x[0]['name'], x[1]), [
            [
                [
                    {
                        'name': 'Alice',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    'Bob',
                ],
                [
                    {
                        'name': 'Bob',
                        'friends_of_others': None,  # this is Bob
                    },
                    'Bob',
                ],
                [
                    {
                        'name': 'Carol',
                        'friends_of_others': [
                            {'name': 'Bob'},
                        ],
                    },
                    'Bob',
                ],
                [
                    {
                        'name': 'Dave',
                        'friends_of_others': None,  # Bob is a direct friend
                    },
                    'Bob',
                ],
            ]
        ])

    async def test_edgeql_scope_tuple_08(self):
        await self.assert_query_result(r'''
        WITH MODULE test
        SELECT (User.name, User.deck_cost, count(User.deck),
                User.deck_cost / count(User.deck))
        ORDER BY User.name;

        WITH MODULE test
        # in the below expression User.friends is the
        # longest common prefix, so we know that for
        # each friend, the average cost will be
        # calculated.
        SELECT User.friends.deck_cost / count(User.friends.deck)
        ORDER BY User.friends.name;

        WITH MODULE test
        # in the below expression User.friends is the
        # longest common prefix, so we know that for
        # each friend, the average cost will be
        # calculated.
        SELECT User.friends.deck_cost / count(User.friends.deck)
        FILTER User.friends.name = 'Bob';
        ''', [
            [
                ['Alice', 11, 4, 2.75],
                ['Bob', 9, 4, 2.25],
                ['Carol', 16, 7, 2.2857142857142856],
                ['Dave', 20, 7, 2.857142857142857],
            ],
            [
                2.25,                # Bob (friend of Alice)
                2.25,                # Bob (also friend of Dave)
                2.2857142857142856,  # Carol
                2.857142857142857    # Dave
            ],
            [2.25, 2.25],
        ])

    async def test_edgeql_scope_tuple_09(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            SELECT (
                Card {
                    name,
                    percent_cost := (
                        WITH CARDINALITY '1'
                        SELECT <int>(100 * Card.cost / Card.<deck.deck_cost)
                    ),
                },
                Card.<deck.name
            );
        ''', lambda x: (x[1], x[0]['name']), [
            [
                [{'name': 'Bog monster', 'percent_cost': 18}, 'Alice'],
                [{'name': 'Dragon', 'percent_cost': 45}, 'Alice'],
                [{'name': 'Giant turtle', 'percent_cost': 27}, 'Alice'],
                [{'name': 'Imp', 'percent_cost': 9}, 'Alice'],

                [{'name': 'Bog monster', 'percent_cost': 22}, 'Bob'],
                [{'name': 'Dwarf', 'percent_cost': 11}, 'Bob'],
                [{'name': 'Giant turtle', 'percent_cost': 33}, 'Bob'],
                [{'name': 'Golem', 'percent_cost': 33}, 'Bob'],

                [{'name': 'Bog monster', 'percent_cost': 13}, 'Carol'],
                [{'name': 'Djinn', 'percent_cost': 25}, 'Carol'],
                [{'name': 'Dwarf', 'percent_cost': 6}, 'Carol'],
                [{'name': 'Giant eagle', 'percent_cost': 13}, 'Carol'],
                [{'name': 'Giant turtle', 'percent_cost': 19}, 'Carol'],
                [{'name': 'Golem', 'percent_cost': 19}, 'Carol'],
                [{'name': 'Sprite', 'percent_cost': 6}, 'Carol'],

                [{'name': 'Bog monster', 'percent_cost': 10}, 'Dave'],
                [{'name': 'Djinn', 'percent_cost': 20}, 'Dave'],
                [{'name': 'Dragon', 'percent_cost': 25}, 'Dave'],
                [{'name': 'Giant eagle', 'percent_cost': 10}, 'Dave'],
                [{'name': 'Giant turtle', 'percent_cost': 15}, 'Dave'],
                [{'name': 'Golem', 'percent_cost': 15}, 'Dave'],
                [{'name': 'Sprite', 'percent_cost': 5}, 'Dave'],
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_filter_01(self):
        await self.assert_query_result(r'''
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
        ''', [
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
        ])

    async def test_edgeql_scope_filter_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User.friends {name}
            FILTER User.friends NOT IN {}
            ORDER BY User.friends.name;
        ''', [
            [
                {'name': 'Bob'},
                {'name': 'Carol'},
                {'name': 'Dave'},
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_filter_03(self):
        await self.assert_query_result(r'''
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
        ''', [
            [
                {
                    'name': 'Alice',
                    'friends_of_others': None,
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
        ])

    async def test_edgeql_scope_filter_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {
                name,
                friends: {
                    name
                } ORDER BY User.friends.name
            }
            FILTER User.friends.name = 'Carol';
        ''', [
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
        ])

    async def test_edgeql_scope_order_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {
                name,
                friends: {
                    name
                } ORDER BY User.friends.name
            }
            ORDER BY (
                    WITH CARDINALITY '1'
                    SELECT
                        User.friends.name
                    FILTER User.friends@nickname = 'Firefighter'
                ) EMPTY FIRST
                THEN
                User.name;
        ''', [
            [
                {
                    'name': 'Bob',
                    'friends': None
                },
                {
                    'name': 'Carol',
                    'friends': None
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
        ])

    # NOTE: LIMIT tests are largely identical to OFFSET tests, any
    # time there is a new OFFSET test, there should be a corresponding
    # LIMIT one.
    @unittest.expectedFailure
    async def test_edgeql_scope_offset_01(self):
        await self.assert_query_result(r'''
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
                    WITH CARDINALITY '1'
                    SELECT
                        # in the below expression User.friends is the
                        # longest common prefix, so we know that for
                        # each friend, the average cost will be
                        # calculated.
                        <int>(User.friends.deck_cost /
                                count(User.friends.deck))
                    # finally, filter out the only answer we need, to
                    # satisfy the CARDINALITY requirement of the OFFSET
                    FILTER User.friends@nickname = 'Firefighter'
                );
        ''', [
            [
                {
                    'name': 'Carol',
                    'friends': None
                },
                {
                    'name': 'Dave',
                    'friends': [
                        {'name': 'Bob'},
                    ],
                },
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_offset_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {
                name,
                friends: {
                    name
                }  # User.friends is scoped from the enclosing shape
                ORDER BY User.friends.name
                OFFSET (count(User.friends) - 1) IF EXISTS User.friends ELSE 0
                # the above is equivalent to getting the last friend,
                # ordered by name
            }
            ORDER BY User.name;
        ''', [
            [
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Dave'},
                    ],
                },
                {
                    'name': 'Bob',
                    'friends': None
                },
                {
                    'name': 'Carol',
                    'friends': None
                },
                {
                    'name': 'Dave',
                    'friends': [
                        {'name': 'Bob'},
                    ],
                },
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_limit_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {
                name,
                friends: {
                    name
                } ORDER BY User.friends.name
            }
            ORDER BY User.name
            # the LIMIT clause is in a sibling scope to SELECT, so
            # the User.friends are completely independent in them.
            LIMIT (# NOTE: effectively it's LIMIT 2
                    #
                    # Select the average card value (rounded to an
                    # int) for the user who is someone's friend AND
                    # nicknamed 'Firefighter':
                    # - the user happens to be Carol
                    # - her average deck cost is 2
                    #   (see test_edgeql_scope_tuple_08)
                    WITH CARDINALITY '1'
                    SELECT
                        # in the below expression User.friends is the
                        # longest common prefix, so we know that for
                        # each friend, the average cost will be
                        # calculated.
                        <int>(User.friends.deck_cost /
                                count(User.friends.deck))
                    # finally, filter out the only answer we need, to
                    # satisfy the CARDINALITY requirement of the LIMIT
                    FILTER User.friends@nickname = 'Firefighter'
                );
        ''', [
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
                    'friends': None
                },
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_limit_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {
                name,
                friends: {
                    name
                }  # User.friends is scoped from the enclosing shape
                ORDER BY User.friends.name
                LIMIT (count(User.friends) - 1) IF EXISTS User.friends ELSE 0
                # the above is equivalent to getting the all except
                # last friend, ordered by name
            }
            ORDER BY User.name;
        ''', [
            [
                {
                    'name': 'Alice',
                    'friends': [
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                    ],
                },
                {
                    'name': 'Bob',
                    'friends': None
                },
                {
                    'name': 'Carol',
                    'friends': None
                },
                {
                    'name': 'Dave',
                    'friends': None,
                },
            ]
        ])

    async def test_edgeql_scope_nested_01(self):
        await self.assert_query_result(r'''
            # control query Q1
            WITH MODULE test
            SELECT Card.element + ' ' + Card.name
            FILTER Card.name > Card.element
            ORDER BY Card.name;
        ''', [
            ['Air Djinn', 'Air Giant eagle', 'Earth Golem', 'Fire Imp',
             'Air Sprite']
        ])

    async def test_edgeql_scope_nested_02(self):
        await self.assert_query_result(r'''
            # semantically same as control query Q1, with lots of
            # nested aliases
            WITH
                MODULE test,
                A := Card
            SELECT
                A.element + ' ' + (WITH B := A SELECT B).name
            FILTER
                (WITH C := A SELECT
                    (WITH D := C SELECT D.name) > C.element
                )
            ORDER BY
                (WITH E := A SELECT E.name);
        ''', [
            ['Air Djinn', 'Air Giant eagle', 'Earth Golem', 'Fire Imp',
             'Air Sprite'],
        ])

    async def test_edgeql_scope_nested_03(self):
        await self.assert_query_result(r'''
            # semantically same as control query Q1, with lots of
            # nested aliases
            WITH
                MODULE test,
                A := Card
            SELECT
                A.element + ' ' + (WITH B := A SELECT B).name
            FILTER
                (WITH C := A SELECT
                    # D is defined in terms of A directly
                    (WITH D := A SELECT D.name) > C.element
                )
            ORDER BY
                (WITH E := A SELECT E.name);
        ''', [
            ['Air Djinn', 'Air Giant eagle', 'Earth Golem', 'Fire Imp',
             'Air Sprite'],
        ])

    async def test_edgeql_scope_nested_04(self):
        await self.assert_query_result(r'''
            # semantically same as control query Q1, with lots of
            # nested aliases reusing the same symbol
            WITH
                MODULE test,
                A := Card
            SELECT
                B := A.element + ' ' + (WITH B := A SELECT B).name
            FILTER
                (WITH B := A SELECT
                    (WITH B := A SELECT B.name) > B.element
                )
            ORDER BY
                (WITH B := A SELECT B.name);
        ''', [
            ['Air Djinn', 'Air Giant eagle', 'Earth Golem', 'Fire Imp',
             'Air Sprite'],
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_nested_05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            # `Card.element` and `Card.name` have `Card` as the LCP,
            # therefore the `SELECT` clause will be evaluated element-
            # wise for each `Card`
            SELECT Card.element + <str>count(Card.name)
            # `Card` is a valid prefix to be used in nested scopes,
            # because the outer scope already is using expressions
            # that are element-wise based on `Card`
            FILTER Card.name > Card.element
            ORDER BY Card.name;
        ''', [
            ['Air1', 'Air1', 'Earth1', 'Fire1', 'Air1']
        ])

    async def test_edgeql_scope_nested_06(self):
        await self.assert_query_result(r'''
            # control query Q2
            WITH MODULE test
            # combination of element + SET OF with a common prefix
            SELECT Card.name + <str>count(Card.owners)
            FILTER
                # some element filters
                Card.name < Card.element
                AND
                # a SET OF filter that shares a prefix with SELECT SET
                # OF, but is actually independent
                count(Card.owners.friends) > 2
            ORDER BY Card.name;
        ''', [
            ['Bog monster4', 'Dragon2', 'Giant turtle4']
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_nested_07(self):
        await self.assert_query_result(r'''
            # semantically same as control query Q2, with lots of
            # nested aliases
            WITH
                MODULE test,
                A := Card
            SELECT (WITH B := A SELECT A.name) + <str>count(A.owners)
            FILTER
                (WITH C := A SELECT C.name <
                    (WITH D := C SELECT D.element))
                AND
                (WITH E := A
                 SELECT count((WITH F := E SELECT F.owners.friends)) > 2
                )
            ORDER BY A.name;
        ''', [
            ['Bog monster4', 'Dragon2', 'Giant turtle4']
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_nested_08(self):
        await self.assert_query_result(r'''
            # semantically same as control query Q2, with lots of
            # nested aliases, all referring to the top level alias
            WITH
                MODULE test,
                A := Card
            SELECT (WITH B := A SELECT A.name) + <str>count(A.owners)
            FILTER
                (WITH C := A SELECT C.name <
                    (WITH D := A SELECT D.element))
                AND
                (WITH E := A
                 SELECT count((WITH F := A SELECT F.owners.friends)) > 2
                )
            ORDER BY A.name;
        ''', [
            ['Bog monster4', 'Dragon2', 'Giant turtle4']
        ])

    async def test_edgeql_scope_nested_09(self):
        await self.assert_query_result(r'''
            # control query Q3
            WITH MODULE test
            SELECT Card.name + <str>count(Card.owners);
        ''', [
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'}
        ])

    @unittest.expectedFailure
    async def test_edgeql_scope_nested_10(self):
        await self.assert_query_result(r'''
            # semantically same as control query Q3, except that some
            # aliases are introduced
            WITH MODULE test
            SELECT (WITH A := Card SELECT A).name + <str>count(Card.owners);

            WITH MODULE test
            SELECT (WITH A := Card SELECT A.name) + <str>count(Card.owners);
        ''', [
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
        ])

    async def test_edgeql_scope_nested_11(self):
        await self.assert_query_result(r'''
            # semantically same as control query Q3, except that some
            # aliases are introduced
            WITH MODULE test
            SELECT Card.name + <str>count((WITH A := Card SELECT A).owners);

            WITH MODULE test
            SELECT Card.name + <str>count((WITH A := Card SELECT A.owners));
        ''', [
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
            {'Imp1', 'Dragon2', 'Bog monster4', 'Giant turtle4', 'Dwarf2',
             'Golem3', 'Sprite2', 'Giant eagle2', 'Djinn2'},
        ])
