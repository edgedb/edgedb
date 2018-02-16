##
# Copyright (c) 2018-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest  # NOQA

from edgedb.server import _testbase as tb


class TestEdgeQLFor(tb.QueryTestCase):
    '''These tests are focused on using FOR statement.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.eql')

    async def test_edgeql_for_cross_01(self):
        cards = ['Bog monster', 'Djinn', 'Dragon', 'Dwarf', 'Giant eagle',
                 'Giant turtle', 'Golem', 'Imp', 'Sprite']
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            FOR C IN {Card}
            # C and Card are not related here
            UNION (C.name, Card.name);
        ''', lambda x: x, [
            [[a, b] for a in cards for b in cards],
        ])

    @unittest.expectedFailure
    async def test_edgeql_for_cross_02(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            FOR C IN {Card}
            # C and Card are not related here, so count(Card) should be 9
            UNION (C.name, count(Card));
        ''', lambda x: x, [
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
        ])

    async def test_edgeql_for_cross_03(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            FOR Card IN {Card}
            # Card is shadowed here
            UNION (Card.name, count(Card));
        ''', lambda x: x, [
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
        ])

    async def test_edgeql_for_cross_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR C IN {Card}
            # C and Card are not related here, so count(Card) should be 9
            UNION (count(C), count(Card));
        ''', [
            [
                [1, 9],
            ] * 9,
        ])

    async def test_edgeql_for_mix_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR X IN {Card.name, User.name}
            UNION X;
        ''', [
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
        ])

    @unittest.expectedFailure
    async def test_edgeql_for_mix_02(self):
        await self.assert_sorted_query_result(r'''
            WITH MODULE test
            FOR X IN {Card.name, User.name}
            # both Card and User should be independent of X
            UNION (X, count(Card), count(User));
        ''', lambda x: x, [
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
            ]
        ])

    async def test_edgeql_for_mix_03(self):
        await self.assert_sorted_query_result(r'''
            # should be the same result as above
            WITH MODULE test
            FOR X IN {Card.name, User.name}
            UNION (X, count(Card FILTER TRUE), count(User FILTER TRUE));
        ''', lambda x: x, [
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
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_for_mix_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR X IN {Card.name, User.name}
            # this should be just [3] for each name (9 + 4 of names)
            UNION count(User.friends);
        ''', [
            [3] * 13
        ])

    async def test_edgeql_for_filter_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR X IN {User.name}
            UNION X
            # this FILTER should have no impact
            FILTER Card.element = 'Air';
        ''', [
            {
                'Alice',
                'Bob',
                'Carol',
                'Dave',
            }
        ])

    @unittest.expectedFailure
    async def test_edgeql_for_filter_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            FOR X IN {Card.name}
            UNION X
            # this FILTER should have no impact
            FILTER Card.element = 'Air';
        ''', [
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
        ])

    @unittest.expectedFailure
    async def test_edgeql_for_filter_03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            # get a combination of names from different concepts
            FOR X IN {Card.name, User.name}
            UNION X
            # this FILTER should have no impact
            FILTER Card.element = 'Air';
        ''', [
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
        ])
