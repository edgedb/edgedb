##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest  # NOQA

from edgedb.server import _testbase as tb


class TestEdgeQLViews(tb.QueryTestCase):
    '''The scope is to test views.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    SETUP = [os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_setup.eql'),
             os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_views_setup.eql')]

    async def test_edgeql_views_basic01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT AirCard {
                name,
                owners: {
                    name
                } ORDER BY .name
            } ORDER BY AirCard.name;
        ''', [
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
        ])

    async def test_edgeql_views_basic02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT expert_map;
        ''', [
            [
                {
                    'Alice': 'pro',
                    'Bob': 'noob',
                    'Carol': 'noob',
                    'Dave': 'casual',
                }
            ],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_views_basic03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT scores ORDER BY scores.name;
        ''', [
            [
                {'name': 'Alice', 'score': 100, 'games': 10},
                {'name': 'Bob', 'score': 11, 'games': 2},
                {'name': 'Carol', 'score': 31, 'games': 5},
                {'name': 'Dave', 'score': 78, 'games': 10},
            ],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_views_basic04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT <tuple<str, int, int>>scores
            ORDER BY scores.name;
        ''', [
            [
                ['Alice', 100, 10],
                ['Bob', 11, 2],
                ['Carol', 31, 5],
                ['Dave', 78, 10],
            ],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_views_basic05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT <tuple<name: str, points: int, plays: int>>scores
            ORDER BY scores.name;
        ''', [
            [
                {'name': 'Alice', 'points': 100, 'plays': 10},
                {'name': 'Bob', 'points': 11, 'plays': 2},
                {'name': 'Carol', 'points': 31, 'plays': 5},
                {'name': 'Dave', 'points': 78, 'plays': 10},
            ],
        ])

    async def test_edgeql_views_create01(self):
        await self.assert_query_result(r'''
            CREATE VIEW test::DCard := (
                WITH MODULE test
                SELECT Card {
                    owners := (
                        SELECT Card.<deck[IS User]
                    )
                } FILTER Card.name LIKE 'D%'
            );

            WITH MODULE test
            SELECT DCard {
                name,
                owners: {
                    name
                } ORDER BY .name
            } ORDER BY DCard.name;

            DROP VIEW test::DCard;
        ''', [
            None,
            [
                {
                    'name': 'Djinn',
                    'owners': [{'name': 'Carol'}, {'name': 'Dave'}],
                },
                {
                    'name': 'Dragon',
                    'owners': [{'name': 'Alice'}, {'name': 'Dave'}],
                },
                {
                    'name': 'Dwarf',
                    'owners': [{'name': 'Bob'}, {'name': 'Carol'}],
                }
            ],
            None
        ])

    async def test_edgeql_views_filter01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT FireCard {name}
            FILTER FireCard = DaveCard
            ORDER BY FireCard.name;
        ''', [
            [{'name': 'Dragon'}],
        ])

    @unittest.expectedFailure
    async def test_edgeql_views_filter02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT AirCard {name}
            FILTER AirCard NOT IN (SELECT Card FILTER Card.name LIKE 'D%')
            ORDER BY AirCard.name;
        ''', [
            [
                {'name': 'Giant eagle'},
                {'name': 'Sprite'},
            ],
        ])
