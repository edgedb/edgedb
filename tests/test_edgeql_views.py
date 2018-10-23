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
import unittest  # NOQA

from edb.server import _testbase as tb


class TestEdgeQLViews(tb.QueryTestCase):
    '''The scope is to test views.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    SETUP = [os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_setup.eql'),
             os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_views_setup.eql')]

    async def test_edgeql_views_basic_01(self):
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

    async def test_edgeql_views_basic_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT expert_map
            ORDER BY expert_map;
        ''', [
            [
                ['Alice', 'pro'],
                ['Bob', 'noob'],
                ['Carol', 'noob'],
                ['Dave', 'casual'],
            ],
        ])

    async def test_edgeql_views_basic_03(self):
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

    async def test_edgeql_views_basic_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT <tuple<str, int64, int64>>scores
            ORDER BY scores.name;
        ''', [
            [
                ['Alice', 100, 10],
                ['Bob', 11, 2],
                ['Carol', 31, 5],
                ['Dave', 78, 10],
            ],
        ])

    async def test_edgeql_views_basic_05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT <tuple<name: str, points: int64, plays: int64>>scores
            ORDER BY scores.name;
        ''', [
            [
                {'name': 'Alice', 'points': 100, 'plays': 10},
                {'name': 'Bob', 'points': 11, 'plays': 2},
                {'name': 'Carol', 'points': 31, 'plays': 5},
                {'name': 'Dave', 'points': 78, 'plays': 10},
            ],
        ])

    async def test_edgeql_views_create_01(self):
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

    async def test_edgeql_views_filter_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT FireCard {name}
            FILTER FireCard = DaveCard
            ORDER BY FireCard.name;
        ''', [
            [{'name': 'Dragon'}],
        ])

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

    async def test_edgeql_computable_link_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Card {
                owners: {
                    name
                } ORDER BY .name
            }
            FILTER .name = 'Djinn';
        ''', [
            [{
                'owners': [
                    {'name': 'Carol'},
                    {'name': 'Dave'}
                ]
            }]
        ])

    async def test_edgeql_computable_link_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT User {
                name,
                deck_cost
            }
            ORDER BY User.name;
        ''', [
            [{
                'name': 'Alice',
                'deck_cost': 11
            }, {
                'name': 'Bob',
                'deck_cost': 9
            }, {
                'name': 'Carol',
                'deck_cost': 16
            }, {
                'name': 'Dave',
                'deck_cost': 20
            }]
        ])

    async def test_edgeql_computable_aliased_link_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT AliasedFriends {
                my_name,
                my_friends: {
                    @nickname
                } ORDER BY .name
            }
            FILTER .name = 'Alice';
        ''', [
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
        ])

    async def test_edgeql_computable_nested_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Card {
                name,
                owned := (
                    WITH O := Card.<deck
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
        ''', [
            [{
                'name': 'Giant turtle',
                'owned': [
                    {'fr0': 3, 'fr1': 3, 'name': 'Alice'},
                    {'fr0': 0, 'fr1': 0, 'name': 'Bob'},
                    {'fr0': 0, 'fr1': 0, 'name': 'Carol'},
                    {'fr0': 1, 'fr1': 1, 'name': 'Dave'},
                ]
            }]
        ])

    @unittest.expectedFailure
    async def test_edgeql_computable_propagation_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT _ := {
                (SELECT User FILTER .name = 'Alice').deck,
                (SELECT User FILTER .name = 'Bob').deck
            } {name}
            ORDER BY _.name;

            WITH MODULE test
            # the view should be propagated through _ := DISTINCT since it
            # maps `any` to `any`
            SELECT _ := DISTINCT {
                (SELECT User FILTER .name = 'Alice').deck,
                (SELECT User FILTER .name = 'Bob').deck
            } {name}
            ORDER BY _.name;

            WITH MODULE test
            # the view should be propagated through _ := DETACHED
            SELECT _ := DETACHED {
                (SELECT User FILTER .name = 'Alice').deck,
                (SELECT User FILTER .name = 'Bob').deck
            } {name}
            ORDER BY _.name;

            WITH MODULE test
            # the view should be propagated through _ := DETACHED
            SELECT _ := DETACHED ({
                (SELECT User FILTER .name = 'Alice').deck,
                (SELECT User FILTER .name = 'Bob').deck
            } {name})
            ORDER BY _.name;
        ''', [
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
            [
                {'name': 'Bog monster'},
                {'name': 'Dragon'},
                {'name': 'Dwarf'},
                {'name': 'Giant turtle'},
                {'name': 'Golem'},
                {'name': 'Imp'},
            ],
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
        ])
