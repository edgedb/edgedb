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
    '''The scope is to test link properties.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'cards_setup.eql')

    async def test_edgeql_views_01(self):
        await self.assert_query_result(r'''
            CREATE VIEW test::AirCard := (
                WITH MODULE test
                SELECT Card {
                    owners := (
                        SELECT Card.<deck[IS User]
                    )
                } FILTER Card.element = 'Air'
            );

            WITH MODULE test
            SELECT AirCard {
                name,
                owners: {
                    name
                } ORDER BY .name
            } ORDER BY AirCard.name;

            DROP VIEW test::AirCard;
        ''', [
            None,
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
            None
        ])
