##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest  # NOQA

from edgedb.server import _testbase as tb


class TestEdgeQLLinkToAtoms(tb.QueryTestCase):
    '''The scope is to test unusual atomic links.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'linkatoms.eschema')

    SETUP = r"""
        # create some items
        WITH MODULE test
        INSERT Item {
            name := 'table',

            tag_set1 := 'wood' UNION 'rectangle',
            tag_set2 := 'wood' UNION 'rectangle',
            tag_array := ['wood', 'rectangle'],
            components := ['board' -> 1, 'legs' -> 4],
        };

        WITH MODULE test
        INSERT Item {
            name := 'floor lamp',

            tag_set1 := 'metal' UNION 'plastic',
            tag_set2 := 'metal' UNION 'plastic',
            tag_array := ['metal', 'plastic'],
            components := ['shaft' -> 1, 'bulbs' -> 3],
        };
    """

    async def test_edgeql_links_basic00(self):
        # HACK: this will pass if there were no problems applying the schema
        await self.assert_query_result(r'''
            SELECT 1;
        ''', [[1]])

    async def test_edgeql_links_basic01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {
                name,
                tag_set1,  # XXX: needs syntax for ordering?
                tag_set2,  # XXX: needs syntax for ordering?
                tag_array,
                components,
            } ORDER BY .name;
        ''', [
            [
                {
                    'name': 'floor lamp',
                    'tag_set1': {'metal', 'plastic'},
                    'tag_set2': {'metal', 'plastic'},
                    'tag_array': ['metal', 'plastic'],
                    'components': {'shaft': 1, 'bulbs': 3},
                }, {
                    'name': 'table',
                    'tag_set1': {'rectangle', 'wood'},
                    'tag_set2': {'rectangle', 'wood'},
                    'tag_array': ['wood', 'rectangle'],
                    'components': {'board': 1, 'legs': 4},
                }
            ]
        ])
