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
                          'inventory.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'inventory_setup.eql')

    async def test_edgeql_links_basic01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {
                name,
                tag_set1,
                tag_set2,
                tag_array,
                components,
            } ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'tag_set1': {'plastic', 'round'},
                    'tag_set2': {'plastic', 'round'},
                    'tag_array': None,
                    'components': None
                }, {
                    'name': 'chair',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': None,
                    'tag_array': ['wood', 'rectangle'],
                    'components': {'legs': 4, 'board': 2}
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': None,
                    'tag_set2': None,
                    'tag_array': None,
                    'components': None
                }, {
                    'name': 'floor lamp',
                    'tag_set1': {'metal', 'plastic'},
                    'tag_set2': {'metal', 'plastic'},
                    'tag_array': ['metal', 'plastic'],
                    'components': {'legs': 1, 'bulbs': 3}
                }, {
                    'name': 'mystery toy',
                    'tag_set1': None,
                    'tag_set2': None,
                    'tag_array': None,
                    'components': {'bulbs': 4, 'screen': 1, 'buttons': 42}
                }, {
                    'name': 'table',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': {'wood', 'rectangle'},
                    'tag_array': ['wood', 'rectangle'],
                    'components': {'legs': 4, 'board': 1}
                }, {
                    'name': 'teapot',
                    'tag_set1': None,
                    'tag_set2': None,
                    'tag_array': ['ceramic', 'round'],
                    'components': None
                }, {
                    'name': 'tv',
                    'tag_set1': None,
                    'tag_set2': {'plastic', 'rectangle'},
                    'tag_array': ['plastic', 'rectangle'],
                    'components': {'screen': 1}
                },
            ]
        ])

    async def test_edgeql_links_map_atoms01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {
                name,
                tag_set1 ORDER BY Item.tag_set1 DESC,
                tag_set2 ORDER BY Item.tag_set2 ASC,
            } ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'tag_set1': ['round', 'plastic'],
                    'tag_set2': ['plastic', 'round'],
                }, {
                    'name': 'chair',
                    'tag_set1': ['wood', 'rectangle'],
                    'tag_set2': None,
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'floor lamp',
                    'tag_set1': ['plastic', 'metal'],
                    'tag_set2': ['metal', 'plastic'],
                }, {
                    'name': 'mystery toy',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'table',
                    'tag_set1': ['wood', 'rectangle'],
                    'tag_set2': ['rectangle', 'wood'],
                }, {
                    'name': 'teapot',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'tv',
                    'tag_set1': None,
                    'tag_set2': ['plastic', 'rectangle'],
                },
            ]
        ])

    async def test_edgeql_links_map_atoms02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {
                name,
                tag_set1 ORDER BY Item.tag_set1 DESC LIMIT 1,
                tag_set2 ORDER BY Item.tag_set2 ASC OFFSET 1,
            } ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'tag_set1': ['round'],
                    'tag_set2': ['round'],
                }, {
                    'name': 'chair',
                    'tag_set1': ['wood'],
                    'tag_set2': None,
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'floor lamp',
                    'tag_set1': ['plastic'],
                    'tag_set2': ['plastic'],
                }, {
                    'name': 'mystery toy',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'table',
                    'tag_set1': ['wood'],
                    'tag_set2': ['wood'],
                }, {
                    'name': 'teapot',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'tv',
                    'tag_set1': None,
                    'tag_set2': ['rectangle'],
                },
            ]
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_links_map_atoms03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {
                name,
                tag_set1 FILTER Item.tag_set1 > 'p',
                tag_set2 FILTER Item.tag_set2 < 'w',
            } ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'tag_set1': {'plastic', 'round'},
                    'tag_set2': {'plastic', 'round'},
                }, {
                    'name': 'chair',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': None,
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'floor lamp',
                    'tag_set1': {'plastic'},
                    'tag_set2': {'metal', 'plastic'},
                }, {
                    'name': 'mystery toy',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'table',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': {'rectangle'},
                }, {
                    'name': 'teapot',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'tv',
                    'tag_set1': None,
                    'tag_set2': {'plastic', 'rectangle'},
                },
            ]
        ])

    async def test_edgeql_links_set01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER 'plastic' = .tag_set1
            ORDER BY .name;

            WITH MODULE test
            SELECT Item {name}
            FILTER 'plastic' = .tag_set2
            ORDER BY .name;
        ''', [
            [
                {'name': 'ball'},
                {'name': 'floor lamp'},
            ], [
                {'name': 'ball'},
                {'name': 'floor lamp'},
                {'name': 'tv'},
            ],
        ])

    @tb.expected_no_optimizer_failure
    async def test_edgeql_links_set02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER 'plastic' IN .tag_set1
            ORDER BY .name;

            WITH MODULE test
            SELECT Item {name}
            FILTER 'plastic' IN .tag_set2
            ORDER BY .name;
        ''', [
            [
                {'name': 'ball'},
                {'name': 'floor lamp'},
            ], [
                {'name': 'ball'},
                {'name': 'floor lamp'},
                {'name': 'tv'},
            ],
        ])

    async def test_edgeql_links_set03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER
                array_agg(ALL .tag_set1 ORDER BY .tag_set1) =
                    ['rectangle', 'wood']
            ORDER BY .name;

            WITH MODULE test
            SELECT Item {name}
            FILTER
                array_agg(ALL .tag_set2 ORDER BY .tag_set2) =
                    ['rectangle', 'wood']
            ORDER BY .name;
        ''', [
            [
                {'name': 'chair'},
                {'name': 'table'},
            ], [
                {'name': 'table'},
            ],
        ])

    async def test_edgeql_links_set04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER .tag_set1 = {'rectangle', 'wood'}
            ORDER BY .name;

            WITH MODULE test
            SELECT Item {name}
            FILTER .tag_set2 = {'rectangle', 'wood'}
            ORDER BY .name;
        ''', [
            [
                {'name': 'chair'},
                {'name': 'table'},
            ], [
                {'name': 'table'},
                {'name': 'tv'},
            ],
        ])

    async def test_edgeql_links_set05(self):
        await self.assert_query_result(r'''
            # subsets
            #
            WITH MODULE test
            SELECT Item {name}
            FILTER .tag_set1 IN {'rectangle', 'wood'}
            ORDER BY .name;

            WITH MODULE test
            SELECT Item {name}
            FILTER .tag_set2 IN {'rectangle', 'wood'}
            ORDER BY .name;
        ''', [
            [
                {'name': 'chair'},
                {'name': 'table'},
            ], [
                {'name': 'table'},
                {'name': 'tv'},
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_set06(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {
                name,
                foo := (
                    # XXX: check test_edgeql_expr_alias for failures first
                    SELECT _ := Item.tag_set1
                    FILTER _ = {'rectangle', 'wood'}
                ),
                bar := (
                    # XXX: check test_edgeql_expr_alias for failures first
                    SELECT _ := Item.tag_set2
                    FILTER _ = {'rectangle', 'wood'}
                ),
            }
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'chair',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': None,
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'floor lamp',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'mystery toy',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'table',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': {'wood', 'rectangle'},
                }, {
                    'name': 'teapot',
                    'tag_set1': None,
                    'tag_set2': None,
                }, {
                    'name': 'tv',
                    'tag_set1': None,
                    'tag_set2': {'rectangle'},
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_set07(self):
        await self.assert_query_result(r'''
            # subsets
            #
            WITH MODULE test
            SELECT Item {name}
            FILTER count(ALL (
                # XXX: check test_edgeql_expr_alias for failures first
                SELECT _ := Item.tag_set1
                FILTER _ = {'rectangle', 'wood'}
            )) = 2
            ORDER BY .name;

            WITH MODULE test
            SELECT Item {name}
            FILTER count(ALL (
                # XXX: check test_edgeql_expr_alias for failures first
                SELECT _ := Item.tag_set2
                FILTER _ = {'rectangle', 'wood'}
            )) = 2
            ORDER BY .name;
        ''', [
            [
                {'name': 'chair'},
                {'name': 'table'},
            ], [
                {'name': 'table'},
                {'name': 'tv'},
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_set08(self):
        await self.assert_query_result(r'''
            # match sets
            #
            WITH
                MODULE test,
                cmp := {'rectangle', 'wood'},
                cmp_count := count(ALL cmp)
            SELECT Item {name}
            FILTER
                cmp_count = count(ALL .tag_set1)
                AND
                cmp_count = count(ALL .tag_set1 UNION cmp)
            ORDER BY .name;

            WITH
                MODULE test,
                cmp := {'rectangle', 'wood'},
                cmp_count := count(ALL cmp)
            SELECT Item {name}
            FILTER
                cmp_count = count(ALL .tag_set2)
                AND
                cmp_count = count(ALL .tag_set2 UNION cmp)
            ORDER BY .name;
        ''', [
            [
                {'name': 'chair'},
                {'name': 'table'},
            ], [
                {'name': 'table'},
                {'name': 'tv'},
            ],
        ])

    async def test_edgeql_links_set09(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER
                array_agg(ALL .tag_set1 ORDER BY .tag_set1) =
                    array_agg(ALL .tag_set2 ORDER BY .tag_set2)
            ORDER BY .name;
        ''', [
            [
                {'name': 'ball'},
                {'name': 'ectoplasm'},
                {'name': 'floor lamp'},
                {'name': 'mystery toy'},
                {'name': 'table'},
                {'name': 'teapot'},
            ],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_links_set10(self):
        await self.assert_query_result(r'''
            # find an item with a unique quality
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                unique := (
                    SELECT _ := Item.tag_set1
                    FILTER _ NOT IN (
                        SELECT I2.tag_set1 FILTER I2 != Item
                    )
                )
            }
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'unique': ['round']
                }, {
                    'name': 'chair',
                    'unique': None
                }, {
                    'name': 'ectoplasm',
                    'unique': None
                }, {
                    'name': 'floor lamp',
                    'unique': ['metal']
                }, {
                    'name': 'mystery toy',
                    'unique': None
                }, {
                    'name': 'table',
                    'unique': None
                }, {
                    'name': 'teapot',
                    'unique': None
                }, {
                    'name': 'tv',
                    'unique': None
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_set11(self):
        await self.assert_query_result(r'''
            # find an item with a unique quality
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                unique := count(ALL (
                    SELECT _ := Item.tag_set1
                    FILTER _ NOT IN (
                        SELECT I2.tag_set1 FILTER I2 != Item
                    )
                ))
            }
            FILTER .unique > 0
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'unique': 1,
                }, {
                    'name': 'floor lamp',
                    'unique': 1,
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_set12(self):
        await self.assert_query_result(r'''
            # find an item with a unique quality
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                unique := (
                    # XXX: check test_edgeql_expr_alias for failures first
                    SELECT _ := Item.tag_set1
                    FILTER _ NOT IN (
                        SELECT I2.tag_set1 FILTER I2 != Item
                    )
                )
            }
            FILTER count(ALL .unique) > 0
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'unique': ['round'],
                }, {
                    'name': 'floor lamp',
                    'unique': ['metal'],
                },
            ],
        ])

    async def test_edgeql_links_array01(self):
        await self.assert_query_result(r'''
            # just a simple unpack
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                unpack := (SELECT array_unpack(Item.tag_array))
            }
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'unpack': None
                }, {
                    'name': 'chair',
                    'unpack': {'rectangle', 'wood'}
                }, {
                    'name': 'ectoplasm',
                    'unpack': None
                }, {
                    'name': 'floor lamp',
                    'unpack': {'metal', 'plastic'}
                }, {
                    'name': 'mystery toy',
                    'unpack': None
                }, {
                    'name': 'table',
                    'unpack': {'rectangle', 'wood'}
                }, {
                    'name': 'teapot',
                    'unpack': {'ceramic', 'round'}
                }, {
                    'name': 'tv',
                    'unpack': {'plastic', 'rectangle'}
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_array02(self):
        await self.assert_query_result(r'''
            # just a simple unpack
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                unpack := array_unpack(Item.tag_array)
            }
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'unpack': None
                }, {
                    'name': 'chair',
                    'unpack': {'rectangle', 'wood'}
                }, {
                    'name': 'ectoplasm',
                    'unpack': None
                }, {
                    'name': 'floor lamp',
                    'unpack': {'metal', 'plastic'}
                }, {
                    'name': 'mystery toy',
                    'unpack': None
                }, {
                    'name': 'table',
                    'unpack': {'rectangle', 'wood'}
                }, {
                    'name': 'teapot',
                    'unpack': {'ceramic', 'round'}
                }, {
                    'name': 'tv',
                    'unpack': {'plastic', 'rectangle'}
                },
            ],
        ])

    @tb.expected_no_optimizer_failure
    async def test_edgeql_links_array03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER 'metal' IN array_unpack(.tag_array)
            ORDER BY .name;
        ''', [
            [
                {'name': 'floor lamp'}
            ],
        ])

    async def test_edgeql_links_array04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER 'metal' = array_unpack(.tag_array)
            ORDER BY .name;
        ''', [
            [
                {'name': 'floor lamp'}
            ],
        ])

    async def test_edgeql_links_array05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER .tag_array[0] = 'metal'
            ORDER BY .name;
        ''', [
            [
                {'name': 'floor lamp'}
            ],
        ])

    async def test_edgeql_links_array06(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER .tag_array = ['metal', 'plastic']
            ORDER BY .name;
        ''', [
            [
                {'name': 'floor lamp'}
            ],
        ])

    async def test_edgeql_links_array07(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER NOT EXISTS .tag_array
            ORDER BY .name;
        ''', [
            [
                {'name': 'ball'},
                {'name': 'ectoplasm'},
                {'name': 'mystery toy'},
            ],
        ])

    async def test_edgeql_links_array08(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER NOT EXISTS .tag_array[3]  # no item has 3 elements
            ORDER BY .name;
        ''', [
            [
                {'name': 'ball'},
                {'name': 'chair'},
                {'name': 'ectoplasm'},
                {'name': 'floor lamp'},
                {'name': 'mystery toy'},
                {'name': 'table'},
                {'name': 'teapot'},
                {'name': 'tv'},
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_array09(self):
        await self.assert_query_result(r'''
            # find an item with a unique quality
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                unique := (
                    # XXX: check test_edgeql_expr_alias for failures first
                    SELECT _ := array_unpack(Item.tag_array)
                    FILTER _ NOT IN (
                        SELECT array_unpack(I2.tag_array) FILTER I2 != Item
                    )
                )
            }
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'ball',
                    'unique': None
                }, {
                    'name': 'chair',
                    'unique': None
                }, {
                    'name': 'ectoplasm',
                    'unique': None
                }, {
                    'name': 'floor lamp',
                    'unique': {'metal'}
                }, {
                    'name': 'mystery toy',
                    'unique': None
                }, {
                    'name': 'table',
                    'unique': None
                }, {
                    'name': 'teapot',
                    'unique': {'ceramic', 'round'}
                }, {
                    'name': 'tv',
                    'unique': None
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_array10(self):
        await self.assert_query_result(r'''
            # find an item with a unique quality
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                unique := (
                    # XXX: check test_edgeql_expr_alias for failures first
                    SELECT _ := array_unpack(Item.tag_array)
                    FILTER _ NOT IN (
                        SELECT array_unpack(I2.tag_array) FILTER I2 != Item
                    )
                )
            }
            FILTER count(ALL .unique) > 0
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'floor lamp',
                    'unique': {'metal'}
                }, {
                    'name': 'teapot',
                    'unique': {'ceramic', 'round'}
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_links_array11(self):
        await self.assert_query_result(r'''
            # find an item with ALL unique qualities
            WITH
                MODULE test,
                I2 := Item
            SELECT Item {
                name,
                tag_array,
            }
            FILTER
                # such that does not exist
                NOT EXISTS (
                    # another item
                    SELECT I2
                    FILTER
                        # different from current one
                        I2 != Item
                        AND
                        # matching at least one tag
                        array_unpack(I2.tag_array) =
                            array_unpack(Item.tag_array)
                )
            ORDER BY .name;
        ''', [
            [
                {
                    'name': 'teapot',
                    'unique': {'ceramic', 'round'}
                },
            ],
        ])

    async def test_edgeql_links_map01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER .components['bulbs'] > 0
            ORDER BY .name;
        ''', [
            [
                {'name': 'floor lamp'},
                {'name': 'mystery toy'},
            ],
        ])

    async def test_edgeql_links_map02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER EXISTS .components['bulbs']
            ORDER BY .name;
        ''', [
            [
                {'name': 'floor lamp'},
                {'name': 'mystery toy'},
            ],
        ])

    async def test_edgeql_links_map03(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER .components['bulbs'] = 0
            ORDER BY .name;
        ''', [
            [],
        ])

    async def test_edgeql_links_map04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Item {name}
            FILTER NOT EXISTS .components['bulbs']
            ORDER BY .name;
        ''', [
            [
                {'name': 'ball'},
                {'name': 'chair'},
                {'name': 'ectoplasm'},
                {'name': 'table'},
                {'name': 'teapot'},
                {'name': 'tv'},
            ],
        ])
