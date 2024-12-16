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

from edb.testbase import server as tb


class TestEdgeQLLinkToScalarTypes(tb.QueryTestCase):
    '''The scope is to test unusual scalar links.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'inventory.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'inventory_setup.edgeql')

    async def test_edgeql_links_basic_02(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    tag_set1,
                    tag_set2,
                    tag_array,
                } ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'tag_set1': {'plastic', 'round'},
                    'tag_set2': {'plastic', 'round'},
                    'tag_array': None,
                }, {
                    'name': 'chair',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': [],
                    'tag_array': ['wood', 'rectangle'],
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': [],
                    'tag_set2': [],
                    'tag_array': None,
                }, {
                    'name': 'floor lamp',
                    'tag_set1': {'metal', 'plastic'},
                    'tag_set2': {'metal', 'plastic'},
                    'tag_array': ['metal', 'plastic'],
                }, {
                    'name': 'mystery toy',
                    'tag_set1': [],
                    'tag_set2': [],
                    'tag_array': None,
                }, {
                    'name': 'table',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': {'wood', 'rectangle'},
                    'tag_array': ['wood', 'rectangle'],
                }, {
                    'name': 'teapot',
                    'tag_set1': [],
                    'tag_set2': [],
                    'tag_array': ['ceramic', 'round'],
                }, {
                    'name': 'tv',
                    'tag_set1': [],
                    'tag_set2': {'plastic', 'rectangle'},
                    'tag_array': ['plastic', 'rectangle'],
                },
            ]
        )

    async def test_edgeql_links_map_scalars_01(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    tag_set1 ORDER BY Item.tag_set1 DESC,
                    tag_set2 ORDER BY Item.tag_set2 ASC,
                } ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'tag_set1': ['round', 'plastic'],
                    'tag_set2': ['plastic', 'round'],
                }, {
                    'name': 'chair',
                    'tag_set1': ['wood', 'rectangle'],
                    'tag_set2': [],
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'floor lamp',
                    'tag_set1': ['plastic', 'metal'],
                    'tag_set2': ['metal', 'plastic'],
                }, {
                    'name': 'mystery toy',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'table',
                    'tag_set1': ['wood', 'rectangle'],
                    'tag_set2': ['rectangle', 'wood'],
                }, {
                    'name': 'teapot',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'tv',
                    'tag_set1': [],
                    'tag_set2': ['plastic', 'rectangle'],
                },
            ]
        )

    async def test_edgeql_links_map_scalars_02(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    tag_set1 ORDER BY Item.tag_set1 DESC LIMIT 1,
                    tag_set2 ORDER BY Item.tag_set2 ASC OFFSET 1,
                } ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'tag_set1': ['round'],
                    'tag_set2': ['round'],
                }, {
                    'name': 'chair',
                    'tag_set1': ['wood'],
                    'tag_set2': [],
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'floor lamp',
                    'tag_set1': ['plastic'],
                    'tag_set2': ['plastic'],
                }, {
                    'name': 'mystery toy',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'table',
                    'tag_set1': ['wood'],
                    'tag_set2': ['wood'],
                }, {
                    'name': 'teapot',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'tv',
                    'tag_set1': [],
                    'tag_set2': ['rectangle'],
                },
            ]
        )

    async def test_edgeql_links_map_scalars_03(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    tag_set1 FILTER Item.tag_set1 > 'p',
                    tag_set2 FILTER Item.tag_set2 < 'w',
                } ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'tag_set1': {'plastic', 'round'},
                    'tag_set2': {'plastic', 'round'},
                }, {
                    'name': 'chair',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': [],
                }, {
                    'name': 'ectoplasm',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'floor lamp',
                    'tag_set1': {'plastic'},
                    'tag_set2': {'metal', 'plastic'},
                }, {
                    'name': 'mystery toy',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'table',
                    'tag_set1': {'wood', 'rectangle'},
                    'tag_set2': {'rectangle'},
                }, {
                    'name': 'teapot',
                    'tag_set1': [],
                    'tag_set2': [],
                }, {
                    'name': 'tv',
                    'tag_set1': [],
                    'tag_set2': {'plastic', 'rectangle'},
                },
            ]
        )

    async def test_edgeql_links_set_01(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER 'plastic' IN .tag_set1
                ORDER BY .name;
            ''',
            [
                {'name': 'ball'},
                {'name': 'floor lamp'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER 'plastic' IN .tag_set2
                ORDER BY .name;
            ''',
            [
                {'name': 'ball'},
                {'name': 'floor lamp'},
                {'name': 'tv'},
            ]
        )

    async def test_edgeql_links_set_02(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER 'plastic' IN .tag_set1
                ORDER BY .name;
            ''',
            [
                {'name': 'ball'},
                {'name': 'floor lamp'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER 'plastic' IN .tag_set2
                ORDER BY .name;
            ''',
            [
                {'name': 'ball'},
                {'name': 'floor lamp'},
                {'name': 'tv'},
            ]
        )

    async def test_edgeql_links_set_03(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER
                    array_agg(Item.tag_set1 ORDER BY Item.tag_set1) =
                        ['rectangle', 'wood']
                ORDER BY .name;
            ''',
            [
                {'name': 'chair'},
                {'name': 'table'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER
                    array_agg(Item.tag_set2 ORDER BY Item.tag_set2) =
                        ['rectangle', 'wood']
                ORDER BY .name;
            ''',
            [
                {'name': 'table'},
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_links_set_04(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER .tag_set1 = {'rectangle', 'wood'}
                ORDER BY .name;
            ''',
            [
                {'name': 'chair'},
                {'name': 'table'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER .tag_set2 = {'rectangle', 'wood'}
                ORDER BY .name;
            ''',
            [
                {'name': 'table'},
                {'name': 'tv'},
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_links_set_05(self):
        await self.assert_query_result(
            r'''
                # subsets
                #
                SELECT Item {name}
                FILTER .tag_set1 IN {'rectangle', 'wood'}
                ORDER BY .name;
            ''',
            [
                {'name': 'chair'},
                {'name': 'table'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER .tag_set2 IN {'rectangle', 'wood'}
                ORDER BY .name;
            ''',
            [
                {'name': 'table'},
                {'name': 'tv'},
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_links_set_06(self):
        await self.assert_query_result(
            r'''
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
            ''',
            [
                {
                    'name': 'ball',
                    'foo': [],
                    'bar': [],
                }, {
                    'name': 'chair',
                    'foo': {'wood', 'rectangle'},
                    'bar': [],
                }, {
                    'name': 'ectoplasm',
                    'foo': [],
                    'bar': [],
                }, {
                    'name': 'floor lamp',
                    'foo': [],
                    'bar': [],
                }, {
                    'name': 'mystery toy',
                    'foo': [],
                    'bar': [],
                }, {
                    'name': 'table',
                    'foo': {'wood', 'rectangle'},
                    'bar': {'wood', 'rectangle'},
                }, {
                    'name': 'teapot',
                    'foo': [],
                    'bar': [],
                }, {
                    'name': 'tv',
                    'foo': [],
                    'bar': {'rectangle'},
                },
            ],
        )

    async def test_edgeql_links_set_07(self):
        await self.assert_query_result(
            r'''
                # subsets
                SELECT Item {name}
                FILTER count( (
                    # XXX: check test_edgeql_expr_alias for failures first
                    SELECT _ := Item.tag_set1
                    FILTER _ IN {'rectangle', 'wood'}
                )) = 2
                ORDER BY .name;
            ''',
            [
                {'name': 'chair'},
                {'name': 'table'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER count( (
                    # XXX: check test_edgeql_expr_alias for failures first
                    SELECT _ := Item.tag_set2
                    FILTER _ IN {'rectangle', 'wood'}
                )) = 2
                ORDER BY .name;
            ''',
            [
                {'name': 'table'},
            ],
        )

    async def test_edgeql_links_set_08(self):
        await self.assert_query_result(
            r'''
                # match sets
                WITH
                    cmp := {'rectangle', 'wood'},
                    cmp_count := count(cmp)
                SELECT Item {name}
                FILTER
                    cmp_count = count(Item.tag_set1)
                    AND
                    cmp_count = count(DISTINCT (Item.tag_set1 UNION cmp))
                ORDER BY .name;
            ''',
            [
                {'name': 'chair'},
                {'name': 'table'},
            ]
        )

        await self.assert_query_result(
            r'''
                WITH
                    cmp := {'rectangle', 'wood'},
                    cmp_count := count(cmp)
                SELECT Item {name}
                FILTER
                    cmp_count = count(.tag_set2)
                    AND
                    cmp_count = count(DISTINCT (.tag_set2 UNION cmp))
                ORDER BY .name;
            ''',
            [
                {'name': 'table'},
            ],
        )

    async def test_edgeql_links_set_10(self):
        await self.assert_query_result(
            r'''
                # same as previous, but with a different syntax, leading
                # to a different failure scenario
                WITH
                    cmp := {'rectangle', 'wood'},
                    cmp_count := count(cmp)
                # includes tag_set1 in the shape
                SELECT Item {name, tag_set1}
                FILTER
                    cmp_count = count(Item.tag_set1)
                    AND
                    cmp_count = count(DISTINCT (Item.tag_set1 UNION cmp))
                ORDER BY .name;
            ''',
            [
                {'name': 'chair', 'tag_set1': {'rectangle', 'wood'}},
                {'name': 'table', 'tag_set1': {'rectangle', 'wood'}},
            ]
        )

        await self.assert_query_result(
            r'''
                WITH
                    cmp := {'rectangle', 'wood'},
                    cmp_count := count(cmp)
                # includes tag_set1 in the shape
                SELECT Item {name, tag_set2}
                FILTER
                    cmp_count = count(Item.tag_set2)
                    AND
                    cmp_count = count(DISTINCT (Item.tag_set2 UNION cmp))
                ORDER BY .name;
            ''',
            [
                {'name': 'table', 'tag_set2': {'rectangle', 'wood'}},
            ],
        )

    async def test_edgeql_links_set_11(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER
                    array_agg(Item.tag_set1 ORDER BY Item.tag_set1) =
                        array_agg(Item.tag_set2 ORDER BY Item.tag_set2)
                ORDER BY .name;
            ''',
            [
                {'name': 'ball'},
                {'name': 'ectoplasm'},
                {'name': 'floor lamp'},
                {'name': 'mystery toy'},
                {'name': 'table'},
                {'name': 'teapot'},
            ],
        )

    async def test_edgeql_links_set_12(self):
        await self.assert_query_result(
            r'''
                # find an item with a unique quality
                WITH
                    I2 := Item
                SELECT Item {
                    name,
                    unique := (
                        SELECT _ := Item.tag_set1
                        FILTER _ NOT IN (
                            (SELECT I2 FILTER I2 != Item).tag_set1
                        )
                    )
                }
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'unique': ['round']
                }, {
                    'name': 'chair',
                    'unique': []
                }, {
                    'name': 'ectoplasm',
                    'unique': []
                }, {
                    'name': 'floor lamp',
                    'unique': ['metal']
                }, {
                    'name': 'mystery toy',
                    'unique': []
                }, {
                    'name': 'table',
                    'unique': []
                }, {
                    'name': 'teapot',
                    'unique': []
                }, {
                    'name': 'tv',
                    'unique': []
                },
            ],
        )

    async def test_edgeql_links_set_13(self):
        await self.assert_query_result(
            r'''
                # find an item with a unique quality
                WITH
                    I2 := Item
                SELECT Item {
                    name,
                    unique := count( (
                        SELECT _ := Item.tag_set1
                        FILTER _ NOT IN (
                            (SELECT I2 FILTER I2 != Item).tag_set1
                        )
                    ))
                }
                FILTER .unique > 0
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'unique': 1,
                }, {
                    'name': 'floor lamp',
                    'unique': 1,
                },
            ],
        )

    async def test_edgeql_links_set_14(self):
        await self.assert_query_result(
            r'''
                # find an item with a unique quality
                WITH
                    I2 := Item
                SELECT Item {
                    name,
                    unique := (
                        # XXX: check test_edgeql_expr_alias for failures first
                        SELECT _ := Item.tag_set1
                        FILTER _ NOT IN (
                            (SELECT I2 FILTER I2 != Item).tag_set1
                        )
                    )
                }
                FILTER count(.unique) > 0
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'unique': ['round'],
                }, {
                    'name': 'floor lamp',
                    'unique': ['metal'],
                },
            ],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_links_set_15(self):
        await self.assert_query_result(
            r'''
                # subsets
                SELECT Item {name}
                FILTER .tag_set1 IN {'wood', 'plastic'}
                ORDER BY count((
                    SELECT _ := Item.tag_set1
                    FILTER _ IN {'rectangle', 'plastic', 'wood'}
                )) DESC THEN .name;
            ''',
            [
                {'name': 'chair'},
                {'name': 'table'},
                {'name': 'ball'},
                {'name': 'floor lamp'},
            ],
        )

    async def test_edgeql_links_array_01(self):
        await self.assert_query_result(
            r'''
                # just a simple unpack
                SELECT Item {
                    name,
                    unpack := (SELECT array_unpack(Item.tag_array))
                }
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'unpack': []
                }, {
                    'name': 'chair',
                    'unpack': {'rectangle', 'wood'}
                }, {
                    'name': 'ectoplasm',
                    'unpack': []
                }, {
                    'name': 'floor lamp',
                    'unpack': {'metal', 'plastic'}
                }, {
                    'name': 'mystery toy',
                    'unpack': []
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
        )

    async def test_edgeql_links_array_02(self):
        await self.assert_query_result(
            r'''
                # just a simple unpack
                SELECT Item {
                    name,
                    unpack := array_unpack(Item.tag_array)
                }
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'unpack': []
                }, {
                    'name': 'chair',
                    'unpack': {'rectangle', 'wood'}
                }, {
                    'name': 'ectoplasm',
                    'unpack': []
                }, {
                    'name': 'floor lamp',
                    'unpack': {'metal', 'plastic'}
                }, {
                    'name': 'mystery toy',
                    'unpack': []
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
        )

    async def test_edgeql_links_array_03(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER 'metal' IN array_unpack(.tag_array)
                ORDER BY .name;
            ''',
            [
                {'name': 'floor lamp'}
            ],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_links_array_04(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER 'metal' = array_unpack(.tag_array)
                ORDER BY .name;
            ''',
            [
                {'name': 'floor lamp'}
            ],
        )

    async def test_edgeql_links_array_05(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                # array_get is used to safely default to {}
                FILTER array_get(.tag_array, 0) = 'metal'
                ORDER BY .name;
            ''',
            [
                {'name': 'floor lamp'}
            ],
        )

    async def test_edgeql_links_array_06(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER .tag_array = ['metal', 'plastic']
                ORDER BY .name;
            ''',
            [
                {'name': 'floor lamp'}
            ],
        )

    async def test_edgeql_links_array_07(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                FILTER NOT EXISTS .tag_array
                ORDER BY .name;
            ''',
            [
                {'name': 'ball'},
                {'name': 'ectoplasm'},
                {'name': 'mystery toy'},
            ],
        )

    async def test_edgeql_links_array_08(self):
        await self.assert_query_result(
            r'''
                SELECT Item {name}
                # no item has 3 elements
                FILTER NOT EXISTS array_get(.tag_array, 3)
                ORDER BY .name;
            ''',
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
        )

    async def test_edgeql_links_array_09(self):
        await self.assert_query_result(
            r'''
                # find an item with a unique quality
                WITH
                    I2 := Item
                SELECT Item {
                    name,
                    unique := (
                        SELECT _ := array_unpack(Item.tag_array)
                        FILTER _ NOT IN (
                            SELECT array_unpack(
                                (SELECT I2 FILTER I2 != Item).tag_array
                            )
                        )
                    )
                }
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'unique': []
                }, {
                    'name': 'chair',
                    'unique': []
                }, {
                    'name': 'ectoplasm',
                    'unique': []
                }, {
                    'name': 'floor lamp',
                    'unique': {'metal'}
                }, {
                    'name': 'mystery toy',
                    'unique': []
                }, {
                    'name': 'table',
                    'unique': []
                }, {
                    'name': 'teapot',
                    'unique': {'ceramic', 'round'}
                }, {
                    'name': 'tv',
                    'unique': []
                },
            ],
        )

    async def test_edgeql_links_array_10(self):
        await self.assert_query_result(
            r'''
                # find an item with a unique quality
                WITH
                    I2 := Item
                SELECT Item {
                    name,
                    unique := (
                        SELECT _ := array_unpack(Item.tag_array)
                        FILTER _ NOT IN (
                            SELECT array_unpack(
                                (SELECT I2 FILTER I2 != Item).tag_array
                            )
                        )
                    )
                }
                FILTER count(.unique) > 0
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'floor lamp',
                    'unique': {'metal'}
                }, {
                    'name': 'teapot',
                    'unique': {'ceramic', 'round'}
                },
            ],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_links_array_11(self):
        await self.assert_query_result(
            r'''
                # find an item with ALL unique qualities
                WITH
                    I2 := Item
                SELECT Item {
                    name,
                    tag_array,
                }
                FILTER
                    # such that has tag_array
                    EXISTS Item.tag_array AND
                    # and such that does not exist
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
            ''',
            [
                {
                    'name': 'teapot',
                    'tag_array': {'ceramic', 'round'}
                },
            ],
        )

    async def test_edgeql_links_derived_tuple_01(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    n1 := (Item.name,),
                    n2 := (Item.name,).0,
                    t1 := (Item.tag_set1,),
                    t2 := (Item.tag_set1, Item.tag_set2),
                    t3 := (Item.tag_set1,).0,
                    t4 := (Item.tag_set1, Item.tag_set2).1,
                }
                FILTER .name IN {'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'n1': ['chair'],
                    'n2': 'chair',
                    't1': [['rectangle'], ['wood']],
                    't2': [],
                    't3': ['rectangle', 'wood'],
                    't4': [],
                },
                {
                    'n1': ['table'],
                    'n2': 'table',
                    't1': [['rectangle'], ['wood']],
                    't2': [['rectangle', 'rectangle'], ['rectangle', 'wood'],
                           ['wood', 'rectangle'], ['wood', 'wood']],
                    't3': ['rectangle', 'wood'],
                    't4': ['rectangle', 'rectangle', 'wood', 'wood'],
                },
            ],
            sort={
                # sort the data
                't1': lambda x: x[0],
                't2': lambda x: (x[0], x[1]),
                't3': lambda x: x,
                't4': lambda x: x,
            }
        )

    async def test_edgeql_links_derived_tuple_02(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    n1 := (Item.name, 'foo'),
                }
                FILTER
                    .n1.0 = 'chair'
                ORDER BY
                    .name;
            ''',
            [
                {
                    'n1': ['chair', 'foo'],
                },
            ],
        )

    async def test_edgeql_links_derived_array_01(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    n1 := [Item.name],
                    n2 := [Item.name][0],
                    t1 := [Item.tag_set1],
                    t2 := [Item.tag_set1, Item.tag_set2],
                    t3 := [Item.tag_set1][0],
                    t4 := [Item.tag_set1, Item.tag_set2][1],
                    a1 := Item.tag_array,
                    a2 := Item.tag_array[0],
                }
                FILTER .name IN {'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'n1': ['chair'],
                    'n2': 'chair',
                    't1': [['rectangle'], ['wood']],
                    't2': [],
                    't3': ['rectangle', 'wood'],
                    't4': [],
                    'a1': ['wood', 'rectangle'],
                    'a2': 'wood',
                },
                {
                    'n1': ['table'],
                    'n2': 'table',
                    't1': [['rectangle'], ['wood']],
                    't2': [['rectangle', 'rectangle'], ['rectangle', 'wood'],
                           ['wood', 'rectangle'], ['wood', 'wood']],
                    't3': ['rectangle', 'wood'],
                    't4': ['rectangle', 'rectangle', 'wood', 'wood'],
                    'a1': ['wood', 'rectangle'],
                    'a2': 'wood',
                },
            ],
            sort={
                # sort the data
                't1': lambda x: x[0],
                't2': lambda x: (x[0], x[1]),
                't3': lambda x: x,
                't4': lambda x: x,
            }
        )

    async def test_edgeql_links_derived_array_02(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    n1 := [Item.name],
                    n2 := array_get([Item.name], 0),
                    t1 := [Item.tag_set1],
                    t2 := [Item.tag_set1, Item.tag_set2],
                    t3 := array_get([Item.tag_set1], 0),
                    t4 := array_get([Item.tag_set1, Item.tag_set2], 1),
                    a1 := Item.tag_array,
                    a2 := array_get(Item.tag_array, 0),
                }
                FILTER .name IN {'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'n1': ['chair'],
                    'n2': 'chair',
                    't1': [['rectangle'], ['wood']],
                    't2': [],
                    't3': ['rectangle', 'wood'],
                    't4': [],
                    'a1': ['wood', 'rectangle'],
                    'a2': 'wood',
                },
                {
                    'n1': ['table'],
                    'n2': 'table',
                    't1': [['rectangle'], ['wood']],
                    't2': [['rectangle', 'rectangle'], ['rectangle', 'wood'],
                           ['wood', 'rectangle'], ['wood', 'wood']],
                    't3': ['rectangle', 'wood'],
                    't4': ['rectangle', 'rectangle', 'wood', 'wood'],
                    'a1': ['wood', 'rectangle'],
                    'a2': 'wood',
                },
            ],
            sort={
                # sort the data
                't1': lambda x: x[0],
                't2': lambda x: (x[0], x[1]),
                't3': lambda x: x,
                't4': lambda x: x,
            }
        )

    async def test_edgeql_links_derived_array_03(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    a_a1 := Item.tag_array[{0, 1}],
                    a_t2 := [Item.tag_set1, Item.tag_set2][{0, 1}],
                }
                FILTER .name IN {'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'chair',
                    'a_a1': ['rectangle', 'wood'],
                    'a_t2': [],
                },
                {
                    'name': 'table',
                    'a_a1': ['rectangle', 'wood'],
                    'a_t2': ['rectangle', 'rectangle', 'rectangle',
                             'rectangle', 'wood', 'wood', 'wood', 'wood'],
                },
            ],
            sort={
                # sort the data
                'a_a1': lambda x: x,
                'a_t2': lambda x: x,
            }
        )

    async def test_edgeql_links_derived_array_04(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    a_a1 := array_get(Item.tag_array, {0, 1}),
                    a_t2 := array_get([Item.tag_set1, Item.tag_set2], {0, 1}),
                }
                FILTER .name IN {'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'chair',
                    'a_a1': ['rectangle', 'wood'],
                    'a_t2': [],
                },
                {
                    'name': 'table',
                    'a_a1': ['rectangle', 'wood'],
                    'a_t2': ['rectangle', 'rectangle', 'rectangle',
                             'rectangle', 'wood', 'wood', 'wood', 'wood'],
                },
            ],
            sort={
                # sort the data
                'a_a1': lambda x: x,
                'a_t2': lambda x: x,
            }
        )

    async def test_edgeql_links_derived_array_05(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    a_a1 := array_get(Item.tag_array, {0, 2}),
                    a_t2 := array_get([Item.tag_set1, Item.tag_set2], {0, 2}),
                }
                FILTER .name IN {'ball', 'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'a_a1': [],
                    'a_t2': ['plastic', 'plastic', 'round', 'round'],
                },
                {
                    'name': 'chair',
                    'a_a1': ['wood'],
                    'a_t2': [],
                },
                {
                    'name': 'table',
                    'a_a1': ['wood'],
                    'a_t2': ['rectangle', 'rectangle', 'wood', 'wood'],
                },
            ],
            sort={
                # sort the data
                'a_a1': lambda x: x,
                'a_t2': lambda x: x,
            }
        )

    async def test_edgeql_links_derived_array_06(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    a_a1 := Item.tag_array[1:20],
                    a_t2 := [Item.tag_set1, Item.tag_set2][1:20],
                }
                FILTER .name IN {'ball', 'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'a_a1': None,
                    'a_t2': [['plastic'], ['plastic'], ['round'], ['round']],
                },
                {
                    'name': 'chair',
                    'a_a1': ['rectangle'],
                    'a_t2': [],
                },
                {
                    'name': 'table',
                    'a_a1': ['rectangle'],
                    'a_t2': [['rectangle'], ['rectangle'], ['wood'], ['wood']],
                }
            ],
            sort={
                # sort the data
                'a_a1': lambda x: x,
                'a_t2': lambda x: x[0],
            }
        )

    async def test_edgeql_links_derived_array_07(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    a_a1 := Item.tag_array[{1, 2}:20],
                    a_t2 := [Item.tag_set1, Item.tag_set2][{1, 2}:20],
                }
                FILTER .name IN {'ball', 'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'ball',
                    'a_a1': [],  # empty set of arrays
                    'a_t2': [[], [], [], [],
                             ['plastic'], ['plastic'], ['round'], ['round']],
                },
                {
                    'name': 'chair',
                    'a_a1': [[], ['rectangle']],
                    'a_t2': [],  # empty set of arrays
                },
                {
                    'name': 'table',
                    'a_a1': [[], ['rectangle']],
                    'a_t2': [[], [], [], [],
                             ['rectangle'], ['rectangle'], ['wood'], ['wood']],
                }
            ],
            sort={
                # sort the data
                'a_a1': lambda x: x[0] if x else '',
                'a_t2': lambda x: x[0] if x else '',
            }
        )

    async def test_edgeql_links_derived_array_08(self):
        await self.assert_query_result(
            r'''
                SELECT Item {
                    name,
                    re := re_match(Item.tag_set1, Item.tag_set2),
                }
                FILTER .name IN {'chair', 'table'}
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'chair',
                    're': [],
                },
                {
                    'name': 'table',
                    're': [['rectangle'], ['wood']],
                }
            ],
            sort={
                # sort the data
                're': lambda x: x[0],
            }
        )
