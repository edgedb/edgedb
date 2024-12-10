#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import edgedb

from edb.testbase import server as tb


class DumpTestCaseMixin:

    async def ensure_schema_data_integrity(self, include_data=True):
        async for tx in self._run_and_rollback_retrying():
            async with tx:
                await self._ensure_schema_integrity()
                if include_data:
                    await self._ensure_data_integrity()

    async def _ensure_schema_integrity(self):
        # check that all the type annotations are in place
        await self.assert_query_result(
            r'''
            WITH MODULE schema
            SELECT ObjectType {
                name,
                annotations: {
                    name,
                    @value,
                } ORDER BY .name
            }
            FILTER
                EXISTS .annotations
                AND
                .name LIKE 'default::%'
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'default::A',
                    'annotations': [
                        {
                            'name': 'std::title',
                            '@value': 'A',
                        },
                    ],
                },
                {
                    'name': 'default::B',
                    'annotations': [
                        {
                            'name': 'std::title',
                            '@value': 'B',
                        },
                    ],
                },
                {
                    'name': 'default::C',
                    'annotations': [
                        {
                            'name': 'std::title',
                            '@value': 'C',
                        },
                    ],
                },
                {
                    'name': 'default::D',
                    'annotations': [
                        {
                            'name': 'default::heritable_user_anno',
                            '@value': 'all D',
                        },
                        {
                            'name': 'default::user_anno',
                            '@value': 'D only',
                        },
                        {
                            'name': 'std::title',
                            '@value': 'D',
                        },
                    ],
                },
                {
                    'name': 'default::E',
                    'annotations': [
                        {
                            'name': 'default::heritable_user_anno',
                            '@value': 'all D',
                        },
                        {
                            'name': 'std::title',
                            '@value': 'E',
                        },
                    ],
                },
                {
                    'name': 'default::F',
                    'annotations': [
                        {
                            'name': 'default::heritable_user_anno',
                            '@value': 'all D',
                        },
                        {
                            'name': 'std::title',
                            '@value': 'F',
                        },
                    ],
                },
            ]
        )

        # check that all the prop/link annotations are in place
        await self.assert_query_result(
            r'''
            WITH MODULE schema
            SELECT ObjectType {
                name,
                properties: {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                } # keep only annotated props
                FILTER EXISTS .annotations
                ORDER BY .name,
                links: {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                } # keep only annotated links
                FILTER EXISTS .annotations
                ORDER BY .name,
            }
            FILTER
                # keep only types with annotated pointers
                EXISTS .pointers.annotations
                AND
                .name LIKE 'default::%'
            ORDER BY .name;
            ''',
            [

                {
                    'name': 'default::A',
                    'properties': [
                        {
                            'name': 'p_bool',
                            'annotations': [
                                {
                                    'name': 'std::title',
                                    '@value': 'single bool',
                                },
                            ],
                        },
                    ],
                    'links': [],
                },
                {
                    'name': 'default::B',
                    'properties': [
                        {
                            'name': 'p_bool',
                            'annotations': [
                                {
                                    'name': 'std::title',
                                    '@value': 'multi bool',
                                },
                            ],
                        },
                    ],
                    'links': []
                },
                {
                    'name': 'default::C',
                    'properties': [
                        {
                            'name': 'val',
                            'annotations': [
                                {
                                    'name': 'std::title',
                                    '@value': 'val',
                                },
                            ],
                        },
                    ],
                    'links': []
                },
                {
                    'name': 'default::D',
                    'properties': [],
                    'links': [
                        {
                            'name': 'multi_link',
                            'annotations': [
                                {
                                    'name': 'std::title',
                                    '@value': 'multi link to C',
                                },
                            ],
                        },
                        {
                            'name': 'single_link',
                            'annotations': [
                                {
                                    'name': 'std::title',
                                    '@value': 'single link to C',
                                },
                            ],
                        },
                    ]
                }
            ]
        )

        # check that all link prop annotations are in place
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    links: {
                        name,
                        properties: {
                            name,
                            annotations: {
                                name,
                                @value,
                            },
                        }
                        FILTER EXISTS .annotations
                        ORDER BY .name,
                    } # keep only links with user-annotated props
                    FILTER 'std::title' IN .properties.annotations.name
                    ORDER BY .name,
                }
                FILTER
                    .name = 'default::E'
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'default::E',
                    'links': [
                        {
                            'name': 'multi_link',
                            'properties': [
                                {
                                    'name': 'lp1',
                                    'annotations': [
                                        {
                                            'name': 'std::title',
                                            '@value': 'single lp1',
                                        },
                                    ],
                                },
                            ]
                        },
                        {
                            'name': 'single_link',
                            'properties': [
                                {
                                    'name': 'lp0',
                                    'annotations': [
                                        {
                                            'name': 'std::title',
                                            '@value': 'single lp0',
                                        },
                                    ],
                                },
                            ]
                        },
                    ]
                }
            ]
        )

        # check that all constraint annotations are in place
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    properties: {
                        name,
                        constraints: {
                            name,
                            annotations: {
                                name,
                                @value,
                            },
                        },
                    }
                    ORDER BY .name,
                }
                FILTER
                    .name = 'default::C'
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'default::C',
                    'properties': [
                        {
                            'name': 'id',
                            'constraints': [{
                                'annotations': [],
                            }],
                        },
                        {
                            'name': 'val',
                            'constraints': [{
                                'annotations': [
                                    {
                                        'name': 'std::title',
                                        '@value': 'exclusive C val',
                                    },
                                ],
                            }],
                        },
                    ]
                }
            ]
        )

        # check that all constraint annotations are in place
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT `Constraint` {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                    errmessage,
                    expr,
                }
                FILTER
                    .abstract
                    AND
                    .name LIKE 'default::%'
                ORDER BY .name;
            ''',
            [

                {
                    'name': 'default::user_int_constr',
                    'annotations': [
                        {
                            'name': 'std::title',
                            '@value': 'user_int_constraint constraint',
                        },
                    ],
                    'errmessage': '{__subject__} must be greater than {x}',
                    'expr': '(__subject__ > x)',
                },
            ]
        )

        # check that all function annotations are in place
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Function {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                    vol := <str>.volatility,
                }
                FILTER
                    EXISTS .annotations
                    AND
                    .name LIKE 'default::%'
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'default::user_func_0',
                    'annotations': [
                        {
                            'name': 'std::title',
                            '@value': 'user_func(int64) -> str',
                        },
                    ],
                    'vol': 'Immutable',
                },
            ]
        )

        # check that indexes are in place
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    },
                }
                FILTER
                    EXISTS .indexes
                    AND
                    .name LIKE 'default::%'
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'default::K',
                    'indexes': [{'expr': '.k'}],
                },
                {
                    'name': 'default::L',
                    'indexes': [{'expr': '(.l0 ++ .l1)'}],
                }
            ]
        )

        # check the custom scalars
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    name,
                    ancestors: {
                        name,
                    } ORDER BY @index,
                    constraints: {
                        name,
                        params: {
                            name,
                            @value,
                        } FILTER .name != '__subject__',
                    },
                }
                FILTER
                    .name LIKE 'default::User%'
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'default::UserEnum',
                    'ancestors': [
                        {'name': 'std::anyenum'},
                        {'name': 'std::anyscalar'},
                    ],
                    'constraints': [],
                },
                {
                    'name': 'default::UserInt',
                    'ancestors': [
                        {'name': 'std::int64'},
                        {'name': 'std::anyint'},
                        {'name': 'std::anyreal'},
                        {'name': 'std::anydiscrete'},
                        {'name': 'std::anypoint'},
                        {'name': 'std::anyscalar'},
                    ],
                    'constraints': [
                        {
                            'name': 'default::user_int_constr',
                            'params': [{'name': 'x', '@value': '5'}],
                        },
                    ],
                },
                {
                    'name': 'default::UserStr',
                    'ancestors': [
                        {'name': 'std::str'},
                        {'name': 'std::anyscalar'}
                    ],
                    'constraints': [
                        {
                            'name': 'std::max_len_value',
                            'params': [{'name': 'max', '@value': '5'}],
                        },
                    ],
                },
            ]
        )

        # check the custom scalars
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    properties: {
                        name,
                        constraints: {
                            name,
                            params: {
                                name,
                                @value,
                            } FILTER .name != '__subject__',
                        },
                    }
                    FILTER .name IN {'m0', 'm1'}
                    ORDER BY .name,
                }
                FILTER
                    .name = 'default::M';
            ''',
            [
                {
                    'name': 'default::M',
                    'properties': [
                        {
                            'name': 'm0',
                            'constraints': [
                                {
                                    'name': 'default::user_int_constr',
                                    'params': [{
                                        'name': 'x',
                                        '@value': '3'
                                    }],
                                },
                            ],
                        },
                        {
                            'name': 'm1',
                            'constraints': [
                                {
                                    'name': 'std::max_len_value',
                                    'params': [{
                                        'name': 'max',
                                        '@value': '3'
                                    }],
                                },
                            ],
                        }
                    ],
                },
            ]
        )

        # check the custom scalars
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    properties: {
                        name,
                        target: {
                            name,
                        },
                    }
                    FILTER .name IN {'n0', 'n1'}
                    ORDER BY .name,
                }
                FILTER
                    .name = 'default::N';
            ''',
            [
                {
                    'name': 'default::N',
                    'properties': [
                        {
                            'name': 'n0',
                            'target': {'name': 'default::UserInt'},
                        },
                        {
                            'name': 'n1',
                            'target': {'name': 'default::UserStr'},
                        },
                    ],
                },
            ]
        )

        # check that the bases and ancestors order is preserved
        await self.assert_query_result(
            r'''
            WITH MODULE schema
            SELECT ObjectType {
                name,
                bases: {
                    name,
                    @index,
                } ORDER BY @index,
                ancestors: {
                    name,
                    @index,
                } ORDER BY @index,
            }
            FILTER
                .name = 'default::V';
            ''',
            [
                {
                    'name': 'default::V',
                    'bases': [
                        {'name': 'default::U', '@index': 0},
                        {'name': 'default::S', '@index': 1},
                        {'name': 'default::T', '@index': 2},
                    ],
                    'ancestors': [
                        {'name': 'default::U', '@index': 0},
                        {'name': 'default::S', '@index': 1},
                        {'name': 'default::T', '@index': 2},
                        {'name': 'default::R', '@index': 3},
                        {'name': 'std::Object', '@index': 4},
                        {'name': 'std::BaseObject', '@index': 5},
                    ],
                }
            ]
        )

        # check delegated constraint
        await self.assert_query_result(
            r'''
            WITH MODULE schema
            SELECT ObjectType {
                name,
                properties: {
                    name,
                    constraints: {
                        name,
                        delegated,
                    },
                } ORDER BY .name,
            }
            FILTER
                .name = 'default::R'
                OR
                .name = 'default::S'
            ORDER BY .name;
            ''',
            [

                {
                    'name': 'default::R',
                    'properties': [
                        {
                            'name': 'id',
                            'constraints': [
                                {
                                    'name': 'std::exclusive',
                                    'delegated': False,
                                }
                            ],
                        },
                        {
                            'name': 'name',
                            'constraints': [
                                {
                                    'name': 'std::exclusive',
                                    'delegated': True,
                                }
                            ],
                        },
                    ],
                },
                {
                    'name': 'default::S',
                    'properties': [
                        {
                            'name': 'id',
                            'constraints': [
                                {
                                    'name': 'std::exclusive',
                                    'delegated': False,
                                }
                            ],
                        },
                        {
                            'name': 'name',
                            'constraints': [
                                {
                                    'name': 'std::exclusive',
                                    'delegated': False,
                                }
                            ],
                        },
                        {
                            'name': 's',
                            'constraints': [],
                        },
                    ],
                }

            ]
        )

    async def _ensure_data_integrity(self):
        # validate single props for all basic scalar types
        await self.assert_query_result(
            r'''
            SELECT A {
                p_bool,
                p_str,
                p_int16,
                p_int32,
                p_int64,
                p_float32,
                p_float64,
                p_bigint,
                p_decimal,
            };
            ''',
            [{
                'p_bool': True,
                'p_str': 'Hello',
                'p_int16': 12345,
                'p_int32': 1234567890,
                'p_int64': 1234567890123,
                'p_float32': 2.5,
                'p_float64': 2.5,
                'p_bigint': 123456789123456789123456789,
                'p_decimal':
                    123456789123456789123456789.123456789123456789123456789,
            }]
        )

        await self.assert_query_result(
            r'''
            SELECT (
                <str>A.p_datetime,
                <str>A.p_local_datetime,
                <str>A.p_local_date,
                <str>A.p_local_time,
                <str>A.p_duration,
            );
            ''',
            [[
                '2018-05-07T20:01:22.306916+00:00',
                '2018-05-07T20:01:22.306916',
                '2018-05-07',
                '20:01:22.306916',
                'PT20H',
            ]]
        )

        await self.assert_query_result(
            r'''
            SELECT A.p_json;
            ''',
            [[{"a": None, "b": True}, 1, 2.5, "foo"]],
            ['[{"a": null, "b": true}, 1, 2.5, "foo"]'],
        )

        # validate multi props for all basic scalar types
        await self.assert_query_result(
            r'''
            SELECT B {
                p_bool,
                p_str,
                p_int16,
                p_int32,
                p_int64,
                p_float32,
                p_float64,
                p_bigint,
                p_decimal,
            };
            ''',
            [{
                'p_bool': {True, False},
                'p_str': {'Hello', 'world'},
                'p_int16': {12345, -42},
                'p_int32': {1234567890, -42},
                'p_int64': {1234567890123, -42},
                'p_float32': {2.5, -42},
                'p_float64': {2.5, -42},
                'p_bigint': {123456789123456789123456789, -42},
                'p_decimal': {
                    123456789123456789123456789.123456789123456789123456789,
                    -42,
                },
            }]
        )

        await self.assert_query_result(
            r'''
            SELECT B {
                str_datetime := <str>.p_datetime,
                str_local_datetime := <str>.p_local_datetime,
                str_local_date := <str>.p_local_date,
                str_local_time := <str>.p_local_time,
                str_duration := <str>.p_duration,
            };
            ''',
            [{
                'str_datetime': {
                    '2018-05-07T20:01:22.306916+00:00',
                    '2019-05-07T20:01:22.306916+00:00',
                },
                'str_local_datetime': {
                    '2018-05-07T20:01:22.306916',
                    '2019-05-07T20:01:22.306916',
                },
                'str_local_date': {
                    '2018-05-07',
                    '2019-05-07',
                },
                'str_local_time': {
                    '20:01:22.306916',
                    '20:02:22.306916',
                },
                'str_duration': {
                    'PT20H',
                    'PT20S',
                },
            }]
        )

        await self.assert_query_result(
            r'''
            SELECT B.p_json
            ORDER BY B.p_json;
            ''',
            [
                "bar",
                False,
                [{"a": None, "b": True}, 1, 2.5, "foo"],
            ],
            [
                '"bar"',
                'false',
                '[{"a": null, "b": true}, 1, 2.5, "foo"]',
            ],
        )

        # bytes don't play nice with being cast into other types, so
        # we want to test them using binary fetch
        self.assertEqual(
            await self.con.query(r'SELECT A.p_bytes;'),
            edgedb.Set((b'Hello',))
        )
        self.assertEqual(
            await self.con.query(r'SELECT B.p_bytes ORDER BY B.p_bytes;'),
            edgedb.Set((b'Hello', b'world'))
        )

        # validate the data for types used to test links
        await self.assert_query_result(
            r'''
            SELECT C {val}
            ORDER BY .val;
            ''',
            [
                {'val': 'D00'},
                {'val': 'D01'},
                {'val': 'D02'},
                {'val': 'D03'},
                {'val': 'E00'},
                {'val': 'E01'},
                {'val': 'E02'},
                {'val': 'E03'},
                {'val': 'F00'},
                {'val': 'F01'},
                {'val': 'F02'},
                {'val': 'F03'},
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT D {
                num,
                single_link: {
                    val,
                },
                multi_link: {
                    val,
                } ORDER BY .val,
            }
            FILTER .__type__.name = 'default::D'
            ORDER BY .num;
            ''',
            [
                {
                    'num': 0,
                    'single_link': None,
                    'multi_link': [],
                },
                {
                    'num': 1,
                    'single_link': {'val': 'D00'},
                    'multi_link': [],
                },
                {
                    'num': 2,
                    'single_link': None,
                    'multi_link': [
                        {'val': 'D01'}, {'val': 'D02'},
                    ],
                },
                {
                    'num': 3,
                    'single_link': {'val': 'D00'},
                    'multi_link': [
                        {'val': 'D01'}, {'val': 'D02'}, {'val': 'D03'},
                    ],
                },
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT E {
                num,
                single_link: {
                    val,
                },
                multi_link: {
                    val,
                } ORDER BY .val,
            }
            ORDER BY .num;
            ''',
            [
                {
                    'num': 4,
                    'single_link': None,
                    'multi_link': [],
                },
                {
                    'num': 5,
                    'single_link': {'val': 'E00'},
                    'multi_link': [],
                },
                {
                    'num': 6,
                    'single_link': None,
                    'multi_link': [
                        {'val': 'E01'}, {'val': 'E02'},
                    ],
                },
                {
                    'num': 7,
                    'single_link': {'val': 'E00'},
                    'multi_link': [
                        {'val': 'E01'}, {'val': 'E02'}, {'val': 'E03'},
                    ],
                },
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT F {
                num,
                single_link: {
                    val,
                },
                multi_link: {
                    val,
                } ORDER BY .val,
            }
            ORDER BY .num;
            ''',
            [
                {
                    'num': 8,
                    'single_link': {'val': 'F00'},
                    'multi_link': [
                        {'val': 'F01'}, {'val': 'F02'}, {'val': 'F03'},
                    ],
                },
            ],
        )

        # validate link prop values
        await self.assert_query_result(
            r'''
            SELECT E {
                num,
                single_link: {
                    val,
                    @lp0,
                },
                multi_link: {
                    val,
                    @lp1,
                } ORDER BY .val,
            } ORDER BY .num;
            ''',
            [
                {
                    'num': 4,
                    'single_link': None,
                    'multi_link': [],
                },
                {
                    'num': 5,
                    'single_link': {
                        'val': 'E00',
                        '@lp0': None,
                    },
                    'multi_link': [],
                },
                {
                    'num': 6,
                    'single_link': None,
                    'multi_link': [
                        {
                            'val': 'E01',
                            '@lp1': None,
                        },
                        {
                            'val': 'E02',
                            '@lp1': None,
                        },
                    ],
                },
                {
                    'num': 7,
                    'single_link': {
                        'val': 'E00',
                        '@lp0': 'E00',
                    },
                    'multi_link': [
                        {
                            'val': 'E01',
                            '@lp1': 'E01',
                        },
                        {
                            'val': 'E02',
                            '@lp1': 'E02',
                        },
                        {
                            'val': 'E03',
                            '@lp1': 'E03',
                        },
                    ],
                },
            ],
        )

        # validate existence of data for types with computables and defaults
        await self.assert_query_result(
            r'''
            SELECT K {
                k,
            };
            ''',
            [
                {
                    'k': 'k0',
                },
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT L {
                l0,
                l1,
            };
            ''',
            [
                {
                    'l0': 'l0_0',
                    'l1': 'l1_0',
                },
            ],
        )

        # validate existence of data for indexed types
        await self.assert_query_result(
            r'''
            SELECT G {g0, g1, g2};
            ''',
            [
                {'g0': 'fixed', 'g1': 'func1', 'g2': '2'},
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT H {h0, h1, h2};
            ''',
            [
                {'h0': 'fixed', 'h1': 'func1', 'h2': '2'},
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT I {
                i0: {val},
                i1: {val},
                i2: {val},
            };
            ''',
            [
                {
                    'i0': {'val': 'D00'},
                    'i1': {'val': 'D01'},
                    'i2': {'val': 'D02'},
                }
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT J {
                j0: {val},
                j1: {val},
                j2: {val},
            };
            ''',
            [
                {
                    'j0': {'val': 'D00'},
                    'j1': {'val': 'D01'},
                    'j2': {'val': 'D02'},
                }
            ],
        )

        # validate existence of data for types with constraints
        await self.assert_query_result(
            r'''
            SELECT M {
                m0,
                m1,
            };
            ''',
            [
                {
                    'm0': 10,
                    'm1': 'm1',
                },
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT N {
                n0,
                n1,
            };
            ''',
            [
                {
                    'n0': 10,
                    'n1': 'n1',
                },
            ],
        )

        # validate user functions
        await self.assert_query_result(
            r'''
            SELECT user_func_0(99);
            ''',
            ['func99'],
        )

        await self.assert_query_result(
            r'''
            SELECT user_func_1([1, 3, -88], '+');
            ''',
            ['1+3+-88'],
        )

        await self.assert_query_result(
            r'''
            SELECT user_func_2(<int64>{});
            ''',
            {'x'},
        )

        await self.assert_query_result(
            r'''
            SELECT user_func_2(11);
            ''',
            {'11', 'x'},
        )

        await self.assert_query_result(
            r'''
            SELECT user_func_2(22, 'a');
            ''',
            {'22', 'a'},
        )

        # validate user enum
        await self.assert_query_result(
            r'''
            WITH w := {'Lorem', 'ipsum', 'dolor', 'sit', 'amet'}
            SELECT w
            ORDER BY str_lower(w);
            ''',
            ['amet', 'dolor', 'ipsum', 'Lorem', 'sit'],
        )

        await self.assert_query_result(
            r'''
            WITH w := {'Lorem', 'ipsum', 'dolor', 'sit', 'amet'}
            SELECT w
            ORDER BY <UserEnum>w;
            ''',
            # the enum ordering is not like str, but like the real phrase
            ['Lorem', 'ipsum', 'dolor', 'sit', 'amet'],
        )

        # validate user enum
        await self.assert_query_result(
            r'''
            SELECT <str>{O.o0, O.o1, O.o2};
            ''',
            {'ipsum', 'Lorem', 'dolor'},
        )

        await self.assert_query_result(
            r'''
            SELECT <str>(
                SELECT _ := {O.o0, O.o1, O.o2}
                ORDER BY _
            );
            ''',
            [
                'Lorem', 'ipsum', 'dolor',
            ]
        )

        await self.assert_query_result(
            r'''
            SELECT {O.o0, O.o1, O.o2} IS UserEnum;
            ''',
            [True, True, True],
        )

        # validate collection properties
        await self.assert_query_result(
            r'''
            SELECT P {
                plink0: {val, @p0},
                plink1: {val, @p1},
                p2,
                p3,
            };
            ''',
            [
                {
                    'plink0': {'val': 'E00', '@p0': ['hello', 'world']},
                    'plink1': {'val': 'E00', '@p1': [2.5, -4.25]},
                    'p2': ['hello', 'world'],
                    'p3': [2.5, -4.25]
                }
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT Q {q0, q1, q2, q3};
            ''',
            [
                {
                    'q0': [2, False],
                    'q1': ['p3', 3.33],
                    'q2': {'x': 2, 'y': False},
                    'q3': {'x': 'p11', 'y': 3.33},
                }
            ],
        )

        # validate multiple inheritance
        await self.assert_query_result(
            r'''
            SELECT S {name, s}
            ORDER BY .name;
            ''',
            [
                {'name': 'name0', 's': 's0'},
                {'name': 'name1', 's': 's1'},
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT T {name, t}
            ORDER BY .name;
            ''',
            [
                {'name': 'name0', 't': 't0'},
                {'name': 'name1', 't': 't1'},
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT V {name, s, t, u};
            ''',
            [
                {
                    'name': 'name1',
                    's': 's1',
                    't': 't1',
                    'u': 'u1',
                },
            ],
        )

        # validate aliases
        await self.assert_query_result(
            r'''
            SELECT Primes;
            ''',
            {2, 3, 5, 7},
        )

        await self.assert_query_result(
            r'''
            SELECT AliasP {
                name,
                plink0: {val, @p0},
                plink1: {val, @p1},
                p2,
                p3,
                f: {
                    num,
                    single_link: {val},
                    multi_link: {val} ORDER BY .val,
                    k: {k},
                },
            };
            ''',
            [
                {
                    'name': 'alias P',
                    'plink0': {'val': 'E00', '@p0': ['hello', 'world']},
                    'plink1': {'val': 'E00', '@p1': [2.5, -4.25]},
                    'p2': ['hello', 'world', '!'],
                    'p3': [2.5, -4.25],
                    'f': [
                        {
                            'num': 8,
                            'single_link': {'val': 'F00'},
                            'multi_link': [
                                {'val': 'F01'},
                                {'val': 'F02'},
                                {'val': 'F03'},
                            ],
                            'k': {'k': 'k0'},
                        },
                    ],
                }
            ],
        )

        # validate self/mutually-referencing types
        await self.assert_query_result(
            r'''
            SELECT W {
                name,
                w: {
                    name
                }
            }
            ORDER BY .name;
            ''',
            [
                {'name': 'w0', 'w': None},
                {'name': 'w1', 'w': {'name': 'w2'}},
                {'name': 'w2', 'w': None},
                {'name': 'w3', 'w': {'name': 'w4'}},
                {'name': 'w4', 'w': {'name': 'w3'}},
            ],
        )

        # validate self/mutually-referencing types
        await self.assert_query_result(
            r'''
            SELECT X {
                name,
                y: {
                    name,
                    x: {
                        name
                    }
                }
            }
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'x0',
                    'y': {
                        'name': 'y0',
                        'x': {
                            'name': 'x0',
                        },
                    },
                },
            ],
        )

        # validate self/mutually-referencing types
        await self.assert_query_result(
            r'''
            SELECT Z {
                ck: {
                    typename := .__type__.name,
                },
                stw: {
                    name,
                    typename := .__type__.name,
                } ORDER BY .typename,
            }
            ORDER BY .ck.typename;
            ''',
            [
                {
                    'ck': {'typename': 'default::C'},
                    'stw': [
                        {'name': 'name0', 'typename': 'default::S'}
                    ],
                },
                {
                    'ck': {'typename': 'default::K'},
                    'stw': [
                        {'name': 'name0', 'typename': 'default::S'},
                        {'name': 'name0', 'typename': 'default::T'},
                        {'name': 'w1', 'typename': 'default::W'},
                    ],
                },
            ],
        )

        # validate cross module types
        await self.assert_query_result(
            r'''
            SELECT DefA {a};
            ''',
            [
                {
                    'a': 'DefA',
                },
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT DefB {
                name,
                other: {
                    b,
                    blink: {
                        a
                    }
                }
            };
            ''',
            [
                {
                    'name': 'test0',
                    'other': {
                        'b': 'TestB',
                        'blink': {
                            'a': 'DefA',
                        },
                    },
                },
            ],
        )

        await self.assert_query_result(
            r'''
            SELECT DefC {
                name,
                other: {
                    c,
                    clink: {
                        name
                    }
                }
            };
            ''',
            [
                {
                    'name': 'test1',
                    'other': {
                        'c': 'TestC',
                        'clink': {
                            'name': 'test1',
                        },
                    },
                },
            ],
        )

        # validate on delete settings
        await self.assert_query_result(
            r'''
            SELECT SourceA {
                name,
                link1: {
                    name,
                },
            }
            FILTER .name = 's1';
            ''',
            [
                {
                    'name': 's1',
                    'link1': {
                        'name': 't1',
                    },
                },
            ],
        )

        await self.con.execute(r'DELETE TargetA FILTER .name = "t1"')

        await self.assert_query_result(
            r'''
            SELECT SourceA {name}
            FILTER .name = 's1';
            ''',
            [],
        )

        # validate on delete settings
        await self.assert_query_result(
            r'''
            SELECT SourceA {
                name,
                link2: {
                    name,
                },
            }
            FILTER .name = 's2';
            ''',
            [
                {
                    'name': 's2',
                    'link2': {
                        'name': 't2',
                    },
                },
            ],
        )

        await self.con.execute(r'DELETE TargetA FILTER .name = "t2"')

        await self.assert_query_result(
            r'''
            SELECT SourceA {
                name,
                link2: {
                    name,
                },
            }
            FILTER .name = 's2';
            ''',
            [
                {
                    'name': 's2',
                    'link2': None,
                },
            ],
        )

        # validate on delete settings
        await self.assert_query_result(
            r'''
            SELECT SourceA {
                name,
                link0: {
                    name,
                },
            }
            FILTER .name = 's0';
            ''',
            [
                {
                    'name': 's0',
                    'link0': {
                        'name': 't0',
                    },
                },
            ],
        )

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'prohibited by link target policy'):
            async with self.con.transaction():
                await self.con.execute(r'DELETE TargetA FILTER .name = "t0"')

        # validate constraints
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'must be greater than 5'):
            async with self.con.transaction():
                await self.con.execute(r'SELECT <UserInt>1;')

        # validate constraints
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'must be no longer than 5 characters'):
            async with self.con.transaction():
                await self.con.execute(r'SELECT <UserStr>"qwerty";')

        # validate constraints
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'must be greater than 3'):
            async with self.con.transaction():
                await self.con.execute(r"INSERT M {m0 := 1, m1 := '1'};")

        # validate constraints
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'must be no longer than 3 characters'):
            async with self.con.transaction():
                await self.con.execute(r"INSERT M {m0 := 4, m1 := '12345'};")

        # validate constraints
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'name violates exclusivity constraint'):
            async with self.con.transaction():
                await self.con.execute(r"INSERT W {name := 'w0'};")

        # validate constraints
        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'missing value for required property'):
            async with self.con.transaction():
                await self.con.execute(r"INSERT C;")

        # validate constraints
        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'missing value for required link'):
            async with self.con.transaction():
                await self.con.execute(r"INSERT F {num := 999};")

        # validate read-only
        await self.assert_query_result(
            r'''
            SELECT ROPropsA {
                name,
                rop0,
                rop1,
            }
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'ro0',
                    'rop0': None,
                    'rop1': int,
                },
                {
                    'name': 'ro1',
                    'rop0': 100,
                    'rop1': int,
                },
                {
                    'name': 'ro2',
                    'rop0': None,
                    'rop1': -2,
                },
            ],
        )

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rop0.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROPropsA
                    SET {
                        rop0 := 99,
                    };
                    ''')

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rop1.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROPropsA
                    SET {
                        rop1 := 99,
                    };
                    ''')

        # validate read-only
        await self.assert_query_result(
            r'''
            SELECT ROLinksA {
                name,
                rol0: {val},
                rol1: {val},
                rol2: {val} ORDER BY .val,
            }
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'ro0',
                    'rol0': None,
                    'rol1': {'val': 'D00'},
                    'rol2': [{'val': 'D01'}, {'val': 'D02'}]
                },
                {
                    'name': 'ro1',
                    'rol0': {'val': 'F00'},
                    'rol1': {'val': 'D00'},
                    'rol2': [{'val': 'D01'}, {'val': 'D02'}],
                },
                {
                    'name': 'ro2',
                    'rol0': None,
                    'rol1': {'val': 'F00'},
                    'rol2': [{'val': 'D01'}, {'val': 'D02'}]
                },
                {
                    'name': 'ro3',
                    'rol0': None,
                    'rol1': {'val': 'D00'},
                    'rol2': [{'val': 'F01'}, {'val': 'F02'}]
                },
            ],
        )

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rol0.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROLinksA
                    SET {
                        rol0 := <C>{},
                    };
                    ''')

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rol1.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROLinksA
                    SET {
                        rol1 := <C>{},
                    };
                    ''')

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rol2.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROLinksA
                    SET {
                        rol2 := <C>{},
                    };
                    ''')

        # validate read-only
        await self.assert_query_result(
            r'''
            SELECT ROLinksB {
                name,
                rol0: {val, @rolp00, @rolp01},
                rol1: {val, @rolp10, @rolp11} ORDER BY .val,
            }
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'ro0',
                    'rol0': {'val': 'D00', '@rolp00': None, '@rolp01': int},
                    'rol1': [
                        {'val': 'D01', '@rolp10': None, '@rolp11': int},
                        {'val': 'D02', '@rolp10': None, '@rolp11': int},
                    ],
                },
                {
                    'name': 'ro1',
                    'rol0': {'val': 'D00', '@rolp00': 99, '@rolp01': int},
                    'rol1': [
                        {'val': 'D01', '@rolp10': 99, '@rolp11': int},
                        {'val': 'D02', '@rolp10': 98, '@rolp11': int},
                    ],
                },
                {
                    'name': 'ro2',
                    'rol0': {'val': 'E00', '@rolp00': None, '@rolp01': -10},
                    'rol1': [
                        {'val': 'E01', '@rolp10': None, '@rolp11': -1},
                        {'val': 'E02', '@rolp10': None, '@rolp11': -2},
                    ],
                },
            ],
        )

        """XXX: uncomment the below once direct link property updates are
                implemented
        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rolp00.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROLinksB
                    SET {
                        rol0: {@rolp00 := 1},
                    };
                    ''')

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rolp01.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROLinksB
                    SET {
                        rol0: {@rolp01 := 1},
                    };
                    ''')

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rolp10.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROLinksB
                    SET {
                        rol1: {@rolp10 := 1},
                    };
                    ''')

        # validate read-only
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'rolp11.*read-only'):
            async with self.con.transaction():
                await self.con.execute(
                    r'''
                    UPDATE ROLinksB
                    SET {
                        rol1: {@rolp11 := 1},
                    };
                    ''')
        """


class TestDump01(tb.StableDumpTestCase, DumpTestCaseMixin):
    SCHEMA_TEST = os.path.join(os.path.dirname(__file__), 'schemas',
                               'dump01_test.esdl')
    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump01_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump01_setup.edgeql')

    async def test_dump01_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump01_branch_schema(self):
        await self.check_branching(
            include_data=False,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump01_branch_data(self):
        await self.check_branching(
            include_data=True,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDump01Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dump01',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    pass
