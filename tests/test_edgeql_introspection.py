##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.server import _testbase as tb


class TestIntrospection(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_introspection_objtype_01(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT `ObjectType` {
                name
            }
            FILTER `ObjectType`.name LIKE 'test::%'
            ORDER BY `ObjectType`.name;
        """, [
            [
                {'name': 'test::Comment'},
                {'name': 'test::Dictionary'},
                {'name': 'test::File'},
                {'name': 'test::Issue'},
                {'name': 'test::LogEntry'},
                {'name': 'test::Named'},
                {'name': 'test::Owned'},
                {'name': 'test::Priority'},
                {'name': 'test::Publication'},
                {'name': 'test::Status'},
                {'name': 'test::Text'},
                {'name': 'test::URL'},
                {'name': 'test::User'}
            ]
        ])

    async def test_edgeql_introspection_objtype_02(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                is_abstract,
                pointers: {
                    name,
                } ORDER BY .name
            }
            FILTER ObjectType.name = 'test::User';
        """, [
            [{
                'name': 'test::User',
                'is_abstract': False,
                'pointers': [{
                    'name': 'std::__type__',
                }, {
                    'name': 'std::id',
                }, {
                    'name': 'test::name',
                }, {
                    'name': 'test::todo',
                }]
            }]
        ])

    async def test_edgeql_introspection_objtype_03(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                is_abstract,
                pointers: {
                    name,
                } ORDER BY .name
            }
            FILTER ObjectType.name = 'test::Owned';
        """, [
            [{
                'name': 'test::Owned',
                'is_abstract': True,
                'pointers': [{
                    'name': 'std::__type__',
                }, {
                    'name': 'std::id',
                }, {
                    'name': 'test::owner',
                }]
            }]
        ])

    async def test_edgeql_introspection_objtype_04(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                is_abstract,
                pointers: {
                    name,
                    attributes: {
                        name,
                        @value
                    } FILTER .name = 'stdattrs::name'
                      ORDER BY .name
                } ORDER BY .name
            }
            FILTER ObjectType.name = 'test::User';
        """, [
            [{
                'name': 'test::User',
                'is_abstract': False,
                'pointers': [{
                    'name': 'std::__type__',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'std::__type__'
                    }]
                }, {
                    'name': 'std::id',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'std::id'
                    }]
                }, {
                    'name': 'test::name',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'test::name'
                    }]
                }, {
                    'name': 'test::todo',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'test::todo'
                    }]
                }]
            }]
        ])

    async def test_edgeql_introspection_objtype_05(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                is_abstract,
                pointers: {
                    attributes: {
                        name,
                        @value
                    } FILTER EXISTS @value
                      ORDER BY .name
                } FILTER .name LIKE 'test::%'
                  ORDER BY .name
            }
            FILTER ObjectType.name = 'test::User';
        """, [
            [{
                'name': 'test::User',
                'is_abstract': False,
                'pointers': [{
                    'attributes': [
                        {'name': 'stdattrs::cardinality', '@value': '11'},
                        {'name': 'stdattrs::is_abstract', '@value': 'false'},
                        {'name': 'stdattrs::is_derived', '@value': 'false'},
                        {'name': 'stdattrs::is_final', '@value': 'false'},
                        {'name': 'stdattrs::is_virtual', '@value': 'false'},
                        {'name': 'stdattrs::name', '@value': 'test::name'},
                        {'name': 'stdattrs::readonly', '@value': 'false'},
                        {'name': 'stdattrs::required', '@value': 'true'},
                    ]
                }, {
                    'attributes': [
                        {'name': 'stdattrs::cardinality', '@value': '**'},
                        {'name': 'stdattrs::is_abstract', '@value': 'false'},
                        {'name': 'stdattrs::is_derived', '@value': 'false'},
                        {'name': 'stdattrs::is_final', '@value': 'false'},
                        {'name': 'stdattrs::is_virtual', '@value': 'false'},
                        {'name': 'stdattrs::name', '@value': 'test::todo'},
                        {'name': 'stdattrs::readonly', '@value': 'false'},
                        {'name': 'stdattrs::required', '@value': 'false'},
                    ]
                }]
            }]
        ])

    async def test_edgeql_introspection_objtype_06(self):
        await self.assert_query_result(r"""
            # get all links, mappings and target names for Comment
            WITH MODULE schema
            SELECT ObjectType {
                name,
                links: {
                    name,
                    target: {
                        name
                    },
                    attributes: {
                        name,
                        @value
                    } FILTER .name LIKE '%cardinality'
                } ORDER BY .name
            }
            FILTER .name LIKE '%Comment';
        """, [
            [{
                'name': 'test::Comment',
                'links': [{
                    'name': 'std::__type__',
                    'target': {'name': 'schema::Type'},
                    'attributes': [{
                        'name': 'stdattrs::cardinality',
                        '@value': '*1',
                    }],
                }, {
                    'name': 'test::issue',
                    'target': {'name': 'test::Issue'},
                    'attributes': [{
                        'name': 'stdattrs::cardinality',
                        '@value': '*1',
                    }],
                }, {
                    'name': 'test::owner',
                    'target': {'name': 'test::User'},
                    'attributes': [{
                        'name': 'stdattrs::cardinality',
                        '@value': '*1',
                    }],
                }, {
                    'name': 'test::parent',
                    'target': {'name': 'test::Comment'},
                    'attributes': [{
                        'name': 'stdattrs::cardinality',
                        '@value': '*1',
                    }],
                }]
            }]
        ])

    async def test_edgeql_introspection_objtype_07(self):
        await self.assert_query_result(r"""
            # get all user-defined object types with at least one ** link
            WITH MODULE schema
            SELECT ObjectType {
                name,
            }
            FILTER
                ObjectType.name LIKE 'test::%'
                AND
                ObjectType.links.attributes.name = 'stdattrs::cardinality'
                AND
                ObjectType.links.attributes@value = '**'
            ORDER BY ObjectType.name;
        """, [
            [{
                'name': 'test::Issue',
            }, {
                'name': 'test::User',
            }]
        ])

    async def test_edgeql_introspection_objtype_08(self):
        await self.assert_query_result(r"""
            # get all user-defined object types with at least one 1* link
            WITH MODULE schema
            SELECT `ObjectType` {
                name,
            }
            FILTER
                `ObjectType`.name LIKE 'test::%'
                AND
                `ObjectType`.links.attributes.name = 'stdattrs::cardinality'
                AND
                `ObjectType`.links.attributes@value = '1*'
            ORDER BY `ObjectType`.name;
        """, [
            [{
                'name': 'test::Issue',
            }]
        ])

    async def test_edgeql_introspection_objtype_09(self):
        await self.assert_query_result(r"""
            # get all user-defined object types with at least one 1* link
            WITH MODULE schema
            SELECT `ObjectType` {
                name,
            }
            FILTER
                `ObjectType`.name LIKE 'test::%'
                AND
                `ObjectType`.<target[IS `Link`].attributes.name =
                    'stdattrs::cardinality'
                AND
                `ObjectType`.<target[IS `Link`].attributes@value = '1*'
            ORDER BY `ObjectType`.name;
        """, [
            [{
                'name': 'test::LogEntry',
            }]
        ])

    async def test_edgeql_introspection_objtype_10(self):
        await self.assert_query_result(r"""
            # get all user-defined object types with at least one
            # array property
            WITH MODULE schema
            SELECT ObjectType {
                properties: {
                    target: Array {
                        name,
                        element_type: {
                            name
                        }
                    }
                } FILTER .name = 'test::tags'
            }
            FILTER
                .name = 'test::Issue';
        """, [
            [{
                'properties': [
                    {
                        'target': {
                            'name': 'array',
                            'element_type': {
                                'name': 'std::str'
                            }
                        }
                    }
                ]
            }]
        ])

    async def test_edgeql_introspection_link_01(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT Link {
                name,
                properties: {
                    name,
                } ORDER BY Link.properties.name
            }
            FILTER
                Link.name = 'test::todo'
                AND EXISTS Link.source;
        """, [
            [{
                'name': 'test::todo',
                'properties': [{
                    'name': 'std::source',
                }, {
                    'name': 'std::target',
                }, {
                    'name': 'test::rank',
                }]
            }]
        ])

    async def test_edgeql_introspection_constraint_01(self):
        await self.assert_query_result(r"""
            SELECT schema::Constraint {
                name,
                params: {
                    num,
                    type: schema::Array {
                        name,
                        element_type: {
                            name
                        }
                    }
                }
            } FILTER
                .name LIKE '%my_enum%' AND
                NOT EXISTS .<constraints;
        """, [
            [{
                'name': 'test::my_enum',
                'params': [
                    {
                        'num': 0,
                        'type': {
                            'name': 'array',
                            'element_type': {
                                'name': 'std::any'
                            }
                        }
                    }
                ]
            }]
        ])

    async def test_edgeql_introspection_constraint_02(self):
        await self.assert_query_result(r"""
            SELECT schema::Constraint {
                name,
                params: {
                    num,
                    type: {
                        name,
                        [IS schema::Array].element_type: {
                            name
                        }
                    }
                }
            } FILTER
                .name LIKE '%my_enum%' AND
                NOT EXISTS .<constraints;
        """, [
            [{
                'name': 'test::my_enum',
                'params': [
                    {
                        'num': 0,
                        'type': {
                            'name': 'array',
                            'element_type': {
                                'name': 'std::any'
                            }
                        }
                    }
                ]
            }]
        ])

    async def test_edgeql_introspection_constraint_03(self):
        await self.assert_query_result(r"""
            SELECT schema::Constraint {
                name,
                params: {
                    num,
                    kind,
                    type: {
                        name,
                    }
                }
            } FILTER
                .name LIKE '%std::enum%' AND
                NOT EXISTS .<constraints;
        """, [
            [{
                'name': 'std::enum',
                'params': [
                    {
                        'num': 0,
                        'kind': 'VARIADIC',
                        'type': {
                            'name': 'std::any',
                        }
                    }
                ]
            }]
        ])

    async def test_edgeql_introspection_constraint_04(self):
        await self.assert_query_result(r"""
            SELECT schema::Constraint {
                name,
                subject: {
                    name
                },
                params: {
                    num,
                    @value
                } ORDER BY .num
            } FILTER .subject.name = 'test::body';
        """, [
            [{
                'name': 'std::maxlength',
                'subject': {
                    'name': 'test::body'
                },
                'params': [{
                    'num': 0,
                    '@value': '10000'
                }]
            }]
        ])

    async def test_edgeql_introspection_meta_01(self):
        # make sure that ALL schema Objects are std::Objects
        res = await self.con.execute(r"""
            WITH MODULE schema
            SELECT count(Object);

            WITH MODULE schema
            SELECT Object IS std::Object;
        """)

        self.assert_data_shape(res[1], [True] * res[0][0])

    async def test_edgeql_introspection_meta_02(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT Object {
                name
            }
            FILTER re_test(Object.name, '^test::\w+$')
            ORDER BY Object.name;
        """, [
            [
                {'name': 'test::Comment'},
                {'name': 'test::Dictionary'},
                {'name': 'test::File'},
                {'name': 'test::Issue'},
                {'name': 'test::LogEntry'},
                {'name': 'test::Named'},
                {'name': 'test::Owned'},
                {'name': 'test::Priority'},
                {'name': 'test::Publication'},
                {'name': 'test::Status'},
                {'name': 'test::Text'},
                {'name': 'test::URL'},
                {'name': 'test::User'},
                {'name': 'test::Virtual_1bfb5401e8affec4c5'
                         '63a35ae764c6a56f7ca60e1d0bb8a0'},
                {'name': 'test::address'},
                {'name': 'test::body'},
                {'name': 'test::due_date'},
                {'name': 'test::issue'},
                {'name': 'test::issue_num_t'},
                {'name': 'test::my_enum'},
                {'name': 'test::name'},
                {'name': 'test::number'},
                {'name': 'test::owner'},
                {'name': 'test::parent'},
                {'name': 'test::priority'},
                {'name': 'test::rank'},
                {'name': 'test::references'},
                {'name': 'test::related_to'},
                {'name': 'test::spent_time'},
                {'name': 'test::start_date'},
                {'name': 'test::status'},
                {'name': 'test::tags'},
                {'name': 'test::time_estimate'},
                {'name': 'test::time_spent_log'},
                {'name': 'test::title'},
                {'name': 'test::todo'},
                {'name': 'test::watchers'},
            ]
        ])

    async def test_edgeql_introspection_meta_03(self):
        res = await self.con.execute(r'''
            WITH MODULE schema
            SELECT `Type`;
        ''')
        # just test that there's a non-empty return set for this query
        self.assertTrue(res[0])

    async def test_edgeql_introspection_meta_04(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT ScalarType[IS Object] IS ScalarType LIMIT 1;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_count_01(self):
        await self.con.execute(r"""
            WITH MODULE test
            INSERT Priority {
                name := 'High'
            };

            WITH MODULE test
            INSERT Priority {
                name := 'Low'
            };

            WITH MODULE test
            INSERT Status {
                name := 'Open'
            };

            WITH MODULE test
            INSERT Status {
                name := 'Closed'
            };

            WITH MODULE test
            INSERT Status {
                name := 'Flagged'
            };

            WITH MODULE test
            INSERT User {
                name := 'Elvis'
            };

            WITH MODULE test
            INSERT User {
                name := 'Yury'
            };
        """)

        await self.assert_query_result(r"""
            # Count the number of objects for each object type in module
            # test. This is impossible to do without introspection for
            # object types that have 0 objects.
            WITH MODULE schema
            SELECT ObjectType {
                name,
                count := (
                    WITH CARDINALITY '1'
                    SELECT std::count(ObjectType.<__type__)
                )
            }
            FILTER ObjectType.name LIKE 'test::%'
            ORDER BY ObjectType.name;
        """, [
            [
                {'name': 'test::Comment', 'count': 0},
                {'name': 'test::Dictionary', 'count': 0},
                {'name': 'test::File', 'count': 0},
                {'name': 'test::Issue', 'count': 0},
                {'name': 'test::LogEntry', 'count': 0},
                {'name': 'test::Named', 'count': 0},
                {'name': 'test::Owned', 'count': 0},
                {'name': 'test::Priority', 'count': 2},
                {'name': 'test::Publication', 'count': 0},
                {'name': 'test::Status', 'count': 3},
                {'name': 'test::Text', 'count': 0},
                {'name': 'test::URL', 'count': 0},
                {'name': 'test::User', 'count': 2},
            ]
        ])

        await self.con.execute(r"""
            DELETE test::Priority;
            DELETE test::Status;
            DELETE test::User;
        """)
