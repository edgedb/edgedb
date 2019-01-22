#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from edb.tools import test


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
                    name
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

    async def test_edgeql_introspection_objtype_05(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                is_abstract,
                pointers: {
                    name,
                    cardinality,
                } FILTER .name LIKE 'test::%'
                  ORDER BY .name
            }
            FILTER ObjectType.name = 'test::User';
        """, [
            [{
                'name': 'test::User',
                'is_abstract': False,
                'pointers': [{
                    'name': 'test::name',
                    'cardinality': 'ONE',
                }, {
                    'name': 'test::todo',
                    'cardinality': 'MANY',
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
                    cardinality,
                    target: {
                        name
                    }
                } ORDER BY .name
            }
            FILTER .name LIKE '%Comment';
        """, [
            [{
                'name': 'test::Comment',
                'links': [{
                    'name': 'std::__type__',
                    'target': {'name': 'schema::Type'},
                    'cardinality': 'ONE',
                }, {
                    'name': 'test::issue',
                    'target': {'name': 'test::Issue'},
                    'cardinality': 'ONE',
                }, {
                    'name': 'test::owner',
                    'target': {'name': 'test::User'},
                    'cardinality': 'ONE',
                }, {
                    'name': 'test::parent',
                    'target': {'name': 'test::Comment'},
                    'cardinality': 'ONE',
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
                ObjectType.links.cardinality = 'MANY'
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
            # get all user-defined object types with at least one multi link
            WITH MODULE schema
            SELECT `ObjectType` {
                name,
            }
            FILTER
                `ObjectType`.name LIKE 'test::%'
                AND
                `ObjectType`.links.cardinality = 'MANY'
            ORDER BY `ObjectType`.name;
        """, [
            [{
                'name': 'test::Issue',
            }, {
                'name': 'test::User',
            }]
        ])

    async def test_edgeql_introspection_objtype_09(self):
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
                                'name': 'anytype'
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
                            id,
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
                                'name': 'anytype'
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
                        [IS schema::Array].element_type: {
                            name
                        }
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
                            'name': 'array',
                            'element_type': {
                                'name': 'anytype'
                            }
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
                args: {
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
                'args': [{
                    'num': 0,
                    '@value': '10000'
                }]
            }]
        ])

    @test.xfail('There should be at least 1 Database.')
    async def test_edgeql_introspection_meta_01(self):
        await self.assert_query_result(r'''
            SELECT count(schema::Database) > 0;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_02(self):
        await self.assert_query_result(r'''
            SELECT count(schema::Module) > 0;
            SELECT 'std' IN schema::Module.name;
        ''', [
            [True],
            [True],
        ])

    @test.xfail('''
        There should be many more than 10 Objects in the DB due to
        all the schema objects from standard library.
    ''')
    async def test_edgeql_introspection_meta_03(self):
        await self.assert_query_result(r'''
            SELECT count(Object) > 10;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_04(self):
        await self.assert_query_result(r'''
            SELECT count(schema::Type) > 0;
            SELECT count(schema::ScalarType) > 0;
            SELECT count(schema::ObjectType) > 0;

            SELECT count(schema::ScalarType) < count(schema::Type);
            SELECT count(schema::ObjectType) < count(schema::Type);

            SELECT DISTINCT (schema::ScalarType IS schema::Type);
            SELECT DISTINCT (schema::ObjectType IS schema::Type);
        ''', [
            [True],
            [True],
            [True],

            [True],
            [True],

            [True],
            [True],
        ])

    async def test_edgeql_introspection_meta_05(self):
        await self.assert_query_result(r'''
            SELECT count(schema::PseudoType) > 0;
            SELECT count(schema::PseudoType) < count(schema::Type);
            SELECT DISTINCT (schema::PseudoType IS schema::Type);

        ''', [
            [True],
            [True],
            [True],
        ])

    @test.xfail('''
        ContainerType queries cause the following error:
        relation "edgedbss.6b1e0cfa-1511-11e9-8f2e-b5ee4369b429" does not exist
    ''')
    async def test_edgeql_introspection_meta_06(self):
        await self.assert_query_result(r'''
            SELECT count(schema::ContainerType) > 0;
            SELECT count(schema::Array) > 0;
            SELECT count(schema::Tuple) > 0;

            SELECT count(schema::ContainerType) < count(schema::Type);
            SELECT count(schema::Array) < count(schema::ContainerType);
            SELECT count(schema::Tuple) < count(schema::ContainerType);

            SELECT DISTINCT (schema::ContainerType IS schema::Type);
            SELECT DISTINCT (schema::Array IS schema::ContainerType);
            SELECT DISTINCT (schema::Tuple IS schema::ContainerType);
        ''', [
            [True],
            [True],
            [True],

            [True],
            [True],
            [True],

            [True],
            [True],
            [True],
        ])

    async def test_edgeql_introspection_meta_07(self):
        await self.assert_query_result(r'''
            SELECT count(schema::Operator) > 0;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_08(self):
        await self.assert_query_result(r'''
            SELECT count(schema::Cast) > 0;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_09(self):
        await self.assert_query_result(r'''
            SELECT count(schema::Constraint) > 0;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_10(self):
        await self.assert_query_result(r'''
            SELECT count(schema::Function) > 0;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_11(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT DISTINCT (
                SELECT Type IS Array
                FILTER Type.name = 'array'
            );
        ''', [
            [True],
        ])

    @test.xfail('Tuples get registered as Arrays.')
    async def test_edgeql_introspection_meta_12(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT DISTINCT (
                SELECT Type IS Tuple
                FILTER Type.name = 'tuple'
            );
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_13(self):
        # make sure that ALL schema Objects are std::Objects
        res = await self.query(r"""
            WITH MODULE schema
            SELECT count(Object);

            WITH MODULE schema
            SELECT Object IS std::Object;
        """)

        self.assert_data_shape(res[1], [True] * res[0][0])

    async def test_edgeql_introspection_meta_14(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT InheritingObject {
                name
            }
            FILTER
                re_test(r'^test::\w+$', InheritingObject.name)
                AND InheritingObject.name NOT LIKE '%:Virtual_%'
                AND InheritingObject.is_abstract
            ORDER BY InheritingObject.name;
        """, [
            [
                {'name': 'test::Dictionary'},
                {'name': 'test::Named'},
                {'name': 'test::Owned'},
                {'name': 'test::Text'},
                {'name': 'test::my_enum'},
                {'name': 'test::todo'},
            ]
        ])

    async def test_edgeql_introspection_meta_15(self):
        res = await self.query(r'''
            WITH MODULE schema
            SELECT `Type`;
        ''')
        # just test that there's a non-empty return set for this query
        self.assertTrue(res[0])

    async def test_edgeql_introspection_meta_16(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT ScalarType[IS Object] IS ScalarType LIMIT 1;
        ''', [
            [True],
        ])

    async def test_edgeql_introspection_meta_17(self):
        result = await self.con.fetch('''
            WITH MODULE schema
            SELECT ObjectType {
                id,
                name,

                links: {
                    name,
                    cardinality,
                    target,
                },

                properties: {
                    name,
                    cardinality,
                    target,
                },
            }
            FILTER .name = 'std::Object';
        ''')

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, 'std::Object')
        self.assertEqual(result[0].links[0].name, 'std::__type__')
        self.assertIsNotNone(result[0].links[0].target.id)
        self.assertIsNotNone(result[0].properties[0].target.id)

    async def test_edgeql_introspection_count_01(self):
        await self.query(r"""
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

        await self.query(r"""
            DELETE test::Priority;
            DELETE test::Status;
            DELETE test::User;
        """)

    async def test_edgeql_introspection_database_01(self):
        res = await self.query(r"""
            WITH MODULE schema
            SELECT count(Database.name);
        """)

        self.assertGreater(res[0][0], 0)
