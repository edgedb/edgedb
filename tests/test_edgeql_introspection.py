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
import textwrap

from edb.testbase import server as tb
from edb.tools import test


class TestIntrospection(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_introspection_objtype_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT `ObjectType` {
                    name
                }
                FILTER `ObjectType`.name LIKE 'test::%'
                ORDER BY `ObjectType`.name;
            """,
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
        )

    async def test_edgeql_introspection_objtype_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    is_abstract,
                    pointers: {
                        name,
                    } ORDER BY .name
                }
                FILTER ObjectType.name = 'test::User';
            """,
            [{
                'name': 'test::User',
                'is_abstract': False,
                'pointers': [{
                    'name': '__type__',
                }, {
                    'name': 'id',
                }, {
                    'name': 'name',
                }, {
                    'name': 'todo',
                }]
            }]
        )

    async def test_edgeql_introspection_objtype_03(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    is_abstract,
                    pointers: {
                        name,
                    } ORDER BY .name
                }
                FILTER ObjectType.name = 'test::Owned';
            """,
            [{
                'name': 'test::Owned',
                'is_abstract': True,
                'pointers': [{
                    'name': '__type__',
                }, {
                    'name': 'id',
                }, {
                    'name': 'owner',
                }]
            }]
        )

    async def test_edgeql_introspection_objtype_04(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    is_abstract,
                    pointers: {
                        name
                    } ORDER BY .name
                }
                FILTER ObjectType.name = 'test::User';
            """,
            [{
                'name': 'test::User',
                'is_abstract': False,
                'pointers': [{
                    'name': '__type__',
                }, {
                    'name': 'id',
                }, {
                    'name': 'name',
                }, {
                    'name': 'todo',
                }]
            }]
        )

    async def test_edgeql_introspection_objtype_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    is_abstract,
                    pointers: {
                        name,
                        cardinality,
                    } FILTER @is_local
                      ORDER BY .name
                }
                FILTER ObjectType.name = 'test::User';
            """,
            [{
                'name': 'test::User',
                'is_abstract': False,
                'pointers': [{
                    'name': 'todo',
                    'cardinality': 'MANY',
                }]
            }]
        )

    async def test_edgeql_introspection_objtype_06(self):
        await self.assert_query_result(
            r"""
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
            """,
            [{
                'name': 'test::Comment',
                'links': [{
                    'name': '__type__',
                    'target': {'name': 'schema::Type'},
                    'cardinality': 'ONE',
                }, {
                    'name': 'issue',
                    'target': {'name': 'test::Issue'},
                    'cardinality': 'ONE',
                }, {
                    'name': 'owner',
                    'target': {'name': 'test::User'},
                    'cardinality': 'ONE',
                }, {
                    'name': 'parent',
                    'target': {'name': 'test::Comment'},
                    'cardinality': 'ONE',
                }]
            }]
        )

    async def test_edgeql_introspection_objtype_07(self):
        await self.assert_query_result(
            r"""
                # get all user-defined object types with at least one ** link
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                }
                FILTER
                    ObjectType.name LIKE 'test::%'
                    AND
                    ObjectType.links.cardinality = <Cardinality>'MANY'
                ORDER BY ObjectType.name;
            """,
            [
                {
                    'name': 'test::Issue',
                },
                {
                    'name': 'test::User',
                }
            ]
        )

    async def test_edgeql_introspection_objtype_08(self):
        await self.assert_query_result(
            r"""
                # get all user-defined object types with
                # at least one multi link
                WITH MODULE schema
                SELECT `ObjectType` {
                    name,
                }
                FILTER
                    `ObjectType`.name LIKE 'test::%'
                    AND
                    ObjectType.links.cardinality = <Cardinality>'MANY'
                ORDER BY `ObjectType`.name;
            """,
            [
                {
                    'name': 'test::Issue',
                },
                {
                    'name': 'test::User',
                }
            ]
        )

    async def test_edgeql_introspection_objtype_09(self):
        await self.assert_query_result(
            r"""
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
                    } FILTER .name = 'tags'
                }
                FILTER
                    .name = 'test::Issue';
            """,
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
        )

    async def test_edgeql_introspection_bases_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    bases: {
                        name
                    } ORDER BY @index,
                    ancestors: {
                        name
                    } ORDER BY @index
                }
                FILTER
                    .name = 'test::Issue';
            """,
            [{
                'bases': [{
                    'name': 'test::Named',
                }, {
                    'name': 'test::Owned',
                }, {
                    'name': 'test::Text',
                }],

                'ancestors': [{
                    'name': 'test::Named',
                }, {
                    'name': 'test::Owned',
                }, {
                    'name': 'test::Text',
                }, {
                    'name': 'std::Object',
                }],
            }]
        )

    async def test_edgeql_introspection_link_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Link {
                    name,
                    properties: {
                        name,
                    } ORDER BY Link.properties.name
                }
                FILTER
                    Link.name = 'todo'
                    AND EXISTS Link.source;
            """,
            [{
                'name': 'todo',
                'properties': [{
                    'name': 'rank',
                }, {
                    'name': 'source',
                }, {
                    'name': 'target',
                }]
            }]
        )

    async def test_edgeql_introspection_locality(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    properties: {
                        name,
                        @is_local,
                        inherited_fields,
                    } ORDER BY .name
                }
                FILTER
                    .name = 'test::URL'
            """,
            [{
                'properties': [{
                    "name": "address",
                    "inherited_fields": [],
                    "@is_local": True
                }, {
                    "name": "id",
                    "inherited_fields": {
                        "default", "readonly",
                        "required", "cardinality"
                    },
                    "@is_local": False
                }, {
                    "name": "name",
                    "inherited_fields": {
                        "readonly", "required", "cardinality"
                    },
                    "@is_local": False
                }]
            }]
        )

    async def test_edgeql_introspection_constraint_01(self):
        await self.assert_query_result(
            r"""
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
                    } FILTER .type IS schema::Array
                } FILTER
                    .name LIKE '%my_one_of%' AND
                    NOT EXISTS .<constraints;
            """,
            [{
                'name': 'test::my_one_of',
                'params': [
                    {
                        'num': 1,
                        'type': {
                            'name': 'array',
                            'element_type': {
                                'name': 'anytype'
                            }
                        }
                    }
                ]
            }]
        )

    async def test_edgeql_introspection_constraint_02(self):
        await self.assert_query_result(
            r"""
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
                    } FILTER .name != '__subject__'
                } FILTER
                    .name LIKE '%my_one_of%' AND
                    NOT EXISTS .<constraints;
            """,
            [{
                'name': 'test::my_one_of',
                'params': [
                    {
                        'num': 1,
                        'type': {
                            'name': 'array',
                            'element_type': {
                                'name': 'anytype'
                            }
                        }
                    }
                ]
            }]
        )

    async def test_edgeql_introspection_constraint_03(self):
        await self.assert_query_result(
            r"""
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
                    } ORDER BY .num
                } FILTER
                    .name LIKE '%std::one_of%' AND
                    NOT EXISTS .<constraints;
            """,
            [{
                'name': 'std::one_of',
                'params': [
                    {
                        'num': 0,
                        'kind': 'POSITIONAL',
                        'type': {
                            'name': 'anytype',
                        }
                    },
                    {
                        'num': 1,
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
        )

    async def test_edgeql_introspection_constraint_04(self):
        await self.assert_query_result(
            r"""
                SELECT schema::Constraint {
                    name,
                    subject: {
                        name
                    },
                    args: {
                        num,
                        @value
                    } ORDER BY .num
                }
                FILTER
                    .subject.name = 'body'
                    AND .subject[IS schema::Property].source.name
                        = 'test::Text';
            """,
            [{
                'name': 'std::max_len_value',
                'subject': {
                    'name': 'body'
                },
                'args': [{
                    'num': 1,
                    '@value': '10000'
                }]
            }]
        )

    async def test_edgeql_introspection_constraint_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    properties: {
                        name,
                        constraints: {
                            name,
                            expr,
                            annotations: { name, @value },
                            subject: { name },
                            args: { name, @value, type: { name } },
                            return_typemod,
                            return_type: { name },
                            errmessage,
                        },
                    } ORDER BY .name,
                }
                FILTER .name = 'test::Text';
            """,
            [{
                'name': 'test::Text',
                'properties': [
                    {
                        'name': 'body',
                        'constraints': [
                            {
                                'name': 'std::max_len_value',
                                'expr': '(__subject__ <= max)',
                                'annotations': {},
                                'subject': {'name': 'body'},
                                'args': [
                                    {
                                        'name': 'max',
                                        'type': {'name': 'std::int64'},
                                        '@value': '10000'
                                    }
                                ],
                                'return_typemod': 'SINGLETON',
                                'return_type': {'name': 'std::bool'},
                                'errmessage':
                                    '{__subject__} must be no longer than '
                                    '{max} characters.'
                            }
                        ]
                    },
                    {
                        'name': 'id',
                        'constraints': [
                            {
                                'name': 'std::exclusive',
                                'expr': 'std::_is_exclusive(__subject__)',
                                'annotations': {},
                                'subject': {'name': 'id'},
                                'args': {},
                                'return_typemod': 'SINGLETON',
                                'return_type': {'name': 'std::bool'},
                                'errmessage':
                                    '{__subject__} violates exclusivity '
                                    'constraint'
                            }
                        ]
                    }
                ]
            }]
        )

    async def test_edgeql_introspection_constraint_06(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ScalarType {
                    name,
                    constraints: {
                        name,
                        expr,
                        annotations: { name, @value },
                        subject: { name },
                        args: { name, @value, type: { name } },
                        return_typemod,
                        return_type: { name },
                        errmessage,
                    },
                }
                FILTER .name = 'test::EmulatedEnum';
            """,
            [{

                'name': 'test::EmulatedEnum',
                'constraints': [
                    {
                        'name': 'std::one_of',
                        'expr': 'contains(vals, __subject__)',
                        'annotations': {},
                        'subject': {'name': 'test::EmulatedEnum'},
                        'args': [
                            {
                                'name': 'vals',
                                'type': {'name': 'array'},
                                '@value': "['v1', 'v2']"
                            }
                        ],
                        'return_typemod': 'SINGLETON',
                        'return_type': {'name': 'std::bool'},
                        'errmessage':
                            "{__subject__} must be one of: {vals}."
                    }
                ]
            }]
        )

    async def test_edgeql_introspection_function_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT `Function` {
                    name,
                    annotations: { name, @value },
                    params: {
                        kind,
                        name,
                        num,
                        typemod,
                        type: { name },
                        default,
                    },
                    return_typemod,
                    return_type: {
                        name,
                        [IS Tuple].element_types: {
                            name,
                            type: {
                                name
                            }
                        } ORDER BY .num
                    },
                }
                FILTER .name = 'std::count' OR .name = 'sys::get_version'
                ORDER BY .name;
            """,
            [
                {
                    "name": "std::count",
                    "annotations": [],
                    "params": [
                        {
                            "kind": "POSITIONAL",
                            "name": "s",
                            "num": 0,
                            "typemod": "SET OF",
                            "type": {"name": "anytype"},
                            "default": None
                        }
                    ],
                    "return_typemod": "SINGLETON",
                    "return_type": {"name": "std::int64", "element_types": []}
                },
                {
                    "name": "sys::get_version",
                    "annotations": [],
                    "params": [],
                    "return_typemod": "SINGLETON",
                    "return_type": {
                        "name": "tuple",
                        "element_types": [
                            {"name": "major",
                             "type": {"name": "std::int64"}},
                            {"name": "minor",
                             "type": {"name": "std::int64"}},
                            {"name": "stage",
                             "type": {"name": "sys::version_stage"}},
                            {"name": "stage_no",
                             "type": {"name": "std::int64"}},
                            {"name": "local",
                             "type": {"name": "array"}}
                        ]
                    }
                }
            ]
        )

    async def test_edgeql_introspection_function_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT `Function` {
                    name,
                    session_only
                }
                FILTER .name IN {'std::count', 'sys::advisory_lock'}
                ORDER BY .name;
            """,
            [
                {
                    "name": "std::count",
                    "session_only": False,
                },
                {
                    "name": "sys::advisory_lock",
                    "session_only": True,
                }
            ]
        )

    async def test_edgeql_introspection_volatility_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT `Function` {
                    name,
                    volatility
                }
                FILTER .name IN {
                    'std::datetime_current',
                    'std::datetime_of_transaction',
                    'std::re_match'
                }
                ORDER BY .name;
            """,
            [
                {
                    'name': 'std::datetime_current',
                    'volatility': 'VOLATILE'
                },
                {
                    'name': 'std::datetime_of_transaction',
                    'volatility': 'STABLE'
                },
                {
                    'name': 'std::re_match',
                    'volatility': 'IMMUTABLE'
                }
            ]
        )

    async def test_edgeql_introspection_volatility_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT `Operator` {
                    name,
                    params: {
                        name, type: {name}
                    } ORDER BY .name,
                    volatility
                }
                FILTER
                    .name = 'std::+'
                    AND
                    (
                        SELECT .params FILTER .name = 'r'
                    ).type.name = 'std::duration'
                    AND
                    (
                        SELECT .params FILTER .name = 'l'
                    ).type.name IN {'std::duration', 'std::datetime'}
                ORDER BY .name;
            """,
            [
                {
                    'name': 'std::+',
                    'params': [
                        {'name': 'l', 'type': {'name': 'std::datetime'}},
                        {'name': 'r', 'type': {'name': 'std::duration'}},
                    ],
                    'volatility': 'STABLE'
                },
                {
                    'name': 'std::+',
                    'params': [
                        {'name': 'l', 'type': {'name': 'std::duration'}},
                        {'name': 'r', 'type': {'name': 'std::duration'}},
                    ],
                    'volatility': 'IMMUTABLE'
                }
            ]
        )

    async def test_edgeql_introspection_volatility_03(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT `Cast` {
                    from_type: {name},
                    to_type: {name},
                    volatility
                }
                FILTER
                    .from_type.name IN {'std::duration', 'std::datetime'}
                    AND
                    .to_type.name = 'std::str'
                ORDER BY .from_type.name;
            """,
            [
                {
                    'from_type': {'name': 'std::datetime'},
                    'to_type': {'name': 'std::str'},
                    'volatility': 'STABLE'
                },
                {
                    'from_type': {'name': 'std::duration'},
                    'to_type': {'name': 'std::str'},
                    'volatility': 'IMMUTABLE'
                }
            ]
        )

    async def test_edgeql_introspection_meta_01(self):
        await self.assert_query_result(
            r'''
                SELECT count(sys::Database) > 0;
            ''',
            [True],
        )

        # regression test: sys::Database view must have a __tid__ column
        dbs = await self.con.fetchall('SELECT sys::Database')
        self.assertTrue(len(dbs))

    async def test_edgeql_introspection_meta_02(self):
        await self.assert_query_result(
            r'''SELECT count(schema::Module) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 'std' IN schema::Module.name;''',
            [True],
        )

    @test.xfail('''
        There should be many more than 10 Objects in the DB due to
        all the schema objects from standard library.
    ''')
    async def test_edgeql_introspection_meta_03(self):
        await self.assert_query_result(
            r'''
                SELECT count(Object) > 10;
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_04(self):
        await self.assert_query_result(
            r'''SELECT count(schema::Type) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::ScalarType) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::ObjectType) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::ScalarType) < count(schema::Type);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::ObjectType) < count(schema::Type);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (schema::ScalarType IS schema::Type);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (schema::ObjectType IS schema::Type);''',
            [True],
        )

    async def test_edgeql_introspection_meta_05(self):
        await self.assert_query_result(
            r'''SELECT count(schema::PseudoType) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::PseudoType) < count(schema::Type);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (schema::PseudoType IS schema::Type);''',
            [True],
        )

    @test.xfail('''
        CollectionType queries cause the following error:
        relation "edgedbss.6b1e0cfa-1511-11e9-8f2e-b5ee4369b429" does not exist
    ''')
    async def test_edgeql_introspection_meta_06(self):
        await self.assert_query_result(
            r'''SELECT count(schema::CollectionType) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::Array) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::Tuple) > 0;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::CollectionType) < count(schema::Type);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::Array) <
                    count(schema::CollectionType);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT count(schema::Tuple) <
                    count(schema::CollectionType);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (schema::CollectionType IS schema::Type);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (schema::Array IS schema::CollectionType);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (schema::Tuple IS schema::CollectionType);''',
            [True],
        )

    async def test_edgeql_introspection_meta_07(self):
        await self.assert_query_result(
            r'''
                SELECT count(schema::Operator) > 0;
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_08(self):
        await self.assert_query_result(
            r'''
                SELECT count(schema::Cast) > 0;
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_09(self):
        await self.assert_query_result(
            r'''
                SELECT count(schema::Constraint) > 0;
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_10(self):
        await self.assert_query_result(
            r'''
                SELECT count(schema::Function) > 0;
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_11(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT DISTINCT (
                    SELECT Type IS Array
                    FILTER Type.name = 'array'
                );
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_12(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT DISTINCT (
                    SELECT Type IS Tuple
                    FILTER Type.name = 'tuple'
                );
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_13(self):
        # make sure that ALL schema Objects are std::Objects
        res = await self.con.fetchone(r"""
            SELECT count(schema::Object);
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Object IS std::Object;
            """,
            [True] * res
        )

    async def test_edgeql_introspection_meta_14(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT InheritingObject {
                    name
                }
                FILTER
                    re_test(r'^test::\w+$', InheritingObject.name)
                    AND InheritingObject.name NOT LIKE '%:Virtual_%'
                    AND InheritingObject.is_abstract
                ORDER BY InheritingObject.name;
            """,
            [
                {'name': 'test::Dictionary'},
                {'name': 'test::Named'},
                {'name': 'test::Owned'},
                {'name': 'test::Text'},
                {'name': 'test::my_one_of'},
            ]
        )

    async def test_edgeql_introspection_meta_15(self):
        res = await self.con.fetchall(r'''
            WITH MODULE schema
            SELECT `Type`;
        ''')
        # just test that there's a non-empty return set for this query
        self.assertTrue(res)

    async def test_edgeql_introspection_meta_16(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType[IS Object] IS ScalarType LIMIT 1;
            ''',
            [True],
        )

    async def test_edgeql_introspection_meta_17(self):
        result = await self.con.fetchall('''
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
        self.assertEqual(result[0].links[0].name, '__type__')
        self.assertIsNotNone(result[0].links[0].target.id)
        self.assertIsNotNone(result[0].properties[0].target.id)

    async def test_edgeql_introspection_meta_18(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT DISTINCT (`Function` IS VolatilitySubject);
            ''',
            [True]
        )

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT DISTINCT (Cast IS VolatilitySubject);
            ''',
            [True]
        )

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT DISTINCT (Operator IS VolatilitySubject);
            ''',
            [True]
        )

    async def test_edgeql_introspection_meta_default_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    is_abstract
                }
                FILTER .name IN {'test::Comment', 'test::Text'}
                ORDER BY .name;
            ''',
            [
                {'name': 'test::Comment', 'is_abstract': False},
                {'name': 'test::Text', 'is_abstract': True},
            ],
        )

    async def test_edgeql_introspection_meta_default_02(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    is_abstract
                }
                FILTER .name IN {'schema::Pointer', 'schema::Link'}
                ORDER BY .name;
            ''',
            [
                {'name': 'schema::Link', 'is_abstract': False},
                {'name': 'schema::Pointer', 'is_abstract': True},
            ],
        )

    async def test_edgeql_introspection_meta_default_03(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    links: {
                        name,
                        required,
                    } ORDER BY .name
                }
                FILTER .name = 'test::Comment';
            ''',
            [
                {
                    'name': 'test::Comment',
                    'links': [
                        {'name': '__type__', 'required': False},
                        {'name': 'issue', 'required': True},
                        {'name': 'owner', 'required': True},
                        {'name': 'parent', 'required': False},
                    ]
                }
            ],
        )

    async def test_edgeql_introspection_meta_default_04(self):
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    links: {
                        name,
                        required,
                    } ORDER BY .name
                }
                FILTER .name IN {'schema::CallableObject', 'schema::Parameter'}
                ORDER BY .name;
            ''',
            [
                {
                    'name': 'schema::CallableObject',
                    'links': [
                        {'name': '__type__', 'required': False},
                        {'name': 'annotations', 'required': False},
                        {'name': 'params', 'required': False},
                        {'name': 'return_type', 'required': False},
                    ]
                },
                {
                    'name': 'schema::Parameter',
                    'links': [
                        {'name': '__type__', 'required': False},
                        {'name': 'type', 'required': True},
                    ]
                }
            ],
        )

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

        await self.assert_query_result(
            r"""
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
            """,
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
        )

        await self.con.execute(r"""
            DELETE test::Priority;
            DELETE test::Status;
            DELETE test::User;
        """)

    async def test_edgeql_introspection_database_01(self):
        res = await self.con.fetchone(r"""
            WITH MODULE sys
            SELECT count(Database.name);
        """)

        self.assertGreater(res, 0)

    async def test_edgeql_introspection_describe_01(self):
        # Test that things like "\1" are serialized correctly
        # by the DESCRIBE command as they would in a raw string.
        async with self.con.transaction():
            await self.con.execute(r'''
                CREATE FUNCTION bad() -> str
                    USING ( SELECT r'\1' );
            ''')

            desc = await self.con.fetchone('''
                DESCRIBE OBJECT bad AS TEXT
            ''')

        self.assertEqual(
            desc,
            r"function default::bad() ->  std::str "
            r"using ( SELECT r'\1' );"
        )

    async def test_edgeql_introspection_describe_02(self):
        # Test that things like "\1" are serialized correctly
        # by the DESCRIBE command as they would in a raw string.

        output = await self.con.fetchone('''
            DESCRIBE OBJECT test::User AS TEXT VERBOSE
        ''')

        expected = textwrap.dedent('''\
            type test::User extending test::Dictionary {
                index on (__subject__.name);
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                multi link todo -> test::Issue {
                    single property rank -> std::int64 {
                        default := 42;
                    };
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property name -> std::str {
                    constraint std::exclusive;
                };
            };''')

        self.assertEqual(
            output,
            expected,
            f'output:\n\n{output}\n\nIS NOT EQUAL TO EXPECTED:\n\n{expected}'
        )
