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


import os

import edgedb

from edb.testbase import http as tb


class TestGraphQLSchema(tb.GraphQLTestCase):
    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'graphql_schema.esdl')

    SCHEMA_OTHER = os.path.join(os.path.dirname(__file__), 'schemas',
                                'graphql_schema_other.esdl')

    # GraphQL queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    def test_graphql_schema_base_01(self):
        self.assert_graphql_query_result(r"""
            query {
                __schema {
                    directives {
                        name
                        description
                        locations
                        args {
                            name
                            defaultValue
                            description
                            type {
                                kind
                                name
                                ofType {
                                    kind
                                    name
                                }
                            }
                        }
                    }
                }
            }
        """, {
            '__schema': {
                "directives": [
                    {
                        "name": "deprecated",
                        "description":
                            "Marks an element of a GraphQL schema as "
                            "no longer supported.",
                        "locations": {
                            "FIELD_DEFINITION",
                            "ARGUMENT_DEFINITION",
                            "INPUT_FIELD_DEFINITION",
                            "ENUM_VALUE",
                        },
                        "args": [
                            {
                                "name": "reason",
                                "defaultValue": '"No longer supported"',
                                "description":

                                    "Explains why this element was "
                                    "deprecated, usually also including "
                                    "a suggestion for how to access "
                                    "supported similar data. Formatted using "
                                    "the Markdown syntax, as specified by "
                                    "[CommonMark](https://commonmark.org/).",

                                "type": {
                                    "kind": "SCALAR",
                                    "name": "String",
                                    "ofType": None
                                }
                            }
                        ]
                    },
                    {
                        "name": "include",
                        "description":
                            "Directs the executor to include this "
                            "field or fragment only when the `if` "
                            "argument is true.",
                        "locations": {
                            "FIELD",
                            "FRAGMENT_SPREAD",
                            "INLINE_FRAGMENT"
                        },
                        "args": [
                            {
                                "name": "if",
                                "defaultValue": None,
                                "description": "Included when true.",
                                "type": {
                                    "kind": "NON_NULL",
                                    "name": None,
                                    "ofType": {
                                        "kind": "SCALAR",
                                        "name": "Boolean"
                                    }
                                }
                            }
                        ]
                    },
                    {
                        "name": "skip",
                        "description":
                            "Directs the executor to skip this field "
                            "or fragment when the `if` argument is "
                            "true.",
                        "locations": [
                            "FIELD",
                            "FRAGMENT_SPREAD",
                            "INLINE_FRAGMENT"
                        ],
                        "args": [
                            {
                                "name": "if",
                                "defaultValue": None,
                                "description": "Skipped when true.",
                                "type": {
                                    "kind": "NON_NULL",
                                    "name": None,
                                    "ofType": {
                                        "kind": "SCALAR",
                                        "name": "Boolean"
                                    }
                                }
                            }
                        ]
                    },
                    {
                        "name": 'specifiedBy',
                        "description":
                            "Exposes a URL that specifies the behaviour of "
                            "this scalar.",
                        "locations": ['SCALAR'],
                        "args": [
                            {
                                "name": 'url',
                                "type": {
                                    "kind": 'NON_NULL',
                                    "name": None,
                                    "ofType": {
                                        "kind": 'SCALAR',
                                        "name": 'String'
                                    },
                                },
                                "description":
                                    "The URL that specifies the behaviour of "
                                    "this scalar.",
                                "defaultValue": None
                            }
                        ],
                    },
                ]
            }
        }, sort={
            'directives': lambda x: x['name'],
        })

    def test_graphql_schema_base_02(self):
        self.assert_graphql_query_result(r"""
            query {
                __schema {
                    mutationType {
                        name
                    }
                }
            }
        """, {
            '__schema': {
                'mutationType': {'name': 'Mutation'},
            }
        })

    def test_graphql_schema_base_03(self):
        self.assert_graphql_query_result(r"""
            query {
                __schema {
                    queryType {
                        kind
                        name
                        description
                        interfaces {
                            name
                        }
                        possibleTypes {
                            name
                        }
                        enumValues {
                            name
                        }
                        inputFields {
                            name
                        }
                        ofType {
                            name
                        }
                    }
                }
            }
        """, {
            '__schema': {
                'queryType': {
                    'kind': 'OBJECT',
                    'name': 'Query',
                    'description': None,
                    'interfaces': [],
                    'possibleTypes': None,
                    'inputFields': None,
                    'ofType': None,
                }
            }
        })

    def test_graphql_schema_base_04(self):
        self.assert_graphql_query_result(r"""
            query {
                __schema {
                    __typename
                }
                __schema {
                    __typename
                }
            }
        """, {
            '__schema': {
                '__typename': '__Schema',
            },
        })

    def test_graphql_schema_base_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Unknown argument 'name' on field 'Query\.__schema'",
                _line=3, _col=30):
            self.graphql_query(r"""
                query {
                    __schema(name: "foo") {
                        __typename
                    }
                }
            """)

    def test_graphql_schema_base_06(self):
        result = self.graphql_query(r"""
            query {
                __schema {
                    types {
                        kind
                        name
                    }
                }
            }
        """)

        types = [(t['kind'], t['name']) for t in result['__schema']['types']]

        items = [
            ('INPUT_OBJECT', 'other__FilterFoo'),
            ('INPUT_OBJECT', 'other__OrderFoo'),
            ('INTERFACE', 'other__Foo'),
            ('OBJECT', 'other__Foo_Type'),
            ('SCALAR', 'ID'),
            ('ENUM', 'directionEnum'),
            ('OBJECT', '__Schema'),
        ]

        for item in items:
            self.assertIn(item, types)

    def test_graphql_schema_base_07(self):
        result = self.graphql_query(r"""
            query {
                Foo : __schema {
                    types {
                        kind
                        name
                    }
                }
            }
        """)

        types = [(t['kind'], t['name']) for t in result['Foo']['types']]

        items = [
            ('INPUT_OBJECT', 'other__FilterFoo'),
            ('INPUT_OBJECT', 'other__OrderFoo'),
            ('INTERFACE', 'other__Foo'),
            ('OBJECT', 'other__Foo_Type'),
            ('SCALAR', 'ID'),
            ('ENUM', 'directionEnum'),
            ('OBJECT', '__Schema'),
        ]

        for item in items:
            self.assertIn(item, types)

    def test_graphql_schema_type_01(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "User") {
                    __typename
                    name
                    kind
                }
            }
        """, {
            "__type": {
                "kind": "INTERFACE",
                "name": "User",
                "__typename": "__Type"
            }
        })

    def test_graphql_schema_type_02(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "User_Type") {
                    __typename
                    kind
                    name
                    description
                    interfaces {
                        name
                    }
                    possibleTypes {
                        name
                    }
                    enumValues {
                        name
                    }
                    inputFields {
                        name
                    }
                    ofType {
                        name
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "kind": "OBJECT",
                "name": "User_Type",
                "description": None,
                "interfaces": [
                    {"name": "BaseObject"},
                    {"name": "NamedObject"},
                    {"name": "Object"},
                    {"name": "User"},
                ],
                "possibleTypes": None,
                "enumValues": None,
                "inputFields": None,
                "ofType": None,
            }
        }, sort={
            'interfaces': lambda x: x['name'],
        })

    def test_graphql_schema_type_03(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "User") {
                    __typename
                    kind
                    name
                    description
                    interfaces {
                        name
                    }
                    possibleTypes {
                        name
                    }
                    enumValues {
                        name
                    }
                    inputFields {
                        name
                    }
                    ofType {
                        name
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "kind": "INTERFACE",
                "name": "User",
                "description": None,
                "interfaces": [],
                "possibleTypes": [
                    {"name": "Person_Type"},
                    {"name": "User_Type"}
                ],
                "enumValues": None,
                "inputFields": None,
                "ofType": None,
            }
        }, sort={
            'possibleTypes': lambda x: x['name'],
        })

    def test_graphql_schema_type_04(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "UserGroup") {
                    __typename
                    name
                    kind
                    fields {
                        __typename
                        name
                        description
                        type {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                                ofType {
                                    __typename
                                    name
                                    kind
                                    ofType {
                                        __typename
                                        name
                                        kind
                                        ofType {
                                            __typename
                                            name
                                            kind
                                            ofType {
                                                name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        isDeprecated
                        deprecationReason
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "UserGroup",
                "kind": "INTERFACE",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR",
                                "ofType": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "settings",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "Setting",
                                    "kind": "INTERFACE",
                                    "ofType": None
                                }
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    }
                ]
            }
        }, sort={
            'fields': lambda x: x['name'],
        })

    def test_graphql_schema_type_05(self):
        self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                __typename
                name
                kind
            }

            query {
                __type(name: "UserGroup_Type") {
                    ..._t
                    fields {
                        __typename
                        name
                        description
                        type {
                            ..._t
                            ofType {
                                ..._t
                                ofType {
                                    ..._t
                                    ofType {
                                        ..._t
                                        ofType {
                                            ..._t
                                            ofType {
                                                name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        isDeprecated
                        deprecationReason
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "UserGroup_Type",
                "kind": "OBJECT",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR",
                                "ofType": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "settings",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "Setting",
                                    "kind": "INTERFACE",
                                    "ofType": None
                                }
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    }
                ]
            }
        }, sort={
            'fields': lambda x: x['name'],
        })

    def test_graphql_schema_type_06(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "Profile_Type") {
                    __typename
                    name
                    kind
                    fields {
                        __typename
                        name
                        description
                        type {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                                ofType {
                                    __typename
                                    name
                                    kind
                                    ofType {
                                        __typename
                                        name
                                        kind
                                        ofType {
                                            __typename
                                            name
                                            kind
                                            ofType {
                                                name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        isDeprecated
                        deprecationReason
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "Profile_Type",
                "kind": "OBJECT",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR",
                                "ofType": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "odd",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL",
                                        "ofType": {
                                            "__typename": "__Type",
                                            "name": "Int64",
                                            "kind": "SCALAR",
                                            "ofType": None
                                        }
                                    }
                                }
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "owner_name",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "String",
                                    "kind": "SCALAR",
                                    "ofType": None
                                }
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "owner_user",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "User",
                                    "kind": "INTERFACE",
                                    "ofType": None
                                }
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "tags",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "String",
                                    "kind": "SCALAR",
                                    "ofType": None
                                }
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "value",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    }
                ]
            }
        }, sort={
            'fields': lambda x: x['name'],
        })

    def test_graphql_schema_type_07(self):
        self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                __typename
                name
                kind
            }

            fragment _f on __Type {
                fields {
                    __typename
                    name
                    description
                    type {
                        ..._t
                        ofType {
                            ..._t
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }

            fragment _T on __Type {
                        __typename
                        kind
                        name
                        description
                        ..._f
                        interfaces {
                            ..._t
                        }
                        possibleTypes {
                            name
                        }
                        enumValues {
                            name
                        }
                        inputFields {
                            name
                        }
                        ofType {
                            name
                        }
            }

            query {
                __type(name: "NamedObject") {
                    __typename
                    kind
                    name
                    description
                    ..._f
                    interfaces {
                        ..._T
                    }
                    possibleTypes {
                        ..._T
                    }
                    enumValues {
                        name
                    }
                    inputFields {
                        name
                    }
                    ofType {
                        name
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "kind": "INTERFACE",
                "name": "NamedObject",
                "description": "An object with a name",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR"
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": None,
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR"
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    }
                ],
                "interfaces": [],
                "possibleTypes": [
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "Person_Type",
                        "description": None,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "active",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Boolean",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "age",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Int64",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "groups",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "profile",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "Profile",
                                    "kind": "INTERFACE",
                                    "ofType": None
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "score",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Float",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "BaseObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Person",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "User",
                                "kind": "INTERFACE"
                            },
                        ],
                        "possibleTypes": None,
                        "enumValues": None,
                        "inputFields": None,
                        "ofType": None
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "Profile_Type",
                        "description": None,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "odd",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "owner_name",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "owner_user",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "tags",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "value",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "BaseObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Profile",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Profile_OR_Setting",
                                "kind": "INTERFACE"
                            },
                        ],
                        "possibleTypes": None,
                        "enumValues": None,
                        "inputFields": None,
                        "ofType": None
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "Setting_Type",
                        "description": None,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "value",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "BaseObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Profile_OR_Setting",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Setting",
                                "kind": "INTERFACE"
                            },
                        ],
                        "possibleTypes": None,
                        "enumValues": None,
                        "inputFields": None,
                        "ofType": None
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "UserGroup_Type",
                        "description": None,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "settings",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "BaseObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "UserGroup",
                                "kind": "INTERFACE"
                            },
                        ],
                        "possibleTypes": None,
                        "enumValues": None,
                        "inputFields": None,
                        "ofType": None
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "User_Type",
                        "description": None,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "active",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Boolean",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "age",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Int64",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "groups",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": None,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "profile",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "Profile",
                                    "kind": "INTERFACE",
                                    "ofType": None
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            },
                            {
                                "__typename": "__Field",
                                "name": "score",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": None,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Float",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": False,
                                "deprecationReason": None
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "BaseObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "User",
                                "kind": "INTERFACE"
                            },
                        ],
                        "possibleTypes": None,
                        "enumValues": None,
                        "inputFields": None,
                        "ofType": None
                    },
                ],
                "enumValues": None,
                "inputFields": None,
                "ofType": None
            }
        }, sort={
            'fields': lambda x: x['name'],
            'possibleTypes': {
                '.': lambda x: x['name'],
                'fields': lambda x: x['name'],
                'interfaces': lambda x: x['name'],
            },
        })

    def test_graphql_schema_type_08(self):
        self.assert_graphql_query_result(r"""
        query {
            __type(name: "UserGroup_Type") {
                __typename
                name
                kind
                fields {
                    __typename
                    name
                    description
                    args {
                        name
                        description
                        type {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                            }
                        }
                        defaultValue
                    }
                    type {
                        __typename
                        name
                        kind
                        fields {name}
                        ofType {
                            name
                            kind
                            fields {name}
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }
        }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "UserGroup_Type",
                "kind": "OBJECT",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": None,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "fields": None,
                            "ofType": {
                                "name": "ID",
                                "kind": "SCALAR",
                                "fields": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": None,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "fields": None,
                            "ofType": {
                                "name": "String",
                                "kind": "SCALAR",
                                "fields": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "settings",
                        "description": None,
                        "args": [
                            {
                                "name": "after",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "String",
                                    "kind": "SCALAR",
                                    "ofType": None
                                },
                                "defaultValue": None
                            },
                            {
                                "name": "before",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "String",
                                    "kind": "SCALAR",
                                    "ofType": None
                                },
                                "defaultValue": None
                            },
                            {
                                "name": "filter",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "FilterSetting",
                                    "kind": "INPUT_OBJECT",
                                    "ofType": None
                                },
                                "defaultValue": None
                            },
                            {
                                "name": "first",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "Int",
                                    "kind": "SCALAR",
                                    "ofType": None
                                },
                                "defaultValue": None
                            },
                            {
                                "name": "last",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "Int",
                                    "kind": "SCALAR",
                                    "ofType": None
                                },
                                "defaultValue": None
                            },
                            {
                                "name": "order",
                                "description": None,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "OrderSetting",
                                    "kind": "INPUT_OBJECT",
                                    "ofType": None
                                },
                                "defaultValue": None
                            },
                        ],
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "LIST",
                            "fields": None,
                            "ofType": {
                                "name": None,
                                "kind": "NON_NULL",
                                "fields": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    }
                ]
            }
        }, sort={
            'fields': {
                '.': lambda x: x['name'],
                'args': lambda x: x['name'],
            }
        })

    def test_graphql_schema_type_09(self):
        self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                name
                kind
            }

            query {
                __type(name: "FilterUser") {
                    __typename
                    ..._t
                    inputFields {
                        name
                        type {
                            ..._t
                            ofType {
                                ..._t
                                ofType {
                                    ..._t
                                    ofType {
                                        ..._t
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "FilterUser",
                "kind": "INPUT_OBJECT",
                "inputFields": [
                    {
                        "name": "active",
                        "type": {
                            "name": "FilterBoolean",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "age",
                        "type": {
                            "name": "FilterInt64",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "and",
                        "type": {
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "name": "FilterUser",
                                    "kind": "INPUT_OBJECT",
                                    "ofType": None
                                }
                            }
                        }
                    },
                    {
                        "name": "groups",
                        "type": {
                            "kind": "INPUT_OBJECT",
                            "name": "NestedFilterUserGroup",
                            "ofType": None
                        }
                    },
                    {
                        "name": "id",
                        "type": {
                            "name": "FilterID",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "name",
                        "type": {
                            "name": "FilterString",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "not",
                        "type": {
                            "name": "FilterUser",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "or",
                        "type": {
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "name": "FilterUser",
                                    "kind": "INPUT_OBJECT",
                                    "ofType": None
                                }
                            }
                        }
                    },
                    {
                        "name": "profile",
                        "type": {
                            "kind": "INPUT_OBJECT",
                            "name": "NestedFilterProfile",
                            "ofType": None
                        }
                    },
                    {
                        "name": "score",
                        "type": {
                            "name": "FilterFloat",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    }
                ]
            }
        }, sort={
            'inputFields': lambda x: x['name'],
        })

    def test_graphql_schema_type_10(self):
        self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                name
                kind
            }

            query {
                __type(name: "OrderUser") {
                    __typename
                    ..._t
                    inputFields {
                        name
                        type {
                            ..._t
                            ofType {
                                ..._t
                                ofType {
                                    ..._t
                                    ofType {
                                        ..._t
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "OrderUser",
                "kind": "INPUT_OBJECT",
                "inputFields": [
                    {
                        "name": "active",
                        "type": {
                            "name": "Ordering",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "age",
                        "type": {
                            "name": "Ordering",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "id",
                        "type": {
                            "name": "Ordering",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "name",
                        "type": {
                            "name": "Ordering",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "profile",
                        "type": {
                            "name": "OrderProfile",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "score",
                        "type": {
                            "name": "Ordering",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    }
                ]
            }
        }, sort={
            'inputFields': lambda x: x['name'],
        })

    def test_graphql_schema_type_11(self):
        self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                name
                kind
            }

            query {
                __type(name: "Ordering") {
                    __typename
                    ..._t
                    inputFields {
                        name
                        defaultValue
                        type {
                            ..._t
                            ofType {
                                ..._t
                                ofType {
                                    ..._t
                                    ofType {
                                        ..._t
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "Ordering",
                "kind": "INPUT_OBJECT",
                "inputFields": [
                    {
                        "name": "dir",
                        "defaultValue": None,
                        "type": {
                            "name": None,
                            "kind": "NON_NULL",
                            "ofType": {
                                "name": "directionEnum",
                                "kind": "ENUM",
                                "ofType": None
                            }
                        }
                    },
                    {
                        "name": "nulls",
                        "defaultValue": 'SMALLEST',
                        "type": {
                            "name": "nullsOrderingEnum",
                            "kind": "ENUM",
                            "ofType": None
                        }
                    }
                ]
            }
        }, sort={
            'inputFields': lambda x: x['name'],
        })

    def test_graphql_schema_type_12(self):
        self.assert_graphql_query_result(r"""
            query {
                directionEnum: __type(name: "directionEnum") {
                    __typename
                    name
                    kind
                    enumValues {
                        name
                    }
                }
                nullsOrderingEnum: __type(name: "nullsOrderingEnum") {
                    __typename
                    name
                    kind
                    enumValues {
                        name
                    }
                }
            }
        """, {
            'directionEnum': {
                "__typename": "__Type",
                "name": "directionEnum",
                "kind": "ENUM",
                "enumValues": [
                    {"name": "ASC"},
                    {"name": "DESC"},
                ]
            },
            'nullsOrderingEnum': {
                "__typename": "__Type",
                "name": "nullsOrderingEnum",
                "kind": "ENUM",
                "enumValues": [
                    {"name": "BIGGEST"},
                    {"name": "SMALLEST"},
                ]
            }
        }, sort={
            'enumValues': lambda x: x['name'],
        })

    def test_graphql_schema_type_13(self):
        result = self.graphql_query(r"""
            query IntrospectionQuery {
                __schema {
                  queryType { name }
                  mutationType { name }
                  subscriptionType { name }
                  types {
                    ...FullType
                  }
                  directives {
                    name
                    description
                    locations
                    args {
                      ...InputValue
                    }
                  }
                }
              }

              fragment FullType on __Type {
                kind
                name
                description
                fields(includeDeprecated: true) {
                  name
                  description
                  args {
                    ...InputValue
                  }
                  type {
                    ...TypeRef
                  }
                  isDeprecated
                  deprecationReason
                }
                inputFields {
                  ...InputValue
                }
                interfaces {
                  ...TypeRef
                }
                enumValues(includeDeprecated: true) {
                  name
                  description
                  isDeprecated
                  deprecationReason
                }
                possibleTypes {
                  ...TypeRef
                }
              }

              fragment InputValue on __InputValue {
                name
                description
                type { ...TypeRef }
                defaultValue
              }

              fragment TypeRef on __Type {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                            ofType {
                              kind
                              name
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
        """)

        types = [(t['kind'], t['name']) for t in result['__schema']['types']]

        items = [
            ('INPUT_OBJECT', 'other__FilterFoo'),
            ('INPUT_OBJECT', 'other__OrderFoo'),
            ('INTERFACE', 'other__Foo'),
            ('OBJECT', 'other__Foo_Type'),
            ('SCALAR', 'ID'),
            ('ENUM', 'directionEnum'),
            ('OBJECT', '__Schema'),
        ]

        for item in items:
            self.assertIn(item, types)

    def test_graphql_schema_type_14(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name:"other__ColorEnum") {
                    __typename
                    name
                    kind
                    description
                    enumValues {
                        name
                    }
                }
            }
        """, {

            "__type": {
                "kind": "ENUM",
                "name": "other__ColorEnum",
                "__typename": "__Type",
                "description": "RGB color enum",
                "enumValues": [
                    {
                        "name": "RED"
                    },
                    {
                        "name": "GREEN"
                    },
                    {
                        "name": "BLUE"
                    },
                ]
            }
        })

    def test_graphql_schema_type_15(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "NamedObject") {
                    __typename
                    name
                    kind
                    description
                }
            }
        """, {
            "__type": {
                "kind": "INTERFACE",
                "name": "NamedObject",
                "__typename": "__Type",
                "description": 'An object with a name',
            }
        })

    def test_graphql_schema_type_16(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "other__Foo") {
                    __typename
                    name
                    kind
                    description
                }
            }
        """, {
            "__type": {
                "kind": "INTERFACE",
                "name": "other__Foo",
                "__typename": "__Type",
                "description": 'Test type "Foo"',
            }
        })

    def test_graphql_schema_type_17(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "other__Foo_Type") {
                    __typename
                    name
                    kind
                    description
                }
            }
        """, {
            "__type": {
                "kind": "OBJECT",
                "name": "other__Foo_Type",
                "__typename": "__Type",
                "description": 'Test type "Foo"',
            }
        })

    def test_graphql_schema_type_18(self):
        self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                __typename
                name
                kind
            }

            fragment _f on __Type {
                fields {
                    __typename
                    name
                    description
                    type {
                        ..._t
                        ofType {
                            ..._t
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }

            fragment _T on __Type {
                        __typename
                        kind
                        name
                        description
                        ..._f
                        interfaces {
                            ..._t
                        }
                        possibleTypes {
                            name
                        }
                        enumValues {
                            name
                        }
                        inputFields {
                            name
                        }
                        ofType {
                            name
                        }
            }

            query {
                __type(name: "SettingAliasAugmented") {
                    __typename
                    kind
                    name
                    description
                    ..._f
                    interfaces {
                        ..._T
                    }
                    possibleTypes {
                        ..._T
                    }
                    enumValues {
                        name
                    }
                    inputFields {
                        name
                    }
                    ofType {
                        name
                    }
                }
            }
        """, {
            "__type": {
                "kind": "OBJECT",
                "name": "SettingAliasAugmented",
                "fields": [
                    {
                        "name": "id",
                        "type": {
                            "kind": "NON_NULL",
                            "name": None,
                            "ofType": {
                                "kind": "SCALAR",
                                "name": "ID",
                                "__typename": "__Type"
                            },
                            "__typename": "__Type"
                        },
                        "__typename": "__Field",
                        "description": None,
                        "isDeprecated": False,
                        "deprecationReason": None,
                    },
                    {
                        "name": "name",
                        "type": {
                            "kind": "NON_NULL",
                            "name": None,
                            "ofType": {
                                "kind": "SCALAR",
                                "name": "String",
                                "__typename": "__Type"
                            },
                            "__typename": "__Type"
                        },
                        "__typename": "__Field",
                        "description": None,
                        "isDeprecated": False,
                        "deprecationReason": None,
                    },
                    {
                        "name": "of_group",
                        "type": {
                            "kind": "OBJECT",
                            "name": "_edb__SettingAliasAugmented__of_group",
                            "ofType": None,
                            "__typename": "__Type"
                        },
                        "__typename": "__Field",
                        "description": None,
                        "isDeprecated": False,
                        "deprecationReason": None,
                    },
                    {
                        "name": "value",
                        "type": {
                            "kind": "NON_NULL",
                            "name": None,
                            "ofType": {
                                "kind": "SCALAR",
                                "name": "String",
                                "__typename": "__Type"
                            },
                            "__typename": "__Type"
                        },
                        "__typename": "__Field",
                        "description": None,
                        "isDeprecated": False,
                        "deprecationReason": None,
                    },
                ],
                "ofType": None,
                "__typename": "__Type",
                "enumValues": None,
                "interfaces": [],
                "description": None,
                "inputFields": None,
                "possibleTypes": None,
            }
        })

    def test_graphql_schema_type_19(self):
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "String") {
                    __typename
                    name
                    kind
                    specifiedByUrl
                }
            }
        """, {
            "__type": {
                "kind": "SCALAR",
                "name": "String",
                "__typename": "__Type",
                "specifiedByUrl": None,
            }
        })

    def test_graphql_schema_type_20(self):
        # make sure the union type got reflected
        self.assert_graphql_query_result(r"""
        query {
            __type(name: "Profile_OR_Setting") {
                __typename
                name
                kind
                fields {
                    __typename
                    name
                    description
                    args {
                        name
                        description
                        type {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                            }
                        }
                        defaultValue
                    }
                    type {
                        __typename
                        name
                        kind
                        fields {name}
                        ofType {
                            name
                            kind
                            fields {name}
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
                possibleTypes {
                    name
                }
            }
        }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "Profile_OR_Setting",
                "kind": "INTERFACE",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": None,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "fields": None,
                            "ofType": {
                                "name": "ID",
                                "kind": "SCALAR",
                                "fields": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": None,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "fields": None,
                            "ofType": {
                                "name": "String",
                                "kind": "SCALAR",
                                "fields": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "value",
                        "description": None,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "fields": None,
                            "ofType": {
                                "name": "String",
                                "kind": "SCALAR",
                                "fields": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                ],
                "possibleTypes": [
                    {"name": "Profile_Type"},
                    {"name": "Setting_Type"},
                ],
            }
        }, sort={
            'fields': {
                '.': lambda x: x['name'],
                'args': lambda x: x['name'],
            }
        })

    def test_graphql_schema_type_21(self):
        # make sure that the union type supports filtering
        self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                name
                kind
            }

            query {
                __type(name: "FilterProfile_OR_Setting") {
                    __typename
                    ..._t
                    inputFields {
                        name
                        type {
                            ..._t
                            ofType {
                                ..._t
                                ofType {
                                    ..._t
                                    ofType {
                                        ..._t
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """, {
            "__type": {
                "__typename": "__Type",
                "name": "FilterProfile_OR_Setting",
                "kind": "INPUT_OBJECT",
                "inputFields": [
                    {
                        "name": "and",
                        "type": {
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "name": "FilterProfile_OR_Setting",
                                    "kind": "INPUT_OBJECT",
                                    "ofType": None
                                }
                            }
                        }
                    },
                    {
                        "name": "id",
                        "type": {
                            "name": "FilterID",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "name",
                        "type": {
                            "name": "FilterString",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "not",
                        "type": {
                            "name": "FilterProfile_OR_Setting",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                    {
                        "name": "or",
                        "type": {
                            "name": None,
                            "kind": "LIST",
                            "ofType": {
                                "name": None,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "name": "FilterProfile_OR_Setting",
                                    "kind": "INPUT_OBJECT",
                                    "ofType": None
                                }
                            }
                        }
                    },
                    {
                        "name": "value",
                        "type": {
                            "name": "FilterString",
                            "kind": "INPUT_OBJECT",
                            "ofType": None
                        }
                    },
                ]
            }
        }, sort={
            'inputFields': lambda x: x['name'],
        })

    def test_graphql_schema_type_22(self):
        # make sure the union type is used as link target
        self.assert_graphql_query_result(r"""
        query {
            __type(name: "Combo") {
                __typename
                name
                kind
                fields {
                    __typename
                    name
                    description
                    args {
                        name
                        description
                        type {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                            }
                        }
                        defaultValue
                    }
                    type {
                        __typename
                        name
                        kind
                        fields {name}
                        ofType {
                            name
                            kind
                            fields {name}
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }
        }
        """, {
            "__type": {

                "__typename": "__Type",
                "kind": "INTERFACE",
                "name": "Combo",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "data",
                        "description": None,
                        "args": [
                            {
                                "name": "after",
                                "type": {
                                    "kind": "SCALAR",
                                    "name": "String",
                                    "ofType": None,
                                    "__typename": "__Type"
                                },
                                "description": None,
                                "defaultValue": None
                            },
                            {
                                "name": "before",
                                "type": {
                                    "kind": "SCALAR",
                                    "name": "String",
                                    "ofType": None,
                                    "__typename": "__Type"
                                },
                                "description": None,
                                "defaultValue": None
                            },
                            {
                                "name": "filter",
                                "type": {
                                    "kind": "INPUT_OBJECT",
                                    "name": "FilterProfile_OR_Setting",
                                    "ofType": None,
                                    "__typename": "__Type"
                                },
                                "description": None,
                                "defaultValue": None
                            },
                            {
                                "name": "first",
                                "type": {
                                    "kind": "SCALAR",
                                    "name": "Int",
                                    "ofType": None,
                                    "__typename": "__Type"
                                },
                                "description": None,
                                "defaultValue": None
                            },
                            {
                                "name": "last",
                                "type": {
                                    "kind": "SCALAR",
                                    "name": "Int",
                                    "ofType": None,
                                    "__typename": "__Type"
                                },
                                "description": None,
                                "defaultValue": None
                            },
                            {
                                "name": "order",
                                "type": {
                                    "kind": "INPUT_OBJECT",
                                    "name": "OrderProfile_OR_Setting",
                                    "ofType": None,
                                    "__typename": "__Type"
                                },
                                "description": None,
                                "defaultValue": None
                            },
                        ],
                        "type": {
                            "kind": "INTERFACE",
                            "name": "Profile_OR_Setting",
                            "fields": [
                                {
                                    "name": "id"
                                },
                                {
                                    "name": "name"
                                },
                                {
                                    "name": "value"
                                }
                            ],
                            "ofType": None,
                            "__typename": "__Type"
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": None,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": None,
                            "kind": "NON_NULL",
                            "fields": None,
                            "ofType": {
                                "name": "ID",
                                "kind": "SCALAR",
                                "fields": None
                            }
                        },
                        "isDeprecated": False,
                        "deprecationReason": None
                    },
                ],

            }
        }, sort={
            'fields': {
                '.': lambda x: x['name'],
                'args': lambda x: x['name'],
            }
        })

    def test_graphql_schema_reflection_01(self):
        # Make sure that FreeObject is not reflected.
        result = self.graphql_query(r"""
            query {
                __type(name: "FeeObject") {
                    name
                }
                __schema {
                    queryType {
                        fields {
                            name
                        }
                    }
                    mutationType {
                        fields {
                            name
                        }
                    }
                }
            }
        """)

        self.assertIsNone(result['__type'])
        self.assertNotIn(
            'FreeObject',
            [t['name'] for t in result['__schema']['queryType']['fields']]
        )
        self.assertNotIn(
            'delete_FreeObject',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertNotIn(
            'update_FreeObject',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertNotIn(
            'insert_FreeObject',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )

    def test_graphql_schema_reflection_02(self):
        # Make sure that "id", as well as computed "owner_user" and
        # "owner_name" are not reflected into insert or update
        result = self.graphql_query(r"""
            query {
                in: __type(name: "InsertUser") {
                    inputFields {
                        name
                    }
                }
                up: __type(name: "UpdateUser") {
                    inputFields {
                        name
                    }
                }
            }
        """)

        for bad in ['id', 'owner_user', 'owner_name']:
            self.assertNotIn(
                bad,
                [t['name'] for t in result['in']['inputFields']]
            )
            self.assertNotIn(
                bad,
                [t['name'] for t in result['up']['inputFields']]
            )

    def test_graphql_schema_reflection_03(self):
        # Make sure that union type `Profile | Setting` is not reflected at
        # the root of Query or Mutation.
        result = self.graphql_query(r"""
            query {
                __schema {
                    queryType {
                        fields {
                            name
                        }
                    }
                    mutationType {
                        fields {
                            name
                        }
                    }
                }
            }
        """)

        self.assertNotIn(
            'Profile_OR_Setting',
            [t['name'] for t in result['__schema']['queryType']['fields']]
        )
        self.assertNotIn(
            'delete_Profile_OR_Setting',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertNotIn(
            'update_Profile_OR_Setting',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertNotIn(
            'insert_Profile_OR_Setting',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )

    def test_graphql_schema_reflection_04(self):
        # Make sure that `Fixed` and `NotEditable` types are reflected, but
        # don't have an "update".
        result = self.graphql_query(r"""
            query {
                __schema {
                    queryType {
                        fields {
                            name
                        }
                    }
                    mutationType {
                        fields {
                            name
                        }
                    }
                }
            }
        """)

        self.assertIn(
            'Fixed',
            [t['name'] for t in result['__schema']['queryType']['fields']]
        )
        self.assertIn(
            'insert_Fixed',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertNotIn(
            'update_Fixed',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertIn(
            'delete_Fixed',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )

        self.assertIn(
            'NotEditable',
            [t['name'] for t in result['__schema']['queryType']['fields']]
        )
        self.assertIn(
            'insert_NotEditable',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertNotIn(
            'update_NotEditable',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )
        self.assertIn(
            'delete_NotEditable',
            [t['name'] for t in result['__schema']['mutationType']['fields']]
        )

    def test_graphql_schema_reflection_05(self):
        # `Fixed` is not supposed to have either "input" or "update" types.
        self.assert_graphql_query_result(r"""
            query {
                in: __type(name: "InsertFixed") {
                    inputFields {
                        name
                    }
                }
                up: __type(name: "UpdateFixed") {
                    inputFields {
                        name
                    }
                }
            }
        """, {
            'in': None,
            'up': None,
        })

        # `NotEditable` is only supposed to have "input" type.
        self.assert_graphql_query_result(r"""
            query {
                in: __type(name: "InsertNotEditable") {
                    inputFields {
                        name
                    }
                }
                up: __type(name: "UpdateNotEditable") {
                    inputFields {
                        name
                    }
                }
            }
        """, {
            'in': {
                'inputFields': [{'name': 'once'}],
            },
            'up': None,
        })
