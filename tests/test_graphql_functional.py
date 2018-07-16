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


import os
import uuid

from edb.server import _testbase as tb


class TestGraphQLFunctional(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'graphql.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'graphql_setup.eql')

    async def test_graphql_functional_query_01(self):
        result = await self.con.execute(r"""
            query {
                Setting {
                    name
                    value
                }
            }
        """, graphql=True)

        result[0][0]['Setting'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'Setting': [{
                'name': 'perks',
                'value': 'full',
            }, {
                'name': 'template',
                'value': 'blue',
            }],
        }]])

    async def test_graphql_functional_query_02(self):
        result = await self.con.execute(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    name
                    age
                    groups {
                        id
                        name
                    }
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'Alice',
                'age': 27,
                'groups': []
            }, {
                'name': 'Bob',
                'age': 21,
                'groups': []
            }, {
                'name': 'Jane',
                'age': 25,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                }]
            }, {
                'name': 'John',
                'age': 25,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                }]
            }],
        }]])

    async def test_graphql_functional_query_03(self):
        result = await self.con.execute(r"""
            query {
                User(filter: {name: {eq: "John"}}) {
                    name
                    age
                    groups {
                        id
                        name
                    }
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'John',
                'age': 25,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                }]
            }],
        }]])

    async def test_graphql_functional_arguments_01(self):
        result = await self.con.execute(r"""
            query {
                User {
                    id
                    name
                    age
                }
            }
        """, graphql=True)

        alice = [res for res in result[0][0]['User']
                 if res['name'] == 'Alice'][0]

        result = await self.con.execute(f"""
            query {{
                User(filter: {{id: {{eq: "{alice['id']}"}}}}) {{
                    id
                    name
                    age
                }}
            }}
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [alice]
        }]])

    async def test_graphql_functional_arguments_02(self):
        result = await self.con.execute(r"""
            query {
                User(filter: {
                    name: {eq: "Bob"},
                    active: {eq: true},
                    age: {eq: 21}
                }) {
                    name
                    age
                    groups {
                        id
                        name
                    }
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'Bob',
                'age': 21,
                'groups': [],
            }],
        }]])

    async def test_graphql_functional_arguments_03(self):
        result = await self.con.execute(r"""
            query {
                User(filter: {
                    and: [{name: {eq: "Bob"}}, {active: {eq: true}}],
                    age: {eq: 21}
                }) {
                    name
                    score
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'Bob',
                'score': 4.2,
            }],
        }]])

    async def test_graphql_functional_arguments_04(self):
        result = await self.con.execute(r"""
            query {
                User(filter: {
                    not: {name: {eq: "Bob"}},
                    age: {eq: 21}
                }) {
                    name
                    score
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [],
        }]])

    async def test_graphql_functional_arguments_05(self):
        result = await self.con.execute(r"""
            query {
                User(
                    filter: {
                        or: [
                            {not: {name: {eq: "Bob"}}},
                            {age: {eq: 20}}
                        ]
                    },
                    order: {name: {dir: ASC}}
                ) {
                    name
                    score
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [
                {'name': 'Alice', 'score': 5},
                {'name': 'Jane', 'score': 1.23},
                {'name': 'John', 'score': 3.14},
            ],
        }]])

    async def test_graphql_functional_arguments_06(self):
        result = await self.con.execute(r"""
            query {
                User(
                    filter: {
                        or: [
                            {name: {neq: "Bob"}},
                            {age: {eq: 20}}
                        ]
                    },
                    order: {name: {dir: ASC}}
                ) {
                    name
                    score
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [
                {'name': 'Alice', 'score': 5},
                {'name': 'Jane', 'score': 1.23},
                {'name': 'John', 'score': 3.14},
            ],
        }]])

    async def test_graphql_functional_arguments_07(self):
        result = await self.con.execute(r"""
            query {
                User(filter: {
                    name: {ilike: "%o%"},
                    age: {gt: 22}
                }) {
                    name
                    age
                }
            }
        """, graphql=True)

        result[0][0]['User'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'User': [
                {'name': 'John', 'age': 25},
            ],
        }]])

    async def test_graphql_functional_arguments_08(self):
        result = await self.con.execute(r"""
            query {
                User(filter: {
                    name: {like: "J%"},
                    score: {
                        gte: 3
                        lt: 4.5
                    }
                }) {
                    name
                    score
                }
            }
        """, graphql=True)

        result[0][0]['User'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'User': [
                {'name': 'John', 'score': 3.14},
            ],
        }]])

    async def test_graphql_functional_arguments_09(self):
        result = await self.con.execute(r"""
            query {
                User(filter: {
                    name: {ilike: "%e"},
                    age: {lte: 25}
                }) {
                    name
                    age
                }
            }
        """, graphql=True)

        result[0][0]['User'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'User': [
                {'name': 'Jane', 'age': 25},
            ],
        }]])

    async def test_graphql_functional_arguments_10(self):
        result = await self.con.execute(r"""
            query {
                User(
                    order: {
                        age: {dir: DESC}
                        name: {dir: ASC}
                    }
                ) {
                    name
                    age
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [
                {'age': 27, 'name': 'Alice'},
                {'age': 25, 'name': 'Jane'},
                {'age': 25, 'name': 'John'},
                {'age': 21, 'name': 'Bob'},
            ],
        }]])

    async def test_graphql_functional_arguments_11(self):
        result = await self.con.execute(r"""
            query {
                User(
                    order: {
                        name: {dir: ASC}
                        age: {dir: DESC}
                    }
                ) {
                    name
                    age
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [
                {'age': 27, 'name': 'Alice'},
                {'age': 21, 'name': 'Bob'},
                {'age': 25, 'name': 'Jane'},
                {'age': 25, 'name': 'John'},
            ],
        }]])

    async def test_graphql_functional_arguments_12(self):
        result = await self.con.execute(r"""
            query {
                Foo(
                    order: {
                        select: {dir: ASC, nulls: BIGGEST}
                    }
                ) {
                    after
                    select
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'Foo': [
                {'after': None, 'select': 'a'},
                {'after': 'w', 'select': 'b'},
                {'after': 'q', 'select': None},
            ],
        }]])

    async def test_graphql_functional_arguments_13(self):
        result = await self.con.execute(r"""
            query {
                Foo(
                    order: {
                        select: {dir: DESC, nulls: SMALLEST}
                    }
                ) {
                    after
                    select
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'Foo': [
                {'after': 'w', 'select': 'b'},
                {'after': None, 'select': 'a'},
                {'after': 'q', 'select': None},
            ],
        }]])

    async def test_graphql_functional_fragment_02(self):
        result = await self.con.execute(r"""
            fragment userFrag on User {
                age
                score
            }

            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    name
                    ... userFrag
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'NamedObject': [{
                'name': 'Alice',
                'age': 27,
                'score': 5,
            }],
        }]])

    async def test_graphql_functional_typename_01(self):
        result = await self.con.execute(r"""
            query {
                User {
                    name
                    __typename
                    groups {
                        id
                        name
                        __typename
                    }
                }
            }
        """, graphql=True)

        result[0][0]['User'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'Alice',
                '__typename': 'UserType',
                'groups': []
            }, {
                'name': 'Bob',
                '__typename': 'PersonType',
                'groups': []
            }, {
                'name': 'Jane',
                '__typename': 'UserType',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                    '__typename': 'UserGroupType',
                }]
            }, {
                'name': 'John',
                '__typename': 'UserType',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                    '__typename': 'UserGroupType',
                }]
            }],
        }]])

    async def test_graphql_functional_typename_02(self):
        result = await self.con.execute(r"""
            query {
                __typename
                __schema {
                    __typename
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            '__typename': 'Query',
            '__schema': {
                '__typename': '__Schema',
            },
        }]])

    async def test_graphql_functional_schema_01(self):
        result = await self.con.execute(r"""
            query {
                __schema {
                    directives {
                        name
                        description
                        locations
                        args {
                            name
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
        """, graphql=True)

        result[0][0]['__schema']['directives'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            '__schema': {
                "directives": [
                    {
                        "name": "deprecated",
                        "description":
                            "Marks an element of a GraphQL schema as "
                            "no longer supported.",
                        "locations": [
                            "FIELD_DEFINITION",
                            "ENUM_VALUE"
                        ],
                        "args": [
                            {
                                "name": "reason",
                                "description":

                                    "Explains why this element was "
                                    "deprecated, usually also including "
                                    "a suggestion for how toaccess "
                                    "supported similar data. Formatted "
                                    "in [Markdown](https://daringfireba"
                                    "ll.net/projects/markdown/).",

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
                        "locations": [
                            "FIELD",
                            "FRAGMENT_SPREAD",
                            "INLINE_FRAGMENT"
                        ],
                        "args": [
                            {
                                "name": "if",
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
                ]
            }
        }]])

    async def test_graphql_functional_schema_02(self):
        result = await self.con.execute(r"""
            query {
                __schema {
                    mutationType {
                        name
                    }
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            '__schema': {
                'mutationType': None
            }
        }]])

    async def test_graphql_functional_schema_03(self):
        result = await self.con.execute(r"""
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
        """, graphql=True)

        self.assert_data_shape(result, [[{
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
        }]])
