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
                User {
                    name
                    age
                    groups {
                        id
                        name
                    }
                }
            }
        """, graphql=True)

        result[0][0]['User'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'Alice',
                'age': 27,
                'groups': None
            }, {
                'name': 'Bob',
                'age': 21,
                'groups': None
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
                User(name: "John") {
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
                User(id: "{alice['id']}") {{
                    id
                    name
                    age
                }}
            }}
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [alice]
        }]])

    async def test_graphql_functional_fragment_02(self):
        result = await self.con.execute(r"""
            fragment userFrag on User {
                age
                score
            }

            query {
                NamedObject(name: "Alice") {
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
                '__typename': 'User',
                'groups': None
            }, {
                'name': 'Bob',
                '__typename': 'Person',
                'groups': None
            }, {
                'name': 'Jane',
                '__typename': 'User',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                    '__typename': 'UserGroup',
                }]
            }, {
                'name': 'John',
                '__typename': 'User',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                    '__typename': 'UserGroup',
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
