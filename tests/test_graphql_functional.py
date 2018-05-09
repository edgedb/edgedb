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


import uuid

from edgedb.server import _testbase as tb


class TestGraphQLFunctional(tb.QueryTestCase):
    SETUP = r"""
        CREATE MIGRATION test::d1 TO eschema $$
            abstract type NamedObject:
                required property name -> str

            type UserGroup extending NamedObject:
                link settings -> Setting:
                    cardinality := '**'

            type Setting extending NamedObject:
                required property value -> str

            type Profile extending NamedObject:
                required property value -> str
                property tags -> array<str>
                property odd -> array<int64>:
                    cardinality := '1*'

            type User extending NamedObject:
                required property active -> bool
                link groups -> UserGroup:
                    cardinality := '**'
                required property age -> int64
                required property score -> float64
                link profile -> Profile:
                    cardinality := '*1'
        $$;

        COMMIT MIGRATION test::d1;

        WITH MODULE test
        INSERT Setting {
            name := 'template',
            value := 'blue'
        };

        WITH MODULE test
        INSERT Setting {
            name := 'perks',
            value := 'full'
        };

        WITH MODULE test
        INSERT UserGroup {
            name := 'basic'
        };

        WITH MODULE test
        INSERT UserGroup {
            name := 'upgraded'
        };

        WITH MODULE test
        INSERT User {
            name := 'John',
            age := 25,
            active := True,
            score := 3.14,
            groups := (SELECT UserGroup FILTER UserGroup.name = 'basic')
        };

        WITH MODULE test
        INSERT User {
            name := 'Jane',
            age := 26,
            active := True,
            score := 1.23,
            groups := (SELECT UserGroup FILTER UserGroup.name = 'upgraded')
        };

        WITH MODULE test
        INSERT User {
            name := 'Alice',
            age := 27,
            active := True,
            score := 5.0
        };
    """

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
                'name': 'Jane',
                'age': 26,
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
