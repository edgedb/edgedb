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
import unittest  # NOQA
import uuid

from edb.testbase import server as tb


class TestGraphQLFunctional(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'graphql.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'graphql_setup.eql')

    # GraphQL queries cannot run in a transaction
    ISOLATED_METHODS = False

    async def test_graphql_functional_query_01(self):
        await self.assert_graphql_query_result(r"""
            query {
                Setting {
                    name
                    value
                }
            }
        """, {
            'Setting': [{
                'name': 'perks',
                'value': 'full',
            }, {
                'name': 'template',
                'value': 'blue',
            }],
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_query_02(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
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
        })

    async def test_graphql_functional_query_03(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'User': [{
                'name': 'John',
                'age': 25,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                }]
            }],
        })

    async def test_graphql_functional_arguments_01(self):
        result = await self.graphql_query(r"""
            query {
                User {
                    id
                    name
                    age
                }
            }
        """)

        alice = [res for res in result['User']
                 if res['name'] == 'Alice'][0]

        result = await self.assert_graphql_query_result(f"""
            query {{
                User(filter: {{id: {{eq: "{alice['id']}"}}}}) {{
                    id
                    name
                    age
                }}
            }}
        """, {
            'User': [alice]
        })

    async def test_graphql_functional_arguments_02(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'User': [{
                'name': 'Bob',
                'age': 21,
                'groups': [],
            }],
        })

    async def test_graphql_functional_arguments_03(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(filter: {
                    and: [{name: {eq: "Bob"}}, {active: {eq: true}}],
                    age: {eq: 21}
                }) {
                    name
                    score
                }
            }
        """, {
            'User': [{
                'name': 'Bob',
                'score': 4.2,
            }],
        })

    async def test_graphql_functional_arguments_04(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(filter: {
                    not: {name: {eq: "Bob"}},
                    age: {eq: 21}
                }) {
                    name
                    score
                }
            }
        """, {
            'User': [],
        })

    async def test_graphql_functional_arguments_05(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'User': [
                {'name': 'Alice', 'score': 5},
                {'name': 'Jane', 'score': 1.23},
                {'name': 'John', 'score': 3.14},
            ],
        })

    async def test_graphql_functional_arguments_06(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'User': [
                {'name': 'Alice', 'score': 5},
                {'name': 'Jane', 'score': 1.23},
                {'name': 'John', 'score': 3.14},
            ],
        })

    async def test_graphql_functional_arguments_07(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(filter: {
                    name: {ilike: "%o%"},
                    age: {gt: 22}
                }) {
                    name
                    age
                }
            }
        """, {
            'User': [
                {'name': 'John', 'age': 25},
            ],
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_arguments_08(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'User': [
                {'name': 'John', 'score': 3.14},
            ],
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_arguments_09(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(filter: {
                    name: {ilike: "%e"},
                    age: {lte: 25}
                }) {
                    name
                    age
                }
            }
        """, {
            'User': [
                {'name': 'Jane', 'age': 25},
            ],
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_arguments_10(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'User': [
                {'age': 27, 'name': 'Alice'},
                {'age': 25, 'name': 'Jane'},
                {'age': 25, 'name': 'John'},
                {'age': 21, 'name': 'Bob'},
            ],
        })

    async def test_graphql_functional_arguments_11(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'User': [
                {'age': 27, 'name': 'Alice'},
                {'age': 21, 'name': 'Bob'},
                {'age': 25, 'name': 'Jane'},
                {'age': 25, 'name': 'John'},
            ],
        })

    async def test_graphql_functional_arguments_12(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'Foo': [
                {'after': None, 'select': 'a'},
                {'after': 'w', 'select': 'b'},
                {'after': 'q', 'select': None},
            ],
        })

    async def test_graphql_functional_arguments_13(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'Foo': [
                {'after': 'w', 'select': 'b'},
                {'after': None, 'select': 'a'},
                {'after': 'q', 'select': None},
            ],
        })

    async def test_graphql_functional_arguments_14(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(
                    order: {name: {dir: ASC}},
                    first: 2
                ) {
                    name
                    age
                }
            }
        """, {
            'User': [
                {'age': 27, 'name': 'Alice'},
                {'age': 21, 'name': 'Bob'},
            ],
        })

    async def test_graphql_functional_arguments_15(self):
        await self.assert_graphql_query_result(r"""
            query {
                u0: User(
                    order: {name: {dir: ASC}},
                    after: "0",
                    first: 2
                ) {
                    name
                }
                u1: User(
                    order: {name: {dir: ASC}},
                    first: 2
                ) {
                    name
                }
                u2: User(
                    order: {name: {dir: ASC}},
                    after: "0",
                    before: "2"
                ) {
                    name
                }
                u3: User(
                    order: {name: {dir: ASC}},
                    before: "2",
                    last: 1
                ) {
                    name
                }
            }
        """, {
            'u0': [
                {'name': 'Bob'},
                {'name': 'Jane'},
            ],
            'u1': [
                {'name': 'Alice'},
                {'name': 'Bob'},
            ],
            'u2': [
                {'name': 'Bob'},
            ],
            'u3': [
                {'name': 'Bob'},
            ],
        })

    # FIXME: 'last' is not fully implemented in all cases and ideally
    # requires negative OFFSET to be implemented
    @unittest.expectedFailure
    async def test_graphql_functional_arguments_16(self):
        await self.assert_graphql_query_result(r"""
            query {
                u4: User(
                    order: {name: {dir: ASC}},
                    after: "2",
                    last: 2
                ) {
                    name
                }
                u5: User(
                    order: {name: {dir: ASC}},
                    after: "0",
                    last: 2
                ) {
                    name
                }
                u6: User(
                    order: {name: {dir: ASC}},
                    after: "0",
                    before: "3",
                    first: 2,
                    last: 1
                ) {
                    name
                }
            }
        """, {
            'u4': [
                {'name': 'John'},
            ],
            'u5': [
                {'name': 'Jane'},
                {'name': 'John'},
            ],
            'u6': [
                {'name': 'Jane'},
            ],
        })

    async def test_graphql_functional_arguments_17(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "Jane"}}) {
                    name
                    groups {
                        name
                        settings(
                            order: {name: {dir: ASC}},
                            first: 1
                        ) {
                            name
                        }
                    }
                }
            }
        """, {
            'User': [{
                'name': 'Jane',
                'groups': [{
                    'name': 'upgraded',
                    'settings': [{
                        'name': 'perks'
                    }]
                }]
            }]
        })

    async def test_graphql_functional_fragment_02(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'age': 27,
                'score': 5,
            }],
        })

    async def test_graphql_functional_typename_01(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
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
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_typename_02(self):
        await self.assert_graphql_query_result(r"""
            query {
                __typename
                __schema {
                    __typename
                }
            }
        """, {
            '__typename': 'Query',
            '__schema': {
                '__typename': '__Schema',
            },
        })

    async def test_graphql_functional_schema_01(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
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
        }, sort={
            'directives': lambda x: x['name'],
        })

    async def test_graphql_functional_schema_02(self):
        await self.assert_graphql_query_result(r"""
            query {
                __schema {
                    mutationType {
                        name
                    }
                }
            }
        """, {
            '__schema': {
                'mutationType': None
            }
        })

    async def test_graphql_functional_schema_03(self):
        await self.assert_graphql_query_result(r"""
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
