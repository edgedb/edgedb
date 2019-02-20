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

import edgedb

from edb.testbase import server as tb
from edb.tools import test


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
            query mixed {
                User {
                    name
                }
                Setting {
                    name
                }
            }
        """, {
            'User': [{
                'name': 'Alice',
            }, {
                'name': 'Bob',
            }, {
                'name': 'Jane',
            }, {
                'name': 'John',
            }],
            'Setting': [{
                'name': 'perks',
            }, {
                'name': 'template',
            }],
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_query_04(self):
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

    async def test_graphql_functional_query_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Cannot query field "Bogus" on type "Query"',
                line=3, col=21):
            await self.con._execute_graphql(r"""
                query {
                    Bogus {
                        name,
                        groups {
                            id
                            name
                        }
                    }
                }
            """)

    async def test_graphql_functional_query_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Cannot query field "bogus" on type "User"',
                line=5, col=25):
            await self.con._execute_graphql(r"""
                query {
                    User {
                        name,
                        bogus,
                        groups {
                            id
                            name
                        }
                    }
                }
            """)

    async def test_graphql_functional_query_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Cannot query field "age" on type "NamedObject"',
                line=5, col=25):
            await self.con._execute_graphql(r"""
                query {
                    NamedObject {
                        name,
                        age,
                        groups {
                            id
                            name
                        }
                    }
                }
            """)

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

    @test.xfail('''
        'last' is not fully implemented in all cases and ideally
        requires negative OFFSET to be implemented
    ''')
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

    async def test_graphql_functional_arguments_18(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type "String", found 42',
                line=3, col=34):
            await self.con._execute_graphql(r"""
                query {
                    User(filter: {name: {eq: 42}}) {
                        id,
                    }
                }
            """)

    async def test_graphql_functional_arguments_19(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type "String", found 20\.5',
                line=3, col=34):
            await self.con._execute_graphql(r"""
                query {
                    User(filter: {name: {eq: 20.5}}) {
                        id,
                    }
                }
            """)

    async def test_graphql_functional_arguments_20(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type "Float", found "3\.5"',
                line=3, col=34):
            await self.con._execute_graphql(r"""
                query {
                    User(filter: {score: {eq: "3.5"}}) {
                        id,
                    }
                }
            """)

    async def test_graphql_functional_arguments_21(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type "Boolean", found 0',
                line=3, col=34):
            await self.con._execute_graphql(r"""
                query {
                    User(filter: {active: {eq: 0}}) {
                        id,
                    }
                }
            """)

    async def test_graphql_functional_fragment_01(self):
        await self.assert_graphql_query_result(r"""
            fragment groupFrag on UserGroup {
                id
                name
            }

            query {
                User(filter: {name: {eq: "Jane"}}) {
                    name,
                    groups {
                        ... groupFrag
                    }
                }
            }
        """, {
            'User': [{
                'name': 'Jane',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                }]
            }],
        })

    async def test_graphql_functional_fragment_02(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag1 on User {
                name
                ... userFrag2
            }

            fragment userFrag2 on User {
                groups {
                    ... groupFrag
                }
            }

            fragment groupFrag on UserGroup {
                id
                name
            }

            query {
                User(filter: {name: {eq: "Jane"}}) {
                    ... userFrag1
                }
            }
        """, {
            'User': [{
                'name': 'Jane',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                }]
            }],
        })

    async def test_graphql_functional_fragment_03(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag2 on User {
                groups {
                    ... groupFrag
                }
            }

            fragment groupFrag on UserGroup {
                id
                name
            }

            query {
                User(filter: {name: {eq: "Jane"}}) {
                    ... on User {
                        name
                        ... userFrag2
                    }
                }
            }
        """, {
            'User': [{
                'name': 'Jane',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                }]
            }],
        })

    async def test_graphql_functional_fragment_04(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag1 on User {
                name
                ... {
                    groups {
                        ... groupFrag
                    }
                }
            }

            fragment groupFrag on UserGroup {
                id
                name
            }

            query {
                User(filter: {name: {eq: "Jane"}}) {
                    ... userFrag1
                }
            }
        """, {
            'User': [{
                'name': 'Jane',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                }]
            }],
        })

    async def test_graphql_functional_fragment_type_01(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag on User {
                id,
                name,
            }

            query {
                User(filter: {name: {eq: "Alice"}}) {
                    ... userFrag
                }
            }
        """, {
            'User': [{
                'id': uuid.UUID,
                'name': 'Alice',
            }],
        })

    async def test_graphql_functional_fragment_type_02(self):
        await self.assert_graphql_query_result(r"""
            fragment namedFrag on NamedObject {
                id,
                name,
            }

            query {
                User(filter: {name: {eq: "Alice"}}) {
                    ... namedFrag
                }
            }
        """, {
            'User': [{
                'id': uuid.UUID,
                'name': 'Alice',
            }],
        })

    async def test_graphql_functional_fragment_type_03(self):
        await self.assert_graphql_query_result(r"""
            fragment namedFrag on NamedObject {
                id,
                name,
            }

            fragment userFrag on User {
                ... namedFrag
                age
            }

            query {
                User(filter: {name: {eq: "Alice"}}) {
                    ... userFrag
                }
            }
        """, {
            'User': [{
                'id': uuid.UUID,
                'name': 'Alice',
                'age': 27,
            }],
        })

    async def test_graphql_functional_fragment_type_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'userFrag cannot be spread.*?'
                r'UserGroup can never be of type User',
                line=9, col=25):
            await self.con._execute_graphql(r"""
                fragment userFrag on User {
                    id,
                    name,
                }

                query {
                    UserGroup {
                        ... userFrag
                    }
                }
            """)

    async def test_graphql_functional_fragment_type_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'userFrag cannot be spread.*?'
                r'UserGroup can never be of type User',
                line=8, col=21):
            await self.con._execute_graphql(r"""
                fragment userFrag on User {
                    id,
                    name,
                }

                fragment groupFrag on UserGroup {
                    ... userFrag
                }

                query {
                    User {
                        ... userFrag
                        groups {
                            ... groupFrag
                        }
                    }
                }
            """)

    async def test_graphql_functional_fragment_type_06(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag on User {
                age
                score
            }

            query {
                NamedObject {
                    name
                    ... userFrag
                }
            }
        """, {
            "NamedObject": [
                {"age": 27, "name": "Alice", "score": 5},
                {"age": 21, "name": "Bob", "score": 4.2},
                {"age": 25, "name": "Jane", "score": 1.23},
                {"age": 25, "name": "John", "score": 3.14},
                {"age": None, "name": "basic", "score": None},
                {"age": None, "name": "perks", "score": None},
                {"age": None, "name": "template", "score": None},
                {"age": None, "name": "upgraded", "score": None},
            ]
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_fragment_type_07(self):
        await self.assert_graphql_query_result(r"""
            fragment frag on NamedObject {
                id,
                name,
            }

            query {
                NamedObject {
                    ... frag
                }
            }
        """, {
            "NamedObject": [
                {"id": uuid.UUID, "name": "Alice"},
                {"id": uuid.UUID, "name": "Bob"},
                {"id": uuid.UUID, "name": "Jane"},
                {"id": uuid.UUID, "name": "John"},
                {"id": uuid.UUID, "name": "basic"},
                {"id": uuid.UUID, "name": "perks"},
                {"id": uuid.UUID, "name": "template"},
                {"id": uuid.UUID, "name": "upgraded"},
            ]
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_fragment_type_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'Cannot query field "age" on type "NamedObject"',
                line=5, col=21):
            await self.con._execute_graphql(r"""
                fragment frag on NamedObject {
                    id,
                    name,
                    age,
                }

                query {
                    User {
                        ... frag
                    }
                }
            """)

    async def test_graphql_functional_fragment_type_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'Cannot query field "age" on type "NamedObject"',
                line=7, col=29):
            await self.con._execute_graphql(r"""
                query {
                    User {
                        ... on NamedObject {
                            id,
                            name,
                            age,
                        }
                    }
                }
            """)

    async def test_graphql_functional_fragment_type_10(self):
        await self.assert_graphql_query_result(r"""
            fragment namedFrag on NamedObject {
                id,
                name,
                ... userFrag
            }

            fragment userFrag on User {
                age
            }

            query {
                NamedObject {
                    ... namedFrag
                }
            }
        """, {
            "NamedObject": [
                {"id": uuid.UUID, "name": "Alice", "age": 27},
                {"id": uuid.UUID, "name": "Bob", "age": 21},
                {"id": uuid.UUID, "name": "Jane", "age": 25},
                {"id": uuid.UUID, "name": "John", "age": 25},
                {"id": uuid.UUID, "name": "basic", "age": None},
                {"id": uuid.UUID, "name": "perks", "age": None},
                {"id": uuid.UUID, "name": "template", "age": None},
                {"id": uuid.UUID, "name": "upgraded", "age": None},
            ]
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_fragment_type_11(self):
        await self.assert_graphql_query_result(r"""
            fragment namedFrag on NamedObject {
                id,
                name,
                ... userFrag
            }

            fragment userFrag on User {
                age
            }

            query {
                User {
                    ... namedFrag
                }
            }
        """, {
            "User": [
                {"id": uuid.UUID, "name": "Alice", "age": 27},
                {"id": uuid.UUID, "name": "Bob", "age": 21},
                {"id": uuid.UUID, "name": "Jane", "age": 25},
                {"id": uuid.UUID, "name": "John", "age": 25},
            ]
        }, sort=lambda x: x['name'])

    async def test_graphql_functional_fragment_type_12(self):
        await self.assert_graphql_query_result(r"""
            query {
                NamedObject(order: {name: {dir: ASC}}) {
                    ... on User {
                        age
                    }
                }
            }
        """, {
            "NamedObject": [
                {"age": 27},
                {"age": 21},
                {"age": 25},
                {"age": 25},
                {"age": None},
                {"age": None},
                {"age": None},
                {"age": None},
            ]
        })

    async def test_graphql_functional_directives_01(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    name @include(if: true),
                    groups @include(if: false) {
                        id
                        name
                    }
                }
            }
        """, {
            "User": [
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Jane"},
                {"name": "John"},
            ]
        })

    async def test_graphql_functional_directives_02(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    name @skip(if: true),
                    groups @skip(if: false) {
                        id @skip(if: true)
                        name @skip(if: false)
                    }
                }
            }
        """, {
            "User": [
                {"groups": []},
                {"groups": []},
                {"groups": [{"name": "upgraded"}]},
                {"groups": [{"name": "basic"}]},
            ]
        })

    async def test_graphql_functional_directives_03(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    name @skip(if: true), @include(if: true),

                    groups @skip(if: false), @include(if: true) {
                        id @skip(if: true), @include(if: false)
                        name @skip(if: false), @include(if: true)
                    }
                }
            }
        """, {
            "User": [
                {"groups": []},
                {"groups": []},
                {"groups": [{"name": "upgraded"}]},
                {"groups": [{"name": "basic"}]},
            ]
        })

    async def test_graphql_functional_directives_04(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag1 on User {
                name
                ... {
                    groups @include(if: false) {
                        ... groupFrag
                    }
                }
            }

            fragment groupFrag on UserGroup {
                id
                name
            }

            query {
                User(order: {name: {dir: ASC}}) {
                    ... userFrag1
                }
            }
        """, {
            "User": [
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Jane"},
                {"name": "John"},
            ]
        })

    async def test_graphql_functional_directives_05(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag1 on User {
                name
                ... @skip(if: true) {
                    groups {
                        ... groupFrag
                    }
                }
            }

            fragment groupFrag on UserGroup {
                id
                name
            }

            query {
                User(order: {name: {dir: ASC}}) {
                    ... userFrag1
                }
            }
        """, {
            "User": [
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Jane"},
                {"name": "John"},
            ]
        })

    async def test_graphql_functional_directives_06(self):
        await self.assert_graphql_query_result(r"""
            fragment userFrag1 on User {
                name
                ... {
                    groups {
                        ... groupFrag @skip(if: true)
                        name
                    }
                }
            }

            fragment groupFrag on UserGroup {
                id
            }

            query {
                User(order: {name: {dir: ASC}}) {
                    ... userFrag1
                }
            }
        """, {
            "User": [
                {"name": "Alice", "groups": []},
                {"name": "Bob", "groups": []},
                {"name": "Jane", "groups": [{"name": "upgraded"}]},
                {"name": "John", "groups": [{"name": "basic"}]},
            ]
        })

    async def test_graphql_functional_directives_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'invalid value "true"',
                line=4, col=43):
            await self.con._execute_graphql(r"""
                query {
                    User {
                        name @include(if: "true"),
                        id
                    }
                }
            """)

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

    async def test_graphql_functional_typename_03(self):
        await self.assert_graphql_query_result(r"""
            query {
                foo: __typename
                User(order: {name: {dir: ASC}}) {
                    name
                    bar: __typename
                }
            }
        """, {
            "foo": "Query",
            "User": [
                {"bar": "UserType", "name": "Alice"},
                {"bar": "PersonType", "name": "Bob"},
                {"bar": "UserType", "name": "Jane"},
                {"bar": "UserType", "name": "John"},
            ]
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
                        "locations": [
                            "FIELD_DEFINITION",
                            "ENUM_VALUE"
                        ],
                        "args": [
                            {
                                "name": "reason",
                                "defaultValue": '"No longer supported"',
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

    async def test_graphql_functional_schema_04(self):
        await self.assert_graphql_query_result(r"""
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

    async def test_graphql_functional_schema_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Unknown argument "name" on field "__schema" of type "Query"',
                line=3, col=30):
            await self.con._execute_graphql(r"""
                query {
                    __schema(name: "foo") {
                        __typename
                    }
                }
            """)

    async def test_graphql_functional_schema_06(self):
        await self.assert_graphql_query_result(r"""
            query {
                __schema {
                    types {
                        kind
                        name
                    }
                }
            }
        """, {
            "__schema": {
                "types": [
                    {'kind': 'SCALAR', 'name': 'Boolean'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterBoolean'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterFloat'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterFoo'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterID'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterInt'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterNamedObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterPerson'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterProfile'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterSetting'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterString'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterUser'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterUserGroup'},
                    {'kind': 'SCALAR', 'name': 'Float'},
                    {'kind': 'INTERFACE', 'name': 'Foo'},
                    {'kind': 'OBJECT', 'name': 'FooType'},
                    {'kind': 'SCALAR', 'name': 'ID'},
                    {'kind': 'SCALAR', 'name': 'Int'},
                    {'kind': 'INTERFACE', 'name': 'NamedObject'},
                    {'kind': 'INTERFACE', 'name': 'Object'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderFoo'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderNamedObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderPerson'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderProfile'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderSetting'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderUser'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderUserGroup'},
                    {'kind': 'INPUT_OBJECT', 'name': 'Ordering'},
                    {'kind': 'INTERFACE', 'name': 'Person'},
                    {'kind': 'OBJECT', 'name': 'PersonType'},
                    {'kind': 'INTERFACE', 'name': 'Profile'},
                    {'kind': 'OBJECT', 'name': 'ProfileType'},
                    {'kind': 'OBJECT', 'name': 'Query'},
                    {'kind': 'INTERFACE', 'name': 'Setting'},
                    {'kind': 'OBJECT', 'name': 'SettingType'},
                    {'kind': 'SCALAR', 'name': 'String'},
                    {'kind': 'INTERFACE', 'name': 'User'},
                    {'kind': 'INTERFACE', 'name': 'UserGroup'},
                    {'kind': 'OBJECT', 'name': 'UserGroupType'},
                    {'kind': 'OBJECT', 'name': 'UserType'},
                    {'kind': 'OBJECT', 'name': '__Directive'},
                    {'kind': 'ENUM', 'name': '__DirectiveLocation'},
                    {'kind': 'OBJECT', 'name': '__EnumValue'},
                    {'kind': 'OBJECT', 'name': '__Field'},
                    {'kind': 'OBJECT', 'name': '__InputValue'},
                    {'kind': 'OBJECT', 'name': '__Schema'},
                    {'kind': 'OBJECT', 'name': '__Type'},
                    {'kind': 'ENUM', 'name': '__TypeKind'},
                    {'kind': 'ENUM', 'name': 'directionEnum'},
                    {'kind': 'ENUM', 'name': 'nullsOrderingEnum'},
                ]
            }
        }, sort={
            'types': lambda x: x['name'],
        })

    async def test_graphql_functional_schema_07(self):
        await self.assert_graphql_query_result(r"""
            query {
                Foo : __schema {
                    types {
                        kind
                        name
                    }
                }
            }
        """, {
            "Foo": {
                "types": [
                    {'kind': 'SCALAR', 'name': 'Boolean'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterBoolean'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterFloat'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterFoo'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterID'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterInt'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterNamedObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterPerson'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterProfile'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterSetting'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterString'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterUser'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterUserGroup'},
                    {'kind': 'SCALAR', 'name': 'Float'},
                    {'kind': 'INTERFACE', 'name': 'Foo'},
                    {'kind': 'OBJECT', 'name': 'FooType'},
                    {'kind': 'SCALAR', 'name': 'ID'},
                    {'kind': 'SCALAR', 'name': 'Int'},
                    {'kind': 'INTERFACE', 'name': 'NamedObject'},
                    {'kind': 'INTERFACE', 'name': 'Object'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderFoo'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderNamedObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderPerson'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderProfile'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderSetting'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderUser'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderUserGroup'},
                    {'kind': 'INPUT_OBJECT', 'name': 'Ordering'},
                    {'kind': 'INTERFACE', 'name': 'Person'},
                    {'kind': 'OBJECT', 'name': 'PersonType'},
                    {'kind': 'INTERFACE', 'name': 'Profile'},
                    {'kind': 'OBJECT', 'name': 'ProfileType'},
                    {'kind': 'OBJECT', 'name': 'Query'},
                    {'kind': 'INTERFACE', 'name': 'Setting'},
                    {'kind': 'OBJECT', 'name': 'SettingType'},
                    {'kind': 'SCALAR', 'name': 'String'},
                    {'kind': 'INTERFACE', 'name': 'User'},
                    {'kind': 'INTERFACE', 'name': 'UserGroup'},
                    {'kind': 'OBJECT', 'name': 'UserGroupType'},
                    {'kind': 'OBJECT', 'name': 'UserType'},
                    {'kind': 'OBJECT', 'name': '__Directive'},
                    {'kind': 'ENUM', 'name': '__DirectiveLocation'},
                    {'kind': 'OBJECT', 'name': '__EnumValue'},
                    {'kind': 'OBJECT', 'name': '__Field'},
                    {'kind': 'OBJECT', 'name': '__InputValue'},
                    {'kind': 'OBJECT', 'name': '__Schema'},
                    {'kind': 'OBJECT', 'name': '__Type'},
                    {'kind': 'ENUM', 'name': '__TypeKind'},
                    {'kind': 'ENUM', 'name': 'directionEnum'},
                    {'kind': 'ENUM', 'name': 'nullsOrderingEnum'},
                ]
            }
        }, sort={
            'types': lambda x: x['name'],
        })

    async def test_graphql_functional_duplicates_01(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    name
                    name
                    name
                    age
                }
            }
        """, {
            'User': [
                {"age": 27, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Jane"},
                {"age": 25, "name": "John"},
            ]
        })

    async def test_graphql_functional_duplicates_02(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    name @include(if: true)
                    age
                    name @include(if: true)
                }
            }
        """, {
            'User': [
                {"age": 27, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Jane"},
                {"age": 25, "name": "John"},
            ]
        })

    async def test_graphql_functional_duplicates_03(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    ... on User @skip(if: false) {
                        name @include(if: true)
                    }
                    age
                    name @include(if: true)
                }
            }
        """, {
            'User': [
                {"age": 27, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Jane"},
                {"age": 25, "name": "John"},
            ]
        })

    async def test_graphql_functional_duplicates_04(self):
        await self.assert_graphql_query_result(r"""
            fragment f1 on User {
                name @include(if: true)
            }

            fragment f2 on User {
                age
                name @include(if: true)
                ... f1
            }

            query {
                User(order: {name: {dir: ASC}}) {
                    ... f2
                    age
                    name @include(if: true)
                }
            }
        """, {
            'User': [
                {"age": 27, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Jane"},
                {"age": 25, "name": "John"},
            ]
        })

    async def test_graphql_functional_duplicates_05(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    age
                    name
                    name @include(if: true)
                    name @skip(if: false)
                }
            }
        """, {
            'User': [
                {"age": 27, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Jane"},
                {"age": 25, "name": "John"},
            ]
        })

    @test.xfail('graphql parser has an issue here')
    async def test_graphql_functional_duplicates_06(self):
        await self.assert_graphql_query_result(r"""
            query {
                User(order: {name: {dir: ASC}}) {
                    ... @skip(if: false) {
                        name @include(if: true)
                    }
                    age
                    name
                }
            }
        """, {
            'User': [
                {"age": 27, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Jane"},
                {"age": 25, "name": "John"},
            ]
        })

    async def test_graphql_functional_duplicates_07(self):
        await self.assert_graphql_query_result(r"""
            fragment f1 on User {
                name @skip(if: false)
            }

            fragment f2 on User {
                age
                name @include(if: true)
                ... f1
            }

            query {
                User(order: {name: {dir: ASC}}) {
                    ... f2
                    age
                    name @include(if: true)
                }
            }
        """, {
            'User': [
                {"age": 27, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Jane"},
                {"age": 25, "name": "John"},
            ]
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_01(self):
        # FIXME: don't have a way of supplying the parameter $name
        await self.assert_graphql_query_result(r"""
            query($name: String) {
                User(filter: {name: {eq: $name}}) {
                    name,
                    groups {
                        name
                    }
                }
            }
        """, {
            'User': [{
                'name': 'John',
                'groups': [{
                    'name': 'basic',
                }]
            }],
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_03(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: Int = 3) {
                User(filter: {score: {eq: $val}}) {
                    id,
                }
            }
        """, {
            'User': [],
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_04(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: Boolean = true) {
                User(order: {name: {dir: ASC}}) {
                    name @include(if: $val),
                    groups @skip(if: $val) {
                        name
                    }
                }
            }
        """, {
            "User": [
                {"name": "Alice", "groups": []},
                {"name": "Bob", "groups": []},
                {"name": "Jane", "groups": [{"name": "upgraded"}]},
                {"name": "John", "groups": [{"name": "basic"}]},
            ]
        })

    async def test_graphql_functional_variables_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Boolean!" is required and '
                r'will not use the default value',
                line=2, col=40):
            await self.con._execute_graphql(r"""
                query($val: Boolean! = true) {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of required type "Boolean!" '
                r'was not provided',
                line=2, col=23):
            await self.con._execute_graphql(r"""
                query($val: Boolean!) {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_07(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: String = "John") {
                User(filter: {name: {eq: $val}}) {
                    age,
                }
            }
        """, {
            "User": [
                {"age": 25},
            ]
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_08(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: Int = 20) {
                User(filter: {age: {eq: $val}}) {
                    name,
                }
            }
        """, {
            "User": []
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_09(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: Float = 3.5) {
                User(filter: {score: {eq: $val}}) {
                    name,
                }
            }
        """, {
            "User": []
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_10(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: Int = 3) {
                User(filter: {score: {eq: $val}}) {
                    id,
                }
            }
        """, {
            "User": []
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_11(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: Float = 3) {
                User(filter: {score: {eq: $val}}) {
                    id,
                }
            }
        """, {
            "User": []
        })

    async def test_graphql_functional_variables_12(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Boolean" '
                r'has invalid default value: 1',
                line=2, col=39):
            await self.con._execute_graphql(r"""
                query($val: Boolean = 1) {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_13(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Boolean" '
                r'has invalid default value: "1"',
                line=2, col=39):
            await self.con._execute_graphql(r"""
                query($val: Boolean = "1") {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_14(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Boolean" '
                r'has invalid default value: 1\.3',
                line=2, col=39):
            await self.con._execute_graphql(r"""
                query($val: Boolean = 1.3) {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_15(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "String" '
                r'has invalid default value: 1',
                line=2, col=38):
            await self.con._execute_graphql(r"""
                query($val: String = 1) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_16(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "String" '
                r'has invalid default value: 1\.1',
                line=2, col=38):
            await self.con._execute_graphql(r"""
                query($val: String = 1.1) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "String" '
                r'has invalid default value: true',
                line=2, col=38):
            await self.con._execute_graphql(r"""
                query($val: String = true) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_18(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Int" '
                r'has invalid default value: 1\.1',
                line=2, col=35):
            await self.con._execute_graphql(r"""
                query($val: Int = 1.1) {
                    User(filter: {age: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_19(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Int" '
                r'has invalid default value: "1"',
                line=2, col=35):
            await self.con._execute_graphql(r"""
                query($val: Int = "1") {
                    User(filter: {age: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_20(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Int" '
                r'has invalid default value: true',
                line=2, col=35):
            await self.con._execute_graphql(r"""
                query($val: Int = true) {
                    User(filter: {age: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_21(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Float" '
                r'has invalid default value: "1"',
                line=2, col=37):
            await self.con._execute_graphql(r"""
                query($val: Float = "1") {
                    User(filter: {score: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_22(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "Float" '
                r'has invalid default value: true',
                line=2, col=37):
            await self.con._execute_graphql(r"""
                query($val: Float = true) {
                    User(filter: {score: {eq: $val}}) {
                        id
                    }
                }
            """)

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_23(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: ID = "1") {
                User(filter: {id: {eq: $val}}) {
                    name
                }
            }
        """, {
            "User": []
        })

    @test.not_implemented('query parameters are not yet implemented')
    async def test_graphql_functional_variables_24(self):
        # FIXME: don't have a way of supplying the parameter $val
        await self.assert_graphql_query_result(r"""
            query($val: ID = 1) {
                User(filter: {id: {eq: $val}}) {
                    name
                }
            }
        """, {
            "User": []
        })

    async def test_graphql_functional_variables_25(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "ID" '
                r'has invalid default value: 1\.1',
                line=2, col=34):
            await self.con._execute_graphql(r"""
                query($val: ID = 1.1) {
                    User(filter: {id: {eq: $val}}) {
                        name
                    }
                }
            """)

    async def test_graphql_functional_variables_26(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "ID" '
                r'has invalid default value: true',
                line=2, col=34):
            await self.con._execute_graphql(r"""
                query($val: ID = true) {
                    User(filter: {id: {eq: $val}}) {
                        name
                    }
                }
            """)

    async def test_graphql_functional_variables_27(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "\[String\]" '
                r'used in position expecting type "String"'):
            await self.con._execute_graphql(r"""
                query($val: [String] = "Foo") {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_28(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "\[String\]" '
                r'used in position expecting type "String"'):
            await self.con._execute_graphql(r"""
                query($val: [String]) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_29(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "\[String\]!" '
                r'used in position expecting type "String"'):
            await self.con._execute_graphql(r"""
                query($val: [String]!) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_30(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of required type "String!" '
                r'was not provided'):
            await self.con._execute_graphql(r"""
                query($val: String!) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_31(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "\[String\]" '
                r'has invalid default value: \["Foo", 123\]',
                line=2, col=40):
            await self.con._execute_graphql(r"""
                query($val: [String] = ["Foo", 123]) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_variables_32(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'val. of type "\[String\]" '
                r'used in position expecting type "String"'):
            await self.con._execute_graphql(r"""
                query($val: [String]) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    async def test_graphql_functional_enum_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type "String", found admin',
                line=4, col=39):
            await self.con._execute_graphql(r"""
                query {
                    # enum supplied instead of a string
                    UserGroup(filter: {name: {eq: admin}}) {
                        id,
                        name,
                    }
                }
            """)

    async def test_graphql_functional_type_01(self):
        await self.assert_graphql_query_result(r"""
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

    async def test_graphql_functional_type_02(self):
        await self.assert_graphql_query_result(r"""
            query {
                __type(name: "UserType") {
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
                "name": "UserType",
                "description": None,
                "interfaces": [
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

    async def test_graphql_functional_type_03(self):
        await self.assert_graphql_query_result(r"""
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
                "interfaces": None,
                "possibleTypes": [
                    {"name": "PersonType"},
                    {"name": "UserType"}
                ],
                "enumValues": None,
                "inputFields": None,
                "ofType": None,
            }
        }, sort={
            'possibleTypes': lambda x: x['name'],
        })

    async def test_graphql_functional_type_04(self):
        await self.assert_graphql_query_result(r"""
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

    async def test_graphql_functional_type_05(self):
        await self.assert_graphql_query_result(r"""
            fragment _t on __Type {
                __typename
                name
                kind
            }

            query {
                __type(name: "UserGroupType") {
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
                "name": "UserGroupType",
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

    async def test_graphql_functional_type_06(self):
        await self.assert_graphql_query_result(r"""
            query {
                __type(name: "ProfileType") {
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
                "name": "ProfileType",
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
                                            "name": "Int",
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

    async def test_graphql_functional_type_07(self):
        await self.assert_graphql_query_result(r"""
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
                    }
                ],
                "interfaces": None,
                "possibleTypes": [
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "PersonType",
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
                                        "name": "Int",
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
                        "name": "ProfileType",
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
                        ],
                        "possibleTypes": None,
                        "enumValues": None,
                        "inputFields": None,
                        "ofType": None
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "SettingType",
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
                        "name": "UserGroupType",
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
                        "name": "UserType",
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
                                        "name": "Int",
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
                    }
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

    async def test_graphql_functional_type_08(self):
        await self.assert_graphql_query_result(r"""
        query {
            __type(name: "UserGroupType") {
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
                "name": "UserGroupType",
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

    async def test_graphql_functional_type_09(self):
        await self.assert_graphql_query_result(r"""
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
                            "name": "FilterInt",
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

    async def test_graphql_functional_type_10(self):
        await self.assert_graphql_query_result(r"""
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

    async def test_graphql_functional_type_11(self):
        await self.assert_graphql_query_result(r"""
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
                        "defaultValue": '"SMALLEST"',
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

    async def test_graphql_functional_type_12(self):
        await self.assert_graphql_query_result(r"""
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

    async def test_graphql_functional_type_13(self):
        await self.assert_graphql_query_result(r"""
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
        """, {
            # NOTE: we mainly care about the GraphQL query being
            # parsed here, so we only test a portion of the output
            "__schema": {
                "types": [
                    {'kind': 'SCALAR', 'name': 'Boolean'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterBoolean'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterFloat'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterFoo'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterID'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterInt'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterNamedObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterPerson'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterProfile'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterSetting'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterString'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterUser'},
                    {'kind': 'INPUT_OBJECT', 'name': 'FilterUserGroup'},
                    {'kind': 'SCALAR', 'name': 'Float'},
                    {'kind': 'INTERFACE', 'name': 'Foo'},
                    {'kind': 'OBJECT', 'name': 'FooType'},
                    {'kind': 'SCALAR', 'name': 'ID'},
                    {'kind': 'SCALAR', 'name': 'Int'},
                    {'kind': 'INTERFACE', 'name': 'NamedObject'},
                    {'kind': 'INTERFACE', 'name': 'Object'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderFoo'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderNamedObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderObject'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderPerson'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderProfile'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderSetting'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderUser'},
                    {'kind': 'INPUT_OBJECT', 'name': 'OrderUserGroup'},
                    {'kind': 'INPUT_OBJECT', 'name': 'Ordering'},
                    {'kind': 'INTERFACE', 'name': 'Person'},
                    {'kind': 'OBJECT', 'name': 'PersonType'},
                    {'kind': 'INTERFACE', 'name': 'Profile'},
                    {'kind': 'OBJECT', 'name': 'ProfileType'},
                    {'kind': 'OBJECT', 'name': 'Query'},
                    {'kind': 'INTERFACE', 'name': 'Setting'},
                    {'kind': 'OBJECT', 'name': 'SettingType'},
                    {'kind': 'SCALAR', 'name': 'String'},
                    {'kind': 'INTERFACE', 'name': 'User'},
                    {'kind': 'INTERFACE', 'name': 'UserGroup'},
                    {'kind': 'OBJECT', 'name': 'UserGroupType'},
                    {'kind': 'OBJECT', 'name': 'UserType'},
                    {'kind': 'OBJECT', 'name': '__Directive'},
                    {'kind': 'ENUM', 'name': '__DirectiveLocation'},
                    {'kind': 'OBJECT', 'name': '__EnumValue'},
                    {'kind': 'OBJECT', 'name': '__Field'},
                    {'kind': 'OBJECT', 'name': '__InputValue'},
                    {'kind': 'OBJECT', 'name': '__Schema'},
                    {'kind': 'OBJECT', 'name': '__Type'},
                    {'kind': 'ENUM', 'name': '__TypeKind'},
                    {'kind': 'ENUM', 'name': 'directionEnum'},
                    {'kind': 'ENUM', 'name': 'nullsOrderingEnum'},
                ]
            }
        }, sort={
            'types': lambda x: x['name'],
        })
