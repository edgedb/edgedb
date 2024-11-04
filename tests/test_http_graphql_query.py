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


import json
import os
import uuid
import urllib

import edgedb

from edb.testbase import http as tb
from edb.tools import test


class TestGraphQLFunctional(tb.GraphQLTestCase):
    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'graphql.esdl')

    SCHEMA_OTHER = os.path.join(os.path.dirname(__file__), 'schemas',
                                'graphql_other.esdl')

    SCHEMA_OTHER_DEEP = os.path.join(os.path.dirname(__file__), 'schemas',
                                     'graphql_schema_other_deep.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'graphql_setup.edgeql')

    # GraphQL queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    def test_graphql_http_keepalive_01(self):
        with self.http_con() as con:
            for _ in range(3):
                req1_data = {
                    'query': '''
                        {
                            Setting(order: {value: {dir: ASC}}) {
                                value
                            }
                        }
                    '''
                }
                data, headers, status = self.http_con_request(
                    con, req1_data,
                    headers={
                        'Authorization': self.make_auth_header(),
                    },
                )
                self.assertEqual(status, 200)
                self.assertNotIn('connection', headers)
                self.assertEqual(
                    headers.get('content-type'),
                    'application/json')
                self.assertEqual(
                    json.loads(data)['data'],
                    {'Setting': [{'value': 'blue'}, {'value': 'full'},
                                 {'value': 'none'}]})

                req2_data = {
                    'query': '''
                        {
                            NON_EXISTING_TYPE {
                                name
                            }
                        }
                    '''
                }
                data, headers, status = self.http_con_request(
                    con, req2_data,
                    headers={
                        'Authorization': self.make_auth_header(),
                    },
                )
                self.assertEqual(status, 200)
                self.assertNotIn('connection', headers)
                self.assertEqual(
                    headers.get('content-type'),
                    'application/json')
                self.assertIn(
                    'QueryError:',
                    json.loads(data)['errors'][0]['message'])

    def test_graphql_http_errors_01(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(
                con, {}, path='non-existant',
                headers={
                    'Authorization': self.make_auth_header(),
                },
            )

            self.assertEqual(status, 404)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'Unknown path', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant2')

    def test_graphql_http_errors_02(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(
                con, {},
                headers={
                    'Authorization': self.make_auth_header(),
                },
            )

            self.assertEqual(status, 400)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'query is missing', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant')

    def test_graphql_http_errors_03(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(
                con, {'query': 'blah', 'variables': 'bazz'},
                headers={
                    'Authorization': self.make_auth_header(),
                },
            )

            self.assertEqual(status, 400)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'must be a JSON object', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant')

    def test_graphql_http_errors_04(self):
        with self.http_con() as con:
            con.send(b'blah\r\n\r\n\r\n\r\n')
            data, headers, status = self.http_con_request(
                con, {'query': 'blah', 'variables': 'bazz'},
                headers={
                    'Authorization': self.make_auth_header(),
                },
            )

            self.assertEqual(status, 400)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'HttpParserInvalidMethodError', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant')

    def test_graphql_functional_query_01(self):
        for _ in range(10):  # repeat to test prepared pgcon statements
            self.assert_graphql_query_result(r"""
                query {
                    Setting {
                        name
                        value
                    }
                }
            """, {
                'Setting': [{
                    'name': 'template',
                    'value': 'blue',
                }, {
                    'name': 'perks',
                    'value': 'full',
                }, {
                    'name': 'template',
                    'value': 'none',
                }],
            }, sort=lambda x: x['value'])

    def test_graphql_functional_query_02(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_query_03(self):
        self.assert_graphql_query_result(r"""
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
            }, {
                'name': 'template',
            }],
        }, sort=lambda x: x['name'])

    def test_graphql_functional_query_04(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_query_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'Bogus' on type 'Query'",
                _line=3, _col=21):
            self.graphql_query(r"""
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

    def test_graphql_functional_query_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'bogus' on type 'User'",
                _line=5, _col=25):
            self.graphql_query(r"""
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

    def test_graphql_functional_query_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'age' on type 'NamedObject'",
                _line=5, _col=25):
            self.graphql_query(r"""
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

    def test_graphql_functional_query_08(self):
        self.assert_graphql_query_result(
            r"""
                query names {
                    Setting {
                        name
                    }
                }

                query values {
                    Setting {
                        value
                    }
                }
            """,
            {
                'Setting': [{
                    'name': 'perks',
                }, {
                    'name': 'template',
                }, {
                    'name': 'template',
                }],
            },
            sort=lambda x: x['name'],
            operation_name='names'
        )

        self.assert_graphql_query_result(
            r"""
                query names {
                    Setting {
                        name
                    }
                }

                query values {
                    Setting {
                        value
                    }
                }
            """,
            {
                'Setting': [{
                    'value': 'blue',
                }, {
                    'value': 'full',
                }, {
                    'value': 'none',
                }],
            },
            sort=lambda x: x['value'],
            operation_name='values',
            use_http_post=False
        )

    def test_graphql_functional_query_09(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'provide operation name'):

            self.graphql_query('''
                query names {
                    Setting {
                        name
                    }
                }

                query values {
                    Setting {
                        value
                    }
                }
            ''')

    def test_graphql_functional_query_10(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'unknown operation named "foo"'):

            self.graphql_query('''
                query names {
                    Setting {
                        name
                    }
                }

                query values {
                    Setting {
                        value
                    }
                }
            ''', operation_name='foo')

    def test_graphql_functional_query_11(self):
        # Test that parse error marshal from the compiler correctly.
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r"Expected Name, found '}'",
                                    _line=4, _col=21):
            self.graphql_query(r"""
                query {
                    Setting {
                    }
                }
            """)

    def test_graphql_functional_query_12(self):
        # Regression test: variables names were shadowing query names.
        self.assert_graphql_query_result(
            r"""
                query users($name: String, $age: Int64) {
                    User(filter: {or: [{name: {eq: $name}},
                                       {age: {gt: $age}}]},
                         order: {name: {dir: ASC}})
                    {
                        name
                        age
                    }
                }

                query settings {
                    Setting {
                        name
                    }
                }
            """,
            {
                'User': [{
                    'name': 'Alice',
                    'age': 27
                }],
            },
            variables={'age': 25, 'name': 'Alice'},
            operation_name='users'
        )

    def test_graphql_functional_query_13(self):
        # Test special case errors.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'gibberish' on type 'Query'\. "
                r"There's no corresponding type or alias \"gibberish\" "
                r"exposed in Gel\. Please check the configuration settings "
                r"for this port to make sure that you're connecting to the "
                r"right database\.",
                _line=3, _col=21):
            self.graphql_query(r"""
                query {
                    gibberish
                }
            """)

    def test_graphql_functional_query_14(self):
        # Test special case errors.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'more__gibberish' on type 'Query'\. "
                r"There's no corresponding type or alias \"more::gibberish\" "
                r"exposed in Gel\. Please check the configuration settings "
                r"for this port to make sure that you're connecting to the "
                r"right database\.",
                _line=3, _col=21):
            self.graphql_query(r"""
                query {
                    more__gibberish
                }
            """)

    def test_graphql_functional_query_15(self):
        # Test special case errors.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'Uxer' on type 'Query'\. "
                r"Did you mean 'User'\?",
                _line=3, _col=21):
            self.graphql_query(r"""
                query {
                    Uxer
                }
            """)

    def test_graphql_functional_query_16(self):
        # test filtering by nested object
        self.assert_graphql_query_result(r"""
            query {
                User(filter: {groups: {name: {eq: "basic"}}}) {
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

    def test_graphql_functional_query_17(self):
        # Test unused & null variables
        self.assert_graphql_query_result(
            r"""
                query Person {
                    Person
                    {
                        name
                    }
                }
            """,
            {
                'Person': [{
                    'name': 'Bob',
                }],
            },
            variables={'name': None},
        )

        self.assert_graphql_query_result(
            r"""
                query Person($name: String) {
                    Person(filter: {name: {eq: $name}})
                    {
                        name
                    }
                }
            """,
            {
                'Person': [],
            },
            variables={'name': None},
        )

    def test_graphql_functional_query_18(self):
        # test filtering by nested object
        self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "Alice"}}) {
                    name
                    favorites(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            'User': [{
                'name': 'Alice',
                'favorites': [
                    {'name': 'basic'},
                    {'name': 'perks'},
                    {'name': 'template'},
                    {'name': 'template'},
                    {'name': 'unused'},
                    {'name': 'upgraded'},
                ]
            }],
        })

    def test_graphql_functional_query_19(self):
        # Test built-in object types, by making sure we can query them
        # and get some results.
        res = self.graphql_query(r"""
            {
                Object {id}
            }
        """)
        self.assertTrue(len(res) > 0,
                        'querying "Object" returned no results')

    def test_graphql_functional_query_20(self):
        # Test built-in object types, by making sure we can query them
        # and get some results.
        res = self.graphql_query(r"""
            {
                BaseObject {id}
            }
        """)
        self.assertTrue(len(res) > 0,
                        'querying "BaseObject" returned no results')

    def test_graphql_functional_query_21(self):
        # test filtering by nested object
        self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "Bob"}}) {
                    name
                    fav_users(order: {name: {dir: ASC}}) {
                        name
                        # only present in User
                        active
                        score
                    }
                }
            }
        """, {
            'User': [{
                'name': 'Bob',
                'fav_users': [
                    {
                        'name': 'Alice',
                        'active': True,
                        'score': 5,
                    },
                    {
                        'name': 'Jane',
                        'active': True,
                        'score': 1.23,
                    },
                    {
                        'name': 'John',
                        'active': True,
                        'score': 3.14,
                    },
                ]
            }],
        })

    def test_graphql_functional_query_22(self):
        # get an object that has a bunch of readonly properties
        self.assert_graphql_query_result(r"""
            query {
                NotEditable {
                    __typename
                    id
                    computed
                    once
                }
            }
        """, {
            'NotEditable': [{
                '__typename': 'NotEditable_Type',
                'id': uuid.UUID,
                'computed': 'a computed value',
                'once': 'init',
            }],
        })

    def test_graphql_functional_query_23(self):
        # get an object from a deeply nested module
        self.assert_graphql_query_result(r"""
            query {
                other__deep__NestedMod {
                    __typename
                    val
                }
            }
        """, {
            'other__deep__NestedMod': [{
                '__typename': 'other__deep__NestedMod_Type',
                'val': 'in nested module',
            }],
        })

    def test_graphql_functional_alias_01(self):
        self.assert_graphql_query_result(
            r"""
                {
                    SettingAlias {
                        __typename
                        name
                        value
                    }
                    Setting {
                        __typename
                        name
                        value
                    }
                }
            """,
            {
                "SettingAlias": [
                    {
                        "__typename": "SettingAlias",
                        "name": "template",
                        "value": "blue",
                    },
                    {
                        "__typename": "SettingAlias",
                        "name": "perks",
                        "value": "full",
                    },
                    {
                        "__typename": "SettingAlias",
                        "name": "template",
                        "value": "none",
                    },
                ],
                "Setting": [
                    {
                        "__typename": "Setting_Type",
                        "name": "template",
                        "value": "blue",
                    },
                    {
                        "__typename": "Setting_Type",
                        "name": "perks",
                        "value": "full",
                    },
                    {
                        "__typename": "Setting_Type",
                        "name": "template",
                        "value": "none",
                    },
                ],
            },
            sort=lambda x: x['value']
        )

    def test_graphql_functional_alias_02(self):
        self.assert_graphql_query_result(
            r"""
                {
                    SettingAlias {
                        __typename
                        name
                        value
                        of_group {
                            __typename
                            name
                        }
                    }
                }
            """,
            {
                "SettingAlias": [
                    {
                        "__typename": "SettingAlias",
                        "name": "template",
                        "value": "blue",
                        "of_group": {
                            "__typename": "UserGroup_Type",
                            "name": "upgraded",
                        }
                    },
                    {
                        "__typename": "SettingAlias",
                        "name": "perks",
                        "value": "full",
                        "of_group": {
                            "__typename": "UserGroup_Type",
                            "name": "upgraded",
                        }
                    },
                    {
                        "__typename": "SettingAlias",
                        "name": "template",
                        "value": "none",
                        "of_group": {
                            "__typename": "UserGroup_Type",
                            "name": "unused",
                        }
                    },
                ],
            },
            sort=lambda x: x['value']
        )

    def test_graphql_functional_alias_03(self):
        self.assert_graphql_query_result(
            r"""
                {
                    SettingAliasAugmented {
                        __typename
                        name
                        value
                        of_group {
                            __typename
                            name
                            name_upper
                        }
                    }
                }
            """,
            {
                "SettingAliasAugmented": [
                    {
                        "__typename": "SettingAliasAugmented",
                        "name": "template",
                        "value": "blue",
                        "of_group": {
                            "__typename":
                                "__SettingAliasAugmented__of_group",
                            "name": "upgraded",
                            "name_upper": "UPGRADED",
                        }
                    },
                    {
                        "__typename": "SettingAliasAugmented",
                        "name": "perks",
                        "value": "full",
                        "of_group": {
                            "__typename":
                                "__SettingAliasAugmented__of_group",
                            "name": "upgraded",
                            "name_upper": "UPGRADED",
                        }
                    },
                    {
                        "__typename": "SettingAliasAugmented",
                        "name": "template",
                        "value": "none",
                        "of_group": {
                            "__typename":
                                "__SettingAliasAugmented__of_group",
                            "name": "unused",
                            "name_upper": "UNUSED",
                        }
                    },
                ],
            },
            sort=lambda x: x['value']
        )

    def test_graphql_functional_alias_04(self):
        self.assert_graphql_query_result(
            r"""
                {
                    ProfileAlias {
                        __typename
                        name
                        value
                        owner {
                            __typename
                            id
                        }
                    }
                }
            """,
            {
                "ProfileAlias": [
                    {
                        "__typename": "ProfileAlias",
                        "name": "Alice profile",
                        "value": "special",
                        "owner": [
                            {
                                "__typename": "User_Type",
                                "id": uuid.UUID,
                            }
                        ]
                    },
                    {
                        "__typename": "ProfileAlias",
                        "name": "Bob profile",
                        "value": "special",
                        "owner": [
                            {
                                "__typename": "Person_Type",
                                "id": uuid.UUID,
                            }
                        ]
                    }
                ]
            },
        )

        result = self.graphql_query(r"""
            query {
                ProfileAlias {
                    owner {
                        id
                    }
                }
            }
        """)
        user_id = result['ProfileAlias'][0]['owner'][0]['id']

        self.assert_graphql_query_result(f"""
            query {{
                User(filter: {{id: {{eq: "{user_id}"}}}}) {{
                    name
                }}
            }}
        """, {
            'User': [{'name': 'Alice'}]
        })

    def test_graphql_functional_alias_05(self):
        self.assert_graphql_query_result(
            r"""
                {
                    SettingAliasAugmented(
                        filter: {of_group: {name_upper: {eq: "UPGRADED"}}}
                    ) {
                        name
                        of_group {
                            name
                            name_upper
                        }
                    }
                }
            """,
            {
                "SettingAliasAugmented": [
                    {
                        "name": "perks",
                        "of_group": {
                            "name": "upgraded",
                            "name_upper": "UPGRADED",
                        }
                    },
                    {
                        "name": "template",
                        "of_group": {
                            "name": "upgraded",
                            "name_upper": "UPGRADED",
                        }
                    },
                ],
            },
            sort=lambda x: x['name']
        )

    def test_graphql_functional_alias_06(self):
        self.assert_graphql_query_result(
            r"""
                {
                    SettingAliasAugmented(
                        filter: {name: {eq: "perks"}}
                    ) {
                        name
                        of_group(
                            filter: {name_upper: {gt: "U"}}
                        ) {
                            name
                            name_upper
                        }
                    }
                }
            """,
            {
                "SettingAliasAugmented": [
                    {
                        "name": "perks",
                        "of_group": {
                            "name": "upgraded",
                            "name_upper": "UPGRADED",
                        }
                    },
                ],
            },
        )

    def test_graphql_functional_alias_07(self):
        self.assert_graphql_query_result(
            r"""
                {
                    TwoUsers {
                        name
                        initial
                    }
                }
            """,
            {
                "TwoUsers": [
                    {
                        "name": "Alice",
                        "initial": "A",
                    },
                    {
                        "name": "Bob",
                        "initial": "B",
                    },
                ],
            },
        )

    def test_graphql_functional_alias_08(self):
        self.assert_graphql_query_result(
            r"""
                {
                    TwoUsers(
                        filter: {initial: {eq: "A"}}
                    ) {
                        name
                        initial
                    }
                }
            """,
            {
                "TwoUsers": [
                    {
                        "name": "Alice",
                        "initial": "A",
                    },
                ],
            },
        )

    def test_graphql_functional_arguments_01(self):
        result = self.graphql_query(r"""
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

        self.assert_graphql_query_result(f"""
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

    def test_graphql_functional_arguments_02(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_03(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_04(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_05(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_06(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_07(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_08(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_09(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_10(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_11(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_12(self):
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    order: {
                        select: {dir: ASC, nulls: BIGGEST}
                    }
                ) {
                    after
                    select
                }
            }
        """, {
            'other__Foo': [
                {'after': None, 'select': 'a'},
                {'after': 'w', 'select': 'b'},
                {'after': 'q', 'select': None},
            ],
        })

    def test_graphql_functional_arguments_13(self):
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    order: {
                        select: {dir: DESC, nulls: SMALLEST}
                    }
                ) {
                    after
                    select
                }
            }
        """, {
            'other__Foo': [
                {'after': 'w', 'select': 'b'},
                {'after': None, 'select': 'a'},
                {'after': 'q', 'select': None},
            ],
        })

    def test_graphql_functional_arguments_14(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_15(self):
        self.assert_graphql_query_result(r"""
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

    @test.xerror('''
        'last' is not fully implemented in all cases and ideally
        requires negative OFFSET to be implemented
    ''')
    def test_graphql_functional_arguments_16(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_17(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_arguments_18(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type String, found 42',
                _line=3, _col=46):
            self.graphql_query(r"""
                query {
                    User(filter: {name: {eq: 42}}) {
                        id,
                    }
                }
            """)

    def test_graphql_functional_arguments_19(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type String, found 20\.5',
                _line=3, _col=46):
            self.graphql_query(r"""
                query {
                    User(filter: {name: {eq: 20.5}}) {
                        id,
                    }
                }
            """)

    def test_graphql_functional_arguments_20(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type Float, found "3\.5"',
                _line=3, _col=47):
            self.graphql_query(r"""
                query {
                    User(filter: {score: {eq: "3.5"}}) {
                        id,
                    }
                }
            """)

    def test_graphql_functional_arguments_21(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Expected type Boolean, found 0',
                _line=3, _col=48):
            self.graphql_query(r"""
                query {
                    User(filter: {active: {eq: 0}}) {
                        id,
                    }
                }
            """)

    def test_graphql_functional_arguments_22(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                # this error message is subpar, but this is what we get
                # from postgres, because we transfer bigint values to postgres
                # as strings
                r'invalid input syntax for type Int64: "aaaaa"',
                # _line=5, _col=32,
        ):
            self.graphql_query(r"""
                query {
                    u0: User(
                        order: {name: {dir: ASC}},
                        after: "aaaaa",
                        first: 2
                    ) {
                        name
                    }
                }
            """)

    def test_graphql_functional_arguments_23(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    order: {name: {dir: ASC}},
                    first: 1
                ) {
                    name
                }
            }
        """, {
            'User': [{
                'name': 'Alice',
            }]
        })

    def test_graphql_functional_arguments_24(self):
        # Test boolean AND handling {} like Postgres
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    filter: {
                        not: {
                            color: {eq: GREEN},
                            after: {neq: "b"},
                        },
                    },
                    order: {color: {dir: ASC}}
                ) {
                    select
                    after
                    color
                }
            }
        """, {
            "other__Foo": [{
                "select": "a",
                "after": None,
                "color": "RED",
            }, {
                "select": None,
                "after": "q",
                "color": "BLUE",
            }]
        })

    def test_graphql_functional_arguments_25(self):
        # Test boolean AND handling {} like Postgres
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    filter: {
                        not: {
                          and: [
                            {color: {eq: GREEN}},
                            {after: {neq: "b"}},
                          ]
                        },
                    },
                    order: {color: {dir: ASC}}
                ) {
                    select
                    after
                    color
                }
            }
        """, {
            "other__Foo": [{
                "select": "a",
                "after": None,
                "color": "RED",
            }, {
                "select": None,
                "after": "q",
                "color": "BLUE",
            }]
        })

    def test_graphql_functional_arguments_26(self):
        # Test boolean OR handling {} like Postgres
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    filter: {
                      or: [
                        {color: {neq: GREEN}},
                        {after: {eq: "b"}},
                      ]
                    },
                    order: {color: {dir: ASC}}
                ) {
                    select
                    after
                    color
                }
            }
        """, {
            "other__Foo": [{
                "select": "a",
                "after": None,
                "color": "RED",
            }, {
                "select": None,
                "after": "q",
                "color": "BLUE",
            }]
        })

    def test_graphql_functional_enums_01(self):
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    order: {color: {dir: DESC}},
                    first: 1
                ) {
                    select
                    color
                }
            }
        """, {
            'other__Foo': [{
                'select': None,
                'color': "BLUE",
            }]
        })

    def test_graphql_functional_enums_02(self):
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    order: {color: {dir: ASC}},
                    after: "0"
                ) {
                    select
                    color
                }
            }
        """, {
            "other__Foo": [{
                "select": "b",
                "color": "GREEN",
            }, {
                "select": None,
                "color": "BLUE",
            }]
        })

    def test_graphql_functional_enums_03(self):
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(
                    filter: {color: {eq: RED}},
                ) {
                    select
                    color
                }
            }
        """, {
            "other__Foo": [{
                "select": "a",
                "color": "RED",
            }]
        })

    def test_graphql_functional_enums_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'String cannot represent a non string value: admin',
                _line=4, _col=51):
            self.graphql_query(r"""
                query {
                    # enum supplied instead of a string
                    UserGroup(filter: {name: {eq: admin}}) {
                        id,
                        name,
                    }
                }
            """)

    def test_graphql_functional_fragment_01(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_fragment_02(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_fragment_03(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_fragment_04(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_fragment_05(self):
        # ISSUE #3514
        #
        # Fragment on the actual type should also work.
        self.assert_graphql_query_result(r"""
            fragment userFrag on User_Type {
                active
                profile {
                    value
                }
            }

            query {
                User(filter: {name: {eq: "Alice"}}) {
                    name
                    ... userFrag
                }
            }
        """, {
            'User': [{
                'name': 'Alice',
                'active': True,
                'profile': {
                    'value': 'special',
                }
            }],
        })

    def test_graphql_functional_fragment_06(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_fragment_07(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_fragment_08(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_fragment_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Fragment 'userFrag' cannot be spread here "
                r"as objects of type 'UserGroup' can never be of type 'User'.",
                _line=9, _col=25):
            self.graphql_query(r"""
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

    def test_graphql_functional_fragment_10(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Fragment 'userFrag' cannot be spread here "
                r"as objects of type 'UserGroup' can never be of type 'User'.",
                _line=8, _col=21):
            self.graphql_query(r"""
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

    def test_graphql_functional_fragment_11(self):
        self.assert_graphql_query_result(r"""
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
                {"age": None, "name": "1st", "score": None},
                {"age": None, "name": "2nd", "score": None},
                {"age": None, "name": "3rd", "score": None},
                {"age": None, "name": "4th", "score": None},
                {"age": 27, "name": "Alice", "score": 5},
                {"age": None, "name": "Alice profile", "score": None},
                {"age": 21, "name": "Bob", "score": 4.2},
                {"age": None, "name": "Bob profile", "score": None},
                {"age": 25, "name": "Jane", "score": 1.23},
                {"age": 25, "name": "John", "score": 3.14},
                {"age": None, "name": "basic", "score": None},
                {"age": None, "name": "perks", "score": None},
                {"age": None, "name": "template", "score": None},
                {"age": None, "name": "template", "score": None},
                {"age": None, "name": "unused", "score": None},
                {"age": None, "name": "upgraded", "score": None},
            ]
        }, sort=lambda x: x['name'])

    def test_graphql_functional_fragment_12(self):
        self.assert_graphql_query_result(r"""
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
                {"id": uuid.UUID, "name": "1st"},
                {"id": uuid.UUID, "name": "2nd"},
                {"id": uuid.UUID, "name": "3rd"},
                {"id": uuid.UUID, "name": "4th"},
                {"id": uuid.UUID, "name": "Alice"},
                {"id": uuid.UUID, "name": "Alice profile"},
                {"id": uuid.UUID, "name": "Bob"},
                {"id": uuid.UUID, "name": "Bob profile"},
                {"id": uuid.UUID, "name": "Jane"},
                {"id": uuid.UUID, "name": "John"},
                {"id": uuid.UUID, "name": "basic"},
                {"id": uuid.UUID, "name": "perks"},
                {"id": uuid.UUID, "name": "template"},
                {"id": uuid.UUID, "name": "template"},
                {"id": uuid.UUID, "name": "unused"},
                {"id": uuid.UUID, "name": "upgraded"},
            ]
        }, sort=lambda x: x['name'])

    def test_graphql_functional_fragment_13(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "Cannot query field 'age' on type 'NamedObject'",
                _line=5, _col=21):
            self.graphql_query(r"""
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

    def test_graphql_functional_fragment_14(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "Cannot query field 'age' on type 'NamedObject'",
                _line=7, _col=29):
            self.graphql_query(r"""
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

    def test_graphql_functional_fragment_15(self):
        self.assert_graphql_query_result(r"""
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
                {"id": uuid.UUID, "name": "1st", "age": None},
                {"id": uuid.UUID, "name": "2nd", "age": None},
                {"id": uuid.UUID, "name": "3rd", "age": None},
                {"id": uuid.UUID, "name": "4th", "age": None},
                {"id": uuid.UUID, "name": "Alice", "age": 27},
                {"id": uuid.UUID, "name": "Alice profile", "age": None},
                {"id": uuid.UUID, "name": "Bob", "age": 21},
                {"id": uuid.UUID, "name": "Bob profile", "age": None},
                {"id": uuid.UUID, "name": "Jane", "age": 25},
                {"id": uuid.UUID, "name": "John", "age": 25},
                {"id": uuid.UUID, "name": "basic", "age": None},
                {"id": uuid.UUID, "name": "perks", "age": None},
                {"id": uuid.UUID, "name": "template", "age": None},
                {"id": uuid.UUID, "name": "template", "age": None},
                {"id": uuid.UUID, "name": "unused", "age": None},
                {"id": uuid.UUID, "name": "upgraded", "age": None},
            ]
        }, sort=lambda x: x['name'])

    def test_graphql_functional_fragment_16(self):
        self.assert_graphql_query_result(r"""
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

    @test.xerror(
        "Known collation issue on Heroku Postgres",
        unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    )
    def test_graphql_functional_fragment_17(self):
        self.assert_graphql_query_result(r"""
            query {
                NamedObject(order: {name: {dir: ASC}}) {
                    ... on User {
                        age
                    }
                }
            }
        """, {
            "NamedObject": [
                {"age": None},
                {"age": None},
                {"age": None},
                {"age": None},
                {"age": 27},
                {"age": None},
                {"age": 21},
                {"age": None},
                {"age": 25},
                {"age": 25},
                {"age": None},
                {"age": None},
                {"age": None},
                {"age": None},
                {"age": None},
                {"age": None},
            ]
        })

    def test_graphql_functional_fragment_18(self):
        # ISSUE #1800
        #
        # After using a typed inline fragment the nested fields or
        # fields following after the fragment are erroneously using
        # the type intersection.
        self.assert_graphql_query_result(r"""
            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    ... on User {
                        active
                        profile {
                            value
                        }
                    }
                    name
                }
            }
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'active': True,
                'profile': {
                    'value': 'special',
                }
            }],
        })

    def test_graphql_functional_fragment_19(self):
        # ISSUE #1800
        #
        # After using a typed inline fragment the nested fields or
        # fields following after the fragment are erroneously using
        # the type intersection.
        self.assert_graphql_query_result(r"""
            fragment userFrag on User {
                active
                profile {
                    value
                }
            }

            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    ... userFrag
                    name
                }
            }
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'active': True,
                'profile': {
                    'value': 'special',
                }
            }],
        })

    def test_graphql_functional_fragment_20(self):
        # ISSUE #1800
        #
        # After using a typed inline fragment the nested fields or
        # fields following after the fragment are erroneously using
        # the type intersection.
        self.assert_graphql_query_result(r"""
            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    ... on User {
                        active
                        profile(filter: {name: {eq: "Alice profile"}}) {
                            value
                        }
                    }
                    name
                }
            }
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'active': True,
                'profile': {
                    'value': 'special',
                }
            }],
        })

        self.assert_graphql_query_result(r"""
            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    ... on User {
                        active
                        profile(filter: {name: {eq: "no such profile"}}) {
                            value
                        }
                    }
                    name
                }
            }
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'active': True,
                'profile': None,
            }],
        })

    def test_graphql_functional_fragment_21(self):
        # ISSUE #1800
        #
        # After using a typed inline fragment the nested fields or
        # fields following after the fragment are erroneously using
        # the type intersection.
        self.assert_graphql_query_result(r"""
            fragment userFrag on User {
                active
                profile(filter: {name: {eq: "Alice profile"}}) {
                    value
                }
            }

            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    ... userFrag
                    name
                }
            }
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'active': True,
                'profile': {
                    'value': 'special',
                }
            }],
        })

        self.assert_graphql_query_result(r"""
            fragment userFrag on User {
                active
                profile(filter: {name: {eq: "no such profile"}}) {
                    value
                }
            }

            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    ... userFrag
                    name
                }
            }
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'active': True,
                'profile': None,
            }],
        })

    def test_graphql_functional_fragment_22(self):
        # ISSUE #1800
        #
        # After using a typed inline fragment the nested fields or
        # fields following after the fragment are erroneously using
        # the type intersection.
        self.assert_graphql_query_result(r"""
            query {
                NamedObject(filter: {name: {eq: "Alice"}}) {
                    ... on User {
                        ... {
                            active
                            profile {
                                value
                            }
                        }
                    }
                    name
                }
            }
        """, {
            'NamedObject': [{
                'name': 'Alice',
                'active': True,
                'profile': {
                    'value': 'special',
                }
            }],
        })

    def test_graphql_functional_fragment_23(self):
        # ISSUE #3514
        #
        # Fragment on the actual type should also work.
        self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "Alice"}}) {
                    ... on User_Type {
                        active
                        profile {
                            value
                        }
                    }
                    name
                }
            }
        """, {
            'User': [{
                'name': 'Alice',
                'active': True,
                'profile': {
                    'value': 'special',
                }
            }],
        })

    def test_graphql_functional_fragment_24(self):
        # ISSUE #5985
        #
        # Types from non-default module should be recognized in fragments.
        self.assert_graphql_query_result(r"""
            fragment myFrag on other__Foo {
                id,
                color,
            }

            query {
                other__Foo(filter: {color: {eq: RED}}) {
                    ... myFrag
                }
            }
        """, {
            'other__Foo': [{
                'id': uuid.UUID,
                'color': 'RED',
            }],
        })

        self.assert_graphql_query_result(r"""
            fragment myFrag on other__Foo_Type {
                id,
                color,
            }

            query {
                other__Foo(filter: {color: {eq: RED}}) {
                    ... myFrag
                }
            }
        """, {
            'other__Foo': [{
                'id': uuid.UUID,
                'color': 'RED',
            }],
        })

    def test_graphql_functional_fragment_25(self):
        # ISSUE #5985
        #
        # Types from non-default module should be recognized in fragments.
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(filter: {color: {eq: RED}}) {
                    ... on other__Foo {
                        id,
                        color,
                    }
                }
            }
        """, {
            'other__Foo': [{
                'id': uuid.UUID,
                'color': 'RED',
            }],
        })

        self.assert_graphql_query_result(r"""
            query {
                other__Foo(filter: {color: {eq: RED}}) {
                    ... on other__Foo_Type {
                        id,
                        color,
                    }
                }
            }
        """, {
            'other__Foo': [{
                'id': uuid.UUID,
                'color': 'RED',
            }],
        })

    def test_graphql_functional_directives_01(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_directives_02(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_directives_03(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_directives_04(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_directives_05(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_directives_06(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_directives_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'Expected type Boolean!, found "true".',
                _line=4, _col=43):
            self.graphql_query(r"""
                query {
                    User {
                        name @include(if: "true"),
                        id
                    }
                }
            """)

    def test_graphql_functional_typename_01(self):
        self.assert_graphql_query_result(r"""
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
                '__typename': 'User_Type',
                'groups': []
            }, {
                'name': 'Bob',
                '__typename': 'Person_Type',
                'groups': []
            }, {
                'name': 'Jane',
                '__typename': 'User_Type',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                    '__typename': 'UserGroup_Type',
                }]
            }, {
                'name': 'John',
                '__typename': 'User_Type',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                    '__typename': 'UserGroup_Type',
                }]
            }],
        }, sort=lambda x: x['name'])

    def test_graphql_functional_typename_02(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_typename_03(self):
        self.assert_graphql_query_result(r"""
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
                {"bar": "User_Type", "name": "Alice"},
                {"bar": "Person_Type", "name": "Bob"},
                {"bar": "User_Type", "name": "Jane"},
                {"bar": "User_Type", "name": "John"},
            ]
        })

    def test_graphql_functional_typename_04(self):
        self.assert_graphql_query_result(r"""
            query {
                other__Foo(order: {color: {dir: ASC}}) {
                    __typename
                    color
                }
            }
        """, {
            "other__Foo": [
                {"__typename": "other__Foo_Type", "color": "RED"},
                {"__typename": "other__Foo_Type", "color": "GREEN"},
                {"__typename": "other__Foo_Type", "color": "BLUE"},
            ]
        })

    def test_graphql_functional_scalars_01(self):
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest {
                    p_bool
                    p_str
                    p_datetime
                    p_local_datetime
                    p_local_date
                    p_local_time
                    p_duration
                    p_int16
                    p_int32
                    p_int64
                    p_bigint
                    p_float32
                    p_float64
                    p_decimal
                }
            }
        """, {
            "ScalarTest": [{
                'p_bool': True,
                'p_str': 'Hello world',
                'p_datetime': '2018-05-07T20:01:22.306916+00:00',
                'p_local_datetime': '2018-05-07T20:01:22.306916',
                'p_local_date': '2018-05-07',
                'p_local_time': '20:01:22.306916',
                'p_duration': 'PT20H',
                'p_int16': 12345,
                'p_int32': 1234567890,
                'p_int64': 1234567890123,
                'p_bigint': 123456789123456789123456789,
                'p_float32': 2.5,
                'p_float64': 2.5,
                'p_decimal':
                    123456789123456789123456789.123456789123456789123456789,
            }]
        })

    def test_graphql_functional_scalars_02(self):
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest {
                    p_json
                }
            }
        """, {
            "ScalarTest": [{
                'p_json': {"foo": [1, None, "bar"]},
            }]
        })

    def test_graphql_functional_scalars_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'p_bytes' on type 'ScalarTest'",
                _line=4, _col=25):
            self.graphql_query(r"""
                query {
                    ScalarTest {
                        p_bytes
                    }
                }
            """)

    def test_graphql_functional_scalars_04(self):
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest {
                    p_array_json
                }
            }
        """, {
            "ScalarTest": [{
                'p_array_json': ["hello", "world"],
            }]
        })

    def test_graphql_functional_scalars_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'p_array_bytes' on type 'ScalarTest'",
                _line=4, _col=25):
            self.graphql_query(r"""
                query {
                    ScalarTest {
                        p_array_bytes
                    }
                }
            """)

    def test_graphql_functional_scalars_06(self):
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest {
                    p_posint
                }
            }
        """, {
            "ScalarTest": [{
                'p_posint': 42,
            }]
        })

    def test_graphql_functional_scalars_07(self):
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest {
                    p_array_str
                }
            }
        """, {
            "ScalarTest": [{
                'p_array_str': ['hello', 'world'],
            }]
        })

    def test_graphql_functional_scalars_08(self):
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest {
                    p_tuple
                }
            }
        """, {
            "ScalarTest": [{
                'p_tuple': [123, "test"],
            }]
        })

    def test_graphql_functional_scalars_09(self):
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest {
                    p_array_tuple
                }
            }
        """, {
            "ScalarTest": [{
                'p_array_tuple': [["hello", True], ["world", False]],
            }]
        })

    def test_graphql_functional_range_01(self):
        self.assert_graphql_query_result(r"""
            query {
                RangeTest(order: {name: {dir: ASC}}) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "RangeTest": [
                {
                    "name": "missing boundaries",
                    "rval": {
                        "empty": True,
                    },
                    "mval": [
                        {
                            "lower": None,
                            "upper": None,
                            "inc_lower": False,
                            "inc_upper": False,
                        }
                    ],
                    "rdate": {
                        "empty": True,
                    },
                    "mdate": [
                        {
                            "lower": None,
                            "upper": None,
                            "inc_lower": False,
                            "inc_upper": False,
                        },
                    ]
                },
                {
                    "name": "test01",
                    "rval": {
                        "lower": -1.3,
                        "upper": 1.2,
                        "inc_lower": True,
                        "inc_upper": False
                    },
                    "mval": [
                        {
                            "lower": None,
                            "upper": -10,
                            "inc_lower": False,
                            "inc_upper": False
                        },
                        {
                            "lower": -1.3,
                            "upper": 1.2,
                            "inc_lower": True,
                            "inc_upper": False
                        },
                        {
                            "lower": 10,
                            "upper": None,
                            "inc_lower": True,
                            "inc_upper": False
                        }
                    ],
                    "rdate": {
                        "lower": "2018-01-23",
                        "upper": "2023-07-25",
                        "inc_lower": True,
                        "inc_upper": False
                    },
                    "mdate": [
                        {
                            "lower": "2018-01-23",
                            "upper": "2023-07-25",
                            "inc_lower": True,
                            "inc_upper": False
                        },
                        {
                            "lower": "2025-11-22",
                            "upper": None,
                            "inc_lower": True,
                            "inc_upper": False
                        }
                    ]
                }
            ]
        })

    def test_graphql_functional_duplicates_01(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_duplicates_02(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_duplicates_03(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_duplicates_04(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_duplicates_05(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_duplicates_06(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_duplicates_07(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_variables_01(self):
        query = r"""
            query($name: String) {
                User(filter: {name: {eq: $name}}) {
                    name,
                    groups {
                        name
                    }
                }
            }
        """

        expected_result = {
            'User': [{
                'name': 'John',
                'groups': [{
                    'name': 'basic',
                }]
            }],
        }

        self.assert_graphql_query_result(
            query,
            expected_result,
            variables={'name': 'John'},
            use_http_post=True
        )

        self.assert_graphql_query_result(
            query,
            expected_result,
            variables={'name': 'John'},
            use_http_post=False
        )

    def test_graphql_functional_variables_02(self):
        self.assert_graphql_query_result(
            r"""
                query($name: String, $age: Int64) {
                    User(filter: {or: [{name: {eq: $name}},
                                       {age: {gt: $age}}]},
                         order: {name: {dir: ASC}})
                    {
                        name
                        age
                    }
                }
            """,
            {
                "User": [
                    {
                        "name": "Alice",
                        "age": 27,
                    },
                    {
                        "name": "Jane",
                        "age": 25,
                    },
                    {
                        "name": "John",
                        "age": 25,
                    },
                ]
            },
            variables={
                "age": 24,
                "name": "Alice"
            }
        )

    def test_graphql_functional_variables_03(self):
        self.assert_graphql_query_result(r"""
            query($val: Int = 3) {
                User(filter: {score: {eq: $val}}) {
                    id,
                }
            }
        """, {
            'User': [],
        })

    def test_graphql_functional_variables_04(self):
        self.assert_graphql_query_result(r"""
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
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Jane"},
                {"name": "John"},
            ]
        })

    def test_graphql_functional_variables_05(self):
        self.assert_graphql_query_result(r"""
            query($val: Boolean! = true) {
                User(order: {name: {dir: ASC}}) {
                    name @include(if: $val),
                    id
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

    def test_graphql_functional_variables_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"no value for the 'val' variable",
                _line=4, _col=31):
            self.graphql_query(r"""
                query($val: Boolean!) {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_07(self):
        self.assert_graphql_query_result(r"""
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

    def test_graphql_functional_variables_08(self):
        self.assert_graphql_query_result(r"""
            query($val: Int64 = 20) {
                User(filter: {age: {eq: $val}}) {
                    name,
                }
            }
        """, {
            "User": []
        })

    def test_graphql_functional_variables_09(self):
        self.assert_graphql_query_result(r"""
            query($val: Float = 3.5) {
                User(filter: {score: {eq: $val}}) {
                    name,
                }
            }
        """, {
            "User": []
        })

    def test_graphql_functional_variables_10(self):
        self.assert_graphql_query_result(r"""
            query($val: Int = 3) {
                User(filter: {score: {eq: $val}}) {
                    id,
                }
            }
        """, {
            "User": []
        })

    def test_graphql_functional_variables_11(self):
        self.assert_graphql_query_result(r"""
            query($val: Float = 3) {
                User(filter: {score: {eq: $val}}) {
                    id,
                }
            }
        """, {
            "User": []
        })

    def test_graphql_functional_variables_12(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Boolean cannot represent a non boolean value: 1',
                _line=2, _col=39):
            self.graphql_query(r"""
                query($val: Boolean = 1) {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_13(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Boolean cannot represent a non boolean value: "1"',
                _line=2, _col=39):
            self.graphql_query(r"""
                query($val: Boolean = "1") {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_14(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Boolean cannot represent a non boolean value: 1\.3',
                _line=2, _col=39):
            self.graphql_query(r"""
                query($val: Boolean = 1.3) {
                    User {
                        name @include(if: $val),
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_15(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'String cannot represent a non string value: 1',
                _line=2, _col=38):
            self.graphql_query(r"""
                query($val: String = 1) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_16(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'String cannot represent a non string value: 1\.1',
                _line=2, _col=38):
            self.graphql_query(r"""
                query($val: String = 1.1) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'String cannot represent a non string value: true',
                _line=2, _col=38):
            self.graphql_query(r"""
                query($val: String = true) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_18(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Int cannot represent non-integer value: 1\.1',
                _line=2, _col=35):
            self.graphql_query(r"""
                query($val: Int = 1.1) {
                    User(filter: {age: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_19(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Int cannot represent non-integer value: "1"',
                _line=2, _col=35):
            self.graphql_query(r"""
                query($val: Int = "1") {
                    User(filter: {age: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_20(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Int cannot represent non-integer value: true',
                _line=2, _col=35):
            self.graphql_query(r"""
                query($val: Int = true) {
                    User(filter: {age: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_21(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Float cannot represent non numeric value: "1"',
                _line=2, _col=37):
            self.graphql_query(r"""
                query($val: Float = "1") {
                    User(filter: {score: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_22(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Float cannot represent non numeric value: true',
                _line=2, _col=37):
            self.graphql_query(r"""
                query($val: Float = true) {
                    User(filter: {score: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_23(self):
        self.assert_graphql_query_result(r"""
            query($val: ID = "00000000-3576-11e9-8723-cf18c8790091") {
                User(filter: {id: {eq: $val}}) {
                    name
                }
            }
        """, {
            "User": []
        })

    def test_graphql_functional_variables_25(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'ID cannot represent a non-string and non-integer.+: 1\.1',
                _line=2, _col=34):
            self.graphql_query(r"""
                query($val: ID = 1.1) {
                    User(filter: {id: {eq: $val}}) {
                        name
                    }
                }
            """)

    def test_graphql_functional_variables_26(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'ID cannot represent a non-string and non-integer.+: true',
                _line=2, _col=34):
            self.graphql_query(r"""
                query($val: ID = true) {
                    User(filter: {id: {eq: $val}}) {
                        name
                    }
                }
            """)

    def test_graphql_functional_variables_27(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Variable '\$val' of type '\[String\]' used in position "
                r"expecting type 'String'\."):
            self.graphql_query(r"""
                query($val: [String] = "Foo") {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_28(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Variable '\$val' of type '\[String\]' used in position "
                r"expecting type 'String'\."):
            self.graphql_query(r"""
                query($val: [String]) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_29(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Variable '\$val' of type '\[String\]!' used in position "
                r"expecting type 'String'."):
            self.graphql_query(r"""
                query($val: [String]!) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_30(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"parameter \$val is required"):
            self.graphql_query(r"""
                query($val: String!) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_31(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"String cannot represent a non string value: 123",
                _line=2, _col=48):
            self.graphql_query(r"""
                query($val: [String] = ["Foo", 123]) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_32(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Variable '\$val' of type '\[String\]' used in position "
                r"expecting type 'String'\."):
            self.graphql_query(r"""
                query($val: [String]) {
                    User(filter: {name: {eq: $val}}) {
                        id
                    }
                }
            """)

    def test_graphql_functional_variables_33(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'expected JSON string or null; got JSON number'):

            self.graphql_query(
                r"""
                    query($name: String) {
                        User(filter: {name: {eq: $name}}) {
                            name,
                            groups {
                                name
                            }
                        }
                    }
                """,
                variables={'name': 11})

    def test_graphql_functional_variables_34(self):
        # Test multiple requests to make sure that caching works correctly
        for _ in range(2):
            for _ in range(2):
                self.assert_graphql_query_result(
                    r"""
                        query($val: Boolean!, $min_age: Int64!) {
                            User(filter: {age: {gt: $min_age}}) {
                                name @include(if: $val),
                                age
                            }
                        }
                    """,
                    {'User': [{'age': 27, 'name': 'Alice'}]},
                    variables={'val': True, 'min_age': 26}
                )

            self.assert_graphql_query_result(
                r"""
                    query($val: Boolean!, $min_age: Int64!) {
                        User(filter: {age: {gt: $min_age}}) {
                            name @include(if: $val),
                            age
                        }
                    }
                """,
                {'User': [{'age': 27}]},
                variables={'val': False, 'min_age': 26}
            )

    def test_graphql_functional_variables_35(self):
        self.assert_graphql_query_result(
            r"""
                query($limit: Int!) {
                    User(
                        order: {name: {dir: ASC}},
                        first: $limit
                    ) {
                        name
                    }
                }
            """,
            {
                'User': [{
                    'name': 'Alice',
                }]
            },
            variables={'limit': 1},
        )

    def test_graphql_functional_variables_36(self):
        self.assert_graphql_query_result(
            r"""
                query($idx: String!) {
                    User(
                        order: {name: {dir: ASC}},
                        # this is actually equivalent to OFFSET 2,
                        # since 'after' doesn't include the value
                        # referenced by the index
                        after: $idx
                    ) {
                        name
                    }
                }
            """,
            {
                'User': [{
                    'name': 'Jane',
                }, {
                    'name': 'John',
                }]
            },
            variables={'idx': '1'},
        )

    def test_graphql_functional_variables_37(self):
        self.assert_graphql_query_result(
            r"""
                query($idx: String!, $num: Int!) {
                    User(
                        order: {name: {dir: ASC}},
                        # this is actually equivalent to OFFSET 2,
                        # since 'after' doesn't include the value
                        # referenced by the index
                        after: $idx,
                        first: $num
                    ) {
                        name
                    }
                }
            """,
            {
                'User': [{
                    'name': 'Jane',
                }]
            },
            variables={'idx': '1', 'num': 1},
        )

    def test_graphql_functional_variables_38(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Variable '\$limit' of type 'String!' used in position "
                r"expecting type 'Int'."):
            self.graphql_query(
                r"""
                    query($limit: String!) {
                        User(
                            order: {name: {dir: ASC}},
                            first: $limit
                        ) {
                            name
                        }
                    }
                """,
                variables={'limit': '1'},
            )

    # FIXME: the error here comes all the way from Postgres and as
    # such refers to Postgres types, ideally we'd like to have an
    # error message expressed in terms of GraphQL types.
    def test_graphql_functional_variables_39(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'expected JSON number.+got JSON string'):
            self.graphql_query(
                r"""
                    query($limit: Int!) {
                        User(
                            order: {name: {dir: ASC}},
                            first: $limit
                        ) {
                            name
                        }
                    }
                """,
                variables={'limit': '1'},
            )

    def test_graphql_functional_variables_40(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Only scalar defaults are allowed\. "
                r"Variable 'val' has non-scalar default value\."):
            self.graphql_query(r"""
                query($val: FilterFloat = {eq: 3.0}) {
                    User(filter: {score: $val}) {
                        id,
                    }
                }
            """)

    def test_graphql_functional_variables_41(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Variables starting with '_edb_arg__' are prohibited"):
            self.graphql_query(r"""
                query($_edb_arg__1: Int!) {
                    User(limit: $_edb_arg__1) {
                        id,
                    }
                }
            """, variables={'_edb_arg__1': 1})

    def test_graphql_functional_variables_42(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Variables starting with '_edb_arg__' are prohibited"):
            self.graphql_query(r"""
                query($_edb_arg__1: Int = 1) {
                    User(limit: $_edb_arg__1) {
                        id,
                    }
                }
            """)

    def test_graphql_functional_variables_43(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Only scalar input variables are allowed\. "
                r"Variable 'f' has non-scalar value\."):
            self.graphql_query(r"""
                query user($f: FilterUser!) {
                    User(filter: $f) {
                        name
                    }
                }
            """, variables={"f": {"name": {"eq": "Alice"}}})

    def test_graphql_functional_variables_44(self):
        self.assert_graphql_query_result(
            r"""
                query foo($color: other__ColorEnum!) {
                    other__Foo(
                        filter: {color: {eq: $color}},
                    ) {
                        select
                        color
                    }
                }
            """, {
                "other__Foo": [{
                    "select": "a",
                    "color": "RED",
                }]
            },
            variables={"color": "RED"},
        )

    def test_graphql_functional_variables_45(self):
        self.assert_graphql_query_result(
            r"""
                query foo($color: other__ColorEnum! = GREEN) {
                    other__Foo(
                        filter: {color: {eq: $color}},
                    ) {
                        select
                        color
                    }
                }
            """, {
                "other__Foo": [{
                    "select": "b",
                    "color": "GREEN",
                }]
            },
        )

    def test_graphql_functional_variables_46(self):
        self.assert_graphql_query_result(
            r"""
                query($val: JSON) {
                    ScalarTest(filter: {p_json: {eq: $val}}) {
                        p_json
                    }
                }
            """, {
                "ScalarTest": [{
                    'p_json': {"foo": [1, None, "bar"]},
                }]
            },
            # JSON can only be passed as a variable.
            variables={"val": {"foo": [1, None, "bar"]}},
        )

    def test_graphql_functional_variables_47(self):
        # Test boolean AND handling {} like Postgres
        self.assert_graphql_query_result(
            r"""
                query($color: other__ColorEnum!, $after: String!) {
                    other__Foo(
                        filter: {
                            not: {
                                color: {eq: $color},
                                after: {neq: $after},
                            },
                        },
                        order: {color: {dir: ASC}}
                    ) {
                        select
                        after
                        color
                    }
                }
            """, {
                "other__Foo": [{
                    "select": "a",
                    "after": None,
                    "color": "RED",
                }, {
                    "select": None,
                    "after": "q",
                    "color": "BLUE",
                }]
            },
            variables={'color': 'GREEN', 'after': 'b'},
        )

    def test_graphql_functional_variables_48(self):
        # Test boolean AND handling {} like Postgres
        self.assert_graphql_query_result(
            r"""
                query($color: other__ColorEnum!, $after: String!) {
                    other__Foo(
                        filter: {
                            not: {
                              and: [
                                {color: {eq: $color}},
                                {after: {neq: $after}},
                              ]
                            },
                        },
                        order: {color: {dir: ASC}}
                    ) {
                        select
                        after
                        color
                    }
                }
            """, {
                "other__Foo": [{
                    "select": "a",
                    "after": None,
                    "color": "RED",
                }, {
                    "select": None,
                    "after": "q",
                    "color": "BLUE",
                }]
            },
            variables={'color': 'GREEN', 'after': 'b'},
        )

    def test_graphql_functional_variables_49(self):
        # Test boolean OR handling {} like Postgres
        self.assert_graphql_query_result(
            r"""
                query($color: other__ColorEnum!, $after: String!) {
                    other__Foo(
                        filter: {
                          or: [
                            {color: {neq: $color}},
                            {after: {eq: $after}},
                          ]
                        },
                        order: {color: {dir: ASC}}
                    ) {
                        select
                        after
                        color
                    }
                }
            """, {
                "other__Foo": [{
                    "select": "a",
                    "after": None,
                    "color": "RED",
                }, {
                    "select": None,
                    "after": "q",
                    "color": "BLUE",
                }]
            },
            variables={'color': 'GREEN', 'after': 'b'},
        )

    def test_graphql_functional_inheritance_01(self):
        # ISSUE: #709
        #
        # Testing type and sub-type.
        self.assert_graphql_query_result(r"""
            query {
                Bar {
                    __typename
                    q
                }
            }
        """, {
            'Bar': [{
                '__typename': 'Bar_Type',
                'q': 'bar',
            }, {
                '__typename': 'Bar2_Type',
                'q': 'bar2',
            }],
        }, sort=lambda x: x['q'])

    def test_graphql_functional_inheritance_02(self):
        # ISSUE: #709
        #
        # Testing type and sub-type, with a covariant lint target.
        self.assert_graphql_query_result(r"""
            query {
                Rab {
                    __typename
                    blah {
                        __typename
                        q
                    }
                }
            }
        """, {
            'Rab': [{
                '__typename': 'Rab_Type',
                'blah': {
                    '__typename': 'Bar_Type',
                    'q': 'bar',
                }
            }, {
                '__typename': 'Rab2_Type',
                'blah': {
                    '__typename': 'Bar2_Type',
                    'q': 'bar2',
                }
            }],
        }, sort=lambda x: x['blah']['q'])

    def test_graphql_functional_inheritance_03(self):
        # ISSUE: #709
        #
        # Testing type and sub-type, with a covariant lint target.
        #
        # Rab2 must keep the target type of the link same as the base
        # type, due to limitations of GraphQL inheritance. But as long
        # as the actual target type is known, it can be explicitly
        # referenced.
        self.assert_graphql_query_result(r"""
            query {
                Rab2 {
                    blah {
                        __typename
                        ... on Bar2 {
                            q
                            w
                        }
                    }
                }
            }
        """, {
            'Rab2': [{
                'blah': {
                    '__typename': 'Bar2_Type',
                    'q': 'bar2',
                    'w': 'special'
                }
            }],
        })

    def test_graphql_functional_order_01(self):
        # Test order by nested objects
        self.assert_graphql_query_result(r"""
            query {
                Rab(order: {blah: {q: {dir: DESC}}}) {
                    blah {
                        q
                    }
                }
            }
        """, {
            "Rab": [
                {
                    "blah": {
                        "q": "bar2"
                    }
                },
                {
                    "blah": {
                        "q": "bar"
                    },
                }
            ]
        })

    def test_graphql_functional_order_02(self):
        # Test order by nested objects
        self.assert_graphql_query_result(r"""
            query {
                SettingAliasAugmented(
                    order: {
                        of_group: {name_upper: {dir: ASC}},
                        name: {dir: DESC}
                    }
                ) {
                    name
                    of_group {
                        name_upper
                    }
                }
            }
        """, {
            "SettingAliasAugmented": [
                {
                    "name": "template",
                    "of_group": {
                        "name_upper": "UNUSED"
                    },
                },
                {
                    "name": "template",
                    "of_group": {
                        "name_upper": "UPGRADED"
                    },
                },
                {
                    "name": "perks",
                    "of_group": {
                        "name_upper": "UPGRADED"
                    },
                },
            ]
        })

    def test_graphql_functional_order_03(self):
        # Test order by nested objects
        self.assert_graphql_query_result(r"""
            query {
                LinkedList(order: {
                    next: {next: {name: {dir: DESC, nulls: SMALLEST}}},
                    name: {dir: ASC}
                }) {
                    name
                }
            }
        """, {

            "LinkedList": [
                {
                    "name": "2nd"
                },
                {
                    "name": "1st"
                },
                {
                    "name": "3rd"
                },
                {
                    "name": "4th"
                }
            ]
        })

    def test_graphql_functional_order_04(self):
        # Test order by nested objects
        self.assert_graphql_query_result(r"""
            query {
                User(order: {
                    profile: {
                        value: {dir: ASC},
                        name: {dir: DESC}
                    }
                }) {
                    name
                    profile {
                        name
                        value
                    }
                }
            }
        """, {
            "User": [
                {
                    "name": "John",
                    "profile": None,
                },
                {
                    "name": "Jane",
                    "profile": None,
                },
                {
                    "name": "Bob",
                    "profile": {
                        "name": "Bob profile",
                        "value": "special",
                    },
                },
                {
                    "name": "Alice",
                    "profile": {
                        "name": "Alice profile",
                        "value": "special",
                    },
                }
            ]
        })

    def test_graphql_functional_exists_01(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {profile: {exists: true}},
                    order: {name: {dir: ASC}}
                ) {
                    name
                    profile {
                        name
                    }
                }
            }
        """, {
            "User": [
                {
                    "name": "Alice",
                    "profile": {
                        "name": "Alice profile",
                    },
                },
                {
                    "name": "Bob",
                    "profile": {
                        "name": "Bob profile",
                    },
                },
            ]
        })

    def test_graphql_functional_exists_02(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {profile: {exists: false}},
                    order: {name: {dir: ASC}}
                ) {
                    name
                    profile {
                        name
                    }
                }
            }
        """, {
            "User": [
                {
                    "name": "Jane",
                    "profile": None,
                },
                {
                    "name": "John",
                    "profile": None,
                },
            ]
        })

    def test_graphql_functional_exists_03(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {groups: {settings: {exists: false}}},
                    order: {name: {dir: ASC}}
                ) {
                    name
                    groups {
                        name
                        settings {
                            name
                        }
                    }
                }
            }
        """, {
            "User": [
                {
                    "name": "Alice",
                    "groups": [],
                },
                {
                    "name": "Bob",
                    "groups": [],
                },
                {
                    "name": "John",
                    "groups": [
                        {
                            "name": "basic",
                            "settings": [],
                        }
                    ],
                },
            ]
        })

    def test_graphql_functional_exists_04(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {groups: {settings: {exists: true}}}
                ) {
                    name
                    groups {
                        name
                        settings(order: {name: {dir: ASC}}) {
                            name
                        }
                    }
                }
            }
        """, {
            "User": [
                {
                    "name": "Jane",
                    "groups": [
                        {
                            "name": "upgraded",
                            "settings": [
                                {
                                    "name": "perks",
                                },
                                {
                                    "name": "template",
                                },
                            ]
                        }
                    ]
                }
            ]
        })

    def test_graphql_functional_exists_05(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {groups: {settings: {id: {exists: false}}}},
                    order: {name: {dir: ASC}}
                ) {
                    name
                    groups {
                        name
                        settings {
                            name
                        }
                    }
                }
            }
        """, {
            "User": [
                {
                    "name": "Alice",
                    "groups": [],
                },
                {
                    "name": "Bob",
                    "groups": [],
                },
                {
                    "name": "John",
                    "groups": [
                        {
                            "name": "basic",
                            "settings": [],
                        }
                    ],
                },
            ]
        })

    def test_graphql_functional_exists_06(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {groups: {settings: {id: {exists: true}}}}
                ) {
                    name
                    groups {
                        name
                        settings(order: {name: {dir: ASC}}) {
                            name
                        }
                    }
                }
            }
        """, {
            "User": [
                {
                    "name": "Jane",
                    "groups": [
                        {
                            "name": "upgraded",
                            "settings": [
                                {
                                    "name": "perks",
                                },
                                {
                                    "name": "template",
                                },
                            ]
                        }
                    ]
                }
            ]
        })

    def test_graphql_functional_in_01(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {name: {in: ["Alice", "Bob"]}},
                    order: {name: {dir: ASC}}
                ) {
                    name
                }
            }
        """, {
            "User": [
                {
                    "name": "Alice",
                },
                {
                    "name": "Bob",
                },
            ]
        })

    def test_graphql_functional_in_02(self):
        self.assert_graphql_query_result(r"""
            query {
                User(
                    filter: {name: {in: ["Zoe", "Alice"]}},
                    order: {name: {dir: ASC}}
                ) {
                    name
                }
            }
        """, {
            "User": [
                {
                    "name": "Alice",
                },
            ]
        })

    def test_graphql_functional_type_union_01(self):
        self.assert_graphql_query_result(r"""
            query {
                Combo(
                    order: {name: {dir: ASC}}
                ) {
                    name
                    data {
                        __typename
                        name
                        value
                        ... on Profile {
                            tags
                        }
                    }
                }
            }
        """, {
            "Combo": [
                {
                    "name": "combo 0",
                    "data": None,
                },
                {
                    "name": "combo 1",
                    "data": {
                        "__typename": "Setting_Type",
                        "name": "template",
                        "value": "blue",
                    },
                },
                {
                    "name": "combo 2",
                    "data": {
                        "__typename": "Profile_Type",
                        "name": "Alice profile",
                        "value": "special",
                        "tags": ['1st', '2nd'],
                    },
                },
            ]
        })

    def test_graphql_functional_edgedb_errors_01(self):
        with self.assertRaisesRegex(
            edgedb.DivisionByZeroError,
            r"division by zero"
        ):
            self.graphql_query(r"""
                query {
                    ErrorTest {
                        div_by_val
                    }
                }
            """)

    def test_graphql_functional_edgedb_errors_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r"LIMIT must not be negative"
        ):
            self.graphql_query(r"""
                query {
                    ErrorTest(first: -2) {
                        text
                    }
                }
            """)

    def test_graphql_functional_edgedb_errors_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r"OFFSET must not be negative"
        ):
            self.graphql_query(r"""
                query {
                    ErrorTest(after: "-2") {
                        text
                    }
                }
            """)

    def test_graphql_functional_edgedb_errors_04(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r"invalid regular expression"
        ):
            self.graphql_query(r"""
                query {
                    ErrorTest {
                        re_text
                    }
                }
            """)

    def test_graphql_globals_01(self):
        Q = r'''query { GlobalTest { gstr, garray, gid, gdef, gdef2 } }'''

        for use_http_post in [True, False]:
            self.assert_graphql_query_result(
                Q,
                {
                    "GlobalTest": [{
                        'gstr': 'WOO',
                        'gid': '84ed3d8b-5eb2-4d31-9e1e-efb66180445c',
                        'gdef': '',
                        'gdef2': None, 'garray': ['x', 'y', 'z']
                    }],
                },
                use_http_post=use_http_post,
                deprecated_globals={
                    'default::test_global_str': "WOO",
                    'default::test_global_id': (
                        '84ed3d8b-5eb2-4d31-9e1e-efb66180445c'),
                    'default::test_global_def': None,
                    'default::test_global_def2': None,
                    'default::test_global_array': ['x', 'y', 'z'],
                },
            )

            self.assert_graphql_query_result(
                Q,
                {'GlobalTest': [{'gdef': 'x', 'gdef2': 'x'}]},
                use_http_post=use_http_post,
                deprecated_globals={
                    'default::test_global_def': 'x',
                    'default::test_global_def2': 'x',
                },
            )

            self.assert_graphql_query_result(
                Q,
                {'GlobalTest': [
                    {'gstr': None, 'garray': None, 'gid': None,
                     'gdef': '', 'gdef2': ''}
                ]},
                use_http_post=use_http_post,
            )

    def test_graphql_globals_02(self):
        Q = r'''query { GlobalTest { gstr, garray, gid, gdef, gdef2 } }'''

        for use_http_post in [True, False]:
            self.assert_graphql_query_result(
                Q,
                {
                    "GlobalTest": [{
                        'gstr': 'WOO',
                        'gid': '84ed3d8b-5eb2-4d31-9e1e-efb66180445c',
                        'gdef': '',
                        'gdef2': None, 'garray': ['x', 'y', 'z']
                    }],
                },
                use_http_post=use_http_post,
                globals={
                    'default::test_global_str': "WOO",
                    'default::test_global_id': (
                        '84ed3d8b-5eb2-4d31-9e1e-efb66180445c'),
                    'default::test_global_def': None,
                    'default::test_global_def2': None,
                    'default::test_global_array': ['x', 'y', 'z'],
                },
            )

            self.assert_graphql_query_result(
                Q,
                {'GlobalTest': [{'gdef': 'x', 'gdef2': 'x'}]},
                use_http_post=use_http_post,
                globals={
                    'default::test_global_def': 'x',
                    'default::test_global_def2': 'x',
                },
            )

            self.assert_graphql_query_result(
                Q,
                {'GlobalTest': [
                    {'gstr': None, 'garray': None, 'gid': None,
                     'gdef': '', 'gdef2': ''}
                ]},
                use_http_post=use_http_post,
            )

    def test_graphql_globals_03(self):
        Q = r'''query { GlobalTest { gstr, garray, gid, gdef, gdef2 } }'''

        # Test that globals work fine if both the deprecated way and the new
        # way are used and the values match.
        for use_http_post in [True, False]:
            self.assert_graphql_query_result(
                Q,
                {'GlobalTest': [{'gdef': 'x', 'gdef2': 'x'}]},
                use_http_post=use_http_post,
                globals={
                    'default::test_global_def': 'x',
                    'default::test_global_def2': 'x',
                },
                deprecated_globals={
                    'default::test_global_def': 'x',
                    'default::test_global_def2': 'x',
                },
            )

    def test_graphql_globals_04(self):
        Q = r'''query { GlobalTest { gstr, garray, gid, gdef, gdef2 } }'''

        # Test that globals must match if two ways are used to specify them.
        for use_http_post in [True, False]:
            with self.assertRaises(
                urllib.error.HTTPError,
            ):
                self.assert_graphql_query_result(
                    Q,
                    {'GlobalTest': [{'gdef': 'x', 'gdef2': 'x'}]},
                    use_http_post=use_http_post,
                    globals={
                        'default::test_global_def': 'x',
                        'default::test_global_def2': 'x',
                    },
                    deprecated_globals={
                        'default::test_global_def': 'not x',
                        'default::test_global_def2': 'not x',
                    },
                )

    def test_graphql_globals_05(self):
        Q = r'''query { GlobalTest { gstr, garray, gid, gdef, gdef2 } }'''

        # Test that globals must match if two ways are used to specify them,
        # even if one of these is an empty dict.
        for use_http_post in [True, False]:
            with self.assertRaises(
                urllib.error.HTTPError,
            ):
                self.assert_graphql_query_result(
                    Q,
                    {'GlobalTest': [{'gdef': 'x', 'gdef2': 'x'}]},
                    use_http_post=use_http_post,
                    globals={
                        'default::test_global_def': 'x',
                        'default::test_global_def2': 'x',
                    },
                    deprecated_globals={},
                )

        for use_http_post in [True, False]:
            with self.assertRaises(
                urllib.error.HTTPError,
            ):
                self.assert_graphql_query_result(
                    Q,
                    {'GlobalTest': [{'gdef': 'x', 'gdef2': 'x'}]},
                    use_http_post=use_http_post,
                    globals={},
                    deprecated_globals={
                        'default::test_global_def': 'x',
                        'default::test_global_def2': 'x',
                    },
                )

    def test_graphql_func_01(self):
        Q = r'''query { FuncTest { fstr } }'''

        for use_http_post in [True, False]:
            self.assert_graphql_query_result(
                Q,
                {
                    "FuncTest": [{
                        'fstr': 'test',
                    }]
                },
                use_http_post=use_http_post,
            )

    async def test_graphql_cors(self):
        req = urllib.request.Request(self.http_addr, method='OPTIONS')
        req.add_header('Origin', 'https://example.edgedb.com')
        response = urllib.request.urlopen(
            req, context=self.tls_context
        )

        self.assertNotIn('Access-Control-Allow-Origin', response.headers)

        await self.con.execute(
            'configure current database '
            'set cors_allow_origins := {"https://example.edgedb.com"}')
        await self._wait_for_db_config('cors_allow_origins')

        req = urllib.request.Request(self.http_addr, method='OPTIONS')
        req.add_header('Origin', 'https://example.edgedb.com')
        response = urllib.request.urlopen(
            req, context=self.tls_context
        )

        headers = response.headers

        self.assertIn('Access-Control-Allow-Origin', headers)
        self.assertEqual(
            headers['Access-Control-Allow-Origin'],
            'https://example.edgedb.com'
        )
        self.assertIn('POST', headers['Access-Control-Allow-Methods'])
        self.assertIn('GET', headers['Access-Control-Allow-Methods'])
        self.assertIn('Authorization', headers['Access-Control-Allow-Headers'])
        self.assertIn('X-EdgeDB-User', headers['Access-Control-Allow-Headers'])
        self.assertEqual(headers['Access-Control-Allow-Credentials'], 'true')


class TestGraphQLInit(tb.GraphQLTestCase):
    """Test GraphQL initialization on an empty database."""

    # GraphQL queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    def test_graphql_init_type_01(self):
        # An empty database should still have an "Object" interface.
        self.assert_graphql_query_result(r"""
            query {
                __type(name: "Object") {
                    __typename
                    name
                    kind
                }
            }
        """, {
            "__type": {
                "kind": "INTERFACE",
                "name": "Object",
                "__typename": "__Type"
            }
        })
