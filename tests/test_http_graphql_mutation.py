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
import unittest  # NOQA

import edgedb

from edb.testbase import http as tb


class TestGraphQLMutation(tb.GraphQLTestCase):
    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'graphql.esdl')

    SCHEMA_OTHER = os.path.join(os.path.dirname(__file__), 'schemas',
                                'graphql_other.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'graphql_setup.edgeql')

    # GraphQL queries cannot run in a transaction
    ISOLATED_METHODS = False
    SERIALIZED = True

    def test_graphql_mutation_insert_scalars_01(self):
        data = {
            'p_bool': False,
            'p_str': 'New ScalarTest01',
            'p_datetime': '2019-05-01T01:02:35.196811+00:00',
            'p_local_datetime': '2019-05-01T01:02:35.196811',
            'p_local_date': '2019-05-01',
            'p_local_time': '01:02:35.196811',
            'p_duration': '21:30:00',
            'p_int16': 12345,
            'p_int32': 1234567890,
            # Some GraphQL implementations seem to have a limitation
            # of not being able to handle 64-bit integer literals
            # (GraphiQL is among them).
            'p_int64': 1234567890,
            'p_bigint': 123456789123456789123456789,
            'p_float32': 4.5,
            'p_float64': 4.5,
            'p_decimal':
                123456789123456789123456789.123456789123456789123456789,
        }

        validation_query = r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest01"}}) {
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
        """

        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_bool: false,
                        p_str: "New ScalarTest01",
                        p_datetime: "2019-05-01T01:02:35.196811+00:00",
                        p_local_datetime: "2019-05-01T01:02:35.196811",
                        p_local_date: "2019-05-01",
                        p_local_time: "01:02:35.196811",
                        p_duration: "21:30:00",
                        p_int16: 12345,
                        p_int32: 1234567890,
                        p_int64: 1234567890,
                        p_bigint: 123456789123456789123456789,
                        p_float32: 4.5,
                        p_float64: 4.5,
                        p_decimal:
                123456789123456789123456789.123456789123456789123456789,
                    }]
                ) {
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
            "insert_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(filter: {p_str: {eq: "New ScalarTest01"}}) {
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
            "delete_ScalarTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": []
        })

    def test_graphql_mutation_insert_scalars_02(self):
        # This tests int64 insertion. Apparently as long as the number
        # is provided as a variable parameter in JSON, there's no
        # limit on the number of digits of an Int.
        data = {
            'p_str': 'New ScalarTest02',
            'p_int64': 1234567890123,
        }

        validation_query = r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest02"}}) {
                    p_str
                    p_int64
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest($num: Int64!) {
                insert_ScalarTest(
                    data: [{
                        p_str: "New ScalarTest02",
                        p_int64: $num,
                    }]
                ) {
                    p_str
                    p_int64
                }
            }
        """, {
            "insert_ScalarTest": [data]
        }, variables={'num': data['p_int64']})

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(filter: {p_str: {eq: "New ScalarTest02"}}) {
                    p_str
                    p_int64
                }
            }
        """, {
            "delete_ScalarTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": []
        })

    def test_graphql_mutation_insert_scalars_03(self):
        # This tests custom scalar insertion.
        data = {
            'p_str': 'New ScalarTest03',
            'p_posint': 42,
        }

        validation_query = r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest03"}}) {
                    p_str
                    p_posint
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_str: "New ScalarTest03",
                        p_posint: 42,
                    }]
                ) {
                    p_str
                    p_posint
                }
            }
        """, {
            "insert_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(filter: {p_str: {eq: "New ScalarTest03"}}) {
                    p_str
                    p_posint
                }
            }
        """, {
            "delete_ScalarTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": []
        })

    def test_graphql_mutation_insert_scalars_04(self):
        # This tests JSON insertion.
        data = {
            'p_str': 'New ScalarTest04',
            'p_json': '{"foo": [1, null, "aardvark"]}',
        }

        validation_query = r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest04"}}) {
                    p_str
                    p_json
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_str: "New ScalarTest04",
                        p_json: "{\"foo\": [1, null, \"aardvark\"]}",
                    }]
                ) {
                    p_str
                    p_json
                }
            }
        """, {
            "insert_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(filter: {p_str: {eq: "New ScalarTest04"}}) {
                    p_str
                    p_json
                }
            }
        """, {
            "delete_ScalarTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": []
        })

    def test_graphql_mutation_insert_scalars_05(self):
        # This tests string escapes.
        data = {
            'p_str': 'New \"ScalarTest05\"\\',
        }

        validation_query = r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New \"ScalarTest05\"\\"}}) {
                    p_str
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_str: "New \"ScalarTest05\"\\",
                    }]
                ) {
                    p_str
                }
            }
        """, {
            "insert_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(
                    filter: {p_str: {eq: "New \"ScalarTest05\"\\"}}
                ) {
                    p_str
                }
            }
        """, {
            "delete_ScalarTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": []
        })

    def test_graphql_mutation_insert_scalars_06(self):
        # This tests float vs. decimal literals.
        data = {
            'p_str': 'New ScalarTest06',
            'p_decimal':
                123456789123456789123456789.123456789123456789123456789,
            'p_decimal_str':
                '123456789123456789123456789.123456789123456789123456789',
        }

        validation_query = r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest06"}}) {
                    p_str
                    p_decimal
                    p_decimal_str
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_str: "New ScalarTest06",
                        p_decimal:
                    123456789123456789123456789.123456789123456789123456789,
                    }]
                ) {
                    p_str
                    p_decimal
                    p_decimal_str
                }
            }
        """, {
            "insert_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(
                    filter: {p_str: {eq: "New ScalarTest06"}}
                ) {
                    p_str
                    p_decimal
                    p_decimal_str
                }
            }
        """, {
            "delete_ScalarTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": []
        })

    def test_graphql_mutation_insert_enum_01(self):
        # This tests enum values in insertion.
        data = {
            'select': 'New EnumTest01',
            'color': 'GREEN',
        }

        validation_query = r"""
            query {
                other__Foo(filter: {select: {eq: "New EnumTest01"}}) {
                    select
                    color
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        select: "New EnumTest01",
                        color: GREEN,
                    }]
                ) {
                    select
                    color
                }
            }
        """, {
            "insert_other__Foo": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "other__Foo": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {select: {eq: "New EnumTest01"}}
                ) {
                    select
                    color
                }
            }
        """, {
            "delete_other__Foo": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "other__Foo": []
        })

    def test_graphql_mutation_insert_nested_01(self):
        # Test nested insert.
        data = {
            'name': 'New UserGroup01',
            'settings': [{
                'name': 'setting01',
                'value': 'aardvark01',
            }],
        }

        validation_query = r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup01"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_UserGroup {
                insert_UserGroup(
                    data: [{
                        name: "New UserGroup01",
                        settings: [{
                            data: {
                                name: "setting01",
                                value: "aardvark01"
                            }
                        }],
                    }]
                ) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "insert_UserGroup": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "UserGroup": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_UserGroup {
                delete_UserGroup(filter: {name: {eq: "New UserGroup01"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "delete_UserGroup": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "UserGroup": []
        })

    def test_graphql_mutation_insert_nested_02(self):
        # Test insert with nested existing object.
        data = {
            'name': 'New UserGroup02',
            'settings': [{
                'name': 'setting02',
                'value': 'aardvark02'
            }],
        }

        validation_query = r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup02"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """

        setting = self.graphql_query(r"""
            mutation insert_Setting {
                insert_Setting(data: [{
                    name: "setting02",
                    value: "aardvark02"
                }]) {
                    id
                    name
                    value
                }
            }
        """)['insert_Setting'][0]

        self.assert_graphql_query_result(rf"""
            mutation insert_UserGroup {{
                insert_UserGroup(
                    data: [{{
                        name: "New UserGroup02",
                        settings: [{{
                            filter: {{
                                id: {{eq: "{setting['id']}"}}
                            }}
                        }}],
                    }}]
                ) {{
                    name
                    settings {{
                        name
                        value
                    }}
                }}
            }}
        """, {
            "insert_UserGroup": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "UserGroup": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_UserGroup {
                delete_UserGroup(filter: {name: {eq: "New UserGroup02"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "delete_UserGroup": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "UserGroup": []
        })

    def test_graphql_mutation_insert_nested_03(self):
        # Test insert with nested existing object.
        data = {
            'name': 'New UserGroup03',
            'settings': [{
                'name': 'setting031',
                'value': 'aardvark03',
            }, {
                'name': 'setting032',
                'value': 'other03',
            }, {
                'name': 'setting033',
                'value': 'special03',
            }],
        }

        validation_query = r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup03"}}) {
                    name
                    settings(order: {name: {dir: ASC}}) {
                        name
                        value
                    }
                }
            }
        """

        settings = self.graphql_query(r"""
            mutation insert_Setting {
                insert_Setting(data: [{
                    name: "setting031",
                    value: "aardvark03"
                }, {
                    name: "setting032",
                    value: "other03"
                }]) {
                    id
                    name
                    value
                }
            }
        """)['insert_Setting']

        # nested results aren't fetching correctly
        self.assert_graphql_query_result(rf"""
            mutation insert_UserGroup {{
                insert_UserGroup(
                    data: [{{
                        name: "New UserGroup03",
                        settings: [{{
                            filter: {{
                                id: {{eq: "{settings[0]['id']}"}}
                            }}
                        }}, {{
                            data: {{
                                name: "setting033",
                                value: "special03",
                            }}
                        }}, {{
                            filter: {{
                                name: {{eq: "{settings[1]['name']}"}}
                            }}
                        }}],
                    }}]
                ) {{
                    name
                    settings(order: {{name: {{dir: ASC}}}}) {{
                        name
                        value
                    }}
                }}
            }}
        """, {
            "insert_UserGroup": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "UserGroup": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_UserGroup {
                delete_UserGroup(filter: {name: {eq: "New UserGroup03"}}) {
                    name
                    settings(order: {name: {dir: ASC}}) {
                        name
                        value
                    }
                }
            }
        """, {
            "delete_UserGroup": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "UserGroup": []
        })

    def test_graphql_mutation_insert_nested_04(self):
        # Test nested insert for a singular link.
        data = {
            "name": "New User04",
            "age": 99,
            "score": 99.99,
            "profile": {
                "name": "Alice profile",
                "value": "special"
            }
        }

        validation_query = r"""
            query {
                User(filter: {name: {eq: "New User04"}}) {
                    name
                    age
                    score
                    profile {
                        name
                        value
                    }
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_User {
                insert_User(
                    data: [{
                        name: "New User04",
                        active: false,
                        age: 99,
                        score: 99.99,
                        profile: {
                            filter: {
                                name: {eq: "Alice profile"}
                            },
                            first: 1
                        },
                    }]
                ) {
                    name
                    age
                    score
                    profile {
                        name
                        value
                    }
                }
            }
        """, {
            "insert_User": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_User {
                delete_User(filter: {name: {eq: "New User04"}}) {
                    name
                    age
                    score
                    profile {
                        name
                        value
                    }
                }
            }
        """, {
            "delete_User": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "User": []
        })

    def test_graphql_mutation_insert_nested_05(self):
        # Test nested insert for a singular link.
        profile = self.graphql_query(r"""
            query {
                Profile(filter: {
                    name: {eq: "Alice profile"}
                }) {
                    id
                    name
                    value
                }
            }
        """)['Profile'][0]

        data = {
            "name": "New User05",
            "age": 99,
            "score": 99.99,
            "profile": profile
        }

        validation_query = r"""
            query {
                User(filter: {name: {eq: "New User05"}}) {
                    name
                    age
                    score
                    profile {
                        id
                        name
                        value
                    }
                }
            }
        """

        self.assert_graphql_query_result(rf"""
            mutation insert_User {{
                insert_User(
                    data: [{{
                        name: "New User05",
                        active: false,
                        age: 99,
                        score: 99.99,
                        profile: {{
                            filter: {{
                                id: {{eq: "{profile['id']}"}}
                            }},
                            first: 1
                        }},
                    }}]
                ) {{
                    name
                    age
                    score
                    profile {{
                        id
                        name
                        value
                    }}
                }}
            }}
        """, {
            "insert_User": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_User {
                delete_User(filter: {name: {eq: "New User05"}}) {
                    name
                    age
                    score
                    profile {
                        id
                        name
                        value
                    }
                }
            }
        """, {
            "delete_User": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "User": []
        })

    def test_graphql_mutation_insert_nested_06(self):
        # Test delete based on nested field.
        data = {
            'name': 'New UserGroup06',
            'settings': [{
                'name': 'setting06',
                'value': 'aardvark06',
            }],
        }

        validation_query = r"""
            query {
                UserGroup(
                    filter: {settings: {name: {eq: "setting06"}}}
                ) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_UserGroup {
                insert_UserGroup(
                    data: [{
                        name: "New UserGroup06",
                        settings: [{
                            data: {
                                name: "setting06",
                                value: "aardvark06"
                            }
                        }],
                    }]
                ) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "insert_UserGroup": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "UserGroup": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_UserGroup {
                delete_UserGroup(
                    filter: {settings: {name: {eq: "setting06"}}}
                ) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "delete_UserGroup": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "UserGroup": []
        })

    def test_graphql_mutation_insert_nested_07(self):
        # Test insert with nested object filter.
        data = {
            "name": "New User07",
            "age": 33,
            "score": 33.33,
            "groups": [{
                'name': 'New UserGroup07',
                'settings': [{
                    'name': 'setting07',
                    'value': 'aardvark07',
                }],
            }]
        }

        validation_query = r"""
            query {
                User(
                    filter: {groups: {settings: {name: {eq: "setting07"}}}}
                ) {
                    name
                    age
                    score
                    groups {
                        name
                        settings {
                            name
                            value
                        }
                    }
                }
            }
        """

        # insert the user groups first
        self.assert_graphql_query_result(r"""
            mutation insert_UserGroup {
                insert_UserGroup(
                    data: [{
                        name: "New UserGroup07",
                        settings: [{
                            data: {
                                name: "setting07",
                                value: "aardvark07"
                            }
                        }],
                    }]
                ) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "insert_UserGroup": [data['groups'][0]]
        })

        # insert the User
        self.assert_graphql_query_result(r"""
            mutation insert_User {
                insert_User(
                    data: [{
                        name: "New User07",
                        active: true,
                        age: 33,
                        score: 33.33,
                        groups: {
                            filter: {
                                settings: {name: {eq: "setting07"}}
                            },
                            first: 1
                        },
                    }]
                ) {
                    name
                    age
                    score
                    groups {
                        name
                        settings {
                            name
                            value
                        }
                    }
                }
            }
        """, {
            "insert_User": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_User {
                delete_User(
                    filter: {groups: {settings: {name: {eq: "setting07"}}}}
                ) {
                    name
                    age
                    score
                    groups {
                        name
                        settings {
                            name
                            value
                        }
                    }
                }
            }
        """, {
            "delete_User": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "User": []
        })

        # cleanup
        self.assert_graphql_query_result(r"""
            mutation delete_UserGroup {
                delete_UserGroup(
                    filter: {settings: {name: {eq: "setting07"}}}
                ) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "delete_UserGroup": data['groups']
        })

    def test_graphql_mutation_insert_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Cannot query field "insert_SettingAlias"'):
            self.graphql_query(r"""
                mutation insert_SettingAlias {
                    insert_SettingAlias(
                        data: [{
                            name: "badsetting01",
                            value: "red"
                        }]
                    ) {
                        name
                        value
                    }
                }
            """)

    def test_graphql_mutation_insert_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Argument "data" has invalid value(.|\n)*'
                r'In field "favorites": .*In field "data": Unknown field'):
            self.graphql_query(r"""
                mutation insert_User {
                    insert_User(
                        data: [{
                            name: "Bad User02",
                            active: true,
                            age: 33,
                            score: 33.33,
                            favorites: [{
                                data: {
                                    name: "badsetting02",
                                }
                            }],
                        }]
                    ) {
                        name
                        age
                        score
                        favorites {
                            name
                            value
                        }
                    }
                }
            """)

    def test_graphql_mutation_update_scalars_01(self):
        orig_data = {
            'p_bool': True,
            'p_str': 'Hello',
            'p_datetime': '2018-05-07T20:01:22.306916+00:00',
            'p_local_datetime': '2018-05-07T20:01:22.306916',
            'p_local_date': '2018-05-07',
            'p_local_time': '20:01:22.306916',
            'p_duration': '20:00:00',
            'p_int16': 12345,
            'p_int32': 1234567890,
            'p_int64': 1234567890123,
            'p_bigint': 123456789123456789123456789,
            'p_float32': 2.5,
            'p_float64': 2.5,
            'p_decimal':
                123456789123456789123456789.123456789123456789123456789,
        }
        data = {
            'p_bool': False,
            'p_str': 'Update ScalarTest01',
            'p_datetime': '2019-05-01T01:02:35.196811+00:00',
            'p_local_datetime': '2019-05-01T01:02:35.196811',
            'p_local_date': '2019-05-01',
            'p_local_time': '01:02:35.196811',
            'p_duration': '21:30:00',
            'p_int16': 4321,
            'p_int32': 876543210,
            # Some GraphQL implementations seem to have a limitation
            # of not being able to handle 64-bit integer literals
            # (GraphiQL is among them).
            'p_int64': 876543210,
            'p_bigint': 333333333333333333333333333,
            'p_float32': 4.5,
            'p_float64': 4.5,
            'p_decimal':
                444444444444444444444444444.222222222222222222222222222,
        }

        validation_query = rf"""
            query {{
                ScalarTest(
                    filter: {{
                        or: [{{
                            p_str: {{eq: "{orig_data['p_str']}"}}
                        }}, {{
                            p_str: {{eq: "{data['p_str']}"}}
                        }}]
                    }}
                ) {{
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
                }}
            }}
        """

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [orig_data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    data: {
                        p_bool: {set: false},
                        p_str: {set: "Update ScalarTest01"},
                        p_datetime: {set: "2019-05-01T01:02:35.196811+00:00"},
                        p_local_datetime: {set: "2019-05-01T01:02:35.196811"},
                        p_local_date: {set: "2019-05-01"},
                        p_local_time: {set: "01:02:35.196811"},
                        p_duration: {set: "21:30:00"},
                        p_int16: {set: 4321},
                        p_int32: {set: 876543210},
                        # Some GraphQL implementations seem to have a
                        # limitation of not being able to handle 64-bit
                        # integer literals (GraphiQL is among them).
                        p_int64: {set: 876543210},
                        p_bigint: {set: 333333333333333333333333333},
                        p_float32: {set: 4.5},
                        p_float64: {set: 4.5},
                        p_decimal: {set:
                    444444444444444444444444444.222222222222222222222222222},
                    }
                ) {
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
            "update_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest(
                $p_bool: Boolean,
                $p_str: String,
                $p_datetime: String,
                $p_local_datetime: String,
                $p_local_date: String,
                $p_local_time: String,
                $p_duration: String,
                $p_int16: Int,
                $p_int32: Int,
                $p_int64: Int64,
                $p_bigint: Bigint,
                $p_float32: Float,
                $p_float64: Float,
                $p_decimal: Decimal,
            ) {
                update_ScalarTest(
                    data: {
                        p_bool: {set: $p_bool},
                        p_str: {set: $p_str},
                        p_datetime: {set: $p_datetime},
                        p_local_datetime: {set: $p_local_datetime},
                        p_local_date: {set: $p_local_date},
                        p_local_time: {set: $p_local_time},
                        p_duration: {set: $p_duration},
                        p_int16: {set: $p_int16},
                        p_int32: {set: $p_int32},
                        p_int64: {set: $p_int64},
                        p_bigint: {set: $p_bigint},
                        p_float32: {set: $p_float32},
                        p_float64: {set: $p_float64},
                        p_decimal: {set: $p_decimal},
                    }
                ) {
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
            "update_ScalarTest": [orig_data]
        }, variables=orig_data)

        # validate that the final update worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [orig_data]
        })

    def test_graphql_mutation_update_scalars_02(self):
        orig_data = {
            'p_str': 'Hello',
            'p_posint': 42,
        }
        data = {
            'p_str': 'Update ScalarTest02',
            'p_posint': 9999,
        }

        validation_query = rf"""
            query {{
                ScalarTest(
                    filter: {{
                        or: [{{
                            p_str: {{eq: "{orig_data['p_str']}"}}
                        }}, {{
                            p_str: {{eq: "{data['p_str']}"}}
                        }}]
                    }}
                ) {{
                    p_str
                    p_posint
                }}
            }}
        """

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [orig_data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    data: {
                        p_str: {set: "Update ScalarTest02"},
                        p_posint: {set: 9999},
                    }
                ) {
                    p_str
                    p_posint
                }
            }
        """, {
            "update_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest(
                $p_str: String,
                $p_posint: Int64,
            ) {
                update_ScalarTest(
                    data: {
                        p_str: {set: $p_str},
                        p_posint: {set: $p_posint},
                    }
                ) {
                    p_str
                    p_posint
                }
            }
        """, {
            "update_ScalarTest": [orig_data]
        }, variables=orig_data)

        # validate that the final update worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [orig_data]
        })

    def test_graphql_mutation_update_scalars_03(self):
        orig_data = {
            'p_str': 'Hello',
            'p_json': '{"foo": [1, null, "bar"]}',
        }
        data = {
            'p_str': 'Update ScalarTest03',
            'p_json': '{"bar": [null, 2, "aardvark"]}',
        }

        validation_query = rf"""
            query {{
                ScalarTest(
                    filter: {{
                        or: [{{
                            p_str: {{eq: "{orig_data['p_str']}"}}
                        }}, {{
                            p_str: {{eq: "{data['p_str']}"}}
                        }}]
                    }}
                ) {{
                    p_str
                    p_json
                }}
            }}
        """

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [orig_data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    data: {
                        p_str: {set: "Update ScalarTest03"},
                        p_json: {set: "{\"bar\": [null, 2, \"aardvark\"]}"},
                    }
                ) {
                    p_str
                    p_json
                }
            }
        """, {
            "update_ScalarTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest(
                $p_str: String,
                $p_json: String,
            ) {
                update_ScalarTest(
                    data: {
                        p_str: {set: $p_str},
                        p_json: {set: $p_json},
                    }
                ) {
                    p_str
                    p_json
                }
            }
        """, {
            "update_ScalarTest": [orig_data]
        }, variables=orig_data)

        # validate that the final update worked
        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [orig_data]
        })

    def test_graphql_mutation_update_scalars_04(self):
        # This tests update ops for various numerical types.
        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_str: "Update ScalarTest04",
                        p_int16: 0,
                        p_int32: 0,
                        p_int64: 0,
                        p_bigint: 0,
                        p_float32: 0,
                        p_float64: 0,
                        p_decimal: 0,
                    }]
                ) {
                    p_str
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
            "insert_ScalarTest": [{
                'p_str': 'Update ScalarTest04',
                'p_int16': 0,
                'p_int32': 0,
                'p_int64': 0,
                'p_bigint': 0,
                'p_float32': 0,
                'p_float64': 0,
                'p_decimal': 0,
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest04"}}
                    data: {
                        p_int16: {increment: 2},
                        p_int32: {increment: 2},
                        p_int64: {increment: 2},
                        p_bigint: {increment: 2},
                        p_float32: {increment: 1.5},
                        p_float64: {increment: 1.5},
                        p_decimal: {increment: 1.5},
                    }
                ) {
                    p_str
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
            "update_ScalarTest": [{
                'p_str': 'Update ScalarTest04',
                'p_int16': 2,
                'p_int32': 2,
                'p_int64': 2,
                'p_bigint': 2,
                'p_float32': 1.5,
                'p_float64': 1.5,
                'p_decimal': 1.5,
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest04"}}
                    data: {
                        p_int16: {decrement: 1},
                        p_int32: {decrement: 1},
                        p_int64: {decrement: 1},
                        p_bigint: {decrement: 1},
                        p_float32: {decrement: 0.4},
                        p_float64: {decrement: 0.4},
                        p_decimal: {decrement: 0.4},
                    }
                ) {
                    p_str
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
            "update_ScalarTest": [{
                'p_str': 'Update ScalarTest04',
                'p_int16': 1,
                'p_int32': 1,
                'p_int64': 1,
                'p_bigint': 1,
                'p_float32': 1.1,
                'p_float64': 1.1,
                'p_decimal': 1.1,
            }]
        })

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest04"}}
                ) {
                    p_str
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
            "delete_ScalarTest": [{
                'p_str': 'Update ScalarTest04',
                'p_int16': 1,
                'p_int32': 1,
                'p_int64': 1,
                'p_bigint': 1,
                'p_float32': 1.1,
                'p_float64': 1.1,
                'p_decimal': 1.1,
            }]
        })

    def test_graphql_mutation_update_scalars_05(self):
        # This tests update ops for various numerical types.
        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_str: "Update ScalarTest05",
                    }]
                ) {
                    p_str
                }
            }
        """, {
            "insert_ScalarTest": [{
                'p_str': 'Update ScalarTest05',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {like: "%Update ScalarTest05%"}}
                    data: {
                        p_str: {prepend: "--"},
                    }
                ) {
                    p_str
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': '--Update ScalarTest05',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {like: "%Update ScalarTest05%"}}
                    data: {
                        p_str: {append: "!!!"},
                    }
                ) {
                    p_str
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': '--Update ScalarTest05!!!',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {like: "%Update ScalarTest05%"}}
                    data: {
                        p_str: {slice: [1]},
                    }
                ) {
                    p_str
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': '-Update ScalarTest05!!!',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {like: "%Update ScalarTest05%"}}
                    data: {
                        p_str: {slice: [0, -1]},
                    }
                ) {
                    p_str
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': '-Update ScalarTest05!!',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {like: "%Update ScalarTest05%"}}
                    data: {
                        p_str: {slice: [1, -2]},
                    }
                ) {
                    p_str
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': 'Update ScalarTest05',
            }]
        })

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(
                    filter: {p_str: {like: "%Update ScalarTest05%"}}
                ) {
                    p_str
                }
            }
        """, {
            "delete_ScalarTest": [{
                'p_str': 'Update ScalarTest05',
            }]
        })

    def test_graphql_mutation_update_scalars_06(self):
        # This tests update ops for various numerical types.
        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest {
                insert_ScalarTest(
                    data: [{
                        p_str: "Update ScalarTest06",
                        p_array_str: ["world"],
                        p_array_int64: [0],
                    }]
                ) {
                    p_str
                    p_array_str
                    p_array_int64
                }
            }
        """, {
            "insert_ScalarTest": [{
                'p_str': 'Update ScalarTest06',
                'p_array_str': ['world'],
                'p_array_int64': [0],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest06"}}
                    data: {
                        p_array_str: {prepend: ["Hello"]},
                        p_array_int64: {prepend: [1, 2]},
                    }
                ) {
                    p_str
                    p_array_str
                    p_array_int64
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': 'Update ScalarTest06',
                'p_array_str': ['Hello', 'world'],
                'p_array_int64': [1, 2, 0],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest06"}}
                    data: {
                        p_array_str: {append: ["!"]},
                        p_array_int64: {append: [3, 4]},
                    }
                ) {
                    p_str
                    p_array_str
                    p_array_int64
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': 'Update ScalarTest06',
                'p_array_str': ['Hello', 'world', '!'],
                'p_array_int64': [1, 2, 0, 3, 4],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest06"}}
                    data: {
                        p_array_str: {slice: [1]},
                        p_array_int64: {slice: [1]},
                    }
                ) {
                    p_str
                    p_array_str
                    p_array_int64
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': 'Update ScalarTest06',
                'p_array_str': ['world', '!'],
                'p_array_int64': [2, 0, 3, 4],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_ScalarTest {
                update_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest06"}}
                    data: {
                        p_array_str: {slice: [1, -2]},
                        p_array_int64: {slice: [1, -2]},
                    }
                ) {
                    p_str
                    p_array_str
                    p_array_int64
                }
            }
        """, {
            "update_ScalarTest": [{
                'p_str': 'Update ScalarTest06',
                'p_array_str': [],
                'p_array_int64': [0],
            }]
        })

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_ScalarTest {
                delete_ScalarTest(
                    filter: {p_str: {eq: "Update ScalarTest06"}}
                ) {
                    p_str
                    p_array_str
                    p_array_int64
                }
            }
        """, {
            "delete_ScalarTest": [{
                'p_str': 'Update ScalarTest06',
                'p_array_str': [],
                'p_array_int64': [0],
            }]
        })

    def test_graphql_mutation_update_enum_01(self):
        # This tests enum values in updates.

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        select: "Update EnumTest01",
                        color: BLUE
                    }]
                ) {
                    select
                    color
                }
            }
        """, {
            "insert_other__Foo": [{
                'select': 'Update EnumTest01',
                'color': 'BLUE',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo {
                update_other__Foo(
                    filter: {select: {eq: "Update EnumTest01"}}
                    data: {
                        color: {set: RED}
                    }
                ) {
                    select
                    color
                }
            }
        """, {
            "update_other__Foo": [{
                'select': 'Update EnumTest01',
                'color': 'RED',
            }]
        })

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {select: {eq: "Update EnumTest01"}}
                ) {
                    select
                    color
                }
            }
        """, {
            "delete_other__Foo": [{
                'select': 'Update EnumTest01',
                'color': 'RED',
            }]
        })

    def test_graphql_mutation_update_link_01(self):
        orig_data = {
            'name': 'John',
            'groups': [{
                'name': 'basic'
            }],
        }
        data1 = {
            'name': 'John',
            'groups': [],
        }
        data2 = {
            'name': 'John',
            'groups': [{
                'name': 'basic'
            }, {
                'name': 'upgraded'
            }],
        }

        validation_query = r"""
            query {
                User(
                    filter: {
                        name: {eq: "John"}
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """

        self.assert_graphql_query_result(validation_query, {
            "User": [orig_data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    data: {
                        groups: {
                            clear: true
                        }
                    },
                    filter: {
                        name: {eq: "John"}
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [data1]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [data1]
        })

        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    data: {
                        groups: {
                            set: [{
                                filter: {
                                    name: {like: "%"}
                                }
                            }]
                        }
                    },
                    filter: {
                        name: {eq: "John"}
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [data2]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [data2]
        })

        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    data: {
                        groups: {
                            set: [{
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }]
                        }
                    },
                    filter: {
                        name: {eq: "John"}
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [orig_data]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [orig_data]
        })

    def test_graphql_mutation_update_link_02(self):
        # test fancy filters for updates
        orig_data = {
            'name': 'John',
            'groups': [{
                'name': 'basic'
            }],
        }
        data2 = {
            'name': 'John',
            'groups': [{
                'name': 'basic'
            }, {
                'name': 'upgraded'
            }],
        }

        validation_query = r"""
            query {
                User(
                    filter: {
                        name: {eq: "John"}
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """

        self.assert_graphql_query_result(validation_query, {
            "User": [orig_data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    data: {
                        groups: {
                            set: [{
                                filter: {
                                    settings: {name: {eq: "template"}}
                                }
                            }, {
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }]
                        }
                    },
                    filter: {
                        name: {eq: "John"}
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [data2]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [data2]
        })

        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    data: {
                        groups: {
                            set: [{
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }]
                        }
                    },
                    filter: {
                        name: {eq: "John"}
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [orig_data]
        })

        self.assert_graphql_query_result(validation_query, {
            "User": [orig_data]
        })

    def test_graphql_mutation_update_link_03(self):
        # test set ops for update
        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    filter: {
                        name: {eq: "Jane"}
                    },
                    data: {
                        groups: {
                            add: [{
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }]
                        }
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [{
                'name': 'Jane',
                'groups': [{
                    'name': 'basic'
                }, {
                    'name': 'upgraded'
                }],
            }]
        })

        # add an existing group
        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    filter: {
                        name: {eq: "Jane"}
                    },
                    data: {
                        groups: {
                            add: [{
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }]
                        }
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [{
                'name': 'Jane',
                'groups': [{
                    'name': 'basic'
                }, {
                    'name': 'upgraded'
                }],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    filter: {
                        name: {eq: "Jane"}
                    },
                    data: {
                        groups: {
                            remove: [{
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }]
                        }
                    }
                ) {
                    name
                    groups(order: {name: {dir: ASC}}) {
                        name
                    }
                }
            }
        """, {
            "update_User": [{
                'name': 'Jane',
                'groups': [{
                    'name': 'upgraded'
                }],
            }]
        })

    def test_graphql_mutation_update_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'Cannot query field "update_SettingAlias"'):
            self.graphql_query(r"""
                mutation update_SettingAlias {
                    update_SettingAlias(
                        filter: {name: {eq: "template"}}
                        data: {
                            value: {set: "red"},
                        }
                    ) {
                        name
                        value
                    }
                }
            """)

    def test_graphql_mutation_delete_alias_01(self):
        self.assert_graphql_query_result(r"""
            mutation insert_Setting {
                insert_Setting(
                    data: [{
                        name: "delsetting01",
                        value: "red"
                    }]
                ) {
                    name
                    value
                }
            }
        """, {
            "insert_Setting": [{
                'name': 'delsetting01',
                'value': 'red',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_SettingAlias {
                delete_SettingAlias(
                    filter: {
                        name: {eq: "delsetting01"}
                    }
                ) {
                    name
                    value
                }
            }
        """, {
            "delete_SettingAlias": [{
                'name': 'delsetting01',
                'value': 'red',
            }]
        })

        self.assert_graphql_query_result(r"""
            query get_SettingAlias {
                SettingAlias(
                    filter: {
                        name: {eq: "delsetting01"}
                    }
                ) {
                    name
                    value
                }
            }
        """, {
            "SettingAlias": []
        })
