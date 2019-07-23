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

    def test_graphql_mutation_scalars_01(self):
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
            'p_float32': 4.5,
            'p_float64': 4.5,
            'p_decimal':
                123456789123456789123456789.123456789123456789123456789,
        }

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
                    p_float32
                    p_float64
                    p_decimal
                }
            }
        """, {
            "insert_ScalarTest": [data]
        })

        self.assert_graphql_query_result(r"""
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
                    p_float32
                    p_float64
                    p_decimal
                }
            }
        """, {
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
                    p_float32
                    p_float64
                    p_decimal
                }
            }
        """, {
            "delete_ScalarTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(r"""
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
                    p_float32
                    p_float64
                    p_decimal
                }
            }
        """, {
            "ScalarTest": []
        })

    def test_graphql_mutation_scalars_02(self):
        # This tests int64 insertion. Apparently as long as the number
        # is provided as a variable parameter in JSON, there's no
        # limit on the number of digits of an Int.
        data = {
            'p_str': 'New ScalarTest02',
            'p_int64': 1234567890123,
        }

        self.assert_graphql_query_result(r"""
            mutation insert_ScalarTest($num: Int!) {
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

        self.assert_graphql_query_result(r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest02"}}) {
                    p_str
                    p_int64
                }
            }
        """, {
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
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest02"}}) {
                    p_str
                    p_int64
                }
            }
        """, {
            "ScalarTest": []
        })

    def test_graphql_mutation_scalars_03(self):
        # This tests custom scalar insertion.
        data = {
            'p_str': 'New ScalarTest03',
            'p_posint': 42,
        }

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

        self.assert_graphql_query_result(r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest03"}}) {
                    p_str
                    p_posint
                }
            }
        """, {
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
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest03"}}) {
                    p_str
                    p_posint
                }
            }
        """, {
            "ScalarTest": []
        })

    def test_graphql_mutation_scalars_04(self):
        # This tests JSON insertion.
        data = {
            'p_str': 'New ScalarTest04',
            'p_json': '{"foo": [1, null, "aardvark"]}',
        }

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

        self.assert_graphql_query_result(r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest04"}}) {
                    p_str
                    p_json
                }
            }
        """, {
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
        self.assert_graphql_query_result(r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest04"}}) {
                    p_str
                    p_json
                }
            }
        """, {
            "ScalarTest": []
        })

    def test_graphql_mutation_nested_01(self):
        # Test nested insert.
        data = {
            'name': 'New UserGroup01',
            'settings': [{
                'name': 'setting01',
                'value': 'aardvark01',
            }],
        }

        # nested results aren't fetching correctly
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
                }
            }
        """, {
            "insert_UserGroup": [{
                'name': data['name']
            }]
        })

        self.assert_graphql_query_result(r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup01"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
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
        self.assert_graphql_query_result(r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup01"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "UserGroup": []
        })

    def test_graphql_mutation_nested_02(self):
        # Test insert with nested existing object.
        data = {
            'name': 'New UserGroup02',
            'settings': [{
                'name': 'setting02',
                'value': 'aardvark02'
            }],
        }

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

        self.assert_graphql_query_result(r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup02"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
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
        self.assert_graphql_query_result(r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup02"}}) {
                    name
                    settings {
                        name
                        value
                    }
                }
            }
        """, {
            "UserGroup": []
        })

    def test_graphql_mutation_nested_03(self):
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
                }}
            }}
        """, {
            "insert_UserGroup": [{'name': data['name']}]
        })

        self.assert_graphql_query_result(r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup03"}}) {
                    name
                    settings(order: {name: {dir: ASC}}) {
                        name
                        value
                    }
                }
            }
        """, {
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
        self.assert_graphql_query_result(r"""
            query {
                UserGroup(filter: {name: {eq: "New UserGroup03"}}) {
                    name
                    settings(order: {name: {dir: ASC}}) {
                        name
                        value
                    }
                }
            }
        """, {
            "UserGroup": []
        })

    def test_graphql_mutation_nested_05(self):
        # Test nested insert for a singular link.
        data = {
            "name": "New User05",
            "age": 99,
            "score": 99.99,
            "profile": {
                "name": "Alice profile",
                "value": "special"
            }
        }

        # nested results aren't fetching correctly
        self.assert_graphql_query_result(r"""
            mutation insert_User {
                insert_User(
                    data: [{
                        name: "New User05",
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
                }
            }
        """, {
            "insert_User": [{
                'name': data['name']
            }]
        })

        self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "New User05"}}) {
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
            "User": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_User {
                delete_User(filter: {name: {eq: "New User05"}}) {
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
        self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "New User05"}}) {
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
            "User": []
        })

    def test_graphql_mutation_nested_06(self):
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
            "name": "New User06",
            "age": 99,
            "score": 99.99,
            "profile": profile
        }

        # nested results aren't fetching correctly
        self.assert_graphql_query_result(rf"""
            mutation insert_User {{
                insert_User(
                    data: [{{
                        name: "New User06",
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
                }}
            }}
        """, {
            "insert_User": [{
                'name': data['name']
            }]
        })

        self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "New User06"}}) {
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
            "User": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_User {
                delete_User(filter: {name: {eq: "New User06"}}) {
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
        self.assert_graphql_query_result(r"""
            query {
                User(filter: {name: {eq: "New User06"}}) {
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
            "User": []
        })
