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

    SCHEMA_OTHER_DEEP = os.path.join(os.path.dirname(__file__), 'schemas',
                                     'graphql_schema_other_deep.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'graphql_setup.edgeql')

    # GraphQL queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    def test_graphql_mutation_insert_scalars_01(self):
        data = {
            'p_bool': False,
            'p_str': 'New ScalarTest01',
            'p_datetime': '2019-05-01T01:02:35.196811+00:00',
            'p_local_datetime': '2019-05-01T01:02:35.196811',
            'p_local_date': '2019-05-01',
            'p_local_time': '01:02:35.196811',
            'p_duration': 'PT21H30M',
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
        # This tests JSON insertion. JSON can only be inserted via a variable.
        data = {
            'p_str': 'New ScalarTest04',
            'p_json': {"foo": [1, None, "aardvark"]},
        }

        validation_query = r"""
            query {
                ScalarTest(filter: {p_str: {eq: "New ScalarTest04"}}) {
                    p_str
                    p_json
                }
            }
        """

        self.assert_graphql_query_result(
            r"""
                mutation insert_ScalarTest(
                    $p_str: String!,
                    $p_json: JSON
                ) {
                    insert_ScalarTest(
                        data: [{
                            p_str: $p_str,
                            p_json: $p_json,
                        }]
                    ) {
                        p_str
                        p_json
                    }
                }
            """, {
                "insert_ScalarTest": [data]
            },
            variables=data,
        )

        self.assert_graphql_query_result(validation_query, {
            "ScalarTest": [data]
        })

        self.assert_graphql_query_result(
            r"""
                mutation delete_ScalarTest(
                    $p_json: JSON
                ) {
                    delete_ScalarTest(filter: {p_json: {eq: $p_json}}) {
                        p_str
                        p_json
                    }
                }
            """, {
                "delete_ScalarTest": [data]
            },
            variables=data,
        )

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

    def test_graphql_mutation_insert_enum_02(self):
        # This tests enum values in insertion, using variables.
        data = {
            'select': 'New EnumTest02',
            'color': 'RED',
        }

        validation_query = r"""
            query {
                other__Foo(filter: {select: {eq: "New EnumTest02"}}) {
                    select
                    color
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo(
                $select: String!,
                $color: other__ColorEnum!
            ) {
                insert_other__Foo(
                    data: [{
                        select: $select,
                        color: $color,
                    }]
                ) {
                    select
                    color
                }
            }
        """, {
            "insert_other__Foo": [data]
        }, variables=data)

        self.assert_graphql_query_result(validation_query, {
            "other__Foo": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {select: {eq: "New EnumTest02"}}
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

    def test_graphql_mutation_insert_enum_03(self):
        # This tests enum values in insertion.
        data = {
            'select': 'New EnumTest05',
            'color': 'GREEN',
            'color_array': ['RED', 'GREEN', 'RED'],
        }

        validation_query = r"""
            query {
                other__Foo(filter: {select: {eq: "New EnumTest05"}}) {
                    select
                    color
                    color_array
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        select: "New EnumTest05",
                        color: GREEN,
                        color_array: [RED, GREEN, RED],
                    }]
                ) {
                    select
                    color
                    color_array
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
                    filter: {select: {eq: "New EnumTest05"}}
                ) {
                    select
                    color
                    color_array
                }
            }
        """, {
            "delete_other__Foo": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "other__Foo": []
        })

    def test_graphql_mutation_insert_enum_04(self):
        # This tests enum values in insertion.
        data = {
            'select': 'New EnumTest03',
            'color': 'GREEN',
            'multi_color': {'RED', 'GREEN'},
        }

        validation_query = r"""
            query {
                other__Foo(filter: {select: {eq: "New EnumTest03"}}) {
                    select
                    color
                    multi_color
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        select: "New EnumTest03",
                        color: GREEN,
                        multi_color: [RED, GREEN],
                    }]
                ) {
                    select
                    color
                    multi_color
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
                    filter: {select: {eq: "New EnumTest03"}}
                ) {
                    select
                    color
                    multi_color
                }
            }
        """, {
            "delete_other__Foo": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "other__Foo": []
        })

    def test_graphql_mutation_insert_range_01(self):
        # This tests range and multirange values in insertion.
        data = {
            "name": "New RangeTest01",
            "rval": {
                "lower": -1.2,
                "upper": 3.4,
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
                    "lower": -3,
                    "upper": 12,
                    "inc_lower": True,
                    "inc_upper": False
                },
            ],
            "rdate": {
                "lower": "2019-02-13",
                "upper": "2023-08-29",
                "inc_lower": True,
                "inc_upper": False
            },
            "mdate": [
                {
                    "lower": "2019-02-13",
                    "upper": "2023-08-29",
                    "inc_lower": True,
                    "inc_upper": False
                },
                {
                    "lower": "2026-10-20",
                    "upper": None,
                    "inc_lower": True,
                    "inc_upper": False
                }
            ]
        }

        validation_query = r"""
            query {
                RangeTest(filter: {name: {eq: "New RangeTest01"}}) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_RangeTest {
                insert_RangeTest(
                    data: [{
                        name: "New RangeTest01",
                        rval: {
                            lower: -1.2,
                            upper: 3.4,
                            inc_lower: true,
                            inc_upper: false
                        },
                        mval: [
                            {
                                lower: null,
                                upper: -10,
                                inc_lower: false,
                                inc_upper: false
                            },
                            {
                                lower: -3,
                                upper: 12,
                                inc_lower: true,
                                inc_upper: false
                            },
                        ],
                        rdate: {
                            lower: "2019-02-13",
                            upper: "2023-08-29",
                            inc_lower: true,
                            inc_upper: false
                        },
                        mdate: [
                            {
                                lower: "2019-02-13",
                                upper: "2023-08-29",
                                inc_lower: true,
                                inc_upper: false
                            },
                            {
                                lower: "2026-10-20",
                                upper: null,
                                inc_lower: true,
                                inc_upper: false
                            }
                        ]
                    }]
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "insert_RangeTest": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "RangeTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_RangeTest {
                delete_RangeTest(
                    filter: {name: {eq: "New RangeTest01"}}
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "delete_RangeTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "RangeTest": []
        })

    def test_graphql_mutation_insert_range_02(self):
        # This tests range and multirange values in insertion.
        data = {
            "name": "New RangeTest02",
            "rval": {
                "lower": -1.2,
                "upper": 3.4,
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
                    "lower": -3,
                    "upper": 12,
                    "inc_lower": True,
                    "inc_upper": False
                },
            ],
            "rdate": {
                "lower": "2019-02-13",
                "upper": "2023-08-29",
                "inc_lower": True,
                "inc_upper": False
            },
            "mdate": [
                {
                    "lower": "2019-02-13",
                    "upper": "2023-08-29",
                    "inc_lower": True,
                    "inc_upper": False
                },
                {
                    "lower": "2026-10-20",
                    "upper": None,
                    "inc_lower": True,
                    "inc_upper": False
                }
            ]
        }

        validation_query = r"""
            query {
                RangeTest(filter: {name: {eq: "New RangeTest02"}}) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_RangeTest(
                $name: String!,
                $rval: RangeOfFloat,
                $mval: [RangeOfFloat!],
                $rdate: RangeOfString,
                $mdate: [RangeOfString!]
            ) {
                insert_RangeTest(
                    data: [{
                        name: $name,
                        rval: $rval,
                        mval: $mval,
                        rdate: $rdate,
                        mdate: $mdate
                    }]
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "insert_RangeTest": [data]
        }, variables=data)

        self.assert_graphql_query_result(validation_query, {
            "RangeTest": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_RangeTest {
                delete_RangeTest(
                    filter: {name: {eq: "New RangeTest02"}}
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "delete_RangeTest": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "RangeTest": []
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

    def test_graphql_mutation_insert_nested_08(self):
        # Issue #1243
        # Test nested insert.
        data = {
            'name': 'Strategy01',
            'games': [{
                'name': 'SomeGame01',
                'players': [{
                    'name': 'Alice'
                }]
            }],
        }

        validation_query = r"""
            query {
                Genre(filter: {name: {eq: "Strategy01"}}) {
                    name
                    games {
                        name
                        players {
                            name
                        }
                    }
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_genre {
                insert_Genre(data: [
                    {
                        name: "Strategy01",
                        games: [{
                            data: {
                                name: "SomeGame01",
                                players: [{
                                    filter: {name: {eq: "Alice"}}
                                }]
                            }
                        }]
                    }
                ]) {
                    name
                    games {
                        name
                        players {
                            name
                        }
                    }
                }
            }

        """, {
            "insert_Genre": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "Genre": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_Genre {
                delete_Genre(filter: {name: {eq: "Strategy01"}}) {
                    name
                    games {
                        name
                        players {
                            name
                        }
                    }
                }
            }
        """, {
            "delete_Genre": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "Genre": []
        })

        self.assert_graphql_query_result(r"""
            mutation delete_Game {
                delete_Game(filter: {name: {eq: "SomeGame01"}}) {
                    name
                    players {
                        name
                    }
                }
            }
        """, {
            "delete_Game": data['games']
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(r'''
            query {
                Game(filter: {name: {eq: "SomeGame01"}}) {
                    name
                    players {
                        name
                    }
                }
            }
        ''', {
            "Game": []
        })

    def test_graphql_mutation_insert_nested_09(self):
        # Issue #3470
        # Test nested insert of the same type.
        data = {
            'after': 'BaseFoo09',
            'color': 'GREEN',
            'foos': [{
                'after': 'NestedFoo090',
                'color': 'RED',
            }, {
                'after': 'NestedFoo091',
                'color': 'BLUE',
            }],
        }

        validation_query = r"""
            query {
                other__Foo(filter: {after: {eq: "BaseFoo09"}}) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        after: "BaseFoo09",
                        color: GREEN,
                        foos: [{
                            data: {
                                after: "NestedFoo090",
                                color: RED,
                            }
                        }, {
                            data: {
                                after: "NestedFoo091",
                                color: BLUE,
                            }
                        }]
                    }]
                ) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
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
                    filter: {after: {like: "%Foo09%"}},
                    order: {color: {dir: ASC}}
                ) {
                    after
                    color
                }
            }
        """, {
            "delete_other__Foo": [{
                'after': 'NestedFoo090',
                'color': 'RED',
            }, {
                'after': 'BaseFoo09',
                'color': 'GREEN',
            }, {
                'after': 'NestedFoo091',
                'color': 'BLUE',
            }]
        })

    def test_graphql_mutation_insert_nested_10(self):
        # Issue #3470
        # Test nested insert of the same type.
        data = {
            'after': 'BaseFoo10',
            'color': 'GREEN',
            'foos': [{
                'after': 'NestedFoo100',
                'color': 'RED',
            }, {
                'after': 'NestedFoo101',
                'color': 'BLUE',
            }],
        }

        validation_query = r"""
            query {
                other__Foo(filter: {after: {eq: "BaseFoo10"}}) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        after: "NestedFoo100",
                        color: RED,
                    }]
                ) {
                    after
                    color
                }
            }
        """, {
            "insert_other__Foo": [data['foos'][0]]
        })

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        after: "BaseFoo10",
                        color: GREEN,
                        foos: [{
                            filter: {
                                after: {eq: "NestedFoo100"},
                            }
                        }, {
                            data: {
                                after: "NestedFoo101",
                                color: BLUE,
                            }
                        }]
                    }]
                ) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
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
                    filter: {after: {like: "%Foo10%"}},
                    order: {color: {dir: ASC}}
                ) {
                    after
                    color
                }
            }
        """, {
            "delete_other__Foo": [{
                'after': 'NestedFoo100',
                'color': 'RED',
            }, {
                'after': 'BaseFoo10',
                'color': 'GREEN',
            }, {
                'after': 'NestedFoo101',
                'color': 'BLUE',
            }]
        })

    def test_graphql_mutation_insert_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'insert_SettingAlias'"):
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
            r"Field 'data' is not defined by type 'NestedInsertNamedObject'"
        ):
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

    def test_graphql_mutation_insert_bad_03(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"objects provided for 'settings' are not distinct"
        ):
            self.graphql_query(r"""
                mutation insert_UserGroup {
                    insert_UserGroup(
                        data: [{
                            name: "Bad UserGroup03",
                            settings: [{
                                filter: {name: {like: "%"}}
                            }, {
                                filter: {name: {like: "perks"}}
                            }]
                        }]
                    ) {
                        name
                        settings {
                            name
                            value
                        }
                    }
                }
            """)

    def test_graphql_mutation_insert_bad_04(self):
        with self.assertRaisesRegex(
            edgedb.CardinalityViolationError,
            r"more than one object provided for 'profile'"
        ):
            self.graphql_query(r"""
                mutation insert_User {
                    insert_User(
                        data: [{
                            name: "Bad User04",
                            active: true,
                            age: 33,
                            score: 33.33,
                            profile: {
                                filter: {name: {like: "%"}}
                            },
                        }]
                    ) {
                        name
                        age
                        score
                        profile {
                            name
                        }
                    }
                }
            """)

    def test_graphql_mutation_insert_bad_05(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"settings violates exclusivity constraint"
        ):
            self.graphql_query(r"""
                mutation insert_UserGroup {
                    insert_UserGroup(
                        data: [{
                            name: "Bad UserGroup05",
                            settings: {
                                filter: {name: {like: "perks"}}
                            }
                        }]
                    ) {
                        name
                        settings {
                            name
                            value
                        }
                    }
                }
            """)

    def test_graphql_mutation_insert_bad_06(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"Minimum allowed value for positive_int_t is 0"
        ):
            self.graphql_query(r"""
                mutation insert_ScalarTest {
                    insert_ScalarTest(
                        data: [{
                            p_posint: -42,
                        }]
                    ) {
                        p_posint
                    }
                }
            """)

    def test_graphql_mutation_insert_bad_07(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"p_short_str must be no longer than 5 characters"
        ):
            self.graphql_query(r"""
                mutation insert_ScalarTest {
                    insert_ScalarTest(
                        data: [{
                            p_short_str: "too long",
                        }]
                    ) {
                        p_short_str
                    }
                }
            """)

    def test_graphql_mutation_insert_bad_08(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r"invalid input syntax for type String: '2023/05/07'"
        ):
            self.graphql_query(r"""
                mutation insert_ScalarTest {
                    insert_ScalarTest(
                        data: [{
                            p_local_date: "2023/05/07",
                        }]
                    ) {
                        p_local_date
                    }
                }
            """)

    def test_graphql_mutation_insert_bad_09(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            r'String/String field value out of range: "2023-05-77"'
        ):
            self.graphql_query(r"""
                mutation insert_ScalarTest {
                    insert_ScalarTest(
                        data: [{
                            p_local_date: "2023-05-77",
                        }]
                    ) {
                        p_local_date
                    }
                }
            """)

    def test_graphql_mutation_insert_multiple_01(self):
        # Issue #1566
        # Test multiple mutations.
        data_s = {
            'name': 'multisetting01',
            'value': 'yellow',
        }
        data_p = {
            'name': 'multiprofile01',
            'value': 'multirofile',
        }

        validation_query = r"""
            query {
                Setting(filter: {name: {eq: "multisetting01"}}) {
                    name
                    value
                }
                Profile(filter: {name: {eq: "multiprofile01"}}) {
                    name
                    value
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation multi_insert {
                insert_Setting(
                    data: [{
                        name: "multisetting01",
                        value: "yellow"
                    }]
                ) {
                    name
                    value
                }
                insert_Profile(
                    data: [{
                        name: "multiprofile01",
                        value: "multirofile"
                    }]
                ) {
                    name
                    value
                }
            }
        """, {
            "insert_Setting": [data_s],
            "insert_Profile": [data_p]
        })

        self.assert_graphql_query_result(validation_query, {
            "Setting": [data_s],
            "Profile": [data_p],
        })

        self.assert_graphql_query_result(r"""
            mutation multi_delete {
                delete_Setting(
                    filter: {
                        name: {eq: "multisetting01"}
                    }
                ) {
                    name
                    value
                }
                delete_Profile(
                    filter: {
                        name: {eq: "multiprofile01"}
                    }
                ) {
                    name
                    value
                }
            }
        """, {
            "delete_Setting": [data_s],
            "delete_Profile": [data_p]
        })

        self.assert_graphql_query_result(validation_query, {
            "Setting": [],
            "Profile": [],
        })

    def test_graphql_mutation_insert_default_01(self):
        # Test insert object with a required property with a default.
        data = {
            'name': 'New InactiveUser01',
            'age': 99,
            'active': False,
            'score': 0,
        }

        validation_query = r"""
            query {
                User(filter: {name: {eq: "New InactiveUser01"}}) {
                    name
                    age
                    active
                    score
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_User {
                insert_User(
                    data: [{
                        name: "New InactiveUser01",
                        age: 99
                    }]
                ) {
                    name
                    age
                    active
                    score
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
                delete_User(filter: {name: {eq: "New InactiveUser01"}}) {
                    name
                    age
                    active
                    score
                }
            }
        """, {
            "delete_User": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "User": []
        })

    def test_graphql_mutation_insert_readonly_01(self):
        # Test insert object with a readonly property
        data = {
            '__typename': 'NotEditable_Type',
            'once': 'New NotEditable01',
            'computed': 'a computed value',
        }

        validation_query = r"""
            query {
                NotEditable(filter: {once: {eq: "New NotEditable01"}}) {
                    __typename
                    once
                    computed
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_NotEditable {
                insert_NotEditable(
                    data: [{
                        once: "New NotEditable01",
                    }]
                ) {
                    __typename
                    once
                    computed
                }
            }
        """, {
            "insert_NotEditable": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "NotEditable": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_NotEditable {
                delete_NotEditable(filter: {once: {eq: "New NotEditable01"}}) {
                    __typename
                    once
                    computed
                }
            }
        """, {
            "delete_NotEditable": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "NotEditable": []
        })

    def test_graphql_mutation_insert_readonly_02(self):
        # Test insert object without any properties that can be set
        data = {
            '__typename': 'Fixed_Type',
            'computed': 123,
        }

        validation_query = r"""
            query {
                Fixed {
                    __typename
                    computed
                }
            }
        """

        self.assert_graphql_query_result(validation_query, {
            "Fixed": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_Fixed {
                delete_Fixed {
                    __typename
                    computed
                }
            }
        """, {
            "delete_Fixed": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "Fixed": []
        })

        self.assert_graphql_query_result(r"""
            mutation insert_Fixed {
                insert_Fixed {
                    __typename
                    computed
                }
            }
        """, {
            "insert_Fixed": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "Fixed": [data]
        })

    def test_graphql_mutation_insert_typename_01(self):
        # This tests the typename funcitonality after insertion.
        # Issue #5985
        data = {
            '__typename': 'other__Foo_Type',
            'select': 'New TypenameTest01',
            'color': 'GREEN',
        }

        validation_query = r"""
            query {
                __typename
                other__Foo(filter: {select: {eq: "New TypenameTest01"}}) {
                    __typename
                    select
                    color
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                __typename
                insert_other__Foo(
                    data: [{
                        select: "New TypenameTest01",
                        color: GREEN,
                    }]
                ) {
                    __typename
                    select
                    color
                }
            }
        """, {
            "__typename": 'Mutation',
            "insert_other__Foo": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "__typename": 'Query',
            "other__Foo": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                __typename
                delete_other__Foo(
                    filter: {select: {eq: "New TypenameTest01"}}
                ) {
                    __typename
                    select
                    color
                }
            }
        """, {
            "__typename": 'Mutation',
            "delete_other__Foo": [data]
        })

        # validate that the deletion worked
        self.assert_graphql_query_result(validation_query, {
            "other__Foo": []
        })

    def test_graphql_mutation_update_scalars_01(self):
        orig_data = {
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
        }
        data = {
            'p_bool': False,
            'p_str': 'Update ScalarTest01',
            'p_datetime': '2019-05-01T01:02:35.196811+00:00',
            'p_local_datetime': '2019-05-01T01:02:35.196811',
            'p_local_date': '2019-05-01',
            'p_local_time': '01:02:35.196811',
            'p_duration': 'PT21H30M',
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
                        p_duration: {set: "PT21H30M"},
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
            'p_str': 'Hello world',
            'p_posint': 42,
            'p_short_str': 'hello',
        }
        data = {
            'p_str': 'Update ScalarTest02',
            'p_posint': 9999,
            'p_short_str': 'hi',
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
                    p_short_str
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
                        p_short_str: {set: "hi"},
                    }
                ) {
                    p_str
                    p_posint
                    p_short_str
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
                $p_short_str: String,
            ) {
                update_ScalarTest(
                    data: {
                        p_str: {set: $p_str},
                        p_posint: {set: $p_posint},
                        p_short_str: {set: $p_short_str},
                    }
                ) {
                    p_str
                    p_posint
                    p_short_str
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
            'p_str': 'Hello world',
            'p_json': {"foo": [1, None, "bar"]},
        }
        data = {
            'p_str': 'Update ScalarTest03',
            'p_json': {"bar": [None, 2, "aardvark"]},
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

        # Test that basic and complex JSON values can be updated
        for json_val in [data['p_json'], data['p_json']['bar'],
                         True, 123, "hello world", None]:
            data['p_json'] = json_val
            self.assert_graphql_query_result(
                r"""
                    mutation update_ScalarTest(
                        $p_str: String!,
                        $p_json: JSON
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
                    "update_ScalarTest": [data]
                },
                variables=data
            )

            self.assert_graphql_query_result(validation_query, {
                "ScalarTest": [data]
            })

        self.assert_graphql_query_result(
            r"""
                mutation update_ScalarTest(
                    $p_str: String,
                    $p_json: JSON,
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
            },
            variables=orig_data
        )

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

    def test_graphql_mutation_update_scalars_07(self):
        # This tests array of JSON mutations. JSON can only be
        # mutated via a variable.
        data = {
            'el0': {"foo": [1, None, "aardvark"]},
            'el1': False,
        }

        self.assert_graphql_query_result(
            r"""
                mutation insert_ScalarTest($el0: JSON!, $el1: JSON!) {
                    insert_ScalarTest(
                        data: [{
                            p_str: "Update ScalarTest07",
                            p_array_json: [$el0, $el1],
                        }]
                    ) {
                        p_str
                        p_array_json
                    }
                }
            """, {
                "insert_ScalarTest": [{
                    'p_str': 'Update ScalarTest07',
                    'p_array_json': [data['el0'], data['el1']],
                }]
            },
            variables=data,
        )

        self.assert_graphql_query_result(
            r"""
                mutation update_ScalarTest($el: JSON!) {
                    update_ScalarTest(
                        filter: {p_str: {eq: "Update ScalarTest07"}}
                        data: {
                            p_array_json: {prepend: [$el]},
                        }
                    ) {
                        p_str
                        p_array_json
                    }
                }
            """, {
                "update_ScalarTest": [{
                    'p_str': 'Update ScalarTest07',
                    'p_array_json': ["first", data['el0'], data['el1']],
                }]
            },
            variables={"el": "first"}
        )

        self.assert_graphql_query_result(
            r"""
                mutation update_ScalarTest($el: JSON!) {
                    update_ScalarTest(
                        filter: {p_str: {eq: "Update ScalarTest07"}}
                        data: {
                            p_array_json: {append: [$el]},
                        }
                    ) {
                        p_str
                        p_array_json
                    }
                }
            """, {
                "update_ScalarTest": [{
                    'p_str': 'Update ScalarTest07',
                    'p_array_json': ["first", data['el0'], data['el1'], 9999],
                }]
            },
            variables={"el": 9999}
        )

        self.assert_graphql_query_result(
            r"""
                mutation update_ScalarTest {
                    update_ScalarTest(
                        filter: {p_str: {eq: "Update ScalarTest07"}}
                        data: {
                            p_array_json: {slice: [1, 3]},
                        }
                    ) {
                        p_str
                        p_array_json
                    }
                }
            """, {
                "update_ScalarTest": [{
                    'p_str': 'Update ScalarTest07',
                    'p_array_json': [data['el0'], data['el1']],
                }]
            }
        )

        # clean up
        self.assert_graphql_query_result(
            r"""
                mutation delete_ScalarTest {
                    delete_ScalarTest(
                        filter: {p_str: {eq: "Update ScalarTest07"}}
                    ) {
                        p_str
                        p_array_json
                    }
                }
            """, {
                "delete_ScalarTest": [{
                    'p_str': 'Update ScalarTest07',
                    'p_array_json': [data['el0'], data['el1']],
                }]
            }
        )

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

    def test_graphql_mutation_update_enum_02(self):
        # This tests enum values in updates using variables.

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        select: "Update EnumTest02",
                        color: BLUE
                    }]
                ) {
                    select
                    color
                }
            }
        """, {
            "insert_other__Foo": [{
                'select': 'Update EnumTest02',
                'color': 'BLUE',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo (
                $color: other__ColorEnum!
            ) {
                update_other__Foo(
                    filter: {select: {eq: "Update EnumTest02"}}
                    data: {
                        color: {set: $color}
                    }
                ) {
                    select
                    color
                }
            }
        """, {
            "update_other__Foo": [{
                'select': 'Update EnumTest02',
                'color': 'RED',
            }]
        }, variables={"color": "RED"})

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {select: {eq: "Update EnumTest02"}}
                ) {
                    select
                    color
                }
            }
        """, {
            "delete_other__Foo": [{
                'select': 'Update EnumTest02',
                'color': 'RED',
            }]
        })

    def test_graphql_mutation_update_enum_03(self):
        # This tests enum values in updates.

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        select: "Update EnumTest03",
                        color: BLUE,
                        color_array: [RED, BLUE, BLUE]
                    }]
                ) {
                    select
                    color
                    color_array
                }
            }
        """, {
            "insert_other__Foo": [{
                'select': 'Update EnumTest03',
                'color': 'BLUE',
                'color_array': ['RED', 'BLUE', 'BLUE'],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo {
                update_other__Foo(
                    filter: {select: {eq: "Update EnumTest03"}}
                    data: {
                        color_array: {append: [GREEN]}
                    }
                ) {
                    select
                    color_array
                }
            }
        """, {
            "update_other__Foo": [{
                'select': 'Update EnumTest03',
                'color_array': ['RED', 'BLUE', 'BLUE', 'GREEN'],
            }]
        })

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {select: {eq: "Update EnumTest03"}}
                ) {
                    select
                    color_array
                }
            }
        """, {
            "delete_other__Foo": [{
                'select': 'Update EnumTest03',
                'color_array': ['RED', 'BLUE', 'BLUE', 'GREEN'],
            }]
        })

    def test_graphql_mutation_update_enum_04(self):
        # This tests enum values in updates.

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        select: "Update EnumTest04",
                        color: BLUE,
                        multi_color: [RED, BLUE]
                    }]
                ) {
                    select
                    color
                    multi_color
                }
            }
        """, {
            "insert_other__Foo": [{
                'select': 'Update EnumTest04',
                'color': 'BLUE',
                'multi_color': ['RED', 'BLUE'],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo {
                update_other__Foo(
                    filter: {select: {eq: "Update EnumTest04"}}
                    data: {
                        multi_color: {add: GREEN}
                    }
                ) {
                    select
                    multi_color
                }
            }
        """, {
            "update_other__Foo": [{
                'select': 'Update EnumTest04',
                'multi_color': {'RED', 'BLUE', 'GREEN'},
            }]
        })

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {select: {eq: "Update EnumTest04"}}
                ) {
                    select
                    multi_color
                }
            }
        """, {
            "delete_other__Foo": [{
                'select': 'Update EnumTest04',
                'multi_color': {'RED', 'BLUE', 'GREEN'},
            }]
        })

    def test_graphql_mutation_update_range_01(self):
        # This tests range and multirange values in updates.
        self.assert_graphql_query_result(r"""
            mutation insert_RangeTest {
                insert_RangeTest(
                    data: [{
                        name: "Update RangeTest01",
                        rval: {
                            lower: -1.2,
                            upper: 3.4,
                            inc_lower: true,
                            inc_upper: false
                        },
                        mval: [
                            {
                                lower: null,
                                upper: -10,
                                inc_lower: false,
                                inc_upper: false
                            },
                            {
                                lower: -3,
                                upper: 12,
                                inc_lower: true,
                                inc_upper: false
                            },
                        ],
                        rdate: {
                            lower: "2019-02-13",
                            upper: "2023-08-29",
                            inc_lower: true,
                            inc_upper: false
                        },
                        mdate: [
                            {
                                lower: "2019-02-13",
                                upper: "2023-08-29",
                                inc_lower: true,
                                inc_upper: false
                            },
                            {
                                lower: "2026-10-20",
                                upper: null,
                                inc_lower: true,
                                inc_upper: false
                            }
                        ]
                    }]
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "insert_RangeTest": [{
                "name": "Update RangeTest01",
                "rval": {
                    "lower": -1.2,
                    "upper": 3.4,
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
                        "lower": -3,
                        "upper": 12,
                        "inc_lower": True,
                        "inc_upper": False
                    },
                ],
                "rdate": {
                    "lower": "2019-02-13",
                    "upper": "2023-08-29",
                    "inc_lower": True,
                    "inc_upper": False
                },
                "mdate": [
                    {
                        "lower": "2019-02-13",
                        "upper": "2023-08-29",
                        "inc_lower": True,
                        "inc_upper": False
                    },
                    {
                        "lower": "2026-10-20",
                        "upper": None,
                        "inc_lower": True,
                        "inc_upper": False
                    }
                ]
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_RangeTest {
                update_RangeTest(
                    filter: {name: {eq: "Update RangeTest01"}},
                    data: {
                        rval: {
                            set: {
                                lower: 5.6,
                                upper: 7.8,
                                inc_lower: true,
                                inc_upper: true
                            }
                        },
                        mval: {
                            set: [
                                {
                                    lower: 0,
                                    upper: 1.2,
                                    inc_lower: true,
                                    inc_upper: false
                                },
                            ]
                        },
                        rdate: {
                            set: {
                                lower: "2018-12-10",
                                upper: "2022-10-20",
                                inc_lower: true,
                                inc_upper: false
                            }
                        },
                        mdate: {
                            set: [
                                {
                                    lower: null,
                                    upper: "2019-02-13",
                                    inc_lower: true,
                                    inc_upper: false
                                },
                                {
                                    lower: "2023-08-29",
                                    upper: "2026-10-20",
                                    inc_lower: true,
                                    inc_upper: false
                                }
                            ]
                        }
                    }
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "update_RangeTest": [{
                "name": "Update RangeTest01",
                "rval": {
                    "lower": 5.6,
                    "upper": 7.8,
                    "inc_lower": True,
                    "inc_upper": True
                },
                "mval": [
                    {
                        "lower": 0,
                        "upper": 1.2,
                        "inc_lower": True,
                        "inc_upper": False
                    },
                ],
                "rdate": {
                    "lower": "2018-12-10",
                    "upper": "2022-10-20",
                    "inc_lower": True,
                    "inc_upper": False
                },
                "mdate": [
                    {
                        "lower": None,
                        "upper": "2019-02-13",
                        "inc_lower": False,
                        "inc_upper": False
                    },
                    {
                        "lower": "2023-08-29",
                        "upper": "2026-10-20",
                        "inc_lower": True,
                        "inc_upper": False
                    }
                ]
            }]
        })

        # Cleanup
        self.assert_graphql_query_result(r"""
            mutation delete_RangeTest {
                delete_RangeTest(
                    filter: {name: {eq: "Update RangeTest01"}}
                ) {
                    name
                }
            }
        """, {
            "delete_RangeTest": [{"name": "Update RangeTest01"}]
        })

    def test_graphql_mutation_update_range_02(self):
        # This tests range and multirange values in insertion.
        self.assert_graphql_query_result(r"""
            mutation insert_RangeTest {
                insert_RangeTest(
                    data: [{
                        name: "Update RangeTest02",
                        rval: {
                            lower: -1.2,
                            upper: 3.4,
                            inc_lower: true,
                            inc_upper: false
                        },
                        mval: [
                            {
                                lower: null,
                                upper: -10,
                                inc_lower: false,
                                inc_upper: false
                            },
                            {
                                lower: -3,
                                upper: 12,
                                inc_lower: true,
                                inc_upper: false
                            },
                        ],
                        rdate: {
                            lower: "2019-02-13",
                            upper: "2023-08-29",
                            inc_lower: true,
                            inc_upper: false
                        },
                        mdate: [
                            {
                                lower: "2019-02-13",
                                upper: "2023-08-29",
                                inc_lower: true,
                                inc_upper: false
                            },
                            {
                                lower: "2026-10-20",
                                upper: null,
                                inc_lower: true,
                                inc_upper: false
                            }
                        ]
                    }]
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "insert_RangeTest": [{
                "name": "Update RangeTest02",
                "rval": {
                    "lower": -1.2,
                    "upper": 3.4,
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
                        "lower": -3,
                        "upper": 12,
                        "inc_lower": True,
                        "inc_upper": False
                    },
                ],
                "rdate": {
                    "lower": "2019-02-13",
                    "upper": "2023-08-29",
                    "inc_lower": True,
                    "inc_upper": False
                },
                "mdate": [
                    {
                        "lower": "2019-02-13",
                        "upper": "2023-08-29",
                        "inc_lower": True,
                        "inc_upper": False
                    },
                    {
                        "lower": "2026-10-20",
                        "upper": None,
                        "inc_lower": True,
                        "inc_upper": False
                    }
                ]
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_RangeTest(
                $rval: RangeOfFloat,
                $mval: [RangeOfFloat!],
                $rdate: RangeOfString,
                $mdate: [RangeOfString!]
            ) {
                update_RangeTest(
                    filter: {name: {eq: "Update RangeTest02"}},
                    data: {
                        rval: {set: $rval},
                        mval: {set: $mval},
                        rdate: {set: $rdate},
                        mdate: {set: $mdate}
                    }
                ) {
                    name
                    rval
                    mval
                    rdate
                    mdate
                }
            }
        """, {
            "update_RangeTest": [{
                "name": "Update RangeTest02",
                "rval": {
                    "lower": 5.6,
                    "upper": 7.8,
                    "inc_lower": True,
                    "inc_upper": True
                },
                "mval": [
                    {
                        "lower": 0,
                        "upper": 1.2,
                        "inc_lower": True,
                        "inc_upper": False
                    },
                ],
                "rdate": {
                    "lower": "2018-12-10",
                    "upper": "2022-10-20",
                    "inc_lower": True,
                    "inc_upper": False
                },
                "mdate": [
                    {
                        "lower": None,
                        "upper": "2019-02-13",
                        "inc_lower": False,
                        "inc_upper": False
                    },
                    {
                        "lower": "2023-08-29",
                        "upper": "2026-10-20",
                        "inc_lower": True,
                        "inc_upper": False
                    }
                ]
            }]
        }, variables={
            "rval": {
                "lower": 5.6,
                "upper": 7.8,
                "inc_lower": True,
                "inc_upper": True
            },
            "mval": [
                {
                    "lower": 0,
                    "upper": 1.2,
                    "inc_lower": True,
                    "inc_upper": False
                },
            ],
            "rdate": {
                "lower": "2018-12-10",
                "upper": "2022-10-20",
                "inc_lower": True,
                "inc_upper": False
            },
            "mdate": [
                {
                    "lower": None,
                    "upper": "2019-02-13",
                    "inc_lower": True,
                    "inc_upper": False
                },
                {
                    "lower": "2023-08-29",
                    "upper": "2026-10-20",
                    "inc_lower": True,
                    "inc_upper": False
                }
            ]
        })

        # Cleanup
        self.assert_graphql_query_result(r"""
            mutation delete_RangeTest {
                delete_RangeTest(
                    filter: {name: {eq: "Update RangeTest02"}}
                ) {
                    name
                }
            }
        """, {
            "delete_RangeTest": [{"name": "Update RangeTest02"}]
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
                'name': 'unused'
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
                'name': 'unused'
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
        # test set ops for update of multi link
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

    def test_graphql_mutation_update_link_04(self):
        # test set ops for update of multi link with singleton values
        # (omitting the wrapping [...])
        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    filter: {
                        name: {eq: "Jane"}
                    },
                    data: {
                        groups: {
                            add: {
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }
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
                            add: {
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }
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
                            remove: {
                                filter: {
                                    name: {eq: "basic"}
                                }
                            }
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

        # set a specific group
        self.assert_graphql_query_result(r"""
            mutation update_User {
                update_User(
                    filter: {
                        name: {eq: "Jane"}
                    },
                    data: {
                        groups: {
                            set: {
                                filter: {
                                    name: {eq: "unused"}
                                }
                            }
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
                    'name': 'unused'
                }],
            }]
        })

    def test_graphql_mutation_update_link_05(self):
        # updating a single link
        orig_data = {
            'name': 'Alice',
            'profile': {
                'name': 'Alice profile'
            },
        }
        data1 = {
            'name': 'Alice',
            'profile': None,
        }

        validation_query = r"""
            query {
                User(
                    filter: {
                        name: {eq: "Alice"}
                    }
                ) {
                    name
                    profile {
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
                        profile: {
                            clear: true
                        }
                    },
                    filter: {
                        name: {eq: "Alice"}
                    }
                ) {
                    name
                    profile {
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
                        profile: {
                            set: {
                                filter: {
                                    name: {eq: "Alice profile"}
                                }
                            }
                        }
                    },
                    filter: {
                        name: {eq: "Alice"}
                    }
                ) {
                    name
                    profile {
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

    def test_graphql_mutation_update_link_06(self):
        # updating a single link targeting a type union
        orig_data = {
            'name': 'combo 0',
            'data': None,
        }
        data1 = {
            'name': 'combo 0',
            'data': {
                "__typename": "Setting_Type",
                "name": "perks",
            },
        }
        data2 = {
            'name': 'combo 0',
            'data': {
                "__typename": "Profile_Type",
                "name": "Bob profile",
            },
        }

        validation_query = r"""
            query {
                Combo(
                    filter: {name: {eq: "combo 0"}}
                ) {
                    name
                    data {
                        __typename
                        name
                    }
                }
            }
        """

        self.assert_graphql_query_result(validation_query, {
            "Combo": [orig_data]
        })

        self.assert_graphql_query_result(r"""
            mutation update_Combo {
                update_Combo(
                    data: {
                        data: {
                            set: {
                                filter: {name: {eq: "perks"}}
                            }
                        }
                    },
                    filter: {
                        name: {eq: "combo 0"}
                    }
                ) {
                    name
                    data {
                        __typename
                        name
                    }
                }
            }
        """, {
            "update_Combo": [data1]
        })

        self.assert_graphql_query_result(validation_query, {
            "Combo": [data1]
        })

        self.assert_graphql_query_result(r"""
            mutation update_Combo {
                update_Combo(
                    data: {
                        data: {
                            set: {
                                filter: {name: {eq: "Bob profile"}}
                            }
                        }
                    },
                    filter: {
                        name: {eq: "combo 0"}
                    }
                ) {
                    name
                    data {
                        __typename
                        name
                    }
                }
            }
        """, {
            "update_Combo": [data2]
        })

        self.assert_graphql_query_result(validation_query, {
            "Combo": [data2]
        })

        self.assert_graphql_query_result(r"""
            mutation update_Combo {
                update_Combo(
                    data: {
                        data: {
                            clear: true
                        }
                    },
                    filter: {
                        name: {eq: "combo 0"}
                    }
                ) {
                    name
                    data {
                        __typename
                        name
                    }
                }
            }
        """, {
            "update_Combo": [orig_data]
        })

        self.assert_graphql_query_result(validation_query, {
            "Combo": [orig_data]
        })

    def test_graphql_mutation_update_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot query field 'update_SettingAlias'"):
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

    def test_graphql_mutation_update_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"No update operation was specified"):
            self.graphql_query(r"""
                mutation update_User {
                    update_User(
                        data: {
                            groups: {
                            }
                        }
                    ) {
                        name
                        groups {
                            name
                        }
                    }
                }
            """)

    def test_graphql_mutation_update_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Too many update operations were specified"):
            self.graphql_query(r"""
                mutation update_User {
                    update_User(
                        data: {
                            groups: {
                                set: {
                                    filter: {
                                        name: {like: "%"}
                                    }
                                },
                                clear: true
                            }
                        }
                    ) {
                        name
                        groups {
                            name
                        }
                    }
                }
            """)

    def test_graphql_mutation_update_bad_04(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"objects provided for 'settings' are not distinct"
        ):
            self.graphql_query(r"""
                mutation update_UserGroup {
                    update_UserGroup(
                        filter: {
                            name: {eq: "basic"}
                        },
                        data: {
                            settings: {
                                set: [{
                                    filter: {name: {like: "%"}}
                                }, {
                                    filter: {name: {like: "perks"}}
                                }]
                            }
                        }
                    ) {
                        name
                        settings {
                            name
                            value
                        }
                    }
                }
            """)

    def test_graphql_mutation_update_bad_05(self):
        with self.assertRaisesRegex(
            edgedb.CardinalityViolationError,
            r"more than one object provided for 'profile'"
        ):
            self.graphql_query(r"""
                mutation update_User {
                    update_User(
                        filter: {name: {eq: "Alice"}},
                        data: {
                            profile: {
                                set: {
                                    filter: {name: {like: "%"}}
                                }
                            }
                        }
                    ) {
                        name
                        age
                        score
                        profile {
                            name
                        }
                    }
                }
            """)

    def test_graphql_mutation_update_bad_06(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"settings violates exclusivity constraint"
        ):
            self.graphql_query(r"""
                mutation update_UserGroup {
                    update_UserGroup(
                        filter: {name: {eq: "unused"}},
                        data: {
                            settings: {
                                set : {filter: {name: {like: "perks"}}}
                            }
                        }
                    ) {
                        name
                        settings {
                            name
                            value
                        }
                    }
                }
            """)

    def test_graphql_mutation_update_bad_07(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"Minimum allowed value for positive_int_t is 0"
        ):
            self.graphql_query(r"""
                mutation update_ScalarTest {
                    update_ScalarTest(
                        filter: {p_str: {eq: "Hello world"}}
                        data: {
                            p_posint: {set: -42},
                        }
                    ) {
                        p_posint
                    }
                }
            """)

    def test_graphql_mutation_update_bad_08(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"p_short_str must be no longer than 5 characters"
        ):
            self.graphql_query(r"""
                mutation update_ScalarTest {
                    update_ScalarTest(
                        filter: {p_str: {eq: "Hello world"}}
                        data: {
                            p_short_str: {set: "too long"},
                        }
                    ) {
                        p_short_str
                    }
                }
            """)

    def test_graphql_mutation_update_bad_09(self):
        with self.assertRaisesRegex(
            edgedb.NumericOutOfRangeError,
            r"Int out of range"
        ):
            self.graphql_query(r"""
                mutation update_ScalarTest {
                    update_ScalarTest(
                        filter: {p_str: {eq: "Hello world"}}
                        data: {
                            p_int32: {increment: 1234567890},
                        }
                    ) {
                        p_int32
                    }
                }
            """)

    def test_graphql_mutation_update_bad_10(self):
        with self.assertRaisesRegex(
            edgedb.AccessPolicyError,
            r"access policy violation on update of ErrorTest"
        ):
            self.graphql_query(r"""
                mutation update_ErrorTest {
                    update_ErrorTest(
                        data: {val: {set: -5}}
                    ) {
                        text
                    }
                }
        """)

    def test_graphql_mutation_update_bad_11(self):
        # We expect a rewrite rule to cause a division by 0 error.
        with self.assertRaisesRegex(
            edgedb.DivisionByZeroError,
            r"division by zero"
        ):
            self.graphql_query(r"""
                mutation update_ErrorTest {
                    update_ErrorTest(
                        data: {val: {set: 15}}
                    ) {
                        text
                    }
                }
        """)

    def test_graphql_mutation_update_bad_12(self):
        # We expect a rewrite rule to convert "" to empty set.
        with self.assertRaisesRegex(
            edgedb.MissingRequiredError,
            r"missing value for required property 'text' of object "
            r"type 'ErrorTest'"
        ):
            self.graphql_query(r"""
                mutation update_ErrorTest {
                    update_ErrorTest(
                        data: {text: {set: ""}}
                    ) {
                        text
                    }
                }
        """)

    def test_graphql_mutation_update_bad_13(self):
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"ErrorTest cannot have val equal to the length of text field"
        ):
            self.graphql_query(r"""
                mutation update_ErrorTest {
                    update_ErrorTest(
                        data: {val: {set: 7}}
                    ) {
                        text
                    }
                }
        """)

    def test_graphql_mutation_update_multiple_01(self):
        # Issue #1566
        # Test multiple mutations.
        validation_query = r"""
            query {
                Setting(filter: {name: {eq: "perks"}}) {
                    name
                    value
                }
                Profile(filter: {name: {eq: "Alice profile"}}) {
                    name
                    value
                }
            }
        """

        self.assert_graphql_query_result(r"""
            mutation multi_update {
                update_Setting(
                    filter: {name: {eq: "perks"}},
                    data: {
                        value: {set: "min"}
                    }
                ) {
                    name
                    value
                }
                update_Profile(
                    filter: {name: {eq: "Alice profile"}},
                    data: {
                        value: {set: "updated"}
                    }
                ) {
                    name
                    value
                }
            }
        """, {
            "update_Setting": [{
                'name': 'perks',
                'value': 'min',
            }],
            "update_Profile": [{
                'name': 'Alice profile',
                'value': 'updated',
            }]
        })

        self.assert_graphql_query_result(validation_query, {
            "Setting": [{
                'name': 'perks',
                'value': 'min',
            }],
            "Profile": [{
                'name': 'Alice profile',
                'value': 'updated',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation multi_update {
                update_Setting(
                    filter: {name: {eq: "perks"}},
                    data: {
                        value: {set: "full"}
                    }
                ) {
                    name
                    value
                }
                update_Profile(
                    filter: {name: {eq: "Alice profile"}},
                    data: {
                        value: {set: "special"}
                    }
                ) {
                    name
                    value
                }
            }
        """, {
            "update_Setting": [{
                'name': 'perks',
                'value': 'full',
            }],
            "update_Profile": [{
                'name': 'Alice profile',
                'value': 'special',
            }]
        })

    def test_graphql_mutation_update_multiple_02(self):
        # Issue #3470
        # Test nested update of the same type.
        data = {
            'after': 'BaseFoo02',
            'color': 'GREEN',
            'foos': [{
                'after': 'NestedFoo020',
                'color': 'RED',
            }, {
                'after': 'NestedFoo021',
                'color': 'BLUE',
            }],
        }

        validation_query = r"""
            query {
                other__Foo(filter: {after: {eq: "BaseFoo02"}}) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
                }
            }
        """

        self.graphql_query(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        after: "NestedFoo020",
                        color: RED
                    }, {
                        after: "BaseFoo02",
                        color: GREEN
                    }, {
                        after: "NestedFoo021",
                        color: BLUE
                    }]
                ) {
                    color
                }
            }
        """)

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo {
                update_other__Foo(
                    filter: {after: {eq: "BaseFoo02"}},
                    data: {
                        foos: {
                            set: [{
                                filter: {
                                    after: {like: "NestedFoo02%"},
                                }
                            }]
                        }
                    }
                ) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
                }
            }
        """, {
            "update_other__Foo": [data]
        })

        self.assert_graphql_query_result(validation_query, {
            "other__Foo": [data]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {after: {like: "%Foo02%"}},
                    order: {color: {dir: ASC}}
                ) {
                    after
                    color
                }
            }
        """, {
            "delete_other__Foo": [{
                'after': 'NestedFoo020',
                'color': 'RED',
            }, {
                'after': 'BaseFoo02',
                'color': 'GREEN',
            }, {
                'after': 'NestedFoo021',
                'color': 'BLUE',
            }]
        })

    def test_graphql_mutation_update_multiple_03(self):
        # Issue #3470
        # Test nested update of the same type.
        self.graphql_query(r"""
            mutation insert_other__Foo {
                insert_other__Foo(
                    data: [{
                        after: "NestedFoo030",
                        color: RED
                    }, {
                        after: "BaseFoo03",
                        color: GREEN
                    }, {
                        after: "NestedFoo031",
                        color: BLUE
                    }]
                ) {
                    color
                }
            }
        """)

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo {
                update_other__Foo(
                    filter: {after: {eq: "BaseFoo03"}},
                    data: {
                        foos: {
                            add: [{
                                filter: {
                                    after: {like: "NestedFoo03%"},
                                }
                            }]
                        }
                    }
                ) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
                }
            }
        """, {
            "update_other__Foo": [{
                'after': 'BaseFoo03',
                'color': 'GREEN',
                'foos': [{
                    'after': 'NestedFoo030',
                    'color': 'RED',
                }, {
                    'after': 'NestedFoo031',
                    'color': 'BLUE',
                }],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo {
                update_other__Foo(
                    filter: {after: {eq: "BaseFoo03"}},
                    data: {
                        foos: {
                            remove: [{
                                filter: {
                                    after: {eq: "NestedFoo031"},
                                }
                            }]
                        }
                    }
                ) {
                    after
                    color
                    foos(order: {color: {dir: ASC}}) {
                        after
                        color
                    }
                }
            }
        """, {
            "update_other__Foo": [{
                'after': 'BaseFoo03',
                'color': 'GREEN',
                'foos': [{
                    'after': 'NestedFoo030',
                    'color': 'RED',
                }],
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                delete_other__Foo(
                    filter: {after: {like: "%Foo03%"}},
                    order: {color: {dir: ASC}}
                ) {
                    after
                    color
                }
            }
        """, {
            "delete_other__Foo": [{
                'after': 'NestedFoo030',
                'color': 'RED',
            }, {
                'after': 'BaseFoo03',
                'color': 'GREEN',
            }, {
                'after': 'NestedFoo031',
                'color': 'BLUE',
            }]
        })

    def test_graphql_mutation_update_typename_01(self):
        # This tests the typename funcitonality after insertion.
        # Issue #5985

        self.assert_graphql_query_result(r"""
            mutation insert_other__Foo {
                __typename
                insert_other__Foo(
                    data: [{
                        select: "Update TypenameTest01",
                        color: BLUE
                    }]
                ) {
                    __typename
                    select
                    color
                }
            }
        """, {
            "__typename": 'Mutation',
            "insert_other__Foo": [{
                '__typename': 'other__Foo_Type',
                'select': 'Update TypenameTest01',
                'color': 'BLUE',
            }]
        })

        self.assert_graphql_query_result(r"""
            mutation update_other__Foo {
                __typename
                update_other__Foo(
                    filter: {select: {eq: "Update TypenameTest01"}}
                    data: {
                        color: {set: RED}
                    }
                ) {
                    __typename
                    select
                    color
                }
            }
        """, {
            "__typename": 'Mutation',
            "update_other__Foo": [{
                '__typename': 'other__Foo_Type',
                'select': 'Update TypenameTest01',
                'color': 'RED',
            }]
        })

        # clean up
        self.assert_graphql_query_result(r"""
            mutation delete_other__Foo {
                __typename
                delete_other__Foo(
                    filter: {select: {eq: "Update TypenameTest01"}}
                ) {
                    __typename
                    select
                    color
                }
            }
        """, {
            "__typename": 'Mutation',
            "delete_other__Foo": [{
                '__typename': 'other__Foo_Type',
                'select': 'Update TypenameTest01',
                'color': 'RED',
            }]
        })

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

    def test_graphql_mutation_delete_bad_01(self):
        # We expext a rewrite rule to cause a division by 0 error.
        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            r"deletion of UserGroup .+is prohibited by link target policy"
        ):
            self.graphql_query(r"""
                mutation delete_UserGroup {
                    delete_UserGroup {
                        name
                    }
                }
        """)

    def test_graphql_mutation_bigint(self):
        self.assert_graphql_query_result(r"""
            mutation insert_BigIntTest {
                insert_BigIntTest(
                    data: [{value: 1e100}]
                ) {
                    value
                }
            }
        """, {
            "insert_BigIntTest": [{"value": 10**100}]
        })
