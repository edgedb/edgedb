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

import edgedb

from edb.testbase import http as tb


class TestHttpEdgeQL(tb.EdgeQLTestCase):

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'graphql.esdl')

    SCHEMA_OTHER = os.path.join(os.path.dirname(__file__), 'schemas',
                                'graphql_other.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'graphql_setup.edgeql')

    # EdgeQL/HTTP queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    def test_http_edgeql_proto_errors_01(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(
                con, {}, path='non-existant')

            self.assertEqual(status, 404)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'Unknown path', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant2')

    def test_http_edgeql_proto_errors_02(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(con, {})

            self.assertEqual(status, 400)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'query is missing', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant')

    def test_http_edgeql_proto_errors_03(self):
        with self.http_con() as con:
            con.send(b'blah\r\n\r\n\r\n\r\n')
            data, headers, status = self.http_con_request(
                con, {'query': 'blah', 'variables': 'bazz'})

            self.assertEqual(status, 400)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'invalid HTTP method', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant')

    def test_http_edgeql_query_01(self):
        for _ in range(10):  # repeat to test prepared pgcon statements
            for use_http_post in [True, False]:
                self.assert_edgeql_query_result(
                    r"""
                        SELECT Setting {
                            name,
                            value
                        }
                        ORDER BY .value ASC;
                    """,
                    [
                        {'name': 'template', 'value': 'blue'},
                        {'name': 'perks', 'value': 'full'},
                        {'name': 'template', 'value': 'none'},
                    ],
                    use_http_post=use_http_post
                )

    def test_http_edgeql_query_02(self):
        for use_http_post in [True, False]:
            self.assert_edgeql_query_result(
                r"""
                    SELECT Setting {
                        name,
                        value
                    }
                    FILTER .name = <str>$name;
                """,
                [
                    {'name': 'perks', 'value': 'full'},
                ],
                variables={'name': 'perks'},
                use_http_post=use_http_post
            )

    def test_http_edgeql_query_03(self):
        self.assert_edgeql_query_result(
            r"""
                SELECT User {
                    name,
                    age,
                    groups: { name }
                }
                FILTER .name = <str>$name AND .age = <int64>$age;
            """,
            [
                {'name': 'Bob', 'age': 21, 'groups': []},
            ],
            variables=dict(name='Bob', age=21)
        )

    def test_http_edgeql_query_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'no value for the \$name query parameter'):
            self.edgeql_query(
                r"""
                    SELECT Setting {
                        name,
                        value
                    }
                    FILTER .name = <str>$name;
                """
            )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter \$name is required'):
            self.edgeql_query(
                r"""
                    SELECT Setting {
                        name,
                        value
                    }
                    FILTER .name = <str>$name;
                """,
                variables={'name': None})

    def test_http_edgeql_query_05(self):
        with self.assertRaisesRegex(edgedb.InvalidReferenceError,
                                    r'UNRECOGNIZABLE'):
            self.edgeql_query(
                r"""
                    SELECT UNRECOGNIZABLE {
                        value
                    };
                """
            )

    def test_http_edgeql_query_06(self):
        queries = [
            'START TRANSACTION;',
            'SET ALIAS blah AS MODULE std;',
            'CREATE TYPE default::Tmp { CREATE PROPERTY tmp -> std::str; };',
        ]

        for query in queries:
            with self.assertRaisesRegex(
                edgedb.ProtocolError,
                # can fail on transaction commands or on session configuration
                'cannot execute.*',
            ):
                self.edgeql_query(query)

    def test_http_edgeql_query_07(self):
        self.assert_edgeql_query_result(
            r"""
                SELECT Setting {
                    name,
                    value
                }
                FILTER .name = "NON EXISTENT";
            """,
            [],
        )

    def test_http_edgeql_query_08(self):
        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    r'expected one statement, got 2'):
            self.edgeql_query(
                r"""
                    SELECT 1;
                    SELECT 2;
                """)

    def test_http_edgeql_query_09(self):
        self.assert_edgeql_query_result(
            r"""
                SELECT <bigint>$number;
            """,
            [123456789123456789123456789],
            variables={'number': 123456789123456789123456789}
        )

    def test_http_edgeql_query_10(self):
        self.assert_edgeql_query_result(
            r'''SELECT (INTROSPECT TYPEOF <int64>$x).name;''',
            ['std::int64'],
            variables={'x': 7},
        )

    def test_http_edgeql_query_11(self):
        self.assert_edgeql_query_result(
            r'''SELECT <str>$x ++ (INTROSPECT TYPEOF <int64>$y).name;''',
            ['xstd::int64'],
            variables={'x': 'x', 'y': 7},
        )

    def test_http_edgeql_query_12(self):
        self.assert_edgeql_query_result(
            r'''SELECT <str>$x''',
            ['xx'],
            variables={'x': 'xx'},
        )

        self.assert_edgeql_query_result(
            r'''SELECT <REQUIRED str>$x''',
            ['yy'],
            variables={'x': 'yy'},
        )

        self.assert_edgeql_query_result(
            r'''SELECT <OPTIONAL str>$x ?? '-default-' ''',
            ['-default-'],
            variables={'x': None},
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter \$x is required'):
            self.edgeql_query(
                r'''SELECT <REQUIRED str>$x ?? '-default' ''',
                variables={'x': None},
            )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter \$x is required'):
            self.edgeql_query(
                r'''SELECT <str>$x ?? '-default' ''',
                variables={'x': None},
            )
