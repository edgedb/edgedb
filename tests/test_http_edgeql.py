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
import urllib
import json
import decimal

import edgedb

from edb.testbase import http as tb


class TestHttpEdgeQL(tb.EdgeQLTestCase):

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'graphql.esdl')

    SCHEMA_OTHER = os.path.join(os.path.dirname(__file__), 'schemas',
                                'graphql_other.esdl')

    SCHEMA_OTHER_DEEP = os.path.join(os.path.dirname(__file__), 'schemas',
                                     'graphql_schema_other_deep.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'graphql_setup.edgeql')

    # EdgeQL/HTTP queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    def test_http_edgeql_proto_errors_01(self):
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

    def test_http_edgeql_proto_errors_02(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(
                con,
                {},
                headers={
                    'Authorization': self.make_auth_header(),
                },
            )

            self.assertEqual(status, 400)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'query is missing', data)

            with self.assertRaises(OSError):
                self.http_con_request(con, {}, path='non-existant')

    def test_http_edgeql_proto_errors_03(self):
        with self.http_con() as con:
            con.send(b'blah\r\n\r\n\r\n\r\n')
            data, headers, status = self.http_con_request(
                con,
                {'query': 'blah', 'variables': 'bazz'},
                headers={
                    'Authorization': self.make_auth_header(),
                },
            )

            self.assertEqual(status, 400)
            self.assertEqual(headers['connection'], 'close')
            self.assertIn(b'HttpParserInvalidMethodError', data)

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
                r'parameter \$name is required'):
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
        self.assert_edgeql_query_result(
            r"""
                SELECT 1;
                SELECT 2;
            """,
            [2],
        )

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

    def test_http_edgeql_query_13(self):
        self.assert_edgeql_query_result(
            r'''select (<array<int64>>$foo, <tuple<int64, str>>$bar)''',
            [[[1, 2, 3], [1, 'test']]],
            variables=dict(
                foo=[1, 2, 3],
                bar=(1, 'test'),
            )
        )

    def test_http_edgeql_query_14(self):
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'Minimum allowed value for positive_int_t is 0'):
            self.edgeql_query(
                r'''SELECT <positive_int_t>-1''',
            )

    def test_http_edgeql_query_globals_01(self):
        Q = r'''select GlobalTest { gstr, garray, gid, gdef, gdef2 }'''

        for use_http_post in [True, False]:
            self.assert_edgeql_query_result(
                Q,
                [{'gstr': 'WOO',
                  'gid': '84ed3d8b-5eb2-4d31-9e1e-efb66180445c', 'gdef': '',
                  'gdef2': None, 'garray': ['x', 'y', 'z']}],
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

            self.assert_edgeql_query_result(
                Q,
                [{'gdef': 'x', 'gdef2': 'x'}],
                use_http_post=use_http_post,
                globals={
                    'default::test_global_def': 'x',
                    'default::test_global_def2': 'x',
                },
            )

            self.assert_edgeql_query_result(
                Q,
                [{'gstr': None, 'garray': None, 'gid': None,
                  'gdef': '', 'gdef2': ''}],
                use_http_post=use_http_post,
            )

    def test_http_edgeql_query_globals_02(self):
        Q = r'''select (global test_global_str) ++ <str>$test'''

        for use_http_post in [True, False]:
            self.assert_edgeql_query_result(
                Q,
                ['foo!'],
                variables={'test': '!'},
                globals={'default::test_global_str': 'foo'},
                use_http_post=use_http_post,
            )

    def test_http_edgeql_query_globals_03(self):
        Q = r'''select get_glob()'''

        for use_http_post in [True, False]:
            self.assert_edgeql_query_result(
                Q,
                ['foo'],
                globals={'default::test_global_str': 'foo'},
                use_http_post=use_http_post,
            )

    def test_http_edgeql_query_globals_04(self):
        Q = r'''select get_glob()'''

        for use_http_post in [True, False]:
            self.assert_edgeql_query_result(
                Q,
                [],
                use_http_post=use_http_post,
            )

    def test_http_edgeql_query_func_01(self):
        Q = r'''select id_func('foo')'''

        for use_http_post in [True, False]:
            self.assert_edgeql_query_result(
                Q,
                ['foo'],
                use_http_post=use_http_post,
            )

    def test_http_edgeql_query_func_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter \$x is required'):
            self.edgeql_query(
                r'''select modifying_noop(<required str>$x)''',
                variables=dict(x=None),
            )

    def test_http_edgeql_query_func_03(self):
        self.assert_edgeql_query_result(
            r'''select modifying_noop(<required str>$x)''',
            ['foo'],
            variables=dict(x='foo'),
        )

    def test_http_edgeql_query_func_04(self):
        # JSON parameter can be null, so <str>$x is AT_MOST_ONE in this case
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly an empty set passed as non-optional argument '
                r'into modifying function'):
            self.edgeql_query(
                r'''select modifying_noop(<str>$x)''',
                variables=dict(x='foo'),
            )

    def test_http_edgeql_query_duration_01(self):
        Q = r"""select <duration>'15m' < <duration>'1h'"""

        for use_http_post in [True, False]:
            self.assert_edgeql_query_result(
                Q,
                [True],
                use_http_post=use_http_post,
            )

    def test_http_edgeql_decimal_params_01(self):
        req_data = b'''{
            "query": "select <array<decimal>>$test",
            "variables": {
                "test": [1234567890123456789.01234567890123456789]
            }
        }'''

        req = urllib.request.Request(self.http_addr, method='POST')
        req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', self.make_auth_header())
        response = urllib.request.urlopen(
            req, req_data, context=self.tls_context
        )
        resp_data = json.loads(response.read(), parse_float=decimal.Decimal)

        self.assert_data_shape(resp_data, {
            'data': [
                [decimal.Decimal('1234567890123456789.01234567890123456789')]
            ]
        })

    async def test_http_edgeql_cors(self):
        try:
            req = urllib.request.Request(self.http_addr, method='OPTIONS')
            req.add_header('Origin', 'https://example.edgedb.com')
            response = urllib.request.urlopen(
                req, context=self.tls_context
            )
            self.assertNotIn('Access-Control-Allow-Origin', response.headers)

            _, response = self.edgeql_query(
                r"""
                    SELECT User {
                        name,
                        age,
                        groups: { name }
                    }
                    FILTER .name = <str>$name AND .age = <int64>$age;
                """,
                variables=dict(name='Bob', age=21),
                origin='https://example.edgedb.com'
            )
            self.assertNotIn('Access-Control-Allow-Origin', response.headers)

            await self.con.execute(
                'configure current database '
                'set cors_allow_origins := {"https://example.edgedb.com"}')
            await self._wait_for_db_config('cors_allow_origins')

            req = urllib.request.Request(self.http_addr, method='OPTIONS')
            req.add_header('Origin', 'https://other.edgedb.com')
            response = urllib.request.urlopen(
                req, context=self.tls_context
            )
            self.assertNotIn('Access-Control-Allow-Origin', response.headers)

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
            self.assertIn(
                'Authorization', headers['Access-Control-Allow-Headers']
            )
            self.assertIn(
                'X-EdgeDB-User', headers['Access-Control-Allow-Headers']
            )
            self.assertEqual(
                headers['Access-Control-Allow-Credentials'], 'true'
            )

            await self.con.execute(
                'configure current database '
                'set cors_allow_origins := {"https://other.edgedb.com"}')
            await self._wait_for_db_config(
                'cors_allow_origins', value=["https://other.edgedb.com"])

            req = urllib.request.Request(self.http_addr, method='OPTIONS')
            req.add_header('Origin', 'https://example.edgedb.com')
            response = urllib.request.urlopen(
                req, context=self.tls_context
            )
            self.assertNotIn('Access-Control-Allow-Origin', response.headers)

            req = urllib.request.Request(self.http_addr, method='OPTIONS')
            req.add_header('Origin', 'https://other.edgedb.com')
            response = urllib.request.urlopen(
                req, context=self.tls_context
            )
            self.assertIn('Access-Control-Allow-Origin', response.headers)

            _, response = self.edgeql_query(
                r"""
                    SELECT User {
                        name,
                        age,
                        groups: { name }
                    }
                    FILTER .name = <str>$name AND .age = <int64>$age;
                """,
                variables=dict(name='Bob', age=21),
                origin='https://other.edgedb.com'
            )
            self.assertIn('Access-Control-Allow-Origin', response.headers)
        finally:
            await self.con.execute(
                'configure current database reset cors_allow_origins')
