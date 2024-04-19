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


from typing import List

import json
import urllib

from edb.testbase import http as tb


class TestHttpNotebook(tb.BaseHttpExtensionTest):

    # EdgeQL/HTTP queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    EXTENSIONS = ['notebook']

    @classmethod
    def get_extension_path(cls):
        return 'notebook'

    def run_queries(self, queries: List[str]):
        req_data = {
            'queries': queries
        }

        req = urllib.request.Request(
            self.http_addr, method='POST')  # type: ignore
        req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', self.make_auth_header())
        response = urllib.request.urlopen(
            req, json.dumps(req_data).encode(), context=self.tls_context
        )

        self.assertIsNotNone(response.headers['EdgeDB-Protocol-Version'])

        resp_data = json.loads(response.read())
        return resp_data

    def test_http_notebook_01(self):
        results = self.run_queries([
            'SELECT 1',
            'SELECT "AAAA"',
        ])

        self.assert_data_shape(
            results,
            {
                'kind': 'results',
                'results': [
                    {
                        'kind': 'data',
                        'data': [
                            str,
                            str,
                            'RAAAABIAAQAAAAgAAAAAAAAAAQ==',
                            str,
                        ]
                    },
                    {
                        'kind': 'data',
                        'data': [
                            str,
                            str,
                            'RAAAAA4AAQAAAARBQUFB',
                            str,
                        ]
                    },
                ]
            }
        )

    def test_http_notebook_02(self):
        results = self.run_queries([
            'SELECT 1',
            'SELECT "AAAA" * 1',
            'SELECT 55',
        ])

        self.assert_data_shape(
            results,
            {
                'kind': 'results',
                'results': [
                    {
                        'kind': 'data',
                        'data': [
                            str,
                            str,
                            'RAAAABIAAQAAAAgAAAAAAAAAAQ==',
                            str,
                        ]
                    },
                    {
                        'kind': 'error',
                        'error': [
                            'InvalidTypeError',
                            "operator '*' cannot be applied to operands "
                            "of type 'std::str' and 'std::int64'",
                            {
                                '65523': '1',
                                '65524': '8',
                                '65521': '7',
                                '65522': '17',
                                '65525': '7',
                                '65526': '1',
                                '65527': '18',
                                '65528': '17',
                                '65529': '7',
                                '65530': '17',
                                '1': 'Consider using an explicit type '
                                     'cast or a conversion function.'
                            }
                        ]
                    }
                ]
            }
        )

    def test_http_notebook_03(self):
        results = self.run_queries([
            'SELECT "a',
        ])

        self.assertEqual(
            results,
            {
                'kind': 'results',
                'results': [
                    {
                        'kind': 'error',
                        'error': [
                            'EdgeQLSyntaxError',
                            'unterminated string, quoted by `"`',
                            {
                                '65521': '7',
                                '65522': '7',
                                '65523': '1',
                                '65524': '8',
                            }
                        ]
                    }
                ]
            }
        )

    def test_http_notebook_04(self):
        req = urllib.request.Request(self.http_addr + '/status',
                                     method='GET')
        req.add_header('Authorization', self.make_auth_header())
        response = urllib.request.urlopen(
            req,
            context=self.tls_context,
        )
        resp_data = json.loads(response.read())
        self.assertEqual(resp_data, {'kind': 'status', 'status': 'OK'})

    def test_http_notebook_05(self):
        results = self.run_queries([
            'SELECT 1',
            'SELECT [1][2]',
            'SELECT 2'
        ])

        self.assert_data_shape(
            results,
            {
                'kind': 'results',
                'results': [
                    {
                        'kind': 'data',
                        'data': [
                            str,
                            str,
                            'RAAAABIAAQAAAAgAAAAAAAAAAQ==',
                            str,
                        ]
                    },
                    {
                        'kind': 'error',
                        'error': [
                            'InvalidValueError',
                            'array index 2 is out of bounds',
                            {},
                        ]
                    }
                ]
            }
        )

    def test_http_notebook_06(self):
        results = self.run_queries([
            'SELECT {protocol := "notebook"}'
        ])

        self.assertEqual(results['kind'], 'results')
        self.assertEqual(results['results'][0]['kind'], 'data')

    def test_http_notebook_07(self):
        results = self.run_queries([
            '''
            create function add_sub(variadic vals: int64) -> int64
              using (
                select sum(
                  (
                    for tup in enumerate(array_unpack(vals))
                    union (1 if tup.0 % 2 = 0 else -1) * tup.1
                  )
                )
              );
            ''',
            'select add_sub(1, 2, 5, 3);'
        ])

        self.assertNotIn('error', results['results'][0])

        self.assert_data_shape(
            results['results'][1],
            {
                'kind': 'data',
                'data': [
                    str,
                    str,
                    'RAAAABIAAQAAAAgAAAAAAAAAAQ==',
                    str,
                ]
            },
        )

    def test_http_notebook_08(self):
        results = self.run_queries([
            'create global foo -> int64',
            'set global foo := 1',
            'select global foo',
        ])

        self.assertNotIn('error', results['results'][0])
        self.assertNotIn('error', results['results'][1])

        self.assert_data_shape(
            results['results'][2],
            {
                'kind': 'data',
                'data': [
                    str,
                    str,
                    'RAAAABIAAQAAAAgAAAAAAAAAAQ==',
                    str,
                ]
            },
        )

        # Run create global again... to make sure that changes are not
        # committed
        results = self.run_queries([
            'create global foo -> int64',
        ])
        self.assertNotIn('error', results['results'][0])

    def test_http_notebook_09(self):
        results = self.run_queries([
            '''
            select <duration>'15m' < <duration>'1h';
            ''',
        ])

        self.assertNotIn('error', results['results'][0])

    async def test_http_notebook_cors(self):
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
        self.assertIn('Authorization', headers['Access-Control-Allow-Headers'])
        self.assertIn(
            'EdgeDB-Protocol-Version',
            headers['Access-Control-Expose-Headers']
        )

        req = urllib.request.Request(self.http_addr, method='POST')
        req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', self.make_auth_header())
        req.add_header('Origin', 'https://example.edgedb.com')
        response = urllib.request.urlopen(
            req,
            json.dumps({'queries': ['select 123']}).encode(),
            context=self.tls_context
        )
        headers = response.headers
        self.assertIn('Access-Control-Allow-Origin', headers)
        self.assertIn('Access-Control-Expose-Headers', headers)
        self.assertIn(
            'EdgeDB-Protocol-Version',
            headers['Access-Control-Expose-Headers']
        )
