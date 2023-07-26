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


from typing import *

import json
import urllib
import base64

from edb.testbase import http as tb


class TestHttpNotebook(tb.BaseHttpExtensionTest):

    # EdgeQL/HTTP queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    @classmethod
    def get_extension_name(cls):
        return 'notebook'

    def run_queries(
        self,
        queries: List[str],
        params: Optional[str] = None,
        *,
        inject_typenames: Optional[bool] = None,
        json_output: Optional[bool] = None
    ):
        req_data: dict[str, Any] = {
            'queries': queries
        }

        if params is not None:
            req_data['params'] = params
        if inject_typenames is not None:
            req_data['inject_typenames'] = inject_typenames
        if json_output is not None:
            req_data['json_output'] = json_output

        req = urllib.request.Request(
            self.http_addr, method='POST')  # type: ignore
        req.add_header('Content-Type', 'application/json')
        response = urllib.request.urlopen(
            req, json.dumps(req_data).encode(), context=self.tls_context
        )

        self.assertIsNotNone(response.headers['EdgeDB-Protocol-Version'])

        resp_data = json.loads(response.read())
        return resp_data

    def parse_query(self, query: str):
        req = urllib.request.Request(
            self.http_addr + '/parse', method='POST')  # type: ignore
        req.add_header('Content-Type', 'application/json')
        response = urllib.request.urlopen(
            req, json.dumps({'query': query}).encode(),
            context=self.tls_context
        )

        resp_data = json.loads(response.read())

        if resp_data['kind'] != 'error':
            self.assertIsNotNone(response.headers['EdgeDB-Protocol-Version'])

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
        response = urllib.request.urlopen(req, context=self.tls_context)
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
                            'Error', 'array index 2 is out of bounds', {}
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

    def test_http_notebook_10(self):
        # Check that if no 'params' field is sent an error is still thrown
        # when query contains params, to maintain behaviour of edgeql tutorial
        results = self.run_queries(['select <str>$test'])

        self.assert_data_shape(results, {
            'kind': 'results',
            'results': [{
                'kind': 'error',
                'error': [
                    'QueryError',
                    'cannot use query parameters in tutorial',
                    {}
                ]
            }]
        })

    def test_http_notebook_11(self):
        results = self.run_queries(['select <str>$test'],
                                   'AAAAAQAAAAAAAAAIdGVzdCBzdHI=')

        self.assert_data_shape(results, {
            'kind': 'results',
            'results': [{
                'kind': 'data',
                'data': [
                    str,
                    str,
                    'RAAAABIAAQAAAAh0ZXN0IHN0cg==',
                    str
                ]
            }]
        })

    def test_http_notebook_12(self):
        result = self.parse_query('select <array<int32>>$test_param')

        self.assert_data_shape(result, {
            'kind': 'parse_result',
            'in_type_id': str,
            'in_type': str
        })

        error_result = self.parse_query('select $invalid')

        self.assert_data_shape(error_result, {
            'kind': 'error',
            'error': {
                'type': 'QueryError',
                'message': 'missing a type cast before the parameter'
            }
        })

    def test_http_notebook_13(self):
        results = []
        for inject_typenames, json_output in [
            (None, None),
            (True, None),
            (False, None),
            (None, True),
            (None, False),
            (True, True),
            (True, False),
            (False, True),
            (False, False)
        ]:
            result = self.run_queries(['''
                select {
                    some := 'free shape'
                }
            '''], inject_typenames=inject_typenames, json_output=json_output)

            self.assert_data_shape(result, {
                'kind': 'results',
                'results': [{
                    'kind': 'data',
                    'data': [str, str, str, str]
                }]
            })

            results.append(result)

        # Ideally we'd check the decoded data has/hasn't got the injected
        # typeids/names but currently we can't decode the result, so just
        # check the expected number of bytes were returned
        self.assertEqual(
            [
                len(base64.b64decode(result['results'][0]['data'][2]))
                for result in results
            ],
            [80, 80, 33, 36, 80, 36, 80, 36, 33]
        )

        # JSON results should be encoded as str which has a stable type id
        self.assertEqual(
            [
                result['results'][0]['data'][0] == 'AAAAAAAAAAAAAAAAAAABAQ=='
                for result in results
            ],
            [False, False, False, True, False, True, False, True, False]
        )
