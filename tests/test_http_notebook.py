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

from edb.testbase import http as tb


class TestHttpNotebook(tb.BaseHttpTest, tb.server.QueryTestCase):

    # EdgeQL/HTTP queries cannot run in a transaction
    ISOLATED_METHODS = False

    @classmethod
    def get_port_proto(cls):
        return 'notebook'

    def run_queries(self, queries: List[str]):
        req_data = {
            'queries': queries
        }

        req = urllib.request.Request(self.http_addr, method='POST')
        req.add_header('Content-Type', 'application/json')
        response = urllib.request.urlopen(
            req, json.dumps(req_data).encode())
        resp_data = json.loads(response.read())
        return resp_data

    def test_http_notebook_01(self):
        results = self.run_queries([
            'SELECT 1',
            'SELECT "AAAA"',
        ])

        self.assertEqual(
            results,
            {
                'kind': 'results',
                'results': [
                    {
                        'kind': 'data',
                        'data': [
                            'AAAAAAAAAAAAAAAAAAABBQ==',
                            'AgAAAAAAAAAAAAAAAAAAAQU=',
                            'RAAAABIAAQAAAAgAAAAAAAAAAQ=='
                        ]
                    },
                    {
                        'kind': 'data',
                        'data': [
                            'AAAAAAAAAAAAAAAAAAABAQ==',
                            'AgAAAAAAAAAAAAAAAAAAAQE=',
                            'RAAAAA4AAQAAAARBQUFB'
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

        self.assertEqual(
            results,
            {
                'kind': 'results',
                'results': [
                    {
                        'kind': 'data',
                        'data': [
                            'AAAAAAAAAAAAAAAAAAABBQ==',
                            'AgAAAAAAAAAAAAAAAAAAAQU=',
                            'RAAAABIAAQAAAAgAAAAAAAAAAQ=='
                        ]
                    },
                    {
                        'kind': 'error',
                        'error': [
                            'QueryError',
                            "operator '*' cannot be applied to operands "
                            "of type 'std::str' and 'std::int64'",
                            {
                                '65523': '1',
                                '65524': '8',
                                '65521': '7',
                                '65522': '17',
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
                            {}
                        ]
                    }
                ]
            }
        )
