#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

from edb.testbase import server as tb
from edgedb import errors


class TestEdgeQLNetSchema(tb.DDLTestCase):
    '''Tests for net schema.'''

    async def test_edgeql_net_request_state_enum_01(self):
        await self.assert_query_result(
            '''
            select {
                net::RequestState.Pending,
                net::RequestState.InProgress,
                net::RequestState.Completed,
                net::RequestState.Failed,
            };
            ''',
            ['Pending', 'InProgress', 'Completed', 'Failed'],
        )

    async def test_edgeql_net_request_state_failure_kind_01(self):
        await self.assert_query_result(
            '''
            select {
                net::RequestFailureKind.NetworkError,
                net::RequestFailureKind.Timeout,
            };
            ''',
            ['NetworkError', 'Timeout'],
        )

    async def test_edgeql_net_http_method_01(self):
        await self.assert_query_result(
            '''
            select {
                net::http::Method.`GET`,
                net::http::Method.POST,
                net::http::Method.PUT,
                net::http::Method.`DELETE`,
                net::http::Method.HEAD,
                net::http::Method.OPTIONS,
                net::http::Method.PATCH,
            };
            ''',
            ['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS', 'PATCH'],
        )

    async def test_edgeql_net_http_request_response_01(self):
        await self.assert_query_result(
            '''
            with
                nh as module std::net::http,
                response := (
                    insert nh::Response {
                        created_at := datetime_of_statement(),
                        status := 200,
                        headers := [("Content-Type", "text/plain")],
                        body := to_bytes("hello world"),
                    }
                ),
                request := (
                    insert nh::ScheduledRequest {
                        created_at := datetime_of_statement(),
                        updated_at := datetime_of_statement(),
                        state := std::net::RequestState.Completed,

                        url := "http://example.com",
                        method := nh::Method.`GET`,
                        headers := [("Accept", "text/plain")],

                        response := response,
                    }
                ),
            select request {
                state,
                url,
                method,

                response: {
                    status,
                    body_decoded := to_str(.body),
                }
            };
            ''',
            [{
                'state': 'Completed',
                'url': 'http://example.com',
                'method': 'GET',
                'response': {
                    'status': 200,
                    'body_decoded': 'hello world',
                },
            }],
        )

    async def test_edgeql_net_http_schedule_request_01(self):
        await self.assert_query_result(
            '''
            with
                nh as module std::net::http,
                request := (
                    nh::schedule_request(
                        "http://example.com",
                        headers := [("Accept", "text/plain")],
                    )
                ),
            select request {
                state,
                url,
                method,
                headers,

                created_at_is_datetime := (.created_at is std::datetime),
                response_is_empty := (not exists .response),
            };
            ''',
            [{
                'state': 'Pending',
                'url': 'http://example.com',
                'method': 'GET',
                'headers': [{'name': 'Accept', 'value': 'text/plain'}],
                'created_at_is_datetime': True,
                'response_is_empty': True,
            }],
        )

    async def test_edgeql_net_http_schedule_request_empty_method_01(self):
        with self.assertRaisesRegex(errors.QueryError, "possibly an empty set"):
            await self.con.query(
                '''
                with
                    nh as module std::net::http,
                    request := (
                        nh::schedule_request(
                            "http://example.com",
                            method := <nh::Method>{},
                            headers := [("Accept", "text/plain")],
                        )
                    ),
                select request;
                ''',
            )
