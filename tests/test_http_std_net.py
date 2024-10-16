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


import typing
import json

from edb.testbase import http as tb
import edb.testbase.server as server


class StdNetTestCase(server.QueryTestCase):
    mock_server: typing.Optional[tb.MockHttpServer] = None
    base_url: str

    def setUp(self):
        self.mock_server = tb.MockHttpServer()
        self.mock_server.start()
        self.base_url = self.mock_server.get_base_url().rstrip("/")

    def tearDown(self):
        if self.mock_server is not None:
            self.mock_server.stop()
        self.mock_server = None

    async def _wait_for_request_completion(self, request_id: str):
        async for tr in self.try_until_succeeds(
            delay=2, timeout=120, ignore=(AssertionError,)
        ):
            async with tr:
                updated_result = await self.con.query_single(
                    """
                    with
                        nh as module std::net::http,
                        net as module std::net,
                        request := (
                            select nh::ScheduledRequest
                            filter .id = <uuid>$id
                        )
                    select request {
                        id,
                        state,
                        failure,
                    };
                    """,
                    id=request_id,
                )
                state = str(updated_result.state)
                self.assertNotEqual(state, 'Pending')
                self.assertNotEqual(state, 'InProgress')

    async def _get_final_request_result(self, request_id: str):
        return await self.con.query_single(
            """
            with
                nh as module std::net::http,
                net as module std::net,
                request := (
                    select nh::ScheduledRequest
                    filter .id = <uuid>$id
                )
            select request {
                id,
                state,
                failure,
                response: {
                    status,
                    body,
                    headers
                }
            };
            """,
            id=request_id,
        )

    async def test_http_std_net_con_schedule_request_get_01(self):
        assert self.mock_server is not None

        example_request = (
            'GET',
            self.base_url,
            '/test-get-01',
        )
        url = f"{example_request[1]}{example_request[2]}"
        self.mock_server.register_route_handler(*example_request)(
            (
                json.dumps(
                    {
                        "message": "Hello, world!",
                    }
                ),
                200,
                {"Content-Type": "application/json"},
            )
        )

        result = await self.con.query_single(
            """
            with
                nh as module std::net::http,
                net as module std::net,
                url := <str>$url,
                request := (
                    insert nh::ScheduledRequest {
                        created_at := datetime_of_statement(),
                        state := std::net::RequestState.Pending,

                        url := url,
                        method := nh::Method.`GET`,
                        headers := [
                            ("Accept", "application/json"),
                            ("x-test-header", "test-value"),
                        ],
                    }
                )
            select request {*};
            """,
            url=url,
        )

        requests_for_example = None
        async for tr in self.try_until_succeeds(
            delay=2, timeout=120, ignore=(KeyError,)
        ):
            async with tr:
                requests_for_example = self.mock_server.requests[
                    example_request
                ]

        assert requests_for_example is not None
        self.assertEqual(len(requests_for_example), 1)
        headers = list(requests_for_example[0].headers.items())
        self.assertIn(("accept", "application/json"), headers)
        self.assertIn(("x-test-header", "test-value"), headers)

        # Wait for the request to complete
        await self._wait_for_request_completion(result.id)

        # Check the table as well
        table_result = await self._get_final_request_result(result.id)
        self.assertEqual(str(table_result.state), 'Completed')
        self.assertIsNone(table_result.failure)
        self.assertEqual(table_result.response.status, 200)
        self.assertEqual(
            json.loads(table_result.response.body), {"message": "Hello, world!"}
        )
        self.assertIn(
            ("content-type", "application/json"), table_result.response.headers
        )

    async def test_http_std_net_con_schedule_request_post_01(self):
        assert self.mock_server is not None

        example_request = (
            'POST',
            self.base_url,
            '/test-post-01',
        )
        url = f"{example_request[1]}{example_request[2]}"
        self.mock_server.register_route_handler(*example_request)(
            (
                json.dumps(
                    {
                        "message": "Hello, world!",
                    }
                ),
                200,
                {"Content-Type": "application/json"},
            )
        )

        result = await self.con.query_single(
            """
            with
                nh as module std::net::http,
                net as module std::net,
                url := <str>$url,
                body := <bytes>$body,
                request := (
                    nh::schedule_request(
                        url,
                        method := nh::Method.POST,
                        headers := [
                            ("Accept", "application/json"),
                            ("x-test-header", "test-value"),
                        ],
                        body := body,
                    )
                )
            select request {*};
            """,
            url=url,
            body=b"Hello, world!",
        )

        requests_for_example = None
        async for tr in self.try_until_succeeds(
            delay=2, timeout=120, ignore=(KeyError,)
        ):
            async with tr:
                requests_for_example = self.mock_server.requests[
                    example_request
                ]

        assert requests_for_example is not None
        self.assertEqual(len(requests_for_example), 1)
        headers = list(requests_for_example[0].headers.items())
        self.assertIn(("accept", "application/json"), headers)
        self.assertIn(("x-test-header", "test-value"), headers)
        self.assertEqual(requests_for_example[0].body, "Hello, world!")

        # Wait for the request to complete
        await self._wait_for_request_completion(result.id)

        # Check the final result
        table_result = await self._get_final_request_result(result.id)
        self.assertEqual(str(table_result.state), 'Completed')
        self.assertIsNone(table_result.failure)
        self.assertEqual(table_result.response.status, 200)
        self.assertEqual(
            json.loads(table_result.response.body), {"message": "Hello, world!"}
        )
        self.assertIn(
            ("content-type", "application/json"), table_result.response.headers
        )

    async def test_http_std_net_con_schedule_request_bad_address(self):
        # Test a request to a known-bad address
        bad_url = "http://256.256.256.256"

        result = await self.con.query_single(
            """
            with
                nh as module std::net::http,
                net as module std::net,
                url := <str>$url,
                request := (
                    nh::schedule_request(
                        url,
                        method := nh::Method.`GET`
                    )
                )
            select request {
                id,
                state,
                failure,
            };
            """,
            url=bad_url,
        )

        await self._wait_for_request_completion(result.id)
        table_result = await self._get_final_request_result(result.id)
        self.assertEqual(str(table_result.state), 'Failed')
        self.assertIsNotNone(table_result.failure)
        self.assertEqual(str(table_result.failure.kind), 'NetworkError')
        self.assertIsNone(table_result.response)
