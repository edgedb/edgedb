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
import edgedb

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

    async def test_http_std_net_con_send_request(self):
        assert self.mock_server is not None

        example_request = (
            'GET',
            self.base_url,
            '/test',
        )
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

        await self.con.query(
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
                            ("Accept", "text/plain"),
                            ("x-test-header", "test-value"),
                        ],
                    }
                )
            select request {*};
            """,
            url=f"{self.base_url}/test",
        )

        async for tr in self.try_until_succeeds(
            delay=2, timeout=120, ignore=(edgedb.CardinalityViolationError,)
        ):
            async with tr:
                await self.con.query(
                    """
                    with
                        url := <str>$url,
                        request := assert_exists((
                            select std::net::http::ScheduledRequest
                            filter .url = url
                            and .state in {
                                std::net::RequestState.Completed,
                                std::net::RequestState.Failed,
                            }
                            limit 1
                        ))
                    select request {*};
                    """,
                    url=f"{self.base_url}/test",
                )

        requests_for_example = self.mock_server.requests[example_request]
        self.assertEqual(len(requests_for_example), 1)
        headers = list(requests_for_example[0]["headers"].items())
        self.assertIn(("accept", "text/plain"), headers)
        self.assertIn(("x-test-header", "test-value"), headers)
