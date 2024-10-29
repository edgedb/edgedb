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

import asyncio
import json
import random

from edb.server import http
from edb.testbase import http as tb


class HttpTest(tb.BaseHttpTest):
    def setUp(self):
        super().setUp()
        self.mock_server = tb.MockHttpServer()
        self.mock_server.start()
        self.base_url = self.mock_server.get_base_url().rstrip("/")

    def tearDown(self):
        if self.mock_server is not None:
            self.mock_server.stop()
        self.mock_server = None
        super().tearDown()

    async def test_get(self):
        with http.HttpClient(100) as client:
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

            result = await client.get(url)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json(), {"message": "Hello, world!"})

    async def test_post(self):
        with http.HttpClient(100) as client:
            example_request = (
                'POST',
                self.base_url,
                '/test-post-01',
            )
            url = f"{example_request[1]}{example_request[2]}"
            self.mock_server.register_route_handler(*example_request)(
                lambda _handler, request: (
                    request.body,
                    200,
                )
            )

            random_data = [hex(x) for x in random.randbytes(10)]
            result = await client.post(
                url, json={"message": f"Hello, world! {random_data}"}
            )
            self.assertEqual(result.status_code, 200)
            self.assertEqual(
                result.json(), {"message": f"Hello, world! {random_data}"}
            )

    async def test_post_with_headers(self):
        with http.HttpClient(100) as client:
            example_request = (
                'POST',
                self.base_url,
                '/test-post-with-headers',
            )
            url = f"{example_request[1]}{example_request[2]}"
            self.mock_server.register_route_handler(*example_request)(
                lambda _handler, request: (
                    request.body,
                    200,
                    {"X-Test": request.headers["x-test"] + "!"},
                )
            )
            random_data = [hex(x) for x in random.randbytes(10)]
            result = await client.post(
                url,
                json={"message": f"Hello, world! {random_data}"},
                headers={"X-Test": "test"},
            )
            self.assertEqual(result.status_code, 200)
            self.assertEqual(
                result.json(), {"message": f"Hello, world! {random_data}"}
            )
            self.assertEqual(result.headers["X-Test"], "test!")

    async def test_streaming_get_with_no_sse(self):
        with http.HttpClient(100) as client:
            example_request = (
                'GET',
                self.base_url,
                '/test-get-with-sse',
            )
            url = f"{example_request[1]}{example_request[2]}"
            self.mock_server.register_route_handler(*example_request)(
                lambda _handler, request: (
                    "\"ok\"",
                    200,
                )
            )
            result = await client.stream_sse(url, method="GET")
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json(), "ok")

    async def test_sse_with_mock_server(self):
        """Since the regular mock server doesn't support SSE, we need to test
        with a real socket. We handle just enough HTTP to get the job done."""

        is_closed = False

        async def mock_sse_server(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ):
            nonlocal is_closed

            await reader.readline()

            headers = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/event-stream\r\n"
                b"Cache-Control: no-cache\r\n"
                b"Connection: keep-alive\r\n\r\n"
            )
            writer.write(headers)
            await writer.drain()

            for i in range(3):
                writer.write(
                    f"event: message\ndata: Event {i + 1}\n\n".encode()
                )
                await writer.drain()
                await asyncio.sleep(0.1)

            # Write enough messages that we get a broken pipe. The response gets
            # closed below and will refuse any further messages.
            try:
                for _ in range(50):
                    writer.writelines([b"event: message", b"data: XX", b""])
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                is_closed = True

        server = await asyncio.start_server(mock_sse_server, 'localhost', 0)
        addr = server.sockets[0].getsockname()
        url = f'http://{addr[0]}:{addr[1]}/sse'

        async def client_task():
            with http.HttpClient(100) as client:
                response = await client.stream_sse(url, method="GET")
                assert response.status_code == 200
                assert response.headers['Content-Type'] == 'text/event-stream'
                assert isinstance(response, http.ResponseSSE)

                events = []
                async for event in response:
                    self.assertEqual(event.event, 'message')
                    events.append(event)
                    if len(events) == 3:
                        break

                assert len(events) == 3
                assert events[0].data == 'Event 1'
                assert events[1].data == 'Event 2'
                assert events[2].data == 'Event 3'

        async with server:
            client_future = asyncio.create_task(client_task())
            await asyncio.wait_for(client_future, timeout=5.0)

        assert is_closed