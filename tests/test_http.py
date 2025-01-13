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
import inspect
import json
import random
import unittest

from edb.server import http
from edb.testbase import http as http_tb


async def async_timeout(coroutine, timeout=5):
    return await asyncio.wait_for(coroutine, timeout=timeout)


def run_async(coroutine, timeout=5):
    with asyncio.Runner(debug=True) as runner:
        runner.run(async_timeout(coroutine, timeout))


class BaseHttpAsyncTest(unittest.TestCase):
    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if name.startswith("test_") and inspect.iscoroutinefunction(attr):
            return lambda: run_async(attr())
        return attr


class HttpTest(BaseHttpAsyncTest):
    def setUp(self):
        self.mock_server = http_tb.MockHttpServer()
        self.mock_server.start()
        self.base_url = self.mock_server.get_base_url().rstrip("/")

    def tearDown(self):
        if self.mock_server is not None:
            self.mock_server.stop()
        self.mock_server = None

    async def test_get(self):
        async with http.HttpClient(100) as client:
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
        async with http.HttpClient(100) as client:
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
        async with http.HttpClient(100) as client:
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

    async def test_bad_url(self):
        async with http.HttpClient(100) as client:
            with self.assertRaisesRegex(Exception, "Scheme"):
                await client.get("httpx://uh-oh")

    async def test_immediate_connection_drop(self):
        """Test handling of a connection that is dropped immediately by the
        server"""

        async def mock_drop_server(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ):
            # Close connection immediately after reading a byte without sending
            # any response
            await reader.read(1)
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(mock_drop_server, '127.0.0.1', 0)
        addr = server.sockets[0].getsockname()
        url = f'http://{addr[0]}:{addr[1]}/drop'

        try:
            async with http.HttpClient(100) as client:
                with self.assertRaisesRegex(
                    Exception, "Connection reset by peer|IncompleteMessage"
                ):
                    await client.get(url)
        finally:
            server.close()
            await server.wait_closed()

    async def test_streaming_get_with_no_sse(self):
        async with http.HttpClient(100) as client:
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


class HttpSSETest(BaseHttpAsyncTest):
    async def test_immediate_connection_drop_streaming(self):
        """Test handling of a connection that is dropped immediately by the
        server"""

        async def mock_drop_server(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ):
            # Close connection immediately after reading a byte without sending
            # any response
            await reader.read(1)
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(mock_drop_server, '127.0.0.1', 0)
        addr = server.sockets[0].getsockname()
        url = f'http://{addr[0]}:{addr[1]}/drop'

        try:
            async with http.HttpClient(100) as client:
                with self.assertRaisesRegex(
                    Exception, "Connection reset by peer|IncompleteMessage"
                ):
                    await client.stream_sse(url)
        finally:
            server.close()
            await server.wait_closed()

    async def test_sse_with_mock_server_client_close(self):
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
                writer.write(b": test comment that should be ignored\n\n")
                await writer.drain()

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

        server = await asyncio.start_server(mock_sse_server, '127.0.0.1', 0)
        addr = server.sockets[0].getsockname()
        url = f'http://{addr[0]}:{addr[1]}/sse'

        async def client_task():
            async with http.HttpClient(100) as client:
                async with await client.stream_sse(
                    url, method="GET"
                ) as response:
                    assert response.status_code == 200
                    assert (
                        response.headers['Content-Type'] == 'text/event-stream'
                    )
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

    async def test_sse_with_mock_server_close(self):
        """Try to close the server-side stream and see if the client detects
        an end for the iterator. Note that this is technically not correct SSE:
        the client should actually try to reconnect after the specified retry
        interval, _but_ we don't handle retries yet."""

        is_closed = False

        async def mock_sse_server(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ):
            nonlocal is_closed

            # Read until empty line
            while True:
                line = await reader.readline()
                if line == b'\r\n':
                    break

            headers = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/event-stream\r\n"
                b"Cache-Control: no-cache\r\n\r\n"
            )
            writer.write(headers)
            await writer.drain()

            for i in range(3):
                writer.write(b": test comment that should be ignored\n\n")
                await writer.drain()

                writer.write(
                    f"event: message\ndata: Event {i + 1}\n\n".encode()
                )
                await writer.drain()
                await asyncio.sleep(0.1)

            await writer.drain()
            writer.close()
            is_closed = True

        server = await asyncio.start_server(mock_sse_server, '127.0.0.1', 0)
        addr = server.sockets[0].getsockname()
        url = f'http://{addr[0]}:{addr[1]}/sse'

        async def client_task():
            async with http.HttpClient(100) as client:
                async with await client.stream_sse(
                    url, method="GET", headers={"Connection": "close"}
                ) as response:
                    assert response.status_code == 200
                    assert (
                        response.headers['Content-Type'] == 'text/event-stream'
                    )
                    assert isinstance(response, http.ResponseSSE)

                    events = []
                    async for event in response:
                        self.assertEqual(event.event, 'message')
                        events.append(event)

                    assert len(events) == 3
                    assert events[0].data == 'Event 1'
                    assert events[1].data == 'Event 2'
                    assert events[2].data == 'Event 3'

        async with server:
            client_future = asyncio.create_task(client_task())
            await asyncio.wait_for(client_future, timeout=5.0)
        assert is_closed
