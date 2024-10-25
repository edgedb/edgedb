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

from __future__ import annotations

from typing import (
    Tuple,
    Any,
    Mapping,
    Optional,
    Union,
)

import asyncio
import dataclasses
import logging
import os
import json as json_lib
import urllib.parse

from edb.server._http import Http

logger = logging.getLogger("edb.server")


HeaderType = Optional[Union[list[tuple[str, str]], dict[str, str]]]


class HttpClient:
    def __init__(self, limit: int):
        self._client = Http(limit)
        self._fd = self._client._fd
        self._task = None
        self._skip_reads = 0
        self._loop = asyncio.get_running_loop()
        self._task = self._loop.create_task(self._boot(self._loop))
        self._streaming = {}
        self._next_id = 0
        self._requests: dict[int, asyncio.Future] = {}

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    def _update_limit(self, limit: int):
        self._client._update_limit(limit)

    def _process_headers(self, headers: HeaderType) -> list[tuple[str, str]]:
        if headers is None:
            return []
        if isinstance(headers, Mapping):
            return [(k, v) for k, v in headers.items()]
        if isinstance(headers, list):
            return headers
        raise ValueError(f"Invalid headers type: {type(headers)}")

    def _process_content(
        self,
        headers: list[tuple[str, str]],
        data: bytes | str | dict[str, str] | None = None,
        json: Any | None = None,
    ) -> bytes:
        if json is not None:
            data = json_lib.dumps(json).encode('utf-8')
            headers.append(('Content-Type', 'application/json'))
        elif isinstance(data, str):
            data = data.encode('utf-8')
        elif isinstance(data, dict):
            data = urllib.parse.urlencode(data).encode('utf-8')
            headers.append(
                ('Content-Type', 'application/x-www-form-urlencoded')
            )
        elif data is None:
            data = bytes()
        elif isinstance(data, bytes):
            pass
        else:
            raise ValueError(f"Invalid content type: {type(data)}")
        return data

    async def request(
        self,
        *,
        method: str,
        path: str,
        headers: HeaderType = None,
        data: bytes | str | dict[str, str] | None = None,
        json: Any | None = None,
    ) -> tuple[int, bytes, dict[str, str]]:
        headers_list = self._process_headers(headers)
        data = self._process_content(headers_list, data, json)
        id = self._next_id
        self._next_id += 1
        self._requests[id] = asyncio.Future()
        try:
            self._client._request(id, path, method, data, headers_list)
            resp = await self._requests[id]
            return resp
        finally:
            del self._requests[id]

    async def get(self, path: str, *, headers: HeaderType = None) -> Response:
        headers_list = self._process_headers(headers)
        result = await self.request(
            method="GET", path=path, data=None, headers=headers_list
        )
        return Response.from_tuple(result)

    async def post(
        self,
        path: str,
        *,
        headers: HeaderType = None,
        data: bytes | str | dict[str, str] | None = None,
        json: Any | None = None,
    ) -> Response:
        headers_list = self._process_headers(headers)
        data = self._process_content(headers_list, data, json)
        result = await self.request(
            method="POST", path=path, data=data, headers=headers_list
        )
        return Response.from_tuple(result)

    async def stream_sse(
        self,
        path: str,
        *,
        method: str = "POST",
        headers: HeaderType = None,
        data: bytes | str | dict[str, str] | None = None,
        json: Any | None = None,
    ) -> Response | ResponseSSE:
        headers_list = self._process_headers(headers)
        data = self._process_content(headers_list, data, json)

        id = self._next_id
        self._next_id += 1
        self._requests[id] = asyncio.Future()
        try:
            self._client._request_sse(id, path, method, data, headers_list)
            resp = await self._requests[id]
            if id in self._streaming:
                return ResponseSSE.from_tuple(resp, self._streaming[id])
            return Response.from_tuple(resp)
        finally:
            del self._requests[id]

    async def _boot(self, loop: asyncio.AbstractEventLoop) -> None:
        logger.info("Python-side HTTP client booted")
        reader = asyncio.StreamReader(loop=loop)
        reader_protocol = asyncio.StreamReaderProtocol(reader)
        fd = os.fdopen(self._client._fd, 'rb')
        transport, _ = await loop.connect_read_pipe(lambda: reader_protocol, fd)
        try:
            while len(await reader.read(1)) == 1:
                if not self._client or not self._task:
                    break
                if self._skip_reads > 0:
                    self._skip_reads -= 1
                    continue
                msg = self._client._read()
                if not msg:
                    break
                self._process_message(msg)
        finally:
            transport.close()

    def _process_message(self, msg):
        try:
            msg_type, id, data = msg
            if msg_type == 0:  # Error
                if id in self._requests:
                    self._requests[id].set_exception(Exception(data[0]))
            elif msg_type == 1:  # Response
                if id in self._requests:
                    self._requests[id].set_result(data)
            elif msg_type == 2:  # SSEStart
                if id in self._requests:
                    self._streaming[id] = asyncio.Queue()
                    self._requests[id].set_result(data)
            elif msg_type == 3:  # SSEEvent
                if id in self._streaming:
                    self._streaming[id].put_nowait(data)
            elif msg_type == 4:  # SSEEnd
                if id in self._streaming:
                    self._streaming[id].put_nowait(None)
                    del self._streaming[id]
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            raise


class CaseInsensitiveDict(dict):
    def __init__(self, data: Optional[list[Tuple[str, str]]] = None):
        super().__init__()
        if data:
            for k, v in data.items():
                self[k.lower()] = v

    def __setitem__(self, key: str, value: str):
        super().__setitem__(key.lower(), value)

    def __getitem__(self, key: str):
        return super().__getitem__(key.lower())

    def get(self, key: str, default=None):
        return super().get(key.lower(), default)

    def update(self, *args, **kwargs: str) -> None:
        if args:
            data = args[0]
            if isinstance(data, Mapping):
                for key, value in data.items():
                    self[key] = value
            else:
                for key, value in data:
                    self[key] = value
        for key, value in kwargs.items():
            self[key] = value


@dataclasses.dataclass(frozen=True)
class Response:
    status_code: int
    body: bytes
    headers: CaseInsensitiveDict
    is_streaming: bool = False

    @classmethod
    def from_tuple(cls, data: Tuple[int, bytes, dict[str, str]]):
        status_code, body, headers_list = data
        headers = CaseInsensitiveDict([(k, v) for k, v in headers_list.items()])
        return cls(status_code, body, headers)

    def json(self):
        return json_lib.loads(self.body.decode('utf-8'))

    @property
    def text(self) -> str:
        return self.body.decode('utf-8')


@dataclasses.dataclass(frozen=True)
class ResponseSSE:
    status_code: int
    headers: CaseInsensitiveDict
    _stream: asyncio.Queue = dataclasses.field(repr=False)
    is_streaming: bool = True

    @classmethod
    def from_tuple(
        cls, data: Tuple[int, dict[str, str]], stream: asyncio.Queue
    ):
        status_code, headers = data
        headers = CaseInsensitiveDict(headers)
        return cls(status_code, headers, stream)

    @dataclasses.dataclass(frozen=True)
    class SSEEvent:
        event: str
        data: str
        id: Optional[str] = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        next = await self._stream.get()
        if next is None:
            raise StopAsyncIteration
        id, data, event = next
        return self.SSEEvent(event, data, id)
