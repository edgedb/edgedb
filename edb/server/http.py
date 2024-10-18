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
)

import asyncio
import dataclasses
import logging
import os
import json as json_lib
import urllib.parse

from edb.server._http import Http

logger = logging.getLogger("edb.server")


class HttpClient:
    def __init__(self, limit: int):
        self._client = Http(limit)
        self._fd = self._client._fd
        self._task = None
        self._skip_reads = 0
        self._loop = asyncio.get_running_loop()
        self._task = self._loop.create_task(self._boot(self._loop))
        self._next_id = 0
        self._requests: dict[int, asyncio.Future] = {}

    def __del__(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    def _update_limit(self, limit: int):
        self._client._update_limit(limit)

    async def request(
        self,
        *,
        method: str,
        url: str,
        content: bytes | None,
        headers: list[tuple[str, str]] | None,
    ) -> tuple[int, bytes, dict[str, str]]:
        if content is None:
            content = bytes()
        if headers is None:
            headers = []
        id = self._next_id
        self._next_id += 1
        self._requests[id] = asyncio.Future()
        try:
            self._client._request(id, url, method, content, headers)
            resp = await self._requests[id]
            return resp
        finally:
            del self._requests[id]

    async def get(
        self, path: str, *, headers: dict[str, str] | None = None
    ) -> Response:
        headers_list = [(k, v) for k, v in headers.items()] if headers else None
        result = await self.request(
            method="GET", url=path, content=None, headers=headers_list
        )
        return Response.from_tuple(result)

    async def post(
        self,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        data: bytes | str | dict[str, str] | None = None,
        json: Any | None = None,
    ) -> Response:
        if json is not None:
            data = json_lib.dumps(json).encode('utf-8')
            headers = headers or {}
            headers['Content-Type'] = 'application/json'
        elif isinstance(data, str):
            data = data.encode('utf-8')
        elif isinstance(data, dict):
            data = urllib.parse.urlencode(data).encode('utf-8')
            headers = headers or {}
            headers['Content-Type'] = 'application/x-www-form-urlencoded'

        headers_list = [(k, v) for k, v in headers.items()] if headers else None
        result = await self.request(
            method="POST", url=path, content=data, headers=headers_list
        )
        return Response.from_tuple(result)

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
        msg_type, id, data = msg

        if id in self._requests:
            if msg_type == 1:
                self._requests[id].set_result(data)
            elif msg_type == 0:
                self._requests[id].set_exception(Exception(data))


class CaseInsensitiveDict(dict):
    def __init__(self, data: Optional[list[Tuple[str, str]]] = None):
        super().__init__()
        if data:
            for k, v in data:
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


@dataclasses.dataclass
class Response:
    status_code: int
    body: bytes
    headers: CaseInsensitiveDict

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
