#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Any, Callable, Self

from edb.server import http


class AuthHttpClient:
    def __init__(
        self,
        http_client: http.HttpClient,
        url_munger: Callable[[str], str] | None = None,
        base_url: str | None = None,
    ):
        self.url_munger = url_munger
        self.http_client = http_client
        self.base_url = base_url

    async def post(
        self,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        data: bytes | str | dict[str, str] | None = None,
        json: Any | None = None,
    ) -> http.Response:
        if self.base_url:
            path = self.base_url + path
        if self.url_munger:
            path = self.url_munger(path)
        return await self.http_client.post(
            path, headers=headers, data=data, json=json
        )

    async def get(
        self, path: str, *, headers: dict[str, str] | None = None
    ) -> http.Response:
        if self.base_url:
            path = self.base_url + path
        if self.url_munger:
            path = self.url_munger(path)
        return await self.http_client.get(path, headers=headers)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args) -> None:  # type: ignore
        pass
