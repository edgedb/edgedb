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

from typing import Any
import urllib.parse

import hishel
import httpx


class HttpClient(httpx.AsyncClient):
    def __init__(
        self,
        *args: Any,
        edgedb_test_url: str | None,
        base_url: str,
        **kwargs: Any,
    ):
        self.edgedb_orig_base_url = None
        if edgedb_test_url:
            self.edgedb_orig_base_url = urllib.parse.quote(base_url, safe='')
            base_url = edgedb_test_url
        cache = hishel.AsyncCacheTransport(
            transport=httpx.AsyncHTTPTransport(),
            storage=hishel.AsyncInMemoryStorage(capacity=5),
        )
        super().__init__(
            *args, base_url=base_url, transport=cache, **kwargs
        )

    async def post(  # type: ignore[override]
        self,
        path: str,
        *args: Any,
        **kwargs: Any,
    ) -> httpx.Response:
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().post(
            path, *args, **kwargs
        )

    async def get(  # type: ignore[override]
        self,
        path: str,
        *args: Any,
        **kwargs: Any,
    ) -> httpx.Response:
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().get(path, *args, **kwargs)
