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

import urllib.parse

import hishel
import httpx

from edb.common import lru


class HttpClient(httpx.AsyncClient):
    def __init__(
        self, *args, edgedb_test_url: str | None, base_url: str, **kwargs
    ):
        self.edgedb_orig_base_url = None
        if edgedb_test_url:
            self.edgedb_orig_base_url = urllib.parse.quote(base_url, safe='')
            base_url = edgedb_test_url
        cache = hishel.AsyncCacheTransport(
            transport=httpx.AsyncHTTPTransport(), storage=InMemoryStorage()
        )
        super().__init__(*args, base_url=base_url, transport=cache, **kwargs)

    async def post(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().post(path, *args, **kwargs)

    async def get(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().get(path, *args, **kwargs)


class InMemoryStorage(hishel._async._storages.AsyncBaseStorage):
    def __init__(self, maxsize=5):
        super().__init__()
        self._storage = lru.LRUMapping(maxsize=maxsize)

    def store(self, key: str, response, request, metadata):
        self._storage[key] = (response, request, metadata)

    async def retreive(self, key: str):
        return self._storage.get(key, None)

    async def aclose(self):
        pass
