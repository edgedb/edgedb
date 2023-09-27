#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
import json
import httpx
import httpx_cache

from typing import Any
from edb.server.protocol import execute

from . import errors, util, data, base


class HttpClient(httpx.AsyncClient):
    def __init__(
        self, *args, edgedb_test_url: str | None, base_url: str, **kwargs
    ):
        if edgedb_test_url:
            self.edgedb_orig_base_url = urllib.parse.quote(base_url, safe='')
            base_url = edgedb_test_url
        cache = httpx_cache.AsyncCacheControlTransport()
        super().__init__(*args, base_url=base_url, transport=cache, **kwargs)

    async def post(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().post(path, *args, **kwargs)

    async def get(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().get(path, *args, **kwargs)


class Client:
    provider: base.BaseProvider

    def __init__(self, db: Any, provider_id: str, base_url: str | None = None):
        self.db = db
        self.db_config = db.db_config

        http_factory = lambda *args, **kwargs: HttpClient(
            *args, edgedb_test_url=base_url, **kwargs
        )

        (provider_name, client_id, client_secret) = self._get_provider_config(
            provider_id
        )

        match provider_name:
            case "github":
                from . import github

                self.provider = github.GitHubProvider(
                    client_id=client_id,
                    client_secret=client_secret,
                    http_factory=http_factory,
                )
            case "google":
                from . import google

                self.provider = google.GoogleProvider(
                    client_id=client_id,
                    client_secret=client_secret,
                    http_factory=http_factory,
                )
            case "azure":
                from . import azure
                self.provider = azure.AzureProvider(
                    client_id=client_id,
                    client_secret=client_secret,
                    http_factory=http_factory,
                )
            case "apple":
                from . import apple
                self.provider = apple.AppleProvider(
                    client_id=client_id,
                    client_secret=client_secret,
                    http_factory=http_factory,
                )
            case _:
                raise errors.InvalidData(f"Invalid provider: {provider_name}")

    async def get_authorize_url(self, state: str, redirect_uri: str) -> str:
        return await self.provider.get_code_url(
            state=state, redirect_uri=redirect_uri
        )

    async def handle_callback(self, code: str) -> data.Identity:
        response = await self.provider.exchange_code(code)
        user_info = await self.provider.fetch_user_info(response)

        return await self._handle_identity(user_info)

    async def _handle_identity(self, user_info: data.UserInfo) -> data.Identity:
        """Update or create an identity"""

        r = await execute.parse_execute_json(
            db=self.db,
            query="""\
with
  iss := <str>$issuer_url,
  sub := <str>$provider_id,

select (insert ext::auth::Identity {
  issuer := iss,
  subject := sub,
} unless conflict on ((.issuer, .subject)) else (
  select ext::auth::Identity
)) { * };""",
            variables={
                "issuer_url": self.provider.issuer_url,
                "provider_id": user_info.sub,
            },
        )
        result_json = json.loads(r.decode())
        assert len(result_json) == 1

        return data.Identity(**result_json[0])

    def _get_provider_config(self, provider_id: str) -> tuple[str, str, str]:
        provider_client_config = util.get_config(
            self.db_config, "ext::auth::AuthConfig::providers", frozenset
        )
        provider_name: str | None = None
        client_id: str | None = None
        client_secret: str | None = None
        for cfg in provider_client_config:
            if cfg.provider_id == provider_id:
                provider_name = cfg.provider_name
                client_id = cfg.client_id
                client_secret = cfg.secret
        r = (provider_name, client_id, client_secret)
        match r:
            case (str(_), str(_), str(_)):
                return r
            case _:
                raise errors.InvalidData(
                    f"Invalid provider configuration: {provider_id}\n"
                    f"providers={provider_client_config!r}"
                )
