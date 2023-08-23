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
from edb.server.protocol import execute
from edb.common import markup


from typing import Any

from . import errors, util, data


class HttpClient(httpx.AsyncClient):
    def __init__(
        self, *args, edgedb_test_url: str | None, base_url: str, **kwargs
    ):
        if edgedb_test_url:
            self.edgedb_orig_base_url = urllib.parse.quote(base_url, safe='')
            base_url = edgedb_test_url
        super().__init__(*args, base_url=base_url, **kwargs)

    async def post(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().post(path, *args, **kwargs)

    async def get(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().get(path, *args, **kwargs)


class Client:
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
            case _:
                raise errors.InvalidData("Invalid provider: {provider}")

    def get_authorize_url(self, state: str, redirect_uri: str) -> str:
        return self.provider.get_code_url(
            state=state, redirect_uri=redirect_uri
        )

    async def handle_callback(self, code: str) -> data.Session:
        token = await self.provider.exchange_code(code)
        user_info = await self.provider.fetch_user_info(token)

        session = await self._handle_identity(user_info)

        return session

    async def _handle_identity(self, user_info: data.UserInfo) -> data.Session:
        """Update or create an identity and session"""

        r = await execute.parse_execute_json(
            db=self.db,
            query="""\
with
  iss := <str>$issuer_url,
  sub := <str>$provider_id,
  email := <optional str>$email,
  now := std::datetime_of_statement(),
  expires_in := std::assert_single(
    cfg::Config.extensions[is ext::auth::AuthConfig]
      .token_time_to_live
  ),

  Identity := insert ext::auth::Identity {
    iss := iss,
    sub := sub,
    email := email,
  } unless conflict on ((.iss, .sub)) else (
    update ext::auth::Identity set {
        email := email
    }
  ),
  Session := (
    insert ext::auth::Session {
        # TODO: token: use JWT
        token := <str>std::uuid_generate_v4(),
        created_at := now,
        expires_at := now + expires_in,
        identity := Identity,
    }
  )
select Session { * };""",
            variables={
                "issuer_url": self.provider.issuer_url,
                "provider_id": user_info.sub,
                "email": user_info.email,
            },
        )
        results = json.loads(r.decode())
        assert len(results) == 1
        return data.Session(**results[0])

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
                    f"Invalid provider configuration: {provider_id}"
                )
