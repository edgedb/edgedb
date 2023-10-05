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


import json

from typing import Any, Type
from edb.server.protocol import execute

from . import github, google, azure, apple
from . import errors, util, data, base, http_client


class Client:
    provider: base.BaseProvider

    def __init__(
        self, db: Any, provider_name: str, base_url: str | None = None
    ):
        self.db = db

        http_factory = lambda *args, **kwargs: http_client.HttpClient(
            *args, edgedb_test_url=base_url, **kwargs
        )

        provider_config = self._get_provider_config(provider_name)
        provider_args = (provider_config.client_id, provider_config.secret)
        provider_kwargs = {
            "http_factory": http_factory,
            "additional_scope": provider_config.additional_scope,
        }

        provider_class: Type[base.BaseProvider]
        match provider_name:
            case "builtin::oauth_github":
                provider_class = github.GitHubProvider
            case "builtin::oauth_google":
                provider_class = google.GoogleProvider
            case "builtin::oauth_azure":
                provider_class = azure.AzureProvider
            case "builtin::oauth_apple":
                provider_class = apple.AppleProvider
            case _:
                raise errors.InvalidData(f"Invalid provider: {provider_name}")

        self.provider = provider_class(
            *provider_args, **provider_kwargs  # type: ignore
        )

    async def get_authorize_url(self, state: str, redirect_uri: str) -> str:
        return await self.provider.get_code_url(
            state=state,
            redirect_uri=redirect_uri,
            additional_scope=self.provider.additional_scope or "",
        )

    async def handle_callback(
        self, code: str, redirect_uri: str
    ) -> tuple[data.Identity, str | None, str | None]:
        response = await self.provider.exchange_code(code, redirect_uri)
        user_info = await self.provider.fetch_user_info(response)
        auth_token = response.access_token
        refresh_token = response.refresh_token

        return (
            await self._handle_identity(user_info),
            auth_token,
            refresh_token,
        )

    async def _handle_identity(self, user_info: data.UserInfo) -> data.Identity:
        """Update or create an identity"""

        r = await execute.parse_execute_json(
            db=self.db,
            query="""\
with
  iss := <str>$issuer_url,
  sub := <str>$subject,

select (insert ext::auth::Identity {
  issuer := iss,
  subject := sub,
} unless conflict on ((.issuer, .subject)) else (
  select ext::auth::Identity
)) { * };""",
            variables={
                "issuer_url": self.provider.issuer_url,
                "subject": user_info.sub,
            },
        )
        result_json = json.loads(r.decode())
        assert len(result_json) == 1

        return data.Identity(**result_json[0])

    def _get_provider_config(self, provider_name: str):
        provider_client_config = util.get_config(
            self.db, "ext::auth::AuthConfig::providers", frozenset
        )
        for cfg in provider_client_config:
            if cfg.name == provider_name:
                return data.ProviderConfig(
                    cfg.client_id, cfg.secret, cfg.additional_scope
                )

        raise errors.MissingConfiguration(
            provider_name, "Provider is not configured"
        )
