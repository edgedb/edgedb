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

from typing import cast, Any, Callable
from edb.server.protocol import execute
from edb.server.http import HttpClient

from . import github, google, azure, apple, discord, slack
from . import config, errors, util, data, base


class Client:
    provider: base.BaseProvider

    def __init__(
        self,
        *,
        db: Any,
        provider_name: str,
        http_client: HttpClient,
        url_munger: Callable[[str], str] | None = None,
    ):
        self.db = db

        http_factory = lambda *args, **kwargs: http_client.with_context(
            *args, url_munger=url_munger, **kwargs
        )

        provider_config = self._get_provider_config(provider_name)
        provider_args: tuple[str, str] | tuple[str, str, str, str] = (
            provider_config.client_id,
            provider_config.secret,
        )
        provider_kwargs = {
            "http_factory": http_factory,
            "additional_scope": provider_config.additional_scope,
        }

        match (provider_name, provider_config.issuer_url):
            case ("builtin::oauth_github", _):
                self.provider = github.GitHubProvider(
                    *provider_args,
                    **provider_kwargs,
                )
            case ("builtin::oauth_google", _):
                self.provider = google.GoogleProvider(
                    *provider_args,
                    **provider_kwargs,
                )
            case ("builtin::oauth_azure", _):
                self.provider = azure.AzureProvider(
                    *provider_args,
                    **provider_kwargs,
                )
            case ("builtin::oauth_apple", _):
                self.provider = apple.AppleProvider(
                    *provider_args,
                    **provider_kwargs,
                )
            case ("builtin::oauth_discord", _):
                self.provider = discord.DiscordProvider(
                    *provider_args,
                    **provider_kwargs,
                )
            case ("builtin::oauth_slack", _):
                self.provider = slack.SlackProvider(
                    *provider_args,
                    **provider_kwargs,
                )
            case (provider_name, str(issuer_url)):
                self.provider = base.OpenIDConnectProvider(
                    provider_name,
                    issuer_url,
                    *provider_args,
                    **provider_kwargs,
                )
            case _:
                raise errors.InvalidData(f"Invalid provider: {provider_name}")

    async def get_authorize_url(self, state: str, redirect_uri: str) -> str:
        return await self.provider.get_code_url(
            state=state,
            redirect_uri=redirect_uri,
            additional_scope=self.provider.additional_scope or "",
        )

    async def handle_callback(
        self, code: str, redirect_uri: str
    ) -> tuple[data.Identity, bool, str | None, str | None, str | None]:
        response = await self.provider.exchange_code(code, redirect_uri)
        user_info = await self.provider.fetch_user_info(response)
        auth_token = response.access_token
        refresh_token = response.refresh_token
        source_id_token = user_info.source_id_token

        return (
            *(await self._handle_identity(user_info)),
            auth_token,
            refresh_token,
            source_id_token,
        )

    async def _handle_identity(
        self, user_info: data.UserInfo
    ) -> tuple[data.Identity, bool]:
        """Update or create an identity"""

        r = await execute.parse_execute_json(
            db=self.db,
            query="""\
with
  iss := <str>$issuer_url,
  sub := <str>$subject,
  identity := (
    insert ext::auth::Identity {
      issuer := iss,
      subject := sub,
    } unless conflict on ((.issuer, .subject))
      else ext::auth::Identity
  )
select {
  identity := (select identity {*}),
  new := (identity not in ext::auth::Identity)
};""",
            variables={
                "issuer_url": self.provider.issuer_url,
                "subject": user_info.sub,
            },
            cached_globally=True,
            query_tag='gel/auth',
        )
        result_json = json.loads(r.decode())
        assert len(result_json) == 1

        return (
            data.Identity(**result_json[0]['identity']),
            result_json[0]['new'],
        )

    def _get_provider_config(
        self, provider_name: str
    ) -> config.OAuthProviderConfig:
        provider_client_config = util.get_config(
            self.db, "ext::auth::AuthConfig::providers", frozenset
        )
        for cfg in provider_client_config:
            if cfg.name == provider_name:
                cfg = cast(config.OAuthProviderConfig, cfg)
                return config.OAuthProviderConfig(
                    name=cfg.name,
                    display_name=cfg.display_name,
                    client_id=cfg.client_id,
                    secret=cfg.secret,
                    additional_scope=cfg.additional_scope,
                    issuer_url=getattr(cfg, 'issuer_url', None),
                    logo_url=getattr(cfg, 'logo_url', None),
                )

        raise errors.MissingConfiguration(
            provider_name, "Provider is not configured"
        )
