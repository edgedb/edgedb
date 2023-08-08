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


import asyncio
import urllib.parse

import httpx


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
    def __init__(self, db: Any, provider: str, base_url: str | None = None):
        self.db = db
        self.db_config = db.db_config

        http_factory = lambda *args, **kwargs: HttpClient(
            *args, edgedb_test_url=base_url, **kwargs
        )

        match provider:
            case "github":
                from . import github

                self.provider = github.GitHubProvider(
                    *self._get_client_credientials("github"),
                    http_factory=http_factory,
                )
            case _:
                raise errors.InvalidData("Invalid provider: {provider}")

    def get_authorize_url(self, state: str, redirect_uri: str) -> str:
        return self.provider.get_code_url(
            state=state, redirect_uri=redirect_uri
        )

    async def handle_callback(self, code: str) -> None:
        token = await self.provider.exchange_code(code)

        async with asyncio.TaskGroup() as g:
            user_info_t = g.create_task(self.provider.fetch_user_info(token))
            emails_t = g.create_task(self.provider.fetch_emails(token))
        user_info = user_info_t.result()
        emails = emails_t.result()

        await self._handle_identity(user_info, emails)

    async def _handle_identity(
        self, user_info: data.UserInfo, emails: list[data.Email]
    ) -> None:
        ...

    def _get_client_credientials(self, client_name: str) -> tuple[str, str]:
        client_id = util.get_config(
            self.db_config, f"xxx_{client_name}_client_id"
        )
        client_secret = util.get_config(
            self.db_config, f"xxx_{client_name}_client_secret"
        )
        return client_id, client_secret
