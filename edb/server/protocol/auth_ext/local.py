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

from typing import Any

class Client:
    def __init__(self, db: Any, provider_id: str):
        self.db = db
        self.db_config = db.db_config
        provider_type = self._get_provider_config(provider_id)
        match provider_type:
            case "password":
                from . import password
                self.provider = password.PasswordProvider()
            _:
                raise errors.InvalidData(f"Invalid provider: {provider_name}")

    async def register(self, *args, **kwargs):
        return await self.provider.register(*args, **kwargs)

    async def authenticate(self, *args, **kwargs):
        return await self.provider.authenticate(*args, **kwargs)

    async def logout(self, *args, **kwargs):
        return await self.provider.logout(*args, **kwargs)

    def _get_provider_config(self, provider_id: str) -> str:
        provider_client_config = util.get_config(
            self.db_config, "ext::auth::AuthConfig::providers", frozenset
        )
        provider_name: str | None = None
        for cfg in provider_client_config:
            if cfg.provider_id == provider_id:
                provider_name = cfg.provider_name
        match provider_name:
            case "password":
                return "password"
            _:
                raise errors.InvalidData(
                    f"Invalid provider configuration: {provider_id}\n"
                    f"providers={provider_client_config!r}"
                )
