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

from datetime import datetime

from . import data


class BaseProvider:
    def __init__(
        self, name: str, client_id: str, client_secret: str, *, http_factory
    ):
        self.name = name
        self.client_id = client_id
        self.client_secret = client_secret
        self.http_factory = http_factory

    def get_code_url(self, state: str, redirect_uri: str) -> str:
        raise NotImplementedError

    async def exchange_code(self, code: str) -> str:
        raise NotImplementedError

    async def fetch_user_info(self, token: str) -> data.UserInfo:
        raise NotImplementedError

    async def fetch_emails(self, token: str) -> list[data.Email]:
        raise NotImplementedError

    def _maybe_isoformat_to_timestamp(self, value: str | None) -> float | None:
        return (
            datetime.fromisoformat(value).timestamp() if value else None
        )
