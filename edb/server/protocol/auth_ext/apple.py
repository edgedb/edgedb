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
import uuid
import urllib.parse


from . import base


class AppleProvider(base.OpenIDConnectProvider):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(
            "apple",
            "https://appleid.apple.com",
            *args,
            **kwargs,
        )

    async def get_code_url(
        self, state: str, redirect_uri: str, additional_scope: str
    ) -> str:
        oidc_config = await self._get_oidc_config()
        params = {
            "client_id": self.client_id,
            # Non-standard "name" scope
            "scope": f"openid email name {additional_scope}",
            "state": state,
            "redirect_uri": redirect_uri,
            "nonce": str(uuid.uuid4()),
            "response_type": "code id_token",
            "response_mode": "form_post",
        }
        encoded = urllib.parse.urlencode(params)
        return f"{oidc_config.authorization_endpoint}?{encoded}"
