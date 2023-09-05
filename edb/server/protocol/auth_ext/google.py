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


from . import base, data


class GoogleProvider(base.OpenIDProvider):
    def __init__(self, *args, **kwargs):
        super().__init__(
            "google", "https://accounts.google.com", *args, **kwargs
        )

    async def _get_oidc_config(self):
        client = self.http_factory(base_url=self.issuer_url)
        response = await client.get('/.well-known/openid-configuration')
        config = response.json()
        return data.OpenIDConfig(**config)
