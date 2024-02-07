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


from typing import Optional
from dataclasses import dataclass
import urllib.parse


class UIConfig:
    redirect_to: str
    redirect_to_on_signup: Optional[str]


@dataclass
class AppDetailsConfig:
    app_name: Optional[str]
    logo_url: Optional[str]
    dark_logo_url: Optional[str]
    brand_color: Optional[str]


class ProviderConfig:
    name: str


class WebAuthnProviderConfig(ProviderConfig):
    relying_party_origin: str
    require_verification: bool


@dataclass
class WebAuthnProvider:
    name: str
    relying_party_origin: str
    require_verification: bool

    def __init__(
        self, name: str, relying_party_origin: str, require_verification: bool
    ):
        self.name = name
        self.relying_party_origin = relying_party_origin
        self.require_verification = require_verification
        parsed_url = urllib.parse.urlparse(self.relying_party_origin)
        if parsed_url.hostname is None:
            raise ValueError(
                "Invalid relying_party_origin, hostname cannot be None"
            )
        self.relying_party_id = parsed_url.hostname
