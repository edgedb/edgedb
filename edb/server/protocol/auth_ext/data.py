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


import dataclasses
from typing import Optional


@dataclasses.dataclass
class UserInfo:
    """
    OpenID Connect compatible user info.
    See: https://openid.net/specs/openid-connect-core-1_0.html
    """

    sub: str
    name: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    middle_name: Optional[str] = None
    nickname: Optional[str] = None
    preferred_username: Optional[str] = None
    profile: Optional[str] = None
    picture: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    gender: Optional[str] = None
    birthdate: Optional[str] = None
    zoneinfo: Optional[str] = None
    locale: Optional[str] = None
    phone_number: Optional[str] = None
    phone_number_verified: Optional[bool] = None
    address: Optional[dict[str, str]] = None
    updated_at: Optional[float] = None

    def __str__(self) -> str:
        return self.sub

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"sub={self.sub!r} "
            f"name={self.name!r} "
            f"email={self.email!r} "
            f"preferred_username={self.preferred_username!r})"
        )


@dataclasses.dataclass
class Identity:
    id: str
    sub: str
    iss: str
    email: str | None

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r} "
            f"sub={self.sub!r} "
            f"iss={self.iss!r} "
            f"email={self.email!r})"
        )


@dataclasses.dataclass
class OpenIDConfig:
    """
    OpenID Connect configuration.
    See:
    - https://openid.net/specs/openid-connect-discovery-1_0.html
    - https://accounts.google.com/.well-known/openid-configuration
    """

    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    userinfo_endpoint: Optional[str]
    jwks_uri: str
    registration_endpoint: Optional[str]
    scopes_supported: Optional[list[str]]
    response_types_supported: list[str]
    response_modes_supported: Optional[list[str]]
    grant_types_supported: Optional[list[str]]
    acr_values_supported: Optional[list[str]]
    subject_types_supported: list[str]
    id_token_signing_alg_values_supported: list[str]
    id_token_encryption_alg_values_supported: Optional[list[str]]
    id_token_encryption_enc_values_supported: Optional[list[str]]
    userinfo_signing_alg_values_supported: Optional[list[str]]
    userinfo_encryption_alg_values_supported: Optional[list[str]]
    userinfo_encryption_enc_values_supported: Optional[list[str]]
    request_object_signing_alg_values_supported: Optional[list[str]]
    request_object_encryption_alg_values_supported: Optional[list[str]]
    request_object_encryption_enc_values_supported: Optional[list[str]]
    token_endpoint_auth_methods_supported: Optional[list[str]]
    token_endpoint_auth_signing_alg_values_supported: Optional[list[str]]
    display_values_supported: Optional[list[str]]
    claim_types_supported: Optional[list[str]]
    claims_supported: Optional[list[str]]
    service_documentation: Optional[str]
    claims_locales_supported: Optional[list[str]]
    ui_locales_supported: Optional[list[str]]
    claims_parameter_supported: Optional[bool]
    request_parameter_supported: Optional[bool]
    request_uri_parameter_supported: Optional[bool]
    require_request_uri_registration: Optional[bool]
    op_policy_uri: Optional[str]
    op_tos_uri: Optional[str]

    def __init__(self, **kwargs):
        for field in dataclasses.fields(self):
            setattr(self, field.name, kwargs.get(field.name))

    def __str__(self) -> str:
        return self.issuer

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"issuer={self.issuer!r}, "
            f"authorization_endpoint={self.authorization_endpoint!r}, "
            f"token_endpoint={self.token_endpoint!r}, "
            f"jwks_uri={self.jwks_uri!r}, "
            f"response_types_supported={self.response_types_supported!r}, "
            f"subject_types_supported={self.subject_types_supported!r}, "
            "id_token_signing_alg_values_supported="
            f"{self.id_token_signing_alg_values_supported!r})"
        )


@dataclasses.dataclass
class OAuthAccessTokenResponse:
    """
    Access Token Response.
    https://datatracker.ietf.org/doc/html/rfc6749#section-4.1.4
    """

    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str

    def __init__(self, **kwargs):
        for field in dataclasses.fields(self):
            setattr(self, field.name, kwargs.get(field.name))

    def __str__(self) -> str:
        return self.access_token

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"access_token={self.access_token!r}, "
            f"token_type={self.token_type!r}, "
            f"expires_in={self.expires_in!r}, "
            f"refresh_token={self.refresh_token!r})"
        )


@dataclasses.dataclass
class OpenIDConnectAccessTokenResponse(OAuthAccessTokenResponse):
    """
    OpenID Connect Access Token Response.
    https://openid.net/specs/openid-connect-core-1_0.html#TokenResponse
    """

    id_token: str

    def __init__(self, **kwargs):
        for field in dataclasses.fields(self):
            setattr(self, field.name, kwargs.get(field.name))

    def __str__(self) -> str:
        return self.id_token

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"access_token={self.access_token!r}, "
            f"token_type={self.token_type!r}, "
            f"expires_in={self.expires_in!r}, "
            f"refresh_token={self.refresh_token!r}, "
            f"id_token={self.id_token!r})"
        )
