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


from __future__ import annotations

import base64
import urllib.parse
import datetime
import html

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDFExpand
from cryptography.hazmat.backends import default_backend

from jwcrypto import jwt, jwk
from typing import TypeVar, Type, overload, Any, cast, Optional, TYPE_CHECKING

from edb.server import config as edb_config
from edb.server.config.types import CompositeConfigType

from . import errors, config

if TYPE_CHECKING:
    from edb.server import tenant as edbtenant

T = TypeVar("T")


def maybe_get_config_unchecked(db: edbtenant.dbview.Database, key: str) -> Any:
    return edb_config.lookup(key, db.db_config, spec=db.user_config_spec)


@overload
def maybe_get_config(db: Any, key: str, expected_type: Type[T]) -> T | None: ...


@overload
def maybe_get_config(db: Any, key: str) -> str | None: ...


def maybe_get_config(
    db: Any, key: str, expected_type: Type[object] = str
) -> object:
    value = maybe_get_config_unchecked(db, key)

    if value is None:
        return None

    if not isinstance(value, expected_type):
        raise TypeError(
            f"Config value `{key}` must be {expected_type.__name__}, got "
            f"{type(value).__name__}"
        )

    return value


@overload
def get_config(db: Any, key: str, expected_type: Type[T]) -> T: ...


@overload
def get_config(db: Any, key: str) -> str: ...


def get_config(db: Any, key: str, expected_type: Type[object] = str) -> object:
    value = maybe_get_config(db, key, expected_type)
    if value is None:
        raise errors.MissingConfiguration(
            key=key,
            description="Missing configuration value",
        )
    return value


def get_config_unchecked(db: Any, key: str) -> Any:
    value = maybe_get_config_unchecked(db, key)
    if value is None:
        raise errors.MissingConfiguration(
            key=key,
            description="Missing configuration value",
        )
    return value


def get_config_typename(config_value: edb_config.SettingValue) -> str:
    return config_value._tspec.name  # type: ignore


def escape_and_truncate(input_str: str | None, max_len: int) -> str | None:
    if input_str is None:
        return None
    trunc = (
        f"{input_str[:max_len]}..."
        if len(input_str) > max_len
        else input_str
    )
    return html.escape(trunc)


def get_app_details_config(db: Any) -> config.AppDetailsConfig:
    ui_config = cast(
        Optional[config.UIConfig],
        maybe_get_config(db, "ext::auth::AuthConfig::ui", CompositeConfigType),
    )

    return config.AppDetailsConfig(
        app_name=escape_and_truncate(
            maybe_get_config(db, "ext::auth::AuthConfig::app_name")
            or (ui_config.app_name if ui_config else None),
            100,
        ),
        logo_url=escape_and_truncate(
            maybe_get_config(db, "ext::auth::AuthConfig::logo_url")
            or (ui_config.logo_url if ui_config else None),
            2000,
        ),
        dark_logo_url=escape_and_truncate(
            maybe_get_config(db, "ext::auth::AuthConfig::dark_logo_url")
            or (ui_config.dark_logo_url if ui_config else None),
            2000,
        ),
        brand_color=escape_and_truncate(
            maybe_get_config(db, "ext::auth::AuthConfig::brand_color")
            or (ui_config.brand_color if ui_config else None),
            8,
        ),
    )


def join_url_params(url: str, params: dict[str, str]) -> str:
    parsed_url = urllib.parse.urlparse(url)
    query_params = {
        **urllib.parse.parse_qs(parsed_url.query),
        **{key: [val] for key, val in params.items()},
    }
    new_query_params = urllib.parse.urlencode(query_params, doseq=True)
    return parsed_url._replace(query=new_query_params).geturl()


def make_token(
    signing_key: jwk.JWK,
    issuer: str,
    subject: str,
    additional_claims: dict[str, str | int | float | bool | None] | None = None,
    include_issued_at: bool = False,
    expires_in: datetime.timedelta | None = None,
) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    expires_in = (
        datetime.timedelta(seconds=0) if expires_in is None else expires_in
    )
    expires_at = now + expires_in

    claims: dict[str, Any] = {
        "iss": issuer,
        "sub": subject,
        **(additional_claims or {}),
    }
    if expires_in.total_seconds() != 0:
        claims["exp"] = expires_at.timestamp()
    if include_issued_at:
        claims["iat"] = now.timestamp()

    token = jwt.JWT(
        header={"alg": "HS256"},
        claims=claims,
    )
    token.make_signed_token(signing_key)

    return cast(str, token.serialize())


def derive_key(key: jwk.JWK, info: str) -> jwk.JWK:
    """Derive a new key from the given symmetric key using HKDF."""

    # n.b. the key is returned as a base64url-encoded string
    raw_key_base64url = cast(str, key.get_op_key())
    input_key_material = base64.urlsafe_b64decode(raw_key_base64url)

    backend = default_backend()
    hkdf = HKDFExpand(
        algorithm=hashes.SHA256(),
        length=32,
        info=info.encode("utf-8"),
        backend=backend,
    )
    new_key_bytes = hkdf.derive(input_key_material)
    return jwk.JWK(
        kty="oct",
        k=new_key_bytes.hex(),
    )
