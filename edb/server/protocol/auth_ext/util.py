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

import urllib.parse
import html
import logging
import asyncio

from typing import (
    TypeVar, Type, overload, Any, cast, Optional, TYPE_CHECKING, Callable,
    Awaitable
)

from edb.server import config as edb_config, auth as jwt_auth
from edb.server.config.types import CompositeConfigType

from . import errors, config

if TYPE_CHECKING:
    from edb.server import tenant as edbtenant

T = TypeVar("T")

logger = logging.getLogger('edb.server.ext.auth')

# Cache JWKSets for 10 minutes
jwtset_cache = jwt_auth.JWKSetCache(60 * 10)


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


async def get_remote_jwtset(
    url: str,
    fetch_lambda: Callable[[str], Awaitable[jwt_auth.JWKSet]],
) -> jwt_auth.JWKSet:
    """
    Get a JWKSet from the cache, or fetch it from the given URL if it's not in
    the cache.
    """
    is_fresh, jwtset = jwtset_cache.get(url)
    match (is_fresh, jwtset):
        case (_, None):
            jwtset = await fetch_lambda(url)
            jwtset_cache.set(url, jwtset)
        case (True, jwtset):
            pass
        case _:
            # Run fetch in background to refresh cache
            async def refresh_cache(url: str) -> None:
                try:
                    new_jwtset = await fetch_lambda(url)
                    jwtset_cache.set(url, new_jwtset)
                except Exception:
                    logger.exception(
                        f"Failed to refresh JWKSet cache for {url}"
                    )

            asyncio.create_task(refresh_cache(url))

    assert jwtset is not None
    return jwtset
