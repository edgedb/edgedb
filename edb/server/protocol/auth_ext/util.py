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


from typing import TypeVar, Type, overload, Any, cast, Optional

from edb.server import config as edb_config
from edb.server.config.types import CompositeConfigType

from . import errors, config

T = TypeVar("T")


def maybe_get_config_unchecked(db: Any, key: str) -> Any:
    return edb_config.lookup(key, db.db_config, spec=db.user_config_spec)


@overload
def maybe_get_config(db: Any, key: str, expected_type: Type[T]) -> T | None:
    ...


@overload
def maybe_get_config(db: Any, key: str) -> str | None:
    ...


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
def get_config(db: Any, key: str, expected_type: Type[T]) -> T:
    ...


@overload
def get_config(db: Any, key: str) -> str:
    ...


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


def get_app_details_config(db: Any) -> config.AppDetailsConfig:
    ui_config = cast(
        Optional[config.UIConfig],
        maybe_get_config(db, "ext::auth::AuthConfig::ui", CompositeConfigType),
    )

    return config.AppDetailsConfig(
        app_name=(
            maybe_get_config(db, "ext::auth::AuthConfig::app_name")
            or (ui_config.app_name if ui_config else None)
        ),
        logo_url=(
            maybe_get_config(db, "ext::auth::AuthConfig::logo_url")
            or (ui_config.logo_url if ui_config else None)
        ),
        dark_logo_url=(
            maybe_get_config(db, "ext::auth::AuthConfig::dark_logo_url")
            or (ui_config.dark_logo_url if ui_config else None)
        ),
        brand_color=(
            maybe_get_config(db, "ext::auth::AuthConfig::brand_color")
            or (ui_config.brand_color if ui_config else None)
        ),
    )
