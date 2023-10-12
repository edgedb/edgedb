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


from typing import TypeVar, Type, overload, Any

from edb.server import config

from . import errors

T = TypeVar("T")


def maybe_get_config_unchecked(
    db: Any, key: str
) -> Any:
    return config.lookup(key, db.db_config, spec=db.user_config_spec)


@overload
def maybe_get_config(
    db: Any, key: str, expected_type: Type[T]
) -> T | None:
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


def get_config(
    db: Any, key: str, expected_type: Type[object] = str
) -> object:
    value = maybe_get_config(db, key, expected_type)
    if value is None:
        raise errors.MissingConfiguration(
            key=key,
            description="Missing configuration value",
        )
    return value


def get_config_unchecked(
    db: Any, key: str
) -> Any:
    value = maybe_get_config_unchecked(db, key)
    if value is None:
        raise errors.MissingConfiguration(
            key=key,
            description="Missing configuration value",
        )
    return value


def get_config_typename(config_value: config.SettingValue) -> str:
    return config_value._tspec.name  # type: ignore
