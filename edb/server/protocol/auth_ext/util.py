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


from typing import TypeVar, Type, Mapping, overload

from edb.server.config.ops import SettingValue
from . import errors

T = TypeVar("T")

SettingsMap = Mapping[str, SettingValue]


@overload
def maybe_get_config(
    db_config: SettingsMap, key: str, expected_type: Type[T]
) -> T | None:
    ...


@overload
def maybe_get_config(db_config: SettingsMap, key: str) -> str | None:
    ...


def maybe_get_config(
    db_config: SettingsMap, key: str, expected_type: Type[object] = str
) -> object:
    value = db_config.get(key, (None, None, None, None))[1]

    if value is None:
        return None

    if not isinstance(value, expected_type):
        raise TypeError(
            f"Config value `{key}` must be {expected_type.__name__}, got "
            f"{type(value).__name__}"
        )

    return value


@overload
def get_config(db_config: SettingsMap, key: str, expected_type: Type[T]) -> T:
    ...


@overload
def get_config(db_config: SettingsMap, key: str) -> str:
    ...


def get_config(
    db_config: SettingsMap, key: str, expected_type: Type[object] = str
) -> object:
    value = maybe_get_config(db_config, key, expected_type)
    if value is None:
        raise errors.MissingConfiguration(
            key=key,
            description="Missing configuration value",
        )
    return value
