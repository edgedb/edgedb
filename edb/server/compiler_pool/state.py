#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


import typing

import immutables

from edb.schema import schema
from edb.server import config


ReflectionCache = immutables.Map[str, typing.Tuple[str, ...]]


class DatabaseState(typing.NamedTuple):
    name: str
    user_schema: schema.FlatSchema
    reflection_cache: ReflectionCache
    database_config: immutables.Map[str, config.SettingValue]


DatabasesState = immutables.Map[str, DatabaseState]


class PickledDatabaseState(typing.NamedTuple):
    user_schema_pickled: bytes
    reflection_cache: ReflectionCache
    database_config: immutables.Map[str, config.SettingValue]


class FailedStateSync(Exception):
    pass


class StateNotFound(Exception):
    pass


class IncompatibleClient(Exception):
    pass


REUSE_LAST_STATE_MARKER = b'REUSE_LAST_STATE_MARKER'
