#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
from typing import *

import immutables

from edb import errors
from edb.edgeql.qltypes import ConfigScope

from .ops import OpCode, Operation, SettingValue
from .ops import spec_to_json, to_json, from_json, set_value, to_edgeql
from .ops import value_from_json, value_to_json_value, coerce_single_value
from .spec import Spec, Setting, load_spec_from_schema
from .types import ConfigType, CompositeConfigType


__all__ = (
    'lookup',
    'Spec', 'Setting', 'SettingValue',
    'spec_to_json', 'to_json', 'to_edgeql', 'from_json', 'set_value',
    'value_from_json', 'value_to_json_value',
    'ConfigScope', 'OpCode', 'Operation',
    'ConfigType', 'CompositeConfigType',
    'load_spec_from_schema',
    'get_compilation_config',
    'coerce_single_value',
)


def lookup(
    name: str,
    *configs: Mapping[str, SettingValue],
    spec: Spec,
    allow_unrecognized: bool = False,
) -> Any:

    try:
        setting = spec[name]
    except (KeyError, TypeError):
        if allow_unrecognized:
            return None
        else:
            raise errors.ConfigurationError(
                f'unrecognized configuration parameter {name!r}')

    for c in configs:
        try:
            setting_value = c[name]
        except KeyError:
            pass
        else:
            return setting_value.value
    else:
        return setting.default


def get_compilation_config(
    config: Mapping[str, SettingValue],
    *,
    spec: Spec,
) -> immutables.Map[str, SettingValue]:
    return immutables.Map((
        (k, v)
        for k, v in config.items()
        if k in spec  # XXX!!!
        if spec[k].affects_compilation
    ))
