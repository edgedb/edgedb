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

from .ops import OpLevel, OpCode, Operation, lookup
from .ops import spec_to_json, to_json, from_json
from .ops import value_from_json
from .spec import Spec, Setting, load_spec_from_schema, generate_config_query
from .types import ConfigType


__all__ = (
    'get_settings', 'set_settings',
    'lookup',
    'Spec', 'Setting',
    'spec_to_json', 'to_json', 'from_json',
    'value_from_json',
    'OpLevel', 'OpCode', 'Operation',
    'ConfigType',
    'load_spec_from_schema',
    'generate_config_query',
)


_settings = Spec()


def get_settings() -> Spec:
    return _settings


def set_settings(settings: Spec) -> None:
    global _settings
    _settings = settings
