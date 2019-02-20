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


import enum
import json
import typing

import immutables

from edb import errors

from . import spec
from . import types


class OpLevel(enum.Enum):

    SESSION = enum.auto()
    SYSTEM = enum.auto()


class OpCode(enum.Enum):

    CONFIG_ADD = enum.auto()
    CONFIG_REM = enum.auto()
    CONFIG_SET = enum.auto()


class Operation(typing.NamedTuple):

    opcode: OpCode
    level: OpLevel
    setting_name: str
    value: typing.Union[str, int, bool]

    def get_setting(self, spec: spec.Spec):
        try:
            return spec[self.setting_name]
        except KeyError:
            raise errors.ConfigurationError(
                f'unknown setting {self.setting_name!r}') from None

    def coerce_value(self, setting: spec.Setting):
        if issubclass(setting.type, types.ConfigType):
            try:
                return setting.type.from_pyvalue(self.value)
            except (ValueError, TypeError):
                raise errors.ConfigurationError(
                    f'invalid value type for the {setting.name!r} setting')
        else:
            if isinstance(self.value, setting.type):
                return self.value
            else:
                raise errors.ConfigurationError(
                    f'invalid value type for the {setting.name!r} setting')

    def apply(self, spec: spec.Spec,
              storage: typing.Mapping) -> typing.Mapping:

        setting = self.get_setting(spec)
        value = self.coerce_value(setting)

        if self.opcode is OpCode.CONFIG_SET:
            assert not setting.set_of
            storage = storage.set(self.setting_name, value)

        elif self.opcode is OpCode.CONFIG_ADD:
            assert setting.set_of
            exist_value = storage.get(self.setting_name, setting.default)
            new_value = exist_value | {value}
            storage = storage.set(self.setting_name, new_value)

        elif self.opcode is OpCode.CONFIG_REM:
            assert setting.set_of
            exist_value = storage.get(self.setting_name, setting.default)
            new_value = exist_value - {value}
            storage = storage.set(self.setting_name, new_value)

        return storage


def spec_to_json(spec: spec.Spec):
    dct = {}
    for setting in spec.values():
        dct[setting.name] = {
            'default': [
                value_to_json_value(setting, setting.default),
                value_to_json_edgeql_value(setting, setting.default),
            ],
            'internal': setting.internal,
            'system': setting.system,
            'set_of': setting.set_of,
        }
    return json.dumps(dct)


def value_to_json_value(setting: spec.Setting, value: object):
    if setting.set_of:
        if issubclass(setting.type, types.ConfigType):
            return [v.to_json() for v in value]
        else:
            return list(value)
    else:
        if issubclass(setting.type, types.ConfigType):
            return value.to_json()
        else:
            return value


def value_from_json_value(setting: spec.Setting, value: object):
    if setting.set_of:
        if issubclass(setting.type, types.ConfigType):
            return frozenset(setting.type.from_json(v) for v in value)
        else:
            return frozenset(value)
    else:
        if issubclass(setting.type, types.ConfigType):
            return setting.type.from_json(value)
        else:
            return value


def value_from_json(setting, value: str):
    return value_from_json_value(setting, json.loads(value))


def value_to_json(setting: spec.Setting, value: object):
    return json.dumps(value_to_json_value(setting, value))


def value_to_json_edgeql_value(setting: spec.Setting, value: object):
    def py_to_edgeql(v):
        if isinstance(v, bool):
            return repr(v).lower()
        return repr(v)

    if setting.set_of:
        if issubclass(setting.type, types.ConfigType):
            return '{' + ','.join(v.to_edgeql() for v in value) + '}'
        else:
            return '{' + ','.join(py_to_edgeql(v) for v in value) + '}'
    else:
        if issubclass(setting.type, types.ConfigType):
            return value.to_edgeql()
        else:
            return py_to_edgeql(value)


def value_to_json_edgeql(setting: spec.Setting, value: object):
    return json.dumps(value_to_json_edgeql_value(setting, value))


def to_json(spec: spec.Spec, storage: typing.Mapping) -> str:
    dct = {}
    for name, value in storage.items():
        setting = spec[name]
        dct[name] = [
            value_to_json_value(setting, value),
            value_to_json_edgeql_value(setting, value)
        ]
    return json.dumps(dct)


def from_json(spec: spec.Spec, js: str) -> typing.Mapping:
    with immutables.Map().mutate() as mm:
        dct = json.loads(js)

        if not isinstance(dct, dict):
            raise errors.ConfigurationError(
                'invalid JSON: top-level dict was expected')

        for key, value in dct.items():
            if not isinstance(value, list) or len(value) != 2:
                raise errors.ConfigurationError(
                    f'invalid JSON: invalid setting value {value!r} '
                    f'(a two-element list was expected)')

            setting = spec.get(key)
            if setting is None:
                raise errors.ConfigurationError(
                    f'invalid JSON: unknown setting name {key!r}')

            mm[key] = value_from_json_value(setting, value[0])

    return mm.finish()


def lookup(spec: spec.Spec, name: str, *configs: typing.Mapping):
    try:
        setting = spec[name]
    except KeyError:
        raise errors.ConfigurationError(f'unknown setting {name!r}')

    for c in configs:
        try:
            return c[name]
        except KeyError:
            pass

    return setting.default
