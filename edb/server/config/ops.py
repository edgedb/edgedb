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


import dataclasses
import json
from typing import *

import immutables

from edb import errors
from edb.common import enum
from edb.common import typeutils

from edb.edgeql import qltypes
from edb.schema import objects as s_obj

from . import spec
from . import types


if TYPE_CHECKING:
    Mapping_T = TypeVar("Mapping_T", bound=Mapping[str, str])


class OpLevel(enum.StrEnum):

    SESSION = 'SESSION'
    SYSTEM = 'SYSTEM'


class OpCode(enum.StrEnum):

    CONFIG_ADD = 'ADD'
    CONFIG_REM = 'REM'
    CONFIG_SET = 'SET'
    CONFIG_RESET = 'RESET'


class Operation(NamedTuple):

    opcode: OpCode
    level: OpLevel
    setting_name: str
    value: Union[str, int, bool, None]

    def get_setting(self, spec: spec.Spec):
        try:
            return spec[self.setting_name]
        except KeyError:
            raise errors.ConfigurationError(
                f'unknown setting {self.setting_name!r}') from None

    def coerce_value(self, setting: spec.Setting, *,
                     allow_missing: bool = False):
        if issubclass(setting.type, types.ConfigType):
            try:
                return setting.type.from_pyvalue(
                    self.value, allow_missing=allow_missing)
            except (ValueError, TypeError):
                raise errors.ConfigurationError(
                    f'invalid value type for the {setting.name!r} setting')
        elif setting.set_of:
            if self.value is None and allow_missing:
                return None
            elif not typeutils.is_container(self.value):
                raise errors.ConfigurationError(
                    f'invalid value type for the '
                    f'{setting.name!r} setting')
            else:
                for v in self.value:
                    if not isinstance(v, setting.type):
                        raise errors.ConfigurationError(
                            f'invalid value type for the '
                            f'{setting.name!r} setting')

                return frozenset(self.value)
        else:
            if isinstance(self.value, setting.type):
                return self.value
            elif self.value is None and allow_missing:
                return None
            else:
                raise errors.ConfigurationError(
                    f'invalid value type for the {setting.name!r} setting')

    def apply(self, spec: spec.Spec,
              storage: Mapping_T) -> Mapping_T:

        setting = self.get_setting(spec)
        allow_missing = (
            self.opcode is OpCode.CONFIG_REM
            or self.opcode is OpCode.CONFIG_RESET
        )

        value = self.coerce_value(setting, allow_missing=allow_missing)

        if self.opcode is OpCode.CONFIG_SET:
            if issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE SET on a non-primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            storage = storage.set(self.setting_name, value)

        elif self.opcode is OpCode.CONFIG_RESET:
            if issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE RESET on a non-primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            try:
                storage = storage.delete(self.setting_name)
            except KeyError:
                pass

        elif self.opcode is OpCode.CONFIG_ADD:
            if not issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE SET += on a primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            exist_value = storage.get(self.setting_name, setting.default)
            if value in exist_value:
                props = []
                for f in dataclasses.fields(setting.type):
                    if f.compare:
                        props.append(f.name)

                if len(props) > 1:
                    props = f' ({", ".join(props)}) violate'
                else:
                    props = f'.{props[0]} violates'

                raise errors.ConstraintViolationError(
                    f'{setting.type.__name__}{props} '
                    f'exclusivity constriant'
                )

            new_value = exist_value | {value}
            storage = storage.set(self.setting_name, new_value)

        elif self.opcode is OpCode.CONFIG_REM:
            if not issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE SET -= on a primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            exist_value = storage.get(self.setting_name, setting.default)
            new_value = exist_value - {value}
            storage = storage.set(self.setting_name, new_value)

        return storage

    @classmethod
    def from_json(cls, json_value: str) -> Operation:
        op_str, lev_str, name, value = json.loads(json_value)
        return Operation(OpCode(op_str), OpLevel(lev_str), name, value)


def spec_to_json(spec: spec.Spec):
    dct = {}

    for setting in spec.values():
        if issubclass(setting.type, str):
            typeid = s_obj.get_known_type_id('std::str')
        elif issubclass(setting.type, bool):
            typeid = s_obj.get_known_type_id('std::bool')
        elif issubclass(setting.type, int):
            typeid = s_obj.get_known_type_id('std::int64')
        elif issubclass(setting.type, types.ConfigType):
            typeid = setting.type.get_edgeql_typeid()
        else:
            raise RuntimeError(
                f'cannot serialize type for config setting {setting.name}')

        typemod = qltypes.TypeModifier.SINGLETON
        if setting.set_of:
            typemod = qltypes.TypeModifier.SET_OF

        dct[setting.name] = {
            'default': value_to_json_value(setting, setting.default),
            'internal': setting.internal,
            'system': setting.system,
            'typeid': str(typeid),
            'typemod': str(typemod),
        }

    return json.dumps(dct)


def value_to_json_value(setting: spec.Setting, value: Any):
    if setting.set_of:
        if issubclass(setting.type, types.ConfigType):
            return [v.to_json_value() for v in value]
        else:
            return list(value)
    else:
        if issubclass(setting.type, types.ConfigType):
            return value.to_json_value()
        else:
            return value


def value_from_json_value(setting: spec.Setting, value: Any):
    if setting.set_of:
        if issubclass(setting.type, types.ConfigType):
            return frozenset(setting.type.from_json_value(v) for v in value)
        else:
            return frozenset(value)
    else:
        if issubclass(setting.type, types.ConfigType):
            return setting.type.from_json_value(value)
        else:
            return value


def value_from_json(setting, value: str):
    return value_from_json_value(setting, json.loads(value))


def to_json(spec: spec.Spec, storage: Mapping) -> str:
    dct = {}
    for name, value in storage.items():
        setting = spec[name]
        dct[name] = value_to_json_value(setting, value)
    return json.dumps(dct)


def from_json(spec: spec.Spec, js: str) -> Mapping:
    with immutables.Map().mutate() as mm:
        dct = json.loads(js)

        if not isinstance(dct, dict):
            raise errors.ConfigurationError(
                'invalid JSON: top-level dict was expected')

        for key, value in dct.items():
            setting = spec.get(key)
            if setting is None:
                raise errors.ConfigurationError(
                    f'invalid JSON: unknown setting name {key!r}')

            mm[key] = value_from_json_value(setting, value)

    return mm.finish()


def lookup(spec: spec.Spec, name: str, *configs: Mapping,
           allow_unrecognized: bool = False):
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
            return c[name]
        except KeyError:
            pass

    return setting.default
