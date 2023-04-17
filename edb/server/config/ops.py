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


import base64
import dataclasses
import json
from typing import *

import immutables

from edb import errors
from edb.common import enum
from edb.common import typeutils

from edb.ir import statypes

from edb.edgeql import codegen as qlcodegen
from edb.edgeql import qltypes

from edb.schema import objects as s_obj
from edb.schema import utils as s_utils

from . import spec
from . import types


class OpCode(enum.StrEnum):

    CONFIG_ADD = 'ADD'
    CONFIG_REM = 'REM'
    CONFIG_SET = 'SET'
    CONFIG_RESET = 'RESET'


class SettingValue(NamedTuple):

    name: str
    value: Any
    source: str
    scope: qltypes.ConfigScope


if TYPE_CHECKING:
    SettingsMap = immutables.Map[str, SettingValue]


class Operation(NamedTuple):

    opcode: OpCode
    scope: qltypes.ConfigScope
    setting_name: str
    value: Union[str, int, bool, Collection[Union[str, int, bool, None]], None]

    def get_setting(self, spec: spec.Spec) -> spec.Setting:
        try:
            return spec[self.setting_name]
        except KeyError:
            raise errors.ConfigurationError(
                f'unknown setting {self.setting_name!r}') from None

    def coerce_value(self, spec: spec.Spec, setting: spec.Setting, *,
                     allow_missing: bool = False):
        if issubclass(setting.type, types.ConfigType):
            try:
                return setting.type.from_pyvalue(
                    self.value, spec=spec, allow_missing=allow_missing)
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
                for v in self.value:  # type: ignore
                    if not isinstance(v, setting.type):
                        raise errors.ConfigurationError(
                            f'invalid value type for the '
                            f'{setting.name!r} setting')

                return frozenset(self.value)  # type: ignore
        else:
            if isinstance(self.value, setting.type):
                return self.value
            elif (isinstance(self.value, str) and
                    issubclass(setting.type, statypes.Duration)):
                return statypes.Duration(self.value)
            elif (isinstance(self.value, (str, int)) and
                    issubclass(setting.type, statypes.ConfigMemory)):
                return statypes.ConfigMemory(self.value)
            elif self.value is None and allow_missing:
                return None
            else:
                raise errors.ConfigurationError(
                    f'invalid value type for the {setting.name!r} setting')

    def coerce_global_value(
            self, *, allow_missing: bool = False) -> Optional[bytes]:
        if allow_missing and self.value is None:
            return None
        else:
            assert isinstance(self.value, str)
            b = base64.b64decode(self.value)
            # Input comes prefixed with length; if the length is -1,
            # the value has explicitly been set to {}.
            return b[4:] if b[:4] != b'\xff\xff\xff\xff' else None

    def apply(self, spec: spec.Spec, storage: SettingsMap) -> SettingsMap:

        allow_missing = (
            self.opcode is OpCode.CONFIG_REM
            or self.opcode is OpCode.CONFIG_RESET
        )

        if self.scope != qltypes.ConfigScope.GLOBAL:
            setting = self.get_setting(spec)
            value = self.coerce_value(
                spec, setting, allow_missing=allow_missing)
        else:
            setting = None
            value = self.coerce_global_value(allow_missing=allow_missing)

        if self.opcode is OpCode.CONFIG_SET:
            if setting and issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE SET on a non-primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            storage = self._set_value(storage, value)

        elif self.opcode is OpCode.CONFIG_RESET:
            if setting and issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE RESET on a non-primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            try:
                storage = storage.delete(self.setting_name)
            except KeyError:
                pass

        elif self.opcode is OpCode.CONFIG_ADD:
            assert setting
            if not issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE SET += on a primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            exist_setting = storage.get(self.setting_name)
            if exist_setting is not None:
                exist_value = exist_setting.value
            else:
                exist_value = setting.default

            if value in exist_value:
                props = []
                for f in dataclasses.fields(setting.type):
                    if f.compare:
                        props.append(f.name)

                if len(props) > 1:
                    props_s = f' ({", ".join(props)}) violate'
                else:
                    props_s = f'.{props[0]} violates'

                raise errors.ConstraintViolationError(
                    f'{setting.type.__name__}{props_s} '
                    f'exclusivity constraint'
                )

            new_value = exist_value | {value}
            storage = self._set_value(storage, new_value)

        elif self.opcode is OpCode.CONFIG_REM:
            assert setting
            if not issubclass(setting.type, types.ConfigType):
                raise errors.InternalServerError(
                    f'unexpected CONFIGURE SET -= on a primitive '
                    f'configuration parameter: {self.setting_name}'
                )

            exist_setting = storage.get(self.setting_name)
            if exist_setting is not None:
                exist_value = exist_setting.value
            else:
                exist_value = setting.default
            new_value = exist_value - {value}
            storage = self._set_value(storage, new_value)

        return storage

    def _set_value(
        self,
        storage: SettingsMap,
        value: Any,
    ) -> SettingsMap:

        if self.scope is qltypes.ConfigScope.INSTANCE:
            source = 'system override'
        elif self.scope is qltypes.ConfigScope.DATABASE:
            source = 'database'
        elif self.scope is qltypes.ConfigScope.SESSION:
            source = 'session'
        elif self.scope is qltypes.ConfigScope.GLOBAL:
            source = 'global'
        else:
            raise AssertionError(f'unexpected config scope: {self.scope}')

        return set_value(
            storage,
            self.setting_name,
            value,
            source=source,
            scope=self.scope,
        )

    @classmethod
    def from_json(cls, json_value: str) -> Operation:
        op_str, scope_str, name, value = json.loads(json_value)
        return Operation(
            opcode=OpCode(op_str),
            scope=qltypes.ConfigScope(scope_str),
            setting_name=name,
            value=value,
        )


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
        elif issubclass(setting.type, statypes.Duration):
            typeid = s_obj.get_known_type_id('std::duration')
        elif issubclass(setting.type, statypes.ConfigMemory):
            typeid = s_obj.get_known_type_id('cfg::memory')
        else:
            raise RuntimeError(
                f'cannot serialize type for config setting {setting.name}')

        typemod = qltypes.TypeModifier.SingletonType
        if setting.set_of:
            typemod = qltypes.TypeModifier.SetOfType

        dct[setting.name] = {
            'default': value_to_json_value(setting, setting.default),
            'internal': setting.internal,
            'system': setting.system,
            'typeid': str(typeid),
            'typemod': str(typemod),
            'backend_setting': setting.backend_setting,
            'report': setting.report,
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
        elif issubclass(setting.type, statypes.Duration) and value is not None:
            return value.to_iso8601()
        elif (issubclass(setting.type, statypes.ConfigMemory) and
                value is not None):
            return value.to_str()
        else:
            return value


def value_from_json_value(spec: spec.Spec, setting: spec.Setting, value: Any):
    if setting.set_of:
        if issubclass(setting.type, types.ConfigType):
            return frozenset(
                setting.type.from_json_value(v, spec=spec) for v in value)
        else:
            return frozenset(value)
    else:
        if issubclass(setting.type, types.ConfigType):
            return setting.type.from_json_value(value, spec=spec)
        elif issubclass(setting.type, statypes.Duration):
            return statypes.Duration.from_iso8601(value)
        elif issubclass(setting.type, statypes.ConfigMemory):
            return statypes.ConfigMemory(value)
        else:
            return value


def value_from_json(spec, setting, value: str):
    return value_from_json_value(spec, setting, json.loads(value))


def value_to_edgeql_const(setting: spec.Setting, value: Any) -> str:
    if isinstance(setting.type, types.ConfigType):
        raise NotImplementedError(
            'cannot render non-scalar configuration value'
        )

    ql = s_utils.const_ast_from_python(value)
    return qlcodegen.generate_source(ql)


def to_json(
    spec: spec.Spec,
    storage: Mapping[str, SettingValue],
    *,
    setting_filter: Optional[Callable[[SettingValue], bool]] = None,
    include_source: bool = True,
) -> str:
    dct = {}
    for name, value in storage.items():
        setting = spec[name]
        if setting_filter is None or setting_filter(value):
            val = value_to_json_value(setting, value.value)
            if include_source:
                dct[name] = {
                    'name': name,
                    'source': value.source,
                    'scope': str(value.scope),
                    'value': val,
                }
            else:
                dct[name] = val
    return json.dumps(dct)


def from_json(spec: spec.Spec, js: str) -> SettingsMap:
    base: SettingsMap = immutables.Map()
    with base.mutate() as mm:
        dct = json.loads(js)

        if not isinstance(dct, dict):
            raise errors.ConfigurationError(
                'invalid JSON: top-level dict was expected')

        for key, value in dct.items():
            setting = spec.get(key)
            if setting is None:
                raise errors.ConfigurationError(
                    f'invalid JSON: unknown setting name {key!r}')

            mm[key] = SettingValue(
                name=key,
                value=value_from_json_value(spec, setting, value['value']),
                source=value['source'],
                scope=qltypes.ConfigScope(value['scope']),
            )

    return mm.finish()


def to_edgeql(
    spec: spec.Spec,
    storage: Mapping[str, SettingValue],
) -> str:
    stmts = []

    for name, value in storage.items():
        setting = spec[name]
        val = value_to_edgeql_const(setting, value.value)
        stmt = f'CONFIGURE {value.scope.to_edgeql()} SET {name} := {val};'
        stmts.append(stmt)

    return '\n'.join(stmts)


def set_value(
    storage: SettingsMap,
    name: str,
    value: Any,
    source: str,
    scope: qltypes.ConfigScope,
) -> SettingsMap:

    return storage.set(
        name,
        SettingValue(name=name, value=value, source=source, scope=scope),
    )
