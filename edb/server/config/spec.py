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

import collections.abc
import dataclasses
import json
from typing import *

from edb.edgeql import compiler as qlcompiler
from edb.ir import staeval
from edb.ir import statypes
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema

from edb.common.typeutils import downcast

from . import types


SETTING_TYPES = {str, int, bool, statypes.Duration, statypes.ConfigMemory}


@dataclasses.dataclass(frozen=True, eq=True)
class Setting:

    name: str
    type: type | types.ConfigTypeSpec
    default: Any
    schema_type_name: Optional[sn.Name] = None
    set_of: bool = False
    system: bool = False
    internal: bool = False
    requires_restart: bool = False
    backend_setting: Optional[str] = None
    report: bool = False
    affects_compilation: bool = False
    enum_values: Optional[Sequence[str]] = None

    def __post_init__(self) -> None:
        if (self.type not in SETTING_TYPES and
                not isinstance(self.type, types.ConfigTypeSpec)):
            raise ValueError(
                f'invalid config setting {self.name!r}: '
                f'type is expected to be either one of {{str, int, bool}} '
                f'or an edb.server.config.types.ConfigType subclass')

        if self.set_of:
            if not isinstance(self.default, frozenset):
                raise ValueError(
                    f'invalid config setting {self.name!r}: "SET OF" settings '
                    f'must have frozenset() as a default value, got '
                    f'{self.default!r}')

            if self.default:
                # SET OF settings shouldn't have non-empty defaults,
                # as otherwise there are multiple semantical ambiguities:
                # * Can a user add a new element to the set?
                # * What happens of a user discards all elements from the set?
                #   Does the set become non-empty because the default would
                #   propagate?
                # * etc.
                raise ValueError(
                    f'invalid config setting {self.name!r}: "SET OF" settings '
                    f'should not have defaults')

        else:
            if (not self.backend_setting and
                    isinstance(self.type, type) and
                    not isinstance(self.default, self.type)):
                raise ValueError(
                    f'invalid config setting {self.name!r}: '
                    f'the default {self.default!r} '
                    f'is not instance of {self.type}')

        if self.report and not self.system:
            raise ValueError('only instance settings can be reported')


class Spec(collections.abc.Mapping):

    def __init__(self, *settings: Setting):
        self._settings = tuple(settings)
        self._by_name = {s.name: s for s in self._settings}
        self._types_by_name: Dict[str, types.ConfigTypeSpec] = {}

        for s in self._settings:
            if isinstance(s.type, types.ConfigTypeSpec):
                self._register_type(s.type)

    def _register_type(self, t: types.ConfigTypeSpec) -> None:
        self._types_by_name[t.name] = t
        for subclass in t.children:
            self._types_by_name[subclass.name] = downcast(
                types.ConfigTypeSpec, subclass)

        for field in t.fields.values():
            f_type = field.type
            if isinstance(f_type, types.ConfigTypeSpec):
                self._register_type(f_type)

    def get_type_by_name(self, name: str) -> types.ConfigTypeSpec:
        return self._types_by_name[name]

    def __iter__(self):
        return iter(self._by_name)

    def __getitem__(self, name: str) -> Setting:
        return self._by_name[name]

    def __contains__(self, name: object) -> bool:
        return name in self._by_name

    def __len__(self) -> int:
        return len(self._settings)


def load_spec_from_schema(schema: s_schema.Schema) -> Spec:
    cfg = schema.get('cfg::Config', type=s_objtypes.ObjectType)
    settings = []

    for ptr_name, p in cfg.get_pointers(schema).items(schema):
        pn = str(ptr_name)
        if pn in ('id', '__type__') or p.get_computable(schema):
            continue

        ptype = p.get_target(schema)
        assert ptype

        pytype: type | types.ConfigTypeSpec
        if isinstance(ptype, s_objtypes.ObjectType):
            pytype = staeval.object_type_to_spec(
                ptype, schema,
                spec_class=types.ConfigTypeSpec,
            )
        else:
            pytype = staeval.scalar_type_to_python_type(ptype, schema)

        attributes = {
            a: json.loads(v.get_value(schema))
            for a, v in p.get_annotations(schema).items(schema)
        }

        ptr_card = p.get_cardinality(schema)
        set_of = ptr_card.is_multi()
        backend_setting = attributes.get(
            sn.QualName('cfg', 'backend_setting'), None)

        deflt_expr = p.get_default(schema)
        if deflt_expr is not None:
            deflt = qlcompiler.evaluate_to_python_val(
                deflt_expr.text, schema=schema)
            if set_of and not isinstance(deflt, frozenset):
                deflt = frozenset((deflt,))
        else:
            if set_of:
                deflt = frozenset()
            elif backend_setting is None:
                raise RuntimeError(f'cfg::Config.{pn} has no default')
            else:
                deflt = None

        setting = Setting(
            pn,
            type=pytype,
            schema_type_name=ptype.get_name(schema),
            set_of=set_of,
            internal=attributes.get(sn.QualName('cfg', 'internal'), False),
            system=attributes.get(sn.QualName('cfg', 'system'), False),
            requires_restart=attributes.get(
                sn.QualName('cfg', 'requires_restart'), False),
            backend_setting=backend_setting,
            report=attributes.get(
                sn.QualName('cfg', 'report'), None),
            affects_compilation=attributes.get(
                sn.QualName('cfg', 'affects_compilation'), False),
            default=deflt,
            enum_values=(
                ptype.get_enum_values(schema)
                if isinstance(ptype, s_scalars.ScalarType)
                else None
            ),
        )

        settings.append(setting)

    return Spec(*settings)
