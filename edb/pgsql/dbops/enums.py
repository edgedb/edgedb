#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    TypeAlias,
)

import textwrap

from ..common import qname as qn
from ..common import quote_literal as ql

from . import base
from . import ddl


EnumName: TypeAlias = tuple[str, str]


class EnumExists(base.Condition):
    def __init__(self, name: EnumName) -> None:
        self.name = name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                t.typname
            FROM
                pg_catalog.pg_type t
                INNER JOIN pg_namespace nsp
                    ON (t.typnamespace = nsp.oid)
            WHERE
                nsp.nspname = {ql(self.name[0])}
                AND t.typname = {ql(self.name[1])}
                AND t.typtype = 'e'
        ''')


class Enum(base.DBObject):
    def __init__(
        self,
        name: EnumName,
        values: Sequence[str],
        *,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.name = name
        self.values = values
        super().__init__(metadata=metadata)


class CreateEnum(ddl.SchemaObjectOperation):
    def __init__(
        self,
        enum: Enum,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(
            enum.name, conditions=conditions, neg_conditions=neg_conditions)
        self.values = enum.values

    def code(self) -> str:
        vals = ', '.join(ql(v) for v in self.values)
        return f'CREATE TYPE {qn(*self.name)} AS ENUM ({vals})'


class AlterEnum(ddl.DDLOperation):
    def __init__(
        self,
        name: EnumName,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.name = name

    def prefix_code(self) -> str:
        return f'ALTER TYPE {qn(*self.name)}'

    def __repr__(self) -> str:
        return '<edb.sync.%s %s>' % (self.__class__.__name__, self.name)


class AlterEnumAddValue(AlterEnum):
    def __init__(
        self,
        name: EnumName,
        value: str,
        *,
        before: Optional[str] = None,
        after: Optional[str] = None,
        conditional: bool = False,
    ):
        super().__init__(name)
        self.value = value
        self.before = before
        self.after = after
        self.conditional = conditional

    def code(self) -> str:
        code = self.prefix_code()
        code += ' ADD VALUE'
        if self.conditional:
            code += ' IF NOT EXISTS'
        code += f' {ql(self.value)}'
        if self.before:
            code += f' BEFORE {ql(self.before)}'
        elif self.after:
            code += f' AFTER {ql(self.after)}'

        return code


class DropEnum(ddl.SchemaObjectOperation):
    def code(self) -> str:
        return f'DROP TYPE {qn(*self.name)}'
