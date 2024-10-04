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
    Collection,
    Iterable,
    Iterator,
    Optional,
    TypeAlias,
)

import textwrap

from edb.common import ordered

from ..common import qname as qn
from ..common import quote_literal as ql

from . import base
from . import composites
from . import ddl
from . import tables


CompositeTypeName: TypeAlias = tuple[str, str]


class CompositeType(composites.CompositeDBObject):
    def __init__(
        self,
        name: CompositeTypeName,
        columns: Collection[tables.Column] = (),
    ):
        super().__init__(name)
        self._columns = ordered.OrderedSet(columns)

    def iter_columns(self) -> Iterator[tables.Column]:
        return iter(self._columns)


class TypeExists(base.Condition):
    def __init__(self, name: CompositeTypeName):
        self.name = name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                typname
            FROM
                pg_catalog.pg_type typ
                INNER JOIN pg_catalog.pg_namespace nsp
                    ON nsp.oid = typ.typnamespace
            WHERE
                nsp.nspname = {ql(self.name[0])}
                AND typ.typname = {ql(self.name[1])}
        ''')


def type_oid(name: CompositeTypeName) -> base.Query:
    if len(name) == 2:
        typnamespace, typname = name
    else:
        typname = name[0]
        typnamespace = 'pg_catalog'

    qry = textwrap.dedent(f'''\
        SELECT
            typ.oid
        FROM
            pg_catalog.pg_type typ
            INNER JOIN pg_catalog.pg_namespace nsp
                ON nsp.oid = typ.typnamespace
        WHERE
            typ.typname = {ql(typname)}
            AND nsp.nspname = {ql(typnamespace)}
    ''')

    return base.Query(qry)


CompositeTypeExists = TypeExists


class CompositeTypeAttributeExists(base.Condition):
    def __init__(
        self,
        type_name: CompositeTypeName,
        attribute_name: str,
    ):
        self.type_name = type_name
        self.attribute_name = attribute_name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                attribute_name
            FROM
                information_schema.attributes
            WHERE
                udt_schema = {ql(self.type_name[0])}
                AND udt_name = {ql(self.type_name[1])}
                AND attribute_name = {ql(self.attribute_name)}
        ''')


class CreateCompositeType(ddl.SchemaObjectOperation):
    def __init__(
        self,
        type: CompositeType,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(
            type.name, conditions=conditions, neg_conditions=neg_conditions
        )
        self.type = type

    def code(self) -> str:
        elems = [c.code(short=True) for c in self.type.iter_columns()]
        name = qn(*self.type.name)
        cols = ', '.join(c for c in elems)
        return f'CREATE TYPE {name} AS ({cols})'


class AlterCompositeTypeBaseMixin:
    def __init__(self, name: CompositeTypeName, **kwargs: Any):
        self.name = name

    def prefix_code(self) -> str:
        return f'ALTER TYPE {qn(*self.name)}'

    def __repr__(self) -> str:
        return '<%s.%s %s>' % (
            self.__class__.__module__, self.__class__.__name__, self.name)


class AlterCompositeTypeBase(AlterCompositeTypeBaseMixin, ddl.DDLOperation):
    def __init__(
        self,
        name: CompositeTypeName,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        ddl.DDLOperation.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions)
        AlterCompositeTypeBaseMixin.__init__(self, name=name)


class AlterCompositeTypeFragment(ddl.DDLOperation):
    def get_attribute_term(self) -> str:
        return 'ATTRIBUTE'


class AlterCompositeType(
    AlterCompositeTypeBaseMixin, base.CompositeCommandGroup
):
    def __init__(
        self,
        name: CompositeTypeName,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        base.CompositeCommandGroup.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions)
        AlterCompositeTypeBaseMixin.__init__(self, name=name)


class AlterCompositeTypeAddAttribute(  # type: ignore
    composites.AlterCompositeAddAttribute, AlterCompositeTypeFragment
):
    def code(self) -> str:
        return 'ADD {} {}'.format(
            self.get_attribute_term(), self.attribute.code(short=True))


class AlterCompositeTypeDropAttribute(
        composites.AlterCompositeDropAttribute, AlterCompositeTypeFragment):
    pass


class AlterCompositeTypeAlterAttributeType(
        composites.AlterCompositeAlterAttributeType,
        AlterCompositeTypeFragment):
    pass


class DropCompositeType(ddl.SchemaObjectOperation):
    def __init__(
        self,
        name: CompositeTypeName,
        *,
        cascade: bool = False,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions
        )
        self.cascade = cascade

    def code(self) -> str:
        cascade = ' CASCADE' if self.cascade else ''
        return f'DROP TYPE {qn(*self.name)}{cascade}'
