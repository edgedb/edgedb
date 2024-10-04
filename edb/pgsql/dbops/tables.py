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

import collections
import textwrap
from typing import (
    Generic,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    TypeAlias,
    TypeVar,
)

from edb.common import ordered

from ..common import qname as qn
from ..common import quote_ident as qi
from ..common import quote_literal as ql

from .. import ast as pgast

from . import base
from . import composites
from . import constraints
from . import ddl


TableConstraint_T = TypeVar("TableConstraint_T", bound="TableConstraint")
TableName: TypeAlias = tuple[str, ...]
ColumnName: TypeAlias = str


class Table(composites.CompositeDBObject):
    bases: ordered.OrderedSet[Table]
    constraints: ordered.OrderedSet[SingleTableConstraint]

    # Columns from bases and self
    all_columns: collections.OrderedDict[str, Column]

    def __init__(
        self,
        name: TableName,
        *,
        columns: Optional[Iterable[Column]] = None,
        bases: Optional[ordered.OrderedSet[Table]] = None,
        constraints: Optional[ordered.OrderedSet[SingleTableConstraint]] = None
    ) -> None:
        self.bases = ordered.OrderedSet(bases or [])
        self.constraints = ordered.OrderedSet(constraints or [])
        super().__init__(name, columns=columns)

        self.all_columns = collections.OrderedDict(
            (c.name, c) for c in self.iter_columns()
        )

    def iter_columns(
        self,
        writable_only: bool = False,
        only_self: bool = False,
    ) -> Iterable[Column]:
        cols: collections.OrderedDict[ColumnName, Column] = (
            collections.OrderedDict()
        )
        cols.update(
            (c.name, c)
            for c in self._columns
            if not writable_only or not c.readonly
        )

        if not only_self:
            for base in reversed(self.bases):
                cols.update(
                    (name, bc)
                    for name, bc in base.all_columns.items()
                    if not writable_only or not bc.readonly
                )

        return ordered.OrderedSet(cols.values())

    def __iter__(self) -> Iterator[Column]:
        return iter(self._columns)

    def add_bases(self, iterable: Iterable[Table]) -> None:
        self.bases.update(iterable)
        self.all_columns = collections.OrderedDict(
            (c.name, c) for c in self.iter_columns()
        )

    def add_columns(self, iterable: Iterable[Column]) -> None:
        super().add_columns(iterable)
        self.all_columns = collections.OrderedDict(
            (c.name, c) for c in self.iter_columns()
        )

    def add_constraint(self, const: SingleTableConstraint) -> None:
        self.constraints.add(const)

    def get_column(self, name: ColumnName) -> Optional[Column]:
        return self.all_columns.get(name)

    def get_type(self) -> str:
        return 'TABLE'

    def get_id(self) -> str:
        return qn(*self.name)

    @property
    def record(self) -> composites.Record:
        return composites.Record(
            self.__class__.__name__ + '_record',
            list(self.all_columns), default=base.Default)

    @property
    def system_catalog(self) -> str:
        return 'pg_class'

    @property
    def oid_type(self) -> str:
        return 'regclass'

    def __repr__(self) -> str:
        return f'<db.Table {self.name} at {id(self):0x}>'


class InheritableTableObject(base.InheritableDBObject):
    name: str

    @property
    def name_in_catalog(self) -> str:
        return self.name


class Column(base.DBObject):
    def __init__(
        self,
        name: ColumnName,
        type: str | tuple[str, str],
        required: bool = False,
        default: Optional[str] = None,
        constraints: Sequence[ColumnConstraint] = (),
        readonly: bool = False,
        comment: Optional[str] = None,
    ) -> None:
        self.name = name
        self.type = type
        self.required = required
        self.default = default
        self.constraints = constraints
        self.readonly = readonly
        self.comment = comment

    def add_constraint(self, constraint: ColumnConstraint) -> None:
        self.constraints = list(self.constraints) + [constraint]

    def code(self, short: bool = False) -> str:
        code = f"{qi(self.name)} {self.type}"
        if not short:
            if self.required:
                code += ' NOT NULL'

            if self.default is not None:
                code += f' DEFAULT {self.default}'

            for c in self.constraints:
                code += ' ' + c.code()
        return code

    def generate_extra_composite(
        self, block: base.PLBlock, alter_table: base.CompositeCommandGroup
    ) -> None:
        if self.comment is not None:
            assert isinstance(alter_table, AlterTable)
            col = TableColumn(table_name=alter_table.name, column=self)
            cmd = ddl.Comment(object=col, text=self.comment)
            cmd.generate(block)

    def __repr__(self) -> str:
        return '<%s.%s "%s" %s>' % (
            self.__class__.__module__, self.__class__.__name__, self.name,
            self.type)


class TableColumn(base.DBObject):
    def __init__(self, table_name: TableName, column: Column) -> None:
        self.table_name = table_name
        self.column = column

    def get_type(self) -> str:
        return 'COLUMN'

    def get_id(self) -> str:
        return qn(
            self.table_name[0], self.table_name[1], self.column.name
        )


class ColumnConstraint:
    def __init__(self, constraint_name: str):
        self.constraint_name = constraint_name

    def code(self) -> str:
        raise NotImplementedError()


class GeneratedConstraint(ColumnConstraint):
    def __init__(self, constraint_name: str, expr: str) -> None:
        super().__init__(constraint_name)
        self.expr = expr

    def code(self) -> str:
        return (
            f'CONSTRAINT {self.constraint_name} '
            f'GENERATED ALWAYS AS ({self.expr}) STORED'
        )


class TableConstraint(constraints.Constraint):
    def generate_extra(self, block: base.PLBlock) -> None:
        pass

    def get_subject_type(self) -> str:
        return ''  # For table constraints the accepted syntax is
        # simply CONSTRAINT ON "{tab_name}", not
        # CONSTRAINT ON TABLE, unlike constraints on
        # other objects.


class SingleTableConstraint(TableConstraint):

    def constraint_code(self, block: base.PLBlock) -> str:
        raise NotImplementedError()


class PrimaryKey(SingleTableConstraint):
    def __init__(
        self, table_name: TableName, columns: Sequence[str | pgast.Star]
    ) -> None:
        super().__init__(table_name)
        self.columns = columns

    def constraint_code(self, block: base.PLBlock) -> str:
        cols = ', '.join(qi(c) for c in self.columns)
        return f'PRIMARY KEY ({cols})'


class UniqueConstraint(SingleTableConstraint):
    def __init__(
        self, table_name: TableName, columns: Sequence[str | pgast.Star]
    ) -> None:
        super().__init__(table_name)
        self.columns = columns

    def constraint_code(self, block: base.PLBlock) -> str:
        cols = ', '.join(qi(c) for c in self.columns)
        return f'UNIQUE ({cols})'


class CheckConstraint(SingleTableConstraint):
    def __init__(
        self,
        table_name: TableName,
        constraint_name: str,
        expr: base.Query | str,
        inherit: bool = True,
    ) -> None:
        super().__init__(table_name, constraint_name=constraint_name)
        self.expr = expr
        self.inherit = inherit

    def constraint_code(self, block: base.PLBlock) -> str:
        if isinstance(self.expr, base.Query):
            assert self.expr.type
            var = block.declare_var(self.expr.type)
            indent = len(var) + 5
            expr_text = textwrap.indent(self.expr.text, ' ' * indent).strip()
            block.add_command(f'{var} := ({expr_text})')

            code = f"'CHECK (' || {var} || ')'"
            if not self.inherit:
                code += " || ' NO INHERIT'"

            code = base.PLExpression(code)

        else:
            code = f'CHECK ({self.expr})'
            if not self.inherit:
                code += ' NO INHERIT'

        return code


class TableExists(base.Condition):
    def __init__(self, name: TableName) -> None:
        self.name = name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                tablename
            FROM
                pg_catalog.pg_tables
            WHERE
                schemaname = {ql(self.name[0])}
                AND tablename = {ql(self.name[1])}
        ''')


class TableInherits(base.Condition):
    def __init__(
        self,
        name: TableName,
        parent_name: TableName,
    ) -> None:
        self.name = name
        self.parent_name = parent_name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                c.relname
            FROM
                pg_class c
                INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace
                INNER JOIN pg_inherits i ON i.inhrelid = c.oid
                INNER JOIN pg_class pc ON i.inhparent = pc.oid
                INNER JOIN pg_namespace pns ON pns.oid = pc.relnamespace
            WHERE
                ns.nspname = {ql(self.name[0])}
                AND c.relname = {ql(self.name[1])}
                AND pns.nspname = {ql(self.parent_name[0])}
                AND pc.relname = {ql(self.parent_name[1])}
        ''')


class ColumnExists(base.Condition):
    def __init__(
        self,
        table_name: TableName,
        column_name: ColumnName,
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_schema = {ql(self.table_name[0])}
                AND table_name = {ql(self.table_name[1])}
                AND column_name = {ql(self.column_name)}
        ''')


class ColumnIsInherited(base.Condition):
    def __init__(
        self,
        table_name: TableName,
        column_name: ColumnName,
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                True
            FROM
                pg_class c
                INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace
                INNER JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE
                ns.nspname = {ql(self.table_name[0])}
                AND c.relname = {ql(self.table_name[1])}
                AND a.attname = {ql(self.column_name)}
                AND a.attinhcount > 0
        ''')


class CreateTable(ddl.SchemaObjectOperation):
    def __init__(
        self,
        table: Table,
        temporary: bool = False,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(
            table.name, conditions=conditions, neg_conditions=neg_conditions
        )
        self.table = table
        self.temporary = temporary

    def code_with_block(self, block: base.PLBlock) -> str:
        elems = [
            c.code() for c in self.table.iter_columns(only_self=True)
        ]
        for c in self.table.constraints:
            elems.append(c.constraint_code(block))

        name = qn(*self.table.name)
        temp = 'TEMPORARY ' if self.temporary else ''
        chunks = [f'CREATE {temp}TABLE {name} (', ')']
        if self.table.bases:
            bases = ','.join(qn(*b.name) for b in self.table.bases)
            chunks.append(f' INHERITS ({bases})')

        if any(isinstance(e, base.PLExpression) for e in elems):
            # Dynamic declaration
            elem_chunks: list[base.PLExpression | str] = []
            for e in elems:
                if isinstance(e, base.PLExpression):
                    elem_chunks.append(e)
                else:
                    elem_chunks.append(ql(e))

            chunks = [ql(c) for c in chunks]
            chunks.insert(1, " || ',' || ".join(elem_chunks))
            code = 'EXECUTE ' + ' || '.join(chunks)

        else:
            # Static declaration
            chunks.insert(1, ', '.join(elems))
            code = ''.join(chunks)

        return code


class AlterTableBaseMixin:

    name: TableName
    contained: bool

    def __init__(
        self, name: TableName, contained: bool = False
    ) -> None:
        self.name = name
        self.contained = contained

    def prefix_code(self) -> str:
        return 'ALTER TABLE %s%s' % (
            'ONLY ' if self.contained else '', qn(*self.name))

    def __repr__(self) -> str:
        return '<%s.%s %s>' % (
            self.__class__.__module__, self.__class__.__name__, self.name)


class AlterTableBase(AlterTableBaseMixin, ddl.DDLOperation):
    def __init__(
        self,
        name: TableName,
        *,
        contained: bool = False,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        ddl.DDLOperation.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions)
        AlterTableBaseMixin.__init__(self, name=name, contained=contained)

    def get_attribute_term(self) -> str:
        return 'COLUMN'


class AlterTableFragment(ddl.DDLOperation, base.CompositeCommand):
    def get_attribute_term(self) -> str:
        return 'COLUMN'

    def generate_extra_composite(
        self, block: base.PLBlock, group: base.CompositeCommandGroup
    ) -> None:
        pass


class AlterTable(
    AlterTableBaseMixin, ddl.DDLOperation, base.CompositeCommandGroup
):
    def __init__(
        self,
        name: TableName,
        *,
        contained: bool = False,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ):
        base.CompositeCommandGroup.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions)
        AlterTableBaseMixin.__init__(self, name=name, contained=contained)
        self.ops = self.commands

    add_operation = base.CompositeCommandGroup.add_command


class AlterTableAddParent(AlterTableFragment):
    def __init__(self, parent_name: TableName, **kwargs) -> None:
        super().__init__(**kwargs)
        self.parent_name = parent_name

    def code(self) -> str:
        return f'INHERIT {qn(*self.parent_name)}'

    def __repr__(self) -> str:
        return '<%s.%s %s>' % (
            self.__class__.__module__, self.__class__.__name__,
            self.parent_name)


class AlterTableDropParent(AlterTableFragment):
    def __init__(self, parent_name: TableName):
        self.parent_name = parent_name

    def code(self) -> str:
        return f'NO INHERIT {qn(*self.parent_name)}'

    def __repr__(self) -> str:
        return '<%s.%s %s>' % (
            self.__class__.__module__, self.__class__.__name__,
            self.parent_name)


class AlterTableAddColumn(  # type: ignore
        composites.AlterCompositeAddAttribute, AlterTableFragment):
    pass


class AlterTableDropColumn(
        composites.AlterCompositeDropAttribute, AlterTableFragment):
    pass


class AlterTableAlterColumnType(
        composites.AlterCompositeAlterAttributeType, AlterTableFragment):
    pass


class AlterTableAlterColumnNull(AlterTableFragment):
    def __init__(self, column_name: ColumnName, null) -> None:
        self.column_name = column_name
        self.null = null

    def code(self) -> str:
        action = 'DROP' if self.null else 'SET'
        return f'ALTER COLUMN {qi(self.column_name)} {action} NOT NULL'

    def __repr__(self) -> str:
        return '<{}.{} "{}" {} NOT NULL>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self.column_name, 'DROP' if self.null else 'SET')


class AlterTableAlterColumnDefault(AlterTableFragment):
    def __init__(self, column_name: ColumnName, default: Optional[str]):
        self.column_name = column_name
        self.default = default

    def code(self) -> str:
        if self.default is None:
            return f'ALTER COLUMN {qi(self.column_name)} DROP DEFAULT'
        else:
            return (f'ALTER COLUMN {qi(self.column_name)} '
                    f'SET DEFAULT {self.default}')

    def __repr__(self) -> str:
        return '<{}.{} "{}" {} DEFAULT{}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self.column_name, 'DROP' if self.default is None else 'SET', ''
            if self.default is None else ' {!r}'.format(self.default))


class TableConstraintCommand:
    pass


class TableConstraintExists(base.Condition):
    def __init__(self, table_name: TableName, constraint_name: str):
        self.table_name = table_name
        self.constraint_name = constraint_name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                True
            FROM
                pg_catalog.pg_constraint c
                INNER JOIN pg_catalog.pg_class t
                    ON c.conrelid = t.oid
                INNER JOIN pg_catalog.pg_namespace ns
                    ON t.relnamespace = ns.oid
            WHERE
                conname = {ql(self.constraint_name)}
                AND nspname = {ql(self.table_name[0])}
                AND relname = {ql(self.table_name[1])}
        ''')


class AlterTableAddConstraint(
    AlterTableFragment,
    TableConstraintCommand,
    Generic[TableConstraint_T],
):
    constraint: TableConstraint_T

    def __init__(self, constraint: TableConstraint_T):
        assert not isinstance(constraint, list)
        self.constraint = constraint

    def code_with_block(self, block: base.PLBlock) -> str:
        code = 'ADD '
        name = self.constraint.constraint_name()
        if name:
            code += f'CONSTRAINT {name} '

        constr_code = self.constraint.constraint_code(block)
        assert isinstance(constr_code, str)

        if not isinstance(constr_code, base.PLExpression):
            # Static declaration
            return code + constr_code
        else:
            # Dynamic declaration
            return base.PLExpression(f'{ql(code)} || {constr_code}')

    def generate_extra_composite(
        self, block: base.PLBlock, group: base.CompositeCommandGroup
    ) -> None:
        return self.constraint.generate_extra(block)

    def __repr__(self) -> str:
        return '<%s.%s %r>' % (
            self.__class__.__module__, self.__class__.__name__,
            self.constraint)


class AlterTableDropConstraint(AlterTableFragment, TableConstraintCommand):
    def __init__(self, constraint) -> None:
        self.constraint = constraint

    def code(self) -> str:
        return f'DROP CONSTRAINT {self.constraint.constraint_name()}'

    def __repr__(self) -> str:
        return '<%s.%s %r>' % (
            self.__class__.__module__, self.__class__.__name__,
            self.constraint)


class DropTable(ddl.SchemaObjectOperation):
    def code(self) -> str:
        return f'DROP TABLE {qn(*self.name)}'
