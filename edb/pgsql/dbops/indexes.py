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

import json
import textwrap
from typing import *  # NoQA

from edb.common import ordered
from edb.server import defines

from .. import common
from ..common import qname as qn
from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base
from . import ddl
from . import tables


class IndexColumn(base.DBObject):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<%s.%s "%s">' % (
            self.__class__.__module__, self.__class__.__name__, self.name)


class TextSearchIndexColumn(IndexColumn):
    def __init__(self, name, weight, language):
        super().__init__(name)
        self.weight = weight
        self.language = language

    def code(self, block: base.PLBlock) -> str:
        return (f"setweight(to_tsvector({ql(self.language)}, "
                f"coalesce({qi(self.name)}, '')), {ql(self.weight)})")


class Index(tables.InheritableTableObject):
    def __init__(
            self, name, table_name, unique=True, expr=None, predicate=None,
            inherit=False, metadata=None, columns=None):
        super().__init__(inherit=inherit, metadata=metadata)

        assert table_name[1] != 'feature'

        self.name = name
        self.table_name = table_name
        self._columns = ordered.OrderedSet()
        if columns:
            self.add_columns(columns)
        self.predicate = predicate
        self.unique = unique
        self.expr = expr

        if self.name_in_catalog != self.name:
            self.add_metadata('fullname', self.name)

    def rename(self, new_name):
        self.name = new_name
        if self.name_in_catalog != self.name:
            self.add_metadata('fullname', self.name)

    @property
    def name_in_catalog(self):
        return common.edgedb_name_to_pg_name(
            self.table_name[1] + '__' + self.name)

    def add_columns(self, columns):
        for col in columns:
            if isinstance(col, str):
                self._columns.add(col)
            else:
                self._columns.add(col.name)

    def declare_pl_desc_var(self, block: base.PLBlock) -> str:
        desc_var = block.declare_var(('edgedb', 'intro_index_desc_t'))
        cols = ', '.join(ql(c) for c in self.columns)

        code = textwrap.dedent(f'''\
            {desc_var}.table_name := ARRAY[
                {ql(self.table_name[0])}, {ql(self.table_name[1])}];
            {desc_var}.name := {ql(self.name)};
            {desc_var}.is_unique := {self.unique};
            {desc_var}.predicate := {ql(self.predicate) if self.predicate
                                     else 'NULL'};
            {desc_var}.expression := {ql(self.expr) if self.expr
                                      else 'NULL'};
            {desc_var}.columns := ARRAY[{cols}]::text[];
            {desc_var}.metadata := {ql(json.dumps(self.metadata))};
        ''')

        block.add_command(code)

        return desc_var

    @classmethod
    def creation_pl_code(cls, desc_var: str, block: base.PLBlock) -> str:
        unique = (
            f"(CASE WHEN {desc_var}.is_unique THEN 'UNIQUE '"
            f" ELSE '' END)"
        )
        schema_name = (
            f"quote_ident({desc_var}.table_name[1])"
        )
        table_name = (
            f"({schema_name} || '.' || quote_ident({desc_var}.table_name[2]))"
        )
        index_name = (
            f"quote_ident(edgedb.edgedb_name_to_pg_name("
            f"{desc_var}.table_name[2] || '__' || {desc_var}.name))"
        )
        expr = (
            f"COALESCE ({desc_var}.expression,\n"
            f"          (SELECT string_agg(quote_ident(c), ', ')\n"
            f"           FROM unnest({desc_var}.columns) AS c))"
        )

        return textwrap.dedent(f'''\
            EXECUTE
                'CREATE ' || {unique} || 'INDEX '
                || {index_name}
                || ' ON ' || {table_name}
                || '(' || {expr} || ')'
                ;
            EXECUTE
                'COMMENT ON INDEX ' || {schema_name} || '.' || {index_name}
                || ' IS '
                || quote_literal({ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)}
                || {desc_var}.metadata::text)
                ;
        ''')

    def creation_code(self, block: base.PLBlock) -> str:
        if self.expr:
            expr = self.expr
        else:
            expr = ', '.join(qi(c) for c in self.columns)

        code = '''
            CREATE {unique} INDEX {name}
                ON {table} ({expr}) {predicate}'''.format(

            unique='UNIQUE' if self.unique else '',
            name=qn(self.name_in_catalog),
            table=qn(*self.table_name),
            expr=expr,
            predicate=('WHERE {}'.format(self.predicate)
                       if self.predicate else '')
        )
        return code

    @property
    def columns(self):
        return list(self._columns)

    def get_type(self):
        return 'INDEX'

    def get_id(self):
        return qn(self.table_name[0], self.name_in_catalog)

    def get_oid(self):
        qry = textwrap.dedent(f'''\
            SELECT
                'pg_class'::regclass::oid AS classoid,
                i.indexrelid AS objectoid,
                0
            FROM
                pg_class AS c
                INNER JOIN pg_namespace AS ns ON ns.oid = c.relnamespace
                INNER JOIN pg_index AS i ON i.indrelid = c.oid
                INNER JOIN pg_class AS ic ON i.indexrelid = ic.oid
            WHERE
                ic.relname = {ql(self.name_in_catalog)}
                AND ns.nspname = {ql(self.table_name[0])}
                AND c.relname = {ql(self.table_name[1])}
        ''')

        return base.Query(text=qry)

    @classmethod
    def from_introspection(cls, table_name, index_data):
        name, is_unique, predicate, expression, columns, metadata = index_data

        if metadata:
            metadata = json.loads(metadata)
        else:
            metadata = {}

        if 'fullname' in metadata:
            name = metadata['fullname']

        index = cls(
            name=name, table_name=table_name, unique=is_unique,
            predicate=predicate, expr=expression, metadata=metadata)
        if columns:
            index.add_columns(columns)

        return index

    def copy(self):
        return self.__class__(
            name=self.name, table_name=self.table_name, unique=self.unique,
            expr=self.expr, predicate=self.predicate, columns=self.columns,
            metadata=self.metadata.copy()
            if self.metadata is not None else None)

    def __repr__(self):
        return \
            '<%(mod)s.%(cls)s table=%(table)s name=%(name)s ' \
            'cols=(%(cols)s) unique=%(uniq)s predicate=%(pred)s>' % \
            {'mod': self.__class__.__module__,
             'cls': self.__class__.__name__,
             'name': self.name,
             'cols': ','.join('%r' % c for c in self.columns),
             'uniq': self.unique,
             'pred': self.predicate,
             'table': '{}.{}'.format(*self.table_name)}


class TextSearchIndex(Index):
    def __init__(self, name, table_name, columns):
        super().__init__(name, table_name)
        self.add_columns(columns)

    def creation_code(self, block: base.PLBlock) -> str:
        code = \
            'CREATE INDEX %(name)s ON %(table)s ' \
            'USING gin((%(cols)s)) %(predicate)s' % \
            {'name': qn(self.name),
             'table': qn(*self.table_name),
             'cols': ' || '.join(c.code(block) for c in self.columns),
             'predicate': ('WHERE %s' % self.predicate
                           if self.predicate else '')}
        return code


class IndexExists(base.Condition):
    def __init__(self, index_name):
        self.index_name = index_name

    def code(self, block: base.PLBlock) -> str:
        return textwrap.dedent(f'''\
            SELECT
                   i.indexrelid
               FROM
                   pg_catalog.pg_index i
                   INNER JOIN pg_catalog.pg_class ic
                        ON ic.oid = i.indexrelid
                   INNER JOIN pg_catalog.pg_namespace icn
                        ON icn.oid = ic.relnamespace
               WHERE
                   icn.nspname = {ql(self.index_name[1])}
                   AND ic.relname = {ql(self.index_name[0])}
        ''')


class CreateIndex(tables.CreateInheritableTableObject):
    def __init__(self, index, *, conditional=False, **kwargs):
        super().__init__(index, **kwargs)
        self.index = index
        if conditional:
            self.neg_conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog)))

    def code(self, block: base.PLBlock) -> str:
        return self.index.creation_code(block)

    @classmethod
    def pl_code(cls, index_desc_var: str, block: base.PLBlock) -> str:
        return Index.creation_pl_code(index_desc_var, block)


class RenameIndex(tables.RenameInheritableTableObject):
    def __init__(self, index, *, new_name, conditional=False, **kwargs):
        super().__init__(index, new_name=new_name, **kwargs)
        if conditional:
            self.conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog)))

    def code(self, block: base.PLBlock) -> str:
        name = qn(self.object.table_name[0], self.object.name_in_catalog)
        new_name = qi(self.altered_object.name_in_catalog)
        return f'ALTER INDEX {name} RENAME TO {new_name}'

    @classmethod
    def pl_code(cls, index_desc_var: str, block: base.PLBlock) -> str:
        index_name = (
            f"(quote_ident({index_desc_var}.table_name[0])"
            f" || '.' || quote_ident({index_desc_var}.name))"
        )
        new_name = (
            f"quote_ident({index_desc_var}.name)"
        )

        return (
            f"EXECUTE 'ALTER INDEX ' || {index_name} "
            f"|| ' RENAME TO ' || {new_name};"
        )


class RenameIndexSimple(ddl.DDLOperation):
    def __init__(self, old_name, new_name, **kwargs):
        super().__init__(**kwargs)
        self.old_name = old_name
        self.new_name = new_name

    def code(self, block: base.PLBlock) -> str:
        return (f'ALTER INDEX {qn(*self.old_name)} '
                f'RENAME TO {qi(self.new_name)}')

    def __repr__(self):
        return '<{}.{} {} to {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            qn(*self.old_name), self.new_name)


class DropIndex(tables.DropInheritableTableObject):
    def __init__(self, index, *, conditional=False, **kwargs):
        super().__init__(index, **kwargs)
        if conditional:
            self.conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog)))

    def code(self, block: base.PLBlock) -> str:
        name = qn(self.object.table_name[0], self.object.name_in_catalog)
        return f'DROP INDEX {name}'

    @classmethod
    def pl_code(cls, index_desc_var: str, block: base.PLBlock) -> str:
        index_name = (
            f"(quote_ident({index_desc_var}.table_name[0])"
            f" || '.' || quote_ident({index_desc_var}.name))"
        )
        return f"EXECUTE 'DROP INDEX ' || {index_name};"


class DDLTriggerBase:
    @classmethod
    def get_inherited_indexes(
        cls,
        block: base.PLBlock,
        bases: List[Tuple[str, str]],
    ) -> Tuple[str, str]:
        bases = [ql('{}.{}'.format(*base.name)) for base in bases]
        var = block.declare_var(
            ('edgedb', 'intro_index_desc_t'), 'idx', shared=True)

        return var, textwrap.dedent(f'''\
            SELECT * FROM edgedb.introspect_indexes(
                table_list := ARRAY[{', '.join(bases)}]::text[],
                inheritable_only := TRUE,
                include_inherited := TRUE
            )
        ''')


class DDLTriggerCreateTable(
        ddl.DDLTrigger, tables.CreateTableDDLTriggerMixin, DDLTriggerBase):
    operations = tables.CreateTable,

    @classmethod
    def generate_after(cls, block, op):
        return cls.apply_inheritance(
            block, op, cls.get_inherited_indexes, CreateIndex,
            comment='Index inheritance propagation on type table creation.')


class DDLTriggerAlterTable(
        ddl.DDLTrigger, tables.AlterTableDDLTriggerMixin, DDLTriggerBase):
    operations = (tables.AlterTableAddParent, tables.AlterTableDropParent)

    @classmethod
    def generate_after(cls, block, op):
        return cls.apply_inheritance(
            block, op, cls.get_inherited_indexes, CreateIndex, DropIndex,
            comment='Index inheritance propagation on type table alteration.')


# class DDLTriggerAlterTableRename(ddl.DDLTrigger, DDLTriggerBase):
#     operations = tables.AlterTableRenameTo,
