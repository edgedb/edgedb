##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import json

from edgedb.lang.common import datastructures

from .. import common
from ..datasources import introspection

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

    async def code(self, context):
        ql = common.quote_literal
        qi = common.quote_ident

        return "setweight(to_tsvector(%s, coalesce(%s, '')), %s)" % \
            (ql(self.language), qi(self.name), ql(self.weight))


class Index(tables.InheritableTableObject):
    def __init__(
            self, name, table_name, unique=True, expr=None, predicate=None,
            inherit=False, metadata=None, columns=None):
        super().__init__(inherit=inherit, metadata=metadata)

        assert table_name[1] != 'feature'

        self.name = name
        self.table_name = table_name
        self.__columns = datastructures.OrderedSet()
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
        if self.inherit:
            return base.pack_name(self.table_name[1] + '__' + self.name)
        else:
            return self.name

    def add_columns(self, columns):
        self.__columns.update(columns)

    async def creation_code(self, context):
        if self.expr:
            expr = self.expr
        else:
            expr = ', '.join(common.quote_ident(c) for c in self.columns)

        code = '''
            CREATE {unique} INDEX {name}
                ON {table} ({expr}) {predicate}'''.format(

            unique='UNIQUE' if self.unique else '',
            name=common.qname(self.name_in_catalog),
            table=common.qname(*self.table_name),
            expr=expr,
            predicate=('WHERE {}'.format(self.predicate)
                       if self.predicate else '')
        )
        return code

    @property
    def columns(self):
        return iter(self.__columns)

    def get_type(self):
        return 'INDEX'

    def get_id(self):
        return common.qname(self.table_name[0], self.name_in_catalog)

    def get_oid(self):
        qry = '''
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
                ic.relname = $1
                AND ns.nspname = $2
                AND c.relname = $3
        '''
        params = (self.name_in_catalog, ) + self.table_name

        return base.Query(text=qry, params=params)

    @classmethod
    def from_introspection(cls, table_name, index_data):
        (
            name, is_unique, predicate, expression, columns,
            metadata) = index_data

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

    async def creation_code(self, context):
        code = \
            'CREATE INDEX %(name)s ON %(table)s ' \
            'USING gin((%(cols)s)) %(predicate)s' % \
            {'name': common.qname(self.name),
             'table': common.qname(*self.table_name),
             'cols': ' || '.join(c.code(context) for c in self.columns),
             'predicate': ('WHERE %s' % self.predicate
                           if self.predicate else '')}
        return code


class IndexExists(base.Condition):
    def __init__(self, index_name):
        self.index_name = index_name

    async def code(self, context):
        code = '''
            SELECT
                   i.indexrelid
               FROM
                   pg_catalog.pg_index i
                   INNER JOIN pg_catalog.pg_class ic
                        ON ic.oid = i.indexrelid
                   INNER JOIN pg_catalog.pg_namespace icn
                        ON icn.oid = ic.relnamespace
               WHERE
                   icn.nspname = $1 AND ic.relname = $2
               '''

        return code, self.index_name


class CreateIndex(tables.CreateInheritableTableObject):
    def __init__(self, index, *, conditional=False, **kwargs):
        super().__init__(index, **kwargs)
        self.index = index
        if conditional:
            self.neg_conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog)))

    async def code(self, context):
        return await self.index.creation_code(context)


class RenameIndex(tables.RenameInheritableTableObject):
    def __init__(self, index, *, new_name, conditional=False, **kwargs):
        super().__init__(index, new_name=new_name, **kwargs)
        if conditional:
            self.conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog)))

    async def code(self, context):
        code = 'ALTER INDEX {} RENAME TO {}'.format(
            common.qname(
                self.object.table_name[0], self.object.name_in_catalog),
            common.quote_ident(self.altered_object.name_in_catalog))
        return code


class RenameIndexSimple(ddl.DDLOperation):
    def __init__(self, old_name, new_name, **kwargs):
        super().__init__(**kwargs)
        self.old_name = old_name
        self.new_name = new_name

    async def code(self, context):
        code = 'ALTER INDEX {} RENAME TO {}'.format(
            common.qname(*self.old_name), common.quote_ident(self.new_name))
        return code

    def __repr__(self):
        return '<{}.{} {} to {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            common.qname(*self.old_name), self.new_name)


class DropIndex(tables.DropInheritableTableObject):
    def __init__(self, index, *, conditional=False, **kwargs):
        super().__init__(index, **kwargs)
        if conditional:
            self.conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog)))

    async def code(self, context):
        return 'DROP INDEX {}'.format(
            common.qname(
                self.object.table_name[0], self.object.name_in_catalog))


class DDLTriggerBase:
    @classmethod
    async def get_inherited_indexes(cls, db, table_name, bases):
        bases = ['{}.{}'.format(*base) for base in bases]

        ti = introspection.tables.TableIndexes(db)
        idx_records = await ti.fetch(
            table_list=bases, inheritable_only=True, include_inherited=True)

        # Use a dictionary here to filter out any duplicates resulting
        # from the inclusion of inherited indexes.
        #
        indexes = {}
        for row in idx_records:
            for idx_data in row['indexes']:
                index = Index.from_introspection(table_name, idx_data)
                index.add_metadata('ddl:inherited', True)
                indexes[index.name] = index

        return list(indexes.values())


class DDLTriggerCreateTable(
        ddl.DDLTrigger, tables.CreateTableDDLTriggerMixin, DDLTriggerBase):
    operations = tables.CreateTable,

    @classmethod
    async def after(cls, context, op):
        return await cls.apply_inheritance(
            context, op, cls.get_inherited_indexes, CreateIndex)


class DDLTriggerAlterTable(
        ddl.DDLTrigger, tables.AlterTableDDLTriggerMixin, DDLTriggerBase):
    operations = tables.AlterTable,

    @classmethod
    async def after(cls, context, op):
        return await cls.apply_inheritance(
            context, op, cls.get_inherited_indexes, CreateIndex, DropIndex)


class DDLTriggerAlterTableRename(ddl.DDLTrigger, DDLTriggerBase):
    operations = tables.AlterTableRenameTo,

    @classmethod
    async def after(cls, context, op):
        ti = introspection.tables.TableIndexes(context.db)

        idx_records = await ti.fetch(
            table_list=[op.name[0] + '.' + op.new_name], inheritable_only=True,
            include_inherited=True)

        ops = []
        for row in idx_records:
            for idx_data in row['indexes']:
                orig_index = Index.from_introspection(op.name, idx_data)
                renamed_index = orig_index.copy()
                renamed_index.table_name = (op.name[0], op.new_name)

                if orig_index.name_in_catalog != renamed_index.name_in_catalog:
                    orig_name = (op.name[0], orig_index.name_in_catalog)
                    new_name = renamed_index.name_in_catalog
                    op = RenameIndexSimple(orig_name, new_name)
                    ops.append(op)

        if ops:
            grp = base.CommandGroup()
            grp.add_commands(ops)
            return grp
        else:
            return None
