##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql.string

from edgedb.lang.common import datastructures

from .. import common
from ..datasources import introspection

from . import base
from . import composites
from . import constraints
from . import ddl


class Table(composites.CompositeDBObject):
    def __init__(self, name):
        super().__init__(name)

        self.__columns = datastructures.OrderedSet()
        self._columns = []

        self.constraints = set()
        self.bases = set()
        self.data = []

    def columns(self, writable_only=False, only_self=False):
        cols = []
        tables = [self.__class__] if only_self else reversed(self.__class__.__mro__)
        for c in tables:
            if issubclass(c, Table):
                columns = getattr(self, '_' + c.__name__ + '__columns', [])
                if writable_only:
                    cols.extend(c for c in columns if not c.readonly)
                else:
                    cols.extend(columns)
        return cols

    def __iter__(self):
        return iter(self._columns)

    def add_columns(self, iterable):
        self.__columns.update(iterable)
        self._columns = self.columns()

    def get_column(self, name):
        cols = self.columns()
        for col in cols:
            if col.name == name:
                return col

    def get_type(self):
        return 'TABLE'

    def get_id(self):
        return common.qname(*self.name)

    @property
    def system_catalog(self):
        return 'pg_class'

    @property
    def oid_type(self):
        return 'regclass'


class InheritableTableObject(base.InheritableDBObject):
    @property
    def name_in_catalog(self):
        return self.name


class Column(base.DBObject):
    def __init__(self, name, type, required=False, default=None, readonly=False, comment=None):
        self.name = name
        self.type = type
        self.required = required
        self.default = default
        self.readonly = readonly
        self.comment = comment

    def code(self, context, short=False):
        code = '{} {}'.format(common.quote_ident(self.name), self.type)
        if not short:
            default = 'DEFAULT {}'.format(self.default) if self.default is not None else ''
            code += ' {} {}'.format('NOT NULL' if self.required else '', default)
        return code

    async def extra(self, context, alter_table):
        if self.comment is not None:
            col = TableColumn(table_name=alter_table.name, column=self)
            cmd = ddl.Comment(object=col, text=self.comment)
            return [cmd]

    def __repr__(self):
        return '<%s.%s "%s" %s>' % (self.__class__.__module__, self.__class__.__name__,
                                    self.name, self.type)


class TableColumn(base.DBObject):
    def __init__(self, table_name, column):
        self.table_name = table_name
        self.column = column

    def get_type(self):
        return 'COLUMN'

    def get_id(self):
        return common.qname(self.table_name[0], self.table_name[1], self.column.name)


class TableConstraint(constraints.Constraint):
    async def extra(self, context):
        return None

    def rename_extra(self, context, new_name):
        return None

    def get_subject_type(self):
        return '' # For table constraints the accepted syntax is
                  # simply CONSTRAINT ON "{tab_name}", not
                  # CONSTRAINT ON TABLE, unlike constraints on
                  # other objects.


class PrimaryKey(TableConstraint):
    def __init__(self, table_name, columns):
        super().__init__(table_name)
        self.columns = columns

    async def constraint_code(self, context):
        code = 'PRIMARY KEY (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class UniqueConstraint(TableConstraint):
    def __init__(self, table_name, columns):
        super().__init__(table_name)
        self.columns = columns

    async def constraint_code(self, context):
        code = 'UNIQUE (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class CheckConstraint(TableConstraint):
    def __init__(self, table_name, constraint_name, expr, inherit=True):
        super().__init__(table_name, constraint_name=constraint_name)
        self.expr = expr
        self.inherit = inherit

    async def constraint_code(self, context):
        if isinstance(self.expr, base.Query):
            stmt = await context.db.prepare(self.expr.text)
            expr = await stmt.fetchval(*self.expr.params)
        else:
            expr = self.expr

        code = 'CHECK ({})'.format(expr)
        if not self.inherit:
            code += ' NO INHERIT'
        return code


class TableExists(base.Condition):
    def __init__(self, name):
        self.name = name

    async def code(self, context):
        code = '''SELECT
                        tablename
                    FROM
                        pg_catalog.pg_tables
                    WHERE
                        schemaname = $1 AND tablename = $2'''
        return code, self.name


class TableInherits(base.Condition):
    def __init__(self, name, parent_name):
        self.name = name
        self.parent_name = parent_name

    async def code(self, context):
        code = '''SELECT
                        c.relname
                    FROM
                        pg_class c
                        INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace
                        INNER JOIN pg_inherits i ON i.inhrelid = c.oid
                        INNER JOIN pg_class pc ON i.inhparent = pc.oid
                        INNER JOIN pg_namespace pns ON pns.oid = pc.relnamespace
                    WHERE
                        ns.nspname = $1 AND c.relname = $2
                        AND pns.nspname = $3 AND pc.relname = $4
               '''
        return code, self.name + self.parent_name


class ColumnExists(base.Condition):
    def __init__(self, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name

    async def code(self, context):
        code = '''SELECT
                        column_name
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = $1 AND table_name = $2 AND column_name = $3'''
        return code, self.table_name + (self.column_name,)


class CreateTable(ddl.SchemaObjectOperation):
    def __init__(self, table, temporary=False, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(table.name, conditions=conditions, neg_conditions=neg_conditions,
                         priority=priority)
        self.table = table
        self.temporary = temporary

    async def code(self, context):
        elems = [c.code(context) for c in self.table.columns(only_self=True)]
        for c in self.table.constraints:
            elems.append(await c.constraint_code(context))

        name = common.qname(*self.table.name)
        cols = ', '.join(c for c in elems)
        temp = 'TEMPORARY ' if self.temporary else ''

        code = 'CREATE %sTABLE %s (%s)' % (temp, name, cols)

        if self.table.bases:
            code += ' INHERITS (' + ','.join(common.qname(*b) for b in self.table.bases) + ')'

        return code


class CreateTableDDLTriggerMixin:
    """Utility mixin to provide functions to propagate inherited objects"""

    @classmethod
    async def apply_inheritance(cls, context, op, list_inherited_objects,
                                            CreateObject):
        objects = await list_inherited_objects(
                        context.db, op.table.name, op.table.bases)
        if objects:
            cmd = base.CommandGroup()
            cmd.add_commands(CreateObject(obj) for obj in objects)
            return cmd


class AlterTableBaseMixin:
    def __init__(self, name, contained=False, **kwargs):
        self.name = name
        self.contained = contained

    def prefix_code(self, context):
        return 'ALTER TABLE %s%s' % ('ONLY ' if self.contained else '', common.qname(*self.name))

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.name)


class AlterTableBase(AlterTableBaseMixin, ddl.DDLOperation):
    def __init__(self, name, *, contained=False, conditions=None, neg_conditions=None, priority=0):
        ddl.DDLOperation.__init__(self, conditions=conditions, neg_conditions=neg_conditions,
                                  priority=priority)
        AlterTableBaseMixin.__init__(self, name=name, contained=contained)

    def get_attribute_term(self):
        return 'COLUMN'


class AlterTableFragment(ddl.DDLOperation):
    def get_attribute_term(self):
        return 'COLUMN'


class AlterTable(AlterTableBaseMixin, ddl.DDLOperation, base.CompositeCommandGroup):
    def __init__(self, name, *, contained=False, conditions=None, neg_conditions=None, priority=0):
        base.CompositeCommandGroup.__init__(self, conditions=conditions,
                                            neg_conditions=neg_conditions,
                                            priority=priority)
        AlterTableBaseMixin.__init__(self, name=name, contained=contained)
        self.ops = self.commands

    add_operation = base.CompositeCommandGroup.add_command


class AlterTableDDLTriggerMixin:
    """Utility mixin to provide functions to propagate inherited objects"""

    @classmethod
    async def apply_inheritance(
            cls, context, op, list_inherited_objects,
            CreateObject, DropObject):
        dropped_parents = []
        added_parents = []
        ops = []

        for cmd in op.commands:
            if isinstance(cmd, (tuple, list)):
                cmd = cmd[0]

            if isinstance(cmd, AlterTableAddParent):
                added_parents.append(cmd.parent_name)
            elif isinstance(cmd, AlterTableDropParent):
                dropped_parents.append(cmd.parent_name)

        if dropped_parents:
            objects_to_drop = await list_inherited_objects(
                                context.db, op.name, dropped_parents)
        else:
            objects_to_drop = []

        if added_parents:
            objects_to_add = await list_inherited_objects(
                                context.db, op.name, added_parents)
        else:
            objects_to_add = []

        if objects_to_drop:
            for obj in objects_to_drop:
                obj = obj.copy()
                obj.table_name = op.name
                ops.append(DropObject(obj))

        if objects_to_add:
            for obj in objects_to_add:
                obj = obj.copy()
                obj.table_name = op.name
                ops.append(CreateObject(obj))

        if ops:
            grp = base.CommandGroup()
            grp.add_commands(ops)
            return grp
        else:
            return None


class AlterTableAddParent(AlterTableFragment):
    def __init__(self, parent_name, **kwargs):
        super().__init__(**kwargs)
        self.parent_name = parent_name

    async def code(self, context):
        return 'INHERIT %s' % common.qname(*self.parent_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.parent_name)


class AlterTableDropParent(AlterTableFragment):
    def __init__(self, parent_name):
        self.parent_name = parent_name

    async def code(self, context):
        return 'NO INHERIT %s' % common.qname(*self.parent_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.parent_name)


class AlterTableAddColumn(composites.AlterCompositeAddAttribute, AlterTableFragment):
    pass


class AlterTableDropColumn(composites.AlterCompositeDropAttribute, AlterTableFragment):
    pass


class AlterTableAlterColumnType(composites.AlterCompositeAlterAttributeType, AlterTableFragment):
    pass


class AlterTableAlterColumnNull(AlterTableFragment):
    def __init__(self, column_name, null):
        self.column_name = column_name
        self.null = null

    async def code(self, context):
        return 'ALTER COLUMN %s %s NOT NULL' % \
                (common.quote_ident(str(self.column_name)), 'DROP' if self.null else 'SET')

    def __repr__(self):
        return '<%s.%s "%s" %s NOT NULL>' % (self.__class__.__module__, self.__class__.__name__,
                                             self.column_name, 'DROP' if self.null else 'SET')


class AlterTableAlterColumnDefault(AlterTableFragment):
    def __init__(self, column_name, default):
        self.column_name = column_name
        self.default = default

    async def code(self, context):
        if self.default is None:
            return 'ALTER COLUMN %s DROP DEFAULT' % (common.quote_ident(str(self.column_name)),)
        else:
            return 'ALTER COLUMN %s SET DEFAULT %s' % \
                    (common.quote_ident(str(self.column_name)),
                     postgresql.string.quote_literal(str(self.default)))

    def __repr__(self):
        return '<%s.%s "%s" %s DEFAULT%s>' % (self.__class__.__module__, self.__class__.__name__,
                                              self.column_name,
                                              'DROP' if self.default is None else 'SET',
                                              '' if self.default is None else ' %r' % self.default)


class TableConstraintCommand:
    pass


class TableConstraintExists(base.Condition):
    def __init__(self, table_name, constraint_name):
        self.table_name = table_name
        self.constraint_name = constraint_name

    async def code(self, context):
        code = '''SELECT
                        True
                    FROM
                        pg_catalog.pg_constraint c
                        INNER JOIN pg_catalog.pg_class t ON c.conrelid = t.oid
                        INNER JOIN pg_catalog.pg_namespace ns ON t.relnamespace = ns.oid
                    WHERE
                        conname = $1 AND relname = $3 AND nspname = $2'''
        return code, (self.constraint_name,) + self.table_name


class AlterTableAddConstraint(AlterTableFragment, TableConstraintCommand):
    def __init__(self, constraint):
        assert not isinstance(constraint, list)
        self.constraint = constraint

    async def code(self, context):
        code = 'ADD '
        name = self.constraint.constraint_name()
        if name:
            code += 'CONSTRAINT {} '.format(name)

        return code + await self.constraint.constraint_code(context)

    async def extra(self, context, alter_table):
        return await self.constraint.extra(context)

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__,
                               self.constraint)


class AlterTableRenameConstraintSimple(AlterTableBase, TableConstraintCommand):
    def __init__(self, name, *, old_name, new_name, **kwargs):
        assert name
        super().__init__(name=name, **kwargs)
        self.old_name = old_name
        self.new_name = new_name

    async def code(self, context):
        code = self.prefix_code(context)
        code += ' RENAME CONSTRAINT {} TO {}'.format(
                        common.quote_ident(self.old_name),
                        common.quote_ident(self.new_name))
        return code

    def __repr__(self):
        return '<%s.%s %r to %r>' % (self.__class__.__module__, self.__class__.__name__,
                                     self.old_name, self.new_name)


class AlterTableDropConstraint(AlterTableFragment, TableConstraintCommand):
    def __init__(self, constraint):
        self.constraint = constraint

    async def code(self, context):
        return 'DROP CONSTRAINT ' + self.constraint.constraint_name()

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__,
                               self.constraint)


class AlterTableSetSchema(AlterTableBase):
    def __init__(self, name, schema, **kwargs):
        super().__init__(name, **kwargs)
        self.schema = schema

    async def code(self, context):
        code = super().prefix_code(context)
        code += ' SET SCHEMA %s ' % common.quote_ident(self.schema)
        return code


class AlterTableRenameTo(AlterTableBase):
    def __init__(self, name, new_name, **kwargs):
        super().__init__(name, **kwargs)
        self.new_name = new_name

    async def code(self, context):
        code = super().prefix_code(context)
        code += ' RENAME TO %s ' % common.quote_ident(self.new_name)
        return code


class AlterTableRenameColumn(composites.AlterCompositeRenameAttribute, AlterTableBase):
    pass


class DropTable(ddl.SchemaObjectOperation):
    async def code(self, context):
        return 'DROP TABLE %s' % common.qname(*self.name)


class CreateInheritableTableObject(ddl.CreateObject):
    """Base creation operation class for objects with managed """

    def __init__(self, object, **kwargs):
        super().__init__(**kwargs)
        self.object = object

    async def extra(self, context):
        ops = await super().extra(context)

        if self.object.inherit:
            if ops is None:
                ops = []

            # Propagate object to all current descendants.
            # Future descendants will receive the object via
            # a corresponding DDL trigger.
            #
            ds = introspection.tables.TableDescendants(context.db)
            descendants = await ds.fetch(schema_name=self.object.table_name[0],
                                         table_name=self.object.table_name[1],
                                         max_depth=1)

            for dschema, dname, *_ in descendants:
                obj = self.object.copy()
                obj.table_name = (dschema, dname)
                obj.add_metadata('ddl:inherited', True)
                ops.append(self.__class__(obj, conditional=True))

        return ops

    def __repr__(self):
        return '<{mod}.{cls} {object!r}>' \
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        object=self.object)


class RenameInheritableTableObject(ddl.RenameObject):
    """Base rename operation class for objects with managed """

    async def extra(self, context):
        ops = await super().extra(context)

        if self.object.inherit:
            if ops is None:
                ops = []

            # Propagate object rename to all current descendants.
            #
            ds = introspection.tables.TableDescendants(context.db)
            descendants = await ds.fetch(schema_name=self.object.table_name[0],
                                         table_name=self.object.table_name[1],
                                         max_depth=1)

            for dschema, dname, *_ in descendants:
                obj = self.object.copy()
                obj.table_name = (dschema, dname)
                obj.add_metadata('ddl:inherited', True)
                rename = self.__class__(obj, new_name=self.new_name,
                                        conditional=True)
                ops.append(rename)

        return ops

    def __repr__(self):
        return '<{mod}.{cls} {object!r} TO {new_name}>' \
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        object=self.object,
                        new_name=self.new_name)


class DropInheritableTableObject(ddl.DDLOperation):
    """Base drop operation class for objects with managed """

    def __init__(self, object, **kwargs):
        super().__init__(**kwargs)
        self.object = object

    async def extra(self, context):
        ops = await super().extra(context)

        if self.object.inherit:
            if ops is None:
                ops = []

            # Propagate object drop to all current descendants.
            #
            ds = introspection.tables.TableDescendants(context.db)
            descendants = ds.fetch(schema_name=self.object.table_name[0],
                                   table_name=self.object.table_name[1],
                                   max_depth=1)

            for dschema, dname, *_ in descendants:
                obj = self.object.copy()
                obj.table_name = (dschema, dname)
                ops.append(self.__class__(obj, conditional=True))

        return ops

    def __repr__(self):
        return '<{mod}.{cls} {object!r}>' \
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        object=self.object)
