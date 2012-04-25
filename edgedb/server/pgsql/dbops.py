##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Abstractions for low-level database DDL and DML operations and structures."""


import postgresql
import re

from semantix.caos.backends.pgsql import common

from semantix.utils import datastructures
from semantix.utils.debug import debug
from semantix.utils import markup


@markup.serializer.serializer(method='as_markup')
class BaseCommand:
    def get_code_and_vars(self, context):
        code = self.code(context)
        assert code is not None
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = None

        return code, vars

    @debug
    def execute(self, context):
        code, vars = self.get_code_and_vars(context)

        if code:
            """LOG [caos.delta.execute] Sync command code:
            print(code, vars)
            """

            """LINE [caos.delta.execute] EXECUTING
            repr(self)
            """
            result = context.db.prepare(code)(*vars)
            extra = self.extra(context)
            if extra:
                for cmd in extra:
                    cmd.execute(context)
            return result

    @classmethod
    def as_markup(cls, self, *, ctx):
        return markup.serialize(str(self))

    def dump(self):
        return str(self)

    def code(self, context):
        return ''

    def extra(self, context, *args, **kwargs):
        return None


class Command(BaseCommand):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        self.opid = id(self)
        self.conditions = conditions or set()
        self.neg_conditions = neg_conditions or set()
        self.priority = priority

    @debug
    def execute(self, context):
        ok = self.check_conditions(context, self.conditions, True) and \
             self.check_conditions(context, self.neg_conditions, False)

        result = None
        if ok:
            code, vars = self.get_code_and_vars(context)

            if code:
                """LOG [caos.delta.execute] Sync command code:
                print(code, vars)
                """

                """LINE [caos.delta.execute] EXECUTING
                repr(self)
                """

                if vars is not None:
                    result = context.db.prepare(code)(*vars)
                else:
                    result = context.db.execute(code)

                """LINE [caos.delta.execute] EXECUTION RESULT
                repr(result)
                """

                extra = self.extra(context)
                if extra:
                    for cmd in extra:
                        cmd.execute(context)
        return result

    @debug
    def check_conditions(self, context, conditions, positive):
        result = True
        if conditions:
            for condition in conditions:
                code, vars = condition.get_code_and_vars(context)

                """LOG [caos.delta.execute] Sync command condition:
                print(code, vars)
                """

                result = context.db.prepare(code)(*vars)

                """LOG [caos.delta.execute] Sync command condition result:
                print('actual:', bool(result), 'expected:', positive)
                """

                if bool(result) ^ positive:
                    result = False
                    break
            else:
                result = True

        return result


class CommandGroup(Command):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.commands = []

    def add_command(self, cmd):
        self.commands.append(cmd)

    def add_commands(self, cmds):
        self.commands.extend(cmds)

    def execute(self, context):
        result = None
        ok = self.check_conditions(context, self.conditions, True) and \
             self.check_conditions(context, self.neg_conditions, False)

        if ok:
            result = [c.execute(context) for c in self.commands]

        return result

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.commands:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node

    def __iter__(self):
        return iter(self.commands)

    def __len__(self):
        return len(self.commands)


class DDLOperation(Command):
    pass


class DMLOperation(Command):
    pass


class Condition(BaseCommand):
    pass


class Query:
    def __init__(self, text, params, type):
        self.text = text
        self.params = params
        self.type = type


class Insert(DMLOperation):
    def __init__(self, table, records, returning=None, *, conditions=None, neg_conditions=None,
                                                                           priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.table = table
        self.records = records
        self.returning = returning

    def code(self, context):
        cols = [(c.name, c.type) for c in self.table.columns(writable_only=True)]
        l = len(cols)

        vals = []
        placeholders = []
        i = 1
        for row in self.records:
            placeholder_row = []
            for col, coltype in cols:
                val = getattr(row, col, None)
                if val and isinstance(val, Query):
                    vals.extend(val.params)
                    qtext = re.sub(r'\$(\d+)', lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1), val.text)
                    placeholder_row.append('(%s)::%s' % (qtext, val.type))
                    i += len(val.params)
                elif val is Default:
                    placeholder_row.append('DEFAULT')
                else:
                    vals.append(val)
                    placeholder_row.append('$%d::%s' % (i, coltype))
                    i += 1
            placeholders.append('(%s)' % ','.join(placeholder_row))

        code = 'INSERT INTO %s (%s) VALUES %s' % \
                (common.qname(*self.table.name),
                 ','.join(common.quote_ident(c[0]) for c in cols),
                 ','.join(placeholders))

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        vals = (('(%s)' % ', '.join('%s=%r' % (col, v) for col, v in row.items())) for row in self.records)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, ', '.join(vals))


class Update(DMLOperation):
    def __init__(self, table, record, condition, returning=None, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.record = record
        self.fields = [f for f, v in record.items() if v is not Default]
        self.condition = condition
        self.returning = returning
        self.cols = {c.name: c.type for c in self.table.columns(writable_only=True)}


    def code(self, context):
        e = common.quote_ident

        placeholders = []
        vals = []

        i = 1
        for f in self.fields:
            val = getattr(self.record, f)

            if val is Default:
                continue

            if isinstance(val, Query):
                expr = re.sub(r'\$(\d+)', lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1), val.text)
                i += len(val.params)
                vals.extend(val.params)
            else:
                expr = '$%d::%s' % (i, self.cols[f])
                i += 1
                vals.append(val)

            placeholders.append('%s = %s' % (e(f), expr))

        if self.condition:
            cond = []
            for field, value in self.condition:
                field = e(field)

                if value is None:
                    cond.append('%s IS NULL' % field)
                else:
                    cond.append('%s = $%d' % (field, i))
                    vals.append(value)
                    i += 1

            where = 'WHERE ' + ' AND '.join(cond)
        else:
            where = ''

        code = 'UPDATE %s SET %s %s' % \
                (common.qname(*self.table.name), ', '.join(placeholders), where)

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        expr = ','.join('%s=%s' % (f, getattr(self.record, f)) for f in self.fields)
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition) if self.condition else ''
        return '<caos.sync.%s %s %s (%s)>' % (self.__class__.__name__, self.table.name, expr, where)


class Merge(Update):
    def code(self, context):
        code = super().code(context)
        if self.condition:
            cols = (common.quote_ident(c[0]) for c in self.condition)
            returning = ','.join(cols)
        else:
            returning = '*'

        code = (code[0] + ' RETURNING %s' % returning, code[1])
        return code

    def execute(self, context):
        result = super().execute(context)

        if not result:
            op = Insert(self.table, records=[self.record])
            result = op.execute(context)

        return result


class Delete(DMLOperation):
    def __init__(self, table, condition, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.condition = condition

    def code(self, context):
        e = common.quote_ident
        where = ' AND '.join('%s = $%d' % (e(c[0]), i + 1) for i, c in enumerate(self.condition))

        code = 'DELETE FROM %s WHERE %s' % (common.qname(*self.table.name), where)

        vals = [c[1] for c in self.condition]

        return (code, vals)

    def __repr__(self):
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, where)


class CopyFrom(DMLOperation):
    def __init__(self, table, producer, *, format='text', priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.producer = producer
        self.format = format

    def code(self, context):
        code = 'COPY %s FROM STDIN WITH (FORMAT "%s")' % (common.qname(*self.table.name), self.format)
        return code, ()

    def execute(self, context):
        code, vars = self.code(context)
        receive_stmt = context.db.prepare(code)
        receiver = postgresql.copyman.StatementReceiver(receive_stmt)

        cm = postgresql.copyman.CopyManager(self.producer, receiver)
        cm.run()


class DBObject:
    pass


class TableConstraint(DBObject):
    def __init__(self, table_name, column_name=None):
        self.table_name = table_name
        self.column_name = column_name

    def constraint_name(self):
        raise NotImplementedError

    def code(self, context):
        return None

    def rename_code(self, context):
        return None

    def extra(self, context, alter_table):
        return None

    def rename_extra(self, context, new_name):
        return None


class PrimaryKey(TableConstraint):
    def __init__(self, table_name, columns):
        super().__init__(table_name)
        self.columns = columns

    def code(self, context):
        code = 'PRIMARY KEY (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class UniqueConstraint(TableConstraint):
    def __init__(self, table_name, columns):
        super().__init__(table_name)
        self.columns = columns

    def code(self, context):
        code = 'UNIQUE (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class Column(DBObject):
    def __init__(self, name, type, required=False, default=None, readonly=False, comment=None):
        self.name = name
        self.type = type
        self.required = required
        self.default = default
        self.readonly = readonly
        self.comment = comment

    def code(self, context):
        e = common.quote_ident
        return '%s %s %s %s' % (common.quote_ident(self.name), self.type,
                                'NOT NULL' if self.required else '',
                                ('DEFAULT %s' % self.default) if self.default is not None else '')

    def extra(self, context, alter_table):
        if self.comment is not None:
            col = TableColumn(table_name=alter_table.name, column=self)
            cmd = Comment(object=col, text=self.comment)
            return [cmd]

    def __repr__(self):
        return '<%s.%s "%s" %s>' % (self.__class__.__module__, self.__class__.__name__,
                                    self.name, self.type)


class TableColumn(DBObject):
    def __init__(self, table_name, column):
        self.table_name = table_name
        self.column = column


class IndexColumn(DBObject):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__, self.__class__.__name__, self.name)


class TextSearchIndexColumn(IndexColumn):
    def __init__(self, name, weight, language):
        super().__init__(name)
        self.weight = weight
        self.language = language

    def code(self, context):
        ql = postgresql.string.quote_literal
        qi = common.quote_ident

        return "setweight(to_tsvector(%s, coalesce(%s, '')), %s)" % \
                (ql(self.language), qi(self.name), ql(self.weight))



class DefaultMeta(type):
    def __bool__(cls):
        return False

    def __repr__(self):
        return '<DEFAULT>'

    __str__ = __repr__


class Default(metaclass=DefaultMeta):
    pass


class Index(DBObject):
    def __init__(self, name, table_name, unique=True, expr=None):
        super().__init__()

        self.name = name
        self.table_name = table_name
        self.__columns = datastructures.OrderedSet()
        self.predicate = None
        self.unique = unique
        self.expr = expr

    def add_columns(self, columns):
        self.__columns.update(columns)

    def creation_code(self, context):
        if self.expr:
            expr = self.expr
        else:
            expr = ', '.join(self.columns)

        code = 'CREATE %(unique)s INDEX %(name)s ON %(table)s (%(expr)s) %(predicate)s' % \
                {'unique': 'UNIQUE' if self.unique else '',
                 'name': common.qname(self.name),
                 'table': common.qname(*self.table_name),
                 'expr': expr,
                 'predicate': 'WHERE %s' % self.predicate if self.predicate else ''
                }
        return code

    @property
    def columns(self):
        return iter(self.__columns)

    def __repr__(self):
        return '<%(mod)s.%(cls)s name=%(name)s cols=(%(cols)s) unique=%(uniq)s predicate=%(pred)s>'\
               % {'mod': self.__class__.__module__, 'cls': self.__class__.__name__,
                  'name': self.name, 'cols': ','.join('%r' % c for c in self.columns),
                  'uniq': self.unique, 'pred': self.predicate}


class TextSearchIndex(Index):
    def __init__(self, name, table_name, columns):
        super().__init__(name, table_name)
        self.add_columns(columns)

    def creation_code(self, context):
        code = 'CREATE INDEX %(name)s ON %(table)s USING gin((%(cols)s)) %(predicate)s' % \
                {'name': common.qname(self.name),
                 'table': common.qname(*self.table_name),
                 'cols': ' || '.join(c.code(context) for c in self.columns),
                 'predicate': 'WHERE %s' % self.predicate if self.predicate else ''
                }
        return code


class CompositeDBObject(DBObject):
    def __init__(self, name, columns=None):
        super().__init__()
        self.name = name
        self._columns = columns

    @property
    def record(self):
        return datastructures.Record(self.__class__.__name__ + '_record',
                                     [c.name for c in self._columns],
                                     default=Default)


class Table(CompositeDBObject):
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


class CompositeType(CompositeDBObject):
    def columns(self):
        return self.__columns


class TypeExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        t.oid
                    FROM
                        pg_catalog.pg_type t
                        INNER JOIN pg_catalog.pg_namespace ns ON t.typnamespace = ns.oid
                    WHERE
                        t.typname = $2 and ns.nspname = $1'''
        return code, self.name


class SchemaExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        return ('SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1', [self.name])


class CreateSchema(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name
        self.neg_conditions.add(SchemaExists(self.name))

    def code(self, context):
        return 'CREATE SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class DropSchema(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name

    def code(self, context):
        return 'DROP SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class SchemaObjectOperation(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class CreateSequence(SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    def code(self, context):
        return 'CREATE SEQUENCE %s' % common.qname(*self.name)


class RenameSequence(CommandGroup):
    def __init__(self, name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.new_name = new_name

        if name[0] != new_name[0]:
            cmd = AlterSequenceSetSchema(name, new_name[0])
            self.add_command(cmd)
            name = (new_name[0], name[1])

        if name[1] != new_name[1]:
            cmd = AlterSequenceRenameTo(name, new_name[1])
            self.add_command(cmd)

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s.%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_name[0],
                                               self.new_name[1])


class AlterSequenceSetSchema(DDLOperation):
    def __init__(self, name, new_schema, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_schema = new_schema

    def code(self, context):
        code = 'ALTER SEQUENCE %s SET SCHEMA %s' % \
                (common.qname(*self.name),
                 common.quote_ident(self.new_schema))
        return code

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_schema)


class AlterSequenceRenameTo(DDLOperation):
    def __init__(self, name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_name = new_name

    def code(self, context):
        code = 'ALTER SEQUENCE %s RENAME TO %s' % \
                (common.qname(*self.name),
                 common.quote_ident(self.new_name))
        return code

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_name)


class DropSequence(SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    def code(self, context):
        return 'DROP SEQUENCE %s' % common.qname(*self.name)


class DomainExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        domain_name
                    FROM
                        information_schema.domains
                    WHERE
                        domain_schema = $1 AND domain_name = $2'''
        return code, self.name


class CreateDomain(SchemaObjectOperation):
    def __init__(self, name, base):
        super().__init__(name)
        self.base = base

    def code(self, context):
        return 'CREATE DOMAIN %s AS %s' % (common.qname(*self.name), self.base)


class RenameDomain(SchemaObjectOperation):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    def code(self, context):
        return '''UPDATE
                        pg_catalog.pg_type AS t
                    SET
                        typname = $1,
                        typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $2)
                    FROM
                        pg_catalog.pg_namespace ns
                    WHERE
                        t.typname = $3
                        AND t.typnamespace = ns.oid
                        AND ns.nspname = $4
                        AND t.typtype = 'd'
               ''', [self.new_name[1], self.new_name[0], self.name[1], self.name[0]]


class DropDomain(SchemaObjectOperation):
    def code(self, context):
        return 'DROP DOMAIN %s' % common.qname(*self.name)


class AlterDomain(DDLOperation):
    def __init__(self, name):
        super().__init__()

        self.name = name


    def code(self, context):
        return 'ALTER DOMAIN %s ' % common.qname(*self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class AlterDomainAlterDefault(AlterDomain):
    def __init__(self, name, default):
        super().__init__(name)
        self.default = default

    def code(self, context):
        code = super().code(context)
        if self.default is None:
            code += ' DROP DEFAULT ';
        else:
            value = postgresql.string.quote_literal(str(self.default)) if self.default is not None else 'None'
            code += ' SET DEFAULT ' + value
        return code


class AlterDomainAlterNull(AlterDomain):
    def __init__(self, name, null):
        super().__init__(name)
        self.null = null

    def code(self, context):
        code = super().code(context)
        if self.null:
            code += ' DROP NOT NULL ';
        else:
            code += ' SET NOT NULL ';
        return code


class AlterDomainAlterConstraint(AlterDomain):
    def __init__(self, name, constraint_name, constraint_code):
        super().__init__(name)
        self._constraint_name = constraint_name
        self._constraint_code = constraint_code


class AlterDomainDropConstraint(AlterDomainAlterConstraint):
    def code(self, context):
        code = super().code(context)
        code += ' DROP CONSTRAINT {} '.format(self._constraint_name)
        return code


class AlterDomainAddConstraint(AlterDomainAlterConstraint):
    def code(self, context):
        code = super().code(context)
        code += ' ADD CONSTRAINT {} {}'.format(self._constraint_name, self._constraint_code)
        return code


class TableExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        tablename
                    FROM
                        pg_catalog.pg_tables
                    WHERE
                        schemaname = $1 AND tablename = $2'''
        return code, self.name


class TableInherits(Condition):
    def __init__(self, name, parent_name):
        self.name = name
        self.parent_name = parent_name

    def code(self, context):
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


class CreateTable(SchemaObjectOperation):
    def __init__(self, table, temporary=False, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(table.name, conditions=conditions, neg_conditions=neg_conditions,
                         priority=priority)
        self.table = table
        self.temporary = temporary

    def code(self, context):
        elems = [c.code(context) for c in self.table.columns(only_self=True)]
        elems += [c.code(context) for c in self.table.constraints]

        name = common.qname(*self.table.name)
        cols = ', '.join(c for c in elems)
        temp = 'TEMPORARY ' if self.temporary else ''

        code = 'CREATE %sTABLE %s (%s)' % (temp, name, cols)

        if self.table.bases:
            code += ' INHERITS (' + ','.join(common.qname(*b) for b in self.table.bases) + ')'

        return code


class DropTable(SchemaObjectOperation):
    def code(self, context):
        return 'DROP TABLE %s' % common.qname(*self.name)


class AlterTableBase(DDLOperation):
    def __init__(self, name, contained=False, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.contained = contained

    def code(self, context):
        return 'ALTER TABLE %s%s' % ('ONLY ' if self.contained else '', common.qname(*self.name))

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.name)


class AlterTableFragment(DDLOperation):
    pass


class AlterTable(AlterTableBase):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.ops = []

    def add_operation(self, op):
        self.ops.append(op)

    def code(self, context):
        if self.ops:
            code = super().code(context)
            ops = []
            for op in self.ops:
                if isinstance(op, tuple):
                    cond = True
                    if op[1]:
                        cond = cond and self.check_conditions(context, op[1], True)
                    if op[2]:
                        cond = cond and self.check_conditions(context, op[2], False)
                    if cond:
                        ops.append(op[0].code(context))
                else:
                    ops.append(op.code(context))
            if ops:
                return code + ' ' + ', '.join(ops)
        return False

    def extra(self, context):
        extra = []
        for op in self.ops:
            if isinstance(op, tuple):
                op = op[0]
            op_extra = op.extra(context, self)
            if op_extra:
                extra.extend(op_extra)

        return extra

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.ops:
            if isinstance(op, tuple):
                op = op[0]

            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node

    def __iter__(self):
        return iter(self.ops)

    def __call__(self, typ):
        return filter(lambda i: isinstance(i, typ), self.ops)


class CompositeTypeExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        typname
                    FROM
                        pg_catalog.pg_type typ
                        INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = typ.typnamespace
                    WHERE
                        nsp.nspname = $1 AND typ.typname = $2'''
        return code, self.name


class CreateCompositeType(SchemaObjectOperation):
    def __init__(self, type, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(type.name, conditions=conditions, neg_conditions=neg_conditions,
                         priority=priority)
        self.type = type

    def code(self, context):
        elems = [c.code(context) for c in self.type._columns]

        name = common.qname(*self.type.name)
        cols = ', '.join(c for c in elems)

        code = 'CREATE TYPE %s AS (%s)' % (name, cols)

        return code


class DropCompositeType(SchemaObjectOperation):
    def code(self, context):
        return 'DROP TYPE %s' % common.qname(*self.name)


class IndexExists(Condition):
    def __init__(self, index_name):
        self.index_name = index_name

    def code(self, context):
        code = '''SELECT
                       i.indexrelid
                   FROM
                       pg_catalog.pg_index i
                       INNER JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
                       INNER JOIN pg_catalog.pg_namespace icn ON icn.oid = ic.relnamespace
                   WHERE
                       icn.nspname = $1 AND ic.relname = $2'''

        return code, self.index_name


class CreateIndex(DDLOperation):
    def __init__(self, index, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.index = index

    def code(self, context):
        code = self.index.creation_code(context)
        return code

    def __repr__(self):
        return '<%s.%s "%r">' % (self.__class__.__module__, self.__class__.__name__, self.index)


class RenameIndex(DDLOperation):
    def __init__(self, old_name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.old_name = old_name
        self.new_name = new_name

    def code(self, context):
        code = 'ALTER INDEX %s RENAME TO %s' % (common.qname(*self.old_name),
                                                common.quote_ident(self.new_name))
        return code

    def __repr__(self):
        return '<%s.%s "%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                         common.qname(*self.old_name),
                                         common.quote_ident(self.new_name))


class DropIndex(DDLOperation):
    def __init__(self, index_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.index_name = index_name

    def code(self, context):
        return 'DROP INDEX %s' % common.qname(*self.index_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__,
                               common.qname(*self.index_name))


class ColumnExists(Condition):
    def __init__(self, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name

    def code(self, context):
        code = '''SELECT
                        column_name
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = $1 AND table_name = $2 AND column_name = $3'''
        return code, self.table_name + (self.column_name,)


class AlterTableAddParent(AlterTableFragment):
    def __init__(self, parent_name, **kwargs):
        super().__init__(**kwargs)
        self.parent_name = parent_name

    def code(self, context):
        return 'INHERIT %s' % common.qname(*self.parent_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.parent_name)


class AlterTableDropParent(AlterTableFragment):
    def __init__(self, parent_name):
        self.parent_name = parent_name

    def code(self, context):
        return 'NO INHERIT %s' % common.qname(*self.parent_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.parent_name)


class AlterTableAddColumn(AlterTableFragment):
    def __init__(self, column):
        self.column = column

    def code(self, context):
        return 'ADD COLUMN ' + self.column.code(context)

    def extra(self, context, alter_table):
        return self.column.extra(context, alter_table)

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__, self.column)


class AlterTableDropColumn(AlterTableFragment):
    def __init__(self, column):
        self.column = column

    def code(self, context):
        return 'DROP COLUMN %s' % common.quote_ident(self.column.name)

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__, self.column)


class AlterTableAlterColumnType(AlterTableFragment):
    def __init__(self, column_name, new_type):
        self.column_name = column_name
        self.new_type = new_type

    def code(self, context):
        return 'ALTER COLUMN %s SET DATA TYPE %s' % \
                (common.quote_ident(str(self.column_name)), self.new_type)

    def __repr__(self):
        return '<%s.%s "%s" to %s>' % (self.__class__.__module__, self.__class__.__name__,
                                       self.column_name, self.new_type)


class AlterTableAlterColumnNull(AlterTableFragment):
    def __init__(self, column_name, null):
        self.column_name = column_name
        self.null = null

    def code(self, context):
        return 'ALTER COLUMN %s %s NOT NULL' % \
                (common.quote_ident(str(self.column_name)), 'DROP' if self.null else 'SET')

    def __repr__(self):
        return '<%s.%s "%s" %s NOT NULL>' % (self.__class__.__module__, self.__class__.__name__,
                                             self.column_name, 'DROP' if self.null else 'SET')


class AlterTableAlterColumnDefault(AlterTableFragment):
    def __init__(self, column_name, default):
        self.column_name = column_name
        self.default = default

    def code(self, context):
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


class AlterTableAddConstraint(AlterTableFragment, TableConstraintCommand):
    def __init__(self, constraint):
        self.constraint = constraint

    def code(self, context):
        return 'ADD  ' + self.constraint.code(context)

    def extra(self, context, alter_table):
        return self.constraint.extra(context, alter_table)

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__,
                               self.constraint)


class AlterTableRenameConstraint(AlterTableBase, TableConstraintCommand):
    def __init__(self, table_name, constraint, new_constraint):
        super().__init__(table_name)
        self.constraint = constraint
        self.new_constraint = new_constraint

    def code(self, context):
        return self.constraint.rename_code(context, self.new_constraint)

    def extra(self, context):
        return self.constraint.rename_extra(context, self.new_constraint)

    def __repr__(self):
        return '<%s.%s %r to %r>' % (self.__class__.__module__, self.__class__.__name__,
                                       self.constraint, self.new_constraint)


class AlterTableDropConstraint(AlterTableFragment, TableConstraintCommand):
    def __init__(self, constraint):
        self.constraint = constraint

    def code(self, context):
        return 'DROP CONSTRAINT ' + self.constraint.constraint_name()

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__,
                               self.constraint)


class AlterTableSetSchema(AlterTableBase):
    def __init__(self, name, schema, **kwargs):
        super().__init__(name, **kwargs)
        self.schema = schema

    def code(self, context):
        code = super().code(context)
        code += ' SET SCHEMA %s ' % common.quote_ident(self.schema)
        return code


class AlterTableRenameTo(AlterTableBase):
    def __init__(self, name, new_name, **kwargs):
        super().__init__(name, **kwargs)
        self.new_name = new_name

    def code(self, context):
        code = super().code(context)
        code += ' RENAME TO %s ' % common.quote_ident(self.new_name)
        return code


class AlterTableRenameColumn(AlterTableBase):
    def __init__(self, name, old_col_name, new_col_name):
        super().__init__(name)
        self.old_col_name = old_col_name
        self.new_col_name = new_col_name

    def code(self, context):
        code = super().code(context)
        code += ' RENAME COLUMN %s TO %s ' % (common.quote_ident(self.old_col_name),
                                              common.quote_ident(self.new_col_name))
        return code


class CreateFunction(DDLOperation):
    def __init__(self, name, args, returns, text, language='plpgsql', volatility='volatile',
                                                                      **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.args = args
        self.returns = returns
        self.text = text
        self.volatility = volatility
        self.language = language

    def code(self, context):
        code = '''CREATE FUNCTION %(name)s(%(args)s)
                  RETURNS %(return)s
                  LANGUAGE %(lang)s
                  %(volatility)s
                  AS $____funcbody____$
                      %(text)s
                  $____funcbody____$;
               ''' % {
                   'name': common.qname(*self.name),
                   'args': ', '.join(common.quote_ident(a) for a in self.args),
                   'return': common.quote_ident(self.returns),
                   'lang': self.language,
                   'volatility': self.volatility,
                   'text': self.text
               }
        return code


class CreateTriggerFunction(CreateFunction):
    def __init__(self, name, text, language='plpgsql', volatility='volatile', **kwargs):
        super().__init__(name, args=(), returns='trigger', text=text, language=language,
                         volatility=volatility, **kwargs)


class RenameFunction(CommandGroup):
    def __init__(self, name, args, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        if name[0] != new_name[0]:
            cmd = AlterFunctionSetSchema(name, args, new_name[0])
            self.add_command(cmd)
            name = (new_name[0], name[1])

        if name[1] != new_name[1]:
            cmd = AlterFunctionRenameTo(name, args, new_name[1])
            self.add_command(cmd)


class AlterFunctionReplaceText(DDLOperation):
    def __init__(self, name, args, old_text, new_text, *, conditions=None, neg_conditions=None,
                                                                           priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.args = args
        self.old_text = old_text
        self.new_text = new_text

    def code(self, context):
        code = '''SELECT
                        replace(p.prosrc, $4, $5) AS text,
                        l.lanname AS lang,
                        p.provolatile AS volatility,
                        retns.nspname AS retnamens,
                        ret.typname AS retname
                    FROM
                        pg_catalog.pg_proc p
                        INNER JOIN pg_catalog.pg_namespace ns ON (ns.oid = p.pronamespace)
                        INNER JOIN pg_catalog.pg_language l ON (p.prolang = l.oid)
                        INNER JOIN pg_catalog.pg_type ret ON (p.prorettype = ret.oid)
                        INNER JOIN pg_catalog.pg_namespace retns ON (retns.oid = ret.typnamespace)
                    WHERE
                        p.proname = $2 AND ns.nspname = $1
                        AND ($3::text[] IS NULL
                             OR $3::text[] = ARRAY(SELECT
                                                      format_type(t, NULL)::text
                                                    FROM
                                                      unnest(p.proargtypes) t))
                '''

        vars = self.name + (self.args, self.old_text, self.new_text)
        new_text, lang, volatility, *returns = context.db.prepare(code)(*vars)[0]

        code = '''CREATE OR REPLACE FUNCTION {name} ({args})
                  RETURNS {returns}
                  LANGUAGE {lang}
                  {volatility}
                  AS $____funcbody____$
                      {text}
                  $____funcbody____$;
               '''.format(name=common.qname(*self.name),
                          args=', '.join(common.quote_ident(a) for a in self.args),
                          text=new_text,
                          lang=lang,
                          returns=common.qname(*returns),
                          volatility={b'i': 'IMMUTABLE', b's': 'STABLE', b'v': 'VOLATILE'}[volatility])

        return code, ()


class AlterFunctionSetSchema(DDLOperation):
    def __init__(self, name, args, new_schema, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.args = args
        self.new_schema = new_schema

    def code(self, context):
        code = 'ALTER FUNCTION %s(%s) SET SCHEMA %s' % \
                (common.qname(*self.name),
                 ', '.join(common.quote_ident(a) for a in self.args),
                 common.quote_ident(self.new_schema))
        return code


class AlterFunctionRenameTo(DDLOperation):
    def __init__(self, name, args, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.args = args
        self.new_name = new_name

    def code(self, context):
        code = 'ALTER FUNCTION %s(%s) RENAME TO %s' % \
                (common.qname(*self.name),
                 ', '.join(common.quote_ident(a) for a in self.args),
                 common.quote_ident(self.new_name))
        return code


class DropFunction(DDLOperation):
    def __init__(self, name, args, *, conditions=None, neg_conditions=None, priority=0):
        self.conditional = False
        if conditions:
            c = []
            for cond in conditions:
                if isinstance(cond, FunctionExists) and cond.name == name and cond.args == args:
                    self.conditional = True
                else:
                    c.append(cond)
            conditions = c
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.args = args

    def code(self, context):
        code = 'DROP FUNCTION%s %s(%s)' % \
                (' IF EXISTS' if self.conditional else '',
                 common.qname(*self.name),
                 ', '.join(common.quote_ident(a) for a in self.args))
        return code


class FunctionExists(Condition):
    def __init__(self, name, args=None):
        self.name = name
        self.args = args

    def code(self, context):
        code = '''SELECT
                        p.proname
                    FROM
                        pg_catalog.pg_proc p
                        INNER JOIN pg_catalog.pg_namespace ns ON (ns.oid = p.pronamespace)
                    WHERE
                        p.proname = $2 AND ns.nspname = $1
                        AND ($3::text[] IS NULL
                             OR $3::text[] = ARRAY(SELECT
                                                      format_type(t, NULL)::text
                                                    FROM
                                                      unnest(p.proargtypes) t))
                '''

        return code, self.name + (self.args,)


class CreateTrigger(DDLOperation):
    def __init__(self, trigger_name, *, table_name, events, timing='after', granularity='row',
                                                            procedure, condition=None, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.table_name = table_name
        self.events = events
        self.timing = timing
        self.granularity = granularity
        self.procedure = procedure
        self.condition = condition

    def code(self, context):
        return '''CREATE TRIGGER %(trigger_name)s %(timing)s %(events)s ON %(table_name)s
                  FOR EACH %(granularity)s %(condition)s EXECUTE PROCEDURE %(procedure)s
               ''' % {
                      'trigger_name': common.quote_ident(self.trigger_name),
                      'timing': self.timing,
                      'events': ' OR '.join(self.events),
                      'table_name': common.qname(*self.table_name),
                      'granularity': self.granularity,
                      'condition': ('WHEN (%s)' % self.condition) if self.condition else '',
                      'procedure': '%s()' % common.qname(*self.procedure)
                     }


class CreateConstraintTrigger(CreateTrigger):
    def __init__(self, trigger_name, *, table_name, events, procedure, condition=None,
                                        conditions=None, neg_conditions=None, priority=0):

        super().__init__(trigger_name=trigger_name, table_name=table_name, events=events,
                         procedure=procedure, condition=condition,
                         conditions=conditions, neg_conditions=neg_conditions, priority=priority)

    def code(self, context):
        return '''CREATE CONSTRAINT TRIGGER %(trigger_name)s %(timing)s %(events)s
                  ON %(table_name)s
                  FOR EACH %(granularity)s %(condition)s EXECUTE PROCEDURE %(procedure)s
               ''' % {
                      'trigger_name': common.quote_ident(self.trigger_name),
                      'timing': self.timing,
                      'events': ' OR '.join(self.events),
                      'table_name': common.qname(*self.table_name),
                      'granularity': self.granularity,
                      'condition': ('WHEN (%s)' % self.condition) if self.condition else '',
                      'procedure': '%s()' % common.qname(*self.procedure)
                     }


class AlterTriggerRenameTo(DDLOperation):
    def __init__(self, *, trigger_name, new_trigger_name, table_name, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.new_trigger_name = new_trigger_name
        self.table_name = table_name

    def code(self, context):
        return 'ALTER TRIGGER %s ON %s RENAME TO %s' % \
                (common.quote_ident(self.trigger_name), common.qname(*self.table_name),
                 common.quote_ident(self.new_trigger_name))


class DropTrigger(DDLOperation):
    def __init__(self, trigger_name, *, table_name, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.table_name = table_name

    def code(self, context):
        return 'DROP TRIGGER %(trigger_name)s ON %(table_name)s' % \
                {'trigger_name': common.quote_ident(self.trigger_name),
                 'table_name': common.qname(*self.table_name)}


class Comment(DDLOperation):
    def __init__(self, object, text, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__()

        self.object = object
        self.text = text

    def code(self, context):
        if isinstance(self.object, TableConstraint):
            object_type = 'CONSTRAINT'
            object_id = self.object.constraint_name()
            if self.object.table_name:
                object_id += ' ON {}'.format(common.qname(*self.object.table_name))
            else:
                object_id
        elif isinstance(self.object, Table):
            object_type = 'TABLE'
            object_id = common.qname(*self.object.name)
        elif isinstance(self.object, TableColumn):
            object_type = 'COLUMN'
            object_id = common.qname(self.object.table_name[0], self.object.table_name[1],
                                     self.object.column.name)
        else:
            assert False, "unexpected COMMENT target: {}".format(self.object)

        code = 'COMMENT ON {type} {id} IS {text}'.format(
                    type=object_type, id=object_id, text=postgresql.string.quote_literal(self.text))

        return code
