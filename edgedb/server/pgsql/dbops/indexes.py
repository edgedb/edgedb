##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql.string

from semantix.utils import datastructures

from .. import common
from . import base
from . import ddl


class IndexColumn(base.DBObject):
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


class Index(base.DBObject):
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
            expr = ', '.join(common.quote_ident(c) for c in self.columns)

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


class IndexExists(base.Condition):
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
                       icn.nspname = $1 AND ic.relname = $2
               '''

        return code, self.index_name


class CreateIndex(ddl.DDLOperation):
    def __init__(self, index, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.index = index

    def code(self, context):
        code = self.index.creation_code(context)
        return code

    def __repr__(self):
        return '<%s.%s "%r">' % (self.__class__.__module__, self.__class__.__name__, self.index)


class RenameIndex(ddl.DDLOperation):
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


class DropIndex(ddl.DDLOperation):
    def __init__(self, index_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.index_name = index_name

    def code(self, context):
        return 'DROP INDEX %s' % common.qname(*self.index_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__,
                               common.qname(*self.index_name))

