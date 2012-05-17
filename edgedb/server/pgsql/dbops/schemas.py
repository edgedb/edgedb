##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .. import common
from . import base
from . import ddl


class SchemaExists(base.Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        return ('SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1', [self.name])


class CreateSchema(ddl.DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name
        self.neg_conditions.add(SchemaExists(self.name))

    def code(self, context):
        return 'CREATE SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class DropSchema(ddl.DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name

    def code(self, context):
        return 'DROP SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)
