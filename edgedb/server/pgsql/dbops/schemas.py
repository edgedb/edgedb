##
# Copyright (c) 2008-2012 MagicStack Inc.
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

    async def code(self, context):
        return ('SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1', [self.name])


class CreateSchema(ddl.DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name
        self.neg_conditions.add(SchemaExists(self.name))

    async def code(self, context):
        return 'CREATE SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<edgedb.sync.%s %s>' % (self.__class__.__name__, self.name)


class RenameSchema(ddl.SchemaObjectOperation):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    async def code(self, context):
        return '''ALTER SCHEMA {} RENAME TO {}'''.format(common.quote_ident(self.name),
                                                         common.quote_ident(self.new_name)), []


class DropSchema(ddl.DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name

    async def code(self, context):
        return 'DROP SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<edgedb.sync.%s %s>' % (self.__class__.__name__, self.name)
