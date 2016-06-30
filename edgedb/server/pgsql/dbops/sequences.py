##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .. import common
from . import base
from . import ddl


class CreateSequence(ddl.SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    async def code(self, context):
        return 'CREATE SEQUENCE %s' % common.qname(*self.name)


class RenameSequence(base.CommandGroup):
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


class AlterSequenceSetSchema(ddl.DDLOperation):
    def __init__(self, name, new_schema, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_schema = new_schema

    async def code(self, context):
        code = 'ALTER SEQUENCE %s SET SCHEMA %s' % \
                (common.qname(*self.name),
                 common.quote_ident(self.new_schema))
        return code

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_schema)


class AlterSequenceRenameTo(ddl.DDLOperation):
    def __init__(self, name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_name = new_name

    async def code(self, context):
        code = 'ALTER SEQUENCE %s RENAME TO %s' % \
                (common.qname(*self.name),
                 common.quote_ident(self.new_name))
        return code

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_name)


class DropSequence(ddl.SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    async def code(self, context):
        return 'DROP SEQUENCE %s' % common.qname(*self.name)
