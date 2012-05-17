##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql.string

from . import base


class DDLOperation(base.Command):
    pass


class SchemaObjectOperation(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class Comment(DDLOperation):
    def __init__(self, object, text, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__()

        self.object = object
        self.text = text

    def code(self, context):
        object_type = self.object.get_type()
        object_id = self.object.get_id()

        code = 'COMMENT ON {type} {id} IS {text}'.format(
                    type=object_type, id=object_id, text=postgresql.string.quote_literal(self.text))

        return code
