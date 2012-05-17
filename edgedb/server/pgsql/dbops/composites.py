##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import datastructures

from .. import common
from . import base


class CompositeDBObject(base.DBObject):
    def __init__(self, name, columns=None):
        super().__init__()
        self.name = name
        self._columns = columns

    @property
    def record(self):
        return datastructures.Record(self.__class__.__name__ + '_record',
                                     [c.name for c in self._columns],
                                     default=base.Default)


class CompositeAttributeCommand:
    def __init__(self, attribute):
        self.attribute = attribute

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__, self.attribute)


class AlterCompositeAddAttribute(CompositeAttributeCommand):
    def code(self, context):
        return 'ADD {} {}'.format(self.get_attribute_term(), self.attribute.code(context))

    def extra(self, context, alter_type):
        return self.attribute.extra(context, alter_type)


class AlterCompositeDropAttribute(CompositeAttributeCommand):
    def code(self, context):
        attrname = common.qname(self.attribute.name)
        return 'DROP {} {}'.format(self.get_attribute_term(), attrname)


class AlterCompositeAlterAttributeType:
    def __init__(self, attribute_name, new_type):
        self.attribute_name = attribute_name
        self.new_type = new_type

    def code(self, context):
        attrterm = self.get_attribute_term()
        attrname = common.quote_ident(str(self.attribute_name))
        return 'ALTER {} {} SET DATA TYPE {}'.format(attrterm, attrname, self.new_type)

    def __repr__(self):
        return '<%s.%s "%s" to %s>' % (self.__class__.__module__, self.__class__.__name__,
                                       self.attribute_name, self.new_type)


class AlterCompositeRenameAttribute:
    def __init__(self, name, old_attr_name, new_attr_name):
        super().__init__(name)
        self.old_attr_name = old_attr_name
        self.new_attr_name = new_attr_name

    def code(self, context):
        code = super().prefix_code(context)
        attrterm = self.get_attribute_term()
        old_attr_name = common.quote_ident(str(self.old_attr_name))
        new_attr_name = common.quote_ident(str(self.new_attr_name))
        code += ' RENAME {} {} TO {}'.format(attrterm, old_attr_name, new_attr_name)
        return code
