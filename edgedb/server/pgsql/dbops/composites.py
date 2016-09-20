##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common import datastructures

from .. import common
from . import base


class Record(type):
    def __new__(mcls, name, fields, default=None):
        dct = {'_fields___': fields, '_default___': default}
        bases = (RecordBase, )
        return super(Record, mcls).__new__(mcls, name, bases, dct)

    def __init__(cls, name, fields, default):
        pass

    def has_field(cls, name):
        return name in cls._fields___


class RecordBase:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if k not in self.__class__._fields___:
                msg = '__init__() got an unexpected keyword argument %s' % k
                raise TypeError(msg)
            setattr(self, k, v)

        for k in set(self.__class__._fields___) - set(kwargs.keys()):
            setattr(self, k, self.__class__._default___)

    def __setattr__(self, name, value):
        if name not in self.__class__._fields___:
            msg = '%s has no attribute %s' % (self.__class__.__name__, name)
            raise AttributeError(msg)
        super().__setattr__(name, value)

    def __eq__(self, tup):
        if not isinstance(tup, tuple):
            return NotImplemented

        return tuple(self) == tup

    def __getitem__(self, index):
        return getattr(self, self.__class__._fields___[index])

    def __iter__(self):
        for name in self.__class__._fields___:
            yield getattr(self, name)

    def __len__(self):
        return len(self.__class__._fields___)

    def items(self):
        for name in self.__class__._fields___:
            yield name, getattr(self, name)

    def keys(self):
        return iter(self.__class__._fields___)

    def __str__(self):
        f = ', '.join(str(v) for v in self)
        if len(self) == 1:
            f += ','
        return '(%s)' % f

    __repr__ = __str__


class CompositeDBObject(base.DBObject):
    def __init__(self, name, columns=None):
        super().__init__()
        self.name = name
        self._columns = datastructures.OrderedSet()
        self.add_columns(columns or [])

    def add_columns(self, iterable):
        self._columns.update(iterable)

    @property
    def record(self):
        return Record(
            self.__class__.__name__ + '_record',
            [c.name for c in self._columns], default=base.Default)


class CompositeAttributeCommand:
    def __init__(self, attribute):
        self.attribute = attribute

    def __repr__(self):
        return '<%s.%s %r>' % (
            self.__class__.__module__, self.__class__.__name__, self.attribute)


class AlterCompositeAddAttribute(CompositeAttributeCommand):
    async def code(self, context):
        return 'ADD {} {}'.format(
            self.get_attribute_term(), self.attribute.code(context))

    async def extra(self, context, alter_type):
        return await self.attribute.extra(context, alter_type)


class AlterCompositeDropAttribute(CompositeAttributeCommand):
    async def code(self, context):
        attrname = common.qname(self.attribute.name)
        return 'DROP {} {}'.format(self.get_attribute_term(), attrname)


class AlterCompositeAlterAttributeType:
    def __init__(self, attribute_name, new_type):
        self.attribute_name = attribute_name
        self.new_type = new_type

    async def code(self, context):
        attrterm = self.get_attribute_term()
        attrname = common.quote_ident(str(self.attribute_name))
        return 'ALTER {} {} SET DATA TYPE {}'.format(
            attrterm, attrname, self.new_type)

    def __repr__(self):
        return '<%s.%s "%s" to %s>' % (
            self.__class__.__module__, self.__class__.__name__,
            self.attribute_name, self.new_type)


class AlterCompositeRenameAttribute:
    def __init__(
            self, name, old_attr_name, new_attr_name, *, contained=False,
            conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.old_attr_name = old_attr_name
        self.new_attr_name = new_attr_name

    async def code(self, context):
        code = super().prefix_code(context)
        attrterm = self.get_attribute_term()
        old_attr_name = common.quote_ident(str(self.old_attr_name))
        new_attr_name = common.quote_ident(str(self.new_attr_name))
        code += ' RENAME {} {} TO {}'.format(
            attrterm, old_attr_name, new_attr_name)
        return code
