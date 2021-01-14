#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

from edb.common import ordered

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
        self._columns = ordered.OrderedSet()
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
    def code(self, block: base.PLBlock) -> str:
        return (f'ADD {self.get_attribute_term()} '  # type: ignore
                f'{self.attribute.code(block)}')

    def generate_extra(self, block: base.PLBlock,
                       alter: base.CompositeCommandGroup):
        self.attribute.generate_extra(block, alter)


class AlterCompositeDropAttribute(CompositeAttributeCommand):
    def code(self, block: base.PLBlock) -> str:
        attrname = common.qname(self.attribute.name)
        return f'DROP {self.get_attribute_term()} {attrname}'  # type: ignore


class AlterCompositeAlterAttributeType:
    def __init__(self, attribute_name, new_type, *, using_expr=None):
        self.attribute_name = attribute_name
        self.new_type = new_type
        self.using_expr = using_expr

    def code(self, block: base.PLBlock) -> str:
        attrterm = self.get_attribute_term()  # type: ignore
        attrname = common.quote_ident(str(self.attribute_name))
        code = f'ALTER {attrterm} {attrname} SET DATA TYPE {self.new_type}'
        if self.using_expr is not None:
            code += f' USING ({self.using_expr})'

        return code

    def __repr__(self):
        cls = self.__class__
        return f'<{cls.__name__} {self.attribute_name!r} to {self.new_type}>'


class AlterCompositeRenameAttribute:
    def __init__(
            self, name, old_attr_name, new_attr_name, *, contained=False,
            conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.old_attr_name = old_attr_name
        self.new_attr_name = new_attr_name

    def code(self, block: base.PLBlock) -> str:
        code = super().prefix_code()  # type: ignore
        attrterm = self.get_attribute_term()  # type: ignore
        old_attr_name = common.quote_ident(str(self.old_attr_name))
        new_attr_name = common.quote_ident(str(self.new_attr_name))
        code += ' RENAME {} {} TO {}'.format(
            attrterm, old_attr_name, new_attr_name)
        return code
