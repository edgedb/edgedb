##
# Copyright (c) 2008-2010, 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from ..base import SchemaType
from ...error import SchemaValidationError


class SchemaScalarType(SchemaType):
    __slots__ = ['unique']

    def load(self, dct):
        super().load(dct)
        self._init_constrainrs(('enum', 'range', 'unique'), dct)
        self.unique = None

    def check_range(self, range, node, repr=None):
        if repr is None:
            repr = node.value

        value = self.schema.get_constructor().construct_object(node)
        value = value if isinstance(value, int) else len(str(value))

        if 'min' in range:
            if value < range['min']:
                raise SchemaValidationError('range-min validation failed, value: "%s" <= %s' %
                                            (repr, range['min']), node)

        if 'min-ex' in range:
            if value <= range['min-ex']:
                raise SchemaValidationError('range-min-ex validation failed, value: "%s" < %s' %
                                            (repr, range['min-ex']), node)

        if 'max' in range:
            if value > range['max']:
                raise SchemaValidationError('range-max validation failed, value: "%s" > %s' %
                                            (repr, range['max']), node)

        if 'max-ex' in range:
            if value >= range['max-ex']:
                raise SchemaValidationError('range-max-ex validation failed, value: "%s" >= %s' %
                                            (repr, range['max-ex']), node)

    def begin_checks(self):
        self.unique = {}

    def end_checks(self):
        self.unique = None

    def check(self, node):
        super().check(node)

        if 'enum' in self.constraints:
            value = self.schema.get_constructor().construct_object(node)
            if value not in self.constraints['enum']:
                raise SchemaValidationError('enum validation failed, value: "%s" is not in %s' %
                                            (value, self.constraints['enum']), node)

        if 'range' in self.constraints:
            self.check_range(self.constraints['range'], node)

        if 'unique' in self.constraints:
            if node.value in self.unique:
                raise SchemaValidationError('unique value "%s" is already used in %s' %
                                            (node.value, self.unique[node.value]), node)

            self.unique[node.value] = node

        return node

class SchemaTextType(SchemaScalarType):
    def load(self, dct):
        super(SchemaTextType, self).load(dct)
        self._init_constrainrs(('pattern', 'length'), dct)

    def check(self, node):
        super().check(node)

        if 'pattern' in self.constraints:
            if re.match(self.constraints['pattern'], str(node.value)) is None:
                raise SchemaValidationError('pattern validation failed, value: "%s"' % node.value, node)

        if 'length' in self.constraints:
            self.check_range(self.constraints['length'], node, 'len("%s")' % node.value)

        return node

class ScalarType(SchemaScalarType):
    scalar_tags = ['tag:yaml.org,2002:bool',
                   'tag:yaml.org,2002:str',
                   'tag:yaml.org,2002:int',
                   'tag:yaml.org,2002:float']

    def check(self, node):
        super().check(node)

        if not self.check_tag(node, *self.scalar_tags):
            raise SchemaValidationError('expected scalar, got %s' % node.tag, node)

        return node


class NoneType(SchemaScalarType):
    def check(self, node):
        if not self.check_tag(node, 'tag:yaml.org,2002:null'):
            raise SchemaValidationError('expected none', node)

        return super().check(node)
