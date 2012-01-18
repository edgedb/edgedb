##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from semantix.utils.lang.yaml import constructor as yaml_constructor
from ..base import SchemaType
from ...error import SchemaValidationError

class SchemaScalarType(SchemaType):
    __slots__ = ['unique']

    def load(self, dct):
        super().load(dct)
        self._init_constrainrs(('enum', 'range', 'unique'), dct)
        self.unique = None

    @staticmethod
    def check_range(range, node, repr=None):
        if repr is None:
            repr = node.value

        value = SchemaScalarType.get_constructor().construct_object(node)
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

    @classmethod
    def get_constructor(cls):
        constructor = getattr(cls, 'constructor', None)
        if not constructor:
            cls.constructor = yaml_constructor.Constructor()
        return cls.constructor

    def check(self, node):
        super().check(node)

        if 'enum' in self.constraints:
            value = SchemaScalarType.get_constructor().construct_object(node)
            if value not in self.constraints['enum']:
                raise SchemaValidationError('enum validation failed, value: "%s" is not in %s' %
                                            (value, self.constraints['enum']), node)

        if 'range' in self.constraints:
            SchemaScalarType.check_range(self.constraints['range'], node)

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
            SchemaScalarType.check_range(self.constraints['length'], node, 'len("%s")' % node.value)

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
