##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import types, error

class Schema(object):
    @classmethod
    def init_class(cls, data):
        cls.__dct = data

    def __init__(self):
        self.refs = {}
        self.root = None

    def _build(self, dct):
        dct_id = id(dct)
        dct_type = dct['type']

        if dct_id in self.refs:
            return self.refs[dct_id]

        if dct_type == 'choice':
            tp = types.ChoiceType(self)
        elif dct_type == 'map':
            tp = types.MappingType(self)
        elif dct_type == 'seq':
            tp = types.SequenceType(self)
        elif dct_type == 'str':
            tp = types.StringType(self)
        elif dct_type == 'int':
            tp = types.IntType(self)
        elif dct_type == 'float':
            tp = types.FloatType(self)
        elif dct_type == 'number':
            tp = types.NumberType(self)
        elif dct_type == 'text':
            tp = types.TextType(self)
        elif dct_type == 'any':
            tp = types.AnyType(self)
        elif dct_type == 'bool':
            tp = types.BoolType(self)
        elif dct_type == 'scalar':
            tp = types.ScalarType(self)
        else:
            raise error.SchemaError('unknown type: ' + dct_type)

        self.refs[dct_id] = tp

        tp.load(dct)
        return tp

    def check(self, node):
        if self.root is None:
            self.root = self._build(self.__dct)

        self.root.begin_checks()
        result = self.root.check(node)
        self.root.end_checks()

        return result
