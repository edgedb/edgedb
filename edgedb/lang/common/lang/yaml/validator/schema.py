##
# Copyright (c) 2008-2010, 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.lang.import_ import get_object

from . import types, error

class Schema(object):
    @classmethod
    def prepare_class(cls, context, data):
        cls.__dct = data
        cls._context = context

    def __init__(self):
        self.refs = {}
        self.root = None

    def _build(self, dct):
        dct_id = id(dct)

        if dct_id in self.refs:
            return self.refs[dct_id]

        if isinstance(dct, type) and issubclass(dct, Schema):
            # This happens when top-level anchor is assigned to the schema
            dct = dct.__dct

        elif isinstance(dct, str):
            imported_schema = self._get_imported_schema(dct)
            dct = imported_schema.__dct

        elif dct.get('extends'):
            imported_schema = self._get_imported_schema(dct['extends'])()

            imported_schema._build(imported_schema.__dct)
            self.refs.update(imported_schema.refs)

            _dct = imported_schema.__dct.copy()
            _dct.update(dct)
            dct = _dct

        dct_type = dct['type']

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
        elif dct_type == 'class':
            tp = types.ClassType(self)
        elif dct_type == 'none':
            tp = types.NoneType(self)
        else:
            raise error.SchemaError('unknown type: ' + dct_type)

        self.refs[dct_id] = tp

        tp.load(dct)
        return tp

    def _get_imported_schema(self, schema_name):
        # Reference to an external schema
        head, _, tail = schema_name.partition('.')

        imported = self.__class__._context.document.imports.get(head)
        if imported:
            head = imported.__name__

        schema = get_object(head + '.' + tail)

        return schema

    def check(self, node):
        if self.root is None:
            self.root = self._build(self.__dct)

        self.root.begin_checks()
        result = self.root.check(node)
        self.root.end_checks()

        return result

    def push_tag(self, node, tag):
        if not hasattr(node, 'tags'):
            node.tags = [node.tag]
        else:
            node.tags.append(node.tag)
        node.tag = tag

        return tag
