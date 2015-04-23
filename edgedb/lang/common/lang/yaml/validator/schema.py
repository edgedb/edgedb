##
# Copyright (c) 2008-2010, 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from yaml import constructor as std_yaml_constructor

from metamagic.utils.lang.import_ import get_object

from metamagic.utils.lang.yaml import constructor as yaml_constructor
from . import types, error


class SimpleSchema:
    _schema_data = None

    def __init__(self, schema_data=None, *, namespace=None):
        self.refs = {}
        self.root = None
        self.namespace = namespace
        if schema_data is None:
            schema_data = self.__class__._schema_data
        self._schema_data = schema_data
        self.checked_nodes = set()

    def _build(self, dct):
        dct_id = id(dct)

        if dct_id in self.refs:
            return self.refs[dct_id]

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
        elif dct_type == 'multimap':
            tp = types.MultiMappingType(self)
        else:
            raise error.SchemaError('unknown type: ' + dct_type)

        self.refs[dct_id] = tp

        tp.load(dct)
        return tp

    def build_validators(self):
        return self._build(self._schema_data)

    def check(self, node):
        if self.root is None:
            self.root = self.build_validators()

        self.checked_nodes = set()

        self.root.begin_checks()
        result = self.root.check(node)
        self.root.end_checks()

        self.checked_nodes.clear()

        return result

    def push_tag(self, node, tag):
        if not hasattr(node, 'tags'):
            node.tags = [node.tag]
        else:
            node.tags.append(node.tag)
        node.tag = tag

        return tag

    def init_constructor(self):
        return std_yaml_constructor.Constructor()

    def get_constructor(self):
        constructor = getattr(self, 'constructor', None)
        if not constructor:
            self.constructor = self.init_constructor()
        return self.constructor

    @classmethod
    def get_tags(cls):
        return {}


class Schema(SimpleSchema):
    @classmethod
    def prepare_class(cls, context, data):
        cls._schema_data = data
        cls._context = context

    def init_constructor(self):
        return yaml_constructor.Constructor()

    def _build(self, dct):
        dct_id = id(dct)

        if dct_id in self.refs:
            return self.refs[dct_id]

        if isinstance(dct, type) and issubclass(dct, Schema):
            # This happens when top-level anchor is assigned to the schema
            dct = dct._schema_data

        elif isinstance(dct, str):
            imported_schema = self._get_imported_schema(dct)
            dct = imported_schema._schema_data

        elif dct.get('extends'):
            imported_schema = self._get_imported_schema(dct['extends'])()

            imported_schema._build(imported_schema._schema_data)
            self.refs.update(imported_schema.refs)

            _dct = imported_schema._schema_data.copy()
            _dct.update(dct)
            dct = _dct

        return super()._build(dct)

    def _get_imported_schema(self, schema_name):
        # Reference to an external schema
        head, _, tail = schema_name.partition('.')

        try:
            schema = self.__class__._context.document.namespace[head]
        except KeyError:
            raise NameError('reference to undefined name: {!r}'.format(head))

        if tail:
            steps = tail.split('.')

            for step in steps:
                schema = getattr(schema, step)

        return schema
