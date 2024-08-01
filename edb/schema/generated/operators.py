# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.edgeql import qltypes
from edb.schema import name
from edb.edgeql import ast
from edb.common import checked


class OperatorMixin:

    def get_operator_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.OperatorKind':
        field = type(self).get_field('operator_kind')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Operator object has no value '
                'for field `operator_kind`'
            )

    def get_language(
        self, schema: 's_schema.Schema'
    ) -> 'ast.Language':
        field = type(self).get_field('language')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_from_operator(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedList[str]':
        field = type(self).get_field('from_operator')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_from_function(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedList[str]':
        field = type(self).get_field('from_function')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_from_expr(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_expr')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_force_return_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('force_return_cast')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('code')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_derivative_of(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        field = type(self).get_field('derivative_of')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_commutator(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        field = type(self).get_field('commutator')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_negator(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        field = type(self).get_field('negator')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_recursive(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('recursive')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False
