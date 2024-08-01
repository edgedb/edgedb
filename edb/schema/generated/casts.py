# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.edgeql import ast
from edb.schema import types


class CastMixin:

    def get_from_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('from_type')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Cast object has no value '
                'for field `from_type`'
            )

    def get_to_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('to_type')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Cast object has no value '
                'for field `to_type`'
            )

    def get_allow_implicit(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('allow_implicit')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_allow_assignment(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('allow_assignment')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

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

    def get_from_function(
        self, schema: 's_schema.Schema'
    ) -> 'str':
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

    def get_from_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_cast')
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
