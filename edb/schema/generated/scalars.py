# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import expr
from edb.common import checked


class ScalarTypeMixin:

    def get_default(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('default')
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
                'ScalarType object has no value '
                'for field `default`'
            )

    def get_enum_values(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[str]':
        field = type(self).get_field('enum_values')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_sql_type(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_type')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_sql_type_scheme(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_type_scheme')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_num_params(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        field = type(self).get_field('num_params')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_arg_values(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[str]':
        field = type(self).get_field('arg_values')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None
