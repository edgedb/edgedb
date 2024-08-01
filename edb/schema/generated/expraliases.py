# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.schema import expr
from edb.schema import types


class AliasMixin:

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'Alias object has no value '
                'for field `expr`'
            )

    def get_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('type')
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
                'Alias object has no value '
                'for field `type`'
            )

    def get_created_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[types.Type]':
        field = type(self).get_field('created_types')
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
                'Alias object has no value '
                'for field `created_types`'
            )
