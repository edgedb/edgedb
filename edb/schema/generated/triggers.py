# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.schema import expr
from edb.edgeql import qltypes


class TriggerMixin:

    def get_timing(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TriggerTiming':
        field = type(self).get_field('timing')
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
                'Trigger object has no value '
                'for field `timing`'
            )

    def get_kinds(
        self, schema: 's_schema.Schema'
    ) -> 'objects.MultiPropSet[qltypes.TriggerKind]':
        field = type(self).get_field('kinds')
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
                'Trigger object has no value '
                'for field `kinds`'
            )

    def get_scope(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TriggerScope':
        field = type(self).get_field('scope')
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
                'Trigger object has no value '
                'for field `scope`'
            )

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
                'Trigger object has no value '
                'for field `expr`'
            )

    def get_condition(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('condition')
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
                'Trigger object has no value '
                'for field `condition`'
            )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.InheritingObject':
        field = type(self).get_field('subject')
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
                'Trigger object has no value '
                'for field `subject`'
            )

    def get_owned(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('owned')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False
