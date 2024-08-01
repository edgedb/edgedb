# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.schema import triggers
from edb.schema import objtypes
from edb.schema import policies


class ObjectTypeRefMixinMixin:

    def get_access_policies(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[policies.AccessPolicy]':
        field = type(self).get_field('access_policies')
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
                'ObjectTypeRefMixin object has no value '
                'for field `access_policies`'
            )

    def get_triggers(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[triggers.Trigger]':
        field = type(self).get_field('triggers')
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
                'ObjectTypeRefMixin object has no value '
                'for field `triggers`'
            )


class ObjectTypeMixin:

    def get_union_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[objtypes.ObjectType]':
        field = type(self).get_field('union_of')
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
                'ObjectType object has no value '
                'for field `union_of`'
            )

    def get_intersection_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[objtypes.ObjectType]':
        field = type(self).get_field('intersection_of')
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
                'ObjectType object has no value '
                'for field `intersection_of`'
            )

    def get_is_opaque_union(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_opaque_union')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False
