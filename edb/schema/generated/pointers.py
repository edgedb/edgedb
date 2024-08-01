# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.edgeql import qltypes
from edb.schema import pointers
from edb.schema import rewrites
from edb.schema import expr
from edb.schema import types


class PointerMixin:

    def get_source(
        self, schema: 's_schema.Schema'
    ) -> 'objects.InheritingObject':
        field = type(self).get_field('source')
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
                'Pointer object has no value '
                'for field `source`'
            )

    def get_target(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('target')
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
                'Pointer object has no value '
                'for field `target`'
            )

    def get_required(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('required')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_readonly(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('readonly')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_secret(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('secret')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_protected(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('protected')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_computable(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('computable')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_alias')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_defined_here(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('defined_here')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

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
                'Pointer object has no value '
                'for field `expr`'
            )

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
                'Pointer object has no value '
                'for field `default`'
            )

    def get_cardinality(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.SchemaCardinality':
        field = type(self).get_field('cardinality')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return qltypes.SchemaCardinality.One

    def get_union_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[pointers.Pointer]':
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
                'Pointer object has no value '
                'for field `union_of`'
            )

    def get_intersection_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[pointers.Pointer]':
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
                'Pointer object has no value '
                'for field `intersection_of`'
            )

    def get_computed_link_alias_is_backward(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('computed_link_alias_is_backward')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_computed_link_alias(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('computed_link_alias')
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
                'Pointer object has no value '
                'for field `computed_link_alias`'
            )

    def get_rewrites(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[rewrites.Rewrite]':
        field = type(self).get_field('rewrites')
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
                'Pointer object has no value '
                'for field `rewrites`'
            )
