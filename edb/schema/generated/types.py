# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.common import checked
from edb.schema import expr
from edb.schema import types


class TypeMixin:

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
                'Type object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        field = type(self).get_field('expr_type')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_alias')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_global')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('alias_is_persistent')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
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
                'Type object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        field = type(self).get_field('backend_id')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('transient')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False


class QualifiedTypeMixin:
    pass


class InheritingTypeMixin:
    pass


class CollectionMixin:

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_persistent')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False


class CollectionExprAliasMixin:
    pass


class ArrayMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
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
                'Array object has no value '
                'for field `element_type`'
            )

    def get_dimensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[int]':
        field = type(self).get_field('dimensions')
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
                'Array object has no value '
                'for field `dimensions`'
            )


class ArrayExprAliasMixin:
    pass


class TupleMixin:

    def get_named(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('named')
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
                'Tuple object has no value '
                'for field `named`'
            )

    def get_element_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectDict[str, types.Type]':
        field = type(self).get_field('element_types')
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
                'Tuple object has no value '
                'for field `element_types`'
            )


class TupleExprAliasMixin:
    pass


class RangeMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
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
                'Range object has no value '
                'for field `element_type`'
            )


class RangeExprAliasMixin:
    pass


class MultiRangeMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
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
                'MultiRange object has no value '
                'for field `element_type`'
            )


class MultiRangeExprAliasMixin:
    pass
