# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.common import checked
from edb.schema import expr
from edb.schema import types


class TypeMixin:

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'expr'    # type: ignore
        )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'expr_type'    # type: ignore
        )

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'from_alias'    # type: ignore
        )

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'from_global'    # type: ignore
        )

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'alias_is_persistent'    # type: ignore
        )

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'rptr'    # type: ignore
        )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'backend_id'    # type: ignore
        )

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'transient'    # type: ignore
        )


class QualifiedTypeMixin:
    pass


class InheritingTypeMixin:
    pass


class CollectionMixin:

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'is_persistent'    # type: ignore
        )


class CollectionExprAliasMixin:
    pass


class ArrayMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'element_type'    # type: ignore
        )

    def get_dimensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[int]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'dimensions'    # type: ignore
        )


class ArrayExprAliasMixin:
    pass


class TupleMixin:

    def get_named(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'named'    # type: ignore
        )

    def get_element_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectDict[str, types.Type]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'element_types'    # type: ignore
        )


class TupleExprAliasMixin:
    pass


class RangeMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'element_type'    # type: ignore
        )


class RangeExprAliasMixin:
    pass


class MultiRangeMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'element_type'    # type: ignore
        )


class MultiRangeExprAliasMixin:
    pass
