# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
from edb.common import checked
from edb.schema import expr
from edb.schema import types


class TypeMixin:

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        field = type(self).get_field('expr_type')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_alias')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_global')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('alias_is_persistent')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        field = type(self).get_field('backend_id')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('transient')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class QualifiedTypeMixin:
    pass


class InheritingTypeMixin:
    pass


class CollectionMixin:

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_persistent')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class CollectionExprAliasMixin:
    pass


class ArrayMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_dimensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[int]':
        field = type(self).get_field('dimensions')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )


class ArrayExprAliasMixin:
    pass


class TupleMixin:

    def get_named(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('named')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_element_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectDict[str, types.Type]':
        field = type(self).get_field('element_types')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )


class TupleExprAliasMixin:
    pass


class RangeMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )


class RangeExprAliasMixin:
    pass


class MultiRangeMixin:

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )


class MultiRangeExprAliasMixin:
    pass
