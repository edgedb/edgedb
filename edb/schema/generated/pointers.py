# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
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
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_target(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('target')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_required(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('required')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_readonly(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('readonly')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_secret(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('secret')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_protected(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('protected')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_computable(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('computable')
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

    def get_defined_here(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('defined_here')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_default(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('default')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_cardinality(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.SchemaCardinality':
        field = type(self).get_field('cardinality')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_union_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[pointers.Pointer]':
        field = type(self).get_field('union_of')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_intersection_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[pointers.Pointer]':
        field = type(self).get_field('intersection_of')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_computed_link_alias_is_backward(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('computed_link_alias_is_backward')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_computed_link_alias(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('computed_link_alias')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_rewrites(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[rewrites.Rewrite]':
        field = type(self).get_field('rewrites')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )
