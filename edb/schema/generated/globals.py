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
from edb.schema import expr
from edb.schema import types


class GlobalMixin:

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

    def get_cardinality(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.SchemaCardinality':
        field = type(self).get_field('cardinality')
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

    def get_created_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[types.Type]':
        field = type(self).get_field('created_types')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )
