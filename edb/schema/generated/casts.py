# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.edgeql import ast
from edb.schema import types


class CastMixin:

    def get_from_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('from_type')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_to_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('to_type')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_allow_implicit(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('allow_implicit')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_allow_assignment(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('allow_assignment')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_language(
        self, schema: 's_schema.Schema'
    ) -> 'ast.Language':
        field = type(self).get_field('language')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_from_function(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('from_function')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_from_expr(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_expr')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_from_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_cast')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('code')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
