# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.edgeql import qltypes
from edb.schema import name
from edb.edgeql import ast
from edb.common import checked


class OperatorMixin:

    def get_operator_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.OperatorKind':
        field = type(self).get_field('operator_kind')
        return s_getter.regular_getter(
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

    def get_from_operator(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedList[str]':
        field = type(self).get_field('from_operator')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_from_function(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedList[str]':
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

    def get_force_return_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('force_return_cast')
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

    def get_derivative_of(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        field = type(self).get_field('derivative_of')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_commutator(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        field = type(self).get_field('commutator')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_negator(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        field = type(self).get_field('negator')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_recursive(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('recursive')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
