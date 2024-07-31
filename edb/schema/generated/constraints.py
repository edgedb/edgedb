# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
from edb.schema import expr
from edb.schema import constraints
from edb.schema import functions


class ConstraintMixin:

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        field = type(self).get_field('params')
        return s_getter.reducible_getter(
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

    def get_subjectexpr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('subjectexpr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_finalexpr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('finalexpr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_except_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('except_expr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('subject')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_args(
        self, schema: 's_schema.Schema'
    ) -> 'expr.ExpressionList':
        field = type(self).get_field('args')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_delegated(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('delegated')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_errmessage(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('errmessage')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_is_aggregate(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_aggregate')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class ConsistencySubjectMixin:

    def get_constraints(
        self, schema: 's_schema.Schema'
    ) -> 'constraints.ObjectIndexByConstraintName[constraints.Constraint]':
        field = type(self).get_field('constraints')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )
