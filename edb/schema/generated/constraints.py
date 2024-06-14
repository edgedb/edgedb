# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.schema import expr
from edb.schema import constraints
from edb.schema import functions


class ConstraintMixin:

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'params'    # type: ignore
        )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'expr'    # type: ignore
        )

    def get_subjectexpr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'subjectexpr'    # type: ignore
        )

    def get_finalexpr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'finalexpr'    # type: ignore
        )

    def get_except_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'except_expr'    # type: ignore
        )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'subject'    # type: ignore
        )

    def get_args(
        self, schema: 's_schema.Schema'
    ) -> 'expr.ExpressionList':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'args'    # type: ignore
        )

    def get_delegated(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'delegated'    # type: ignore
        )

    def get_errmessage(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'errmessage'    # type: ignore
        )

    def get_is_aggregate(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'is_aggregate'    # type: ignore
        )


class ConsistencySubjectMixin:

    def get_constraints(
        self, schema: 's_schema.Schema'
    ) -> 'constraints.ObjectIndexByConstraintName[constraints.Constraint]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'constraints'    # type: ignore
        )
