# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
import uuid
from edb.edgeql import qltypes
from edb.edgeql import ast
from edb.schema import expr
from edb.schema import functions
from edb.schema import types
from edb.schema import globals


class ParameterMixin:

    def get_num(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        field = type(self).get_field('num')
        return s_getter.regular_getter(
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

    def get_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('type')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_typemod(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TypeModifier':
        field = type(self).get_field('typemod')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.ParameterKind':
        field = type(self).get_field('kind')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )


class VolatilitySubjectMixin:

    def get_volatility(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.Volatility':
        field = type(self).get_field('volatility')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class CallableObjectMixin:

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        field = type(self).get_field('params')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_return_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('return_type')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_return_typemod(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TypeModifier':
        field = type(self).get_field('return_typemod')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_abstract(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('abstract')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_impl_is_strict(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('impl_is_strict')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_prefer_subquery_args(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('prefer_subquery_args')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_is_singleton_set_of(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_singleton_set_of')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class FunctionMixin:

    def get_used_globals(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[globals.Global]':
        field = type(self).get_field('used_globals')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_backend_name(
        self, schema: 's_schema.Schema'
    ) -> 'uuid.UUID':
        field = type(self).get_field('backend_name')
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

    def get_nativecode(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('nativecode')
        return s_getter.reducible_getter(
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

    def get_reflected_language(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('reflected_language')
        return s_getter.regular_getter(
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

    def get_force_return_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('force_return_cast')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_sql_func_has_out_params(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('sql_func_has_out_params')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_error_on_null_result(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('error_on_null_result')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_preserves_optionality(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('preserves_optionality')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_preserves_upper_cardinality(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('preserves_upper_cardinality')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_initial_value(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('initial_value')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_has_dml(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('has_dml')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_fallback(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('fallback')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
