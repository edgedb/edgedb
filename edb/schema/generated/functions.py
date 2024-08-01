# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
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
                'Parameter object has no value '
                'for field `num`'
            )

    def get_default(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('default')
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
                'Parameter object has no value '
                'for field `default`'
            )

    def get_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('type')
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
                'Parameter object has no value '
                'for field `type`'
            )

    def get_typemod(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TypeModifier':
        field = type(self).get_field('typemod')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return qltypes.TypeModifier.SingletonType

    def get_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.ParameterKind':
        field = type(self).get_field('kind')
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
                'Parameter object has no value '
                'for field `kind`'
            )


class VolatilitySubjectMixin:

    def get_volatility(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.Volatility':
        field = type(self).get_field('volatility')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return qltypes.Volatility.Volatile


class CallableObjectMixin:

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        field = type(self).get_field('params')
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
                'CallableObject object has no value '
                'for field `params`'
            )

    def get_return_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('return_type')
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
                'CallableObject object has no value '
                'for field `return_type`'
            )

    def get_return_typemod(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TypeModifier':
        field = type(self).get_field('return_typemod')
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
                'CallableObject object has no value '
                'for field `return_typemod`'
            )

    def get_abstract(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('abstract')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_impl_is_strict(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('impl_is_strict')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return True

    def get_prefer_subquery_args(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('prefer_subquery_args')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_is_singleton_set_of(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_singleton_set_of')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False


class FunctionMixin:

    def get_used_globals(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[globals.Global]':
        field = type(self).get_field('used_globals')
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
                'Function object has no value '
                'for field `used_globals`'
            )

    def get_backend_name(
        self, schema: 's_schema.Schema'
    ) -> 'uuid.UUID':
        field = type(self).get_field('backend_name')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('code')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_nativecode(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('nativecode')
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
                'Function object has no value '
                'for field `nativecode`'
            )

    def get_language(
        self, schema: 's_schema.Schema'
    ) -> 'ast.Language':
        field = type(self).get_field('language')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_reflected_language(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('reflected_language')
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
                'Function object has no value '
                'for field `reflected_language`'
            )

    def get_from_function(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('from_function')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_from_expr(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('from_expr')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_force_return_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('force_return_cast')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_sql_func_has_out_params(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('sql_func_has_out_params')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_error_on_null_result(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('error_on_null_result')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_preserves_optionality(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('preserves_optionality')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_preserves_upper_cardinality(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('preserves_upper_cardinality')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_initial_value(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('initial_value')
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
                'Function object has no value '
                'for field `initial_value`'
            )

    def get_has_dml(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('has_dml')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_fallback(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('fallback')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False
