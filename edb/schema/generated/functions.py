# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.common import span
from edb.schema import name
from edb.edgeql import qltypes
from edb.schema import annos
import uuid
from edb.edgeql import ast
from edb.schema import expr
from edb.schema import functions
from edb.schema import types
from edb.common import checked
from edb.schema import globals


class ParameterMixin:

    def get_internal(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[0]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Parameter object has no value '
                'for field `internal`'
            )

    def get_sourcectx(
        self, schema: 's_schema.Schema'
    ) -> 'span.Span':
        data = schema.get_obj_data_raw(self)
        v = data[1]
        if v is not None:
            return v
        else:
            return None

    def get_name(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        data = schema.get_obj_data_raw(self)
        v = data[2]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Parameter object has no value '
                'for field `name`'
            )

    def get_builtin(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[3]
        if v is not None:
            return v
        else:
            return False

    def get_computed_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        data = schema.get_obj_data_raw(self)
        v = data[4]
        if v is not None:
            return v
        else:
            field = type(self).get_field('computed_fields')
            return field.get_default()

    def get_num(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[5]
        if v is not None:
            return v
        else:
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
        v = data[6]
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
        v = data[7]
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
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return qltypes.TypeModifier.SingletonType

    def get_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.ParameterKind':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Parameter object has no value '
                'for field `kind`'
            )


class VolatilitySubjectMixin:

    def get_internal(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[0]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'VolatilitySubject object has no value '
                'for field `internal`'
            )

    def get_sourcectx(
        self, schema: 's_schema.Schema'
    ) -> 'span.Span':
        data = schema.get_obj_data_raw(self)
        v = data[1]
        if v is not None:
            return v
        else:
            return None

    def get_name(
        self, schema: 's_schema.Schema'
    ) -> 'name.Name':
        data = schema.get_obj_data_raw(self)
        v = data[2]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'VolatilitySubject object has no value '
                'for field `name`'
            )

    def get_builtin(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[3]
        if v is not None:
            return v
        else:
            return False

    def get_computed_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        data = schema.get_obj_data_raw(self)
        v = data[4]
        if v is not None:
            return v
        else:
            field = type(self).get_field('computed_fields')
            return field.get_default()

    def get_volatility(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.Volatility':
        data = schema.get_obj_data_raw(self)
        v = data[5]
        if v is not None:
            return v
        else:
            return qltypes.Volatility.Volatile


class CallableObjectMixin:

    def get_internal(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[0]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'CallableObject object has no value '
                'for field `internal`'
            )

    def get_sourcectx(
        self, schema: 's_schema.Schema'
    ) -> 'span.Span':
        data = schema.get_obj_data_raw(self)
        v = data[1]
        if v is not None:
            return v
        else:
            return None

    def get_name(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        data = schema.get_obj_data_raw(self)
        v = data[2]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'CallableObject object has no value '
                'for field `name`'
            )

    def get_builtin(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[3]
        if v is not None:
            return v
        else:
            return False

    def get_computed_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        data = schema.get_obj_data_raw(self)
        v = data[4]
        if v is not None:
            return v
        else:
            field = type(self).get_field('computed_fields')
            return field.get_default()

    def get_annotations(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByShortname[annos.AnnotationValue]':
        field = type(self).get_field('annotations')
        data = schema.get_obj_data_raw(self)
        v = data[5]
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
                'for field `annotations`'
            )

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        field = type(self).get_field('params')
        data = schema.get_obj_data_raw(self)
        v = data[6]
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
        v = data[7]
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
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'CallableObject object has no value '
                'for field `return_typemod`'
            )

    def get_abstract(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_impl_is_strict(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return True

    def get_prefer_subquery_args(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_is_singleton_set_of(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return v
        else:
            return False


class FunctionMixin:

    def get_internal(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[0]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Function object has no value '
                'for field `internal`'
            )

    def get_sourcectx(
        self, schema: 's_schema.Schema'
    ) -> 'span.Span':
        data = schema.get_obj_data_raw(self)
        v = data[1]
        if v is not None:
            return v
        else:
            return None

    def get_name(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        data = schema.get_obj_data_raw(self)
        v = data[2]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Function object has no value '
                'for field `name`'
            )

    def get_builtin(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[3]
        if v is not None:
            return v
        else:
            return False

    def get_computed_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        data = schema.get_obj_data_raw(self)
        v = data[4]
        if v is not None:
            return v
        else:
            field = type(self).get_field('computed_fields')
            return field.get_default()

    def get_volatility(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.Volatility':
        data = schema.get_obj_data_raw(self)
        v = data[5]
        if v is not None:
            return v
        else:
            return qltypes.Volatility.Volatile

    def get_annotations(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByShortname[annos.AnnotationValue]':
        field = type(self).get_field('annotations')
        data = schema.get_obj_data_raw(self)
        v = data[6]
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
                'for field `annotations`'
            )

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        field = type(self).get_field('params')
        data = schema.get_obj_data_raw(self)
        v = data[7]
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
                'for field `params`'
            )

    def get_return_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('return_type')
        data = schema.get_obj_data_raw(self)
        v = data[8]
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
                'for field `return_type`'
            )

    def get_return_typemod(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TypeModifier':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Function object has no value '
                'for field `return_typemod`'
            )

    def get_abstract(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_impl_is_strict(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return True

    def get_prefer_subquery_args(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return v
        else:
            return False

    def get_is_singleton_set_of(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return False

    def get_used_globals(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[globals.Global]':
        field = type(self).get_field('used_globals')
        data = schema.get_obj_data_raw(self)
        v = data[14]
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
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return None

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return v
        else:
            return None

    def get_nativecode(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('nativecode')
        data = schema.get_obj_data_raw(self)
        v = data[17]
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
        data = schema.get_obj_data_raw(self)
        v = data[18]
        if v is not None:
            return v
        else:
            return None

    def get_reflected_language(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        data = schema.get_obj_data_raw(self)
        v = data[19]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Function object has no value '
                'for field `reflected_language`'
            )

    def get_from_function(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        data = schema.get_obj_data_raw(self)
        v = data[20]
        if v is not None:
            return v
        else:
            return None

    def get_from_expr(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[21]
        if v is not None:
            return v
        else:
            return False

    def get_force_return_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[22]
        if v is not None:
            return v
        else:
            return False

    def get_sql_func_has_out_params(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[23]
        if v is not None:
            return v
        else:
            return False

    def get_error_on_null_result(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        data = schema.get_obj_data_raw(self)
        v = data[24]
        if v is not None:
            return v
        else:
            return None

    def get_preserves_optionality(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[25]
        if v is not None:
            return v
        else:
            return False

    def get_preserves_upper_cardinality(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[26]
        if v is not None:
            return v
        else:
            return False

    def get_initial_value(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('initial_value')
        data = schema.get_obj_data_raw(self)
        v = data[27]
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
        data = schema.get_obj_data_raw(self)
        v = data[28]
        if v is not None:
            return v
        else:
            return False

    def get_fallback(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[29]
        if v is not None:
            return v
        else:
            return False
