# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.common import span
from edb.edgeql import qltypes
from edb.schema import name
from edb.schema import annos
from edb.edgeql import ast
from edb.schema import functions
from edb.schema import types
from edb.common import checked


class OperatorMixin:

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
                'Operator object has no value '
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
                'Operator object has no value '
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
                'Operator object has no value '
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
                'Operator object has no value '
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
                'Operator object has no value '
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
                'Operator object has no value '
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

    def get_operator_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.OperatorKind':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Operator object has no value '
                'for field `operator_kind`'
            )

    def get_language(
        self, schema: 's_schema.Schema'
    ) -> 'ast.Language':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return None

    def get_from_operator(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedList[str]':
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return v
        else:
            return None

    def get_from_function(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedList[str]':
        data = schema.get_obj_data_raw(self)
        v = data[17]
        if v is not None:
            return v
        else:
            return None

    def get_from_expr(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[18]
        if v is not None:
            return v
        else:
            return False

    def get_force_return_cast(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[19]
        if v is not None:
            return v
        else:
            return False

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        data = schema.get_obj_data_raw(self)
        v = data[20]
        if v is not None:
            return v
        else:
            return None

    def get_derivative_of(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        data = schema.get_obj_data_raw(self)
        v = data[21]
        if v is not None:
            return v
        else:
            return None

    def get_commutator(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        data = schema.get_obj_data_raw(self)
        v = data[22]
        if v is not None:
            return v
        else:
            return None

    def get_negator(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        data = schema.get_obj_data_raw(self)
        v = data[23]
        if v is not None:
            return v
        else:
            return None

    def get_recursive(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[24]
        if v is not None:
            return v
        else:
            return False
