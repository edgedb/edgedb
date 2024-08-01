# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.edgeql import qltypes
from edb.schema import indexes
from edb.common import checked
from edb.schema import expr
from edb.schema import functions


class IndexMixin:

    def get_bases(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[indexes.Index]':
        field = type(self).get_field('bases')
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
                'Index object has no value '
                'for field `bases`'
            )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('subject')
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
                'Index object has no value '
                'for field `subject`'
            )

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
                'Index object has no value '
                'for field `params`'
            )

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

    def get_kwargs(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedDict[str, expr.Expression]':
        field = type(self).get_field('kwargs')
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
                'Index object has no value '
                'for field `kwargs`'
            )

    def get_type_args(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[objects.Object]':
        field = type(self).get_field('type_args')
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
                'Index object has no value '
                'for field `type_args`'
            )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'Index object has no value '
                'for field `expr`'
            )

    def get_except_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('except_expr')
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
                'Index object has no value '
                'for field `except_expr`'
            )

    def get_deferrability(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.IndexDeferrability':
        field = type(self).get_field('deferrability')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return qltypes.IndexDeferrability.Prohibited

    def get_deferred(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('deferred')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False


class IndexableSubjectMixin:

    def get_indexes(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByFullname[indexes.Index]':
        field = type(self).get_field('indexes')
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
                'IndexableSubject object has no value '
                'for field `indexes`'
            )
