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
from edb.schema import annos
from edb.edgeql import qltypes
from edb.schema import expr
from edb.schema import functions
from edb.schema import indexes
from edb.common import checked


class IndexMixin:

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
                'Index object has no value '
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
                'Index object has no value '
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
                'Index object has no value '
                'for field `annotations`'
            )

    def get_abstract(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[6]
        if v is not None:
            return v
        else:
            return False

    def get_bases(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[indexes.Index]':
        field = type(self).get_field('bases')
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
                'Index object has no value '
                'for field `bases`'
            )

    def get_ancestors(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[indexes.InheritingObject]':
        field = type(self).get_field('ancestors')
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
                'Index object has no value '
                'for field `ancestors`'
            )

    def get_inherited_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            field = type(self).get_field('inherited_fields')
            return field.get_default()

    def get_is_derived(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_owned(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_declared_overloaded(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return v
        else:
            return False

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('subject')
        data = schema.get_obj_data_raw(self)
        v = data[13]
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
                'Index object has no value '
                'for field `params`'
            )

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return None

    def get_kwargs(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedDict[str, expr.Expression]':
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return v
        else:
            field = type(self).get_field('kwargs')
            return field.get_default()

    def get_type_args(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[objects.Object]':
        field = type(self).get_field('type_args')
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
                'Index object has no value '
                'for field `type_args`'
            )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
        data = schema.get_obj_data_raw(self)
        v = data[18]
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
        v = data[19]
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
        data = schema.get_obj_data_raw(self)
        v = data[20]
        if v is not None:
            return v
        else:
            return qltypes.IndexDeferrability.Prohibited

    def get_deferred(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[21]
        if v is not None:
            return v
        else:
            return False


class IndexableSubjectMixin:

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
                'IndexableSubject object has no value '
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
                'IndexableSubject object has no value '
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

    def get_abstract(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[5]
        if v is not None:
            return v
        else:
            return False

    def get_bases(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[indexes.InheritingObject]':
        field = type(self).get_field('bases')
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
                'IndexableSubject object has no value '
                'for field `bases`'
            )

    def get_ancestors(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[indexes.InheritingObject]':
        field = type(self).get_field('ancestors')
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
                'IndexableSubject object has no value '
                'for field `ancestors`'
            )

    def get_inherited_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            field = type(self).get_field('inherited_fields')
            return field.get_default()

    def get_is_derived(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_indexes(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByFullname[indexes.Index]':
        field = type(self).get_field('indexes')
        data = schema.get_obj_data_raw(self)
        v = data[10]
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
