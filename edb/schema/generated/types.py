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
from edb.schema import expr
from edb.schema import types
from edb.common import checked


class TypeMixin:

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
                'Type object has no value '
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
                'Type object has no value '
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
                'Type object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'Type object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Type object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False


class QualifiedTypeMixin:

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
                'QualifiedType object has no value '
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
                'QualifiedType object has no value '
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
                'QualifiedType object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'QualifiedType object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'QualifiedType object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False


class InheritingTypeMixin:

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
                'InheritingType object has no value '
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
                'InheritingType object has no value '
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
                'InheritingType object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'InheritingType object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'InheritingType object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_bases(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[types.InheritingObject]':
        field = type(self).get_field('bases')
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'InheritingType object has no value '
                'for field `bases`'
            )

    def get_ancestors(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[types.InheritingObject]':
        field = type(self).get_field('ancestors')
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'InheritingType object has no value '
                'for field `ancestors`'
            )

    def get_inherited_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        data = schema.get_obj_data_raw(self)
        v = data[17]
        if v is not None:
            return v
        else:
            field = type(self).get_field('inherited_fields')
            return field.get_default()

    def get_is_derived(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[18]
        if v is not None:
            return v
        else:
            return False


class CollectionMixin:

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
                'Collection object has no value '
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
                'Collection object has no value '
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
                'Collection object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'Collection object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Collection object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False


class CollectionExprAliasMixin:

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
                'CollectionExprAlias object has no value '
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
                'CollectionExprAlias object has no value '
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
                'CollectionExprAlias object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'CollectionExprAlias object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'CollectionExprAlias object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False


class ArrayMixin:

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
                'Array object has no value '
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
                'Array object has no value '
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
                'Array object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'Array object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Array object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Array object has no value '
                'for field `element_type`'
            )

    def get_dimensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[int]':
        data = schema.get_obj_data_raw(self)
        v = data[17]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Array object has no value '
                'for field `dimensions`'
            )


class ArrayExprAliasMixin:

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
                'ArrayExprAlias object has no value '
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
                'ArrayExprAlias object has no value '
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
                'ArrayExprAlias object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'ArrayExprAlias object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'ArrayExprAlias object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'ArrayExprAlias object has no value '
                'for field `element_type`'
            )

    def get_dimensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[int]':
        data = schema.get_obj_data_raw(self)
        v = data[17]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'ArrayExprAlias object has no value '
                'for field `dimensions`'
            )


class TupleMixin:

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
                'Tuple object has no value '
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
                'Tuple object has no value '
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
                'Tuple object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'Tuple object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Tuple object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_named(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Tuple object has no value '
                'for field `named`'
            )

    def get_element_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectDict[str, types.Type]':
        field = type(self).get_field('element_types')
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
                'Tuple object has no value '
                'for field `element_types`'
            )


class TupleExprAliasMixin:

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
                'TupleExprAlias object has no value '
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
                'TupleExprAlias object has no value '
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
                'TupleExprAlias object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'TupleExprAlias object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'TupleExprAlias object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_named(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return v
        else:
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'TupleExprAlias object has no value '
                'for field `named`'
            )

    def get_element_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectDict[str, types.Type]':
        field = type(self).get_field('element_types')
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
                'TupleExprAlias object has no value '
                'for field `element_types`'
            )


class RangeMixin:

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
                'Range object has no value '
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
                'Range object has no value '
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
                'Range object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'Range object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Range object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Range object has no value '
                'for field `element_type`'
            )


class RangeExprAliasMixin:

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
                'RangeExprAlias object has no value '
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
                'RangeExprAlias object has no value '
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
                'RangeExprAlias object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'RangeExprAlias object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'RangeExprAlias object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'RangeExprAlias object has no value '
                'for field `element_type`'
            )


class MultiRangeMixin:

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
                'MultiRange object has no value '
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
                'MultiRange object has no value '
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
                'MultiRange object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'MultiRange object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'MultiRange object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'MultiRange object has no value '
                'for field `element_type`'
            )


class MultiRangeExprAliasMixin:

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
                'MultiRangeExprAlias object has no value '
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
                'MultiRangeExprAlias object has no value '
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
                'MultiRangeExprAlias object has no value '
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

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
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
                'MultiRangeExprAlias object has no value '
                'for field `expr`'
            )

    def get_expr_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.ExprType':
        data = schema.get_obj_data_raw(self)
        v = data[8]
        if v is not None:
            return v
        else:
            return None

    def get_from_alias(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[9]
        if v is not None:
            return v
        else:
            return False

    def get_from_global(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[10]
        if v is not None:
            return v
        else:
            return False

    def get_alias_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return v
        else:
            return False

    def get_rptr(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('rptr')
        data = schema.get_obj_data_raw(self)
        v = data[12]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'MultiRangeExprAlias object has no value '
                'for field `rptr`'
            )

    def get_backend_id(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        data = schema.get_obj_data_raw(self)
        v = data[13]
        if v is not None:
            return v
        else:
            return None

    def get_transient(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[14]
        if v is not None:
            return v
        else:
            return False

    def get_is_persistent(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        data = schema.get_obj_data_raw(self)
        v = data[15]
        if v is not None:
            return v
        else:
            return False

    def get_element_type(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        field = type(self).get_field('element_type')
        data = schema.get_obj_data_raw(self)
        v = data[16]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'MultiRangeExprAlias object has no value '
                'for field `element_type`'
            )
