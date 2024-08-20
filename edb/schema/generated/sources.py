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
from edb.schema import sources
from edb.schema import pointers
from edb.schema import indexes
from edb.common import checked


class SourceMixin:

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
                'Source object has no value '
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
                'Source object has no value '
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
    ) -> 'objects.ObjectList[sources.InheritingObject]':
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
                'Source object has no value '
                'for field `bases`'
            )

    def get_ancestors(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[sources.InheritingObject]':
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
                'Source object has no value '
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
                'Source object has no value '
                'for field `indexes`'
            )

    def get_pointers(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[pointers.Pointer]':
        field = type(self).get_field('pointers')
        data = schema.get_obj_data_raw(self)
        v = data[11]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Source object has no value '
                'for field `pointers`'
            )
