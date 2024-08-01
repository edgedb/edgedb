# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.schema import annos


class AnnotationValueMixin:

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
                'AnnotationValue object has no value '
                'for field `subject`'
            )

    def get_annotation(
        self, schema: 's_schema.Schema'
    ) -> 'annos.Annotation':
        field = type(self).get_field('annotation')
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
                'AnnotationValue object has no value '
                'for field `annotation`'
            )

    def get_value(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('value')
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
                'AnnotationValue object has no value '
                'for field `value`'
            )


class AnnotationSubjectMixin:

    def get_annotations(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByShortname[annos.AnnotationValue]':
        field = type(self).get_field('annotations')
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
                'AnnotationSubject object has no value '
                'for field `annotations`'
            )


class AnnotationMixin:

    def get_inheritable(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('inheritable')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False
