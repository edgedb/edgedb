# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.schema import annos


class AnnotationValueMixin:

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'subject'    # type: ignore
        )

    def get_annotation(
        self, schema: 's_schema.Schema'
    ) -> 'annos.Annotation':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'annotation'    # type: ignore
        )

    def get_value(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'value'    # type: ignore
        )


class AnnotationSubjectMixin:

    def get_annotations(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByShortname[annos.AnnotationValue]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'annotations'    # type: ignore
        )


class AnnotationMixin:

    def get_inheritable(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'inheritable'    # type: ignore
        )
