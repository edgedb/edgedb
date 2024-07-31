# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
from edb.schema import annos


class AnnotationValueMixin:

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('subject')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_annotation(
        self, schema: 's_schema.Schema'
    ) -> 'annos.Annotation':
        field = type(self).get_field('annotation')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_value(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('value')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )


class AnnotationSubjectMixin:

    def get_annotations(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByShortname[annos.AnnotationValue]':
        field = type(self).get_field('annotations')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )


class AnnotationMixin:

    def get_inheritable(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('inheritable')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
