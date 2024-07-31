# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter


class ReferencedObjectMixin:

    def get_owned(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('owned')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class ReferencedInheritingObjectMixin:

    def get_declared_overloaded(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('declared_overloaded')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class NamedReferencedInheritingObjectMixin:
    pass
