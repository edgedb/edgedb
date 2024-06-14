# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm


class ReferencedObjectMixin:

    def get_owned(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'owned'    # type: ignore
        )


class ReferencedInheritingObjectMixin:

    def get_declared_overloaded(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'declared_overloaded'    # type: ignore
        )


class NamedReferencedInheritingObjectMixin:
    pass
