# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class ReferencedObjectMixin:

    def get_owned(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('owned')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False


class ReferencedInheritingObjectMixin:

    def get_declared_overloaded(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('declared_overloaded')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False


class NamedReferencedInheritingObjectMixin:
    pass
