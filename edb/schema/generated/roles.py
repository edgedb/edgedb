# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class RoleMixin:

    def get_superuser(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('superuser')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return False

    def get_password(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('password')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_password_hash(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('password_hash')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None
