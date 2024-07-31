# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter


class RoleMixin:

    def get_superuser(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('superuser')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_password(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('password')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_password_hash(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('password_hash')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
