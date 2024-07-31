# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
from edb.schema import migrations


class MigrationMixin:

    def get_parents(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[migrations.Migration]':
        field = type(self).get_field('parents')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_message(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('message')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_generated_by(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('generated_by')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('script')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )
