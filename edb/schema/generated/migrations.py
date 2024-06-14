# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.schema import migrations


class MigrationMixin:

    def get_parents(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[migrations.Migration]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'parents'    # type: ignore
        )

    def get_message(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'message'    # type: ignore
        )

    def get_generated_by(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'generated_by'    # type: ignore
        )

    def get_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'script'    # type: ignore
        )
