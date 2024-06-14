# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.edgeql import qltypes


class LinkMixin:

    def get_on_target_delete(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.LinkTargetDeleteAction':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'on_target_delete'    # type: ignore
        )

    def get_on_source_delete(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.LinkSourceDeleteAction':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'on_source_delete'    # type: ignore
        )
