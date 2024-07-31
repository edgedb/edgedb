# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.edgeql import qltypes


class LinkMixin:

    def get_on_target_delete(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.LinkTargetDeleteAction':
        field = type(self).get_field('on_target_delete')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_on_source_delete(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.LinkSourceDeleteAction':
        field = type(self).get_field('on_source_delete')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
