# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.edgeql import qltypes


class LinkMixin:

    def get_on_target_delete(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.LinkTargetDeleteAction':
        field = type(self).get_field('on_target_delete')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return qltypes.LinkTargetDeleteAction.Restrict

    def get_on_source_delete(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.LinkSourceDeleteAction':
        field = type(self).get_field('on_source_delete')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return qltypes.LinkSourceDeleteAction.Allow
