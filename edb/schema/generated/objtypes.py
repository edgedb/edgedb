# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.schema import triggers
from edb.schema import objtypes
from edb.schema import policies


class ObjectTypeRefMixinMixin:

    def get_access_policies(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[policies.AccessPolicy]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'access_policies'    # type: ignore
        )

    def get_triggers(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[triggers.Trigger]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'triggers'    # type: ignore
        )


class ObjectTypeMixin:

    def get_union_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[objtypes.ObjectType]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'union_of'    # type: ignore
        )

    def get_intersection_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[objtypes.ObjectType]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'intersection_of'    # type: ignore
        )

    def get_is_opaque_union(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'is_opaque_union'    # type: ignore
        )
