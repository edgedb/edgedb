# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
from edb.schema import triggers
from edb.schema import objtypes
from edb.schema import policies


class ObjectTypeRefMixinMixin:

    def get_access_policies(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[policies.AccessPolicy]':
        field = type(self).get_field('access_policies')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_triggers(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByUnqualifiedName[triggers.Trigger]':
        field = type(self).get_field('triggers')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )


class ObjectTypeMixin:

    def get_union_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[objtypes.ObjectType]':
        field = type(self).get_field('union_of')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_intersection_of(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[objtypes.ObjectType]':
        field = type(self).get_field('intersection_of')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_is_opaque_union(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_opaque_union')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
