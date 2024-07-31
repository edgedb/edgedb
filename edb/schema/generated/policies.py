# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
from edb.schema import expr
from edb.edgeql import qltypes


class AccessPolicyMixin:

    def get_condition(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('condition')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('expr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_action(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.AccessPolicyAction':
        field = type(self).get_field('action')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_access_kinds(
        self, schema: 's_schema.Schema'
    ) -> 'objects.MultiPropSet[qltypes.AccessKind]':
        field = type(self).get_field('access_kinds')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.InheritingObject':
        field = type(self).get_field('subject')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_errmessage(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('errmessage')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_owned(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('owned')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
