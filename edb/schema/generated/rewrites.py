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


class RewriteMixin:

    def get_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.RewriteKind':
        field = type(self).get_field('kind')
        return s_getter.regular_getter(
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

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.InheritingObject':
        field = type(self).get_field('subject')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )
