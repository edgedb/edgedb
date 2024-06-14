# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.schema import expr
from edb.edgeql import qltypes


class RewriteMixin:

    def get_kind(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.RewriteKind':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'kind'    # type: ignore
        )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'expr'    # type: ignore
        )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.InheritingObject':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'subject'    # type: ignore
        )
