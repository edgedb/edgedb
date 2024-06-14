# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.edgeql import qltypes
from edb.schema import expr
from edb.schema import types


class GlobalMixin:

    def get_target(
        self, schema: 's_schema.Schema'
    ) -> 'types.Type':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'target'    # type: ignore
        )

    def get_required(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'required'    # type: ignore
        )

    def get_cardinality(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.SchemaCardinality':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'cardinality'    # type: ignore
        )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'expr'    # type: ignore
        )

    def get_default(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'default'    # type: ignore
        )

    def get_created_types(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectSet[types.Type]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'created_types'    # type: ignore
        )
