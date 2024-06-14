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


class TriggerMixin:

    def get_timing(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TriggerTiming':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'timing'    # type: ignore
        )

    def get_kinds(
        self, schema: 's_schema.Schema'
    ) -> 'objects.MultiPropSet[qltypes.TriggerKind]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'kinds'    # type: ignore
        )

    def get_scope(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.TriggerScope':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'scope'    # type: ignore
        )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'expr'    # type: ignore
        )

    def get_condition(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'condition'    # type: ignore
        )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.InheritingObject':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'subject'    # type: ignore
        )

    def get_owned(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'owned'    # type: ignore
        )
