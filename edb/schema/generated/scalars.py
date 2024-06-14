# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import expr
from edb.common import checked


class ScalarTypeMixin:

    def get_default(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'default'    # type: ignore
        )

    def get_enum_values(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[str]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'enum_values'    # type: ignore
        )

    def get_sql_type(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'sql_type'    # type: ignore
        )

    def get_sql_type_scheme(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'sql_type_scheme'    # type: ignore
        )

    def get_num_params(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'num_params'    # type: ignore
        )

    def get_arg_values(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[str]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'arg_values'    # type: ignore
        )
