# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import expr
from edb.common import checked


class ScalarTypeMixin:

    def get_default(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('default')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_enum_values(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[str]':
        field = type(self).get_field('enum_values')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_sql_type(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_type')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_sql_type_scheme(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_type_scheme')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_num_params(
        self, schema: 's_schema.Schema'
    ) -> 'int':
        field = type(self).get_field('num_params')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_arg_values(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedList[str]':
        field = type(self).get_field('arg_values')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )
