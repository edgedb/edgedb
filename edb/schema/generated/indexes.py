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
from edb.schema import indexes
from edb.common import checked
from edb.schema import expr
from edb.schema import functions


class IndexMixin:

    def get_bases(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[indexes.Index]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'bases'    # type: ignore
        )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'subject'    # type: ignore
        )

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'params'    # type: ignore
        )

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'code'    # type: ignore
        )

    def get_kwargs(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedDict[str, expr.Expression]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'kwargs'    # type: ignore
        )

    def get_type_args(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[objects.Object]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'type_args'    # type: ignore
        )

    def get_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'expr'    # type: ignore
        )

    def get_except_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'except_expr'    # type: ignore
        )

    def get_deferrability(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.IndexDeferrability':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'deferrability'    # type: ignore
        )

    def get_deferred(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'deferred'    # type: ignore
        )


class IndexableSubjectMixin:

    def get_indexes(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByFullname[indexes.Index]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'indexes'    # type: ignore
        )
