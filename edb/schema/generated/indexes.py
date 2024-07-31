# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
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
        field = type(self).get_field('bases')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_subject(
        self, schema: 's_schema.Schema'
    ) -> 'objects.Object':
        field = type(self).get_field('subject')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_params(
        self, schema: 's_schema.Schema'
    ) -> 'functions.FuncParameterList':
        field = type(self).get_field('params')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_code(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('code')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_kwargs(
        self, schema: 's_schema.Schema'
    ) -> 'checked.CheckedDict[str, expr.Expression]':
        field = type(self).get_field('kwargs')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_type_args(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[objects.Object]':
        field = type(self).get_field('type_args')
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

    def get_except_expr(
        self, schema: 's_schema.Schema'
    ) -> 'expr.Expression':
        field = type(self).get_field('except_expr')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_deferrability(
        self, schema: 's_schema.Schema'
    ) -> 'qltypes.IndexDeferrability':
        field = type(self).get_field('deferrability')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_deferred(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('deferred')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class IndexableSubjectMixin:

    def get_indexes(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectIndexByFullname[indexes.Index]':
        field = type(self).get_field('indexes')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )
