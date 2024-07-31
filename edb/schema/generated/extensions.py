# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import objects
from edb.schema import extensions
from edb.common import checked
from edb.common import verutils


class ExtensionPackageMixin:

    def get_version(
        self, schema: 's_schema.Schema'
    ) -> 'verutils.Version':
        field = type(self).get_field('version')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('script')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_sql_extensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        field = type(self).get_field('sql_extensions')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_sql_setup_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_setup_script')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_sql_teardown_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_teardown_script')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_ext_module(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('ext_module')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_dependencies(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        field = type(self).get_field('dependencies')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )


class ExtensionMixin:

    def get_package(
        self, schema: 's_schema.Schema'
    ) -> 'extensions.ExtensionPackage':
        field = type(self).get_field('package')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_dependencies(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[extensions.Extension]':
        field = type(self).get_field('dependencies')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )
