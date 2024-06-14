# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
from edb.schema import objects
from edb.schema import extensions
from edb.common import checked
from edb.common import verutils


class ExtensionPackageMixin:

    def get_version(
        self, schema: 's_schema.Schema'
    ) -> 'verutils.Version':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'version'    # type: ignore
        )

    def get_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'script'    # type: ignore
        )

    def get_sql_extensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'sql_extensions'    # type: ignore
        )

    def get_ext_module(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'ext_module'    # type: ignore
        )

    def get_dependencies(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'dependencies'    # type: ignore
        )


class ExtensionMixin:

    def get_package(
        self, schema: 's_schema.Schema'
    ) -> 'extensions.ExtensionPackage':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'package'    # type: ignore
        )

    def get_dependencies(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[extensions.Extension]':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'dependencies'    # type: ignore
        )
