# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.schema import extensions
from edb.common import checked
from edb.common import verutils


class ExtensionPackageMixin:

    def get_version(
        self, schema: 's_schema.Schema'
    ) -> 'verutils.Version':
        field = type(self).get_field('version')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'ExtensionPackage object has no value '
                'for field `version`'
            )

    def get_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('script')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'ExtensionPackage object has no value '
                'for field `script`'
            )

    def get_sql_extensions(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        field = type(self).get_field('sql_extensions')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'ExtensionPackage object has no value '
                'for field `sql_extensions`'
            )

    def get_sql_setup_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_setup_script')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_sql_teardown_script(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('sql_teardown_script')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_ext_module(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('ext_module')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_dependencies(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        field = type(self).get_field('dependencies')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'ExtensionPackage object has no value '
                'for field `dependencies`'
            )


class ExtensionMixin:

    def get_package(
        self, schema: 's_schema.Schema'
    ) -> 'extensions.ExtensionPackage':
        field = type(self).get_field('package')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Extension object has no value '
                'for field `package`'
            )

    def get_dependencies(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[extensions.Extension]':
        field = type(self).get_field('dependencies')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return field.type.schema_restore(v)
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
            from edb.schema import objects as s_obj
            raise s_obj.FieldValueNotFoundError(
                'Extension object has no value '
                'for field `dependencies`'
            )
