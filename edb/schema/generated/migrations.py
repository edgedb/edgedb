# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import objects
from edb.schema import migrations


class MigrationMixin:

    def get_parents(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[migrations.Migration]':
        field = type(self).get_field('parents')
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
                'Migration object has no value '
                'for field `parents`'
            )

    def get_message(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('message')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

    def get_generated_by(
        self, schema: 's_schema.Schema'
    ) -> 'str':
        field = type(self).get_field('generated_by')
        data = schema.get_obj_data_raw(self)
        v = data[field.index]
        if v is not None:
            return v
        else:
            return None

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
                'Migration object has no value '
                'for field `script`'
            )
