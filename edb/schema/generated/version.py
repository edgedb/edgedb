# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
import uuid


class BaseSchemaVersionMixin:

    def get_version(
        self, schema: 's_schema.Schema'
    ) -> 'uuid.UUID':
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
                'BaseSchemaVersion object has no value '
                'for field `version`'
            )


class SchemaVersionMixin:
    pass


class GlobalSchemaVersionMixin:
    pass
