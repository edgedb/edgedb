# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
import uuid


class BaseSchemaVersionMixin:

    def get_version(
        self, schema: 's_schema.Schema'
    ) -> 'uuid.UUID':
        field = type(self).get_field('version')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )


class SchemaVersionMixin:
    pass


class GlobalSchemaVersionMixin:
    pass
