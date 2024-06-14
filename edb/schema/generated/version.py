# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import orm as s_orm
import uuid


class BaseSchemaVersionMixin:

    def get_version(
        self, schema: 's_schema.Schema'
    ) -> 'uuid.UUID':
        return s_orm.get_field_value(  # type: ignore
            self, schema, 'version'    # type: ignore
        )


class SchemaVersionMixin:
    pass


class GlobalSchemaVersionMixin:
    pass
