# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
from edb.schema import name


class FutureBehaviorMixin:

    def get_name(
        self, schema: 's_schema.Schema'
    ) -> 'name.Name':
        field = type(self).get_field('name')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )
