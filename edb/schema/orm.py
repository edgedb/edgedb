#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Machinery for mapping the schema data onto the model classes."""

from typing import TYPE_CHECKING
import typing

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
    from edb.schema import objects as s_obj


def get_field_value(
    self: 's_obj.Object',
    schema: 's_schema.Schema',
    field_name: str,
) -> typing.Any:
    from edb.schema import objects as s_obj

    field = type(self).get_field(field_name)

    if isinstance(field, s_obj.SchemaField):
        data = schema.get_obj_data_raw(self)
        val = data[field.index]
        if val is not None:
            if field.is_reducible:
                return field.type.schema_restore(val)
            else:
                return val
        else:
            try:
                return field.get_default()
            except ValueError:
                pass
    else:
        try:
            return object.__getattribute__(self, field_name)
        except AttributeError:
            pass

    raise s_obj.FieldValueNotFoundError(
        f'{self!r} object has no value for field {field_name!r}'
    )
