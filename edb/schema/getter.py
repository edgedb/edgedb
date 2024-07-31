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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
    from edb.schema import objects as s_obj


def reducible_getter(
    self: Any,
    schema: 's_schema.Schema',
    field: 's_obj.SchemaField',
) -> Any:
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
            f'{self!r} object has no value for field {field.name!r}'
        )


def regular_default_getter(
    self: Any,
    schema: 's_schema.Schema',
    field: 's_obj.SchemaField',
) -> Any:
    data = schema.get_obj_data_raw(self)
    v = data[field.index]
    if v is not None:
        return v
    else:
        return field.default


def regular_getter(
    self: Any,
    schema: 's_schema.Schema',
    field: 's_obj.SchemaField'
) -> Any:
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
            f'{self!r} object has no value '
            f'for field {field.name!r}'
        )
