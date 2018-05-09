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


import uuid


from edgedb.lang.common import exceptions as edgedb_error
from edgedb.lang.common.persistent_hash import persistent_hash

from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class UUID(uuid.UUID):
    def __init__(self, value, *, hex=None, bytes=None, bytes_le=None,
                 fields=None, int=None, version=None):
        try:
            if isinstance(value, uuid.UUID):
                int = value.int
                super().__init__(hex, bytes, bytes_le, fields, int, version)
            else:
                hex = value
                super().__init__(hex, bytes, bytes_le, fields, int, version)

        except ValueError as e:
            raise edgedb_error.ScalarTypeValueError(e.args[0]) from e

    def persistent_hash(self):
        return persistent_hash(self.int)


_add_impl('std::uuid', UUID)
_add_map(UUID, 'std::uuid')
_add_map(uuid.UUID, 'std::uuid')
