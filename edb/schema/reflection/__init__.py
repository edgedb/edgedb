#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


from .reader import parse_into, SchemaClassLayout
from .structure import generate_structure
from .structure import SchemaTypeLayout, SchemaReflectionParts
from .writer import generate_metadata_write_edgeql

__all__ = (
    'generate_structure',
    'generate_metadata_write_edgeql',
    'parse_into',
    'SchemaTypeLayout',
    'SchemaClassLayout',
    'SchemaReflectionParts'
)
