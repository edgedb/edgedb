#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

cimport cython

cdef char serialize_output_format(val)
cdef deserialize_output_format(char mode)

@cython.final
cdef class CompilationRequest:
    cdef:
        object _serializer

        readonly object source
        readonly object protocol_version
        readonly object output_format
        readonly bint json_parameters
        readonly bint expect_one
        readonly int implicit_limit
        readonly bint inline_typeids
        readonly bint inline_typenames
        readonly bint inline_objectids

        readonly object modaliases
        readonly object session_config
        object database_config
        object system_config
        object schema_version

        bytes serialized_cache
        object cache_key

    cdef _serialize(self)
    cdef _deserialize_v0(self, bytes data, str query_text)
