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

from edb.server.pgproto.pgproto cimport WriteBuffer, ReadBuffer

from . import enums, sertypes

cdef object OUT_FMT_BINARY
cdef object OUT_FMT_JSON
cdef object OUT_FMT_JSON_ELEMENTS
cdef object OUT_FMT_NONE

cdef object IN_FMT_JSON
cdef object IN_FMT_BINARY

cdef object IN_LANG_EDGEQL
cdef object IN_LANG_SQL

cdef char MASK_JSON_PARAMETERS
cdef char MASK_EXPECT_ONE
cdef char MASK_INLINE_TYPEIDS
cdef char MASK_INLINE_TYPENAMES
cdef char MASK_INLINE_OBJECTIDS


cdef char serialize_output_format(val: enums.OutputFormat)
cdef object deserialize_output_format(int mode)
cdef char serialize_input_language(val: enums.InputLanguage)
cdef object deserialize_input_language(int mode)


@cython.final
cdef class CompilationRequest:
    cdef:
        object serializer

        readonly object source
        readonly tuple protocol_version
        readonly object input_language
        readonly object output_format
        readonly object input_format
        readonly bint expect_one
        readonly int implicit_limit
        readonly bint inline_typeids
        readonly bint inline_typenames
        readonly bint inline_objectids
        readonly str role_name
        readonly str branch_name

        readonly object modaliases
        readonly object session_config
        object database_config
        object system_config
        object schema_version

        bytes serialized_cache
        object cache_key

    cdef _serialize(self)


@cython.locals(
    buf=ReadBuffer,
)
cpdef CompilationRequest _deserialize_comp_req(
    bytes data,
    str query_text,
    compilation_config_serializer: sertypes.CompilationConfigSerializer,
)


cdef CompilationRequest _deserialize_comp_req_v1(
    ReadBuffer buf,
    str query_text,
    compilation_config_serializer: sertypes.CompilationConfigSerializer,
)


@cython.locals(
    out=WriteBuffer,
    flags=char,
)
cdef bytes _serialize_comp_req(
    CompilationRequest req,
)
