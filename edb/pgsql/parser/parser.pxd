#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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


from libc.stdint cimport uint8_t

from edb.server.pgproto.pgproto cimport (
    ReadBuffer,
    WriteBuffer,
)


cdef class Source:
    cdef:
        str _text
        bytes _serialized
        bytes _cache_key

    cdef WriteBuffer _serialize(self)


cdef class NormalizedSource(Source):
    cdef:
        str _orig_text
        int _highest_extern_param_id
        list _extracted_constants
