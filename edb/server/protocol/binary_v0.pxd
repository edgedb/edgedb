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


@cython.final
cdef class EdgeConnectionBackwardsCompatible(EdgeConnection):
    cdef legacy_parse_prepare_query_part(self, bint account_for_stmt_name)
    cdef WriteBuffer make_legacy_command_data_description_msg(
        self, CompiledQuery query
    )
    cdef WriteBuffer make_legacy_command_complete_msg(self, query_unit)
    cdef uint64_t _parse_implicit_limit(self, bytes v) except <uint64_t>-1
