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


from edb.server.dbview cimport dbview
from edb.server.pgproto.pgproto cimport WriteBuffer


cdef WriteBuffer recode_bind_args(
    dbview.DatabaseConnectionView dbv,
    dbview.CompiledQuery compiled,
    bytes bind_args,
    list positions = ?,
    list data_types = ?,
)


cdef recode_bind_args_for_script(
    dbview.DatabaseConnectionView dbv,
    dbview.CompiledQuery compiled,
    bytes bind_args,
    ssize_t start,
    ssize_t end,
)

cdef bytes recode_global(
    dbview.DatabaseConnectionView dbv,
    bytes glob,
    object glob_descriptor,
)

cdef WriteBuffer combine_raw_args(object args = ?)
