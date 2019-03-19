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


cimport cython
cimport cpython

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t

from edb.server.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
)

from edb.server.dbview cimport dbview

from edb.server.pgproto.debug cimport PG_DEBUG


cdef enum EdgeConnectionStatus:
    EDGECON_NEW = 0
    EDGECON_STARTED = 1
    EDGECON_OK = 2
    EDGECON_BAD = 3


@cython.final
cdef class EdgeConnection:

    cdef:
        EdgeConnectionStatus _con_status
        bint _awaiting
        bint _parsing
        bint _reading_messages
        str _id
        object _transport

        object port

        object backend
        object loop
        readonly dbview.DatabaseConnectionView dbview

        ReadBuffer buffer

        object _msg_take_waiter
        object _startup_msg_waiter

        object _main_task

        object _last_anon_compiled
        WriteBuffer _write_buf

        bint debug
        bint query_cache_enabled

    cdef parse_json_mode(self, bytes mode)
    cdef parse_cardinality(self, bytes card)
    cdef render_cardinality(self, query_unit)

    cdef write(self, WriteBuffer buf)
    cdef flush(self)
    cdef abort(self)

    cdef fallthrough(self, bint ignore_unhandled)

    cdef pgcon_last_sync_status(self)

    cdef WriteBuffer recode_bind_args(self, bytes bind_args)

    cdef WriteBuffer make_describe_msg(self, query_unit)
    cdef WriteBuffer make_command_complete_msg(
        self, query_unit, bytes details=?)
