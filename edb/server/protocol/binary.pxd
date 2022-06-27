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
from edb.server.pgcon cimport pgcon
from edb.server.pgproto.debug cimport PG_DEBUG


cdef enum EdgeSeverity:
    EDGE_SEVERITY_DEBUG = 20
    EDGE_SEVERITY_INFO = 40
    EDGE_SEVERITY_NOTICE = 60
    EDGE_SEVERITY_WARNING = 80
    EDGE_SEVERITY_ERROR = 120
    EDGE_SEVERITY_FATAL = 200
    EDGE_SEVERITY_PANIC = 255


cdef enum EdgeConnectionStatus:
    EDGECON_NEW = 0
    EDGECON_STARTED = 1
    EDGECON_OK = 2
    EDGECON_BAD = 3


@cython.final
cdef class QueryRequestInfo:
    cdef public object source  # edgeql.Source
    cdef public tuple protocol_version
    cdef public object output_format
    cdef public bint expect_one
    cdef public int implicit_limit
    cdef public bint inline_typeids
    cdef public bint inline_typenames
    cdef public bint inline_objectids
    cdef public uint64_t allow_capabilities

    cdef int cached_hash


@cython.final
cdef class CompiledQuery:
    cdef public object query_unit_group
    cdef public object first_extra  # Optional[int]
    cdef public object extra_counts
    cdef public object extra_blobs


cdef class EdgeConnection:

    cdef:
        EdgeConnectionStatus _con_status
        bint _external_auth
        str _id
        object _transport

        object server

        object loop
        readonly dbview.DatabaseConnectionView _dbview
        str dbname

        ReadBuffer buffer

        object _msg_take_waiter
        object _startup_msg_waiter
        object _write_waiter

        object _main_task

        CompiledQuery _last_anon_compiled
        int _last_anon_compiled_hash
        WriteBuffer _write_buf

        bint debug
        bint query_cache_enabled

        bint authed

        tuple protocol_version
        tuple max_protocol
        tuple min_protocol

        object last_state
        int last_state_id

        pgcon.PGConnection _pinned_pgcon
        bint _pinned_pgcon_in_tx

        int _get_pgcon_cc

        bint _cancelled
        bint _stop_requested
        bint _pgcon_released_in_connection_lost

        bint idling
        object started_idling_at
        bint pending_state_desc_push

        bint _in_dump_restore
        bint _passive_mode

        object _transport_proto
        bytes _auth_data
        dict  _conn_params

    cdef inline dbview.DatabaseConnectionView get_dbview(self)

    cdef interpret_backend_error(self, exc)

    cdef QueryRequestInfo parse_execute_request(self)
    cdef parse_output_format(self, bytes mode)
    cdef parse_cardinality(self, bytes card)
    cdef char render_cardinality(self, query_unit) except -1

    cdef write(self, WriteBuffer buf)
    cdef flush(self)

    cdef abort_pinned_pgcon(self)

    cdef fallthrough(self)

    cdef sync_status(self)

    cdef uint64_t _count_globals(
        self,
        query_unit: object,
    )
    cdef _inject_globals(
        self,
        query_unit: object,
        WriteBuffer out_buf,
    )

    cdef WriteBuffer recode_bind_args(self,
        bytes bind_args, CompiledQuery compiled, object positions,
    )

    cdef WriteBuffer make_negotiate_protocol_version_msg(
        self, tuple target_proto
    )
    cdef WriteBuffer make_command_data_description_msg(
        self, CompiledQuery query
    )
    cdef WriteBuffer make_command_complete_msg(self, capabilities, status)

    cdef inline reject_headers(self)
    cdef inline ignore_headers(self)
    cdef dict parse_headers(self)

    cdef write_status(self, bytes name, bytes value)
    cdef write_error(self, exc)

    cdef write_log(self, EdgeSeverity severity, uint32_t code, str message)

    cdef bytes _describe_state(self)
    cdef inline write_state_desc(self, bint flush=?)


@cython.final
cdef class VirtualTransport:
    cdef:
        WriteBuffer buf
        bint closed


include "binary_v0.pxd"
