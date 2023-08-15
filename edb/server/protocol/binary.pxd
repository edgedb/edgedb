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
)

from edb.server.dbview cimport dbview
from edb.server.pgproto.debug cimport PG_DEBUG
from edb.server.protocol cimport frontend


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


cdef class EdgeConnection(frontend.FrontendConnection):

    cdef:
        EdgeConnectionStatus _con_status

        readonly dbview.DatabaseConnectionView _dbview

        object _startup_msg_waiter

        dbview.CompiledQuery _last_anon_compiled
        int _last_anon_compiled_hash

        bint query_cache_enabled

        tuple protocol_version
        tuple max_protocol
        tuple min_protocol

        object last_state
        int last_state_id

        bint _in_dump_restore

        bytes _auth_data
        dict  _conn_params

    cdef inline dbview.DatabaseConnectionView get_dbview(self)

    cdef dbview.QueryRequestInfo parse_execute_request(self)
    cdef parse_output_format(self, bytes mode)
    cdef parse_cardinality(self, bytes card)
    cdef char render_cardinality(self, query_unit) except -1

    cdef fallthrough(self)

    cdef sync_status(self)

    cdef WriteBuffer make_negotiate_protocol_version_msg(
        self, tuple target_proto
    )
    cdef WriteBuffer make_command_data_description_msg(
        self, dbview.CompiledQuery query
    )
    cdef WriteBuffer make_state_data_description_msg(self)
    cdef WriteBuffer make_command_complete_msg(self, capabilities, status)

    cdef inline reject_headers(self)
    cdef inline ignore_headers(self)
    cdef dict parse_headers(self)

    cdef write_status(self, bytes name, bytes value)

    cdef write_log(self, EdgeSeverity severity, uint32_t code, str message)


@cython.final
cdef class VirtualTransport:
    cdef:
        WriteBuffer buf
        bint closed
