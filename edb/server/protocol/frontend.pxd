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
from edb.server.pgcon cimport pgcon
from edb.server.pgproto.pgproto cimport ReadBuffer, WriteBuffer


cdef class AbstractFrontendConnection:

    cdef write(self, WriteBuffer buf)
    cdef flush(self)


cdef class FrontendConnection(AbstractFrontendConnection):

    cdef:
        str _id
        object server
        readonly object tenant
        object loop
        readonly str dbname
        str username
        dbview.Database database

        pgcon.PGConnection _pinned_pgcon
        bint _pinned_pgcon_in_tx
        int _get_pgcon_cc

        object _transport
        WriteBuffer _write_buf
        object _write_waiter
        object connection_made_at
        int _query_count

        ReadBuffer buffer
        object _msg_take_waiter

        object started_idling_at
        bint idling

        bint _passive_mode

        bint authed
        object _main_task
        bint _cancelled
        bint _stop_requested
        bint _pgcon_released_in_connection_lost

        bint debug

        object _transport_proto
        bint _external_auth

    cdef _after_idling(self)
    cdef _main_task_created(self)
    cdef _main_task_stopped_normally(self)
    cdef write_error(self, exc)
    cdef stop_connection(self)
    cdef abort_pinned_pgcon(self)
    cdef is_in_tx(self)

    cdef WriteBuffer _make_authentication_sasl_initial(self, list methods)
    cdef _expect_sasl_initial_response(self)
    cdef WriteBuffer _make_authentication_sasl_msg(
        self, bytes data, bint final)
    cdef bytes _expect_sasl_response(self)
