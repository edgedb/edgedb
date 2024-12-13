#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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


from edb.server.pgproto.pgproto cimport WriteBuffer
from edb.server.protocol cimport frontend
from edb.server.pgcon.pgcon cimport PGMessage
cimport edb.pgsql.parser.parser as pg_parser


cdef class PreparedStmt:
    cdef:
        PGMessage parse_action
        pg_parser.Source source


cdef class ConnectionView:

    cdef:
        object _settings
        object _fe_settings

        bint _in_tx_explicit
        bint _in_tx_implicit
        object _in_tx_settings
        object _in_tx_fe_settings
        object _in_tx_fe_local_settings
        dict _in_tx_portals
        object _in_tx_new_portals
        object _in_tx_savepoints
        bint _tx_error

        tuple _session_state_db_cache

    cpdef inline current_fe_settings(self)
    cdef inline fe_transaction_state(self)
    cpdef inline bint in_tx(self)
    cdef inline _reset_tx_state(
        self, bint chain_implicit, bint chain_explicit
    )
    cpdef inline close_portal_if_exists(self, str name)
    cpdef inline close_portal(self, str name)
    cdef inline find_portal(self, str name)
    cdef inline portal_exists(self, str name)


cdef class PgConnection(frontend.FrontendConnection):

    cdef:
        ConnectionView _dbview

        bytes secret
        dict prepared_stmts
        dict sql_prepared_stmts
        dict sql_prepared_stmts_map
        dict wrapping_prepared_stmts
        bint ignore_till_sync

        object sslctx
        object endpoint_security
        bint is_tls
        bint _disable_cache

    cdef inline WriteBuffer ready_for_query(self)
