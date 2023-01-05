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


cdef class PgConnection(frontend.FrontendConnection):

    cdef:
        bytes secret
        str client_encoding
        dict prepared_stmts
        dict portals
        bint ignore_till_sync

        object sslctx
        object endpoint_security
        bint is_tls

    cdef inline WriteBuffer ready_for_query(self)
