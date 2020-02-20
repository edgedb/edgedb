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

from edb.server.pgproto.debug cimport PG_DEBUG

from edb.server.cache cimport stmt_cache


cdef enum PGTransactionStatus:
    PQTRANS_IDLE = 0                 # connection idle
    PQTRANS_ACTIVE = 1               # command in progress
    PQTRANS_INTRANS = 2              # idle, within transaction block
    PQTRANS_INERROR = 3              # idle, within failed transaction
    PQTRANS_UNKNOWN = 4              # cannot determine status


cdef enum PGAuthenticationState:
    PGAUTH_SUCCESSFUL = 0
    PGAUTH_REQUIRED_KERBEROS = 2
    PGAUTH_REQUIRED_PASSWORD = 3
    PGAUTH_REQUIRED_PASSWORDMD5 = 5
    PGAUTH_REQUIRED_SCMCRED = 6
    PGAUTH_REQUIRED_GSS = 7
    PGAUTH_REQUIRED_GSS_CONTINUE = 8
    PGAUTH_REQUIRED_SSPI = 9
    PGAUTH_REQUIRED_SASL = 10
    PGAUTH_SASL_CONTINUE = 11
    PGAUTH_SASL_FINAL = 12


@cython.final
cdef class PGProto:

    cdef:
        ReadBuffer buffer

        object loop
        str dbname

        object transport
        object msg_waiter

        bint connected
        object connected_fut

        bint waiting_for_sync
        PGTransactionStatus xact_status

        readonly int32_t backend_pid
        readonly int32_t backend_secret

        stmt_cache.StatementsCache prep_stmts
        list last_parse_prep_stmts

        bint debug

        object pgaddr
        object edgecon_ref

        bint idle

    cdef before_command(self)
    cdef after_command(self)

    cdef write(self, buf)

    cdef parse_error_message(self)
    cdef parse_sync_message(self)

    cdef parse_notification(self)
    cdef fallthrough(self)
    cdef fallthrough_idle(self)

    cdef before_prepare(self, stmt_name, dbver, WriteBuffer outbuf)

    cdef make_clean_stmt_message(self, bytes stmt_name)
    cdef make_auth_password_md5_message(self, bytes salt)
