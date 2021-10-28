# This file is copied from:
# https://github.com/MagicStack/asyncpg/blob/383c711e/asyncpg/protocol/scram.pxd
#
# Copyright (C) 2021-present MagicStack Inc. and the EdgeDB authors.
# Copyright (C) 2016-present the asyncpg authors and contributors
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


cdef class SCRAMAuthentication:
    cdef:
        readonly bytes authentication_method
        readonly bytes authorization_message
        readonly bytes client_channel_binding
        readonly bytes client_first_message_bare
        readonly bytes client_nonce
        readonly bytes client_proof
        readonly bytes password_salt
        readonly int   password_iterations
        readonly bytes server_first_message
        # server_key is an instance of hmac.HAMC
        readonly object server_key
        readonly bytes server_nonce

    cdef create_client_first_message(self, str username)
    cdef create_client_final_message(self, str password)
    cdef parse_server_first_message(self, bytes server_response)
    cdef verify_server_final_message(self, bytes server_final_message)
    cdef _bytes_xor(self, bytes a, bytes b)
    cdef _generate_client_nonce(self, int num_bytes)
    cdef _generate_client_proof(self, str password)
    cdef _generate_salted_password(
        self, str password, bytes salt, int iterations
    )
    cdef _normalize_password(self, str original_password)
