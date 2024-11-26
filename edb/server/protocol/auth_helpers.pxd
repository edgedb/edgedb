#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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


cdef extract_token_from_auth_data(bytes auth_data)
cdef auth_jwt(tenant, prefixed_token, str user, str dbname)
cdef _check_jwt_authz(tenant, claims, token_version, str user, str dbname)
cdef _get_jwt_edb_scope(claims, claim)
cdef scram_get_verifier(tenant, str user)
cdef parse_basic_auth(str auth_payload)
cdef extract_http_user(scheme, auth_payload, params)
cdef auth_basic(tenant, str username, str password)
cdef auth_mtls(transport)
cdef auth_mtls_with_user(transport, str username)
