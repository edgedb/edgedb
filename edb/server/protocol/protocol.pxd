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


cdef class HttpRequest:

    cdef:
        public object url
        public bytes version
        public bint should_keep_alive
        public bytes content_type
        public bytes method
        public bytes body


cdef class HttpResponse:

    cdef:
        public object status
        public bint close_connection
        public bytes content_type
        public bytes body


cdef class HttpProtocol:

    cdef public object server

    cdef:
        object loop
        object parser
        object transport
        object unprocessed
        bint in_response
        bint first_data_call
        bint external_auth

        HttpRequest current_request

    cdef _write(self, bytes req_version, bytes resp_status,
                bytes content_type, bytes body, bint close_connection)

    cdef write(self, HttpRequest request, HttpResponse response)

    cdef unhandled_exception(self, ex)
    cdef resume(self)
    cdef close(self)
