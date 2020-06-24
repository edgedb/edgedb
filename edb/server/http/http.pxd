#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
        object url
        bytes version
        bint should_keep_alive
        bytes content_type
        bytes method
        bytes body


cdef class HttpResponse:

    cdef:
        object status
        bint close_connection
        bytes content_type
        bytes body


cdef class HttpProtocol:

    cdef public object server
    cdef:
        object loop
        object parser
        object transport
        object unprocessed
        bint in_response

        HttpRequest current_request

    cdef _write(self, bytes req_version, bytes resp_status,
                bytes content_type, bytes body, bint close_connection)

    cdef write(self, HttpRequest request, HttpResponse response)

    cdef unhandled_exception(self, ex)
    cdef resume(self)
    cdef close(self)
