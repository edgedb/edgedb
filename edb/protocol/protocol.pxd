#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


from gel.protocol.asyncio_proto cimport AsyncIOProtocol


cdef class Protocol(AsyncIOProtocol):
    cdef:
        public bytes last_state

    cdef parse_command_complete_message(self)
    cdef encode_state(self, state)


cdef class Connection:

    cdef:
        object _transport
        readonly list inbox
        AsyncIOProtocol _protocol
