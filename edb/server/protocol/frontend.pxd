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


from edb.server.pgproto.pgproto cimport ReadBuffer, WriteBuffer


cdef class AbstractFrontendConnection:

    cdef write(self, WriteBuffer buf)
    cdef flush(self)


cdef class FrontendConnection(AbstractFrontendConnection):

    cdef:
        object server
        object loop

        object _transport
        WriteBuffer _write_buf
        object _write_waiter

        ReadBuffer buffer
        object _msg_take_waiter

        object started_idling_at
        bint idling

        bint _passive_mode

        bint authed
        object _main_task
        bint _cancelled
        bint _stop_requested

    cdef _after_idling(self)
    cdef _main_task_created(self)
    cdef _main_task_stopped_normally(self)
    cdef write_error(self, exc)
    cdef _cancel(self)
