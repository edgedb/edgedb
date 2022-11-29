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


import time

DEF FLUSH_BUFFER_AFTER = 100_000


cdef class AbstractFrontendConnection:

    cdef write(self, WriteBuffer buf):
        raise NotImplementedError

    cdef flush(self):
        raise NotImplementedError


cdef class FrontendConnection(AbstractFrontendConnection):

    def __init__(self, server, *, passive: bool):
        self.server = server
        self.loop = server.get_loop()

        self._transport = None
        self._write_buf = None
        self._write_waiter = None

        self.buffer = ReadBuffer()
        self._msg_take_waiter = None

        self.idling = False
        self.started_idling_at = 0.0

        # In "passive" mode the protocol is instantiated to parse and execute
        # just what's in the buffer. It cannot "wait for message". This
        # is used to implement binary protocol over http+fetch.
        self._passive_mode = passive

    # I/O write methods, implements AbstractFrontendConnection

    cdef write(self, WriteBuffer buf):
        # One rule for this method: don't write partial messages.
        if self._write_buf is not None:
            self._write_buf.write_buffer(buf)
            if self._write_buf.len() >= FLUSH_BUFFER_AFTER:
                self.flush()
        else:
            self._write_buf = buf

    cdef flush(self):
        if self._transport is None:
            # could be if the connection is lost and a coroutine
            # method is finalizing.
            raise ConnectionAbortedError
        if self._write_buf is not None and self._write_buf.len():
            buf = self._write_buf
            self._write_buf = None
            self._transport.write(memoryview(buf))

    def pause_writing(self):
        if self._write_waiter and not self._write_waiter.done():
            return
        self._write_waiter = self.loop.create_future()

    def resume_writing(self):
        if not self._write_waiter or self._write_waiter.done():
            return
        self._write_waiter.set_result(True)

    # I/O read methods

    def data_received(self, data):
        self.buffer.feed_data(data)
        if self._msg_take_waiter is not None and self.buffer.take_message():
            self._msg_take_waiter.set_result(True)
            self._msg_take_waiter = None

    def eof_received(self):
        pass

    cdef _after_idling(self):
        # Hook for EdgeConnection
        pass

    async def wait_for_message(self, *, bint report_idling):
        if self.buffer.take_message():
            return
        if self._passive_mode:
            raise RuntimeError('cannot wait for more messages in passive mode')
        if self._transport is None:
            # could be if the connection is lost and a coroutine
            # method is finalizing.
            raise ConnectionAbortedError

        self._msg_take_waiter = self.loop.create_future()
        if report_idling:
            self.idling = True
            self.started_idling_at = time.monotonic()

        try:
            await self._msg_take_waiter
        finally:
            self.idling = False

        self._after_idling()

    def is_idle(self, expiry_time: float):
        # A connection is idle if it awaits for the next message for
        # client for too long (even if it is in an open transaction!)
        return self.idling and self.started_idling_at < expiry_time
