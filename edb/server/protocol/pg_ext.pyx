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


import sys

from libc.stdint cimport int16_t

from edb import errors
from edb.server.pgproto.pgproto cimport WriteBuffer
from edb.server.protocol cimport frontend


cdef class PgConnection(frontend.FrontendConnection):
    cdef _main_task_created(self):
        # complete the client initial message with a mocked type
        self.buffer.feed_data(b'\xff')

    async def authenticate(self):
        cdef int16_t proto_ver_major, proto_ver_minor

        for first in (True, False):
            if not self.buffer.take_message():
                await self.wait_for_message(report_idling=True)

            proto_ver_major = self.buffer.read_int16()
            proto_ver_minor = self.buffer.read_int16()
            if proto_ver_major == 1234:
                if proto_ver_minor == 5678:  # CancelRequest
                    if self.debug:
                        self.debug_print("CancelRequest")
                    raise NotImplementedError

                elif proto_ver_minor == 5679:  # SSLRequest
                    if self.debug:
                        self.debug_print("SSLRequest")
                    if not first:
                        raise errors.ProtocolError("found multiple SSLRequest")

                    self.buffer.finish_message()
                    if self._transport is None:
                        raise ConnectionAbortedError
                    if self.debug:
                        self.debug_print("N for SSLRequest")
                    self._transport.write(b'N')
                    # complete the next client message with a mocked type
                    self.buffer.feed_data(b'\xff')

                elif proto_ver_minor == 5680:  # GSSENCRequest
                    raise NotImplementedError("GSSAPI encryption unsupported")

                else:
                    raise NotImplementedError

            elif proto_ver_major == 3 and proto_ver_minor == 0:
                # StartupMessage with 3.0 protocol
                if self.debug:
                    self.debug_print("StartupMessage")
                raise NotImplementedError

            else:
                raise NotImplementedError("Invalid protocol version")

    def debug_print(self, *args):
        print("::PGEXT::", *args, file=sys.stderr)


def new_pg_connection(server):
    return PgConnection(server, passive=False)
