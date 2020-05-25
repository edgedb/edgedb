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


from __future__ import annotations

import enum
import typing

from edb.testbase import server

from . import protocol
from . import messages
from . import render_utils

from .messages import *  # NoQA


class ProtocolTestCase(server.ClusterTestCase):

    def setUp(self):
        self.con = self.loop.run_until_complete(
            protocol.new_connection(
                **self.get_connect_args()
            )
        )

    def tearDown(self):
        try:
            self.loop.run_until_complete(
                self.con.aclose()
            )
        finally:
            self.con = None


def render(
    obj: typing.Union[typing.Type[enum.Enum], typing.Type[messages.Struct]]
) -> str:
    if issubclass(obj, messages.Struct):
        return obj.render()
    else:
        assert issubclass(obj, enum.Enum)

        buf = render_utils.RenderBuffer()
        buf.write(f'enum {obj.__name__} {{')
        with buf.indent():
            for membername, member in obj.__members__.items():
                buf.write(
                    f'{membername.ljust(messages._PAD - 1)} = '
                    f'{member.value:#x};'
                )
        buf.write('};')
        return str(buf)
