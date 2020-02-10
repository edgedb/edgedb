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


from __future__ import annotations
from typing import *

import functools
import hashlib
import os

import edgedb

from edb.common import binwrapper

from . import consts


class DumpImpl:

    conn: edgedb.BlockingIOConnection

    def __init__(self, conn: edgedb.BlockingIOConnection) -> None:
        self.conn = conn

    def _header_callback(
        self,
        outbuf: binwrapper.BinWrapper,
        data: bytes,
    ) -> None:
        outbuf.write_bytes(b'H')
        outbuf.write_bytes(hashlib.sha1(data).digest())
        outbuf.write_len32_prefixed_bytes(data)

    def _block_callback(
        self,
        outbuf: binwrapper.BinWrapper,
        data: bytes,
    ) -> None:
        outbuf.write_bytes(b'D')
        outbuf.write_bytes(hashlib.sha1(data).digest())
        outbuf.write_len32_prefixed_bytes(data)

    def dump(self, outfn: os.PathLike) -> None:
        with open(outfn, 'wb+') as outf:
            buf = binwrapper.BinWrapper(outf)
            buf.write_bytes(consts.HEADER_TITLE)
            buf.write_ui64(consts.DUMP_PROTO_VER)

            self.conn._dump(
                on_header=functools.partial(self._header_callback, buf),
                on_data=functools.partial(self._block_callback, buf))
