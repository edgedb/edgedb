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
from typing import *  # NoQA

import hashlib
import io
import os

import edgedb

from edb.common import binwrapper

from . import consts


class RestoreImpl:

    def _parse(
        self,
        f: io.FileIO,
    ) -> Tuple[bytes, Iterator[bytes]]:

        def block_reader(buf: binwrapper.BinWrapper) -> Iterable[bytes]:
            while True:
                try:
                    block_type = buf.read_bytes(1)
                except BufferError:
                    # No more data blocks to read
                    return

                if block_type != b'D':
                    raise RuntimeError('cannot read the data block')

                block_hash = buf.read_bytes(20)
                block_bytes = buf.read_len32_prefixed_bytes()
                if hashlib.sha1(block_bytes).digest() != block_hash:
                    raise RuntimeError(
                        'dump integrity is compromised: data block '
                        'does not match the checksum')

                yield block_bytes

        buf = binwrapper.BinWrapper(f)

        header = buf.read_bytes(consts.HEADER_TITLE_LEN)
        if header != consts.HEADER_TITLE:
            raise RuntimeError('not an EdgeDB dump')

        dump_ver = buf.read_ui64()
        if dump_ver > consts.MAX_SUPPORTED_DUMP_VER:
            raise RuntimeError(f'dump version {dump_ver} is not supported')

        block_type = buf.read_bytes(1)
        if block_type != b'H':
            raise RuntimeError('cannot find the header block')

        header_hash = buf.read_bytes(20)
        header_bytes = buf.read_len32_prefixed_bytes()
        if hashlib.sha1(header_bytes).digest() != header_hash:
            raise RuntimeError(
                'dump integrity is compromised: header data does not match '
                'the checksum')

        return header_bytes, block_reader(buf)

    def restore(
        self,
        conn: edgedb.BlockingIOConnection,
        dumpfn: os.PathLike
    ) -> None:
        with open(dumpfn, 'rb') as f:
            header, reader = self._parse(f)

            conn._restore(
                header=header,
                data_gen=reader,
            )
