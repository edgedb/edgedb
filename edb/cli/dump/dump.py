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

import functools
import hashlib
import io
import os
import pathlib
import tempfile

import edgedb
from edgedb.protocol import dstructs

from edb.common import binwrapper

from . import consts


class DumpImpl:

    conn: edgedb.BlockingIOConnection

    # Mapping of `schema_object_id` to a list of data block sizes/checksums.
    blocks_datainfo: Dict[str, List[Tuple[int, bytes]]]

    def __init__(self, conn: edgedb.BlockingIOConnection) -> None:
        self.conn = conn
        self.blocks_datainfo = {}

    def _data_callback(
        self,
        tmpdir: pathlib.Path,
        data: dstructs.DumpDataBlock,
    ) -> None:
        fn = tmpdir / data.schema_object_id.hex
        with open(fn, 'ba+') as f:
            f.write(data.data)

        self.blocks_datainfo.setdefault(data.schema_object_id, []).append(
            (
                len(data.data),
                hashlib.sha1(data.data).digest()
            )
        )

    def _serialize_header(self, desc: dstructs.DumpDesc) -> bytes:
        buf = io.BytesIO()
        binbuf = binwrapper.BinWrapper(buf)

        binbuf.write_ui64(desc.server_ts)
        binbuf.write_len32_prefixed_bytes(desc.server_version)
        binbuf.write_len32_prefixed_bytes(desc.schema)

        binbuf.write_ui64(len(desc.blocks))
        for block in desc.blocks:
            block_di = self.blocks_datainfo[block.schema_object_id]

            if len(block_di) != block.data_blocks_count:
                raise RuntimeError(
                    'server reported data blocks count does not match '
                    'actual received')

            binbuf.write_bytes(block.schema_object_id.bytes)
            binbuf.write_ui32(len(block.schema_deps))
            for dep in block.schema_deps:
                binbuf.write_bytes(dep.bytes)
            binbuf.write_len32_prefixed_bytes(block.type_desc)
            binbuf.write_ui64(block.data_size)

            binbuf.write_ui64(block.data_blocks_count)
            total_size = 0
            for data_size, data_hash in block_di:
                binbuf.write_ui64(data_size)
                binbuf.write_bytes(data_hash)
                total_size += data_size

            if total_size != block.data_size:
                raise RuntimeError(
                    'server reported data block size does not match '
                    'actual received')

        return buf.getvalue()

    def dump(self, outfn: os.PathLike) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = pathlib.Path(tmp)

            desc = self.conn._dump(
                on_data=functools.partial(self._data_callback, tmpdir))

            header = self._serialize_header(desc)
            with open(outfn, 'wb+') as outf:
                buf = binwrapper.BinWrapper(outf)

                buf.write_bytes(consts.HEADER_TITLE)
                buf.write_ui64(consts.DUMP_PROTO_VER)

                buf.write_bytes(hashlib.sha1(header).digest())
                buf.write_ui64(len(header))
                buf.write_bytes(header)

                for block in desc.blocks:
                    datafn = tmpdir / block.schema_object_id.hex
                    with open(datafn, 'br') as dataf:
                        while True:
                            data = dataf.read(consts.COPY_BUFFER_SIZE)
                            if not data:
                                break
                            buf.write_bytes(data)
