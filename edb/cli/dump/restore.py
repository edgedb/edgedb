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

import collections
import hashlib
import io
import itertools
import os

import edgedb

from edb.common import binwrapper

from . import consts


class DumpBlockInfo(NamedTuple):
    schema_object_id: bytes
    schema_deps: List[bytes]
    type_desc: bytes

    data_offset: int
    data_size: int
    data_blocks: List[Tuple[int, bytes]]


class DumpInfo(NamedTuple):

    blocks: List[DumpBlockInfo]
    schema: bytes
    header_offset: int
    filename: str
    server_ts: int
    server_version: bytes
    dump_version: int


class RestoreImpl:

    def _parse(
        self,
        dumpfn: os.PathLike,
    ) -> DumpInfo:

        with open(dumpfn, 'rb') as f:
            buf = binwrapper.BinWrapper(f)

            header = buf.read_bytes(consts.HEADER_TITLE_LEN)
            if header != consts.HEADER_TITLE:
                raise RuntimeError('not an EdgeDB dump')

            dump_ver = buf.read_ui64()
            if dump_ver > consts.MAX_SUPPORTED_DUMP_VER:
                raise RuntimeError(f'dump version {dump_ver} is not supported')

            header_hash = buf.read_bytes(20)
            header_len = buf.read_ui64()
            header_bytes = buf.read_bytes(header_len)

            if hashlib.sha1(header_bytes).digest() != header_hash:
                raise RuntimeError(
                    'dump integrity is compamised: header data does not match '
                    'the checksum')

            header_buf = binwrapper.BinWrapper(io.BytesIO(header_bytes))

            server_ts = header_buf.read_ui64()
            server_version = header_buf.read_len32_prefixed_bytes()

            schema = header_buf.read_len32_prefixed_bytes()

            blocks: List[DumpBlockInfo] = []
            blocks_num = header_buf.read_ui64()
            offset = 0
            for _ in range(blocks_num):
                schema_object_id = header_buf.read_bytes(16)
                schema_deps: List[bytes] = []

                deps_num = header_buf.read_ui32()
                for _ in range(deps_num):
                    schema_deps.append(header_buf.read_bytes(16))

                type_desc = header_buf.read_len32_prefixed_bytes()
                block_size = header_buf.read_ui64()
                data_count = header_buf.read_ui64()

                data_blocks: List[Tuple[int, bytes]] = []
                for _ in range(data_count):
                    data_blocks.append(
                        (
                            header_buf.read_ui64(),
                            header_buf.read_bytes(20),
                        )
                    )

                blocks.append(
                    DumpBlockInfo(
                        schema_object_id=schema_object_id,
                        schema_deps=schema_deps,
                        type_desc=type_desc,
                        data_offset=offset,
                        data_size=block_size,
                        data_blocks=data_blocks,
                    )
                )

                offset += block_size

            return DumpInfo(
                blocks=blocks,
                schema=schema,
                header_offset=f.tell(),
                filename=dumpfn,
                server_ts=server_ts,
                server_version=server_version,
                dump_version=dump_ver,
            )

    def _interleave(
        self,
        factor: int,
        dumpfn: os.PathLike,
        info: DumpInfo
    ) -> Iterable[Tuple[bytes, bytes]]:

        def worker(
            blocks: Deque[DumpBlockInfo]
        ) -> Iterable[Tuple[bytes, bytes]]:
            while True:
                try:
                    block = blocks.popleft()
                except IndexError:
                    break

                with open(dumpfn, 'rb') as f:
                    offset = 0
                    buf = binwrapper.BinWrapper(f)
                    for dlen, dhash in block.data_blocks:
                        f.seek(info.header_offset + block.data_offset + offset)
                        data = buf.read_bytes(dlen)
                        if hashlib.sha1(data).digest() != dhash:
                            raise RuntimeError(
                                'dump integrity is compamised: data block '
                                'does not match the checksum')
                        offset += dlen
                        yield (block.schema_object_id, data)

        if factor <= 0:
            raise ValueError('invalid interleave factor')

        blocks: Deque[DumpBlockInfo] = collections.deque(info.blocks)
        workers = itertools.cycle(worker(blocks) for _ in range(factor))

        while True:
            stopped = 0
            for _ in range(factor):
                wrk = next(workers)
                try:
                    yield next(wrk)
                except StopIteration:
                    stopped += 1
            if stopped == factor:
                # All workers are exhausted
                return

    def _restore(
        self,
        conn: edgedb.BlockingIOConnection,
        dumpfn: os.PathLike,
        info: DumpInfo,
    ) -> None:
        data_gen = self._interleave(1, dumpfn, info)
        conn._restore(
            schema=info.schema,
            blocks=[(b.schema_object_id, b.type_desc) for b in info.blocks],
            data_gen=data_gen,
        )

    def restore(
        self,
        conn: edgedb.BlockingIOConnection,
        dumpfn: os.PathLike
    ) -> None:
        info = self._parse(dumpfn)
        self._restore(conn, dumpfn, info)
