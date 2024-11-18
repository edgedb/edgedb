#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

import asyncio
import os
import io
import logging

logger = logging.getLogger("edb.server")

from typing import Protocol, Optional, Tuple, Any, Callable


MAX_BATCH_SIZE = 16


class RustPipeProtocol(Protocol):
    def _read(self) -> Tuple[Any, ...]: ...

    def _try_read(self) -> Optional[Tuple[Any, ...]]: ...

    def _close_pipe(self) -> None: ...

    _fd: int


class RustAsyncChannel[T: RustPipeProtocol]:
    _buffered_reader: io.BufferedReader
    _buffer: bytes
    _skip_reads: int
    _closed: asyncio.Event

    def __init__(
        self, pipe: T, callback: Callable[[], Tuple[Any, ...]]
    ) -> None:
        fd = pipe._fd
        self._buffered_reader = io.BufferedReader(io.FileIO(fd))
        self._fd = fd
        self._pipe = pipe
        self._callback = callback
        self._buffer = bytearray(MAX_BATCH_SIZE)
        self._skip_reads = 0
        self._closed = asyncio.Event()

    def __del__(self):
        if not self._closed.is_set():
            logger.error(f"RustAsyncChannel {id(self)} was not closed")

    async def run(self):
        loop = asyncio.get_running_loop()
        loop.add_reader(self._fd, self._channel_read)
        try:
            await self._closed.wait()
        finally:
            loop.remove_reader(self._fd)

    def close(self):
        if not self._closed.is_set():
            self._pipe._close_pipe()
            self._buffered_reader.close()
            self._closed.set()

    def read_hint(self):
        while msg := self._pipe._try_read():
            self._skip_reads += 1
            self._callback(msg)

    def _channel_read(self) -> None:
        n = self._buffered_reader.readinto1(self._buffer)
        if n <= 0:
            return
        if self._skip_reads > n:
            self._skip_reads = 0
            return
        n -= self._skip_reads
        self._skip_reads = 0
        try:
            for _ in range(n):
                msg = self._pipe._read()
                if msg is None:
                    self.close()
                    return
                self._callback(msg)
        except Exception as e:
            logger.error(f"Error reading from Rust async channel: {e}")
            self.close()
