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

import io
import struct


class BinWrapper:
    """A utility binary-reader wrapper over any io.BytesIO object."""

    i64 = struct.Struct('!q')
    i32 = struct.Struct('!l')
    i16 = struct.Struct('!h')
    i8 = struct.Struct('!b')

    ui64 = struct.Struct('!Q')
    ui32 = struct.Struct('!L')
    ui16 = struct.Struct('!H')
    ui8 = struct.Struct('!B')

    def __init__(self, buf: io.BytesIO) -> None:
        self.buf = buf

    def write_ui64(self, val: int) -> None:
        self.buf.write(self.ui64.pack(val))

    def write_ui32(self, val: int) -> None:
        self.buf.write(self.ui32.pack(val))

    def write_ui16(self, val: int) -> None:
        self.buf.write(self.ui16.pack(val))

    def write_ui8(self, val: int) -> None:
        self.buf.write(self.ui8.pack(val))

    def write_i64(self, val: int) -> None:
        self.buf.write(self.i64.pack(val))

    def write_i32(self, val: int) -> None:
        self.buf.write(self.i32.pack(val))

    def write_i16(self, val: int) -> None:
        self.buf.write(self.i16.pack(val))

    def write_i8(self, val: int) -> None:
        self.buf.write(self.i8.pack(val))

    def write_len32_prefixed_bytes(self, val: bytes) -> None:
        self.write_ui32(len(val))
        self.buf.write(val)

    def write_bytes(self, val: bytes) -> None:
        self.buf.write(val)

    def read_ui64(self) -> int:
        data = self.buf.read(8)
        return self.ui64.unpack(data)[0]

    def read_ui32(self) -> int:
        data = self.buf.read(4)
        return self.ui32.unpack(data)[0]

    def read_ui16(self) -> int:
        data = self.buf.read(2)
        return self.ui16.unpack(data)[0]

    def read_ui8(self) -> int:
        data = self.buf.read(1)
        return self.ui8.unpack(data)[0]

    def read_i64(self) -> int:
        data = self.buf.read(8)
        return self.i64.unpack(data)[0]

    def read_i32(self) -> int:
        data = self.buf.read(4)
        return self.i32.unpack(data)[0]

    def read_i16(self) -> int:
        data = self.buf.read(2)
        return self.i16.unpack(data)[0]

    def read_i8(self) -> int:
        data = self.buf.read(1)
        return self.i8.unpack(data)[0]

    def read_bytes(self, size: int) -> bytes:
        data = self.buf.read(size)
        if len(data) != size:
            raise BufferError(f'cannot read bytes with len={size}')
        return data

    def read_len32_prefixed_bytes(self) -> bytes:
        size = self.read_ui32()
        return self.read_bytes(size)

    def read_nullable_len32_prefixed_bytes(self) -> bytes | None:
        size = self.read_i32()
        if size == -1:
            return None
        else:
            return self.read_bytes(size)

    def tell(self) -> int:
        return self.buf.tell()
