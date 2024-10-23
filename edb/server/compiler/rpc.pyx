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

from typing import (
    Mapping,
)

import hashlib
import uuid

cimport cython
import immutables

from edb import edgeql, errors
from edb.common import uuidgen
from edb.edgeql import qltypes
from edb.edgeql import tokenizer
from edb.server import config, defines
from edb.server.pgproto.pgproto cimport WriteBuffer, ReadBuffer

from . import enums, sertypes

cdef object OUT_FMT_BINARY = enums.OutputFormat.BINARY
cdef object OUT_FMT_JSON = enums.OutputFormat.JSON
cdef object OUT_FMT_JSON_ELEMENTS = enums.OutputFormat.JSON_ELEMENTS
cdef object OUT_FMT_NONE = enums.OutputFormat.NONE

cdef object IN_FMT_BINARY = enums.InputFormat.BINARY
cdef object IN_FMT_JSON = enums.InputFormat.JSON

cdef char MASK_JSON_PARAMETERS  = 1 << 0
cdef char MASK_EXPECT_ONE       = 1 << 1
cdef char MASK_INLINE_TYPEIDS   = 1 << 2
cdef char MASK_INLINE_TYPENAMES = 1 << 3
cdef char MASK_INLINE_OBJECTIDS = 1 << 4


cdef char serialize_output_format(val):
    if val is OUT_FMT_BINARY:
        return b'b'
    elif val is OUT_FMT_JSON:
        return b'j'
    elif val is OUT_FMT_JSON_ELEMENTS:
        return b'J'
    elif val is OUT_FMT_NONE:
        return b'n'
    else:
        raise AssertionError("unreachable")


cdef deserialize_output_format(char mode):
    if mode == b'b':
        return OUT_FMT_BINARY
    elif mode == b'j':
        return OUT_FMT_JSON
    elif mode == b'J':
        return OUT_FMT_JSON_ELEMENTS
    elif mode == b'n':
        return OUT_FMT_NONE
    else:
        raise errors.BinaryProtocolError(
            f'unknown output mode "{repr(mode)[2:-1]}"')


@cython.final
cdef class CompilationRequest:
    def __cinit__(
        self,
        *,
        source: edgeql.Source,
        protocol_version: defines.ProtocolVersion,
        schema_version: uuid.UUID,
        compilation_config_serializer: sertypes.CompilationConfigSerializer,
        output_format: enums.OutputFormat = OUT_FMT_BINARY,
        input_format: enums.InputFormat = IN_FMT_BINARY,
        expect_one: bint = False,
        implicit_limit: int = 0,
        inline_typeids: bint = False,
        inline_typenames: bint = False,
        inline_objectids: bint = True,
        modaliases: Mapping[str | None, str] | None = None,
        session_config: Mapping[str, config.SettingValue] | None = None,
        database_config: Mapping[str, config.SettingValue] | None = None,
        system_config: Mapping[str, config.SettingValue] | None = None,
    ):
        self._serializer = compilation_config_serializer
        self.source = source
        self.protocol_version = protocol_version
        self.output_format = output_format
        self.input_format = input_format
        self.expect_one = expect_one
        self.implicit_limit = implicit_limit
        self.inline_typeids = inline_typeids
        self.inline_typenames = inline_typenames
        self.inline_objectids = inline_objectids
        self.schema_version = schema_version
        self.modaliases = modaliases
        self.session_config = session_config
        self.database_config = database_config
        self.system_config = system_config

        self.serialized_cache = None
        self.cache_key = None

    def __copy__(self):
        cdef CompilationRequest rv

        rv = CompilationRequest(
            source=self.source,
            protocol_version=self.protocol_version,
            schema_version=self.schema_version,
            compilation_config_serializer=self._serializer,
            output_format=self.output_format,
            input_format=self.input_format,
            expect_one=self.expect_one,
            implicit_limit=self.implicit_limit,
            inline_typeids=self.inline_typeids,
            inline_typenames=self.inline_typenames,
            inline_objectids=self.inline_objectids,
            modaliases=self.modaliases,
            session_config=self.session_config,
            database_config=self.database_config,
            system_config=self.system_config,
        )
        rv.serialized_cache = self.serialized_cache
        rv.cache_key = self.cache_key
        return rv

    def set_modaliases(self, value) -> CompilationRequest:
        self.modaliases = value
        self.serialized_cache = None
        self.cache_key = None
        return self

    def set_session_config(self, value) -> CompilationRequest:
        self.session_config = value
        self.serialized_cache = None
        self.cache_key = None
        return self

    def set_database_config(self, value) -> CompilationRequest:
        self.database_config = value
        self.serialized_cache = None
        self.cache_key = None
        return self

    def set_system_config(self, value) -> CompilationRequest:
        self.system_config = value
        self.serialized_cache = None
        self.cache_key = None
        return self

    def set_schema_version(self, version: uuid.UUID) -> CompilationRequest:
        self.schema_version = version
        self.serialized_cache = None
        self.cache_key = None
        return self

    @classmethod
    def deserialize(
        cls,
        data: bytes,
        query_text: str,
        compilation_config_serializer: sertypes.CompilationConfigSerializer,
    ) -> CompilationRequest:
        buf = ReadBuffer.new_message_parser(data)

        if data[0] == 0:
            return _deserialize_comp_req_v0(
                buf, query_text, compilation_config_serializer)
        else:
            raise errors.UnsupportedProtocolVersionError(
                f"unsupported compile cache: version {data[0]}"
            )

    def serialize(self) -> bytes:
        if self.serialized_cache is None:
            self._serialize()
        return self.serialized_cache

    def get_cache_key(self) -> uuid.UUID:
        if self.cache_key is None:
            self._serialize()
        return self.cache_key

    cdef _serialize(self):
        cache_key, buf = _serialize_comp_req_v0(self)
        self.cache_key = cache_key
        self.serialized_cache = bytes(buf)

    def __hash__(self):
        return hash(self.get_cache_key())

    def __eq__(self, other: CompilationRequest) -> bool:
        return (
            self.source.cache_key() == other.source.cache_key() and
            self.protocol_version == other.protocol_version and
            self.output_format == other.output_format and
            self.input_format == other.input_format and
            self.expect_one == other.expect_one and
            self.implicit_limit == other.implicit_limit and
            self.inline_typeids == other.inline_typeids and
            self.inline_typenames == other.inline_typenames and
            self.inline_objectids == other.inline_objectids
        )


cdef _deserialize_comp_req_v0(
    buf: ReadBuffer,
    query_text: str,
    compilation_config_serializer: sertypes.CompilationConfigSerializer,
):
    # Format:
    #
    # * 1 byte of version (0)
    # * 1 byte of bit flags:
    #   * json_parameters
    #   * expect_one
    #   * inline_typeids
    #   * inline_typenames
    #   * inline_objectids
    # * protocol_version (major: int64, minor: int16)
    # * 1 byte output_format (the same as in the binary protocol)
    # * implicit_limit: int64
    # * Module aliases:
    #   * length: int32 (negative means the modaliases is None)
    #   * For each alias pair:
    #      * 1 byte, 0 if the name is None
    #      * else, C-String as the name
    #      * C-String as the alias
    # * Session config type descriptor
    #   * 16 bytes type ID
    #   * int32-length-prefixed serialized type descriptor
    # * Session config: int32-length-prefixed serialized data
    # * Serialized Source or NormalizedSource without the original query
    #   string
    # * 16-byte cache key = BLAKE-2b hash of:
    #    * All above serialized,
    #    * Except that the source is replaced with Source.cache_key(), and
    #    * Except that the serialized session config is replaced by
    #      serialized combined config (session -> database -> system)
    #      that only affects compilation.
    #    * The schema version
    #  * OPTIONALLY, the schema version. We wanted to bump the protocol
    #    version to include this, but 5.x hard crashes when it reads a
    #    persistent cache with entries it doesn't understand, so instead
    #    we stick it on the end where it will be ignored by old versions.

    cdef char flags

    assert buf.read_byte() == 0  # version

    flags = buf.read_byte()
    if flags & MASK_JSON_PARAMETERS > 0:
        input_format = IN_FMT_JSON
    else:
        input_format = IN_FMT_BINARY
    expect_one = flags & MASK_EXPECT_ONE > 0
    inline_typeids = flags & MASK_INLINE_TYPEIDS > 0
    inline_typenames = flags & MASK_INLINE_TYPENAMES > 0
    inline_objectids = flags & MASK_INLINE_OBJECTIDS > 0

    protocol_version = buf.read_int16(), buf.read_int16()
    output_format = deserialize_output_format(buf.read_byte())
    implicit_limit = buf.read_int64()

    size = buf.read_int32()
    if size >= 0:
        modaliases = []
        for _ in range(size):
            if buf.read_byte():
                k = buf.read_null_str().decode("utf-8")
            else:
                k = None
            v = buf.read_null_str().decode("utf-8")
            modaliases.append((k, v))
        modaliases = immutables.Map(modaliases)
    else:
        modaliases = None

    serializer = compilation_config_serializer
    type_id = uuidgen.from_bytes(buf.read_bytes(16))
    if type_id == serializer.type_id:
        buf.read_len_prefixed_bytes()
    else:
        serializer = sertypes.CompilationConfigSerializer(
            type_id, buf.read_len_prefixed_bytes(), defines.CURRENT_PROTOCOL
        )

    data = buf.read_len_prefixed_bytes()
    if data:
        session_config = immutables.Map(
            (
                k,
                config.SettingValue(
                    name=k,
                    value=v,
                    source='session',
                    scope=qltypes.ConfigScope.SESSION,
                )
            ) for k, v in serializer.decode(data).items()
        )
    else:
        session_config = None

    source = tokenizer.deserialize(
        buf.read_len_prefixed_bytes(), query_text
    )

    cache_key = uuidgen.from_bytes(buf.read_bytes(16))
    schema_version = uuidgen.from_bytes(buf.read_bytes(16))

    req = CompilationRequest(
        source=source,
        protocol_version=protocol_version,
        schema_version=schema_version,
        compilation_config_serializer=serializer,
        output_format=output_format,
        input_format=input_format,
        expect_one=expect_one,
        implicit_limit=implicit_limit,
        inline_typeids=inline_typeids,
        inline_typenames=inline_typenames,
        inline_objectids=inline_objectids,
        modaliases=modaliases,
        session_config=session_config,
    )

    req.serialized_cache = data
    req.cache_key = cache_key

    return req


cdef _serialize_comp_req_v0(req: CompilationRequest):
    # Please see _deserialize_v0 for the format doc

    cdef:
        char version = 0, flags
        WriteBuffer out = WriteBuffer.new()

    out.write_byte(version)

    flags = (
        (MASK_JSON_PARAMETERS if req.input_format is IN_FMT_JSON else 0) |
        (MASK_EXPECT_ONE if req.expect_one else 0) |
        (MASK_INLINE_TYPEIDS if req.inline_typeids else 0) |
        (MASK_INLINE_TYPENAMES if req.inline_typenames else 0) |
        (MASK_INLINE_OBJECTIDS if req.inline_objectids else 0)
    )
    out.write_byte(flags)

    out.write_int16(req.protocol_version[0])
    out.write_int16(req.protocol_version[1])
    out.write_byte(serialize_output_format(req.output_format))
    out.write_int64(req.implicit_limit)

    if req.modaliases is None:
        out.write_int32(-1)
    else:
        out.write_int32(len(req.modaliases))
        for k, v in sorted(
            req.modaliases.items(),
            key=lambda i: (0, i[0]) if i[0] is None else (1, i[0])
        ):
            if k is None:
                out.write_byte(0)
            else:
                out.write_byte(1)
                out.write_str(k, "utf-8")
            out.write_str(v, "utf-8")

    type_id, desc = req._serializer.describe()
    out.write_bytes(type_id.bytes)
    out.write_len_prefixed_bytes(desc)

    hash_obj = hashlib.blake2b(memoryview(out), digest_size=16)
    hash_obj.update(req.source.cache_key())

    if req.session_config is None:
        session_config = b""
    else:
        session_config = req._serializer.encode_configs(
            req.session_config
        )
    out.write_len_prefixed_bytes(session_config)

    # Build config that affects compilation: session -> database -> system.
    # This is only used for calculating cache_key, while session
    # config itreq is separately stored above in the serialized format.
    serialized_comp_config = req._serializer.encode_configs(
        req.system_config, req.database_config, req.session_config
    )
    hash_obj.update(serialized_comp_config)

    # Must set_schema_version() before serializing compilation request
    assert req.schema_version is not None
    hash_obj.update(req.schema_version.bytes)

    cache_key_bytes = hash_obj.digest()
    cache_key = uuidgen.from_bytes(cache_key_bytes)

    out.write_len_prefixed_bytes(req.source.serialize())
    out.write_bytes(cache_key_bytes)
    out.write_bytes(req.schema_version.bytes)

    return cache_key, out
