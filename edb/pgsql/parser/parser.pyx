#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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
    Any,
    NamedTuple,
    Optional,
)

import enum
import hashlib

from .exceptions import PSqlSyntaxError


from edb.server.pgproto.pgproto cimport (
    FRBuffer,
    ReadBuffer,
    WriteBuffer,
)

from libc.stdint cimport int8_t, uint8_t, int32_t


cdef extern from "pg_query.h":
    ctypedef struct PgQueryError:
        char *message
        int lineno
        int cursorpos

    ctypedef struct PgQueryParseResult:
        char *parse_tree
        PgQueryError *error

    ctypedef struct PgQueryNormalizeConstLocation:
        int location
        int length
        int param_id
        int token
        char *val

    ctypedef struct PgQueryNormalizeResult:
        char *normalized_query
        PgQueryError *error
        PgQueryNormalizeConstLocation *clocations
        int clocations_count
        int highest_extern_param_id

    PgQueryParseResult pg_query_parse(const char *input)
    void pg_query_free_parse_result(PgQueryParseResult result)

    PgQueryNormalizeResult pg_query_normalize(const char *input)
    void pg_query_free_normalize_result(PgQueryNormalizeResult result)


cdef extern from "protobuf/pg_query.pb-c.h":
    ctypedef struct ProtobufCEnumValue:
        const char *name
        const char *c_name
        int value

    ctypedef struct ProtobufCEnumDescriptor:
        pass

    ProtobufCEnumDescriptor pg_query__token__descriptor

    const ProtobufCEnumValue *protobuf_c_enum_descriptor_get_value(
        const ProtobufCEnumDescriptor *desc, int value)


def pg_parse(query) -> str:
    cdef PgQueryParseResult result

    result = pg_query_parse(query)
    if result.error:
        error = PSqlSyntaxError(
            result.error.message.decode('utf8'),
            result.error.lineno, result.error.cursorpos
        )
        pg_query_free_parse_result(result)
        raise error

    result_utf8 = result.parse_tree.decode('utf8')
    pg_query_free_parse_result(result)
    return result_utf8


class LiteralTokenType(enum.StrEnum):
    FCONST = "FCONST"
    SCONST = "SCONST"
    BCONST = "BCONST"
    XCONST = "XCONST"
    ICONST = "ICONST"
    TRUE_P = "TRUE_P"
    FALSE_P = "FALSE_P"


class PgLiteralTypeOID(enum.IntEnum):
    BOOL = 16
    INT4 = 23
    TEXT = 25
    UNKNOWN = 705
    VARBIT = 1562
    NUMERIC = 1700


class NormalizedQuery(NamedTuple):
    text: str
    highest_extern_param_id: int
    extracted_constants: list[tuple[int, LiteralTokenType, bytes]]


def pg_normalize(query: str) -> NormalizedQuery:
    cdef:
        PgQueryNormalizeResult result
        PgQueryNormalizeConstLocation loc
        const ProtobufCEnumValue *token
        int i
        bytes queryb
        bytes const

    queryb = query.encode("utf-8")
    result = pg_query_normalize(queryb)

    try:
        if result.error:
            error = PSqlSyntaxError(
                result.error.message.decode('utf8'),
                result.error.lineno, result.error.cursorpos
            )
            raise error

        normalized_query = result.normalized_query.decode('utf8')
        consts = []
        for i in range(result.clocations_count):
            loc = result.clocations[i]
            if loc.length != -1:
                if loc.param_id < 0:
                    # Negative param_id means *relative* to highest explicit
                    # param id (after taking the absolute value).
                    param_id = (
                        abs(loc.param_id)
                        + result.highest_extern_param_id
                    )
                else:
                    # Otherwise it's the absolute param id.
                    param_id = loc.param_id
                if loc.val != NULL:
                    token = protobuf_c_enum_descriptor_get_value(
                        &pg_query__token__descriptor, loc.token)
                    if token == NULL:
                        raise RuntimeError(
                            f"could not lookup pg_query enum descriptor "
                            f"for token value {loc.token}"
                        )
                    consts.append((
                        param_id,
                        LiteralTokenType(bytes(token.name).decode("ascii")),
                        bytes(loc.val),
                    ))

        return NormalizedQuery(
            text=normalized_query,
            highest_extern_param_id=result.highest_extern_param_id,
            extracted_constants=consts,
        )
    finally:
        pg_query_free_normalize_result(result)


cdef ReadBuffer _init_deserializer(serialized: bytes, tag: uint8_t, cls: str):
    cdef ReadBuffer buf

    buf = ReadBuffer.new_message_parser(serialized)

    if <uint8_t>buf.read_byte() != tag:
        raise ValueError(f"malformed {cls} serialization")

    return buf


cdef class Source:
    def __init__(
        self,
        text: str,
        serialized: Optional[bytes] = None,
    ) -> None:
        self._text = text
        if serialized is not None:
            self._serialized = serialized
        else:
            self._serialized = b''
        self._cache_key = b''

    @classmethod
    def _tag(self) -> int:
        return 0

    cdef WriteBuffer _serialize(self):
        cdef WriteBuffer buf = WriteBuffer.new()
        buf.write_byte(<int8_t>self._tag())
        buf.write_len_prefixed_utf8(self._text)
        return buf

    def serialize(self) -> bytes:
        if not self._serialized:
            self._serialized = bytes(self._serialize())
        return self._serialized

    @classmethod
    def from_serialized(cls, serialized: bytes) -> Source:
        cdef ReadBuffer buf

        buf = _init_deserializer(serialized, cls._tag(), cls.__name__)
        text = buf.read_len_prefixed_utf8()

        return Source(text, serialized)

    def text(self) -> str:
        return self._text

    def original_text(self) -> str:
        return self._text

    def cache_key(self) -> bytes:
        if not self._cache_key:
            h = hashlib.blake2b(self._tag().to_bytes())
            h.update(bytes(self.text(), 'UTF-8'))

            # Include types of extracted constants
            for extra_type_oid in self.extra_type_oids():
                h.update(extra_type_oid.to_bytes(8, signed=True))
            self._cache_key = h.digest()

        return self._cache_key

    def variables(self) -> dict[str, Any]:
        return {}

    def first_extra(self) -> Optional[int]:
        return None

    def extra_counts(self) -> Sequence[int]:
        return []

    def extra_blobs(self) -> Sequence[bytes]:
        return ()

    def extra_formatted_as_text(self) -> bool:
        return True

    def extra_type_oids(self) -> Sequence[int]:
        return ()

    @classmethod
    def from_string(cls, text: str) -> Source:
        return Source(text)


cdef class NormalizedSource(Source):
    def __init__(
        self,
        normalized: NormalizedQuery,
        orig_text: str,
        serialized: Optional[bytes] = None,
    ) -> None:
        super().__init__(text=normalized.text, serialized=serialized)
        self._extracted_constants = list(
            sorted(normalized.extracted_constants, key=lambda i: i[0]),
        )
        self._highest_extern_param_id = normalized.highest_extern_param_id
        self._orig_text = orig_text

    @classmethod
    def _tag(cls) -> int:
        return 1

    def original_text(self) -> str:
        return self._orig_text

    cdef WriteBuffer _serialize(self):
        cdef WriteBuffer buf

        buf = Source._serialize(self)
        buf.write_len_prefixed_utf8(self._orig_text)
        buf.write_int32(<int32_t>self._highest_extern_param_id)
        buf.write_int32(<int32_t>len(self._extracted_constants))
        for param_id, token, val in self._extracted_constants:
            buf.write_int32(<int32_t>param_id)
            buf.write_len_prefixed_utf8(token.value)
            buf.write_len_prefixed_bytes(val)

        return buf

    def variables(self) -> dict[str, bytes]:
        return {f"${n}": v for n, _, v in self._extracted_constants}

    def first_extra(self) -> Optional[int]:
        return (
            self._highest_extern_param_id
            if self._extracted_constants
            else None
        )

    def extra_counts(self) -> Sequence[int]:
        return [len(self._extracted_constants)]

    def extra_blobs(self) -> list[bytes]:
        cdef WriteBuffer buf
        buf = WriteBuffer.new()
        for _, _, v in self._extracted_constants:
            buf.write_len_prefixed_bytes(v)

        return [bytes(buf)]

    def extra_type_oids(self) -> Sequence[int]:
        oids = []
        for _, token, _ in self._extracted_constants:
            if token is LiteralTokenType.FCONST:
                oids.append(PgLiteralTypeOID.NUMERIC)
            elif token is LiteralTokenType.ICONST:
                oids.append(PgLiteralTypeOID.INT4)
            elif (
                token is LiteralTokenType.FALSE_P
                or token is LiteralTokenType.TRUE_P
            ):
                oids.append(PgLiteralTypeOID.BOOL)
            elif token is LiteralTokenType.SCONST:
                oids.append(PgLiteralTypeOID.UNKNOWN)
            elif (
                token is LiteralTokenType.XCONST
                or token is LiteralTokenType.BCONST
            ):
                oids.append(PgLiteralTypeOID.VARBIT)
            else:
                raise AssertionError(f"unexpected literal token type: {token}")

        return oids

    @classmethod
    def from_string(cls, text: str) -> NormalizedSource:
        normalized = pg_normalize(text)
        return NormalizedSource(normalized, text)

    @classmethod
    def from_serialized(cls, serialized: bytes) -> NormalizedSource:
        cdef ReadBuffer buf

        buf = _init_deserializer(serialized, cls._tag(), cls.__name__)
        text = buf.read_len_prefixed_utf8()
        orig_text = buf.read_len_prefixed_utf8()
        highest_extern_param_id = buf.read_int32()
        n_constants = buf.read_int32()
        consts = []
        for _ in range(n_constants):
            param_id = buf.read_int32()
            token = buf.read_len_prefixed_utf8()
            val = buf.read_len_prefixed_bytes()
            consts.append((param_id, LiteralTokenType(token), val))

        return NormalizedSource(
            NormalizedQuery(
                text=text,
                highest_extern_param_id=highest_extern_param_id,
                extracted_constants=consts,
            ),
            orig_text,
            serialized,
        )


def deserialize(serialized: bytes) -> Source:
    if serialized[0] == 0:
        return Source.from_serialized(serialized)
    elif serialized[0] == 1:
        return NormalizedSource.from_serialized(serialized)

    raise ValueError(f"Invalid type/version byte: {serialized[0]}")
