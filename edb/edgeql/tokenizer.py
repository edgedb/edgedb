#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Optional, Tuple, Sequence, Dict, List

import re
import hashlib

import edb._edgeql_parser as ql_parser

from edb import errors


TRAILING_WS_IN_CONTINUATION = re.compile(r'\\ \s+\n')


def deserialize(serialized: bytes, text: str) -> Source:
    match serialized[0]:
        case 0:
            tokens = ql_parser.unpack(serialized)
            assert isinstance(tokens, list)
            return Source(text, tokens, serialized)
        case 1:
            entry = ql_parser.unpack(serialized)
            assert isinstance(entry, ql_parser.Entry)
            return NormalizedSource(entry, text, serialized)

    raise ValueError(f"Invalid type/version byte: {serialized[0]}")


class Source:
    def __init__(
        self,
        text: str,
        tokens: List[ql_parser.OpaqueToken],
        serialized: bytes,
    ) -> None:
        self._cache_key = hashlib.blake2b(serialized).digest()
        self._text = text
        self._tokens = tokens
        self._serialized = serialized

    def text(self) -> str:
        return self._text

    def cache_key(self) -> bytes:
        return self._cache_key

    def variables(self) -> Dict[str, Any]:
        return {}

    def tokens(self) -> List[ql_parser.OpaqueToken]:
        return self._tokens

    def first_extra(self) -> Optional[int]:
        return None

    def extra_counts(self) -> Sequence[int]:
        return ()

    def extra_blobs(self) -> Sequence[bytes]:
        return ()

    def extra_formatted_as_text(self) -> bool:
        return False

    def extra_type_oids(self) -> Sequence[int]:
        return ()

    def serialize(self) -> bytes:
        return self._serialized

    @staticmethod
    def from_string(text: str) -> Source:
        result = _tokenize(text)
        assert isinstance(result.out, list)
        return Source(
            text=text, tokens=result.out, serialized=result.pack()
        )

    def __repr__(self):
        return f'<edgeql.Source text={self._text!r}>'


class NormalizedSource(Source):
    def __init__(
        self,
        normalized: ql_parser.Entry,
        text: str,
        serialized: bytes,
    ) -> None:
        self._text = text
        self._cache_key = normalized.key
        self._tokens = normalized.tokens
        self._variables = normalized.get_variables()
        self._first_extra = normalized.first_extra
        self._extra_counts = normalized.extra_counts
        self._extra_blobs = normalized.extra_blobs
        self._serialized = serialized

    def text(self) -> str:
        return self._text

    def cache_key(self) -> bytes:
        return self._cache_key

    def variables(self) -> Dict[str, Any]:
        return self._variables

    def tokens(self) -> List[ql_parser.OpaqueToken]:
        return self._tokens

    def first_extra(self) -> Optional[int]:
        return self._first_extra

    def extra_counts(self) -> Sequence[int]:
        return self._extra_counts

    def extra_blobs(self) -> Sequence[bytes]:
        return self._extra_blobs

    @staticmethod
    def from_string(text: str) -> NormalizedSource:
        normalized = _normalize(text)
        return NormalizedSource(normalized, text, normalized.pack())


def inflate_span(
    source: str, span: Tuple[int, Optional[int]]
) -> Tuple[ql_parser.SourcePoint, Optional[ql_parser.SourcePoint]]:
    (start, end) = span
    source_bytes = source.encode('utf-8')

    [start_sp] = ql_parser.SourcePoint.from_offsets(source_bytes, [start])

    if end is not None:
        [end_sp] = ql_parser.SourcePoint.from_offsets(source_bytes, [end])
    else:
        end_sp = None

    return (start_sp, end_sp)


def inflate_position(
    source: str, span: Tuple[int, Optional[int]]
) -> Tuple[int, int, int, Optional[int]]:
    (start, end) = inflate_span(source, span)
    return (
        start.column,
        start.line,
        start.offset,
        end.offset if end else None,
    )


def _tokenize(eql: str) -> ql_parser.ParserResult:
    result = ql_parser.tokenize(eql)

    if len(result.errors) > 0:
        # TODO: emit multiple errors
        error = result.errors[0]

        message, span, hint, details = error
        position = inflate_position(eql, span)

        hint = _derive_hint(eql, message, position) or hint
        raise errors.EdgeQLSyntaxError(
            message, position=position, hint=hint, details=details
        )

    return result


def _normalize(eql: str) -> ql_parser.Entry:
    try:
        return ql_parser.normalize(eql)
    except ql_parser.SyntaxError as e:
        message, span, hint, details = e.args
        position = inflate_position(eql, span)

        hint = _derive_hint(eql, message, position) or hint
        raise errors.EdgeQLSyntaxError(
            message, position=position, hint=hint, details=details
        ) from e


def _derive_hint(
    input: str,
    message: str,
    position: Tuple[int, int, int, Optional[int]],
) -> Optional[str]:
    _, _, off, _ = position

    if message.endswith(
        r"invalid string literal: invalid escape sequence '\ '"
    ):
        if TRAILING_WS_IN_CONTINUATION.search(input[off:]):
            return "consider removing trailing whitespace"
    return None
