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
from typing import *

import re
import hashlib

from edb._edgeql_rust import tokenize as _tokenize, TokenizerError, Token
from edb._edgeql_rust import normalize as _normalize, Entry
from edb._edgeql_rust import parse_single as _parse_single
from edb._edgeql_rust import parse_block as _parse_block

from edb import errors
from edb.common import context as pctx

from . import ast as qlast


TRAILING_WS_IN_CONTINUATION = re.compile(r'\\ \s+\n')


class Source:
    def __init__(self, text: str, tokens: List[Token]) -> None:
        self._cache_key = hashlib.blake2b(text.encode('utf-8')).digest()
        self._text = text
        self._tokens = tokens

    def text(self) -> str:
        return self._text

    def cache_key(self) -> bytes:
        return self._cache_key

    def variables(self) -> Dict[str, Any]:
        return {}

    def tokens(self) -> List[Token]:
        return self._tokens

    def first_extra(self) -> Optional[int]:
        return None

    def extra_counts(self) -> Sequence[int]:
        return ()

    def extra_blobs(self) -> Sequence[bytes]:
        return ()

    @classmethod
    def from_string(cls, text: str) -> Source:
        return cls(text=text, tokens=tokenize(text))

    def __repr__(self):
        return f'<edgeql.Source text={self._text!r}>'


class NormalizedSource(Source):
    def __init__(self, normalized: Entry, text: str) -> None:
        self._text = text
        self._cache_key = normalized.key()
        self._tokens = normalized.tokens()
        self._variables = normalized.variables()
        self._first_extra = normalized.first_extra()
        self._extra_counts = normalized.extra_counts()
        self._extra_blobs = normalized.extra_blobs()

    def text(self) -> str:
        return self._text

    def cache_key(self) -> bytes:
        return self._cache_key

    def variables(self) -> Dict[str, Any]:
        return self._variables

    def tokens(self) -> List[Token]:
        return self._tokens

    def first_extra(self) -> Optional[int]:
        return self._first_extra

    def extra_counts(self) -> Sequence[int]:
        return self._extra_counts

    def extra_blobs(self) -> Sequence[bytes]:
        return self._extra_blobs

    @classmethod
    def from_string(cls, text: str) -> NormalizedSource:
        return cls(normalize(text), text)


def tokenize(eql: str) -> List[Token]:
    try:
        return _tokenize(eql)
    except TokenizerError as e:
        message, position = e.args
        hint = _derive_hint(eql, message, position)
        raise errors.EdgeQLSyntaxError(
            message, position=position, hint=hint
        ) from e


def normalize(eql: str) -> Entry:
    try:
        return _normalize(eql)
    except TokenizerError as e:
        message, position = e.args
        hint = _derive_hint(eql, message, position)
        raise errors.EdgeQLSyntaxError(
            message, position=position, hint=hint
        ) from e


def parse(eql: str, filename: Optional[str] = None) -> Sequence[qlast.Expr]:
    # WARN: These two imports are used for instantiation of AST nodes from Rust.
    from . import ast as qlast  # NoQA
    from . import qltypes  # NoQA

    ast, error_list = _parse_block(eql)

    if error_list:
        # TODO: return multiple errors instead of the only one here
        error = error_list[0]

        message, start, end = error

        filename = filename or '<string>'
        raise errors.EdgeQLSyntaxError(
            message,
            position=(None, None, start, end),
            context=pctx.ParserContext(
                name=filename, buffer=eql, start=start, end=end
            ),
        )

    return ast


def _derive_hint(
    input: str,
    message: str,
    position: Tuple[int, int, int],
) -> Optional[str]:
    _, _, off = position
    if message.endswith(
        r"invalid string literal: invalid escape sequence '\ '"
    ):
        if TRAILING_WS_IN_CONTINUATION.search(input[off:]):
            return "consider removing trailing whitespace"
    return None
