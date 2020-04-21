import re

from typing import Optional, List, Tuple, Dict, Any

from edb._edgeql_rust import tokenize as _tokenize, TokenizerError, Token
from edb._edgeql_rust import normalize as _normalize, Entry

from edb.common import debug
from edb.errors import base as base_errors, EdgeQLSyntaxError


cdef object TRAILING_WS_IN_CONTINUATION = re.compile(r'\\ \s+\n')


class Denormalized:

    def __init__(self, source: str, tokens: List[Token]):
        self._source = source
        self._tokens = tokens

    def key(self) -> str:
        return self._source

    def variables(self) -> Dict[str, Any]:
        return {}

    def tokens(self) -> List[Token]:
        return self._tokens

    def first_extra(self) -> Optional[int]:
        return None

    def extra_count(self) -> int:
        return 0

    def extra_blob(self) -> bytes:
        return b''


def tokenize(eql: bytes) -> List[Token]:
    try:
        eql_str = eql.decode()
        return _tokenize(eql_str)
    except TokenizerError as e:
        message, position = e.args
        hint = _derive_hint(eql_str, message, position)
        raise EdgeQLSyntaxError(
            message, position=position, hint=hint) from e


def normalize(eql: bytes) -> List[Entry]:
    if debug.flags.edgeql_disable_normalization:
        return Denormalized(eql.decode(), tokenize(eql))
    else:
        eql_str = eql.decode()

        try:
            return _normalize(eql_str)
        except TokenizerError as e:
            message, position = e.args
            hint = _derive_hint(eql_str, message, position)
            raise EdgeQLSyntaxError(
                message, position=position, hint=hint) from e


def _derive_hint(
    input: str,
    message: str,
    position: Tuple[int, int, int],
) -> Optional[str]:
    _, _, off = position
    if message == r"invalid string literal: invalid escape sequence '\ '":
        if TRAILING_WS_IN_CONTINUATION.search(input[off:]):
            return "consider removing trailing whitespace"
