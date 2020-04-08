import re

from typing import Optional, List, Tuple

from edb._edgeql_rust import tokenize as _tokenize, TokenizerError, Token

from edb.errors import base as base_errors, EdgeQLSyntaxError


cdef object TRAILING_WS_IN_CONTINUATION = re.compile(r'\\ \s+\n')


def tokenize(eql: bytes) -> List[Token]:
    try:
        eql_str = eql.decode()
        return _tokenize(eql_str)
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

