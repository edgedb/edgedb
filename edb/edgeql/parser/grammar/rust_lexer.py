from __future__ import annotations

from typing import Deque, Optional
from collections import deque
from edb._edgeql_rust import tokenize as _tokenize, Token


class EdgeQLLexer(object):
    inputstr: str
    tokens: Optional[Deque[Token]]
    filename: Optional[str]
    end_of_input: (int, int, int)

    def __init__(self):
        self.filename = None  # TODO

    def setinputstr(self, text: str) -> None:
        self.inputstr = text
        self.tokens = deque(_tokenize(text))
        self.end_of_input = self.tokens[-1].end()

    def token(self) -> Token:
        if self.tokens:
            return self.tokens.popleft()
