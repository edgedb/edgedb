#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2014-present MagicStack Inc. and the EdgeDB authors.
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

import collections
import re
import types

from edb.common import context as pctx


class LexError(Exception):
    def __init__(
            self, msg, *, line=None, col=None, filename=None, format=True):
        if format and '{' in msg:
            position = self._format_position(line, col, filename)
            msg = msg.format(
                line=line, col=col, filename=filename, position=position)

        super().__init__(msg)
        self.line = line
        self.col = col
        self.filename = filename

    @classmethod
    def _format_position(cls, line, col, filename):
        position = 'at {}:{}'.format(line, col)
        if filename:
            position += ' of ' + str(filename)
        return position


Token = collections.namedtuple(
    'Token', ['value', 'type', 'text', 'start', 'end', 'filename'])


class UnknownTokenError(LexError):
    pass


class Rule:
    _idx = 0
    _map = {}

    def __init__(self, *, token, next_state, regexp):
        cls = self.__class__
        cls._idx += 1
        self.id = 'rule{}'.format(cls._idx)
        cls._map[self.id] = self

        self.token = token
        self.next_state = next_state
        self.regexp = regexp

    def __repr__(self):
        return '<{} {} {!r}>'.format(self.id, self.token, self.regexp)


def group(*literals, _re_alpha=re.compile(r'^\w+$'), asbytes=False):
    rx = []
    for lit in literals:
        if r'\b' not in lit:
            lit = re.escape(lit)
        if _re_alpha.match(lit):
            lit = r'\b' + lit + r'\b'
        rx.append(lit)
    result = ' | '.join(rx)
    if asbytes:
        result = result.encode()

    return result


class Lexer:

    NL = frozenset()
    MULTILINE_TOKENS = frozenset()
    RE_FLAGS = re.X | re.M
    asbytes = False
    _NL = '\n'

    def __init_subclass__(cls):
        if not hasattr(cls, 'states'):
            return

        re_states = {}
        for state, rules in cls.states.items():
            res = []
            for rule in rules:
                if cls.asbytes:
                    res.append(b'(?P<%b>%b)' % (rule.id.encode(), rule.regexp))
                else:
                    res.append('(?P<{}>{})'.format(rule.id, rule.regexp))

            if cls.asbytes:
                res.append(b'(?P<err>.)')
            else:
                res.append('(?P<err>.)')

            if cls.asbytes:
                full_re = b' | '.join(res)
            else:
                full_re = ' | '.join(res)
            re_states[state] = re.compile(full_re, cls.RE_FLAGS)

        cls.re_states = types.MappingProxyType(re_states)

    def __init__(self):
        self.reset()
        if self.asbytes:
            self._NL = b'\n'

    def reset(self):
        self.lineno = 1
        self.column = 1
        self._state = self.start_state
        self._states = []

    def setinputstr(self, inputstr, filename=None):
        self.inputstr = inputstr
        self.filename = filename
        self.start = 0
        self.end = len(inputstr)
        self.reset()
        self._token_stream = None

    def get_start_token(self):
        """Return a start token or None if no start token is wanted."""
        return None

    def get_eof_token(self):
        """Return an EOF token or None if no EOF token is wanted."""
        return None

    def token_from_text(self, rule_token, txt):
        """Given the rule_token with txt create a token.

        Update the lexer lineno, column, and start.
        """
        start_pos = pctx.SourcePoint(self.lineno, self.column, self.start)
        len_txt = len(txt)

        if rule_token is self.NL:
            # Newline -- increase line number & set col to 1
            self.lineno += 1
            self.column = 1

        elif rule_token in self.MULTILINE_TOKENS and self._NL in txt:
            # Advance line & col according to how many new lines
            # are in comments/strings/etc.
            self.lineno += txt.count(self._NL)
            self.column = len(txt.rsplit(self._NL, 1)[1]) + 1
        else:
            self.column += len_txt

        self.start += len_txt
        end_pos = pctx.SourcePoint(self.lineno, self.column, self.start)

        return Token(txt, type=rule_token, text=txt,
                     start=start_pos, end=end_pos,
                     filename=self.filename)

    def lex(self):
        """Tokenize the src.

        Generator. Yields tokens (as defined by the rules).

        May yield special start and EOF tokens.
        May raise UnknownTokenError exception.
        """
        src = self.inputstr

        start_tok = self.get_start_token()
        if start_tok is not None:
            yield start_tok

        while self.start < self.end:
            for match in self.re_states[self._state].finditer(src, self.start):
                rule_id = match.lastgroup

                txt = match.group(rule_id)

                if rule_id == 'err':
                    # Error group -- no rule has been matched
                    self.handle_error(txt)

                rule = Rule._map[rule_id]
                rule_token = rule.token

                token = self.token_from_text(rule_token, txt)

                yield token

                if rule.next_state and rule.next_state != self._state:
                    # Rule dictates that the lexer state should be
                    # switched
                    self._state = rule.next_state
                    break

        # End of file
        eof_tok = self.get_eof_token()
        if eof_tok is not None:
            yield eof_tok

    def handle_error(self, txt, *,
                     exact_message=False, exc_type=UnknownTokenError):
        if exact_message:
            msg = txt
        else:
            msg = f"Unexpected '{txt}'"

        raise exc_type(
            msg, line=self.lineno, col=self.column, filename=self.filename)

    def token(self):
        """Return the next token produced by the lexer.

        The token is an xvalue with the following attributes: type,
        text, start, end, and filename.
        """
        if self._token_stream is None:
            self._token_stream = self.lex()

        try:
            return next(self._token_stream)
        except StopIteration:
            return None
