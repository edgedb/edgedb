##
# Copyright (c) 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from importkit import context as lang_context
from importkit.context import SourcePoint
from metamagic.utils.datastructures import xvalue
from metamagic.utils import markup



class LexError(Exception):
    def __init__(self, msg, *, line=None, col=None, filename=None, format=True):
        if format and '{' in msg:
            position = self._format_position(line, col, filename)
            msg = msg.format(line=line, col=col, filename=filename,
                             position=position)

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


class UnknownTokenError(LexError): pass


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
        return '<{} {} {!r}>'.format(self.id, self.token,
                                     self.regexp)


def group(*literals, _re_alpha=re.compile(r'[^\W\d_]')):
    rx = []
    for l in literals:
        if r'\b' not in l:
            l = re.escape(l)
        if _re_alpha.match(l):
            l = r'\b' + l + r'\b'
        rx.append(l)
    return ' | '.join(rx)


class ParserContext(lang_context.SourceContext, markup.MarkupExceptionContext):
    title = 'Parser Context'

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        prefix = '{} line={} col={}: '.format(self.name, self.start.line,
                                              self.start.column)
        snippet, offset = self.get_line_snippet(self.start,
                                                max_length=80 - len(prefix))
        errpos = ' ' * (len(prefix) + offset) + '^'
        prefix += snippet + '\n'

        body = []
        body.append(me.doc.Text(text=prefix))
        body.append(me.doc.Text(text=errpos))

        return me.lang.ExceptionContext(title=self.title, body=body)

    def get_line_snippet(self, point, max_length):
        if point.line > 1:
            linestart = self.buffer.rfind('\n', point.start.pointer)
        else:
            linestart = 0

        before = min(max_length // 2, point.pointer - linestart)
        after = max_length - before

        start = point.pointer - before
        end = point.pointer + after
        return self.buffer[start:end], before


class Lexer:
    NL = frozenset()
    MULTILINE_TOKENS = frozenset()
    RE_FLAGS = re.X | re.M

    def __init__(self):
        self.reset()

        re_states = {}
        for state, rules in self.states.items():
            res = []
            for rule in rules:
                res.append('(?P<{}>{})'.format(rule.id, rule.regexp))

            res.append('(?P<err>.)')

            full_re = ' | '.join(res)
            re_states[state] = re.compile(full_re, self.RE_FLAGS)

        self.re_states = re_states

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
        '''Returns a start token or None if no start token is wanted.'''
        return None

    def get_eof_token(self):
        '''Returns an EOF token or None if no EOF token is wanted.'''
        return None

    def token_from_text(self, rule_token, txt):
        '''Given the rule_token with txt create a token.

        Update the lexer lineno, column, and start.
        '''

        start_pos = SourcePoint(self.lineno, self.column, self.start)
        len_txt = len(txt)

        if rule_token is self.NL:
            # Newline -- increase line number & set col to 1
            self.lineno += 1
            self.column = 1

        elif rule_token in self.MULTILINE_TOKENS and '\n' in txt:
            # Advance line & col according to how many new lines
            # are in comments/strings/etc.
            self.lineno += txt.count('\n')
            self.column = len(txt.rsplit('\n', 1)[1]) + 1
        else:
            self.column += len_txt

        self.start += len_txt
        end_pos = SourcePoint(self.lineno, self.column, self.start)

        return xvalue(txt, type=rule_token, text=txt, start=start_pos,
                           end=end_pos, filename=self.filename)

    def lex(self):
        """Lexes the src.

        Generator. Yields tokens (as defined by the rules).

        May yield special start and EOF tokens.
        May raise UnknownTokenError exception."""

        src = self.inputstr
        re_states = self.re_states

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

    def handle_error(self, txt):
        raise UnknownTokenError(
            'Unknown token {!r} {{position}}'.format(txt),
            line=self.lineno, col=self.column, filename=self.filename)

    def token(self):
        '''Return the next token produced by the lexer.

        The token is an xvalue with the following attributes: type, text, start,
        end, and filename.
        '''

        if self._token_stream is None:
            self._token_stream = self.lex()

        try:
            return next(self._token_stream)
        except StopIteration:
            return None
