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

import re

from edb.common import lexer

from .keywords import edgeql_keywords


__all__ = ('EdgeQLLexer',)


STATE_KEEP = 0
STATE_BASE = 1


re_dquote = r'\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$'

Rule = lexer.Rule


class UnterminatedStringError(lexer.UnknownTokenError):
    pass


class PseudoRule(Rule):
    def __init__(self, *, token, regexp, rule_id, next_state=STATE_KEEP):
        self.id = rule_id
        Rule._map[rule_id] = self
        self.token = token
        self.next_state = next_state
        self.regexp = regexp


class EdgeQLLexer(lexer.Lexer):

    start_state = STATE_BASE

    MERGE_TOKENS = {('NAMED', 'ONLY'), ('SET', 'ANNOTATION'), ('SET', 'TYPE')}

    NL = 'NL'
    MULTILINE_TOKENS = frozenset(('SCONST', 'BCONST', 'RSCONST'))
    RE_FLAGS = re.X | re.M | re.I

    # Basic keywords
    keyword_rules = [Rule(token=tok[0],
                          next_state=STATE_KEEP,
                          regexp=lexer.group(val))
                     for val, tok in edgeql_keywords.items()]

    common_rules = keyword_rules + [
        Rule(token='WS',
             next_state=STATE_KEEP,
             regexp=r'[^\S\n]+'),

        Rule(token='NL',
             next_state=STATE_KEEP,
             regexp=r'\n'),

        Rule(token='COMMENT',
             next_state=STATE_KEEP,
             regexp=r'''\#.*?$'''),

        Rule(token='ASSIGN',
             next_state=STATE_KEEP,
             regexp=r':='),

        Rule(token='REMASSIGN',
             next_state=STATE_KEEP,
             regexp=r'-='),

        Rule(token='ADDASSIGN',
             next_state=STATE_KEEP,
             regexp=r'\+='),

        Rule(token='ARROW',
             next_state=STATE_KEEP,
             regexp=r'->'),

        Rule(token='??',
             next_state=STATE_KEEP,
             regexp=r'\?\?'),

        Rule(token='::',
             next_state=STATE_KEEP,
             regexp=r'::'),

        # special path operators
        Rule(token='.<',
             next_state=STATE_KEEP,
             regexp=r'\.<'),

        Rule(token='.>',
             next_state=STATE_KEEP,
             regexp=r'\.>'),

        Rule(token='//',
             next_state=STATE_KEEP,
             regexp=r'//'),

        Rule(token='++',
             next_state=STATE_KEEP,
             regexp=r'\+\+'),

        Rule(token='OP',
             next_state=STATE_KEEP,
             regexp=r'''
                (?: >= | <= | != | \?= | \?!=)
             '''),

        Rule(token='self',
             next_state=STATE_KEEP,
             regexp=r'[,()\[\].@;:+\-*/%^<>=&|]'),

        Rule(token='NCONST',
             next_state=STATE_KEEP,
             regexp=r"""
                (?:
                    (?: \d+ (?:\.\d+)?
                        (?:[eE](?:[+\-])?[0-9]+)
                    )
                    |
                    (?: \d+\.\d+)
                    |
                    ([1-9]\d* | 0)(?![0-9])
                )n
                """),

        Rule(token='FCONST',
             next_state=STATE_KEEP,
             regexp=r"""
                    (?: \d+ (?:\.\d+)?
                        (?:[eE](?:[+\-])?[0-9]+)
                    )
                    |
                    (?: \d+\.\d+)
                """),

        Rule(token='ICONST',
             next_state=STATE_KEEP,
             regexp=r'([1-9]\d* | 0)(?![0-9])'),

        Rule(token='BCONST',
             next_state=STATE_KEEP,
             regexp=rf'''
                (?:
                    b
                )
                (?P<BQ>
                    ' | "
                )
                (?:
                    (
                        \\\\ | \\['"] | \n | .
                        # we'll validate escape codes in the parser
                    )*?
                )
                (?P=BQ)
             '''),

        Rule(token='RSCONST',
             next_state=STATE_KEEP,
             regexp=rf'''
                (?:
                    r
                )?
                (?P<RQ>
                    (?:
                        (?<=r) (?: ' | ")
                    ) | (?:
                        (?<!r) (?: {re_dquote})
                    )
                )
                (?:
                    (
                        \n | .
                        # we'll validate escape codes in the parser
                    )*?
                )
                (?P=RQ)
             '''),

        Rule(token='SCONST',
             next_state=STATE_KEEP,
             regexp=rf'''
                (?P<Q>
                    ' | "
                )
                (?:
                    (
                        \\\\ | \\['"] | \n | .
                        # we'll validate escape codes in the parser
                    )*?
                )
                (?P=Q)
             '''),

        # this rule will capture malformed strings and allow us to
        # provide better error messages
        Rule(token='BADSCONST',
             next_state=STATE_KEEP,
             regexp=rf'''
                [rb]?
                (['"] | (?: {re_dquote}))
                [^\n]*
             '''),

        Rule(token='BADIDENT',
             next_state=STATE_KEEP,
             regexp=r'''
                    __[^\W\d]\w*__
                    |
                    `__([^`]|``)*__`(?!`)
                '''),

        Rule(token='IDENT',
             next_state=STATE_KEEP,
             regexp=r'[^\W\d]\w*'),

        Rule(token='QIDENT',
             next_state=STATE_KEEP,
             regexp=r'`([^`]|``)*`'),

        Rule(token='self',
             next_state=STATE_KEEP,
             regexp=r'[\{\}$]'),
    ]

    states = {
        STATE_BASE:
            common_rules,
    }

    # add capacity to handle a few tokens composed of 2 elements
    _possible_long_token = {x[0] for x in MERGE_TOKENS}
    _long_token_match = {x[1]: x[0] for x in MERGE_TOKENS}

    special_rules = [
        PseudoRule(token='UNKNOWN',
                   next_state=STATE_KEEP,
                   regexp=r'.',
                   rule_id='err')
    ]

    def __init__(self, *, strip_whitespace=True, raise_lexerror=True):
        super().__init__()
        self.strip_whitespace = strip_whitespace
        self.raise_lexerror = raise_lexerror

    def get_eof_token(self):
        """Return an EOF token or None if no EOF token is wanted."""
        return self.token_from_text('EOF', '')

    def token_from_text(self, rule_token, txt):
        if rule_token == 'BADSCONST':
            self.handle_error(f"Unterminated string {txt}",
                              exact_message=True,
                              exc_type=UnterminatedStringError)
        elif rule_token == 'BADIDENT':
            self.handle_error(txt)

        elif rule_token == 'QIDENT':
            if txt == '``':
                self.handle_error(f'Identifiers cannot be empty',
                                  exact_message=True)
            elif txt[1] == '@':
                self.handle_error(f'Identifiers cannot start with "@"',
                                  exact_message=True)
            elif '::' in txt:
                self.handle_error(f'Identifiers cannot contain "::"',
                                  exact_message=True)

        tok = super().token_from_text(rule_token, txt)

        if rule_token == 'self':
            tok = tok._replace(type=txt)

        elif rule_token == 'QIDENT':
            # Drop the quotes and replace the "``" inside with a "`"
            val = txt[1:-1].replace('``', '`')
            tok = tok._replace(type='IDENT', value=val)

        return tok

    def lex(self):
        buffer = []

        for tok in super().lex():
            tok_type = tok.type

            if self.strip_whitespace and tok_type in {'WS', 'NL', 'COMMENT'}:
                # Strip out whitespace and comments
                continue

            elif tok_type in self._possible_long_token:
                # Buffer in case this is a merged token
                if not buffer:
                    buffer.append(tok)
                else:
                    yield from iter(buffer)
                    buffer[:] = [tok]

            elif tok_type in self._long_token_match:
                prev_token = buffer[-1] if buffer else None
                if (prev_token and
                        prev_token.type == self._long_token_match[tok_type]):
                    tok = prev_token._replace(
                        value=prev_token.value + ' ' + tok.value,
                        type=prev_token.type + tok_type)
                    buffer.pop()
                yield tok

            else:
                if buffer:
                    yield from iter(buffer)
                    buffer[:] = []
                yield tok

    def lex_highlight(self):
        return super().lex()

    def handle_error(self, txt, *,
                     exact_message=False, exc_type=lexer.UnknownTokenError):
        if self.raise_lexerror:
            super().handle_error(
                txt, exact_message=exact_message, exc_type=exc_type)
