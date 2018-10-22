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


import re

from edb.lang.common import lexer

from .keywords import edgeql_keywords


__all__ = ('EdgeQLLexer',)


STATE_KEEP = 0
STATE_BASE = 1


re_dquote = r'\$([A-Za-z\200-\377_][0-9]*)*\$'

Rule = lexer.Rule


class EdgeQLLexer(lexer.Lexer):

    start_state = STATE_BASE

    NL = 'NL'
    MULTILINE_TOKENS = frozenset(('SCONST',))
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

        Rule(token='OP',
             next_state=STATE_KEEP,
             regexp=r'''
                (?: >= | <= | != | \?= | \?!=)
             '''),

        # SQL ops
        Rule(token='self',
             next_state=STATE_KEEP,
             regexp=r'[,()\[\].@;:+\-*/%^<>=&|]'),

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

        Rule(token='SCONST',
             next_state=STATE_KEEP,
             regexp=rf'''
                (?P<Q>
                    # capture the opening quote in group Q
                    (
                        ' | " |
                        {re_dquote}
                    )
                )
                (?:
                    (\\['"] | \n | .)*?
                )
                (?P=Q)      # match closing quote type with whatever is in Q
             '''),

        Rule(token='BADIDENT',
             next_state=STATE_KEEP,
             regexp=r'''
                    __[^\W\d]\w*__
                    |
                    `__.*?__`
                '''),

        Rule(token='IDENT',
             next_state=STATE_KEEP,
             regexp=r'[^\W\d]\w*'),

        Rule(token='QIDENT',
             next_state=STATE_KEEP,
             regexp=r'`[^@].*?`'),

        Rule(token='self',
             next_state=STATE_KEEP,
             regexp=r'[\{\}$]'),
    ]

    states = {
        STATE_BASE:
            common_rules,
    }

    def token_from_text(self, rule_token, txt):
        if rule_token == 'BADIDENT':
            self.handle_error(txt)

        tok = super().token_from_text(rule_token, txt)

        if rule_token == 'self':
            tok = tok._replace(type=txt)

        elif rule_token == 'QIDENT':
            tok = tok._replace(type='IDENT', value=txt[1:-1])

        return tok

    def lex(self):
        buffer = []

        for tok in super().lex():
            tok_type = tok.type

            if tok_type in {'WS', 'NL', 'COMMENT'}:
                # Strip out whitespace and comments
                continue
            else:
                if buffer:
                    yield from iter(buffer)
                    buffer[:] = []
                yield tok

    def lex_highlight(self):
        return super().lex()
