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


import re

from edb.common import lexer
from .keywords import graphql_keywords


__all__ = ('GraphQLLexer', 'UnterminatedStringError')


class UnterminatedStringError(lexer.LexError):
    pass


STATE_KEEP = 0
STATE_BASE = 1


Rule = lexer.Rule


class GraphQLLexer(lexer.Lexer):

    start_state = STATE_BASE

    NL = 'NL'
    RE_FLAGS = re.X | re.M

    # Basic keywords
    keyword_rules = [Rule(token=tok[0],
                          next_state=STATE_KEEP,
                          regexp=lexer.group(val))
                     for val, tok in graphql_keywords.items()]

    common_rules = keyword_rules + [
        Rule(token='NL',
             next_state=STATE_KEEP,
             regexp=r'\r\n|\n|\r'),

        Rule(token='WS',
             next_state=STATE_KEEP,
             regexp=r'[ \t]+'),

        Rule(token='COMMA',
             next_state=STATE_KEEP,
             regexp=r','),

        Rule(token='COMMENT',
             next_state=STATE_KEEP,
             regexp=r'\#[^\n]*$'),

        Rule(token='LPAREN',
             next_state=STATE_KEEP,
             regexp=r'\('),

        Rule(token='RPAREN',
             next_state=STATE_KEEP,
             regexp=r'\)'),

        Rule(token='LSBRACKET',
             next_state=STATE_KEEP,
             regexp=r'\['),

        Rule(token='RSBRACKET',
             next_state=STATE_KEEP,
             regexp=r'\]'),

        Rule(token='LCBRACKET',
             next_state=STATE_KEEP,
             regexp=r'\{'),

        Rule(token='RCBRACKET',
             next_state=STATE_KEEP,
             regexp=r'\}'),

        Rule(token='BANG',
             next_state=STATE_KEEP,
             regexp=r'\!'),

        Rule(token='ELLIPSIS',
             next_state=STATE_KEEP,
             regexp=r'\.\.\.'),

        Rule(token='COLON',
             next_state=STATE_KEEP,
             regexp=r':'),

        Rule(token='EQUAL',
             next_state=STATE_KEEP,
             regexp=r'='),

        Rule(token='AT',
             next_state=STATE_KEEP,
             regexp=r'@'),

        Rule(token='INTEGER',
             next_state=STATE_KEEP,
             regexp=r'-?(?:0|[1-9][0-9]*)(?![eE.0-9])'),

        Rule(token='FLOAT',
             next_state=STATE_KEEP,
             regexp=r'''
                -?(0|[1-9][0-9]*)
                    (\.[0-9]+)?
                        ([eE][+-]?[0-9]+)?
                        (?![eE.0-9])  # must not be followed by a number
             '''),

        Rule(token='STRING',
             next_state=STATE_KEEP,
             regexp=r'''
                    (?:r)?" [^\n]*?
                    (?<!\\)"
             '''),

        Rule(token='IDENT',
             next_state=STATE_KEEP,
             regexp=r'[_A-Za-z][_0-9A-Za-z]*'),

        Rule(token='VAR',
             next_state=STATE_KEEP,
             regexp=r'\$[_0-9A-Za-z]+'),

        Rule(token='DOLLAR',
             next_state=STATE_KEEP,
             regexp=r'\$'),

    ]

    states = {
        STATE_BASE: list(common_rules),
    }

    def handle_error(self, txt):
        # check if this is unterminated string instead of a generic error
        if txt == '"':

            pos = re.compile(r'$', self.RE_FLAGS).search(self.inputstr,
                                                         self.start).start()
            pos += self.column - self.start
            raise UnterminatedStringError(
                'unterminated string token {position}',
                line=self.lineno, col=pos, filename=self.filename)

        super().handle_error(txt)
