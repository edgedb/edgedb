##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from edgedb.lang.common import lexer

from .keywords import graphql_keywords


__all__ = ('GraphQLLexer')


STATE_KEEP = 0
STATE_BASE = 1


Rule = lexer.Rule


class GraphQLLexer(lexer.Lexer):

    start_state = STATE_BASE

    NL = 'NL'
    RE_FLAGS = re.X | re.M | re.I
    asbytes = True

    # Basic keywords
    keyword_rules = [Rule(token=tok[0],
                          next_state=STATE_KEEP,
                          regexp=lexer.group(val, asbytes=True))
                     for val, tok in graphql_keywords.items()]

    common_rules = keyword_rules + [
        Rule(token='UBOM',
             next_state=STATE_KEEP,
             regexp=rb'(\xef\xbb\xbf)|(\xfe\xff)|(\xff\xfe)'),

        # Both '\r\n' an d '\n\r' could be used to indicate a single
        # newline, so we want to account for them correctly to keep
        # track of line count.
        #
        Rule(token='NL',
             next_state=STATE_KEEP,
             regexp=rb'\r\n|\n\r|\n|\r'),

        Rule(token='WS',
             next_state=STATE_KEEP,
             regexp=rb'[ \t]+'),

        Rule(token='COMMA',
             next_state=STATE_KEEP,
             regexp=rb','),

        Rule(token='COMMENT',
             next_state=STATE_KEEP,
             regexp=rb'\#[^\n]*$'),

        Rule(token='LPAREN',
             next_state=STATE_KEEP,
             regexp=rb'\('),

        Rule(token='RPAREN',
             next_state=STATE_KEEP,
             regexp=rb'\)'),

        Rule(token='LSBRACKET',
             next_state=STATE_KEEP,
             regexp=rb'\['),

        Rule(token='RSBRACKET',
             next_state=STATE_KEEP,
             regexp=rb'\]'),

        Rule(token='LCBRACKET',
             next_state=STATE_KEEP,
             regexp=rb'\{'),

        Rule(token='RCBRACKET',
             next_state=STATE_KEEP,
             regexp=rb'\}'),

        Rule(token='BANG',
             next_state=STATE_KEEP,
             regexp=rb'\!'),

        Rule(token='ELLIPSIS',
             next_state=STATE_KEEP,
             regexp=rb'\.\.\.'),

        Rule(token='COLON',
             next_state=STATE_KEEP,
             regexp=rb':'),

        Rule(token='EQUAL',
             next_state=STATE_KEEP,
             regexp=rb'='),

        Rule(token='AT',
             next_state=STATE_KEEP,
             regexp=rb'@'),

        Rule(token='INTEGER',
             next_state=STATE_KEEP,
             regexp=rb'-?(?:0|[1-9][0-9]*)(?![eE.0-9])'),

        Rule(token='FLOAT',
             next_state=STATE_KEEP,
             regexp=rb'-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?'),

        Rule(token='STRING',
             next_state=STATE_KEEP,
             regexp=rb'''
                    (?:r)?"
                        (\\["/bfnrt\\] |
                         \\u[0-9A-Fa-f]{4} |
                         [^\\\n\f\v\b]
                         )*?
                    "
                '''),

        Rule(token='IDENT',
             next_state=STATE_KEEP,
             regexp=rb'[_A-Za-z][_0-9A-Za-z]*'),

        Rule(token='VAR',
             next_state=STATE_KEEP,
             regexp=rb'\$[_0-9A-Za-z]+'),

        Rule(token='DOLLAR',
             next_state=STATE_KEEP,
             regexp=rb'\$'),

    ]

    states = {
        STATE_BASE: list(common_rules),
    }
