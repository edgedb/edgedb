##
# Copyright (c) 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from metamagic.utils import lexer
from metamagic.utils.datastructures import xvalue

from .errors import WKTSyntaxError


__all__ = ('WKTLexer',)



STATE_KEEP = 0
STATE_BASE = 1

Rule = lexer.Rule

def make_tag(value):
    has_m = has_z = False
    if value[-1] == 'M':
        has_m = True
        value = value[:-1]
    if value[-1] == 'Z':
        has_z = True
        value = value[:-1]
    return xvalue(value, has_z=has_z, has_m=has_m)

class WKTLexer(lexer.Lexer):

    start_state = STATE_BASE

    NL = frozenset('NL')

    # Basic keywords
    common_rules = [
        Rule(token='WS',
             next_state=STATE_KEEP,
             regexp=r'[^\S\n]+'),

        Rule(token='NL',
             next_state=STATE_KEEP,
             regexp=r'\n'),

        Rule(token='LPARENTHESIS',
             next_state=STATE_KEEP,
             regexp=r'\('),

        Rule(token='RPARENTHESIS',
             next_state=STATE_KEEP,
             regexp=r'\)'),

        Rule(token='COMMA',
             next_state=STATE_KEEP,
             regexp=r','),

        Rule(token='SEMICOLON',
             next_state=STATE_KEEP,
             regexp=r';'),

        Rule(token='FCONST',
             next_state=STATE_KEEP,
             regexp=r'''
                -? (?: \d+\.\d*
                       |
                       \.\d+
                   )
             '''),

        Rule(token='FCONST',
             next_state=STATE_KEEP,
             regexp=r'-?\d+'),

        Rule(token='EMPTY',
             next_state=STATE_KEEP,
             regexp=r'EMPTY'),

        Rule(token='SRID',
             next_state=STATE_KEEP,
             regexp=r'SRID=-?[0-9]+'),

        Rule(token='DIM',
             next_state=STATE_KEEP,
             regexp=r'''
                (?:
                   POINT | LINESTRING | POLYGON | MULTIPOINT | MULTILINESTRING |
                   MULTIPOLYGON | GEOMETRYCOLLECTION | CIRCULARSTRING |
                   COMPOUNDCURVE | CURVEPOLYGON | MULTICURVE | MULTISURFACE |
                   CURVE | SURFACE | POLYHEDRALSURFACE | TIN | TRIANGLE
                )
                Z?M?
             '''),
    ]

    states = {
        STATE_BASE:
            common_rules,
    }

    def token_from_text(self, rule_token, txt):
        tok = super().token_from_text(rule_token, txt)

        if rule_token == 'SRID':
            tok.value = int(txt[5:])

        elif rule_token == 'FCONST':
            tok.value = float(txt)

        elif rule_token == 'DIM':
            tok.value = make_tag(txt)
            tok.attrs['type'] = tok.value.value

        return tok


    def handle_error(self, txt):
        raise WKTSyntaxError(token=txt, context=self.context())
