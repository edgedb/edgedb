##
# Copyright (c) 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from metamagic.utils import lexer
from .keywords import pg_keywords


__all__ = ('PgSQLLexer',)



STATE_KEEP = 0
STATE_BASE = 1


re_exppart          = r"(?:[eE](?:[+\-])?[0-9]+)"
re_self             = r'[,()\[\].;:+\-*/%^<>=]'
re_opchars          = r"[~!@\#&|`?+\-*/%^<>=]"
re_opchars_pgsql    = r'[~!@\#&|`?]'
re_opchars_sql      = r'[+\-*/^%<>=]'
re_ident_start      = r"[A-Za-z\200-\377_%]"
re_ident_cont       = r"[A-Za-z\200-\377_0-9\$%]"


clean_string = re.compile(r"'(?:\s|\n)+'")

Rule = lexer.Rule

class PgSQLLexer(lexer.Lexer):

    start_state = STATE_BASE

    NL = frozenset('NL')
    MULTILINE_TOKENS = frozenset(('COMMENT', 'SCONST'))
    RE_FLAGS = re.X | re.M | re.I

    # Basic keywords
    keyword_rules = [Rule(token='KEYWORD',
                          next_state=STATE_KEEP,
                          regexp=lexer.group(*pg_keywords.keys()))]

    common_rules = keyword_rules + [
        Rule(token='WS',
             next_state=STATE_KEEP,
             regexp=r'[^\S\n]+'),

        Rule(token='NL',
             next_state=STATE_KEEP,
             regexp=r'\n'),

        Rule(token='COMMENT',
             next_state=STATE_KEEP,
             regexp=r'''
                    (?:/\*(?:.|\n)*?\*/)
                    | (?:--.*?$)
                '''),

        Rule(token='TYPECAST',
             next_state=STATE_KEEP,
             regexp=r'::'),

        # multichar ops (so 2+ chars)
        Rule(token='Op',
             next_state=STATE_KEEP,
             regexp=r'''
                # CAOSQL-specific multi-char ops
                {opchar_pg} (?:{opchar}(?!/\*|--))+
                |
                (?:{opchar}(?!/\*|--))+ {opchar_pg} (?:{opchar}(?!/\*|--))*
                |
                # SQL-only multi-char ops cannot end in + or -
                (?:{opchar_sql}(?!/\*|--))+[*/^%<>=]
             '''.format(opchar_pg=re_opchars_pgsql,
                        opchar=re_opchars,
                        opchar_sql=re_opchars_sql)),

        # PgSQL single char ops
        Rule(token='Op',
             next_state=STATE_KEEP,
             regexp=re_opchars_pgsql),

        # SQL ops
        Rule(token='self',
             next_state=STATE_KEEP,
             regexp=re_self),

        Rule(token='FCONST',
             next_state=STATE_KEEP,
             regexp=r"""
                    (?: \d+ (?:\.\d*)?
                        |
                        \. \d+
                    ) {exppart}
                """.format(exppart=re_exppart)),

        Rule(token='FCONST',
             next_state=STATE_KEEP,
             regexp=r'''
                (?: \d+\.(?!\.)\d*
                    |
                    \.\d+)
             '''),

        Rule(token='ICONST',
             next_state=STATE_KEEP,
             regexp=r'\d+'),

        Rule(token='BCONST',
             next_state=STATE_KEEP,
             regexp=r'''
                B'(?:
                    [01]
                    |
                    ''
                    |
                    ' (?:\s*\n\s*) '
                )*'
             '''),

        Rule(token='XCONST',
             next_state=STATE_KEEP,
             regexp=r'''
                X'(?:
                    [\da-fA-F]
                    |
                    ''
                    |
                    ' (?:\s*\n\s*) '
                )*'
             '''),

        # don't have extra checks for correct escaping inside
        Rule(token='SCONST',
             next_state=STATE_KEEP,
             regexp=r'''
                [nNeE]?
                '(?:
                    [^']
                    |
                    ''
                    |
                    ' (?:\s*\n\s*) '
                )*'
             '''),

        # dollar quoted strings
        Rule(token='DQCONST',
             next_state=STATE_KEEP,
             regexp=r'''
                \$(?P<dq> (?:{ident_start}{ident_cont}*)? )\$
                    .*?
                \$(?P=dq)\$
                '''.format(ident_start=re_ident_start,
                           ident_cont=re_ident_cont)),

        # specifying custom escape character
        Rule(token='UESCAPE',
             next_state=STATE_KEEP,
             regexp=r"""UESCAPE\s+'[^a-fA-F\d\s+'"]'"""),

        # quoted identifier
        Rule(token='QIDENT',
             next_state=STATE_KEEP,
             regexp=r'''
                    (?:U&)?
                    "(?:
                        [^"]
                        |
                        ""
                    )+"
                '''.format(ident_start=re_ident_start,
                           ident_cont=re_ident_cont)),

        Rule(token='PARAM',
             next_state=STATE_KEEP,
             regexp=r'\$\d+'),

        Rule(token='IDENT',
             next_state=STATE_KEEP,
             regexp=r'''
                    {ident_start}{ident_cont}*
                '''.format(ident_start=re_ident_start,
                           ident_cont=re_ident_cont)),
    ]

    states = {
        STATE_BASE:
            common_rules,
    }

    def token_from_text(self, rule_token, txt):
        tok = super().token_from_text(rule_token, txt)

        if rule_token == 'self':
            tok.attrs['type'] = txt

        elif rule_token == 'IDENT':
            tok.value = txt.lower()

        elif rule_token == 'KEYWORD':
            # process keywords here since having separate rules for them creates
            # > 100 re groups
            tok.value = txt.lower()
            tok.attrs['type'] = pg_keywords[tok.value][0]

        elif rule_token in ('SCONST', 'BCONST', 'XCONST'):
            txt = txt[:-1].split("'", 1)[1]
            txt = clean_string.sub('', txt.replace("''", "'"))
            tok.value = txt

        elif rule_token == 'PARAM':
            tok.value = txt[1:]

        elif rule_token == 'QIDENT':
            tok.attrs['type'] = 'IDENT'
            tok.value = txt[:-1].split('"', 1)[1]

        elif rule_token == 'DQCONST':
            tok.attrs['type'] = 'SCONST'
            txt = txt.rsplit("$", 2)[2]
            txt = txt.split("$", 2)[2]
            tok.value = txt

        return tok
