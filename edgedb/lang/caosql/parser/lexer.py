##
# Copyright (c) 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from metamagic.utils import lexer
from .keywords import caosql_keywords


__all__ = ('CaosQLLexer',)



STATE_KEEP = 0
STATE_BASE = 1


re_exppart          = r"(?:[eE](?:[+\-])?[0-9]+)"
re_self             = r'[,()\[\].@;:+\-*/%^<>=]'
re_opchars          = r'[~!\#&|`?+\-*/^<>=]'
re_opchars_caosql   = r'[~!\#&|`?]'
re_opchars_sql      = r'[+\-*/^<>=]'
re_ident_start      = r"[A-Za-z\200-\377_%]"
re_ident_cont       = r"[A-Za-z\200-\377_0-9\$%]"
re_caosql_special   = r'[\{\}$]'


clean_string = re.compile(r"'(?:\s|\n)+'")

Rule = lexer.Rule

class CaosQLLexer(lexer.Lexer):

    start_state = STATE_BASE

    NL = frozenset('NL')
    MULTILINE_TOKENS = frozenset(('COMMENT', 'SCONST'))
    RE_FLAGS = re.X | re.M | re.I

    # Basic keywords
    keyword_rules = [Rule(token=tok[0],
                          next_state=STATE_KEEP,
                          regexp=lexer.group(val))
                                 for val, tok in caosql_keywords.items()]

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

        Rule(token='COLONEQUALS',
             next_state=STATE_KEEP,
             regexp=r':='),

        Rule(token='<>',
             next_state=STATE_KEEP,
             regexp=r'<>'),

        Rule(token='OP',
             next_state=STATE_KEEP,
             regexp=r'@@!'),

        Rule(token='OP',
             next_state=STATE_KEEP,
             regexp=r'@@'),

        Rule(token='**',
             next_state=STATE_KEEP,
             regexp=r'\*\*'),

        Rule(token='::',
             next_state=STATE_KEEP,
             regexp=r'::'),

        Rule(token='TYPEINDIRECTION',
             next_state=STATE_KEEP,
             regexp=r'__type__'),

        # multichar ops (so 2+ chars)
        Rule(token='OP',
             next_state=STATE_KEEP,
             regexp=r'''
                # CAOSQL-specific multi-char ops
                {opchar_caos} (?:{opchar}(?!/\*|--))+
                |
                (?:{opchar}(?!/\*|--))+ {opchar_caos} (?:{opchar}(?!/\*|--))*
                |
                # SQL-only multi-char ops cannot end in + or -
                (?:{opchar_sql}(?!/\*|--))+[*/^<>=]
             '''.format(opchar_caos=re_opchars_caosql,
                        opchar=re_opchars,
                        opchar_sql=re_opchars_sql)),

        # CAOS-/PgSQL single char ops
        Rule(token='OP',
             next_state=STATE_KEEP,
             regexp=re_opchars_caosql),

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

        Rule(token='SCONST',
             next_state=STATE_KEEP,
             regexp=r'''
                '(?:
                    [^']
                    |
                    ' (?:\s|\n)* '
                )*'
             '''),

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
                '''),

        Rule(token='IDENT',
             next_state=STATE_KEEP,
             regexp=r'''
                    {ident_start}{ident_cont}*
                '''.format(ident_start=re_ident_start,
                           ident_cont=re_ident_cont)),

        Rule(token='self',
             next_state=STATE_KEEP,
             regexp=re_caosql_special),
    ]

    states = {
        STATE_BASE:
            common_rules,
    }

    def token_from_text(self, rule_token, txt):
        tok = super().token_from_text(rule_token, txt)

        if rule_token == 'self':
            tok.attrs['type'] = txt

        elif rule_token == 'QIDENT':
            tok.attrs['type'] = 'IDENT'
            tok.value = txt[:-1].split('"', 1)[1]

        elif rule_token == 'SCONST':
            tok.value = clean_string.sub('', txt[1:-1].replace("''", "'"))

        return tok
