##
# Copyright (c) 2014, 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from edgedb.lang.common import lexer

from .keywords import edgeql_keywords


__all__ = ('EdgeQLLexer',)


STATE_KEEP = 0
STATE_BASE = 1


re_exppart = r"(?:[eE](?:[+\-])?[0-9]+)"
re_self = r'[,()\[\].@;:+\-*/%^<>=]'
re_opchars = r'[~!\#&|`?+\-*/^<>=]'
re_not_opchars = r'[^~!\#&|`?+\-*/^<>=]'
re_opchars_edgeql = r'[~!\#&|`?]'
re_opchars_sql = r'[+\-*/^<>=]'
re_ident_start = r"[A-Za-z\200-\377_%]"
re_ident_cont = r"[A-Za-z\200-\377_0-9\$%]"
re_edgeql_special = r'[\{\}$]'
re_dquote = r'\$([A-Za-z\200-\377_][0-9]*)*\$'


clean_string = re.compile(r"'(?:\s|\n)+'")
string_quote = re.compile(re_dquote)

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

        Rule(token='TURNSTILE',
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
                # no multichar operator can be composed exclusively of
                # ">" or "<", to avoid ambiguity of parsing nested "< ... >"
                (?![<>]+(?:{notopchar} | $)) (?:
                    # EdgeQL-specific multi-char ops
                    {opchar_edgedb} {opchar}+
                    |
                    {opchar}+ {opchar_edgedb} {opchar}*
                    |
                    # SQL-only multi-char ops cannot end in + or -
                    {opchar_sql}+[*/^<>=]
                )
             '''.format(opchar_edgedb=re_opchars_edgeql,
                        opchar=re_opchars,
                        notopchar=re_not_opchars,
                        opchar_sql=re_opchars_sql)),

        # EdgeQL/PgSQL single char ops
        Rule(token='OP',
             next_state=STATE_KEEP,
             regexp=re_opchars_edgeql),

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
                (?P<Q>
                    # capture the opening quote in group Q
                    (
                        ' | " |
                        {dollar_quote}
                    )
                )
                (?:
                    (\\['"] | \n | .)*?
                )
                (?P=Q)      # match closing quote type with whatever is in Q
             '''.format(dollar_quote=re_dquote)),

        Rule(token='IDENT',
             next_state=STATE_KEEP,
             regexp=r'''
                    {ident_start}{ident_cont}*
                '''.format(ident_start=re_ident_start,
                           ident_cont=re_ident_cont)),

        Rule(token='self',
             next_state=STATE_KEEP,
             regexp=re_edgeql_special),
    ]

    states = {
        STATE_BASE:
            common_rules,
    }

    def token_from_text(self, rule_token, txt):
        tok = super().token_from_text(rule_token, txt)

        if rule_token == 'self':
            tok.attrs['type'] = txt

        elif rule_token == 'SCONST':
            # the process of string normalization is slightly different for
            # regular '-quoted strings and $$-quoted ones
            #
            if txt[0] in ("'", '"'):
                tok.value = clean_string.sub('', txt[1:-1].replace(
                    R"\'", "'").replace(R'\"', '"'))
            else:
                # Because of implicit string concatenation there may
                # be more than one pair of dollar quotes in the txt.
                # We want to grab every other chunk from splitting the
                # txt with the quote.
                #
                quote = string_quote.match(txt).group(0)
                tok.value = ''.join((
                    part for n, part in enumerate(txt.split(quote))
                    if n % 2 == 1))

        return tok

    def lex(self):
        buffer = []

        for tok in super().lex():
            tok_type = tok.attrs['type']

            if tok_type in {'WS', 'NL', 'COMMENT'}:
                # Strip out whitespace and comments
                continue
            elif tok_type == 'LINK':
                # Buffer in case this is LINK PROPERTY
                if not buffer:
                    buffer.append(tok)
                else:
                    yield tok
            elif tok_type == 'PROPERTY':
                prev_token = buffer[-1] if buffer else None
                if prev_token and prev_token.attrs['type'] == 'LINK':
                    tok = tok.__class__(prev_token.value + ' ' + tok.value)
                    tok.attrs = prev_token.attrs.copy()
                    tok.attrs['type'] = 'LINKPROPERTY'
                    buffer.pop()
                yield tok
            else:
                if buffer:
                    yield from iter(buffer)
                    buffer[:] = []
                yield tok

    def lex_highlight(self):
        return super().lex()
