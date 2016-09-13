##
# Copyright (c) 2013, 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from .parser.grammar import keywords


_re_ident = re.compile(r'[A-Za-z\200-\377_%][A-Za-z\200-\377_0-9\$%]*')


def quote_literal(text):
    return "'" + text.replace("'", R"\'") + "'"


def dollar_quote_literal(text):
    quote = '$$'
    qq = 0

    while quote in text:
        if qq % 16 < 10:
            qq += 10 - qq % 16

        quote = '${:x}$'.format(qq)[::-1]
        qq += 1

    return quote + text + quote


def disambiguate_identifier(text):
    if (keywords.edgeql_keywords.get(text.lower())
            or not _re_ident.fullmatch(text)):
        return '`{}`'.format(text)
    else:
        return text
