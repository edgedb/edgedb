##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from .parser.grammar import keywords


_re_ident = re.compile(r'[^\W\d]\w*')


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
    if (keywords.by_type[keywords.RESERVED_KEYWORD].get(text.lower()) or
            not _re_ident.fullmatch(text)):
        return '`{}`'.format(text)
    else:
        return text
