##
# Copyright (c) 2013-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from .parser.grammar import keywords


_re_ident = re.compile(r'''(?x)
    [^\W\d]\w*  # alphanumeric identifier
    |
    ([1-9]\d* | 0)  # purely integer identifier
''')


def quote_literal(text):
    return "'" + text.replace(R'\"', R'\\"').replace("'", R"\'") + "'"


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
    if (text.lower() != '__type__' and
        (keywords.by_type[keywords.RESERVED_KEYWORD].get(text.lower()) or
         not _re_ident.fullmatch(text))):
        return '`{}`'.format(text)
    else:
        return text
