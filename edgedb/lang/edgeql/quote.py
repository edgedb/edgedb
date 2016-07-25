##
# Copyright (c) 2013, 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


def quote_literal(text):
    return "'" + text.replace("'", "''") + "'"


def dollar_quote_literal(text):
    quote = '$$'
    qq = 0

    while quote in text:
        if qq % 16 < 10:
            qq += 10 - qq % 16

        quote = '${:x}$'.format(qq)[::-1]
        qq += 1

    return quote + text + quote
