##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import typing

keyword_types = range(1, 3)
UNRESERVED_KEYWORD, RESERVED_KEYWORD = keyword_types

# NOTE: Please update the pygments lexer with any keyword changes made here

unreserved_keywords = frozenset([
    "false",
    "fragment",
    "mutation",
    "null",
    "on",
    "query",
    "subscription",
    "true",
])


# NOTE: all operators are made into RESERVED keywords for reasons of style.
reserved_keywords = frozenset()


def _check_keywords():
    duplicate_keywords = reserved_keywords & unreserved_keywords
    if duplicate_keywords:
        raise ValueError(
            f'The following GraphQL keywords are defined as *both* '
            f'reserved and unreserved: {duplicate_keywords!r}')


_check_keywords()


_dunder_re = re.compile(r'(?i)^__[a-z]+$')


def tok_name(keyword):
    '''Convert a literal keyword into a token name.'''
    if _dunder_re.match(keyword):
        return f'{keyword[2:].upper()}'
    else:
        return keyword.upper()


graphql_keywords = {k: (tok_name(k), UNRESERVED_KEYWORD)
                    for k in unreserved_keywords}
graphql_keywords.update({k: (tok_name(k), RESERVED_KEYWORD)
                         for k in reserved_keywords})


by_type: typing.Dict[int, dict] = {typ: {} for typ in keyword_types}

for val, spec in graphql_keywords.items():
    by_type[spec[1]][val] = spec[0]
