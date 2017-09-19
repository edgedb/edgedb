##
# Copyright (c) 2010-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import typing


# NOTE: Please update the pygments lexer with any keyword changes made here

keyword_types = range(1, 4)
UNRESERVED_KEYWORD, RESERVED_KEYWORD, TYPE_FUNC_NAME_KEYWORD = keyword_types

unreserved_keywords = frozenset([
    "abstract",
    "action",
    "after",
    "any",
    "array",
    "as",
    "asc",
    "atom",
    "attribute",
    "before",
    "by",
    "concept",
    "constraint",
    "database",
    "delegated",
    "desc",
    "event",
    "final",
    "first",
    "for",
    "from",
    "index",
    "initial",
    "last",
    "link",
    "map",
    "migration",
    "of",
    "on",
    "policy",
    "property",
    "required",
    "rename",
    "target",
    "then",
    "to",
    "transaction",
    "tuple",
    "value",
    "view",
])


# NOTE: all operators are made into RESERVED keywords for reasons of style.
reserved_keywords = frozenset([
    "__class__",
    "__subject__",
    "aggregate",
    "all",
    "alter",
    "and",
    "commit",
    "create",
    "delete",
    "distinct",
    "drop",
    "else",
    "empty",
    "exists",
    "extending",
    "false",
    "filter",
    "function",
    "get",
    "group",
    "if",
    "ilike",
    "in",
    "insert",
    "is",
    "like",
    "limit",
    "module",
    "not",
    "offset",
    "or",
    "order",
    "over",
    "partition",
    "rollback",
    "select",
    "self",
    "set",
    "singleton",
    "start",
    "true",
    "update",
    "union",
    "with",
])


def _check_keywords():
    duplicate_keywords = reserved_keywords & unreserved_keywords
    if duplicate_keywords:
        raise ValueError(
            f'The following EdgeQL keywords are defined as *both* '
            f'reserved and unreserved: {duplicate_keywords!r}')


_check_keywords()


_dunder_re = re.compile(r'(?i)^__[a-z]+__$')


def tok_name(keyword):
    '''Convert a literal keyword into a token name.'''
    if _dunder_re.match(keyword):
        return f'DUNDER{keyword[2:-2].upper()}'
    else:
        return keyword.upper()


edgeql_keywords = {k: (tok_name(k), UNRESERVED_KEYWORD)
                   for k in unreserved_keywords}
edgeql_keywords.update({k: (tok_name(k), RESERVED_KEYWORD)
                        for k in reserved_keywords})


by_type: typing.Dict[int, dict] = {typ: {} for typ in keyword_types}

for val, spec in edgeql_keywords.items():
    by_type[spec[1]][val] = spec[0]
