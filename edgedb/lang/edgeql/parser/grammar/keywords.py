##
# Copyright (c) 2010-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing


keyword_types = range(1, 4)
UNRESERVED_KEYWORD, RESERVED_KEYWORD, TYPE_FUNC_NAME_KEYWORD = keyword_types

unreserved_keywords = [
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
    "desc",
    "event",
    "final",
    "first",
    "for",
    "from",
    "index",
    "inherit",
    "inheriting",
    "initial",
    "last",
    "link",
    "map",
    "migration",
    "of",
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
]

# NOTE: all operators are made into RESERVED keywords for reasons of style.
#
reserved_keywords = [
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
    "returning",
    "rollback",
    "select",
    "set",
    "singleton",
    "start",
    "true",
    "update",
    "union",
    "with",
]

edgeql_keywords = {k: (k.upper(), UNRESERVED_KEYWORD)
                   for k in unreserved_keywords}
edgeql_keywords.update({k: (k.upper(), RESERVED_KEYWORD)
                        for k in reserved_keywords})


by_type: typing.Dict[int, dict] = {typ: {} for typ in keyword_types}

for val, spec in edgeql_keywords.items():
    by_type[spec[1]][val] = spec[0]
