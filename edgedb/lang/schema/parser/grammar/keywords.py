##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang import edgeql


# NOTE: Please update the pygments lexer with any keyword changes made here

keyword_types = range(1, 3)
UNRESERVED_KEYWORD, RESERVED_KEYWORD = keyword_types


unreserved_keywords = frozenset([
    "abstract",
    "action",
    "as",
    "attribute",
    "constraint",
    "delegated",
    "event",
    "final",
    "from",
    "import",
    "index",
    "initial",
    "link",
    "of",
    "on",
    "required",
    "scalar",
    "type",
    "value",
    "view",
])


# We use the same reserved keywords in both eschema and EdgeQL
# to enforce consistency in naming.  E.g. if a type name is a reserved
# keyword in EdgeQL and needs to be quoted, the same should apply to
# eschema.
reserved_keywords = edgeql.keywords.reserved_keywords


def _check_keywords():
    # TODO: Fix linkproperty;
    ALLOWED_NEW_UNRESERVED = {'import'}

    invalid_unreserved_keywords = \
        edgeql.keywords.reserved_keywords.intersection(unreserved_keywords)
    if invalid_unreserved_keywords:
        raise ValueError(
            f'The following unreserved eschema keywords are *reserved* '
            f'in EdgeQL: {invalid_unreserved_keywords!r}')

    invalid_reserved_keywords = \
        edgeql.keywords.unreserved_keywords.intersection(reserved_keywords)
    if invalid_reserved_keywords:
        raise ValueError(
            f'The following reserved eschema keywords are *unreserved* '
            f'in EdgeQL: {invalid_reserved_keywords!r}')

    duplicate_keywords = \
        reserved_keywords.intersection(unreserved_keywords)
    if duplicate_keywords:
        raise ValueError(
            f'The following eschema keywords are defined as *both* '
            f'reserved and unreserved: {duplicate_keywords!r}')

    new_reserved_keywords = \
        reserved_keywords - edgeql.keywords.reserved_keywords
    if new_reserved_keywords:
        raise ValueError(
            f'The following reserved keywords are defined in eschema, '
            f'but not in EdgeQL: {new_reserved_keywords!r}')

    new_unreserved_keywords = (unreserved_keywords -
                               edgeql.keywords.unreserved_keywords -
                               ALLOWED_NEW_UNRESERVED)
    if new_unreserved_keywords:
        raise ValueError(
            f'The following unreserved keywords are defined in eschema, '
            f'but not in EdgeQL: {new_unreserved_keywords!r}')


_check_keywords()


edge_schema_keywords = {k: (edgeql.keywords.tok_name(k), UNRESERVED_KEYWORD)
                        for k in unreserved_keywords}
edge_schema_keywords.update({k: (edgeql.keywords.tok_name(k), RESERVED_KEYWORD)
                             for k in reserved_keywords})


by_type: typing.Dict[int, dict] = {typ: {} for typ in keyword_types}

for val, spec in edge_schema_keywords.items():
    by_type[spec[1]][val] = spec[0]
