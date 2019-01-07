#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import typing

from edb.edgeql.parser.grammar import keywords as eql_keywords


# NOTE: Please update the pygments lexer with any keyword changes made here

keyword_types = range(1, 3)
UNRESERVED_KEYWORD, RESERVED_KEYWORD = keyword_types


unreserved_keywords = frozenset([
    "abstract",
    "all",
    "as",
    "attribute",
    "constraint",
    "default",
    "deferred",
    "delegated",
    "final",
    "from",
    "index",
    "inheritable",
    "inherited",
    "link",
    "multi",
    "of",
    "on",
    "property",
    "required",
    "restrict",
    "scalar",
    "single",
    "source",
    "target",
    "type",
    "view",
])


# We use the same reserved keywords in both eschema and EdgeQL
# to enforce consistency in naming.  E.g. if a type name is a reserved
# keyword in EdgeQL and needs to be quoted, the same should apply to
# eschema.
reserved_keywords = eql_keywords.reserved_keywords


def _check_keywords():
    ALLOWED_NEW_UNRESERVED = {'import'}

    invalid_unreserved_keywords = \
        eql_keywords.reserved_keywords.intersection(unreserved_keywords)
    if invalid_unreserved_keywords:
        raise ValueError(
            f'The following unreserved eschema keywords are *reserved* '
            f'in EdgeQL: {invalid_unreserved_keywords!r}')

    invalid_reserved_keywords = \
        eql_keywords.unreserved_keywords.intersection(reserved_keywords)
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
        reserved_keywords - eql_keywords.reserved_keywords
    if new_reserved_keywords:
        raise ValueError(
            f'The following reserved keywords are defined in eschema, '
            f'but not in EdgeQL: {new_reserved_keywords!r}')

    new_unreserved_keywords = (unreserved_keywords -
                               eql_keywords.unreserved_keywords -
                               ALLOWED_NEW_UNRESERVED)
    if new_unreserved_keywords:
        raise ValueError(
            f'The following unreserved keywords are defined in eschema, '
            f'but not in EdgeQL: {new_unreserved_keywords!r}')


_check_keywords()


edge_schema_keywords = {k: (eql_keywords.tok_name(k), UNRESERVED_KEYWORD)
                        for k in unreserved_keywords}
edge_schema_keywords.update({k: (eql_keywords.tok_name(k), RESERVED_KEYWORD)
                             for k in reserved_keywords})


by_type: typing.Dict[int, dict] = {typ: {} for typ in keyword_types}

for val, spec in edge_schema_keywords.items():
    by_type[spec[1]][val] = spec[0]
