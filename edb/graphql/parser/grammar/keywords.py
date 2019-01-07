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


import re
import typing

keyword_types = range(1, 3)
UNRESERVED_KEYWORD, RESERVED_KEYWORD = keyword_types

# NOTE: Please update the pygments lexer with any keyword changes made here

unreserved_keywords = frozenset([
    "__schema",
    "__type",
    "__typename",
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
