#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

import re
from typing import *  # NoQA


keyword_types = range(1, 4)
UNRESERVED_KEYWORD, RESERVED_KEYWORD, TYPE_FUNC_NAME_KEYWORD = keyword_types

unreserved_keywords = frozenset([
    "abstract",
    "after",
    "alias",
    "allow",
    "all",
    "annotation",
    "as",
    "asc",
    "assignment",
    "before",
    "by",
    "cardinality",
    "cast",
    "config",
    "constraint",
    "database",
    "ddl",
    "default",
    "deferrable",
    "deferred",
    "delegated",
    "desc",
    "explicit",
    "expression",
    "final",
    "first",
    "from",
    "implicit",
    "index",
    "infix",
    "inheritable",
    "into",
    "isolation",
    "last",
    "link",
    "migration",
    "multi",
    "named",
    "object",
    "of",
    "on",
    "only",
    "operator",
    "overloaded",
    "postfix",
    "prefix",
    "property",
    "read",
    "rename",
    "required",
    "repeatable",
    "restrict",
    "role",
    "savepoint",
    "scalar",
    "schema",
    "sdl",
    "serializable",
    "session",
    "single",
    "source",
    "superuser",
    "system",
    "target",
    "ternary",
    "text",
    "then",
    "to",
    "transaction",
    "type",
    "using",
    "verbose",
    "view",
    "write",
])


future_reserved_keywords = frozenset([
    "analyze",
    "anyarray",
    "begin",
    "case",
    "check",
    "deallocate",
    "discard",
    "do",
    "end",
    "execute",
    "explain",
    "fetch",
    "get",
    "global",
    "grant",
    "import",
    "listen",
    "load",
    "lock",
    "match",
    "move",
    "notify",
    "prepare",
    "partition",
    "policy",
    "raise",
    "refresh",
    "reindex",
    "revoke",
    "over",
    "when",
    "window",
])


reserved_keywords = future_reserved_keywords | frozenset([
    "__source__",
    "__subject__",
    "__type__",
    "alter",
    "and",
    "anytuple",
    "anytype",
    "commit",
    "configure",
    "create",
    "declare",
    "delete",
    "describe",
    "detached",
    "distinct",
    "drop",
    "else",
    "empty",
    "exists",
    "extending",
    "false",
    "filter",
    "for",
    "function",
    "group",
    "if",
    "ilike",
    "in",
    "insert",
    "introspect",
    "is",
    "like",
    "limit",
    "module",
    "not",
    "offset",
    "optional",
    "or",
    "order",
    "release",
    "reset",
    "rollback",
    "select",
    "set",
    "start",
    "true",
    "typeof",
    "update",
    "union",
    "variadic",
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


by_type: Dict[int, dict] = {typ: {} for typ in keyword_types}

for val, spec in edgeql_keywords.items():
    by_type[spec[1]][val] = spec[0]
