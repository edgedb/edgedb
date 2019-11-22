#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2013-present MagicStack Inc. and the EdgeDB authors.
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

from .parser.grammar import keywords


_re_ident = re.compile(r'''(?x)
    [^\W\d]\w*  # alphanumeric identifier
    |
    ([1-9]\d* | 0)  # purely integer identifier
''')


def escape_string(s: str) -> str:
    split = re.split(r"(\n|\\\\|\\')", s)

    if len(split) == 1:
        return s.replace(r"'", r"\'")

    return ''.join((r if i % 2 else r.replace(r"'", r"\'"))
                   for i, r in enumerate(split))


def quote_literal(string: str) -> str:
    return "'" + escape_string(string) + "'"


def dollar_quote_literal(text: str) -> str:
    quote = '$$'
    qq = 0

    while quote in text:
        if qq % 16 < 10:
            qq += 10 - qq % 16

        quote = '${:x}$'.format(qq)[::-1]
        qq += 1

    return quote + text + quote


def needs_quoting(string: str, allow_reserved: bool) -> bool:
    if not string or string.startswith('@') or '::' in string:
        # some strings are illegal as identifiers and as such don't
        # require quoting
        return False

    isalnum = _re_ident.fullmatch(string)

    string = string.lower()

    is_reserved = (
        string != '__type__'
        and string in keywords.by_type[keywords.RESERVED_KEYWORD]
    )

    return (
        not isalnum
        or (not allow_reserved and is_reserved)
    )


def _quote_ident(string: str) -> str:
    return '`' + string.replace('`', '``') + '`'


def quote_ident(string: str, *,
                force: bool = False, allow_reserved: bool = False) -> str:
    if force or needs_quoting(string, allow_reserved):
        return _quote_ident(string)
    else:
        return string
