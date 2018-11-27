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


import re

from .parser.grammar import keywords


_re_ident = re.compile(r'''(?x)
    [^\W\d]\w*  # alphanumeric identifier
    |
    ([1-9]\d* | 0)  # purely integer identifier
''')


def escape_string(s):
    split = re.split(r"(\n|\\\\|\\')", s)

    if len(split) == 1:
        return s.replace(r"'", r"\'")

    return ''.join((r if i % 2 else r.replace(r"'", r"\'"))
                   for i, r in enumerate(split))


def quote_literal(string):
    return "'" + escape_string(string) + "'"


def dollar_quote_literal(text):
    quote = '$$'
    qq = 0

    while quote in text:
        if qq % 16 < 10:
            qq += 10 - qq % 16

        quote = '${:x}$'.format(qq)[::-1]
        qq += 1

    return quote + text + quote


def disambiguate_identifier(text, *, allow_reserved=False):
    reserved = keywords.by_type[keywords.RESERVED_KEYWORD]
    if (text.lower() != '__type__' and
            ((not allow_reserved and reserved.get(text.lower())) or
                not _re_ident.fullmatch(text))):
        return '`{}`'.format(text)
    else:
        return text
