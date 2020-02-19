#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import functools
import re
from typing import *

from edb.edgeql import quote as eql_quote
from edb.edgeql.parser.grammar import lexer


class BigInt(int):
    pass


@functools.lru_cache(100)
def split_edgeql(
    script: str, *,
    script_mode: bool = True
) -> Tuple[List[str], Optional[str]]:
    '''\
    Split the input string into a list of possible EdgeQL statements.

    No parsing is done, so the statements may be invalid. Lexing
    exceptions will not be raised at this stage.

    The return value is a 2-tuple (`out`, `incomplete`). The `out`
    value is the list of all statements that were detected based on
    the statement separator `;`. The `incomplete` value is None when
    *script_mode* is False. When *script_mode* is True, the
    `incomplete` value may contain a string representing the trailing
    incomplete statement or None if no trailing incomplete statement
    is detected.
    '''

    lex = lexer.EdgeQLLexer(strip_whitespace=False, raise_lexerror=False)
    lex.setinputstr(script)

    out = []
    incomplete = None
    buffer = []
    brace_level = 0
    for tok in lex.lex():
        if tok.type == 'COMMENT':
            # skip the comments completely
            continue

        buffer.append(tok.text)

        if tok.type == '{':
            brace_level += 1
        elif tok.type == '}':
            brace_level -= 1
            if brace_level < 0:
                brace_level = 0
        elif tok.type == ';':
            if brace_level == 0:
                out.append(''.join(buffer))
                buffer.clear()

    # If we still have stuff in the buffer, we need to process it as a
    # potential statement.
    if buffer:
        rem = ''.join(buffer)

        # This statement is incomplete if there was something in the
        # buffer or if there's nothing in the output and the
        # script_mode is True.
        if (not out or rem.strip()) and not script_mode:
            incomplete = rem

        elif rem:
            out.append(rem.strip())

    # Clean up the output by stripping leading and trailing whitespace
    # and extra semicolons.
    out = [line.strip() for line in out]
    out = [line for line in out if line not in {'', ';'}]

    return out, incomplete


def _strip_quote(part: str) -> str:
    if (part != '```'
            and len(part) > 2
            and part.startswith('`') and part.endswith('`')):
        return part[1:-1].replace('``', '`')
    else:
        return part


def normalize_name(name: str) -> str:
    r'''Normalize plain text and backtick-quoted names into valid identifiers.

    The \d command takes a name which is normally expected to be plain
    text even if it contains special symbols that require quoting in
    EdgeQL. However, it is possible for the user to submit a name that
    is properly quoted, too. This function normalizes all these
    options into a string representing valid EdgeQL identifier.
    '''

    # un-backtick-quote the input
    name = ''.join(_strip_quote(part)
                   for part in re.split(r'(`(?:[^`]|``)+`)', name))

    # normalize input by backtick-quoting when necessary
    name = '::'.join(eql_quote.quote_ident(ident)
                     for ident in name.split('::', 1))

    if (not name
            or name.startswith('::') or name.endswith('::')
            or name.startswith('@') or name.endswith('@')):
        # name is illegal and cannot be normalized
        return ''

    return name


def get_filter_based_on_pattern(
    pattern: str,
    fields: Sequence[str] = ('.name',),
    flag: str = '',
    *,
    filter_and: str = '',
    filter_or: str = '',
) -> Tuple[str, Dict[str, str]]:

    # TODO: This is quite ugly.
    # We will re-implement this as soon as we have query builder APIs.

    if flag:
        flag = f'(?{flag})'

    qkw: Dict[str, str] = {}
    filter_cond = ''

    if pattern and fields:
        # the pattern is the same for all fields, so we only use one
        # variable
        qkw = {'re_filter': flag + pattern}
        filters = []
        for field in fields:
            filters.append(f're_test(<str>$re_filter, {field})')

        filter_cond = f'({" OR ".join(filters)})'

    filter_clause = ''

    if filter_cond:
        filter_clause += filter_cond

    if filter_and:
        if filter_clause:
            filter_clause += ' AND '
        filter_clause += filter_and

    if filter_or:
        if filter_clause:
            filter_clause += ' OR '
        filter_clause += filter_or

    if filter_clause:
        filter_clause = 'FILTER ' + filter_clause
        return filter_clause, qkw

    return '', qkw
