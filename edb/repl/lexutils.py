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


import functools

from edb.edgeql.parser.grammar import lexer


@functools.lru_cache(100)
def split_edgeql(script: str, *, script_mode=True):
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
