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


from edb.edgeql.parser.grammar import lexer


def split_edgeql(script: str, *, script_mode=False):
    lex = lexer.EdgeQLLexer(strip_whitespace=False)
    lex.setinputstr(script)

    out = []
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

    if buffer and out:
        rem = ''.join(buffer)
        if not rem.strip():
            buffer.clear()
            out[-1] += rem

    if buffer:
        if not script_mode:
            return None
        out.append(''.join(buffer))

    if script_mode:
        out = [line.strip() for line in out]
        out = [line for line in out if line and line != ';']

    if not out:
        return None

    return out
