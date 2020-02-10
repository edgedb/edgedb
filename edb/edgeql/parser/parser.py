#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

from edb import errors
from edb.common import debug, parsing

from .grammar import rust_lexer


class EdgeQLParserBase(parsing.Parser):
    def get_debug(self):
        return debug.flags.edgeql_parser

    def get_exception(self, native_err, context, token=None):
        msg = native_err.args[0]

        if isinstance(native_err, errors.EdgeQLSyntaxError):
            return native_err
        else:
            if msg.startswith('Unexpected token: '):
                token = token or getattr(native_err, 'token', None)

                if not token or token.kind() == 'EOF':
                    msg = 'Unexpected end of line'
                elif hasattr(token, 'val'):
                    msg = f'Unexpected {token.val!r}'
                elif token.kind() == 'NL':
                    msg = 'Unexpected end of line'
                else:
                    msg = f'Unexpected {token.text()!r}'

        return errors.EdgeQLSyntaxError(msg, context=context, token=token)

    def get_lexer(self):
        return rust_lexer.EdgeQLLexer()


class EdgeQLExpressionParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import single
        return single


class EdgeQLBlockParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import block
        return block


class EdgeSDLParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import sdldocument
        return sdldocument
