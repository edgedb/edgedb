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


from edb.common import debug, parsing
from .error import PgSQLParserError

from . import lexer


class PgSQLParser(parsing.Parser):
    def get_lexer(self):
        return lexer.PgSQLLexer()

    def process_lex_token(self, mod, tok):
        tok_type = tok.type
        if tok_type in ('WS', 'NL', 'COMMENT'):
            return None

        return super().process_lex_token(mod, tok)

    def get_parser_spec_module(self):
        from . import pgsql
        return pgsql

    def get_debug(self):
        return debug.flags.pgsql_parser

    def get_exception(self, native_err, context, token=None):
        return PgSQLParserError(native_err.args[0], context=context)
