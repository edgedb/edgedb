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


from edgedb.lang.common import parsing
from edgedb.lang.schema.error import SchemaSyntaxError
from .grammar import lexer


class EdgeSchemaParser(parsing.Parser):
    def get_exception(self, native_err, context, token=None):
        if isinstance(native_err, SchemaSyntaxError):
            return native_err

        if token and token.type == 'BADLINECONT':
            context.start.column += 1
            return SchemaSyntaxError(
                'Unexpected character after line continuation '
                'character',
                context=context)

        # if the error is about unexpected <$> token, convert the text to be
        # referencing <NL> token
        if native_err.args[0] == 'Unexpected token: <$>':
            return SchemaSyntaxError('Unexpected token: <NL>', context=context)
        return SchemaSyntaxError(native_err.args[0], context=context)

    def get_parser_spec_module(self):
        from .grammar import declarations
        return declarations

    def get_lexer(self):
        return lexer.EdgeSchemaLexer()

    def process_lex_token(self, mod, tok):
        if tok.type in {'NEWLINE', 'WS', 'COMMENT'}:
            return None
        else:
            return super().process_lex_token(mod, tok)
