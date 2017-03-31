##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import parsing
from edgedb.lang.schema.error import SchemaSyntaxError
from .grammar import lexer


class EdgeSchemaParser(parsing.Parser):
    def get_exception(self, native_err, context):
        if isinstance(native_err, SchemaSyntaxError):
            return native_err
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
