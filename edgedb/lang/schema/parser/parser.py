##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import parsing
from .grammar import lexer


class EdgeSchemaParser(parsing.Parser):
    def get_parser_spec_module(self):
        from .grammar import declarations
        return declarations

    def get_lexer(self):
        return lexer.EdgeSchemaLexer()

    def process_lex_token(self, mod, tok):
        if tok.attrs['type'] in {'NEWLINE', 'WS', 'COMMENT'}:
            return None
        else:
            return super().process_lex_token(mod, tok)
