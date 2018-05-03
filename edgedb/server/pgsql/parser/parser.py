##
# Copyright (c) 2010-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common import debug, parsing
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
