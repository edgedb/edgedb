##
# Copyright (c) 2008-2010, 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import ast, parsing, debug

from metamagic.caos import types as caos_types

from metamagic.caos.caosql import ast as qlast
from metamagic.caos.caosql.errors import CaosQLQueryError
from metamagic.caos.caosql.parser.errors import CaosQLSyntaxError

from . import lexer


class CaosQLParser(parsing.Parser):
    def get_parser_spec_module(self):
        from . import caosql
        return caosql

    def get_debug(self):
        return 'caos.caosql.parser' in debug.channels

    def get_exception(self, native_err, context):
        return CaosQLQueryError(native_err.args[0], context=context)

    def get_lexer(self):
        return lexer.CaosQLLexer()

    def process_lex_token(self, mod, tok):
        tok_type = tok.attrs['type']
        if tok_type in ('WS', 'NL', 'COMMENT'):
            return None

        return super().process_lex_token(mod, tok)
