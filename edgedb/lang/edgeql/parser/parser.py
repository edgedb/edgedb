##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import debug, parsing
from edgedb.lang.edgeql.errors import EdgeQLSyntaxError

from .grammar import lexer


class EdgeQLParserBase(parsing.Parser):
    def get_debug(self):
        return debug.flags.edgeql_parser

    def get_exception(self, native_err, context, token=None):
        msg = native_err.args[0]

        if isinstance(native_err, EdgeQLSyntaxError):
            return native_err
        else:
            if msg.startswith('Unexpected token: '):
                token = token or getattr(native_err, 'token', None)

                if not token:
                    msg = 'Unexpected end of line'
                elif hasattr(token, 'val'):
                    msg = f'Unexpected {token.val!r}'
                elif token.type == 'NL':
                    msg = 'Unexpected end of line'
                else:
                    msg = f'Unexpected {token.text!r}'

        return EdgeQLSyntaxError(msg, context=context, token=token)

    def get_lexer(self):
        return lexer.EdgeQLLexer()


class EdgeQLExpressionParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import single
        return single


class EdgeQLBlockParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import block
        return block
