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
    def get_exception(self, native_err, context, token=None):
        if isinstance(native_err, SchemaSyntaxError):
            return native_err

        msg = native_err.args[0]
        if token and token.type == 'BADLINECONT':
            context.start.column += 1
            return SchemaSyntaxError(
                'Unexpected character after line continuation '
                'character',
                context=context)

        # if the error is about unexpected <$> token, convert the text to be
        # referencing <NL> token
        elif msg == 'Unexpected token: <$>':
            return SchemaSyntaxError('Unexpected end of line', context=context)

        else:
            if msg.startswith('Unexpected token: '):
                token = token or native_err.token

                if hasattr(token, 'val'):
                    msg = f'Unexpected {token.val!r}'
                elif token.type == 'NL':
                    msg = 'Unexpected end of line'
                elif token.type == 'INDENT':
                    msg = 'Unexpected indentation level increase'
                elif token.type == 'DEDENT':
                    msg = 'Unexpected indentation level decrease'
                else:
                    msg = f'Unexpected {token.text!r}'

        return SchemaSyntaxError(msg, context=context)

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
