##
# Copyright (c) 2008-2010, 2014 MagicStack Inc.
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

    def get_exception(self, native_err, context):
        if isinstance(native_err, EdgeQLSyntaxError):
            return native_err
        return EdgeQLSyntaxError(native_err.args[0], context=context)

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
