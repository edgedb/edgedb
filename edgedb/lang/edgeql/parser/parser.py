##
# Copyright (c) 2008-2010, 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

from edgedb.lang.common import parsing, debug

from edgedb.lang.edgeql.errors import EdgeQLQueryError
from edgedb.lang.edgeql.parser.errors import EdgeQLSyntaxError

from .grammar import lexer


class EdgeQLParserBase(parsing.Parser):
    def get_debug(self):
        return bool(os.environ.get('EDGEDB_DEBUG_EDGEQL_PARSER'))

    def get_exception(self, native_err, context):
        return EdgeQLQueryError(native_err.args[0], context=context)

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
