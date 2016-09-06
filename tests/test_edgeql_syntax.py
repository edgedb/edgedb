##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from edgedb.lang import _testbase as tb
from edgedb.lang.edgeql import generate_source as edgeql_to_source
from edgedb.lang.edgeql.parser import parser as edgeql_parser


class EdgeQLSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s\'";]+|(#.*?\n)')
    parser_debug_flag = 'DEBUG_EDGEQL'
    markup_dump_lexer = 'edgeql'
    ast_to_source = edgeql_to_source

    def get_parser(self, *, spec):
        return edgeql_parser.EdgeQLBlockParser()


class TestEdgeSchemaParser(EdgeQLSyntaxTest):
    def test_edgeql_syntax_contants01(self):
        """SELECT 1;"""
