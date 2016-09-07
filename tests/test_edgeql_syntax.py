##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from edgedb.lang import _testbase as tb
from edgedb.lang.edgeql import generate_source as edgeql_to_source, errors
from edgedb.lang.edgeql.parser import parser as edgeql_parser


class EdgeQLSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s]+|(#.*?\n)')
    parser_debug_flag = 'DEBUG_EDGEQL'
    markup_dump_lexer = 'edgeql'
    ast_to_source = edgeql_to_source

    def get_parser(self, *, spec):
        return edgeql_parser.EdgeQLBlockParser()


class TestEdgeSchemaParser(EdgeQLSyntaxTest):
    def test_edgeql_syntax_case01(self):
        """
        Select 1;
        select 1;
        SELECT 1;
        SeLeCT 1;
        """

    # this is a statement syntax parser, not expressions, so
    # semicolons are obligatory terminators
    #
    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=9)
    def test_edgeql_syntax_nonstatement01(self):
        """SELECT 1"""

    # 1 + 2 is a valid expression, but it has to have SELECT keyword
    # to be a statement
    #
    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=1)
    def test_edgeql_syntax_nonstatement02(self):
        """1 + 2;"""

    def test_edgeql_syntax_contants01(self):
        """SELECT 1;"""

    def test_edgeql_syntax_contants02(self):
        """SELECT 'a1';"""

    def test_edgeql_syntax_contants03(self):
        """
        SELECT "a1";

% OK %

        SELECT 'a1';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=12)
    def test_edgeql_syntax_ops01(self):
        """SELECT 40 >> 2;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=14)
    def test_edgeql_syntax_ops02(self):
        """SELECT 40 << 2;"""

    def test_edgeql_syntax_ops03(self):
        """
        SELECT 1 + 2;

% OK %

        SELECT (1 + 2);
        """

    def test_edgeql_syntax_ops04(self):
        """
        SELECT (40 <= 2);
        SELECT (40 >= 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=7, col=13)
    def test_edgeql_syntax_name01(self):
        """
        SELECT doo.(goo::first) {
            bar: goo::SomeType {
                name,
                description
            } WHERE name = "Silly",
            `@foo`:= 42
        };
        """
