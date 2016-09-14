##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import unittest

from edgedb.lang import _testbase as tb
from edgedb.lang.edgeql import generate_source as edgeql_to_source, errors
from edgedb.lang.edgeql.parser import parser as edgeql_parser


class EdgeQLSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s]+|(#.*?\n)|(,(?=\s*[})]))')
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

    def test_edgeql_syntax_name01(self):
        """
        SELECT bar;
        SELECT `bar`;
        SELECT foo::bar;
        SELECT foo::`bar`;
        SELECT `foo`::bar;
        SELECT `foo`::`bar`;

% OK %

        SELECT bar;
        SELECT bar;
        SELECT (foo::bar);
        SELECT (foo::bar);
        SELECT (foo::bar);
        SELECT (foo::bar);
        """

    def test_edgeql_syntax_name02(self):
        """
        SELECT (bar);
        SELECT (`bar`);
        SELECT (foo::bar);
        SELECT (foo::`bar`);
        SELECT (`foo`::bar);
        SELECT (`foo`::`bar`);

% OK %

        SELECT bar;
        SELECT bar;
        SELECT (foo::bar);
        SELECT (foo::bar);
        SELECT (foo::bar);
        SELECT (foo::bar);
        """

    def test_edgeql_syntax_name03(self):
        """
        SELECT (action);
        SELECT (`action`);
        SELECT (event::action);
        SELECT (event::`action`);
        SELECT (`event`::action);
        SELECT (`event`::`action`);

% OK %

        SELECT `action`;
        SELECT `action`;
        SELECT (`event`::`action`);
        SELECT (`event`::`action`);
        SELECT (`event`::`action`);
        SELECT (`event`::`action`);
        """

    def test_edgeql_syntax_name04(self):
        """
        SELECT (event::select);
        SELECT (event::`select`);
        SELECT (`event`::select);
        SELECT (`event`::`select`);

% OK %

        SELECT (`event`::`select`);
        SELECT (`event`::`select`);
        SELECT (`event`::`select`);
        SELECT (`event`::`select`);
        """

    def test_edgeql_syntax_name05(self):
        """
        SELECT foo.bar;
        SELECT `foo.bar`;
        SELECT `foo.bar`::spam;
        SELECT `foo.bar`::spam.ham;
        SELECT `foo.bar`::`spam.ham`;

% OK %

        SELECT foo.bar;
        SELECT `foo.bar`;
        SELECT (`foo.bar`::spam);
        SELECT (`foo.bar`::spam).ham;
        SELECT (`foo.bar`::`spam.ham`);
        """

    def test_edgeql_syntax_name06(self):
        """
        SELECT foo.bar;
        SELECT (foo).bar;
        SELECT (foo).(bar);
        SELECT ((foo).bar);
        SELECT ((((foo))).bar);
        SELECT ((((foo))).(((bar))));

% OK %

        SELECT foo.bar;
        SELECT foo.bar;
        SELECT foo.bar;
        SELECT foo.bar;
        SELECT foo.bar;
        SELECT foo.bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_name07(self):
        """
        SELECT event;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=17)
    def test_edgeql_syntax_name08(self):
        """
        SELECT (event::all);
        SELECT (all::event);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=23)
    def test_edgeql_syntax_name09(self):
        """
        SELECT (event::select);
        SELECT (select::event);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_name10(self):
        """
        SELECT `@event`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_name11(self):
        """
        SELECT @event;
        """

    @unittest.expectedFailure
    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_name12(self):
        """
        SELECT foo::`@event`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_name13(self):
        """
        SELECT foo::@event;
        """

    def test_edgeql_syntax_shape01(self):
        """
        SELECT Foo {bar};
        SELECT (Foo) {bar};
        SELECT (((Foo))) {bar};

% OK %

        SELECT Foo {bar};
        SELECT Foo {bar};
        SELECT Foo {bar};
        """

    def test_edgeql_syntax_shape02(self):
        """
        SELECT Foo {bar};
        SELECT Foo {(bar)};
        SELECT Foo {(((bar)))};
        SELECT Foo {@bar};
        SELECT Foo {@(bar)};
        SELECT Foo {@(((bar)))};
        SELECT Foo {>bar};
        SELECT Foo {>(bar)};
        SELECT Foo {>(((bar)))};
        SELECT Foo {<bar};
        SELECT Foo {<(bar)};
        SELECT Foo {<(((bar)))};

% OK %

        SELECT Foo {bar};
        SELECT Foo {bar};
        SELECT Foo {bar};
        SELECT Foo {@bar};
        SELECT Foo {@bar};
        SELECT Foo {@bar};
        SELECT Foo {bar};
        SELECT Foo {bar};
        SELECT Foo {bar};
        SELECT Foo {<bar};
        SELECT Foo {<bar};
        SELECT Foo {<bar};
        """

    def test_edgeql_syntax_shape03(self):
        """
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.(bar)};
        SELECT Foo {Bar.(((bar)))};
        SELECT Foo {Bar.>bar};
        SELECT Foo {Bar.>(bar)};
        SELECT Foo {Bar.>(((bar)))};
        SELECT Foo {Bar.<bar};
        SELECT Foo {Bar.<(bar)};
        SELECT Foo {Bar.<(((bar)))};

% OK %

        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.<bar};
        SELECT Foo {Bar.<bar};
        SELECT Foo {Bar.<bar};
        """

    def test_edgeql_syntax_shape04(self):
        """
        SELECT Foo {Bar.bar};
        SELECT Foo {(Bar).(bar)};
        SELECT Foo {(((Bar))).(((bar)))};
        SELECT Foo {Bar.>bar};
        SELECT Foo {(Bar).>(bar)};
        SELECT Foo {(((Bar))).>(((bar)))};
        SELECT Foo {Bar.<bar};
        SELECT Foo {(Bar).<(bar)};
        SELECT Foo {(((Bar))).<(((bar)))};

% OK %

        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.bar};
        SELECT Foo {Bar.<bar};
        SELECT Foo {Bar.<bar};
        SELECT Foo {Bar.<bar};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=13)
    def test_edgeql_syntax_shape05(self):
        """
        SELECT Foo {
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=13)
    def test_edgeql_syntax_shape06(self):
        """
        SELECT Foo {
            bar,
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=7, col=13)
    def test_edgeql_syntax_shape07(self):
        """
        SELECT Foo {
            bar: {
                baz,
                boo
            },
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=5, col=17)
    def test_edgeql_syntax_shape08(self):
        """
        SELECT Foo {
            bar: {
                baz,
                `@boo`
            },
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=6, col=21)
    def test_edgeql_syntax_shape09(self):
        """
        SELECT Foo {
            bar: {
                baz,
                boo
            } WHERE `@spam` = 'bad',
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=7, col=13)
    def test_edgeql_syntax_shape10(self):
        """
        SELECT Foo {
            bar: {
                baz,
                boo
            } WHERE spam = 'bad',
            `@foo`:= 42
        };
        """

    @unittest.expectedFailure
    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=21)
    def test_edgeql_syntax_shape11(self):
        """
        SELECT Foo {
            __type__.name
        };
        """

    def test_edgeql_syntax_shape12(self):
        """
        SELECT Foo {
            __type__: {
                name,
            }
        };
        """

    def test_edgeql_syntax_shape13(self):
        """
        SELECT Foo {
            __type__: {
                name,
                description,
            }
        };
        """

    def test_edgeql_syntax_shape14(self):
        """
        SELECT {
            name := 'foo',
            description := 'bar'
        };
        """

    def test_edgeql_syntax_path01(self):
        """
        SELECT Foo.bar;
        SELECT Foo.>bar;
        SELECT Foo.<bar;
        SELECT Foo.bar@spam;
        SELECT Foo.>bar@spam;
        SELECT Foo.<bar@spam;
        SELECT Foo.bar[TO Baz];
        SELECT Foo.>bar[TO Baz];
        SELECT Foo.<bar[TO Baz];

% OK %

        SELECT Foo.bar;
        SELECT Foo.bar;
        SELECT Foo.<bar;
        SELECT Foo.bar@spam;
        SELECT Foo.bar@spam;
        SELECT Foo.<bar@spam;
        SELECT Foo.bar[TO Baz];
        SELECT Foo.bar[TO Baz];
        SELECT Foo.<bar[TO Baz];
        """

    def test_edgeql_syntax_path02(self):
        """
        SELECT Foo.event;
        SELECT Foo.>event;
        SELECT Foo.<event;
        SELECT Foo.event@action;
        SELECT Foo.>event@action;
        SELECT Foo.<event@action;
        SELECT Foo.event[TO Action];
        SELECT Foo.>event[TO Action];
        SELECT Foo.<event[TO Action];

% OK %

        SELECT Foo.`event`;
        SELECT Foo.`event`;
        SELECT Foo.<`event`;
        SELECT Foo.`event`@`action`;
        SELECT Foo.`event`@`action`;
        SELECT Foo.<`event`@`action`;
        SELECT Foo.`event`[TO `Action`];
        SELECT Foo.`event`[TO `Action`];
        SELECT Foo.<`event`[TO `Action`];
        """

    def test_edgeql_syntax_path03(self):
        """
        SELECT Foo.(lib::bar);
        SELECT Foo.>(lib::bar);
        SELECT Foo.<(lib::bar);
        SELECT Foo.(lib::bar)@(lib::spam);
        SELECT Foo.>(lib::bar)@(lib::spam);
        SELECT Foo.<(lib::bar)@(lib::spam);
        SELECT Foo.(lib::bar)[TO lib::Baz];
        SELECT Foo.>(lib::bar)[TO lib::Baz];
        SELECT Foo.<(lib::bar)[TO lib::Baz];

% OK %

        SELECT Foo.(lib::bar);
        SELECT Foo.(lib::bar);
        SELECT Foo.<(lib::bar);
        SELECT Foo.(lib::bar)@(lib::spam);
        SELECT Foo.(lib::bar)@(lib::spam);
        SELECT Foo.<(lib::bar)@(lib::spam);
        SELECT Foo.(lib::bar)[TO lib::Baz];
        SELECT Foo.(lib::bar)[TO lib::Baz];
        SELECT Foo.<(lib::bar)[TO lib::Baz];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_path04(self):
        """
        SELECT Foo[TO Bar];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=29)
    def test_edgeql_syntax_path05(self):
        """
        SELECT Foo.bar@spam[TO Bar];
        """

    def test_edgeql_syntax_path06(self):
        """
        SELECT Foo.bar[TO To];  # unreserved keyword as concept name

% OK %

        SELECT Foo.bar[TO `To`];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=30)
    def test_edgeql_syntax_path07(self):
        """
        SELECT Foo.bar[TO To To];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=27)
    def test_edgeql_syntax_path08(self):
        """
        SELECT Foo.bar[TO All];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=27)
    def test_edgeql_syntax_path09(self):
        """
        SELECT Foo.bar[2][TO Baz];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=29)
    def test_edgeql_syntax_path10(self):
        """
        SELECT Foo.bar[2:4][TO Baz];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=28)
    def test_edgeql_syntax_path11(self):
        """
        SELECT Foo.bar[2:][TO Baz];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=28)
    def test_edgeql_syntax_path12(self):
        """
        SELECT Foo.bar[:2][TO Baz];
        """

    def test_edgeql_syntax_path13(self):
        """
        SELECT (Foo.bar)[TO Baz];
        SELECT Foo.(bar)[TO Baz];
        SELECT Foo.<(bar)[TO Baz];

% OK %

        SELECT Foo.bar[TO Baz];
        SELECT Foo.bar[TO Baz];
        SELECT Foo.<bar[TO Baz];
        """

    def test_edgeql_syntax_type_interpretation01(self):
        """
        SELECT (Foo AS Bar);
        SELECT (Foo.bar AS Bar);
        """

    def test_edgeql_syntax_map01(self):
        """
        SELECT {
            'name' : 'foo',
            'description' : 'bar'
        };
        SELECT {
            'name' : 'baz',
        };
        """
