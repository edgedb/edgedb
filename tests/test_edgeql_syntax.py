#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import re
import unittest  # NOQA

from edb import errors

from edb.testbase import lang as tb
from edb.edgeql import generate_source as edgeql_to_source
from edb.edgeql.parser import parser as edgeql_parser
from edb.tools import test


class EdgeQLSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s]+|(#.*?(\n|$))|(,(?=\s*[})]))')
    parser_debug_flag = 'DEBUG_EDGEQL'
    markup_dump_lexer = 'sql'
    ast_to_source = edgeql_to_source

    @classmethod
    def get_parser(cls):
        return edgeql_parser.EdgeQLBlockParser()


class TestEdgeQLParser(EdgeQLSyntaxTest):
    def test_edgeql_syntax_empty_01(self):
        """"""

    def test_edgeql_syntax_empty_02(self):
        """# only comment"""

    def test_edgeql_syntax_empty_03(self):
        """

        # only comment

        """

    def test_edgeql_syntax_empty_04(self):
        """;
% OK %  """

    def test_edgeql_syntax_empty_05(self):
        """;# only comment
% OK %  """

    def test_edgeql_syntax_empty_06(self):
        """
        ;
        # only comment
        ;
% OK %
        """

    def test_edgeql_syntax_case_01(self):
        """
        Select 1;
        select 1;
        SELECT 1;
        SeLeCT 1;
        """

    def test_edgeql_syntax_omit_semicolon_01(self):
        """
        SELECT 1

% OK %

        SELECT 1;
        """

    def test_edgeql_syntax_omit_semicolon_02(self):
        """
        SELECT 2;
        SELECT 1

% OK %

        SELECT 2;
        SELECT 1;
        """

    # 1 + 2 is a valid expression, but it has to have SELECT keyword
    # to be a statement
    #
    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=1)
    def test_edgeql_syntax_nonstatement_02(self):
        """1 + 2;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=8)
    def test_edgeql_syntax_number_too_large(self):
        """SELECT 111111111111111111111111111111111111111111111111111111;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=10)
    def test_edgeql_syntax_float_number_too_large(self):
        """SELECT 2+1e999;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=8)
    def test_edgeql_syntax_float_number_too_small_01(self):
        """SELECT 0.01e-322;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=8)
    def test_edgeql_syntax_float_number_too_small_02(self):
        """SELECT 1e-324;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=8)
    def test_edgeql_syntax_float_number_too_small_03(self):
        (
            "SELECT 0."
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "0000000000_0000000000_0000000000_0000000000"
            "1;"
        )

    def test_edgeql_syntax_constants_01(self):
        """
        SELECT 0;
        SELECT 1;
        SELECT +7;
        SELECT -7;
        SELECT 551;
        SELECT 1_024;
        """

    def test_edgeql_syntax_constants_02(self):
        """
        SELECT 'a1';
        SELECT "a1";;;;;;;;;;;;
        SELECT r'a1';
        SELECT r"a1";
        SELECT $$a1$$;
        SELECT $qwe$a1$qwe$;

% OK %

        SELECT 'a1';
        SELECT 'a1';
        SELECT 'a1';
        SELECT 'a1';
        SELECT 'a1';
        SELECT 'a1';
        """

    def test_edgeql_syntax_constants_03(self):
        """
        SELECT 3.5432;
        SELECT +3.5432;
        SELECT -3.5432;
        """

    def test_edgeql_syntax_constants_04(self):
        """
        SELECT 354.32;
        SELECT 35400000000000.32;
        SELECT 35400000000000000000.32;
        SELECT 3.5432e20;
        SELECT 3.5432e+20;
        SELECT 3.5432e-20;
        SELECT 3.543_2e-20;
        SELECT 354.32e-20;
        SELECT 2_354.32e-20;
        SELECT 0e-999;

% OK %

        SELECT 354.32;
        SELECT 35400000000000.32;
        SELECT 35400000000000000000.32;
        SELECT 3.5432e20;
        SELECT 3.5432e+20;
        SELECT 3.5432e-20;
        SELECT 3.543_2e-20;
        SELECT 354.32e-20;
        SELECT 2_354.32e-20;
        SELECT 0e-999;
        """

    def test_edgeql_syntax_constants_05(self):
        """
        SELECT TRUE;
        SELECT FALSE;
        """

    def test_edgeql_syntax_constants_06(self):
        """
        SELECT $1;
        SELECT $123;
        SELECT $somevar;
        SELECT $select;
        SELECT (($SELECT + $TRUE) + $WITH);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "leading zeros are not allowed in numbers",
                  line=2, col=16)
    def test_edgeql_syntax_constants_07(self):
        """
        SELECT 02;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'expected digit after dot',
                  line=2, col=16)
    def test_edgeql_syntax_constants_08(self):
        """
        SELECT 1.;
        """

    def test_edgeql_syntax_constants_09(self):
        # NOTE: Although it looks like a float, the expression `.1` in
        # this test is not a float, instead it is a partial path (like
        # `.name`). It is syntactically legal, but will fail to
        # resolve to anything (see test_edgeql_expr_paths_03).
        """
        SELECT .1;
        """

    def test_edgeql_syntax_constants_10(self):
        r"""
        SELECT b'1\t\n1' + b"2\x00";
% OK %
        SELECT (b'1\t\n1' + b'2\x00');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid bytes literal: invalid escape sequence '\\c'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_11(self):
        R"""
        SELECT b'aaa\cbbb';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid bytes literal: invalid escape sequence '\\x0z'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_12(self):
        r"""
        SELECT b'aaa\x0zaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid bytes literal: character 'Ł'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_13(self):
        r"""
        SELECT b'Łukasz Langa';
        """

    def test_edgeql_syntax_constants_14(self):
        r"""
        SELECT b'aa
aa';
% OK %
        SELECT b'aa\naa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid string literal: invalid escape sequence '\\c'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_15(self):
        r"""
        SELECT 'aaa\cbbb';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid string literal: invalid escape sequence '\\x0z'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_16(self):
        r"""
        SELECT 'aaa\x0zaa';
        """

    def test_edgeql_syntax_constants_17(self):
        r"""
        SELECT 'Łukasz Langa';
        """

    def test_edgeql_syntax_constants_18(self):
        r"""
        SELECT 'aa
        aa';
% OK %
        SELECT 'aa\naa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid string literal: invalid escape sequence '\\u0zaa'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_19(self):
        r"""
        SELECT 'aaa\u0zaazz';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid escape sequence '\\U0zaazzzz'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_20(self):
        r"""
        SELECT 'aaa\U0zaazzzzzzzzzzz';
        """

    def test_edgeql_syntax_constants_21(self):
        r"""
        SELECT '\'"\\\'\""\\x\\u';
% OK %
        SELECT $$'"\'""\x\u$$;
        """

    def test_edgeql_syntax_constants_22(self):
        r"""
        SELECT to_json('{"defaultValue": "\\"SMALLEST\\""}');
% OK %
        SELECT to_json(r'{"defaultValue": "\"SMALLEST\""}');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"unterminated string, quoted by `'`", line=2, col=20)
    def test_edgeql_syntax_constants_23(self):
        r"""
        SELECT '\\'';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'unterminated string, quoted by `"`', line=2, col=20)
    def test_edgeql_syntax_constants_24(self):
        r"""
        SELECT "\\"";
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"unterminated string, quoted by `'`", line=2, col=21)
    def test_edgeql_syntax_constants_25(self):
        r"""
        SELECT b'\\'';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"unexpected character '☎'", line=2, col=21)
    def test_edgeql_syntax_constants_26(self):
        r"""
        SELECT b"\\"☎️";
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid bytes literal: character '☎'", line=2, col=16)
    def test_edgeql_syntax_constants_26_1(self):
        r"""
        SELECT b"xyz☎️";
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  line=2, col=21)
    def test_edgeql_syntax_constants_27(self):
        r"""
        SELECT b"\\"";
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid escape sequence '\\U0zaa'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_28(self):
        r"""
        SELECT 'aaa\U0zaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid escape sequence '\\u0z'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_29(self):
        r"""
        SELECT 'aaa\u0z';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid escape sequence '\\x0'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_30(self):
        r"""
        SELECT 'aaa\x0';
        """

    def test_edgeql_syntax_constants_31(self):
        r"""
        SELECT 'aa\
                bb \
                aa';
% OK %
        SELECT 'aabb aa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"unterminated string, quoted by `'`",
                  line=2, col=16)
    def test_edgeql_syntax_constants_32(self):
        r"""
        SELECT 'aa\
                bb \
                aa\';
        """

    def test_edgeql_syntax_constants_33(self):
        r"""
        SELECT r'aaa\x0';
        """

    def test_edgeql_syntax_constants_34(self):
        r"""
        SELECT r'\';
        """

    def test_edgeql_syntax_constants_35(self):
        r"""
        SELECT r"\n\w\d";
% OK %
        SELECT r'\n\w\d';
        """

    def test_edgeql_syntax_constants_36(self):
        r"""
        SELECT $aa$\n\w\d$aa$;
% OK %
        SELECT r'\n\w\d';
        """

    def test_edgeql_syntax_constants_37(self):
        r"""
        SELECT "'''";
        """

    def test_edgeql_syntax_constants_38(self):
        r"""
        SELECT "\n";
% OK %
        SELECT '\n';
        """

    def test_edgeql_syntax_constants_39(self):
        r"""
        SELECT '\x1F\x01\x00\x6e';
% OK %
        SELECT '\x1f\x01\x00n';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid escape sequence '\\x8f'",
                  line=2, col=16)
    def test_edgeql_syntax_constants_40(self):
        r"""
        SELECT "\x1F\x01\x8F\x6e";
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"invalid string literal: invalid escape sequence '\\\('",
                  line=2, col=16)
    def test_edgeql_syntax_constants_41(self):
        r"""
        SELECT 'aaa \(aaa) bbb';
        """

    def test_edgeql_syntax_constants_42(self):
        """
        SELECT $select;
        """

    def test_edgeql_syntax_constants_43(self):
        """
        SELECT -0n;
        SELECT 0n;
        SELECT 1n;
        SELECT -1n;
        SELECT 100000n;
        SELECT -100000n;
        SELECT 100_000n;
        SELECT -100_000n;
        SELECT -354.32n;
        SELECT 35400000000000.32n;
        SELECT -35400000000000000000.32n;
        SELECT 3.5432e20n;
        SELECT -3.5432e+20n;
        SELECT 3.5432e-20n;
        SELECT 3.543_2e-20n;
        SELECT 354.32e-20n;

% OK %

        SELECT -0n;
        SELECT 0n;
        SELECT 1n;
        SELECT -1n;
        SELECT 100000n;
        SELECT -100000n;
        SELECT 100_000n;
        SELECT -100_000n;
        SELECT -354.32n;
        SELECT 35400000000000.32n;
        SELECT -35400000000000000000.32n;
        SELECT 3.5432e20n;
        SELECT -3.5432e+20n;
        SELECT 3.5432e-20n;
        SELECT 3.543_2e-20n;
        SELECT 354.32e-20n;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected 'n'",
                  line=2, col=18)
    def test_edgeql_syntax_constants_44(self):
        """
        SELECT 1 n;
        """

    def test_edgeql_syntax_constants_45(self):
        """
        SELECT 123e+100n;
        SELECT 123e100n;

% OK %

        SELECT 123e+100n;
        SELECT 123e100n;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=12)
    def test_edgeql_syntax_ops_01(self):
        """SELECT 40 >> 2;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=14)
    def test_edgeql_syntax_ops_02(self):
        """SELECT 40 << 2;"""

    def test_edgeql_syntax_ops_03(self):
        """
        SELECT (40 <= 2);
        SELECT (40 >= 2);
        """

    def test_edgeql_syntax_ops_04(self):
        """
        SELECT 1 + 2;
        SELECT (1 + 2);
        SELECT (1) + 2;
        SELECT (((1) + (2)));

% OK %

        SELECT (1 + 2);
        SELECT (1 + 2);
        SELECT (1 + 2);
        SELECT (1 + 2);
        """

    def test_edgeql_syntax_ops_05(self):
        """
        SELECT User.age + 2;
        SELECT (User.age + 2);
        SELECT (User.age) + 2;
        SELECT (((User.age) + (2)));

% OK %

        SELECT (User.age + 2);
        SELECT (User.age + 2);
        SELECT (User.age + 2);
        SELECT (User.age + 2);
        """

    def test_edgeql_syntax_ops_06(self):
        """
        SELECT (40 + 2);
        SELECT (40 - 2);
        SELECT (40 * 2);
        SELECT (40 / 2);
        SELECT (40 % 2);
        SELECT (40 ^ 2);
        SELECT (40 < 2);
        SELECT (40 > 2);
        SELECT (40 <= 2);
        SELECT (40 >= 2);
        SELECT (40 = 2);
        SELECT (40 != 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_ops_07(self):
        """
        SELECT 40 == 2;
        """

    def test_edgeql_syntax_ops_08(self):
        """
        SELECT (User.age + 2);
        SELECT (User.age - 2);
        SELECT (User.age * 2);
        SELECT (User.age / 2);
        SELECT (User.age % 2);
        SELECT (User.age ^ 2);
        SELECT (User.age < 2);
        SELECT (User.age > 2);
        SELECT (User.age <= 2);
        SELECT (User.age >= 2);
        SELECT (User.age = 2);
        SELECT (User.age != 2);
        """

    def test_edgeql_syntax_ops_09(self):
        """
        SELECT (Foo.foo AND Foo.bar);
        SELECT (Foo.foo OR Foo.bar);
        SELECT NOT (Foo.foo);
        """

    def test_edgeql_syntax_ops_10(self):
        """
        SELECT (User.name IN {'Alice', 'Bob'});
        SELECT (User.name NOT IN {'Alice', 'Bob'});
        """

    def test_edgeql_syntax_ops_11(self):
        """
        SELECT (User.name LIKE 'Al%');
        SELECT (User.name ILIKE 'al%');
        SELECT (User.name NOT LIKE 'Al%');
        SELECT (User.name NOT ILIKE 'al%');
        """

    def test_edgeql_syntax_ops_12(self):
        """
        SELECT EXISTS (User.groups.description);
        """

    def test_edgeql_syntax_ops_14(self):
        """
        SELECT -1 + 2 * 3 - 5 - 6 / 2 > 0 OR 25 % 4 = 3 AND 42 IN {12, 42, 14};

% OK %

        SELECT (
            (
                (
                    ((-1 + (2 * 3)) - 5)
                    -
                    (6 / 2)
                ) > 0
            )
            OR
            (
                ((25 % 4) = 3)
                AND
                (42 IN {12, 42, 14})
            )
        );
        """

    def test_edgeql_syntax_ops_15(self):
        """
        SELECT
            ((-1 + 2) * 3 - (5 - 6) / 2 > 0 OR 25 % 4 = 3)
            AND 42 IN {12, 42, 14};

% OK %

        SELECT (
            (
                (
                    (
                        ((- 1 + 2) * 3)
                        -
                        ((5 - 6) / 2)
                    ) > 0
                )
                OR
                ((25 % 4) = 3)
            )
            AND
            (42 IN {12, 42, 14})
        );
        """

    def test_edgeql_syntax_ops_16(self):
        """
        SELECT (42 IF foo ELSE 24);
        SELECT (
            42 IF Foo.bar ELSE
            (
                43 IF Foo.baz ELSE
                44
            )
        );
        """

    def test_edgeql_syntax_ops_17(self):
        """
        SELECT 42 IF Foo.bar ELSE
               43 IF Foo.baz ELSE
               44;

% OK %

        SELECT (
            42 IF Foo.bar ELSE
            (
                43 IF Foo.baz ELSE
                44
            )
        );
        """

    def test_edgeql_syntax_ops_18(self):
        """
        SELECT 40 + 2 IF Foo.bar ELSE
               40 + 3 IF Foo.baz ELSE
               40 + 4;

% OK %

        SELECT (
            (40 + 2) IF Foo.bar ELSE
            (
                (40 + 3) IF Foo.baz ELSE
                (40 + 4)
            )
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected '>='", line=2, col=16)
    def test_edgeql_syntax_ops_19(self):
        """
        SELECT >=1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\*'", line=2, col=16)
    def test_edgeql_syntax_ops_20(self):
        """
        SELECT *1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "unexpected character '~'", line=2, col=16)
    def test_edgeql_syntax_ops_21(self):
        """
        SELECT ~1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected '>'", line=2, col=16)
    def test_edgeql_syntax_ops_22(self):
        """
        SELECT >1;
        """

    def test_edgeql_syntax_ops_23(self):
        """
        SELECT (Foo.a ?= Foo.b);
        SELECT (Foo.b ?!= Foo.b);
        """

    def test_edgeql_syntax_ops_24(self):
        """
        SELECT (User.name IS std::str);
        SELECT (User IS SystemUser);
        SELECT (User.name IS NOT std::str);
        SELECT (User IS NOT SystemUser);

        SELECT (User.name IS (array<int>));
        SELECT (User.name IS (tuple<int, str, array<str>>));
        """

    def test_edgeql_syntax_ops_25(self):
        """
        SELECT User IS SystemUser | Foo;
        SELECT User IS SystemUser & Foo;
        SELECT User IS SystemUser & Foo | Bar;
        SELECT User IS SystemUser & Foo | Bar | (array<int>);

% OK %

        SELECT (User IS (SystemUser | Foo));
        SELECT (User IS (SystemUser & Foo));
        SELECT (User IS ((SystemUser & Foo) | Bar));
        SELECT (User IS (((SystemUser & Foo) | Bar) | (array<int>)));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected ','", line=2, col=31)
    def test_edgeql_syntax_ops_26(self):
        """
        SELECT (User IS (Named, Text));
        """

    def test_edgeql_syntax_required_01(self):
        """
        SELECT REQUIRED (User.groups.description);
        """

    def test_edgeql_syntax_list_01(self):
        """
        SELECT (some_list_fn())[2];
        SELECT (some_list_fn())[2:4];
        SELECT (some_list_fn())[2:];
        SELECT (some_list_fn())[:4];
        SELECT (some_list_fn())[-1:];
        SELECT (some_list_fn())[:-1];
        """

    def test_edgeql_syntax_name_01(self):
        """
        SELECT bar;
        SELECT `bar`;
        SELECT foo::bar;
        SELECT foo::`bar`;
        SELECT `foo`::bar;
        SELECT `foo`::`bar`;
        SELECT `foo``bar`;
        SELECT `foo`::`bar```;

% OK %

        SELECT bar;
        SELECT bar;
        SELECT foo::bar;
        SELECT foo::bar;
        SELECT foo::bar;
        SELECT foo::bar;
        SELECT `foo``bar`;
        SELECT foo::`bar```;
        """

    def test_edgeql_syntax_name_02(self):
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
        SELECT foo::bar;
        SELECT foo::bar;
        SELECT foo::bar;
        SELECT foo::bar;
        """

    def test_edgeql_syntax_name_03(self):
        """
        SELECT (action);
        SELECT (`action`);
        SELECT (event::action);
        SELECT (event::`action`);
        SELECT (`event`::action);
        SELECT (`event`::`action`);

% OK %

        SELECT action;
        SELECT action;
        SELECT event::action;
        SELECT event::action;
        SELECT event::action;
        SELECT event::action;
        """

    def test_edgeql_syntax_name_04(self):
        """
        SELECT (event::select);
        SELECT (event::`select`);
        SELECT (`event`::select);
        SELECT (`event`::`select`);

% OK %

        SELECT event::`select`;
        SELECT event::`select`;
        SELECT event::`select`;
        SELECT event::`select`;
        """

    def test_edgeql_syntax_name_05(self):
        """
        SELECT foo.bar;
        SELECT `foo.bar`;
        SELECT `foo.bar`::spam;
        SELECT `foo.bar`::spam.ham;
        SELECT `foo.bar`::`spam.ham`;
        SELECT (foo).bar;

% OK %

        SELECT foo.bar;
        SELECT `foo.bar`;
        SELECT `foo.bar`::spam;
        SELECT `foo.bar`::spam.ham;
        SELECT `foo.bar`::`spam.ham`;
        SELECT foo.bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_name_06(self):
        """
        SELECT foo.(bar);
        """

    def test_edgeql_syntax_name_07(self):
        """
        SELECT event;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=17)
    def test_edgeql_syntax_name_08(self):
        """
        SELECT (event::if);
        SELECT (if::event);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=23)
    def test_edgeql_syntax_name_09(self):
        """
        SELECT (event::select);
        SELECT (select::event);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_name_10(self):
        """
        SELECT `@event`;
        """

    def test_edgeql_syntax_name_11(self):
        # illegal semantically, but syntactically valid
        """
        SELECT @event;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_name_12(self):
        """
        SELECT foo::`@event`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_name_13(self):
        """
        SELECT foo::@event;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_name_14(self):
        """
        SELECT Foo.`@event`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=24)
    def test_edgeql_syntax_name_15(self):
        """
        SELECT (event::`@event`);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_name_16(self):
        """
        SELECT __Foo__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=22)
    def test_edgeql_syntax_name_17(self):
        """
        SELECT __Foo.__bar__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_name_18(self):
        """
        SELECT `__Foo__`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=22)
    def test_edgeql_syntax_name_19(self):
        """
        SELECT __Foo.`__bar__`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_name_20(self):
        """
        SELECT __Foo$;
        """

    def test_edgeql_syntax_name_21(self):
        """
        SELECT Пример;
        """

    def test_edgeql_syntax_name_22(self):
        """
        SELECT mod::Foo.bar.baz.boo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'backtick-quoted name cannot contain `::`', line=2, col=16)
    def test_edgeql_syntax_name_23(self):
        """
        SELECT `foo::bar`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'backtick quotes cannot be empty', line=2, col=16)
    def test_edgeql_syntax_name_24(self):
        """
        SELECT ``;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'backtick quotes cannot be empty', line=2, col=21)
    def test_edgeql_syntax_name_25(self):
        """
        SELECT foo::``;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'backtick quotes cannot be empty', line=2, col=16)
    def test_edgeql_syntax_name_26(self):
        """
        SELECT ``::Bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_name_27(self):
        """
        SELECT `$event`;
        """

    def test_edgeql_syntax_name_28(self):
        """
        SELECT `ok$event`;
        """

    def test_edgeql_syntax_shape_01(self):
        """
        SELECT Foo {bar};
        SELECT (Foo) {bar};
        SELECT (((Foo))) {bar};

% OK %

        SELECT Foo {bar};
        SELECT Foo {bar};
        SELECT Foo {bar};
        """

    def test_edgeql_syntax_shape_02(self):
        """
        SELECT Foo {bar};
        SELECT Foo {@bar};

% OK %

        SELECT Foo {bar};
        SELECT Foo {@bar};
        """

    def test_edgeql_syntax_shape_03(self):
        """
        SELECT Foo {[IS Bar].bar};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_shape_04(self):
        """
        SELECT Foo {<bar};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=13)
    def test_edgeql_syntax_shape_05(self):
        """
        SELECT Foo {
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=13)
    def test_edgeql_syntax_shape_06(self):
        """
        SELECT Foo {
            bar,
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=7, col=13)
    def test_edgeql_syntax_shape_07(self):
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
    def test_edgeql_syntax_shape_08(self):
        """
        SELECT Foo {
            bar: {
                baz,
                `@boo`
            },
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=6, col=22)
    def test_edgeql_syntax_shape_09(self):
        """
        SELECT Foo {
            bar: {
                baz,
                boo
            } FILTER `@spam` = 'bad',
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=7, col=13)
    def test_edgeql_syntax_shape_10(self):
        """
        SELECT Foo {
            bar: {
                baz,
                boo
            } FILTER spam = 'bad',
            `@foo`:= 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\.'", line=3, col=21)
    def test_edgeql_syntax_shape_11(self):
        """
        SELECT Foo {
            __type__.name
        };
        """

    def test_edgeql_syntax_shape_12(self):
        """
        SELECT Foo {
            __type__: {
                name,
            }
        };
        """

    def test_edgeql_syntax_shape_13(self):
        """
        SELECT Foo {
            __type__: {
                name,
                description,
            }
        };
        """

    def test_edgeql_syntax_shape_14(self):
        """
        SELECT {
            name := 'foo',
            description := 'bar'
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\('", line=2, col=21)
    def test_edgeql_syntax_shape_15(self):
        """
        SELECT Foo {(bar)};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\('", line=2, col=30)
    def test_edgeql_syntax_shape_16(self):
        """
        SELECT Foo {[IS Bar].(bar)};
        """

    def test_edgeql_syntax_shape_19(self):
        """
            SELECT
                Issue {
                    number
                }
            FILTER
                (((Issue)).number) = '1';

            SELECT
                (Issue) {
                    number
                }
            FILTER
                (((Issue)).number) = '1';

% OK %

            SELECT
                Issue {
                    number
                }
            FILTER
                (Issue.number = '1');

            SELECT
                Issue {
                    number
                }
            FILTER
                (Issue.number = '1');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected '@'", line=6, col=29)
    def test_edgeql_syntax_shape_20(self):
        """
        INSERT Foo{
            bar: {
                @weight,
                # this syntax may be valid in the future
                [IS BarLink]@special,
            }
        };
        """

    def test_edgeql_syntax_shape_21(self):
        """
        INSERT Foo{
            bar := 'some_string_val' {
                @weight := 3
            }
        };
        """

    def test_edgeql_syntax_shape_23(self):
        """
        SELECT 'Foo' {
            bar := 42
        };
        """

    def test_edgeql_syntax_shape_24(self):
        """
        SELECT Foo {
            spam
        } {
            bar := 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=24)
    def test_edgeql_syntax_shape_25(self):
        """
        SELECT Foo.bar AS bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\*'", line=4, col=24)
    def test_edgeql_syntax_shape_26(self):
        """
        SELECT Issue{
            name,
            related_to *,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\*'", line=4, col=24)
    def test_edgeql_syntax_shape_27(self):
        """
        SELECT Issue{
            name,
            related_to *5,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\*'", line=4, col=24)
    def test_edgeql_syntax_shape_28(self):
        """
        SELECT Issue{
            name,
            related_to *-1,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\*'", line=4, col=24)
    def test_edgeql_syntax_shape_29(self):
        """
        SELECT Issue{
            name,
            related_to *$var,
        };
        """

    def test_edgeql_syntax_shape_30(self):
        """
        SELECT Named {
            [IS Issue].references[IS File]: {
                name
            }
        };
        """

    def test_edgeql_syntax_shape_32(self):
        """
        SELECT User{
            name,
            owned := User.<owner[IS LogEntry] {
                body
            },
        };
        """

    def test_edgeql_syntax_shape_33(self):
        """
        SELECT User {
            name,
            groups: {
                name,
            } FILTER (.name = 'admin')
        };
        """

    def test_edgeql_syntax_shape_34(self):
        """
        SELECT User{
            name,
            owned := User.<owner[IS LogEntry] {
                body
            },
        } FILTER (.<owner.body = 'foo');
        """

    def test_edgeql_syntax_shape_35(self):
        """
        SELECT User {
            name,
            groups: {
                name,
            } FILTER (@special = True)
        };
        """

    def test_edgeql_syntax_shape_36(self):
        """
        SELECT User {
            name,
            groups: {
                name,
                @`rank`,
                @`~crazy`,
            }
        };

% OK %

        SELECT User {
            name,
            groups: {
                name,
                @rank,
                @`~crazy`,
            }
        };
        """

    def test_edgeql_syntax_shape_37(self):
        """
        SELECT Foo {
            foo FILTER (foo > 3),
            bar ORDER BY bar DESC,
            baz OFFSET 1 LIMIT 3,
        };
        """

    def test_edgeql_syntax_shape_38(self):
        """
        SELECT Foo {
            spam: {
                @foo FILTER (foo > 3),
                @bar ORDER BY bar DESC,
                @baz OFFSET 1 LIMIT 3,
            },
        };
        """

    def test_edgeql_syntax_shape_39(self):
        """
        SELECT Foo {
            foo := Foo {
                name
            }
        };
        """

    def test_edgeql_syntax_shape_40(self):
        """
        SELECT Foo {
            multi foo := Foo {
                name
            }
        };
        """

    def test_edgeql_syntax_shape_41(self):
        """
        SELECT Foo {
            single foo := Foo {
                name
            }
        };
        """

    def test_edgeql_syntax_shape_42(self):
        """
        SELECT Foo {
            required multi foo := Foo {
                name
            }
        };
        """

    def test_edgeql_syntax_shape_43(self):
        """
        SELECT Foo {
            required single foo := Foo {
                name
            }
        };
        """

    def test_edgeql_syntax_shape_43a(self):
        """
        SELECT Foo {
            optional multi foo := Foo {
                name
            }
        };
        """

    def test_edgeql_syntax_shape_43b(self):
        """
        SELECT Foo {
            optional single foo := Foo {
                name
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+foo',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'foo'",
                  line=3, col=27)
    def test_edgeql_syntax_shape_44(self):
        """
        SELECT Foo {
            required blah foo := Foo {
                name
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Missing ':' before '{'", line=3, col=17)
    def test_edgeql_syntax_shape_45(self):
        """
        SELECT Foo {
            foo {}
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Missing ':' before '{'", line=3, col=17)
    def test_edgeql_syntax_shape_46(self):
        """
        SELECT Foo {
            foo {
                bar
            }
        };
        """

    def test_edgeql_syntax_shape_47(self):
        """
        UPDATE Foo
        SET {
            foo += Bar
        };
        """

    def test_edgeql_syntax_shape_48(self):
        """
        UPDATE Foo
        SET {
            foo -= Bar
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+name',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'name'",
                  line=4, col=13)
    def test_edgeql_syntax_shape_49(self):
        """
        SELECT Foo {
            id
            name
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+name',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'name'",
                  line=6, col=13)
    def test_edgeql_syntax_shape_50(self):
        """
        SELECT Foo {
            bar: {
                id
            }
            name
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+name',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'name'",
                  line=4, col=13)
    def test_edgeql_syntax_shape_51(self):
        """
        SELECT Foo {
            bar := .id
            name
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+name',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'name'",
                  line=5, col=17)
    def test_edgeql_syntax_shape_52(self):
        """
        SELECT Foo {
            bar: {
                @linkprop
                name
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'Bar'", line=3, col=18)
    def test_edgeql_syntax_shape_53(self):
        """
        INSERT Foo {
            bar: Bar {
                val := 1
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+Foo',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before 'Foo'",
                  line=2, col=19)
    def test_edgeql_syntax_shape_54(self):
        """
        SELECT (1 Foo {
            foo
            bar
        });
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+bar',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'bar'",
                  line=4, col=13)
    def test_edgeql_syntax_shape_55(self):
        """
        SELECT (Foo {
            foo
            bar
        } 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+Foo',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before 'Foo'",
                  line=2, col=19)
    def test_edgeql_syntax_shape_56(self):
        """
        SELECT [1 Foo {
            foo
            bar := .foo + 1
        }.bar];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+bar',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'bar'",
                  line=4, col=13)
    def test_edgeql_syntax_shape_57(self):
        """
        SELECT [Foo {
            foo
            bar := .foo + 1
        }.bar 2];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+Foo',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before 'Foo'",
                  line=2, col=27)
    def test_edgeql_syntax_shape_58(self):
        """
        SELECT somefunc(1 Foo {
            foo
            bar
        });
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+bar',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'bar'",
                  line=4, col=13)
    def test_edgeql_syntax_shape_59(self):
        """
        SELECT somefunc(Foo {
            foo
            bar
        } 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '2'",
                  line=2, col=25)
    def test_edgeql_syntax_shape_60(self):
        """
        SELECT (Foo{id} 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+bar',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before 'bar'",
                  line=2, col=25)
    def test_edgeql_syntax_shape_61(self):
        """
        SELECT (Foo{id} bar);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before '2'",
                  line=2, col=25)
    def test_edgeql_syntax_shape_62(self):
        """
        SELECT [Foo{id} 2];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+bar',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before 'bar'",
                  line=2, col=25)
    def test_edgeql_syntax_shape_63(self):
        """
        SELECT [Foo{id} bar];
        """

    def test_edgeql_syntax_shape_64(self):
        """
        SELECT sys::Database{};

% OK %

        SELECT sys::Database;
        """

    def test_edgeql_syntax_shape_65(self):
        """
        select Foo{union};
        select Foo{except};
        select Foo{intersect};
        """

    def test_edgeql_syntax_shape_66(self):
        """
        select Foo {
            bar: {
                @union,
                @except,
                @intersect,
            }
        };
        """

    def test_edgeql_syntax_shape_67(self):
        """
        select Foo {
            [is Bar].union,
            [is Bar].except,
            [is Bar].intersect,
        };
        """

    def test_edgeql_syntax_shape_68(self):
        """
        select Foo {
            union := 1,
            except := 1,
            intersect := 1
        };
        """

    def test_edgeql_syntax_shape_69(self):
        """
        select Foo {
            required union := 1,
            required except := 1,
            required intersect := 1
        };
        """

    def test_edgeql_syntax_shape_70(self):
        """
        select Foo {
            optional union := 1,
            optional except := 1,
            optional intersect := 1
        };
        """

    def test_edgeql_syntax_shape_71(self):
        """
        select Foo {
            single union := 1,
            single except := 1,
            single intersect := 1
        };
        """

    def test_edgeql_syntax_shape_72(self):
        """
        select Foo {
            multi union := 1,
            multi except := 1,
            multi intersect := 1
        };
        """

    def test_edgeql_syntax_struct_01(self):
        """
        SELECT (
            foo := 1,
            bar := 2
        );
        """

    def test_edgeql_syntax_struct_02(self):
        """
        SELECT (
            foo := (
                foobaz := 1,
                foobiz := 2,
            ),
            bar := 3
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected ':'",
                  line=3, col=16)
    def test_edgeql_syntax_struct_03(self):
        """
        SELECT (
            foo: 1,
            bar := 3
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected ':'",
                  line=3, col=16)
    def test_edgeql_syntax_struct_04(self):
        """
        SELECT (
            foo: (
                bar: 42
            )
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected ':'",
                  line=3, col=16)
    def test_edgeql_syntax_struct_05(self):
        """
        SELECT (
            foo: (
                'bar': 42
            )
        );
        """

    def test_edgeql_syntax_struct_06(self):
        """
        SELECT (
            foo := ['bar']
        );
        """

    def test_edgeql_syntax_struct_07(self):
        """
        WITH
            # unreserved keywords
            abort := 'abort',
            abstract := 'abstract',
            action := 'action',
            declare := 'declare',
            empty := 'empty',
            order := 'order',
            populate := 'populate',
            release := 'release',
            reset := 'reset'
        SELECT 1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected keyword 'if'", line=4, col=13)
    def test_edgeql_syntax_struct_08(self):
        """
        SELECT (
            # reserved keywords
            if := 1,
            select := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected ':='", line=4, col=20)
    def test_edgeql_syntax_struct_09(self):
        """
        SELECT (
            # reserved keywords
            select := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected ':='", line=2, col=22)
    def test_edgeql_syntax_struct_10(self):
        """
        SELECT (1, a := 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected '2'", line=2, col=25)
    def test_edgeql_syntax_struct_11(self):
        """
        SELECT (a := 1, 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\)'", line=2, col=28)
    def test_edgeql_syntax_struct_12(self):
        """
        SELECT (a := 1, foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\.'", line=2, col=28)
    def test_edgeql_syntax_struct_13(self):
        """
        SELECT (a := 1, foo.bar);
        """

    def test_edgeql_syntax_path_01(self):
        """
        SELECT Foo.bar;
        SELECT Foo.<bar;
        SELECT Foo.bar@spam;
        SELECT Foo.<bar@spam;
        SELECT Foo.bar[IS Baz];
        SELECT Foo.<bar[IS Baz];
        SELECT Foo.<var[IS Baz][IS Spam].bar[IS Foo];

% OK %

        SELECT Foo.bar;
        SELECT Foo.<bar;
        SELECT Foo.bar@spam;
        SELECT Foo.<bar@spam;
        SELECT Foo.bar[IS Baz];
        SELECT Foo.<bar[IS Baz];
        SELECT Foo.<var[IS Baz][IS Spam].bar[IS Foo];
        """

    def test_edgeql_syntax_path_02(self):
        """
        SELECT Foo.event;
        SELECT Foo.<event;
        SELECT Foo.event@action;
        SELECT Foo.<event@action;
        SELECT Foo.event[IS Action];
        SELECT Foo.<event[IS Action];

% OK %

        SELECT Foo.event;
        SELECT Foo.<event;
        SELECT Foo.event@action;
        SELECT Foo.<event@action;
        SELECT Foo.event[IS Action];
        SELECT Foo.<event[IS Action];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=23)
    def test_edgeql_syntax_path_03(self):
        """
        SELECT Foo.lib::bar;
        """

    def test_edgeql_syntax_path_04(self):
        """
        SELECT Foo[IS Bar];
        """

    def test_edgeql_syntax_path_05(self):
        """
        SELECT Foo.bar@spam[IS Bar];
        """

    def test_edgeql_syntax_path_06(self):
        """
        SELECT Foo.bar[IS To];  # unreserved keyword as type name
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=30)
    def test_edgeql_syntax_path_07(self):
        """
        SELECT Foo.bar[IS To To];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=27)
    def test_edgeql_syntax_path_08(self):
        """
        SELECT Foo.bar[IS Case];
        """

    def test_edgeql_syntax_path_09(self):
        """
        SELECT Foo.bar[2][IS Baz];

% OK %

        SELECT ((Foo.bar)[2])[IS Baz];
        """

    def test_edgeql_syntax_path_10(self):
        """
        SELECT (Foo.bar)[2:4][IS Baz];
% OK %

        SELECT ((Foo.bar)[2:4])[IS Baz];
        """

    def test_edgeql_syntax_path_11(self):
        """
        SELECT (Foo.bar)[2:][IS Baz];
% OK %

        SELECT ((Foo.bar)[2:])[IS Baz];
        """

    def test_edgeql_syntax_path_12(self):
        """
        SELECT (Foo.bar)[:2][IS Baz];
% OK %

        SELECT ((Foo.bar)[:2])[IS Baz];
        """

    def test_edgeql_syntax_path_13(self):
        """
        SELECT (Foo.bar)[IS Baz];
        SELECT Foo.bar[IS Baz];
        SELECT Foo.<bar[IS Baz];

% OK %

        SELECT Foo.bar[IS Baz];
        SELECT Foo.bar[IS Baz];
        SELECT Foo.<bar[IS Baz];
        """

    def test_edgeql_syntax_path_14(self):
        """
        SELECT User.__type__.name LIMIT 1;
        """

    def test_edgeql_syntax_path_15(self):
        """
        SELECT (42).foo;
% OK %
        SELECT (42).foo;
        """

    def test_edgeql_syntax_path_16(self):
        # illegal semantically, but syntactically valid
        """
        SELECT .foo;
        SELECT .<foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected '.'", line=2)
    def test_edgeql_syntax_path_17(self):
        """
        SELECT ..foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected keyword '__source__'",
                  line=2, col=20)
    def test_edgeql_syntax_path_18(self):
        """
        SELECT Foo.__source__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected keyword '__subject__'",
                  line=2, col=20)
    def test_edgeql_syntax_path_19(self):
        """
        SELECT Foo.__subject__;
        """

    def test_edgeql_syntax_path_20(self):
        # illegal semantically, but syntactically valid
        """
        SELECT __subject__;
        SELECT __source__;
        """

    def test_edgeql_syntax_path_21(self):
        # legal when `TUP` is a tuple
        """
        SELECT TUP.0;
        SELECT TUP.0.name;
        SELECT Foo.TUP.0.name;

        SELECT TUP.0.1;
        SELECT TUP.0.1.name;
        SELECT Foo.TUP.0.1.name;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "unexpected char 'e', only integers are allowed after dot",
                  line=2, col=22)
    def test_edgeql_syntax_path_22(self):
        """
        SELECT TUP.0.2e2;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected keyword '__type__'",
                  line=2, col=16)
    def test_edgeql_syntax_path_23(self):
        """
        SELECT __type__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected keyword '__type__'",
                  line=2, col=24)
    def test_edgeql_syntax_path_24(self):
        """
        SELECT Foo.bar@__type__;
        """

    def test_edgeql_syntax_path_25(self):
        # illegal semantically, but syntactically valid
        """
        SELECT Foo.bar[IS array<int>];
        SELECT Foo.bar[IS int64];
        SELECT Foo.bar[IS tuple<array<int>, str>];
        """

    def test_edgeql_syntax_path_26(self):
        # legal when `TUP` is a tuple
        """
        SELECT TUP.0;
        SELECT TUP.0.name;
        SELECT TUP.0.1.name;
        SELECT TUP.0.1.n;
        SELECT Foo.TUP.0.name;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "unexpected char 'n', only integers are allowed after dot",
                  line=2, col=22)
    def test_edgeql_syntax_path_27(self):
        """
        SELECT TUP.0.1n.2;
        """

    def test_edgeql_syntax_path_28(self):
        """
        SELECT TUP.1.1;
        """

    def test_edgeql_syntax_path_29(self):
        # legal when `$0`, `$1`, `$a` and `$abc` are tuples
        """
        SELECT $0.0;
        SELECT $0.0.name;
        SELECT $0.0.1.name;
        SELECT $0.0.1.n;
        SELECT $abc.0;
        SELECT $abc.0.name;
        SELECT $abc.0.1.name;
        SELECT $abc.0.1.n;
        """

    def test_edgeql_syntax_path_30(self):
        # legal when `$0`, `$1`, `$a` and `$abc` are tuples
        """
        SELECT $1.1.1;
        SELECT $a.1.1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"bare \$ is not allowed",
                  line=2, col=16)
    def test_edgeql_syntax_path_31(self):
        """
        SELECT $ a;
        """

    def test_edgeql_syntax_path_32(self):
        """
        select Foo.union.except.intersect;
        select Foo.<union[is Foo].<except[is Foo].<intersect[is Foo];
        """

    def test_edgeql_syntax_path_33(self):
        """
        select Foo.bar@union;
        select Foo.bar@except;
        select Foo.bar@intersect;
        """

    def test_edgeql_syntax_type_interpretation_01(self):
        """
        SELECT Foo[IS Bar].spam;
        SELECT Foo[IS Bar].<ham;
        """

    def test_edgeql_syntax_type_interpretation_02(self):
        """
        SELECT (Foo + Bar)[IS Spam].ham;
% OK %
        SELECT ((Foo + Bar))[IS Spam].ham;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=18)
    def test_edgeql_syntax_map_03(self):
        """
        SELECT [
            'foo':= {
                bar := 42
            }
        ];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected '->'",
                  line=2, col=24)
    def test_edgeql_syntax_map_05(self):
        """
        SELECT [1, 2, 1->2, 3];
        """

    def test_edgeql_syntax_sequence_01(self):
        """
        SELECT (User.name);  # not a sequence
        SELECT (User.name,);
        SELECT (User.name, User.age, 'comment');
        SELECT (User.name, User.age, 'comment',);
        SELECT (User.name != 'Alice', User.age < 42, 'comment');

% OK %

        SELECT User.name;
        SELECT (User.name,);
        SELECT (User.name, User.age, 'comment');
        SELECT (User.name, User.age, 'comment');
        SELECT ((User.name != 'Alice'), (User.age < 42), 'comment');
        """

    def test_edgeql_syntax_array_01(self):
        """
        SELECT [1];
        SELECT [1, 2, 3, 4, 5];
        SELECT [User.name, User.description];
        SELECT [User.name, User.description, 'filler'];
        """

    def test_edgeql_syntax_array_02(self):
        """
        SELECT [1, 2, 3, 4, 5][2];
        SELECT [1, 2, 3, 4, 5][2:4];

% OK %

        SELECT ([1, 2, 3, 4, 5])[2];
        SELECT ([1, 2, 3, 4, 5])[2:4];
        """

    def test_edgeql_syntax_array_03(self):
        """
        SELECT ([1, 2, 3, 4, 5])[2];
        SELECT ([1, 2, 3, 4, 5])[2:4];
        SELECT ([1, 2, 3, 4, 5])[2:];
        SELECT ([1, 2, 3, 4, 5])[:2];
        SELECT ([1, 2, 3, 4, 5])[2:-1];
        SELECT ([1, 2, 3, 4, 5])[-2:];
        SELECT ([1, 2, 3, 4, 5])[:-2];
        """

    def test_edgeql_syntax_array_04(self):
        """
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[Bar.setting];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[1:Bar.setting];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[Bar.setting:];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[:Bar.setting];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[:-Bar.setting];
        """

    def test_edgeql_syntax_array_05(self):
        """
        SELECT (get_nested_obj())['a']['b']['c'];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+User',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before 'User'",
                  line=4, col=13)
    def test_edgeql_syntax_array_06(self):
        """
        SELECT [
            1
            User
        ];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+1',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before '1'",
                  line=4, col=13)
    def test_edgeql_syntax_array_07(self):
        """
        SELECT [
            User
            1
        ];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+True',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before 'True'",
                  line=4, col=13)
    def test_edgeql_syntax_array_08(self):
        """
        SELECT [
            False
            True
        ];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+b',
                  hint=r'''It appears that a ',' is missing in an array '''
                       r'''before "'b'"''',
                  line=4, col=13)
    def test_edgeql_syntax_array_09(self):
        """
        SELECT [
            'a'
            'b'
        ];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+x',
                  hint=r"It appears that a ':' is missing in an array "
                       r"slice before 'x'",
                  line=3, col=28)
    def test_edgeql_syntax_array_10(self):
        """
        WITH x := 2
        SELECT [1, 2, 3][1 x];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+1',
                  hint=r"It appears that a ':' is missing in an array "
                       r"slice before '1'",
                  line=3, col=28)
    def test_edgeql_syntax_array_11(self):
        """
        WITH x := 2
        SELECT [1, 2, 3][x 1];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before '2'",
                  line=2, col=31)
    def test_edgeql_syntax_array_12(self):
        """
        SELECT [1, 2, 3][x (1 2).1];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\('", line=2, col=24)
    def test_edgeql_syntax_array_13(self):
        """
        SELECT [(1, 2) (2, 3)];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\('", line=2, col=24)
    def test_edgeql_syntax_array_14(self):
        """
        SELECT [([1],) ([2],)];
        """

    def test_edgeql_syntax_cast_01(self):
        """
        SELECT <float64> (SELECT User.age);
        """

    def test_edgeql_syntax_cast_02(self):
        """
        SELECT <float64> (((SELECT User.age)));

% OK %

        SELECT <float64> (SELECT User.age);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\{'", line=3, col=19)
    def test_edgeql_syntax_cast_03(self):
        """
        SELECT
            <User {name, description}> [
                'name' -> 'Alice',
                'description' -> 'sample'
            ];
        """

    def test_edgeql_syntax_cast_04(self):
        """
        SELECT -<int64>{};
        """

    def test_edgeql_syntax_cast_05(self):
        """
        SELECT <array<int64>>$1;
        SELECT <std::array<std::str>>$1;
        SELECT <optional std::array<std::str>>$1;
        """

    def test_edgeql_syntax_cast_07(self):
        """
        SELECT <tuple<Foo, int, str>>$1;
        SELECT <std::tuple<obj: Foo, count: int, name: str>>$1;
        """

    def test_edgeql_syntax_cast_08(self):
        """
        SELECT <array<int64,>>$1;
        SELECT <std::array<std::str,>>$1;

% OK %

        SELECT <array<int64>>$1;
        SELECT <std::array<std::str>>$1;
        """

    def test_edgeql_syntax_cast_09(self):
        """
        SELECT <tuple<Foo, int, str,>>$1;
        SELECT <std::tuple<obj: Foo, count: int, name: str,>>$1;

% OK %

        SELECT <tuple<Foo, int, str>>$1;
        SELECT <std::tuple<obj: Foo, count: int, name: str>>$1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "must have at least one argument",
                  line=2, col=22)
    def test_edgeql_syntax_cast_10(self):
        """
        SELECT <tuple<>>$1;
        """

    def test_edgeql_syntax_with_01(self):
        """
        WITH
            extra AS MODULE lib.extra,
            foo := Bar.foo,
            baz := (SELECT extra::Foo.baz)
        SELECT Bar {
            spam,
            ham := baz
        } FILTER (foo = 'special');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=5, col=9)
    def test_edgeql_syntax_with_02(self):
        """
        WITH
            foo := Bar.foo,
            baz := (SELECT Foo.baz)
        COMMIT;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=16)
    def test_edgeql_syntax_with_03(self):
        """
        WITH MODULE welp
        CREATE DATABASE sample;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=14)
    def test_edgeql_syntax_with_04(self):
        """
        WITH MODULE welp
        DROP DATABASE sample;
        """

    def test_edgeql_syntax_with_06(self):
        """
        WITH MODULE abstract SELECT Foo;
        WITH MODULE all SELECT Foo;
        WITH MODULE all.abstract.bar SELECT Foo;
        """

    def test_edgeql_syntax_with_07(self):
        """
        WITH MODULE `all.abstract.bar` SELECT Foo;

% OK %

        WITH MODULE all.abstract.bar SELECT Foo;
        """

    def test_edgeql_syntax_with_08(self):
        """
        WITH MODULE `~all.abstract.bar` SELECT Foo;
        """

    def test_edgeql_syntax_with_09(self):
        # trailing commas in WITH block
        """
        WITH MODULE foo, SELECT Bar;
        WITH
            MODULE foo,
            x := {1, 2, 3},
        SELECT Bar;
        WITH
            x := {1, 2, 3},
            MODULE foo,
        SELECT Bar;

% OK %

        WITH MODULE foo SELECT Bar;
        WITH
            MODULE foo,
            x := {1, 2, 3}
        SELECT Bar;
        WITH
            x := {1, 2, 3},
            MODULE foo
        SELECT Bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_with_10(self):
        """
        WITH MODULE __std__ SELECT Foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=26)
    def test_edgeql_syntax_with_11(self):
        """
        WITH a AS MODULE __std__ SELECT Foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=26)
    def test_edgeql_syntax_with_12(self):
        """
        WITH a AS MODULE `__std__` SELECT Foo;
        """

    def test_edgeql_syntax_detached_01(self):
        """
        WITH F := DETACHED Foo
        SELECT F;
        """

    def test_edgeql_syntax_detached_02(self):
        """
        WITH F := DETACHED (SELECT Foo FILTER Bar)
        SELECT F;
        """

    def test_edgeql_syntax_detached_03(self):
        """
        SELECT (DETACHED Foo, Foo);
        """

    def test_edgeql_syntax_detached_04(self):
        """
        SELECT DETACHED Foo.bar;

% OK %

        SELECT (DETACHED Foo).bar;
        """

    def test_edgeql_syntax_detached_05(self):
        """
        SELECT DETACHED mod::Foo.bar;

% OK %

        SELECT (DETACHED mod::Foo).bar;
        """

    def test_edgeql_syntax_select_01(self):
        """
        SELECT 42;
        SELECT User{name};
        SELECT User{name}
            FILTER (User.age > 42);
        SELECT User{name}
            ORDER BY User.name ASC;
        SELECT User{name}
            OFFSET 2;
        SELECT User{name}
            LIMIT 5;
        SELECT User{name}
            OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select_02(self):
        """
        SELECT User{name} ORDER BY User.name;
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name DESC;

% OK %

        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name DESC;
        """

    def test_edgeql_syntax_select_03(self):
        """
        SELECT User{name, age} ORDER BY User.name THEN User.age;
        SELECT User{name, age} ORDER BY User.name THEN User.age DESC;
        SELECT User{name, age} ORDER BY User.name ASC THEN User.age DESC;
        SELECT User{name, age} ORDER BY User.name DESC THEN User.age ASC;

% OK %

        SELECT User{name, age} ORDER BY User.name ASC THEN User.age ASC;
        SELECT User{name, age} ORDER BY User.name ASC THEN User.age DESC;
        SELECT User{name, age} ORDER BY User.name ASC THEN User.age DESC;
        SELECT User{name, age} ORDER BY User.name DESC THEN User.age ASC;
        """

    def test_edgeql_syntax_select_04(self):
        """
        SELECT
            User.name
        FILTER
            (User.age > 42)
        ORDER BY
            User.name ASC
        OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select_05(self):
        """
        SELECT 42;
        SELECT User{name};
        SELECT User{name}
            FILTER (User.age > 42);
        SELECT User{name}
            ORDER BY User.name ASC;
        SELECT User{name}
            OFFSET 2;
        SELECT User{name}
            LIMIT 5;
        SELECT User{name}
            OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select_06(self):
        """
        SELECT
            User.name
        FILTER
            (User.age > 42)
        ORDER BY
            User.name ASC
        OFFSET 2 LIMIT 5;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=9)
    def test_edgeql_syntax_select_07(self):
        """
        (SELECT User.name) OFFSET 2;
        """

    def test_edgeql_syntax_select_08(self):
        """
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} OFFSET 2;
        SELECT User{name} LIMIT 2;
        SELECT User{name} OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select_09(self):
        """
        SELECT Issue {name} ORDER BY Issue.priority.name ASC EMPTY FIRST;
        SELECT Issue {name} ORDER BY Issue.priority.name DESC EMPTY LAST;
        """

    def test_edgeql_syntax_select_10(self):
        """
        SELECT User.name OFFSET $1;
        SELECT User.name LIMIT $2;
        SELECT User.name OFFSET $1 LIMIT $2;
        """

    def test_edgeql_syntax_select_11(self):
        """
        SELECT User.name OFFSET Foo.bar;
        SELECT User.name LIMIT (Foo.bar * 10);
        SELECT User.name OFFSET Foo.bar LIMIT (Foo.bar * 10);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected.+bar', hint=None, line=3, col=24)
    def test_edgeql_syntax_select_12(self):
        """
        SELECT (
            SELECT Foo bar
        );
        """

    def test_edgeql_syntax_group_01(self):
        """
        GROUP User
        BY .name;
        """

    def test_edgeql_syntax_group_02(self):
        """
        # define and mask aliases
        WITH
            _1 := User
        GROUP _2 := _1
        USING _ :=  _2.name
        BY _;
        """

    def test_edgeql_syntax_group_03(self):
        """
        GROUP User := User
        USING G :=  User.name
        BY G;
        """

    def test_edgeql_syntax_group_04(self):
        """
        GROUP F := User.friends
        BY .name;
        """

    def test_edgeql_syntax_group_05(self):
        """
        GROUP
            User
        USING
            G1 := User.name,
            G2 := User.age,
            G3 := User.rank,
            G4 := User.status
        BY G1, G2, G3, G4;
        """

    def test_edgeql_syntax_group_06(self):
        """
        GROUP
            User
        BY
            .name,
            .age,
            .rank,
            .status;
        """

    def test_edgeql_syntax_group_07(self):
        """
        GROUP
            User
        USING
            letter := (.name)[0],
        BY
            letter,
            .age,
            .rank,
            .status;

% OK %

        GROUP
            User
        USING
            letter := (.name)[0]
        BY
            letter,
            .age,
            .rank,
            .status;
        """

    def test_edgeql_syntax_group_08(self):
        """
        GROUP
            User
        USING
            letter := (.name)[0]
        BY {letter, .age, ROLLUP(.rank, .status)};
        """

    def test_edgeql_syntax_group_09(self):
        """
        GROUP
            User
        USING
            letter := (.name)[0]
        BY CUBE(letter, .age, .rank, .status);
        """

    def test_edgeql_syntax_group_10(self):
        """
        GROUP
            User
        USING
            letter := (.name)[0]
        BY {letter, {.age, CUBE(.rank, .status)}};
        """

    def test_edgeql_syntax_group_11(self):
        """
        GROUP
            User
        BY
            (.name, .age);
        """

    def test_edgeql_syntax_group_12(self):
        """
        GROUP
            User
        BY
            {(.name, .age), (.rank, .status)};
        """

    def test_edgeql_syntax_group_13(self):
        """
        GROUP
            User
        BY
            ROLLUP((.name, .age), (.rank, .status));
        """

    def test_edgeql_syntax_set_01(self):
        """
        SELECT (1 UNION 2);
        """

    def test_edgeql_syntax_set_02(self):
        """
        SELECT ((SELECT Foo) UNION (SELECT Bar));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=9)
    def test_edgeql_syntax_set_03(self):
        """
        (SELECT Foo) UNION (SELECT Bar);
        """

    def test_edgeql_syntax_set_04(self):
        """
        SELECT 2 * (1 UNION 2 UNION 1);

% OK %

        SELECT (2 * ((1 UNION 2) UNION 1));
        """

    def test_edgeql_syntax_set_05(self):
        """
        SELECT {};
        SELECT {1};
        SELECT {1, 2};
        SELECT {1, 2, {}, {1, 3}};
        SELECT {Foo.bar, Foo.baz};
        SELECT {Foo.bar, Foo.baz}.spam;
        """

    def test_edgeql_syntax_set_06(self):
        """
        SELECT DISTINCT ({1, 2, 2, 3});
        """

    def test_edgeql_syntax_set_07(self):
        """
        SELECT ((1 UNION 2) UNION 3);
        """

    def test_edgeql_syntax_set_08(self):
        """
        SELECT 1 EXCEPT 2 EXCEPT 3;

% OK %

        SELECT ((1 EXCEPT 2) EXCEPT 3);
        """

    def test_edgeql_syntax_set_09(self):
        """
        SELECT 1 EXCEPT 2 UNION 3;

% OK %

        SELECT ((1 EXCEPT 2) UNION 3);
        """

    def test_edgeql_syntax_set_10(self):
        """
        SELECT (1 EXCEPT (2 UNION 3));
        """

    def test_edgeql_syntax_set_11(self):
        """
        SELECT 1 INTERSECT 2 INTERSECT 3;

% OK %

        SELECT ((1 INTERSECT 2) INTERSECT 3);
        """

    def test_edgeql_syntax_set_12(self):
        """
        SELECT 1 UNION 2 INTERSECT 3;

% OK %

        SELECT (1 UNION (2 INTERSECT 3));
        """

    def test_edgeql_syntax_set_13(self):
        """
        SELECT 1 INTERSECT 2 EXCEPT 3 INTERSECT 4 UNION 5;

% OK %

        SELECT (((1 INTERSECT 2) EXCEPT (3 INTERSECT 4)) UNION 5);
        """

    def test_edgeql_syntax_insert_01(self):
        """
        INSERT Foo;
        SELECT (INSERT Foo);
        SELECT (INSERT Foo) {bar};
        """

    def test_edgeql_syntax_insert_02(self):
        """
        INSERT Foo{bar := 42};
        SELECT (INSERT Foo{bar := 42});
        SELECT (INSERT Foo{bar := 42}) {bar};
        """

    def test_edgeql_syntax_insert_03(self):
        """
        INSERT Foo;
        """

    def test_edgeql_syntax_insert_04(self):
        """
        INSERT Foo{bar := 42};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'insert expression must be an object type reference',
                  line=2, col=16)
    def test_edgeql_syntax_insert_05(self):
        """
        INSERT 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert_06(self):
        """
        INSERT Foo FILTER Foo.bar = 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert_07(self):
        """
        INSERT Foo GROUP BY Foo.bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert_08(self):
        """
        INSERT Foo ORDER BY Foo.bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert_09(self):
        """
        INSERT Foo OFFSET 2;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert_10(self):
        """
        INSERT Foo LIMIT 5;
        """

    def test_edgeql_syntax_insert_13(self):
        """
        INSERT Foo{
            bar := 42,
            baz := (SELECT Baz FILTER (Baz.spam = 'ham'))
        };
        """

    def test_edgeql_syntax_insert_15(self):
        """
        INSERT Foo{
            bar := 42,
            baz := 'spam' {
                @weight := 2,
            }
        };

        INSERT Foo{
            bar := 42,
            baz := 24 {
                @weight := 2,
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=18)
    def test_edgeql_syntax_insert_16(self):
        """
        INSERT Foo{
            bar := 42,
            baz: 'spam' {
                @weight := 2,
            }
        };
        """

    def test_edgeql_syntax_insert_17(self):
        """
        INSERT Foo{
            bar := 42,
            baz := (
                SELECT Baz{
                    @weight := 2
                } FILTER (Baz.spam = 'ham')
            )
        };
        """

    def test_edgeql_syntax_insert_18(self):
        """
        INSERT Foo {
            bar := 42,
        } UNLESS CONFLICT;
        """

    def test_edgeql_syntax_insert_19(self):
        """
        INSERT Foo {
            bar := 42,
        } UNLESS CONFLICT ON .bar;
        """

    def test_edgeql_syntax_insert_20(self):
        """
        INSERT Foo {
            bar := 42,
        } UNLESS CONFLICT ON .bar
        ELSE (SELECT Foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=27)
    def test_edgeql_syntax_insert_21(self):
        """
        INSERT Foo {
            bar := 42,
        } UNLESS CONFLICT ELSE (SELECT Foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected.+bar', hint=None, line=3, col=24)
    def test_edgeql_syntax_insert_22(self):
        """
        SELECT (
            INSERT Foo bar
        );
        """

    def test_edgeql_syntax_delete_01(self):
        """
        DELETE Foo;
        """

    def test_edgeql_syntax_delete_02(self):
        """
        DELETE Foo;
        """

    def test_edgeql_syntax_delete_03(self):
        # NOTE: this must be rejected by the compiler
        """
        DELETE 42;
        """

    def test_edgeql_syntax_delete_04(self):
        # this is legal and equivalent to DELETE Foo;
        """
        DELETE Foo{bar};
        """

    def test_edgeql_syntax_delete_05(self):
        """
        DELETE
            User.name
        FILTER
            (User.age > 42)
        ORDER BY
            User.name ASC
        OFFSET 2 LIMIT 5;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected.+bar', hint=None, line=3, col=24)
    def test_edgeql_syntax_delete_06(self):
        """
        SELECT (
            DELETE Foo bar
        );
        """

    def test_edgeql_syntax_update_01(self):
        """
        UPDATE Foo SET {bar := 42};
        UPDATE Foo FILTER (Foo.bar = 24) SET {bar := 42};
        """

    def test_edgeql_syntax_update_02(self):
        """
        UPDATE Foo SET {bar := 42};
        UPDATE Foo FILTER (Foo.bar = 24) SET {bar := 42};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected ';'",
                  line=2, col=18)
    def test_edgeql_syntax_update_03(self):
        """
        UPDATE 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=19)
    def test_edgeql_syntax_update_04(self):
        """
        UPDATE Foo;
        """

    def test_edgeql_syntax_update_07(self):
        """
        UPDATE Foo
        FILTER (Foo.bar = 24)
        SET {
            bar := 42,
            baz := 'spam',
            ham: {
                taste := 'yummy'
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected.+bad', hint=None, line=4, col=25)
    def test_edgeql_syntax_update_08(self):
        """
        WITH x := (
            UPDATE Foo
            FILTER .bar bad = 24
            SET {
                bar := 42,
            };
        )
        SELECT x
        """

    def test_edgeql_syntax_insertfor_01(self):
        """
        FOR name IN {'a', 'b', 'c'}
        UNION (INSERT User{name := name});

        FOR name IN {'a', 'b', Foo.bar, Foo.baz}
        UNION (INSERT User{name := name});
        """

    def test_edgeql_syntax_insertfor_02(self):
        """
        FOR name IN {'a' UNION 'b' UNION 'c'}
        UNION (INSERT User{name := name});

% OK %

        FOR name IN {(('a' UNION 'b') UNION 'c')}
        UNION (INSERT User{name := name});
        """

    def test_edgeql_syntax_insertfor_03(self):
        """
        FOR name IN {(SELECT Foo.bar FILTER (Foo.bar.baz = TRUE))}
        UNION (INSERT Foo{name := name});
        """

    def test_edgeql_syntax_insertfor_04(self):
        """
        FOR bar IN {(INSERT Bar{name := 'bar'})}
        UNION (INSERT Foo{name := bar.name});
        """

    def test_edgeql_syntax_insertfor_05(self):
        """
        FOR bar IN {(DELETE Bar)}
        UNION (INSERT Foo{name := bar.name});
        """

    def test_edgeql_syntax_insertfor_06(self):
        """
        FOR bar IN {(
            UPDATE Bar SET {name := (name ++ 'bar')}
        )}
        UNION (INSERT Foo{name := bar.name});
        """

    def test_edgeql_syntax_selectfor_01(self):
        """
        FOR x IN {(('Alice', 'White') UNION ('Bob', 'Green'))}
        UNION (
            SELECT User{first_tname, last_name, age}
            FILTER (
                (.first_name = x.0)
                AND
                (.last_name = x.1)
            )
        );
        """

    def test_edgeql_syntax_selectfor_02(self):
        """
        SELECT (FOR s IN array_unpack([1, 2, 3]) UNION s);
        """

    def test_edgeql_syntax_selectfor_03(self):
        """
        WITH x := (
            FOR s IN array_unpack([1, 2, 3]) UNION s
        )
        SELECT x;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected.+bad', hint=None, line=3, col=56)
    def test_edgeql_syntax_selectfor_04(self):
        """
        WITH x := (
            FOR s IN {array_unpack([1, 2, 3])} UNION s bad
        )
        SELECT x;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected ':='", hint=None, line=3, col=17)
    def test_edgeql_syntax_selectfor_05(self):
        """
        FOR x IN {1, 2, 3}
        UNION y := (x + 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Missing parentheses around complex expression in a '
                  'FOR iterator clause',
                  hint=None, line=2, col=18)
    def test_edgeql_syntax_selectfor_06(self):
        """
        FOR x in DETACHED foo UNION x;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Missing parentheses around complex expression in a '
                  'FOR iterator clause',
                  hint=None, line=2, col=18)
    def test_edgeql_syntax_selectfor_08(self):
        """
        FOR x in foo + bar UNION x;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Missing parentheses around complex expression in a '
                  'FOR iterator clause',
                  hint=None, line=2, col=18)
    def test_edgeql_syntax_selectfor_09(self):
        """
        FOR x in foo.bar + bar UNION x;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Missing parentheses around complex expression in a '
                  'FOR iterator clause',
                  hint=None, line=2, col=18)
    def test_edgeql_syntax_selectfor_10(self):
        """
        FOR x in foo { x } UNION x;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Missing parentheses around complex expression in a '
                  'FOR iterator clause',
                  hint=None, line=2, col=18)
    def test_edgeql_syntax_selectfor_11(self):
        """
        FOR x in SELECT 1 UNION x;
        """

    def test_edgeql_syntax_selectfor_12(self):
        """
        FOR x in Foo UNION x;
        """

    def test_edgeql_syntax_selectfor_13(self):
        """
        FOR x in Foo.bar UNION x;
        """

    def test_edgeql_syntax_selectfor_14(self):
        """
        FOR x in (SELECT 1) UNION x;
        """

    def test_edgeql_syntax_selectfor_15(self):
        """
        FOR x in [1,2,3] UNION x;
        """

    def test_edgeql_syntax_selectfor_16(self):
        """
        FOR x in (1,2,3) UNION x;
        """

    def test_edgeql_syntax_selectfor_17(self):
        """
        FOR x in .test UNION x;
        """

    def test_edgeql_syntax_selectfor_18(self):
        """
        FOR x in ({1,2} + {3,4}) UNION x;
        """

    def test_edgeql_syntax_selectfor_19(self):
        """
        FOR x in <datetime>'1999-03-31T15:17:00Z' UNION x;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Missing parentheses around complex expression in a '
                  'FOR iterator clause',
                  hint=None, line=2, col=18)
    def test_edgeql_syntax_selectfor_20(self):
        """
        FOR x in <datetime>'1999-03-31T15:17:00Z'++'' UNION x;
        """

    def test_edgeql_syntax_deletefor_01(self):
        """
        FOR x IN {(('Alice', 'White') UNION ('Bob', 'Green'))}
        UNION (
            DELETE (
                SELECT User
                FILTER (
                    (.first_name = x.0)
                    AND
                    (.last_name = x.1)
                )
            )
        );
        """

    def test_edgeql_syntax_updatefor_01(self):
        """
        FOR x IN {((1, 'a') UNION (2, 'b'))}
        UNION (UPDATE Foo FILTER (Foo.id = x.0) SET {bar := x.1});
        """

    def test_edgeql_syntax_coalesce_01(self):
        """
        SELECT (a ?? x);
        SELECT (a ?? x.a);
        SELECT (a ?? x.a[IS ABC]);
        SELECT ((a ?? x.a[IS ABC]@aaa) + 1);
        """

    def test_edgeql_syntax_function_01(self):
        """
        SELECT foo();
        SELECT bar(User.name);
        SELECT baz(User.name, User.age);
        SELECT str_lower(User.name);
        """

    def test_edgeql_syntax_function_02(self):
        """
        SELECT str_lower(string := User.name);
        SELECT baz(age := User.age, of := User.name, `select` := 1);
        """

    def test_edgeql_syntax_function_03(self):
        """
        SELECT some_agg(User.name ORDER BY User.age ASC);
        SELECT some_agg(User.name
                        FILTER (strlen(User.name) > 2)
                        ORDER BY User.age DESC);
        SELECT some_agg(User.name
                        FILTER (strlen(User.name) > 2)
                        ORDER BY User.age DESC THEN User.email ASC);
        SELECT some_agg(
            Post.title ORDER BY Post.date ASC,
            User.name
            FILTER (strlen(User.name) > 2)
            ORDER BY User.age DESC THEN User.email ASC
        );
        """

    # NOTE: this test is a remnant of an attempt to define syntax for
    # window functions. It may become valid again.
    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected keyword 'OVER'", line=2, col=36)
    def test_edgeql_syntax_function_04(self):
        """
        SELECT some_agg(User.name) OVER (ORDER BY User.age ASC);
        SELECT some_agg(User.name) OVER (
            PARTITION BY strlen(User.name)
            ORDER BY User.age ASC);
        SELECT some_agg(User.name) OVER (
            PARTITION BY User.email, User.age
            ORDER BY User.age ASC);
        SELECT some_agg(User.name) OVER (
            PARTITION BY User.email, User.age
            ORDER BY User.age ASC THEN User.name ASC);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+1',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before '1'",
                  line=2, col=26)
    def test_edgeql_syntax_function_05(self):
        """
        SELECT count(ALL 1);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"positional argument after named argument `b`",
                  line=2, col=41)
    def test_edgeql_syntax_function_06(self):
        """
        SELECT count(1, a := 1, b := 1, 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"duplicate named argument `a`",
                  line=2, col=33)
    def test_edgeql_syntax_function_07(self):
        """
        SELECT count(1, a := 1, a := 1);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"named arguments do not need a '\$' prefix, "
                  r"rewrite as 'a := \.\.\.'",
                  line=2, col=25)
    def test_edgeql_syntax_function_08(self):
        """
        SELECT count(1, $a := 1);
        """

    def test_edgeql_syntax_function_09(self):
        """
        SELECT bar(User.name,);
        SELECT baz(User.name, User.age,);
        SELECT str_lower(string := User.name,);
        SELECT baz(age := User.age, of := User.name, `select` := 1,);

% OK %

        SELECT bar(User.name);
        SELECT baz(User.name, User.age);
        SELECT str_lower(string := User.name);
        SELECT baz(age := User.age, of := User.name, `select` := 1);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+User',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before 'User'",
                  line=2, col=22)
    def test_edgeql_syntax_function_10(self):
        """
        SELECT foo(1 User);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+y',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before 'y'",
                  line=2, col=34)
    def test_edgeql_syntax_function_11(self):
        """
        SELECT baz(x := User.age y := User.name);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=22)
    def test_edgeql_syntax_function_12(self):
        """
        SELECT count(SELECT 1);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=22)
    def test_edgeql_syntax_function_13(self):
        """
        SELECT count(INSERT Foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=22)
    def test_edgeql_syntax_function_14(self):
        """
        SELECT count(UPDATE Foo SET {bar := 1});
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=22)
    def test_edgeql_syntax_function_15(self):
        """
        SELECT count(DELETE Foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=22)
    def test_edgeql_syntax_function_16(self):
        """
        SELECT count(FOR X IN {Foo} UNION X);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=22)
    def test_edgeql_syntax_function_17(self):
        """
        SELECT count(WITH X := 1 SELECT Foo FILTER .bar = X);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=23)
    def test_edgeql_syntax_function_18(self):
        """
        SELECT (count(SELECT 1) 1);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token',
                  hint=r"Missing parentheses around statement used "
                       r"as an expression",
                  line=2, col=26)
    def test_edgeql_syntax_function_19(self):
        """
        SELECT ((((count(SELECT 1)))));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+1',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before '1'",
                  line=2, col=30)
    def test_edgeql_syntax_function_20(self):
        """
        SELECT ((((count(foo 1)))));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+bar',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before 'bar'",
                  line=2, col=35)
    def test_edgeql_syntax_function_21(self):
        """
        SELECT ((((count(foo, 1)) bar)));
        """

    def test_edgeql_syntax_function_22(self):
        """
        SELECT count((((((((((SELECT 1))))))))));

% OK %

        SELECT count((SELECT 1));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '2'",
                  line=2, col=35)
    def test_edgeql_syntax_function_23(self):
        """
        SELECT (count((SELECT 1)) 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before '2'",
                  line=2, col=35)
    def test_edgeql_syntax_function_24(self):
        """
        SELECT [count((SELECT 1)) 2];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before '2'",
                  line=2, col=29)
    def test_edgeql_syntax_function_25(self):
        """
        SELECT count((0, 1) 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+foo',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before 'foo'",
                  line=2, col=29)
    def test_edgeql_syntax_function_26(self):
        """
        SELECT count((0, 1) foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before '2'",
                  line=2, col=29)
    def test_edgeql_syntax_function_27(self):
        """
        SELECT count([0, 1] 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+foo',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before 'foo'",
                  line=2, col=29)
    def test_edgeql_syntax_function_28(self):
        """
        SELECT count([0, 1] foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before '2'",
                  line=2, col=28)
    def test_edgeql_syntax_function_29(self):
        """
        SELECT count(([1]) 2);
        """

    def test_edgeql_syntax_tuple_01(self):
        """
        SELECT ('foo', 42).0;
        SELECT ('foo', 42).1;
        """

    def test_edgeql_syntax_tuple_02(self):
        """
        SELECT (name := 'foo', val := 42).name;
        SELECT (name := 'foo', val := 42).val;
        """

    def test_edgeql_syntax_tuple_03(self):
        """
        SELECT ();
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+User',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before 'User'",
                  line=4, col=13)
    def test_edgeql_syntax_tuple_04(self):
        """
        SELECT (
            1
            User
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+1',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '1'",
                  line=4, col=13)
    def test_edgeql_syntax_tuple_05(self):
        """
        SELECT (
            User
            1
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+True',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before 'True'",
                  line=4, col=13)
    def test_edgeql_syntax_tuple_06(self):
        """
        SELECT (
            False
            True
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+b',
                  hint=r'''It appears that a ',' is missing in a tuple '''
                       r'''before "'b'"''',
                  line=4, col=13)
    def test_edgeql_syntax_tuple_07(self):
        """
        SELECT (
            'a'
            'b'
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '2'",
                  line=2, col=22)
    def test_edgeql_syntax_tuple_08(self):
        """
        SELECT ((((1 2))));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\('", line=2, col=24)
    def test_edgeql_syntax_tuple_09(self):
        """
        SELECT ((1, 2) (3, 4));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\('", line=2, col=19)
    def test_edgeql_syntax_tuple_10(self):
        """
        SELECT (0 (1, 2));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '\('", line=2, col=19)
    def test_edgeql_syntax_tuple_11(self):
        """
        SELECT (0 (((1 2) 3)) 4);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '2'",
                  line=2, col=25)
    def test_edgeql_syntax_tuple_12(self):
        """
        SELECT (0, (((1 2) 3)) 4);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+3',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '3'",
                  line=2, col=29)
    def test_edgeql_syntax_tuple_13(self):
        """
        SELECT (0, (((1, 2) 3)) 4);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+4',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '4'",
                  line=2, col=34)
    def test_edgeql_syntax_tuple_14(self):
        """
        SELECT (0, (((1, 2), 3)) 4);
        """

    def test_edgeql_syntax_tuple_15(self):
        """
        SELECT (0, (((1, 2), 3)), 4);

% OK %

        SELECT (0, ((1, 2), 3), 4);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '2'",
                  line=2, col=26)
    def test_edgeql_syntax_tuple_16(self):
        """
        SELECT (foo (((1 2) 3)) 4);
        """

    def test_edgeql_syntax_tuple_17(self):
        """
        SELECT ((((1, 2))));

% OK %

        SELECT (1, 2);
        """

    def test_edgeql_syntax_introspect_01(self):
        """
        SELECT INTROSPECT std::int64;
        """

    def test_edgeql_syntax_introspect_02(self):
        """
        SELECT INTROSPECT (tuple<str>);
        """

    def test_edgeql_syntax_introspect_03(self):
        """
        SELECT INTROSPECT TYPEOF '1';
        """

    def test_edgeql_syntax_introspect_04(self):
        """
        SELECT INTROSPECT TYPEOF (3 + 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected '>'",
                  line=2, col=38)
    def test_edgeql_syntax_introspect_05(self):
        """
        SELECT INTROSPECT tuple<int64>;
        """

    # DDL
    #

    def test_edgeql_syntax_ddl_database_01(self):
        """
        CREATE DATABASE mytestdb;
        DROP DATABASE mytestdb;
        CREATE DATABASE `mytest"db"`;
        DROP DATABASE `mytest"db"`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=25)
    def test_edgeql_syntax_ddl_database_02(self):
        """
        CREATE DATABASE (mytestdb);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=28)
    def test_edgeql_syntax_ddl_database_03(self):
        """
        CREATE DATABASE foo::mytestdb;
        """

    def test_edgeql_syntax_ddl_database_04(self):
        """
        CREATE DATABASE if;
        CREATE DATABASE abstract;

% OK %

        CREATE DATABASE `if`;
        CREATE DATABASE abstract;
        """

    def test_edgeql_syntax_ddl_database_05(self):
        """
        DROP DATABASE if;
        DROP DATABASE abstract;

% OK %

        DROP DATABASE `if`;
        DROP DATABASE abstract;
        """

    def test_edgeql_syntax_ddl_role_01(self):
        """
        CREATE ROLE username;
        CREATE SUPERUSER ROLE username;
        CREATE ROLE abstract;
        CREATE ROLE `mytest"role"`;
        CREATE ROLE `mytest"role"`
            EXTENDING delegated, `mytest"baserole"`;

% OK %

        CREATE ROLE username;
        CREATE SUPERUSER ROLE username;
        CREATE ROLE abstract;
        CREATE ROLE `mytest"role"`;
        CREATE ROLE `mytest"role"`
            EXTENDING delegated, `mytest"baserole"`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected keyword 'if'",
                  line=2, col=21)
    def test_edgeql_syntax_ddl_role_02(self):
        """
        CREATE ROLE if;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected '::'",
                  line=2, col=24)
    def test_edgeql_syntax_ddl_role_03(self):
        """
        CREATE ROLE foo::bar;
        """

    def test_edgeql_syntax_ddl_role_04(self):
        """
        DROP ROLE username;
        """

    def test_edgeql_syntax_ddl_role_05(self):
        """
        CREATE ROLE username EXTENDING generic {
            SET password := 'secret';
        };
        """

    def test_edgeql_syntax_ddl_role_06(self):
        """
        ALTER ROLE username {
            SET password := {};
            EXTENDING generic, morestuff;
        };
        """

    def test_edgeql_syntax_ddl_role_07(self):
        """
        ALTER ROLE username {
            RESET password;
            EXTENDING generic, morestuff;
        };
        """

    def test_edgeql_syntax_ddl_role_08(self):
        """
        CREATE ROLE username IF NOT EXISTS;
        CREATE SUPERUSER ROLE username IF NOT EXISTS;
        CREATE ROLE username EXTENDING generic IF NOT EXISTS;
        CREATE ROLE username EXTENDING generic IF NOT EXISTS {
            SET password := 'secret';
        };
        """

    def test_edgeql_syntax_ddl_delta_02(self):
        """
        START MIGRATION TO {type default::Foo;};
        ALTER MIGRATION m1231231231fd
            SET message := 'foo';
        COMMIT MIGRATION;
        """

    def test_edgeql_syntax_ddl_delta_03(self):
        """
        START MIGRATION TO {
            module test {
                type Foo;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected 'BadLang'", line=2, col=28)
    def test_edgeql_syntax_ddl_delta_04(self):
        """
        START MIGRATION TO BadLang $$type Foo$$;
        """

    def test_edgeql_syntax_ddl_delta_05(self):
        """
        START MIGRATION TO {
            type test::Foo {
                property bar -> str
            }
        };

% OK %

        START MIGRATION TO {
            type test::Foo {
                property bar -> str;
            };
        };
        """

    def test_edgeql_syntax_ddl_delta_06(self):
        """
        POPULATE MIGRATION;
        ABORT MIGRATION;
        COMMIT MIGRATION;
        DESCRIBE CURRENT MIGRATION AS JSON;
        ALTER CURRENT MIGRATION REJECT PROPOSED;
        """

    def test_edgeql_syntax_ddl_rewrite_01(self):
        """
        START MIGRATION REWRITE;
        ABORT MIGRATION REWRITE;
        COMMIT MIGRATION REWRITE;
        START MIGRATION TO COMMITTED SCHEMA;
        """

    def test_edgeql_syntax_ddl_create_migration_01(self):
        """
        CREATE MIGRATION {};

% OK %

        CREATE MIGRATION;
        """

    def test_edgeql_syntax_ddl_create_migration_02(self):
        """
        CREATE MIGRATION { ;;; CREATE TYPE Foo ;;; CREATE TYPE Bar ;;; };
        """

    def test_edgeql_syntax_ddl_create_migration_03(self):
        """
        CREATE MIGRATION {
            CREATE TYPE Foo;
        };
        """

    def test_edgeql_syntax_ddl_create_migration_04(self):
        """
        CREATE MIGRATION m123123123 {
            CREATE TYPE Foo;
        };
% OK %
        CREATE MIGRATION m123123123 ONTO initial {
            CREATE TYPE Foo;
        };
        """

    def test_edgeql_syntax_ddl_create_migration_05(self):
        """
        CREATE MIGRATION m123123123 ONTO m134134134 {
            CREATE TYPE Foo;
        };
        """

    def test_edgeql_syntax_ddl_create_migration_06(self):
        """
        CREATE APPLIED MIGRATION m123123123 ONTO m134134134 {
            CREATE TYPE Foo;
        };
        """

    def test_edgeql_syntax_ddl_create_migration_07(self):
        """
        START MIGRATION TO {
            using extension graphql version '2.0';
        };
        """

    def test_edgeql_syntax_ddl_create_migration_08(self):
        """
        START MIGRATION TO {
            using extension graphql;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "'using extension' cannot be used inside a module block",
                  line=4, col=17)
    def test_edgeql_syntax_ddl_create_migration_09(self):
        """
        START MIGRATION TO {
            module foo {
                using extension graphql;
            }
        };
        """

    def test_edgeql_syntax_ddl_create_migration_10(self):
        """
        CREATE APPLIED MIGRATION m123123123 ONTO m134134134 {
            WITH MODULE x CREATE TYPE Foo;
        };
        """

    def test_edgeql_syntax_ddl_create_extension_package_01(self):
        """
        CREATE EXTENSION PACKAGE foo VERSION '1.0';
        """

    def test_edgeql_syntax_ddl_create_extension_package_02(self):
        """
        CREATE EXTENSION PACKAGE foo VERSION '1.0' {
            ;;; CREATE TYPE Foo ;;; CREATE TYPE Bar ;;;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "invalid extension version format", line=2)
    def test_edgeql_syntax_ddl_create_extension_package_03(self):
        """
        CREATE EXTENSION PACKAGE foo VERSION 'aaa';
        """

    def test_edgeql_syntax_ddl_drop_extension_package_01(self):
        """
        DROP EXTENSION PACKAGE foo VERSION '1.0';
        """

    def test_edgeql_syntax_ddl_create_extension_01(self):
        """
        CREATE EXTENSION foo;
        """

    def test_edgeql_syntax_ddl_create_extension_02(self):
        """
        CREATE EXTENSION foo VERSION '1.0';
        """

    def test_edgeql_syntax_ddl_drop_extension_01(self):
        """
        DROP EXTENSION foo;
        """

    def test_edgeql_syntax_ddl_create_future_01(self):
        """
        CREATE FUTURE foo;
        """

    def test_edgeql_syntax_ddl_drop_future_01(self):
        """
        DROP FUTURE foo;
        """

        # TODO: remove this test once the entire grammar is converted
    def test_edgeql_syntax_ddl_aggregate_00(self):
        """
        CREATE FUNCTION std::sum(v: SET OF std::int64)
            -> std::int64
            USING SQL FUNCTION 'sum';
        """

    def test_edgeql_syntax_ddl_aggregate_01(self):
        """
        CREATE FUNCTION std::sum(v: SET OF std::int64)
            -> std::int64 {
            SET initial_value := 0;
            USING SQL FUNCTION 'test';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_02(self):
        """
        CREATE FUNCTION std::sum(arg: SET OF std::int64)
            -> std::int64 {
            SET initial_value := 0;
            USING SQL FUNCTION 'sum';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_03(self):
        """
        CREATE FUNCTION std::sum(integer: SET OF std::int64)
            -> std::int64 {
            SET initial_value := 0;
            USING SQL FUNCTION 'sum';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_04(self):
        """
        CREATE FUNCTION std::sum(integer: SET OF std::int64)
            -> std::int64 {
            SET initial_value := 0;
            USING SQL FUNCTION 'sum';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=5)
    def test_edgeql_syntax_ddl_aggregate_06(self):
        """
        CREATE FUNCTION foo(string: SET OF std::str)
            -> std::int64 {
            SET initial_value := 0;
            USING AAA FUNCTION 'foo';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_08(self):
        """
        CREATE FUNCTION std::count(expression: SET OF anytype)
            -> std::int64 {
            SET initial_value := 0;
            USING SQL FUNCTION 'count';
        };
        """

    def test_edgeql_syntax_ddl_scalar_01(self):
        """
        CREATE ABSTRACT SCALAR TYPE std::foo;
        CREATE SCALAR TYPE std::typeref;
        CREATE SCALAR TYPE std::scalarref EXTENDING std::typeref;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected keyword 'anytype'", line=2, col=28)
    def test_edgeql_syntax_ddl_scalar_02(self):
        """
        CREATE SCALAR TYPE anytype EXTENDING int64;
        """

    def test_edgeql_syntax_ddl_scalar_03(self):
        """
        CREATE SCALAR TYPE myenum EXTENDING enum<'foo', 'bar'>;
        """

    def test_edgeql_syntax_ddl_scalar_04(self):
        """
        CREATE SCALAR TYPE myenum EXTENDING enum<foo, bar>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "mixing string type literals and type names",
                  line=2, col=50)
    def test_edgeql_syntax_ddl_scalar_05(self):
        """
        CREATE SCALAR TYPE myenum EXTENDING enum<'foo', bar>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "mixing named and unnamed subtype",
                  line=2, col=50)
    def test_edgeql_syntax_ddl_scalar_06(self):
        """
        CREATE SCALAR TYPE myenum EXTENDING enum<baz: int64, bar>;
        """

    def test_edgeql_syntax_ddl_create_pseudo_type_01(self):
        """
        CREATE PSEUDO TYPE `anytype`;
        """

    def test_edgeql_syntax_ddl_annotation_01(self):
        """
        CREATE ABSTRACT ANNOTATION std::paramtypes;
        """

    def test_edgeql_syntax_ddl_annotation_02(self):
        """
        CREATE ABSTRACT INHERITABLE ANNOTATION std::paramtypes;
        """

    def test_edgeql_syntax_ddl_annotation_03(self):
        """
        DROP ABSTRACT ANNOTATION foo::my_annotation;
        """

    def test_edgeql_syntax_ddl_annotation_04(self):
        """
        ALTER ABSTRACT ANNOTATION foo::my_annotation
            RENAME TO foo::renamed_annotation;
        """

    def test_edgeql_syntax_ddl_annotation_05(self):
        # N.B: The multi line string literal here *must* get turned
        # into a single line literal with a \n in it, since otherwise
        # generated DDL migrations with multi-line literals will be
        # mangled by the cli indenting them.
        r"""
        CREATE TYPE Foo {
            CREATE ANNOTATION description :=
                "multi
                 line";
        };
% OK %
        CREATE TYPE Foo {
            CREATE ANNOTATION description :=
                'multi\n                 line';
        };
        """

    def test_edgeql_syntax_ddl_constraint_01(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum(VARIADIC p: anytype)
            EXTENDING std::constraint
        {
            SET errmessage := '{subject} must be one of: {p}.';
            USING (contains($p, __subject__));
        };
        """

    def test_edgeql_syntax_ddl_constraint_02(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum(VARIADIC p: anytype) {
            SET errmessage := '{subject} must be one of: {$p}.';
            USING (contains($p, __subject__));
        };
        """

    def test_edgeql_syntax_ddl_constraint_03(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum {
            SET errmessage := '{subject} must be one of: {param}.';
            USING (contains($param, __subject__));
        };
        """

    def test_edgeql_syntax_ddl_constraint_04(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum() {
            SET errmessage := '{subject} must be one of: {param}.';
            USING (contains($param, __subject__));
        };

% OK %

        CREATE ABSTRACT CONSTRAINT std::enum {
            SET errmessage := '{subject} must be one of: {param}.';
            USING (contains($param, __subject__));
        };
        """

    def test_edgeql_syntax_ddl_constraint_05(self):
        """
        CREATE SCALAR TYPE std::decimal_rounding_t EXTENDING std::str {
            CREATE CONSTRAINT std::enum('a', 'b');
        };
        """

    def test_edgeql_syntax_ddl_constraint_06(self):
        """
        CREATE ABSTRACT CONSTRAINT std::len_constraint ON
                (len(<std::str>__subject__))
            EXTENDING std::constraint
        {
            SET errmessage := 'invalid {subject}';
        };
        """

    def test_edgeql_syntax_ddl_constraint_07(self):
        """
        CREATE SCALAR TYPE std::decimal_rounding_t EXTENDING std::str {
            CREATE CONSTRAINT max_value(99) ON (<int64>__subject__);
        };
        """

    def test_edgeql_syntax_ddl_constraint_08(self):
        """
        CREATE ABSTRACT CONSTRAINT len_fail(f: std::str) {
            USING (__subject__ <= f);
            SET subjectexpr := len(__subject__);
        };

% OK %

        CREATE ABSTRACT CONSTRAINT len_fail(f: std::str) {
            USING ((__subject__ <= f));
            SET subjectexpr := (len(__subject__));
        };
        """

    def test_edgeql_syntax_ddl_constraint_09(self):
        """
        CREATE TYPE Foo {
            CREATE LINK bar -> Bar {
                CREATE CONSTRAINT my_constraint ON (
                    # It's possible to use shapes in the "ON" expression.
                    # This would be ambiguous without parentheses.
                    __source__{
                        baz := __source__.a + __source__.b
                    }.baz
                ) {
                    CREATE ANNOTATION title := 'special';
                };
            };
        };

% OK %

        CREATE TYPE Foo {
            CREATE LINK bar -> Bar {
                CREATE CONSTRAINT my_constraint ON (
                    (__source__{
                        baz := (__source__.a + __source__.b)
                    }).baz
                ) {
                    CREATE ANNOTATION title := 'special';
                };
            };
        };
        """

    def test_edgeql_syntax_ddl_constraint_10(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                ALTER CONSTRAINT my_constraint ON (foo) {
                    CREATE ANNOTATION title := 'special';
                    RESET errmessage;
                };
            };
            ALTER LINK baz {
                DROP CONSTRAINT my_length(10);
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'RENAME'", line=5, col=21)
    def test_edgeql_syntax_ddl_constraint_11(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                ALTER CONSTRAINT my_constraint ON (foo) {
                    RENAME TO myconstraint;
                };
            };
        };
        """

    def test_edgeql_syntax_ddl_constraint_12(self):
        """
        ALTER ABSTRACT CONSTRAINT my_constraint
        RESET errmessage;
        """

    def test_edgeql_syntax_ddl_constraint_13(self):
        """
        ALTER ABSTRACT CONSTRAINT not_bad
            USING (((__subject__ != 'bad') and (__subject__ != 'terrible')));
        """

    def test_edgeql_syntax_ddl_constraint_14(self):
        """
        ALTER TYPE Foo {
            CREATE CONSTRAINT exclusive ON (.name) EXCEPT (.reject);
        };
        """

    def test_edgeql_syntax_ddl_constraint_15(self):
        """
        ALTER TYPE Foo {
            DROP CONSTRAINT exclusive ON (.name) EXCEPT (.reject);
        };
        """

    def test_edgeql_syntax_ddl_function_01(self):
        """
        CREATE FUNCTION std::strlen(string: std::str) -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_02(self):
        """
        CREATE FUNCTION std::strlen(a: std::str) -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_03(self):
        """
        CREATE FUNCTION std::strlen(string: std::str) -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_04(self):
        """
        CREATE FUNCTION std::strlen(string: std::str, integer: std::int64)
            -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_05(self):
        """
        CREATE FUNCTION std::strlen(string: std::str, a: std::int64)
            -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_06(self):
        """
        CREATE FUNCTION std::strlen(string: std::str = '1')
            -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'`abc` without default follows.*`string` with default',
                  line=2, col=61)
    def test_edgeql_syntax_ddl_function_07(self):
        """
        CREATE FUNCTION std::strlen(string: std::str = '1', abc: std::str)
            -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'positional argument `abc` follows.*`string`',
                  line=3, col=37)
    def test_edgeql_syntax_ddl_function_08(self):
        """
        CREATE FUNCTION std::strlen(VARIADIC string: std::str,
                                    abc: std::str)
            -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'more than one variadic argument', line=3, col=37)
    def test_edgeql_syntax_ddl_function_09(self):
        """
        CREATE FUNCTION std::strlen(VARIADIC string: std::str,
                                    VARIADIC abc: std::str)
            -> std::int64;
        """

    def test_edgeql_syntax_ddl_function_10(self):
        """
        CREATE FUNCTION std::strlen(a: std::str = '1', VARIADIC b: std::str)
            -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_11(self):
        """
        CREATE FUNCTION no_params() -> std::int64
        USING ( SELECT 1 );
        """

    def test_edgeql_syntax_ddl_function_13(self):
        """
        CREATE FUNCTION foo(string: std::str) -> tuple<bar: std::int64>
        USING (SELECT (bar := 123));
        """

    def test_edgeql_syntax_ddl_function_14(self):
        """
        CREATE FUNCTION foo(string: std::str)
        -> tuple<
            bar: std::int64,
            baz: std::str
        > USING (SELECT smth());
        """
    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=3)
    def test_edgeql_syntax_ddl_function_16(self):
        """
        CREATE FUNCTION foo(string: std::str)
        -> std::int64 USING AAA FUNCTION 'foo';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=3)
    def test_edgeql_syntax_ddl_function_19(self):
        """
        CREATE FUNCTION foo(string: std::str)
        -> std::int64 USING AAA 'code';
        """

    def test_edgeql_syntax_ddl_function_20(self):
        """
        CREATE FUNCTION foo() -> std::int64 USING SQL 'SELECT 1';

% OK %

        CREATE FUNCTION foo() -> std::int64 USING SQL $$SELECT 1$$;
        """

    def test_edgeql_syntax_ddl_function_21(self):
        """
        CREATE FUNCTION foo() -> std::int64 USING SQL FUNCTION 'aaa';
        """

    def test_edgeql_syntax_ddl_function_24(self):
        """
        CREATE FUNCTION foo() -> std::str USING SQL $a$SELECT $$foo$$$a$;
        """

    def test_edgeql_syntax_ddl_function_25(self):
        """
        CREATE FUNCTION foo() -> std::str {
            CREATE ANNOTATION description := 'aaaa';
            USING SQL $a$SELECT $$foo$$$a$;
        };
        """

    def test_edgeql_syntax_ddl_function_26(self):
        """
        CREATE FUNCTION foo() -> std::str {
            SET volatility := 'Volatile';
            CREATE ANNOTATION description := 'aaaa';
            USING SQL $a$SELECT $$foo$$$a$;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "missing a USING clause", line=2)
    def test_edgeql_syntax_ddl_function_27(self):
        """
        CREATE FUNCTION foo() -> std::str {
            CREATE ANNOTATION description := 'aaaa';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "more than one USING <code> clause", line=5)
    def test_edgeql_syntax_ddl_function_28(self):
        """
        CREATE FUNCTION foo() -> std::str {
            USING SQL 'SELECT 1';
            CREATE ANNOTATION description := 'aaaa';
            USING SQL 'SELECT 2';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"missing type declaration for",
                  line=3, col=46)
    def test_edgeql_syntax_ddl_function_30(self):
        """
        CREATE FUNCTION std::foobar(arg1: str, arg2: str = 'DEFAULT',
                                    VARIADIC arg3)
            -> std::int64
            USING EdgeQL $$$$;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected keyword 'SET'", line=2, col=43)
    def test_edgeql_syntax_ddl_function_31(self):
        # parameter name is missing
        """
        CREATE FUNCTION std::foo(VARIADIC SET OF std::str) -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected keyword 'SET'", line=2, col=43)
    def test_edgeql_syntax_ddl_function_32(self):
        """
        CREATE FUNCTION std::foo(VARIADIC SET OF std::str) -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected keyword 'VARIADIC'", line=2, col=39)
    def test_edgeql_syntax_ddl_function_33(self):
        """
        CREATE FUNCTION std::foo(bar: VARIADIC SET OF std::str) -> std::int64;
        """

    def test_edgeql_syntax_ddl_function_34(self):
        """
        CREATE FUNCTION foo(a: OPTIONAL std::str) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected keyword 'VARIADIC'", line=2, col=57)
    def test_edgeql_syntax_ddl_function_35(self):
        """
        CREATE FUNCTION std::foo(a: SET OF std::str) -> VARIADIC std::int64
            USING SQL $a$SELECT $$foo$$$a$;
        """

    def test_edgeql_syntax_ddl_function_36(self):
        """
        CREATE FUNCTION foo(
            a: OPTIONAL std::str,
            NAMED ONLY b: OPTIONAL std::str,
            NAMED ONLY c: OPTIONAL std::str = '1',
            NAMED ONLY d: OPTIONAL std::str
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"positional argument `d` follows NAMED ONLY.*`c`",
                  line=6, col=13)
    def test_edgeql_syntax_ddl_function_37(self):
        """
        CREATE FUNCTION foo(
            a: OPTIONAL std::str,
            NAMED ONLY b: OPTIONAL std::str = '1',
            NAMED ONLY c: OPTIONAL std::str,
            d: OPTIONAL std::str
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"NAMED ONLY argument `s1`.*before VARIADIC.*`v`",
                  line=5, col=13)
    def test_edgeql_syntax_ddl_function_38(self):
        """
        CREATE FUNCTION foo(
            s: OPTIONAL std::str,
            NAMED ONLY c: OPTIONAL std::str,
            NAMED ONLY s1: OPTIONAL std::str = '1',
            VARIADIC v: OPTIONAL std::str = '1'
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"NAMED ONLY argument `c`.*before VARIADIC.*`v`",
                  line=4, col=13)
    def test_edgeql_syntax_ddl_function_39(self):
        """
        CREATE FUNCTION foo(
            s: OPTIONAL std::str,
            NAMED ONLY c: OPTIONAL std::str,
            VARIADIC v: OPTIONAL std::str = '1',
            NAMED ONLY s1: OPTIONAL std::str = '1'
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"positional argument `select` follows VARIADIC.*`variadic`",
                  line=5, col=13)
    def test_edgeql_syntax_ddl_function_40(self):
        """
        CREATE FUNCTION foo(
            `set`: OPTIONAL std::str,
            VARIADIC `variadic`: OPTIONAL std::str,
            `select`: OPTIONAL std::str = '1'
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    def test_edgeql_syntax_ddl_function_41(self):
        """
        CREATE FUNCTION foo(
            `set`: OPTIONAL std::str,
            VARIADIC `variadic`: OPTIONAL std::str,
            NAMED ONLY `create`: OPTIONAL std::str,
            NAMED ONLY `select`: OPTIONAL std::str = '1'
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"VARIADIC argument `b` cannot have a default",
                  line=2, col=37)
    def test_edgeql_syntax_ddl_function_42(self):
        """
        CREATE FUNCTION std::strlen(VARIADIC b: std::str = '1')
            -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'numeric parameters are not supported',
                  line=2, col=37)
    def test_edgeql_syntax_ddl_function_43(self):
        """
        CREATE FUNCTION std::strlen($1: int32) -> int64
            USING EdgeQL $$ SELECT 1 $$;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'duplicate parameter name `a`',
                  line=2, col=55)
    def test_edgeql_syntax_ddl_function_44(self):
        """
        CREATE FUNCTION std::strlen(a: int16, b: str, a: int16) -> int64
            USING EdgeQL $$ SELECT 1 $$;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'duplicate parameter name `aa`',
                  line=3, col=37)
    def test_edgeql_syntax_ddl_function_45(self):
        """
        CREATE FUNCTION std::strlen(aa: int16, b: str,
                                    NAMED ONLY aa: int16) -> int64
            USING EdgeQL $$ SELECT 1 $$;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'duplicate parameter name `aa`',
                  line=3, col=37)
    def test_edgeql_syntax_ddl_function_46(self):
        """
        CREATE FUNCTION std::strlen(aa: int16, b: str,
                                    VARIADIC aa: int16) -> int64
            USING EdgeQL $$ SELECT 1 $$;
        """

    def test_edgeql_syntax_ddl_function_47(self):
        """
        CREATE FUNCTION foo(
            variadiC f: int64,
            named only foo: OPTIONAL std::str,
            nameD onlY bar: OPTIONAL std::str = '1'
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=32)
    def test_edgeql_syntax_ddl_function_48(self):
        """
        CREATE FUNCTION __std__(
            f: int64
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    def test_edgeql_syntax_ddl_function_49(self):
        """
        CREATE FUNCTION std::strlen(string: std::str,) -> std::int64
            USING SQL FUNCTION 'strlen';

% OK %

        CREATE FUNCTION std::strlen(string: std::str) -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_50(self):
        """
        CREATE FUNCTION std::strlen(string: std::str = '1',)
            -> std::int64
            USING SQL FUNCTION 'strlen';

% OK %

        CREATE FUNCTION std::strlen(string: std::str = '1')
            -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_51(self):
        """
        CREATE FUNCTION std::strlen(
            a: std::str = '1',
            VARIADIC b: std::str,
        ) -> std::int64
            USING SQL FUNCTION 'strlen';

% OK %

        CREATE FUNCTION std::strlen(
            a: std::str = '1',
            VARIADIC b: std::str
        ) -> std::int64
            USING SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_52(self):
        """
        CREATE FUNCTION foo(
            a: OPTIONAL std::str,
            NAMED ONLY b: OPTIONAL std::str,
            NAMED ONLY c: OPTIONAL std::str = '1',
            NAMED ONLY d: OPTIONAL std::str,
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';

% OK %

        CREATE FUNCTION foo(
            a: OPTIONAL std::str,
            NAMED ONLY b: OPTIONAL std::str,
            NAMED ONLY c: OPTIONAL std::str = '1',
            NAMED ONLY d: OPTIONAL std::str
        ) ->
            std::int64 USING SQL FUNCTION 'aaa';
        """

    def test_edgeql_syntax_ddl_function_53(self):
        """
        ALTER FUNCTION foo() USING ('no');
        """

    def test_edgeql_syntax_ddl_function_54(self):
        """
        ALTER FUNCTION foo() {
            SET volatility := 'volatile';
            USING ('no');
        };
        """

    def test_edgeql_syntax_ddl_operator_01(self):
        """
        CREATE INFIX OPERATOR
        std::`OR` (a: std::bool, b: std::bool) -> std::bool {
            SET volatility := 'Immutable';
            USING SQL $$
            SELECT ("a" OR "b") AND ("a"::int | "b"::int)::bool
            $$;
        };
        """

    def test_edgeql_syntax_ddl_operator_02(self):
        """
        CREATE INFIX OPERATOR
        std::`AND` (a: std::bool, b: std::bool) -> std::bool {
            SET volatility := 'Immutable';
            USING SQL EXPRESSION;
        };
        """

    def test_edgeql_syntax_ddl_operator_03(self):
        """
        CREATE INFIX OPERATOR
        std::`=` (l: std::bool, r: std::bool) -> std::bool {
            SET volatility := 'Immutable';
            SET commutator := 'std::=';
            SET negator := 'std::!=';
            USING SQL OPERATOR '=';
        };
        """

    def test_edgeql_syntax_ddl_operator_04(self):
        """
        CREATE INFIX OPERATOR
        std::`>` (l: std::int32, r: std::float32) -> std::bool {
            SET volatility := 'Immutable';
            SET commutator := 'std::<';
            SET negator := 'std::<=';
            USING SQL OPERATOR '>(float8,float8)';
        };
        """

    def test_edgeql_syntax_ddl_operator_05(self):
        """
        CREATE ABSTRACT INFIX OPERATOR
        std::`>=` (l: anytype, r: anytype) -> std::bool;
        """

    def test_edgeql_syntax_ddl_operator_06(self):
        """
        ALTER INFIX OPERATOR std::`>=` (l: anytype, r: anytype) {
            CREATE ANNOTATION description := 'gte';
        };
        """

    def test_edgeql_syntax_ddl_operator_07(self):
        """
        DROP INFIX OPERATOR std::`>=` (l: anytype, r: anytype);
        """

    def test_edgeql_syntax_ddl_cast_01(self):
        """
        CREATE CAST FROM std::str TO std::bool {
            SET volatility := 'Immutable';
            USING SQL FUNCTION 'edgedb.str_to_bool';
        };
        """

    def test_edgeql_syntax_ddl_cast_02(self):
        """
        CREATE CAST FROM std::bool TO std::str {
            SET volatility := 'Immutable';
            USING SQL CAST;
        };
        """

    def test_edgeql_syntax_ddl_cast_03(self):
        """
        CREATE CAST FROM std::json TO std::bigint {
            SET volatility := 'Stable';
            USING SQL $$
            SELECT edgedb.str_to_bigint(
                edgedb.jsonb_extract_scalar(val, 'number')
            );
            $$;
        };
        """

    def test_edgeql_syntax_ddl_cast_04(self):
        """
        CREATE CAST FROM std::int32 TO std::int64 {
            SET volatility := 'Immutable';
            USING SQL CAST;
            ALLOW IMPLICIT;
        };
        """

    def test_edgeql_syntax_ddl_cast_05(self):
        """
        CREATE CAST FROM std::int64 TO std::int16 {
            SET volatility := 'Immutable';
            USING SQL CAST;
            ALLOW ASSIGNMENT;
        };
        """

    def test_edgeql_syntax_ddl_cast_06(self):
        """
        CREATE CAST FROM std::BaseObject TO std::json {
            SET volatility := 'Immutable';
            USING SQL EXPRESSION;
        };
        """

    def test_edgeql_syntax_ddl_cast_07(self):
        """
        ALTER CAST FROM std::BaseObject TO std::json {
            CREATE ANNOTATION description := 'json';
        };
        """

    def test_edgeql_syntax_ddl_cast_08(self):
        """
        DROP CAST FROM std::BaseObject TO std::json;
        """

    def test_edgeql_syntax_ddl_property_01(self):
        """
        CREATE ABSTRACT PROPERTY std::property {
            SET title := 'Base property';
        };
        """

    def test_edgeql_syntax_ddl_property_02(self):
        """
        CREATE ABSTRACT PROPERTY std::property {
            SET title := 'Base property';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=43)
    def test_edgeql_syntax_ddl_property_03(self):
        """
        CREATE ABSTRACT PROPERTY PROPERTY std::property {
            SET title := 'Base property';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=34)
    def test_edgeql_syntax_ddl_property_04(self):
        """
        CREATE ABSTRACT PROPERTY __type__ {
            SET title := 'Base property';
        };
        """

    def test_edgeql_syntax_ddl_property_05(self):
        # omit optional semicolons
        """
        CREATE ABSTRACT PROPERTY std::property {
            SET title := 'Base property'
        }

% OK %

        CREATE ABSTRACT PROPERTY std::property {
            SET title := 'Base property';
        };
        """

    def test_edgeql_syntax_ddl_property_06(self):
        """
        ALTER ABSTRACT PROPERTY prop {
            RESET default;
        };

% OK %

        ALTER ABSTRACT PROPERTY prop RESET default;
        """

    def test_edgeql_syntax_ddl_property_07(self):
        """
        create abstract property union;
        alter abstract property union reset default;
        drop abstract property union;
        """

    def test_edgeql_syntax_ddl_link_01(self):
        """
        create abstract link union;
        alter abstract link union reset default;
        drop abstract link union;
        """

    def test_edgeql_syntax_ddl_module_01(self):
        """
        CREATE MODULE foo;
        CREATE MODULE foo.bar;
        CREATE MODULE all.abstract.bar;

% OK %

        CREATE MODULE foo;
        CREATE MODULE `foo.bar`;
        CREATE MODULE `all.abstract.bar`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=23)
    def test_edgeql_syntax_ddl_module_02(self):
        """
        CREATE MODULE __subject__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=23)
    def test_edgeql_syntax_ddl_module_03(self):
        """
        CREATE MODULE __std__.a;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=25)
    def test_edgeql_syntax_ddl_module_04(self):
        """
        CREATE MODULE a.__std__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=23)
    def test_edgeql_syntax_ddl_module_05(self):
        """
        CREATE MODULE `__std__`;
        """

    def test_edgeql_syntax_ddl_module_06(self):
        """
        CREATE MODULE foo IF NOT EXISTS;
        """

    def test_edgeql_syntax_ddl_type_01(self):
        """
        CREATE ABSTRACT TYPE schema::Type EXTENDING schema::Object;
        """

    def test_edgeql_syntax_ddl_type_02(self):
        """
        CREATE TYPE schema::TypeElement {
            CREATE REQUIRED LINK type -> schema::Type;
            CREATE REQUIRED LINK num -> std::int64;
            CREATE PROPERTY name EXTENDING foo, bar -> std::str;
            CREATE LINK lnk EXTENDING l1 -> schema::Type;
            CREATE LINK lnk1 EXTENDING l1, l2 -> schema::Type;
            CREATE LINK lnk2 EXTENDING l1, l2 -> schema::Type {
                CREATE PROPERTY lnk2_prop -> std::str;
                CREATE PROPERTY lnk2_prop2 EXTENDING foo -> std::str;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_03(self):
        """
        ALTER TYPE schema::Object {
            CREATE MULTI LINK attributes -> schema::Attribute;
        };

% OK %

        ALTER TYPE schema::Object {
            CREATE MULTI LINK attributes -> schema::Attribute;
        };
        """

    def test_edgeql_syntax_ddl_type_04(self):
        """
        CREATE TYPE mymod::Foo {
            CREATE LINK bar0 -> mymod::Bar {
                ON TARGET DELETE RESTRICT;
            };
            CREATE LINK bar1 -> mymod::Bar {
                ON TARGET DELETE DELETE SOURCE;
            };
            CREATE LINK bar2 -> mymod::Bar {
                ON TARGET DELETE ALLOW;
            };
            CREATE LINK bar3 -> mymod::Bar {
                ON TARGET DELETE DEFERRED RESTRICT;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_05(self):
        """
        CREATE TYPE mymod::Foo {
            CREATE SINGLE LINK foo -> mymod::Foo;
            CREATE MULTI LINK bar -> mymod::Bar;
            CREATE REQUIRED SINGLE LINK baz -> mymod::Baz;
            CREATE REQUIRED MULTI LINK spam -> mymod::Spam;
        };
        """

    def test_edgeql_syntax_ddl_type_06(self):
        """
        CREATE TYPE mymod::Foo {
            CREATE SINGLE PROPERTY foo -> str;
            CREATE MULTI PROPERTY bar -> str;
            CREATE REQUIRED SINGLE PROPERTY baz -> str;
            CREATE REQUIRED MULTI PROPERTY spam -> str;
        };
        """

    def test_edgeql_syntax_ddl_type_07(self):
        """
        ALTER TYPE mymod::Foo {
            ALTER PROPERTY foo {
                SET SINGLE;
                SET REQUIRED;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_08(self):
        """
        ALTER TYPE mymod::Foo ALTER LINK foo {
            SET MULTI;
            SET OPTIONAL;
        };
% OK %
        ALTER TYPE mymod::Foo {
            ALTER LINK foo {
                SET MULTI;
                SET OPTIONAL;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_09(self):
        # omit optional semicolons
        """
        ALTER TYPE mymod::Foo ALTER LINK foo {
            SET MULTI;
            SET OPTIONAL
        }

% OK %

        ALTER TYPE mymod::Foo {
            ALTER LINK foo {
                SET MULTI;
                SET OPTIONAL;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_10(self):
        """
        ALTER TYPE mymod::Foo {
            ALTER PROPERTY foo {
                SET OWNED;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_11(self):
        """
        ALTER TYPE mymod::Foo {
            ALTER PROPERTY foo {
                DROP OWNED;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_12(self):
        """
        CREATE TYPE Foo {
            CREATE PROPERTY bar := 'something';
        };

% OK %

        CREATE TYPE Foo {
            CREATE PROPERTY bar := ('something');
        };
        """

    def test_edgeql_syntax_ddl_type_13(self):
        """
        ALTER TYPE Foo {
            ALTER PROPERTY bar {
                RESET EXPRESSION;
                RESET default;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_14(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                RESET EXPRESSION;
                RESET default;
            };
        };
        """

    def test_edgeql_syntax_ddl_type_15(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                SET TYPE int64 USING (SELECT (.bar, 1));
            };
        };
        """

    def test_edgeql_syntax_ddl_type_16(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                SET REQUIRED USING (SELECT '123');
            };
        };
        """

    def test_edgeql_syntax_ddl_type_17(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                SET SINGLE USING (SELECT '123');
            };
        };
        """

    def test_edgeql_syntax_ddl_type_18(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                RESET CARDINALITY USING (SELECT '123');
            };
        };
        """

    def test_edgeql_syntax_ddl_type_19(self):
        """
        ALTER TYPE Foo {
            CREATE PROPERTY bar -> str {
                USING (4);
            };
        };
        """

    def test_edgeql_syntax_ddl_type_20(self):
        """
        ALTER TYPE Foo {
            ALTER PROPERTY bar {
                SET TYPE str;
                USING (4);
            };
        };
        """

    def test_edgeql_syntax_ddl_type_21(self):
        """
        ALTER TYPE Foo {
            CREATE LINK bar -> Object {
                USING (SELECT Object);
            };
        };
        """

    def test_edgeql_syntax_ddl_type_22(self):
        """
        ALTER TYPE Foo {
            ALTER LINK bar {
                SET TYPE Object;
                USING (SELECT Object);
            };
        };
        """

    def test_edgeql_syntax_ddl_type_23(self):
        """
        CREATE TYPE `123` {
            CREATE PROPERTY `456` -> str;
        };
        """

    def test_edgeql_syntax_ddl_type_24(self):
        """
        ALTER TYPE mymod::Foo {
            ALTER LINK union {
                USING (SELECT Object);
            };
            ALTER PROPERTY except {
                USING (1312);
            };
        };
        """

    def test_edgeql_syntax_ddl_type_25(self):
        """
        ALTER TYPE mymod::Foo {
            DROP LINK union;
            DROP PROPERTY except;
        };
        """

    def test_edgeql_syntax_set_command_01(self):
        """
        SET MODULE default;
        """

    def test_edgeql_syntax_set_command_02(self):
        """
        SET ALIAS foo AS MODULE default;
        """

    def test_edgeql_syntax_set_command_03(self):
        """
        SET MODULE default;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected ','")
    def test_edgeql_syntax_set_command_04(self):
        # Old and no longer supported syntax that allowed to
        # specify multiple comma-separated SET subcommands.
        """
        SET ALIAS foo AS MODULE foo1, ALIAS bar AS MODULE foo2;
        """

    def test_edgeql_syntax_reset_command_01(self):
        """
        RESET MODULE;
        RESET ALIAS foo;
        RESET ALIAS *;
        """

    def test_edgeql_syntax_configure_01(self):
        """
        CONFIGURE INSTANCE SET foo := (SELECT User);
        CONFIGURE SESSION SET foo := (SELECT User);
        CONFIGURE CURRENT DATABASE SET foo := (SELECT User);
        CONFIGURE INSTANCE SET cfg::foo := (SELECT User);
        CONFIGURE SESSION SET cfg::foo := (SELECT User);
        CONFIGURE CURRENT DATABASE SET cfg::foo := (SELECT User);
        CONFIGURE INSTANCE RESET foo;
        CONFIGURE SESSION RESET foo;
        CONFIGURE CURRENT DATABASE RESET foo;
        CONFIGURE INSTANCE RESET cfg::foo;
        CONFIGURE SESSION RESET cfg::foo;
        CONFIGURE CURRENT DATABASE RESET cfg::foo;
        CONFIGURE INSTANCE INSERT Foo {bar := (SELECT 1)};
        CONFIGURE SESSION INSERT Foo {bar := (SELECT 1)};
        CONFIGURE CURRENT DATABASE INSERT Foo {bar := (SELECT 1)};
        CONFIGURE INSTANCE INSERT cfg::Foo {bar := (SELECT 1)};
        CONFIGURE SESSION INSERT cfg::Foo {bar := (SELECT 1)};
        CONFIGURE CURRENT DATABASE INSERT cfg::Foo {bar := (SELECT 1)};
        CONFIGURE INSTANCE RESET Foo FILTER (.bar = 2);
        CONFIGURE SESSION RESET Foo FILTER (.bar = 2);
        CONFIGURE CURRENT DATABASE RESET Foo FILTER (.bar = 2);
        """

    @tb.must_fail(
        errors.EdgeQLSyntaxError,
        r"'CONFIGURE DATABASE' is invalid syntax. "
        r"Did you mean 'CONFIGURE CURRENT DATABASE'?",
        line=2, col=19)
    def test_edgeql_syntax_configure_02(self):
        """
        CONFIGURE DATABASE SET foo := (SELECT User);
        """

    @tb.must_fail(
        errors.EdgeQLSyntaxError,
        r"'configure database' is invalid syntax. "
        r"Did you mean 'configure current database'?",
        line=2, col=19)
    def test_edgeql_syntax_configure_03(self):
        """
        configure database set foo := (SELECT User);
        """

    def test_edgeql_syntax_ddl_alias_01(self):
        """
        CREATE ALIAS Foo := (SELECT User);
        """

    def test_edgeql_syntax_ddl_alias_02(self):
        """
        CREATE ALIAS Foo {
            USING (SELECT User);
        };

        ALTER ALIAS Foo
            USING (SELECT Person);

        DROP ALIAS Foo;

% OK %

        CREATE ALIAS Foo := (SELECT User);

        ALTER ALIAS Foo
            USING (SELECT Person);

        DROP ALIAS Foo;
        """

    def test_edgeql_syntax_ddl_alias_03(self):
        """
        CREATE ALIAS Foo := User;

% OK %

        CREATE ALIAS Foo := (User);
        """

    def test_edgeql_syntax_ddl_index_01(self):
        """
        CREATE TYPE Foo {
            CREATE INDEX ON (.title);

            CREATE INDEX ON (SELECT __subject__.title);

            CREATE INDEX ON (.foo) EXCEPT (.bar);
        };
        """

    def test_edgeql_syntax_ddl_index_02(self):
        """
        ALTER TYPE Foo {
            DROP INDEX ON (.title);

            CREATE INDEX ON (.title) {
                CREATE ANNOTATION system := 'Foo';
            };

            ALTER INDEX ON (.title) {
                ALTER ANNOTATION system := 'Foo';
            };

            ALTER INDEX ON (.title) {
                DROP ANNOTATION system;
            };

            DROP INDEX ON (.foo) EXCEPT (.bar);

            ALTER INDEX ON (.foo) EXCEPT (.bar) {
                DROP ANNOTATION system;
            };
        };
        """

    def test_edgeql_syntax_ddl_index_03(self):
        """
        ALTER TYPE Foo {
            ALTER INDEX ON (.title) {
                CREATE ANNOTATION system := 'Foo'
            };

            ALTER INDEX ON (.title) {
                DROP ANNOTATION system
            };
        };

% OK %

        ALTER TYPE Foo {
            ALTER INDEX ON (.title) {
                CREATE ANNOTATION system := 'Foo';
            };

            ALTER INDEX ON (.title) {
                DROP ANNOTATION system;
            };
        };
        """

    def test_edgeql_syntax_ddl_index_04(self):
        """
        CREATE TYPE Foo {
            CREATE INDEX pg::gist ON (.title);
        };
        """

    @test.xerror('index parameters not implemented yet')
    def test_edgeql_syntax_ddl_index_05(self):
        """
        CREATE TYPE Foo {
            CREATE INDEX myindex0() ON (.bar);

            CREATE INDEX myindex1(a := 13, b := 'ab', conf := [4, 3, 2])
                ON (.baz);

            CREATE INDEX myindex2(num := 13, val := 'ab')
                ON (.foo);
        };

% OK %

        CREATE TYPE Foo {
            CREATE INDEX myindex0 ON (.bar);

            CREATE INDEX myindex1(a := 13, b := 'ab', conf := [4, 3, 2])
                ON (.baz);

            CREATE INDEX myindex2(num := 13, val := 'ab')
                ON (.foo);
        };
        """

    def test_edgeql_syntax_ddl_index_06(self):
        """
        CREATE ABSTRACT INDEX myindex0;
        """

    @test.xerror('index parameters and fallback not implemented yet')
    def test_edgeql_syntax_ddl_index_07(self):
        """
        CREATE ABSTRACT INDEX myindex1(conf: str = 'special');
        CREATE ABSTRACT INDEX myindex2(val: int64);
        CREATE ABSTRACT INDEX myindex3(a: int64, b: str = 'default')
            USING myindex2(val := a),
                  myindex1(conf := b),
                  myindex1;
        """

    def test_edgeql_syntax_ddl_index_08(self):
        """
        CREATE ABSTRACT INDEX myindex1 EXTENDING fts;
        CREATE ABSTRACT INDEX myindex2(conf := 'test') EXTENDING fts;
        """

    def test_edgeql_syntax_ddl_index_09(self):
        """
        ALTER ABSTRACT INDEX myindex0 {
            DROP ANNOTATION system;
        };
        """

    def test_edgeql_syntax_ddl_index_10(self):
        """
        DROP ABSTRACT INDEX myindex0;
        """

    @test.xerror('providing SQL is not implemented')
    def test_edgeql_syntax_ddl_index_11(self):
        """
        CREATE ABSTRACT INDEX std::btree ON anytype {
            USING SQL $$hash ((%) NULLS FIRST)$$;
        };
        """

    def test_edgeql_syntax_ddl_global_01(self):
        """
        CREATE GLOBAL Foo := (SELECT User);
        """

    def test_edgeql_syntax_ddl_global_02(self):
        """
        CREATE GLOBAL foo -> str;
        """

    def test_edgeql_syntax_ddl_global_03(self):
        """
        CREATE GLOBAL Foo {
            USING (SELECT User);
        };

        ALTER GLOBAL Foo
            USING (SELECT Person);

        DROP GLOBAL Foo;

% OK %

        CREATE GLOBAL Foo := (SELECT User);

        ALTER GLOBAL Foo
            USING (SELECT Person);

        DROP GLOBAL Foo;
        """

    def test_edgeql_syntax_ddl_global_04(self):
        """
        CREATE GLOBAL foo -> str {
            SET DEFAULT := '20';
            CREATE ANNOTATION title := 'foo';
        };
        """

    def test_edgeql_syntax_ddl_global_05(self):
        """
        CREATE REQUIRED GLOBAL foo -> str {
            CREATE ANNOTATION title := 'foo';
            SET default := 'lol';
        };
        """

    def test_edgeql_syntax_ddl_global_06(self):
        """
        ALTER GLOBAL foo {
            set default := '10';
            using (20);
            reset default;
            rename to bar;
            set required;
            set single;
            reset cardinality;
            reset optionality;
            reset expression;
            set type int64;
            create annotation title := 'foo';
            alter annotation title := 'foo';
            drop annotation title;
        };
        """
    def test_edgeql_syntax_ddl_global_07(self):
        """
        CREATE GLOBAL test::foo -> str;
        """

    def test_edgeql_syntax_global_01(self):
        """
        select global bar;
        """

    def test_edgeql_syntax_global_02(self):
        """
        select (global bar + 1);
        """

    def test_edgeql_syntax_global_03(self):
        """
        select (global bar);

% OK %

        select global bar;
        """

    def test_edgeql_syntax_config_global_01(self):
        """
        set global foo := 10;
        """

    def test_edgeql_syntax_config_global_02(self):
        """
        set global test::foo := 10;
        """

    def test_edgeql_syntax_config_global_03(self):
        """
        reset global foo;
        """

    def test_edgeql_syntax_config_global_04(self):
        """
        reset global test::foo;
        """

    def test_edgeql_syntax_ddl_policy_01(self):
        """
        create type Foo {
            create access policy test
            allow all
            using (true);
        };
        """

    def test_edgeql_syntax_ddl_policy_02(self):
        """
        create type Foo {
            create access policy test
            allow select, update write
            using (true);
        };
        """

    def test_edgeql_syntax_ddl_policy_03(self):
        """
        alter type Foo {
            create access policy test
            when (true)
            deny all
            using (true) {
                create annotation title := 'foo';
            };
        };
        """

    def test_edgeql_syntax_ddl_policy_04(self):
        """
        alter type Foo {
            alter access policy test {
                rename to bar;
                create annotation title := 'foo';
            };
        };
        """

    def test_edgeql_syntax_ddl_policy_05(self):
        """
        alter type Foo {
            drop access policy test;
        };
        """

    def test_edgeql_syntax_ddl_policy_06(self):
        """
        create type Foo {
            alter access policy test {
                when (false);
                allow all;
                using (true);
            };
        };
        """

    def test_edgeql_syntax_ddl_policy_07(self):
        """
        create type Foo {
            alter access policy test {
                reset when;
                allow all;
                using (true);
            };
        };
        """

    def test_edgeql_syntax_ddl_empty_01(self):
        """
        CREATE TYPE Foo { };

% OK %

        CREATE TYPE Foo;
        """

    def test_edgeql_syntax_ddl_empty_02(self):
        """
        CREATE TYPE Foo { CREATE PROPERTY bar -> str { } };

% OK %

        CREATE TYPE Foo {
            CREATE PROPERTY bar -> str;
        };
        """

    def test_edgeql_syntax_sdl_empty_01(self):
        """
        START MIGRATION to {
            type default::User {

            };
        };

% OK %

        START MIGRATION to {
            type default::User;
        };
        """

    def test_edgeql_syntax_sdl_empty_02(self):
        """
        START MIGRATION to {
            type default::User {
                property name -> str {

                };
            };
        };

% OK %

        START MIGRATION to {
            type default::User {
                property name -> str;
            };
        };
        """

    def test_edgeql_syntax_ddl_semi_01(self):
        """
        CREATE TYPE Foo { ;;; };

% OK %

        CREATE TYPE Foo;
        """

    def test_edgeql_syntax_ddl_semi_02(self):
        """
        CREATE TYPE Foo {
            ;;;
            CREATE PROPERTY bar -> str
            ;;;
            CREATE PROPERTY baz -> int64;
            ;;;
        };

% OK %

        CREATE TYPE Foo {
            CREATE PROPERTY bar -> str;
            CREATE PROPERTY baz -> int64;
        };
        """

    def test_edgeql_syntax_sdl_semi_01(self):
        """
        START MIGRATION to {
            type default::User {
                ;;;;
            };
        };

% OK %

        START MIGRATION to {
            type default::User;
        };
        """

    def test_edgeql_syntax_sdl_semi_02(self):
        """
        START MIGRATION to {
            type default::User {
                ;;;
                property bar -> int64;
                ;;;
                property name -> str;
                ;;;
            };
        };

% OK %

        START MIGRATION to {
            type default::User {
                property bar -> int64;
                property name -> str;
            };
        };
        """

    def test_edgeql_syntax_transaction_01(self):
        """
        START TRANSACTION;
        ROLLBACK;
        COMMIT;

        DECLARE SAVEPOINT foo;
        ROLLBACK TO SAVEPOINT foo;
        RELEASE SAVEPOINT foo;
        """

    def test_edgeql_syntax_transaction_02(self):
        """
        START TRANSACTION ISOLATION SERIALIZABLE, READ ONLY, DEFERRABLE;
        START TRANSACTION ISOLATION SERIALIZABLE, READ ONLY;
        START TRANSACTION READ ONLY, DEFERRABLE;
        START TRANSACTION READ ONLY, NOT DEFERRABLE;
        START TRANSACTION READ WRITE, NOT DEFERRABLE;
        START TRANSACTION READ WRITE;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'only one isolation level can be specified',
                  line=2, col=51)
    def test_edgeql_syntax_transaction_03(self):
        """
        START TRANSACTION ISOLATION SERIALIZABLE, ISOLATION SERIALIZABLE;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'deferrable mode can only be specified once',
                  line=2, col=39)
    def test_edgeql_syntax_transaction_04(self):
        """
        START TRANSACTION DEFERRABLE, NOT DEFERRABLE;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'only one access mode can be specified',
                  line=2, col=51)
    def test_edgeql_syntax_transaction_05(self):
        """
        START TRANSACTION READ WRITE, DEFERRABLE, READ ONLY;
        """

    def test_edgeql_syntax_transaction_06(self):
        """
        START TRANSACTION READ WRITE, NOT DEFERRABLE, ISOLATION SERIALIZABLE;
% OK %
        START TRANSACTION ISOLATION SERIALIZABLE, READ WRITE, NOT DEFERRABLE;
        """

    def test_edgeql_syntax_describe_01(self):
        """
        DESCRIBE SCHEMA AS DDL;
        """

    def test_edgeql_syntax_describe_02(self):
        """
        DESCRIBE TYPE foo::Bar AS SDL;
        """

    def test_edgeql_syntax_describe_03(self):
        """
        DESCRIBE TYPE foo::Bar AS TEXT VERBOSE;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'VERBOSE'",
                  line=2, col=39)
    def test_edgeql_syntax_describe_04(self):
        """
        DESCRIBE TYPE foo::Bar AS DDL VERBOSE;
        """

    def test_edgeql_syntax_describe_05(self):
        """
        DESCRIBE INSTANCE CONFIG;
% OK %
        DESCRIBE INSTANCE CONFIG AS DDL;
        """

    def test_edgeql_syntax_describe_06(self):
        """
        DESCRIBE INSTANCE CONFIG AS DDL;
        """

    def test_edgeql_syntax_describe_07(self):
        """
        DESCRIBE ROLES;
% OK %
        DESCRIBE ROLES AS DDL;
        """

    def test_edgeql_syntax_describe_08(self):
        """
        DESCRIBE ROLES AS DDL;
        """

    def test_edgeql_syntax_describe_09(self):
        """
        DESCRIBE SYSTEM CONFIG;
% OK %
        DESCRIBE INSTANCE CONFIG AS DDL;
        """
