##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import unittest  # NOQA

from edgedb.lang import _testbase as tb
from edgedb.lang.edgeql import generate_source as edgeql_to_source, errors
from edgedb.lang.edgeql.parser import parser as edgeql_parser


class EdgeQLSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s]+|(#.*?(\n|$))|(,(?=\s*[})]))')
    parser_debug_flag = 'DEBUG_EDGEQL'
    markup_dump_lexer = 'sql'
    ast_to_source = edgeql_to_source

    def get_parser(self, *, spec):
        return edgeql_parser.EdgeQLBlockParser()


class TestEdgeSchemaParser(EdgeQLSyntaxTest):
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

    # this is a statement syntax parser, not expressions, so
    # semicolons are obligatory terminators
    #
    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=9)
    def test_edgeql_syntax_nonstatement_01(self):
        """SELECT 1"""

    # 1 + 2 is a valid expression, but it has to have SELECT keyword
    # to be a statement
    #
    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=1)
    def test_edgeql_syntax_nonstatement_02(self):
        """1 + 2;"""

    def test_edgeql_syntax_constants_01(self):
        """
        SELECT 0;
        SELECT 1;
        SELECT +7;
        SELECT -7;
        SELECT 551;
        """

    def test_edgeql_syntax_constants_02(self):
        """
        SELECT 'a1';
        SELECT "a1";
        SELECT $$a1$$;
        SELECT $qwe$a1$qwe$;

% OK %

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
        SELECT 354.32e-20;

% OK %

        SELECT 354.32;
        SELECT 35400000000000.32;
        SELECT 3.54e+19;
        SELECT 3.5432e+20;
        SELECT 3.5432e+20;
        SELECT 3.5432e-20;
        SELECT 3.5432e-18;
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
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unknown token', line=2, col=16)
    def test_edgeql_syntax_constants_07(self):
        """
        SELECT 02;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*SEMICOLON',
                  line=2, col=18)
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
        SELECT NOT Foo.foo;
        """

    def test_edgeql_syntax_ops_10(self):
        """
        SELECT (User.name IN {'Alice', 'Bob'});
        SELECT (User.name NOT IN {'Alice', 'Bob'});
        SELECT (User.name IS std::str);
        SELECT (User IS SystemUser);
        SELECT (User.name IS NOT std::str);
        SELECT (User IS NOT SystemUser);
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
                  'Unexpected token.*>=', line=2, col=16)
    def test_edgeql_syntax_ops_19(self):
        """
        SELECT >=1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*\*', line=2, col=16)
    def test_edgeql_syntax_ops_20(self):
        """
        SELECT *1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unknown token.*~', line=2, col=16)
    def test_edgeql_syntax_ops_21(self):
        """
        SELECT ~1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*>', line=2, col=16)
    def test_edgeql_syntax_ops_22(self):
        """
        SELECT >1;
        """

    def test_edgeql_syntax_ops_23(self):
        """
        SELECT (Foo.a ?= Foo.b);
        SELECT (Foo.b ?!= Foo.b);
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

% OK %

        SELECT bar;
        SELECT bar;
        SELECT foo::bar;
        SELECT foo::bar;
        SELECT foo::bar;
        SELECT foo::bar;
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
        SELECT (event::all);
        SELECT (all::event);
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
                  "Unexpected token.*DOT", line=3, col=21)
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

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*TURNSTILE", line=3, col=18)
    def test_edgeql_syntax_shape_14(self):
        """
        SELECT {
            name := 'foo',
            description := 'bar'
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*LPAREN", line=2, col=21)
    def test_edgeql_syntax_shape_15(self):
        """
        SELECT Foo {(bar)};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*LPAREN", line=2, col=30)
    def test_edgeql_syntax_shape_16(self):
        """
        SELECT Foo {[IS Bar].(bar)};
        """

    def test_edgeql_syntax_shape_19(self):
        """
            SELECT
                test::Issue {
                    number
                }
            FILTER
                (((test::Issue)).number) = '1';

            SELECT
                (test::Issue) {
                    number
                }
            FILTER
                (((test::Issue)).number) = '1';

% OK %

            SELECT
                test::Issue {
                    number
                }
            FILTER
                (test::Issue.number = '1');

            SELECT
                test::Issue {
                    number
                }
            FILTER
                (test::Issue.number = '1');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*AT", line=6, col=29)
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
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape_26(self):
        """
        SELECT Issue{
            name,
            related_to *,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape_27(self):
        """
        SELECT Issue{
            name,
            related_to *5,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape_28(self):
        """
        SELECT Issue{
            name,
            related_to *-1,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape_29(self):
        """
        SELECT Issue{
            name,
            related_to *$var,
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

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token: <Token COLON ":">',
                  line=3, col=16)
    def test_edgeql_syntax_struct_03(self):
        """
        SELECT (
            foo: 1,
            bar := 3
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token: <Token COLON ":">',
                  line=3, col=16)
    def test_edgeql_syntax_struct_04(self):
        """
        SELECT (
            foo: (
                bar: 42
            )
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token: <Token COLON ":">',
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
            foo := [
                'bar' -> 42
            ]
        );
        """

    def test_edgeql_syntax_struct_07(self):
        """
        SELECT (
            # unreserved keywords
            abstract := 1,
            action := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*ALL", line=4, col=13)
    def test_edgeql_syntax_struct_08(self):
        """
        SELECT (
            # reserved keywords
            all := 1,
            select := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*TURNSTILE", line=4, col=20)
    def test_edgeql_syntax_struct_09(self):
        """
        SELECT (
            # reserved keywords
            select := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*TURNSTILE", line=2, col=22)
    def test_edgeql_syntax_struct_10(self):
        """
        SELECT (1, a := 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*ICONST", line=2, col=25)
    def test_edgeql_syntax_struct_11(self):
        """
        SELECT (a := 1, 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*RPAREN", line=2, col=28)
    def test_edgeql_syntax_struct_12(self):
        """
        SELECT (a := 1, foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*DOT", line=2, col=28)
    def test_edgeql_syntax_struct_13(self):
        """
        SELECT (a := 1, foo.bar);
        """

    def test_edgeql_syntax_path_01(self):
        """
        SELECT Foo.bar;
        SELECT Foo.>bar;
        SELECT Foo.<bar;
        SELECT Foo.bar@spam;
        SELECT Foo.>bar@spam;
        SELECT Foo.<bar@spam;
        SELECT Foo.bar[IS Baz];
        SELECT Foo.>bar[IS Baz];
        SELECT Foo.<bar[IS Baz];

% OK %

        SELECT Foo.bar;
        SELECT Foo.bar;
        SELECT Foo.<bar;
        SELECT Foo.bar@spam;
        SELECT Foo.bar@spam;
        SELECT Foo.<bar@spam;
        SELECT Foo.bar[IS Baz];
        SELECT Foo.bar[IS Baz];
        SELECT Foo.<bar[IS Baz];
        """

    def test_edgeql_syntax_path_02(self):
        """
        SELECT Foo.event;
        SELECT Foo.>event;
        SELECT Foo.<event;
        SELECT Foo.event@action;
        SELECT Foo.>event@action;
        SELECT Foo.<event@action;
        SELECT Foo.event[IS Action];
        SELECT Foo.>event[IS Action];
        SELECT Foo.<event[IS Action];

% OK %

        SELECT Foo.event;
        SELECT Foo.event;
        SELECT Foo.<event;
        SELECT Foo.event@action;
        SELECT Foo.event@action;
        SELECT Foo.<event@action;
        SELECT Foo.event[IS Action];
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
        SELECT Foo.bar[IS All];
        """

    # This is actually odd, but legal, simply filtering by type a
    # particular array element.
    #
    def test_edgeql_syntax_path_09(self):
        """
        SELECT Foo.bar[2][IS Baz];

% OK %

        SELECT (Foo.bar)[2][IS Baz];
        """

    def test_edgeql_syntax_path_10(self):
        """
        SELECT (Foo.bar)[2:4][IS Baz];
        """

    def test_edgeql_syntax_path_11(self):
        """
        SELECT (Foo.bar)[2:][IS Baz];
        """

    def test_edgeql_syntax_path_12(self):
        """
        SELECT (Foo.bar)[:2][IS Baz];
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

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*"."', line=2)
    def test_edgeql_syntax_path_17(self):
        """
        SELECT ..foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*DUNDERSOURCE',
                  line=2, col=20)
    def test_edgeql_syntax_path_18(self):
        """
        SELECT Foo.__source__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*DUNDERSUBJECT',
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

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*FCONST',
                  line=2, col=20)
    def test_edgeql_syntax_path_22(self):
        """
        SELECT TUP.0.2e2;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*DUNDERTYPE',
                  line=2, col=16)
    def test_edgeql_syntax_path_23(self):
        """
        SELECT __type__;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*DUNDERTYPE',
                  line=2, col=24)
    def test_edgeql_syntax_path_24(self):
        """
        SELECT Foo.bar@__type__;
        """

    def test_edgeql_syntax_type_interpretation_01(self):
        """
        SELECT Foo[IS Bar].spam;
        SELECT Foo[IS Bar].<ham;
        """

    def test_edgeql_syntax_type_interpretation_02(self):
        """
        SELECT (Foo + Bar)[IS Spam].ham;
        """

    def test_edgeql_syntax_map_01(self):
        """
        SELECT [
            'name' -> 'foo',
            'description' -> 'bar'
        ];
        SELECT [
            'name' -> 'baz'
        ];
        SELECT [
            'first' -> [
                'name' -> 'foo'
            ],
            'second' -> [
                'description' -> 'bar'
            ]
        ];
        """

    def test_edgeql_syntax_map_02(self):
        """
        SELECT [
            'foo' -> [
                bar -> 42
            ]
        ];
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

    def test_edgeql_syntax_map_04(self):
        """
        SELECT ['foo'-> 42, 'bar'-> 'something'];
        SELECT ['foo'-> 42, 'bar'-> 'something']['foo'];
        SELECT (['foo'-> 42, 'bar'-> 'something'])['foo'];

% OK %

        SELECT ['foo'-> 42, 'bar'-> 'something'];
        SELECT (['foo'-> 42, 'bar'-> 'something'])['foo'];
        SELECT (['foo'-> 42, 'bar'-> 'something'])['foo'];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'unexpected map item in array',
                  line=2, col=23)
    def test_edgeql_syntax_map_05(self):
        """
        SELECT [1, 2, 1->2, 3];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'unexpected array item in map',
                  line=2, col=29)
    def test_edgeql_syntax_map_06(self):
        """
        SELECT [1->1, 2->2, 1, 3];
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
                  "Unexpected token.*LBRACE", line=3, col=19)
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
        SELECT <array<int[2][3]>>$1;
        """

    def test_edgeql_syntax_cast_06(self):
        """
        SELECT <map<str, int>>$1;
        """

    def test_edgeql_syntax_cast_07(self):
        """
        SELECT <tuple>$1;
        SELECT <tuple<Foo, int, str>>$1;
        SELECT <tuple<obj: Foo, count: int, name: str>>$1;
        """

    def test_edgeql_syntax_cardinality_01(self):
        """
        WITH CARDINALITY '1'
        SELECT User.name FILTER (User.name = 'special');
        """

    def test_edgeql_syntax_cardinality_02(self):
        """
        WITH CARDINALITY '*'
        SELECT User.name FILTER (User.name = 'special');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=26)
    def test_edgeql_syntax_cardinality_03(self):
        """
        WITH CARDINALITY 'M'
        SELECT User.name FILTER (User.name = 'special');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=9)
    def test_edgeql_syntax_cardinality_04(self):
        """
        WITH CARDINALITY '1' CREATE ACTION sample;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=9)
    def test_edgeql_syntax_cardinality_05(self):
        """
        WITH
            CARDINALITY '1',
            MODULE test
        DROP ACTION sample;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=13)
    def test_edgeql_syntax_cardinality_06(self):
        """
        WITH
            CARDINALITY '1',
            CARDINALITY '1'
        SELECT 1;
        """

    def test_edgeql_syntax_with_01(self):
        """
        WITH
            MODULE test,
            extra := MODULE lib.extra,
            foo := Bar.foo,
            baz := (SELECT extra::Foo.baz)
        SELECT Bar {
            spam,
            ham := baz
        } FILTER (foo = 'special');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=6, col=15)
    def test_edgeql_syntax_with_02(self):
        """
        WITH
            MODULE test,
            foo := Bar.foo,
            baz := (SELECT Foo.baz)
        COMMIT;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=16)
    def test_edgeql_syntax_with_03(self):
        """
        WITH
            MODULE test
        CREATE DATABASE sample;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=14)
    def test_edgeql_syntax_with_04(self):
        """
        WITH
            MODULE test
        DROP DATABASE sample;
        """

    def test_edgeql_syntax_with_05(self):
        """
        WITH MODULE test CREATE ACTION sample;
        WITH MODULE test DROP ACTION sample;
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
        WITH MODULE test
        SELECT 42;
        WITH MODULE test
        SELECT User{name};
        WITH MODULE test
        SELECT User{name}
            FILTER (User.age > 42);
        WITH MODULE test
        SELECT User{name}
            ORDER BY User.name ASC;
        WITH MODULE test
        SELECT User{name}
            OFFSET 2;
        WITH MODULE test
        SELECT User{name}
            LIMIT 5;
        WITH MODULE test
        SELECT User{name}
            OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select_06(self):
        """
        WITH MODULE test
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
        WITH MODULE test
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

    def test_edgeql_syntax_group_01(self):
        """
        GROUP User
        USING _ :=  User.name
        BY _
        INTO User
        UNION count(User.tasks);
        """

    def test_edgeql_syntax_group_02(self):
        """
        # define and mask aliases
        WITH
            _1 := User
        GROUP _2 := _1
        USING _ :=  _2.name
        BY _
        INTO U
        UNION _3 := (
            num_tasks := count(DISTINCT _2.tasks)
        )
        ORDER BY _3.num_tasks ASC;
        """

    def test_edgeql_syntax_group_03(self):
        """
        GROUP User := User
        USING G :=  User.name
        BY G
        INTO User
        UNION (
            name := G,
            num_tasks := count(User.tasks)
        );
        """

    def test_edgeql_syntax_group_04(self):
        """
        GROUP F := User.friends
        USING G :=  F.name
        BY G
        INTO F
        UNION (
            name := G,
            num_tasks := count(F.tasks)
        );
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
        BY
            G1, G2, G3, G4
        INTO U
        UNION (
            name := G1,
            num_tasks := count(U.tasks)
        );
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
        SELECT DISTINCT {1, 2, 2, 3};
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
        WITH MODULE test
        INSERT Foo;
        """

    def test_edgeql_syntax_insert_04(self):
        """
        WITH MODULE test
        INSERT Foo{bar := 42};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'insert expression must be an object type or a view',
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

    def test_edgeql_syntax_insert_12(self):
        """
        INSERT Foo{
            bar := 42,
            baz: Baz{
                spam := 'ham'
            }
        };
        """

    def test_edgeql_syntax_insert_13(self):
        """
        INSERT Foo{
            bar := 42,
            baz := (SELECT Baz FILTER (Baz.spam = 'ham'))
        };
        """

    def test_edgeql_syntax_insert_14(self):
        """
        INSERT Foo{
            bar := 42,
            baz: Baz{
                spam := 'ham',
                @weight := 2,
            }
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

    def test_edgeql_syntax_delete_01(self):
        """
        DELETE Foo;
        """

    def test_edgeql_syntax_delete_02(self):
        """
        WITH MODULE test
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

    def test_edgeql_syntax_update_01(self):
        """
        UPDATE Foo SET {bar := 42};
        UPDATE Foo FILTER (Foo.bar = 24) SET {bar := 42};
        """

    def test_edgeql_syntax_update_02(self):
        """
        WITH MODULE test
        UPDATE Foo SET {bar := 42};
        WITH MODULE test
        UPDATE Foo FILTER (Foo.bar = 24) SET {bar := 42};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token: <Token SEMICOLON ";">',
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
            UPDATE Bar SET {name := (name + 'bar')}
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
        SELECT a ?? x;
        SELECT a ?? x.a;
        SELECT a ?? x.a[IS ABC];
        SELECT (a ?? x.a[IS ABC]@aaa + 1);
        """

    def test_edgeql_syntax_function_01(self):
        """
        SELECT foo();
        SELECT bar(User.name);
        SELECT baz(User.name, User.age);
        SELECT lower(User.name);
        """

    def test_edgeql_syntax_function_02(self):
        """
        SELECT lower(string := User.name);
        SELECT baz(age := User.age, of := User.name);
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
                  r'Unexpected token:.*OVER', line=2, col=36)
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
                  r'Unexpected token:.*ALL', line=2, col=22)
    def test_edgeql_syntax_function_05(self):
        """
        SELECT count(ALL 1);
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
        CREATE DATABASE all;
        CREATE DATABASE abstract;

% OK %

        CREATE DATABASE `all`;
        CREATE DATABASE abstract;
        """

    def test_edgeql_syntax_ddl_database_05(self):
        """
        DROP DATABASE all;
        DROP DATABASE abstract;

% OK %

        DROP DATABASE `all`;
        DROP DATABASE abstract;
        """

    def test_edgeql_syntax_ddl_delta_01(self):
        """
        ALTER MIGRATION test::d_links01_0 {
            RENAME TO test::pretty_name;
        };

% OK %

        ALTER MIGRATION test::d_links01_0
            RENAME TO test::pretty_name;
        """

    def test_edgeql_syntax_ddl_delta_02(self):
        """
        CREATE MIGRATION test::d_links01_0 TO eschema $$type Foo$$;
        ALTER MIGRATION test::d_links01_0
            RENAME TO test::pretty_name;
        COMMIT MIGRATION test::d_links01_0;
        DROP MIGRATION test::d_links01_0;
        """

    def test_edgeql_syntax_ddl_delta_03(self):
        """
        CREATE MIGRATION test::d_links01_0 TO eschema $$type Foo$$;
        CREATE MIGRATION test::d_links01_0 TO ESCHEMA $$type Foo$$;
        CREATE MIGRATION test::d_links01_0 TO ESchema $$type Foo$$;

% OK %

        CREATE MIGRATION test::d_links01_0 TO eschema $$type Foo$$;
        CREATE MIGRATION test::d_links01_0 TO eschema $$type Foo$$;
        CREATE MIGRATION test::d_links01_0 TO eschema $$type Foo$$;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'unknown migration language: BadLang', line=2, col=47)
    def test_edgeql_syntax_ddl_delta_04(self):
        """
        CREATE MIGRATION test::d_links01_0 TO BadLang $$type Foo$$;
        """

    def test_edgeql_syntax_ddl_action_01(self):
        """
        CREATE ACTION std::restrict {
            SET title := 'Abort the event if a pointer exists';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=13)
    def test_edgeql_syntax_ddl_action_02(self):
        """
        CREATE ACTION std::restrict
            SET title := 'Abort the event if a pointer exists';
        """

    # TODO: remove this test once the entire grammar is converted
    def test_edgeql_syntax_ddl_aggregate_00(self):
        """
        CREATE FUNCTION std::sum($v: SET OF std::int64)
            -> std::int64
            FROM SQL FUNCTION 'sum';
        """

    def test_edgeql_syntax_ddl_aggregate_01(self):
        """
        CREATE FUNCTION std::sum($v: SET OF std::int64)
            -> std::int64 {
            INITIAL VALUE 0;
            FROM SQL FUNCTION 'test';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_02(self):
        """
        CREATE FUNCTION std::sum(SET OF std::int64)
            -> std::int64 {
            INITIAL VALUE 0;
            FROM SQL FUNCTION 'sum';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_03(self):
        """
        CREATE FUNCTION std::sum($integer: SET OF std::int64)
            -> std::int64 {
            INITIAL VALUE 0;
            FROM SQL FUNCTION 'sum';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_04(self):
        """
        CREATE FUNCTION std::sum($integer: SET OF std::int64)
            -> std::int64 {
            INITIAL VALUE 0;
            FROM SQL FUNCTION 'sum';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=5)
    def test_edgeql_syntax_ddl_aggregate_06(self):
        """
        CREATE FUNCTION foo($string: SET OF std::str)
            -> std::int64 {
            INITIAL VALUE 0;
            FROM AAA FUNCTION 'foo';
        };
        """

    def test_edgeql_syntax_ddl_aggregate_08(self):
        """
        CREATE FUNCTION std::count($expression: SET OF std::any)
            -> std::int64 {
            INITIAL VALUE 0;
            FROM SQL FUNCTION 'count';
        };
        """

    def test_edgeql_syntax_ddl_scalar_01(self):
        """
        CREATE ABSTRACT SCALAR TYPE std::any;
        CREATE SCALAR TYPE std::typeref;
        CREATE SCALAR TYPE std::scalarref EXTENDING std::typeref;
        """

    def test_edgeql_syntax_ddl_attribute_01(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::paramtypes map<std::str, std::typeref>;
        """

    def test_edgeql_syntax_ddl_attribute_02(self):
        """
        CREATE ABSTRACT ATTRIBUTE stdattrs::precision array<std::int64>;
        """

    def test_edgeql_syntax_ddl_attribute_03(self):
        # test parsing of array types
        #
        """
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[][]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[][][]>;

        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[3]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[4][5]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[3][4][5]>;

        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[][5]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[4][]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[][4][5]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[3][4][]>;
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64[][4][]>;
        """

    def test_edgeql_syntax_ddl_attribute_04(self):
        # test parsing of map types
        #
        """
        CREATE ABSTRACT ATTRIBUTE std::foo map<int64, str>;
        CREATE ABSTRACT ATTRIBUTE std::foo map<array<int64>, str>;
        CREATE ABSTRACT ATTRIBUTE std::foo map<array<int64>, tuple<str, str>>;
        CREATE ABSTRACT ATTRIBUTE std::foo map<int64, foo::Bar>;
        """

    def test_edgeql_syntax_ddl_attribute_05(self):
        # test parsing of tuple types
        #
        """
        CREATE ABSTRACT ATTRIBUTE std::foo tuple;
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<float64>;
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<int64, str>;
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<array<int64>, str>;
        CREATE ABSTRACT ATTRIBUTE std::foo
            tuple<array<int64>, tuple<str, str>>;
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<int64, foo::Bar>;

        CREATE ABSTRACT ATTRIBUTE std::foo tuple<count: int64, name: str>;

        CREATE ABSTRACT ATTRIBUTE std::foo tuple<Baz, map<int64, str>>;
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<Baz, map<array<int64>, str>>;
        CREATE ABSTRACT ATTRIBUTE std::foo
            tuple<Baz, map<array<int64>, tuple<str, str>>>;
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<Baz, map<int64, foo::Bar>>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*map", line=2, col=44)
    def test_edgeql_syntax_ddl_attribute_06(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo map;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*>", line=2, col=53)
    def test_edgeql_syntax_ddl_attribute_07(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo map<int64>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*COMMA", line=2, col=58)
    def test_edgeql_syntax_ddl_attribute_08(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo map<int64, str, float64>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*array", line=2, col=44)
    def test_edgeql_syntax_ddl_attribute_09(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo array;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*COMMA", line=2, col=55)
    def test_edgeql_syntax_ddl_attribute_10(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo array<int64, int64, int64>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*<", line=2, col=55)
    def test_edgeql_syntax_ddl_attribute_11(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo array<array<int64[]>>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*COLON", line=2, col=60)
    def test_edgeql_syntax_ddl_attribute_12(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<int64, foo:int64>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*>", line=2, col=64)
    def test_edgeql_syntax_ddl_attribute_13(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::foo tuple<foo:int64, str>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*SEMICOLON", line=2, col=50)
    def test_edgeql_syntax_ddl_attribute_14(self):
        """
        CREATE ABSTRACT ATTRIBUTE std::paramtypes;
        """

    def test_edgeql_syntax_ddl_constraint_01(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum(array<std::any>)
            EXTENDING std::constraint
        {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, __subject__);
        };
        """

    def test_edgeql_syntax_ddl_constraint_02(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum(array<std::any>) {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, __subject__);
        };
        """

    def test_edgeql_syntax_ddl_constraint_03(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, __subject__);
        };
        """

    def test_edgeql_syntax_ddl_constraint_04(self):
        """
        CREATE ABSTRACT CONSTRAINT std::enum() {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, __subject__);
        };

% OK %

        CREATE ABSTRACT CONSTRAINT std::enum {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, __subject__);
        };
        """

    def test_edgeql_syntax_ddl_constraint_05(self):
        """
        CREATE SCALAR TYPE std::decimal_rounding_t EXTENDING std::str {
            CREATE CONSTRAINT std::enum(['a', 'b']);
        };
        """

    def test_edgeql_syntax_ddl_constraint_06(self):
        """
        CREATE ABSTRACT CONSTRAINT std::length ON (len(<std::str>__subject__))
            EXTENDING std::constraint
        {
            SET errmessage := 'invalid {subject}';
        };
        """

    def test_edgeql_syntax_ddl_constraint_07(self):
        """
        CREATE SCALAR TYPE std::decimal_rounding_t EXTENDING std::str {
            CREATE CONSTRAINT max(99) ON (<int64>__subject__);
        };
        """

    def test_edgeql_syntax_ddl_function_01(self):
        """
        CREATE FUNCTION std::strlen($string: std::str) -> std::int64
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_02(self):
        """
        CREATE FUNCTION std::strlen(std::str) -> std::int64
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_03(self):
        """
        CREATE FUNCTION std::strlen($string: std::str) -> std::int64
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_04(self):
        """
        CREATE FUNCTION std::strlen($string: std::str, $integer: std::int64)
            -> std::int64
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_05(self):
        """
        CREATE FUNCTION std::strlen($string: std::str, std::int64)
            -> std::int64
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_06(self):
        """
        CREATE FUNCTION std::strlen($string: std::str = '1')
            -> std::int64
            FROM SQL FUNCTION 'strlen';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'non-default argument follows', line=2, col=62)
    def test_edgeql_syntax_ddl_function_07(self):
        """
        CREATE FUNCTION std::strlen($string: std::str = '1', $abc: std::str)
            -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'non-variadic argument follows', line=3, col=37)
    def test_edgeql_syntax_ddl_function_08(self):
        """
        CREATE FUNCTION std::strlen($string: VARIADIC std::str = '1',
                                    $abc: std::str)
            -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'more than one variadic argument', line=3, col=37)
    def test_edgeql_syntax_ddl_function_09(self):
        """
        CREATE FUNCTION std::strlen($string: VARIADIC std::str = '1',
                                    $abc: VARIADIC std::str)
            -> std::int64;
        """

    def test_edgeql_syntax_ddl_function_10(self):
        """
        CREATE FUNCTION std::strlen(std::str = '1', VARIADIC std::str)
            -> std::int64
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function_11(self):
        """
        CREATE FUNCTION no_params() -> std::int64
        FROM EdgeQL $$ SELECT 1 $$;
        """

    def test_edgeql_syntax_ddl_function_13(self):
        """
        CREATE FUNCTION foo($string: std::str) -> tuple<bar: std::int64>
        FROM EDGEQL $$ SELECT (bar := 123) $$;
        """

    def test_edgeql_syntax_ddl_function_14(self):
        """
        CREATE FUNCTION foo($string: std::str)
        -> tuple<
            bar: std::int64,
            baz: std::str
        > FROM EdgeQL $$ SELECT smth() $$;
        """
    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=3)
    def test_edgeql_syntax_ddl_function_16(self):
        """
        CREATE FUNCTION foo($string: std::str)
        -> std::int64 FROM AAA FUNCTION 'foo';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=3)
    def test_edgeql_syntax_ddl_function_19(self):
        """
        CREATE FUNCTION foo($string: std::str)
        -> std::int64 FROM AAA 'code';
        """

    def test_edgeql_syntax_ddl_function_20(self):
        """
        CREATE FUNCTION foo() -> std::int64 FROM SQL 'SELECT 1';

% OK %

        CREATE FUNCTION foo() -> std::int64 FROM SQL $$SELECT 1$$;
        """

    def test_edgeql_syntax_ddl_function_21(self):
        """
        CREATE FUNCTION foo() -> std::int64 FROM SQL FUNCTION 'aaa';
        """

    def test_edgeql_syntax_ddl_function_24(self):
        """
        CREATE FUNCTION foo() -> std::str FROM SQL $a$SELECT $$foo$$$a$;
        """

    def test_edgeql_syntax_ddl_function_25(self):
        """
        CREATE FUNCTION foo() -> std::str {
            SET description := 'aaaa';
            FROM SQL $a$SELECT $$foo$$$a$;
        };
        """

    def test_edgeql_syntax_ddl_function_26(self):
        """
        CREATE FUNCTION foo() -> std::str {
            SET volatility := 'volatile';
            SET description := 'aaaa';
            FROM SQL $a$SELECT $$foo$$$a$;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "FROM clause is missing", line=2)
    def test_edgeql_syntax_ddl_function_27(self):
        """
        CREATE FUNCTION foo() -> std::str {
            SET description := 'aaaa';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "more than one FROM clause", line=5)
    def test_edgeql_syntax_ddl_function_28(self):
        """
        CREATE FUNCTION foo() -> std::str {
            FROM SQL 'SELECT 1';
            SET description := 'aaaa';
            FROM SQL 'SELECT 2';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r'Unexpected token:.*RPAREN',
                  line=3, col=52)
    def test_edgeql_syntax_ddl_function_30(self):
        """
        CREATE FUNCTION std::foobar($arg1: str, $arg2: str = 'DEFAULT',
                                    $arg3: VARIADIC)
            -> std::int64
            FROM EdgeQL $$$$;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*VARIADIC', line=2, col=41)
    def test_edgeql_syntax_ddl_function_31(self):
        """
        CREATE FUNCTION std::foo(SET OF VARIADIC std::str) -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*SET', line=2, col=43)
    def test_edgeql_syntax_ddl_function_32(self):
        """
        CREATE FUNCTION std::foo(VARIADIC SET OF std::str) -> std::int64;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*SET', line=2, col=49)
    def test_edgeql_syntax_ddl_function_33(self):
        """
        CREATE FUNCTION std::foo($bar: VARIADIC SET OF std::str) -> std::int64;
        """

    def test_edgeql_syntax_ddl_function_34(self):
        """
        CREATE FUNCTION foo(OPTIONAL std::str) ->
            std::int64 FROM SQL FUNCTION 'aaa';
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

    def test_edgeql_syntax_ddl_type_01(self):
        """
        CREATE ABSTRACT TYPE schema::Type EXTENDING schema::Object;
        """

    def test_edgeql_syntax_ddl_type_02(self):
        """
        CREATE TYPE schema::TypeElement {
            CREATE REQUIRED LINK schema::type -> schema::Type;
            CREATE REQUIRED LINK schema::num -> std::int64;
            CREATE LINK schema::name -> std::str;
        };
        """

    def test_edgeql_syntax_ddl_type_03(self):
        """
        ALTER TYPE schema::Object {
            CREATE LINK schema::attributes -> schema::Attribute {
                SET cardinality := '**';
            };
        };

% OK %

        ALTER TYPE schema::Object
            CREATE LINK schema::attributes -> schema::Attribute {
                SET cardinality := '**';
            };
        """

    def test_edgeql_syntax_set_command_01(self):
        """
        SET MODULE default;

% OK %

        SET MODULE default;
        """

    def test_edgeql_syntax_set_command_02(self):
        """
        SET foo := MODULE default;

% OK %

        SET foo := MODULE default;
        """

    def test_edgeql_syntax_set_command_03(self):
        """
        SET MODULE default, foo := (SELECT User);

% OK %

        SET MODULE default, foo := (SELECT User);
        """
