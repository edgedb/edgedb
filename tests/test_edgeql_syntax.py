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
    def test_edgeql_syntax_empty01(self):
        """"""

    def test_edgeql_syntax_empty02(self):
        """# only comment"""

    def test_edgeql_syntax_empty03(self):
        """

        # only comment

        """

    def test_edgeql_syntax_empty04(self):
        """;
% OK %  """

    def test_edgeql_syntax_empty05(self):
        """;# only comment
% OK %  """

    def test_edgeql_syntax_empty06(self):
        """
        ;
        # only comment
        ;
% OK %
        """

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
        """
        SELECT 0;
        SELECT 1;
        SELECT +7;
        SELECT -7;
        SELECT 551;
        """

    def test_edgeql_syntax_contants02(self):
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

    def test_edgeql_syntax_contants03(self):
        """
        SELECT 3.5432;
        SELECT +3.5432;
        SELECT -3.5432;
        """

    def test_edgeql_syntax_contants04(self):
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

    def test_edgeql_syntax_contants05(self):
        """
        SELECT TRUE;
        SELECT FALSE;
        """

    def test_edgeql_syntax_contants06(self):
        """
        SELECT $1;
        SELECT $123;
        SELECT $somevar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unknown token', line=2, col=16)
    def test_edgeql_syntax_contants07(self):
        """
        SELECT 02;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=12)
    def test_edgeql_syntax_ops01(self):
        """SELECT 40 >> 2;"""

    @tb.must_fail(errors.EdgeQLSyntaxError, line=1, col=14)
    def test_edgeql_syntax_ops02(self):
        """SELECT 40 << 2;"""

    def test_edgeql_syntax_ops03(self):
        """
        SELECT (40 <= 2);
        SELECT (40 >= 2);
        """

    def test_edgeql_syntax_ops04(self):
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

    def test_edgeql_syntax_ops05(self):
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

    def test_edgeql_syntax_ops06(self):
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
    def test_edgeql_syntax_ops07(self):
        """
        SELECT 40 == 2;
        """

    def test_edgeql_syntax_ops08(self):
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

    def test_edgeql_syntax_ops09(self):
        """
        SELECT (Foo.foo AND Foo.bar);
        SELECT (Foo.foo OR Foo.bar);
        SELECT NOT Foo.foo;
        """

    def test_edgeql_syntax_ops10(self):
        """
        SELECT (User.name IN ['Alice', 'Bob']);
        SELECT (User.name NOT IN ['Alice', 'Bob']);
        SELECT (User.name IS (std::str));
        SELECT (User IS SystemUser);
        SELECT (User.name IS NOT (std::str));
        SELECT (User IS NOT SystemUser);
        """

    def test_edgeql_syntax_ops11(self):
        """
        SELECT (User.name LIKE 'Al%');
        SELECT (User.name ILIKE 'al%');
        SELECT (User.name NOT LIKE 'Al%');
        SELECT (User.name NOT ILIKE 'al%');
        """

    def test_edgeql_syntax_ops12(self):
        """
        SELECT EXISTS (User.groups.description);
        """

    def test_edgeql_syntax_ops13(self):
        """
        SELECT (User.name @@ 'bob');
        SELECT (User.name MATCHES '^[[:lower:]]+$');
        SELECT (User.name NOT MATCHES 'don');
        """

    def test_edgeql_syntax_ops14(self):
        """
        SELECT -1 + 2 * 3 - 5 - 6 / 2 > 0 OR 25 % 4 = 3 AND 42 IN [12, 42, 14];

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
                (42 IN [12, 42, 14])
            )
        );
        """

    def test_edgeql_syntax_ops15(self):
        """
        SELECT
            ((-1 + 2) * 3 - (5 - 6) / 2 > 0 OR 25 % 4 = 3)
            AND 42 IN [12, 42, 14];

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
            (42 IN [12, 42, 14])
        );
        """

    def test_edgeql_syntax_ops16(self):
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

    def test_edgeql_syntax_ops17(self):
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

    def test_edgeql_syntax_ops18(self):
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
    def test_edgeql_syntax_ops19(self):
        """
        SELECT >=1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*\*', line=2, col=16)
    def test_edgeql_syntax_ops20(self):
        """
        SELECT *1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unknown token.*~', line=2, col=16)
    def test_edgeql_syntax_ops21(self):
        """
        SELECT ~1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*>', line=2, col=16)
    def test_edgeql_syntax_ops22(self):
        """
        SELECT >1;
        """

    def test_edgeql_syntax_ops23(self):
        """
        SELECT (Foo.a ?= Foo.b);
        SELECT (Foo.b ?!= Foo.b);
        """

    def test_edgeql_syntax_list01(self):
        """
        SELECT (some_list_fn())[2];
        SELECT (some_list_fn())[2:4];
        SELECT (some_list_fn())[2:];
        SELECT (some_list_fn())[:4];
        SELECT (some_list_fn())[-1:];
        SELECT (some_list_fn())[:-1];
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

        SELECT action;
        SELECT action;
        SELECT (event::action);
        SELECT (event::action);
        SELECT (event::action);
        SELECT (event::action);
        """

    def test_edgeql_syntax_name04(self):
        """
        SELECT (event::select);
        SELECT (event::`select`);
        SELECT (`event`::select);
        SELECT (`event`::`select`);

% OK %

        SELECT (event::`select`);
        SELECT (event::`select`);
        SELECT (event::`select`);
        SELECT (event::`select`);
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

    def test_edgeql_syntax_name11(self):
        # illegal semantically, but syntactically valid
        """
        SELECT @event;
        """

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

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_name14(self):
        """
        SELECT Foo.`@event`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=24)
    def test_edgeql_syntax_name15(self):
        """
        SELECT (event::`@event`);
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

    @tb.must_fail(errors.EdgeQLSyntaxError, line=6, col=22)
    def test_edgeql_syntax_shape09(self):
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
    def test_edgeql_syntax_shape10(self):
        """
        SELECT Foo {
            bar: {
                baz,
                boo
            } FILTER spam = 'bad',
            `@foo`:= 42
        };
        """

    # NOTE: this is syntactically allowed, but the compiler should
    # throw an error
    def test_edgeql_syntax_shape11(self):
        """
        SELECT Foo {
            __class__.name
        };
        """

    def test_edgeql_syntax_shape12(self):
        """
        SELECT Foo {
            __class__: {
                name,
            }
        };
        """

    def test_edgeql_syntax_shape13(self):
        """
        SELECT Foo {
            __class__: {
                name,
                description,
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*TURNSTILE", line=3, col=18)
    def test_edgeql_syntax_shape14(self):
        """
        SELECT {
            name := 'foo',
            description := 'bar'
        };
        """

    def test_edgeql_syntax_shape19(self):
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
                (((test::Issue)).(number)) = '1';

            SELECT
                test::Issue {
                    test::number
                }
            FILTER
                (((test::Issue)).(test::number)) = '1';

% OK %

            SELECT
                (test::Issue) {
                    number
                }
            FILTER
                ((test::Issue).number = '1');

            SELECT
                (test::Issue) {
                    number
                }
            FILTER
                ((test::Issue).number = '1');

            SELECT
                (test::Issue) {
                    (test::number)
                }
            FILTER
                ((test::Issue).(test::number) = '1');

        """

    def test_edgeql_syntax_shape20(self):
        """
        INSERT Foo{
            bar: {
                @weight,
                BarLink@special,
            }
        };
        """

    def test_edgeql_syntax_shape21(self):
        """
        INSERT Foo{
            bar := 'some_string_val' {
                @weight := 3
            }
        };
        """

    def test_edgeql_syntax_shape23(self):
        """
        SELECT 'Foo' {
            bar := 42
        };
        """

    def test_edgeql_syntax_shape24(self):
        """
        SELECT Foo {
            spam
        } {
            bar := 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=24)
    def test_edgeql_syntax_shape25(self):
        """
        SELECT Foo.bar AS bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape26(self):
        """
        SELECT Issue{
            name,
            related_to *,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape27(self):
        """
        SELECT Issue{
            name,
            related_to *5,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape28(self):
        """
        SELECT Issue{
            name,
            related_to *-1,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*STAR", line=4, col=24)
    def test_edgeql_syntax_shape29(self):
        """
        SELECT Issue{
            name,
            related_to *$var,
        };
        """

    def test_edgeql_syntax_shape32(self):
        """
        SELECT User{
            name,
            <owner: LogEntry {
                body
            },
        };
        """

    def test_edgeql_syntax_shape33(self):
        """
        SELECT User {
            name,
            groups: {
                name,
            } FILTER (.name = 'admin')
        };
        """

    def test_edgeql_syntax_shape34(self):
        """
        SELECT User{
            name,
            <owner: LogEntry {
                body
            },
        } FILTER (.<owner.body = 'foo');
        """

    def test_edgeql_syntax_shape35(self):
        """
        SELECT User {
            name,
            groups: {
                name,
            } FILTER (@special = True)
        };
        """

    def test_edgeql_syntax_shape36(self):
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

    def test_edgeql_syntax_shape37(self):
        """
        SELECT Foo {
            foo FILTER (foo > 3),
            bar ORDER BY bar DESC,
            baz OFFSET 1 LIMIT 3,
        };
        """

    def test_edgeql_syntax_shape38(self):
        """
        SELECT Foo {
            spam: {
                @foo FILTER (foo > 3),
                @bar ORDER BY bar DESC,
                @baz OFFSET 1 LIMIT 3,
            },
        };
        """

    def test_edgeql_syntax_struct01(self):
        """
        SELECT (
            foo := 1,
            bar := 2
        );
        """

    def test_edgeql_syntax_struct02(self):
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
    def test_edgeql_syntax_struct03(self):
        """
        SELECT (
            foo: 1,
            bar := 3
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token: <Token COLON ":">',
                  line=3, col=16)
    def test_edgeql_syntax_struct04(self):
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
    def test_edgeql_syntax_struct05(self):
        """
        SELECT (
            foo: (
                'bar': 42
            )
        );
        """

    def test_edgeql_syntax_struct06(self):
        """
        SELECT (
            foo := [
                'bar' -> 42
            ]
        );
        """

    def test_edgeql_syntax_struct07(self):
        """
        SELECT (
            # unreserved keywords
            abstract := 1,
            action := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*ALL", line=4, col=13)
    def test_edgeql_syntax_struct08(self):
        """
        SELECT (
            # reserved keywords
            all := 1,
            select := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*TURNSTILE", line=4, col=20)
    def test_edgeql_syntax_struct09(self):
        """
        SELECT (
            # reserved keywords
            select := 2
        );
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*TURNSTILE", line=2, col=22)
    def test_edgeql_syntax_struct10(self):
        """
        SELECT (1, a := 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*ICONST", line=2, col=25)
    def test_edgeql_syntax_struct11(self):
        """
        SELECT (a := 1, 2);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*RPAREN", line=2, col=28)
    def test_edgeql_syntax_struct12(self):
        """
        SELECT (a := 1, foo);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*DOT", line=2, col=28)
    def test_edgeql_syntax_struct13(self):
        """
        SELECT (a := 1, foo.bar);
        """

    def test_edgeql_syntax_path01(self):
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

    def test_edgeql_syntax_path02(self):
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

    def test_edgeql_syntax_path03(self):
        """
        SELECT Foo.(lib::bar);
        SELECT Foo.>(lib::bar);
        SELECT Foo.<(lib::bar);
        SELECT Foo.(lib::bar)@(lib::spam);
        SELECT Foo.>(lib::bar)@(lib::spam);
        SELECT Foo.<(lib::bar)@(lib::spam);
        SELECT Foo.(lib::bar)[IS lib::Baz];
        SELECT Foo.>(lib::bar)[IS lib::Baz];
        SELECT Foo.<(lib::bar)[IS lib::Baz];

% OK %

        SELECT Foo.(lib::bar);
        SELECT Foo.(lib::bar);
        SELECT Foo.<(lib::bar);
        SELECT Foo.(lib::bar)@(lib::spam);
        SELECT Foo.(lib::bar)@(lib::spam);
        SELECT Foo.<(lib::bar)@(lib::spam);
        SELECT Foo.(lib::bar)[IS lib::Baz];
        SELECT Foo.(lib::bar)[IS lib::Baz];
        SELECT Foo.<(lib::bar)[IS lib::Baz];
        """

    def test_edgeql_syntax_path04(self):
        """
        SELECT Foo[IS Bar];
        """

    def test_edgeql_syntax_path05(self):
        """
        SELECT Foo.bar@spam[IS Bar];
        """

    def test_edgeql_syntax_path06(self):
        """
        SELECT Foo.bar[IS To];  # unreserved keyword as concept name
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=30)
    def test_edgeql_syntax_path07(self):
        """
        SELECT Foo.bar[IS To To];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=27)
    def test_edgeql_syntax_path08(self):
        """
        SELECT Foo.bar[IS All];
        """

    # This is actually odd, but legal, simply filtering by type a
    # particular array element.
    #
    def test_edgeql_syntax_path09(self):
        """
        SELECT Foo.bar[2][IS Baz];

% OK %

        SELECT (Foo.bar)[2][IS Baz];
        """

    def test_edgeql_syntax_path10(self):
        """
        SELECT (Foo.bar)[2:4][IS Baz];
        """

    def test_edgeql_syntax_path11(self):
        """
        SELECT (Foo.bar)[2:][IS Baz];
        """

    def test_edgeql_syntax_path12(self):
        """
        SELECT (Foo.bar)[:2][IS Baz];
        """

    def test_edgeql_syntax_path13(self):
        """
        SELECT (Foo.bar)[IS Baz];
        SELECT Foo.(bar)[IS Baz];
        SELECT Foo.<(bar)[IS Baz];

% OK %

        SELECT Foo.bar[IS Baz];
        SELECT Foo.bar[IS Baz];
        SELECT Foo.<bar[IS Baz];
        """

    def test_edgeql_syntax_path14(self):
        """
        SELECT User.__class__.name LIMIT 1;
        """

    def test_edgeql_syntax_path15(self):
        """
        SELECT (42).foo;
% OK %
        SELECT (42).foo;
        """

    def test_edgeql_syntax_path16(self):
        # illegal semantically, but syntactically valid
        """
        SELECT .foo;
        SELECT .<foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, 'Unexpected token.*"."', line=2)
    def test_edgeql_syntax_path17(self):
        """
        SELECT ..foo;
        """

    def test_edgeql_syntax_type_interpretation01(self):
        """
        SELECT Foo[IS Bar].spam;
        SELECT Foo[IS Bar].<ham;
        """

    def test_edgeql_syntax_type_interpretation02(self):
        """
        SELECT (Foo + Bar)[IS Spam].ham;
        """

    def test_edgeql_syntax_map01(self):
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

    def test_edgeql_syntax_map02(self):
        """
        SELECT [
            'foo' -> [
                bar -> 42
            ]
        ];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=18)
    def test_edgeql_syntax_map03(self):
        """
        SELECT [
            'foo':= {
                bar := 42
            }
        ];
        """

    def test_edgeql_syntax_map04(self):
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
    def test_edgeql_syntax_map05(self):
        """
        SELECT [1, 2, 1->2, 3];
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'unexpected array item in map',
                  line=2, col=29)
    def test_edgeql_syntax_map06(self):
        """
        SELECT [1->1, 2->2, 1, 3];
        """

    def test_edgeql_syntax_sequence01(self):
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

    def test_edgeql_syntax_array01(self):
        """
        SELECT [1];
        SELECT [1, 2, 3, 4, 5];
        SELECT [User.name, User.description];
        SELECT [User.name, User.description, 'filler'];
        """

    def test_edgeql_syntax_array02(self):
        """
        SELECT [1, 2, 3, 4, 5][2];
        SELECT [1, 2, 3, 4, 5][2:4];

% OK %

        SELECT ([1, 2, 3, 4, 5])[2];
        SELECT ([1, 2, 3, 4, 5])[2:4];
        """

    def test_edgeql_syntax_array03(self):
        """
        SELECT ([1, 2, 3, 4, 5])[2];
        SELECT ([1, 2, 3, 4, 5])[2:4];
        SELECT ([1, 2, 3, 4, 5])[2:];
        SELECT ([1, 2, 3, 4, 5])[:2];
        SELECT ([1, 2, 3, 4, 5])[2:-1];
        SELECT ([1, 2, 3, 4, 5])[-2:];
        SELECT ([1, 2, 3, 4, 5])[:-2];
        """

    def test_edgeql_syntax_array04(self):
        """
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[Bar.setting];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[1:Bar.setting];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[Bar.setting:];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[:Bar.setting];
        SELECT ([Foo.bar, Foo.baz, Foo.spam, Foo.ham])[:-Bar.setting];
        """

    def test_edgeql_syntax_array05(self):
        """
        SELECT (get_nested_obj())['a']['b']['c'];
        """

    def test_edgeql_syntax_cast01(self):
        """
        SELECT <float> (SELECT User.age);
        """

    def test_edgeql_syntax_cast02(self):
        """
        SELECT <float> (((SELECT User.age)));

% OK %

        SELECT <float> (SELECT User.age);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*LBRACE", line=3, col=19)
    def test_edgeql_syntax_cast03(self):
        """
        SELECT
            <User {name, description}> [
                'name' -> 'Alice',
                'description' -> 'sample'
            ];
        """

    def test_edgeql_syntax_cast04(self):
        """
        SELECT -<int>{};
        """

    def test_edgeql_syntax_cast05(self):
        """
        SELECT <array<int>>$1;
        SELECT <array<int[2][3]>>$1;
        """

    def test_edgeql_syntax_cast06(self):
        """
        SELECT <map<str, int>>$1;
        """

    def test_edgeql_syntax_cast07(self):
        """
        SELECT <tuple>$1;
        SELECT <tuple<Foo, int, str>>$1;
        SELECT <tuple<obj: Foo, count: int, name: str>>$1;
        """

    def test_edgeql_syntax_cardinality01(self):
        """
        SELECT SINGLETON User.name FILTER (User.name = 'special');
        CREATE FUNCTION spam($foo: str) RETURNING SET OF str
            FROM EdgeQL $$ SELECT "a" $$;
        """

    def test_edgeql_syntax_with01(self):
        """
        WITH
            MODULE test,
            extra := MODULE lib.extra,
            foo := Bar.foo,
            baz := (SELECT (extra::Foo).baz)
        SELECT Bar {
            spam,
            ham := baz
        } FILTER (foo = 'special');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=6, col=9)
    def test_edgeql_syntax_with02(self):
        """
        WITH
            MODULE test,
            foo := Bar.foo,
            baz := (SELECT Foo.baz)
        COMMIT;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=16)
    def test_edgeql_syntax_with03(self):
        """
        WITH
            MODULE test
        CREATE DATABASE sample;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=14)
    def test_edgeql_syntax_with04(self):
        """
        WITH
            MODULE test
        DROP DATABASE sample;
        """

    def test_edgeql_syntax_with05(self):
        """
        WITH MODULE test CREATE ACTION sample;
        WITH MODULE test DROP ACTION sample;
        """

    def test_edgeql_syntax_with06(self):
        """
        WITH MODULE abstract SELECT Foo;
        WITH MODULE all SELECT Foo;
        WITH MODULE all.abstract.bar SELECT Foo;
        """

    def test_edgeql_syntax_with07(self):
        """
        WITH MODULE `all.abstract.bar` SELECT Foo;

% OK %

        WITH MODULE all.abstract.bar SELECT Foo;
        """

    def test_edgeql_syntax_with08(self):
        """
        WITH MODULE `~all.abstract.bar` SELECT Foo;
        """

    def test_edgeql_syntax_select01(self):
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

    def test_edgeql_syntax_select02(self):
        """
        SELECT User{name} ORDER BY User.name;
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name DESC;

% OK %

        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name DESC;
        """

    def test_edgeql_syntax_select03(self):
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

    def test_edgeql_syntax_select04(self):
        """
        SELECT
            User.name
        FILTER
            (User.age > 42)
        ORDER BY
            User.name ASC
        OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select05(self):
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

    def test_edgeql_syntax_select06(self):
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
    def test_edgeql_syntax_select07(self):
        """
        (SELECT User.name) OFFSET 2;
        """

    def test_edgeql_syntax_select08(self):
        """
        WITH MODULE test
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} OFFSET 2;
        SELECT User{name} LIMIT 2;
        SELECT User{name} OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select09(self):
        """
        SELECT Issue {name} ORDER BY Issue.priority.name ASC EMPTY FIRST;
        SELECT Issue {name} ORDER BY Issue.priority.name DESC EMPTY LAST;
        """

    def test_edgeql_syntax_select10(self):
        """
        SELECT User.name OFFSET $1;
        SELECT User.name LIMIT $2;
        SELECT User.name OFFSET $1 LIMIT $2;
        """

    def test_edgeql_syntax_select11(self):
        """
        SELECT User.name OFFSET Foo.bar;
        SELECT User.name LIMIT (Foo.bar * 10);
        SELECT User.name OFFSET Foo.bar LIMIT (Foo.bar * 10);
        """

    def test_edgeql_syntax_group01(self):
        """
        GROUP User
            BY User.name
            SELECT (
                name := User.name,
                num_tasks := count(ALL User.tasks)
            );
        """

    def test_edgeql_syntax_group02(self):
        """
        GROUP _1 := User
            BY _1.name
            SELECT _2 := (
                name := _1.name,
                num_tasks := count(DISTINCT _1.tasks)
            )
            ORDER BY _2.num_tasks ASC;
        """

    def test_edgeql_syntax_set01(self):
        """
        SELECT (1 UNION 2);
        """

    def test_edgeql_syntax_set02(self):
        """
        SELECT ((SELECT Foo) UNION (SELECT Bar));
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=9)
    def test_edgeql_syntax_set03(self):
        """
        (SELECT Foo) UNION (SELECT Bar);
        """

    def test_edgeql_syntax_set04(self):
        """
        SELECT 2 * (1 UNION 2 UNION 1);

% OK %

        SELECT (2 * ((1 UNION 2) UNION 1));
        """

    def test_edgeql_syntax_set05(self):
        """
        SELECT {};
        SELECT {1};
        SELECT {1, 2};
        SELECT {1, 2, {}, {1, 3}};
        SELECT {Foo.bar, Foo.baz};
        SELECT {Foo.bar, Foo.baz}.spam;
        """

    def test_edgeql_syntax_insert01(self):
        """
        INSERT Foo;
        SELECT (INSERT Foo);
        SELECT (INSERT Foo) {bar};
        """

    def test_edgeql_syntax_insert02(self):
        """
        INSERT Foo{bar := 42};
        SELECT (INSERT Foo{bar := 42});
        SELECT (INSERT Foo{bar := 42}) {bar};
        """

    def test_edgeql_syntax_insert03(self):
        """
        WITH MODULE test
        INSERT Foo;
        """

    def test_edgeql_syntax_insert04(self):
        """
        WITH MODULE test
        INSERT Foo{bar := 42};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'insert expression must be a concept or a view',
                  line=2, col=16)
    def test_edgeql_syntax_insert05(self):
        """
        INSERT 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert06(self):
        """
        INSERT Foo FILTER Foo.bar = 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert07(self):
        """
        INSERT Foo GROUP BY Foo.bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert08(self):
        """
        INSERT Foo ORDER BY Foo.bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert09(self):
        """
        INSERT Foo OFFSET 2;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert10(self):
        """
        INSERT Foo LIMIT 5;
        """

    def test_edgeql_syntax_insert12(self):
        """
        INSERT Foo{
            bar := 42,
            baz: Baz{
                spam := 'ham'
            }
        };
        """

    def test_edgeql_syntax_insert13(self):
        """
        INSERT Foo{
            bar := 42,
            baz := (SELECT Baz FILTER (Baz.spam = 'ham'))
        };
        """

    def test_edgeql_syntax_insert14(self):
        """
        INSERT Foo{
            bar := 42,
            baz: Baz{
                spam := 'ham',
                @weight := 2,
            }
        };
        """

    def test_edgeql_syntax_insert15(self):
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
    def test_edgeql_syntax_insert16(self):
        """
        INSERT Foo{
            bar := 42,
            baz: 'spam' {
                @weight := 2,
            }
        };
        """

    def test_edgeql_syntax_insert17(self):
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

    def test_edgeql_syntax_delete01(self):
        """
        DELETE Foo;
        """

    def test_edgeql_syntax_delete02(self):
        """
        WITH MODULE test
        DELETE Foo;
        """

    def test_edgeql_syntax_delete03(self):
        # NOTE: this must be rejected by the compiler
        """
        DELETE 42;
        """

    def test_edgeql_syntax_delete04(self):
        # this is legal and equivalent to DELETE Foo;
        """
        DELETE Foo{bar};
        """

    def test_edgeql_syntax_update01(self):
        """
        UPDATE Foo SET {bar := 42};
        UPDATE Foo FILTER (Foo.bar = 24) SET {bar := 42};
        """

    def test_edgeql_syntax_update02(self):
        """
        WITH MODULE test
        UPDATE Foo SET {bar := 42};
        WITH MODULE test
        UPDATE Foo FILTER (Foo.bar = 24) SET {bar := 42};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token: <Token SEMICOLON ";">',
                  line=2, col=18)
    def test_edgeql_syntax_update03(self):
        """
        UPDATE 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=19)
    def test_edgeql_syntax_update04(self):
        """
        UPDATE Foo;
        """

    def test_edgeql_syntax_update07(self):
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

    def test_edgeql_syntax_insertfor01(self):
        """
        FOR name in 'a' UNION 'b' UNION 'c'
        INSERT User{name := name};

% OK %

        FOR name in (('a' UNION 'b') UNION 'c')
        INSERT User{name := name};
        """

    def test_edgeql_syntax_insertfor02(self):
        """
        FOR name IN (SELECT Foo.bar FILTER (Foo.baz = TRUE))
        INSERT Foo{name := name};
        """

    def test_edgeql_syntax_insertfor03(self):
        """
        FOR bar IN (INSERT Bar{name := 'bar'})
        INSERT Foo{name := bar.name};
        """

    def test_edgeql_syntax_insertfor04(self):
        """
        FOR bar IN (DELETE Bar)
        INSERT Foo{name := bar.name};
        """

    def test_edgeql_syntax_insertfor05(self):
        """
        FOR bar IN (
            UPDATE Bar SET {name := (name + 'bar')}
        )
        INSERT Foo{name := bar.name};
        """

    def test_edgeql_syntax_selectfor01(self):
        """
        FOR x in (('Alice', 'White') UNION ('Bob', 'Green'))
        SELECT User{first_tname, last_name, age}
        FILTER (
            (.first_name = x.0)
            AND
            (.last_name = x.1)
        );
        """

    def test_edgeql_syntax_deletefor01(self):
        """
        FOR x in (('Alice', 'White') UNION ('Bob', 'Green'))
        DELETE (
            SELECT User
            FILTER (
                (.first_name = x.0)
                AND
                (.last_name = x.1)
            )
        );
        """

    def test_edgeql_syntax_updatefor01(self):
        """
        FOR x in ((1, 'a') UNION (2, 'b'))
        UPDATE Foo FILTER (Foo.id = x.0) SET {bar := x.1};
        """

    def test_edgeql_syntax_coalesce01(self):
        """
        SELECT a ?? x;
        SELECT a ?? x.a;
        SELECT a ?? x.a[IS ABC];
        SELECT (a ?? x.a[IS ABC]@aaa + 1);
        """

    def test_edgeql_syntax_function01(self):
        """
        SELECT foo();
        SELECT bar(User.name);
        SELECT baz(User.name, User.age);
        SELECT lower(User.name);
        """

    def test_edgeql_syntax_function02(self):
        """
        SELECT lower(string := User.name);
        SELECT baz(name := User.name, for := User.age);
        """

    def test_edgeql_syntax_function03(self):
        """
        SELECT some_agg(User.name ORDER BY User.age ASC);
        SELECT some_agg(User.name
                        FILTER (strlen(User.name) > 2)
                        ORDER BY User.age DESC);
        SELECT some_agg(User.name
                        FILTER (strlen(User.name) > 2)
                        ORDER BY User.age DESC THEN User.email ASC);
        """

    def test_edgeql_syntax_function04(self):
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

    def test_edgeql_syntax_tuple01(self):
        """
        SELECT ('foo', 42).0;
        SELECT ('foo', 42).1;
        """

    def test_edgeql_syntax_tuple02(self):
        """
        SELECT (name := 'foo', val := 42).name;
        SELECT (name := 'foo', val := 42).val;
        """

    # DDL
    #

    def test_edgeql_syntax_ddl_database01(self):
        """
        CREATE DATABASE mytestdb;
        DROP DATABASE mytestdb;
        CREATE DATABASE `mytest"db"`;
        DROP DATABASE `mytest"db"`;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=25)
    def test_edgeql_syntax_ddl_database02(self):
        """
        CREATE DATABASE (mytestdb);
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=28)
    def test_edgeql_syntax_ddl_database03(self):
        """
        CREATE DATABASE foo::mytestdb;
        """

    def test_edgeql_syntax_ddl_database04(self):
        """
        CREATE DATABASE all;
        CREATE DATABASE abstract;

% OK %

        CREATE DATABASE `all`;
        CREATE DATABASE abstract;
        """

    def test_edgeql_syntax_ddl_database05(self):
        """
        DROP DATABASE all;
        DROP DATABASE abstract;

% OK %

        DROP DATABASE `all`;
        DROP DATABASE abstract;
        """

    def test_edgeql_syntax_ddl_delta01(self):
        """
        ALTER MIGRATION test::d_links01_0 {
            RENAME TO test::pretty_name;
        };

% OK %

        ALTER MIGRATION test::d_links01_0
            RENAME TO test::pretty_name;
        """

    def test_edgeql_syntax_ddl_delta02(self):
        """
        CREATE MIGRATION test::d_links01_0 TO eschema $$concept Foo$$;
        ALTER MIGRATION test::d_links01_0
            RENAME TO test::pretty_name;
        COMMIT MIGRATION test::d_links01_0;
        DROP MIGRATION test::d_links01_0;
        """

    def test_edgeql_syntax_ddl_delta03(self):
        """
        CREATE MIGRATION test::d_links01_0 TO eschema $$concept Foo$$;
        CREATE MIGRATION test::d_links01_0 TO ESCHEMA $$concept Foo$$;
        CREATE MIGRATION test::d_links01_0 TO ESchema $$concept Foo$$;

% OK %

        CREATE MIGRATION test::d_links01_0 TO eschema $$concept Foo$$;
        CREATE MIGRATION test::d_links01_0 TO eschema $$concept Foo$$;
        CREATE MIGRATION test::d_links01_0 TO eschema $$concept Foo$$;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'unknown migration language: BadLang', line=2, col=47)
    def test_edgeql_syntax_ddl_delta04(self):
        """
        CREATE MIGRATION test::d_links01_0 TO BadLang $$concept Foo$$;
        """

    def test_edgeql_syntax_ddl_action01(self):
        """
        CREATE ACTION std::restrict {
            SET title := 'Abort the event if a pointer exists';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=13)
    def test_edgeql_syntax_ddl_action02(self):
        """
        CREATE ACTION std::restrict
            SET title := 'Abort the event if a pointer exists';
        """

    def test_edgeql_syntax_ddl_aggregate01(self):
        """
        CREATE AGGREGATE std::sum($v: std::int)
            RETURNING std::int
            INITIAL VALUE 0
            FROM SQL AGGREGATE 'test';
        """

    def test_edgeql_syntax_ddl_aggregate02(self):
        """
        CREATE AGGREGATE std::sum(std::int)
            RETURNING std::int
            INITIAL VALUE 0
            FROM SQL AGGREGATE 'sum';
        """

    def test_edgeql_syntax_ddl_aggregate03(self):
        """
        CREATE AGGREGATE std::sum($integer: std::int)
            RETURNING std::int
            INITIAL VALUE 0
            FROM SQL AGGREGATE 'sum';
        """

    def test_edgeql_syntax_ddl_aggregate04(self):
        """
        CREATE AGGREGATE std::sum($integer: std::int)
            RETURNING std::int
            INITIAL VALUE 0
            FROM SQL AGGREGATE 'sum';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*FROM", line=4, col=13)
    def test_edgeql_syntax_ddl_aggregate05(self):
        """
        CREATE AGGREGATE std::sum($integer: std::int)
            RETURNING std::int
            FROM SQL AGGREGATE 'sum';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=4)
    def test_edgeql_syntax_ddl_aggregate06(self):
        """
        CREATE AGGREGATE foo($string: std::str)
            RETURNING std::int INITIAL VALUE 0
            FROM AAA AGGREGATE 'foo';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*FUNCTION", line=4, col=22)
    def test_edgeql_syntax_ddl_aggregate07(self):
        """
        CREATE AGGREGATE foo($string: std::str)
            RETURNING std::int INITIAL VALUE 0
            FROM SQL FUNCTION 'foo';
        """

    def test_edgeql_syntax_ddl_aggregate08(self):
        """
        CREATE AGGREGATE std::count($expression: std::any)
            RETURNING std::int INITIAL VALUE 0
            FROM SQL AGGREGATE 'count';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*SELECT 1", line=5)
    def test_edgeql_syntax_ddl_aggregate09(self):
        # We don't yet support creating aggregates from any kind of code.
        """
        CREATE AGGREGATE foo()
            RETURNING std::int
            INITIAL VALUE 0
            FROM SQL 'SELECT 1';
        """

    def test_edgeql_syntax_ddl_atom01(self):
        """
        CREATE ABSTRACT ATOM std::any;
        CREATE ATOM std::typeref;
        CREATE ATOM std::atomref INHERITING std::typeref;
        """

    def test_edgeql_syntax_ddl_attribute01(self):
        """
        CREATE ATTRIBUTE std::paramtypes map<std::str, std::typeref>;
        """

    def test_edgeql_syntax_ddl_attribute02(self):
        """
        CREATE ATTRIBUTE stdattrs::precision array<std::int>;
        """

    def test_edgeql_syntax_ddl_attribute03(self):
        # test parsing of array types
        #
        """
        CREATE ATTRIBUTE std::foo array<int>;
        CREATE ATTRIBUTE std::foo array<int[]>;
        CREATE ATTRIBUTE std::foo array<int[][]>;
        CREATE ATTRIBUTE std::foo array<int[][][]>;

        CREATE ATTRIBUTE std::foo array<int[3]>;
        CREATE ATTRIBUTE std::foo array<int[4][5]>;
        CREATE ATTRIBUTE std::foo array<int[3][4][5]>;

        CREATE ATTRIBUTE std::foo array<int[][5]>;
        CREATE ATTRIBUTE std::foo array<int[4][]>;
        CREATE ATTRIBUTE std::foo array<int[][4][5]>;
        CREATE ATTRIBUTE std::foo array<int[3][4][]>;
        CREATE ATTRIBUTE std::foo array<int[][4][]>;
        """

    def test_edgeql_syntax_ddl_attribute04(self):
        # test parsing of map types
        #
        """
        CREATE ATTRIBUTE std::foo map<int, str>;
        CREATE ATTRIBUTE std::foo map<array<int>, str>;
        CREATE ATTRIBUTE std::foo map<array<int>, tuple<str, str>>;
        CREATE ATTRIBUTE std::foo map<int, foo::Bar>;
        """

    def test_edgeql_syntax_ddl_attribute05(self):
        # test parsing of tuple types
        #
        """
        CREATE ATTRIBUTE std::foo tuple;
        CREATE ATTRIBUTE std::foo tuple<float>;
        CREATE ATTRIBUTE std::foo tuple<int, str>;
        CREATE ATTRIBUTE std::foo tuple<array<int>, str>;
        CREATE ATTRIBUTE std::foo tuple<array<int>, tuple<str, str>>;
        CREATE ATTRIBUTE std::foo tuple<int, foo::Bar>;

        CREATE ATTRIBUTE std::foo tuple<count: int, name: str>;

        CREATE ATTRIBUTE std::foo tuple<Baz, map<int, str>>;
        CREATE ATTRIBUTE std::foo tuple<Baz, map<array<int>, str>>;
        CREATE ATTRIBUTE std::foo tuple<Baz, map<array<int>, tuple<str, str>>>;
        CREATE ATTRIBUTE std::foo tuple<Baz, map<int, foo::Bar>>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*map", line=2, col=35)
    def test_edgeql_syntax_ddl_attribute06(self):
        """
        CREATE ATTRIBUTE std::foo map;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*>", line=2, col=42)
    def test_edgeql_syntax_ddl_attribute07(self):
        """
        CREATE ATTRIBUTE std::foo map<int>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*COMMA", line=2, col=47)
    def test_edgeql_syntax_ddl_attribute08(self):
        """
        CREATE ATTRIBUTE std::foo map<int, str, float>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*array", line=2, col=35)
    def test_edgeql_syntax_ddl_attribute09(self):
        """
        CREATE ATTRIBUTE std::foo array;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*COMMA", line=2, col=44)
    def test_edgeql_syntax_ddl_attribute10(self):
        """
        CREATE ATTRIBUTE std::foo array<int, int, int>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*<", line=2, col=46)
    def test_edgeql_syntax_ddl_attribute11(self):
        """
        CREATE ATTRIBUTE std::foo array<array<int[]>>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*COLON", line=2, col=49)
    def test_edgeql_syntax_ddl_attribute12(self):
        """
        CREATE ATTRIBUTE std::foo tuple<int, foo:int>;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected token.*>", line=2, col=53)
    def test_edgeql_syntax_ddl_attribute13(self):
        """
        CREATE ATTRIBUTE std::foo tuple<foo:int, str>;
        """

    def test_edgeql_syntax_ddl_constraint01(self):
        """
        CREATE CONSTRAINT std::enum(array<std::any>)
            INHERITING std::constraint
        {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, subject);
        };
        """

    def test_edgeql_syntax_ddl_constraint02(self):
        """
        CREATE CONSTRAINT std::enum(array<std::any>) {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, subject);
        };
        """

    def test_edgeql_syntax_ddl_constraint03(self):
        """
        CREATE CONSTRAINT std::enum {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, subject);
        };
        """

    def test_edgeql_syntax_ddl_constraint04(self):
        """
        CREATE CONSTRAINT std::enum() {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, subject);
        };

% OK %

        CREATE CONSTRAINT std::enum {
            SET errmessage := '{subject} must be one of: {param}.';
            SET expr := array_contains($param, subject);
        };
        """

    def test_edgeql_syntax_ddl_constraint05(self):
        """
        CREATE ATOM std::decimal_rounding_t INHERITING std::str {
            CREATE CONSTRAINT std::enum(['a', 'b']);
        };
        """

    def test_edgeql_syntax_ddl_function01(self):
        """
        CREATE FUNCTION std::strlen($string: std::str) RETURNING std::int
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function02(self):
        """
        CREATE FUNCTION std::strlen(std::str) RETURNING std::int
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function03(self):
        """
        CREATE FUNCTION std::strlen($string: std::str) RETURNING std::int
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function04(self):
        """
        CREATE FUNCTION std::strlen($string: std::str, $integer: std::int)
            RETURNING std::int
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function05(self):
        """
        CREATE FUNCTION std::strlen($string: std::str, std::int)
            RETURNING std::int
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function06(self):
        """
        CREATE FUNCTION std::strlen($string: std::str = '1')
            RETURNING std::int
            FROM SQL FUNCTION 'strlen';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'non-default argument follows', line=2, col=62)
    def test_edgeql_syntax_ddl_function07(self):
        """
        CREATE FUNCTION std::strlen($string: std::str = '1', $abc: std::str)
            RETURNING std::int;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'non-variadic argument follows', line=2, col=63)
    def test_edgeql_syntax_ddl_function08(self):
        """
        CREATE FUNCTION std::strlen(*$string: std::str = '1', $abc: std::str)
            RETURNING std::int;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'more than one variadic argument', line=2, col=63)
    def test_edgeql_syntax_ddl_function09(self):
        """
        CREATE FUNCTION std::strlen(*$string: std::str = '1', *$abc: std::str)
            RETURNING std::int;
        """

    def test_edgeql_syntax_ddl_function10(self):
        """
        CREATE FUNCTION std::strlen(std::str = '1', *std::str)
            RETURNING std::int
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_function11(self):
        """
        CREATE FUNCTION no_params() RETURNING std::int
        FROM EdgeQL $$ SELECT 1 $$;
        """

    def test_edgeql_syntax_ddl_function13(self):
        """
        CREATE FUNCTION foo($string: std::str) RETURNING {bar: std::int}
        FROM EDGEQL $$ SELECT { bar := 123 } $$;
        """

    def test_edgeql_syntax_ddl_function14(self):
        """
        CREATE FUNCTION foo($string: std::str)
        RETURNING {
            bar: std::int,
            baz: std::str
        } FROM EdgeQL $$ SELECT smth() $$;
        """
    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=3)
    def test_edgeql_syntax_ddl_function16(self):
        """
        CREATE FUNCTION foo($string: std::str)
        RETURNING std::int FROM AAA FUNCTION 'foo';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected token.*AGGREGATE", line=3)
    def test_edgeql_syntax_ddl_function18(self):
        """
        CREATE FUNCTION foo($string: std::str)
        RETURNING std::int FROM SQL AGGREGATE 'foo';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "AAA is not a valid language", line=3)
    def test_edgeql_syntax_ddl_function19(self):
        """
        CREATE FUNCTION foo($string: std::str)
        RETURNING std::int FROM AAA 'code';
        """

    def test_edgeql_syntax_ddl_function20(self):
        """
        CREATE FUNCTION foo() RETURNING std::int FROM SQL 'SELECT 1';

% OK %

        CREATE FUNCTION foo() RETURNING std::int FROM SQL $$SELECT 1$$;
        """

    def test_edgeql_syntax_ddl_function21(self):
        """
        CREATE FUNCTION foo() RETURNING std::int FROM SQL FUNCTION 'aaa';
        """

    def test_edgeql_syntax_ddl_function24(self):
        """
        CREATE FUNCTION foo() RETURNING std::str FROM SQL $a$SELECT $$foo$$$a$;
        """

    def test_edgeql_syntax_ddl_function25(self):
        """
        CREATE FUNCTION foo() RETURNING std::str {
            SET description := 'aaaa';
            FROM SQL $a$SELECT $$foo$$$a$;
        };
        """

    def test_edgeql_syntax_ddl_function26(self):
        """
        CREATE FUNCTION foo() RETURNING std::str {
            SET volatility := 'volatile';
            SET description := 'aaaa';
            FROM SQL $a$SELECT $$foo$$$a$;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "FROM clause is missing", line=2)
    def test_edgeql_syntax_ddl_function27(self):
        """
        CREATE FUNCTION foo() RETURNING std::str {
            SET description := 'aaaa';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "more than one FROM clause", line=5)
    def test_edgeql_syntax_ddl_function28(self):
        """
        CREATE FUNCTION foo() RETURNING std::str {
            FROM SQL 'SELECT 1';
            SET description := 'aaaa';
            FROM SQL 'SELECT 2';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  'Unexpected token.*INITIAL', line=4, col=13)
    def test_edgeql_syntax_ddl_function29(self):
        """
        CREATE FUNCTION std::strlen(std::str = '1', *std::str)
            RETURNING std::int
            INITIAL VALUE 0
            FROM SQL FUNCTION 'strlen';
        """

    def test_edgeql_syntax_ddl_linkproperty01(self):
        """
        CREATE LINK PROPERTY std::linkproperty {
            SET title := 'Base link property';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=21)
    def test_edgeql_syntax_ddl_linkproperty02(self):
        """
        CREATE LINK LINK PROPERTY std::linkproperty {
            SET title := 'Base link property';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=39)
    def test_edgeql_syntax_ddl_linkproperty03(self):
        """
        CREATE LINK PROPERTY PROPERTY std::linkproperty {
            SET title := 'Base link property';
        };
        """

    def test_edgeql_syntax_ddl_module01(self):
        """
        CREATE MODULE foo;
        CREATE MODULE foo.bar;
        CREATE MODULE all.abstract.bar;

% OK %

        CREATE MODULE foo;
        CREATE MODULE `foo.bar`;
        CREATE MODULE `all.abstract.bar`;
        """
