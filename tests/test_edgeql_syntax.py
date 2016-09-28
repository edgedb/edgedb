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
    re_filter = re.compile(r'[\s]+|(#.*?(\n|$))|(,(?=\s*[})]))')
    parser_debug_flag = 'DEBUG_EDGEQL'
    markup_dump_lexer = 'edgeql'
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
        SELECT 1;
        SELECT +7;
        SELECT -7;
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
        SELECT NULL;
        """

    def test_edgeql_syntax_contants06(self):
        """
        SELECT $1;
        SELECT $123;
        SELECT $somevar;
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

    def test_edgeql_syntax_ops07(self):
        """
        SELECT 40 == 2;

% OK %

        SELECT (40 = 2);
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
        SELECT (User.name IN ('Alice', 'Bob'));
        SELECT (User.name NOT IN ('Alice', 'Bob'));
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
        SELECT (User.name ~ '^[[:lower:]]+$');
        SELECT (User.name ~* 'don');
        """

    def test_edgeql_syntax_ops14(self):
        """
        SELECT -1 + 2 * 3 - 5 - 6 / 2 > 0 OR 25 % 4 = 3 AND 42 IN (12, 42, 14);

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
                (42 IN (12, 42, 14))
            )
        );
        """

    def test_edgeql_syntax_ops15(self):
        """
        SELECT
            ((-1 + 2) * 3 - (5 - 6) / 2 > 0 OR 25 % 4 = 3)
            AND 42 IN (12, 42, 14);

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
            (42 IN (12, 42, 14))
        );
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

    # NOTE: this is syntactically allowed, but the compiler should
    # throw an error
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

    # NOTE: this is syntactically allowed, but the compiler should
    # throw an error
    def test_edgeql_syntax_shape15(self):
        """
        SELECT {
            foo: {
                bar:= 42
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=22)
    def test_edgeql_syntax_shape16(self):
        """
        SELECT {
            foo: {
                bar: 42
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=17)
    def test_edgeql_syntax_shape17(self):
        """
        SELECT {
            foo: {
                'bar': 42
            }
        };
        """

    def test_edgeql_syntax_shape18(self):
        """
        SELECT {
            foo := {
                'bar': 42
            }
        };
        """

    def test_edgeql_syntax_shape19(self):
        """
            SELECT
                test::Issue {
                    number
                }
            WHERE
                (((test::Issue)).number) = '1';

            SELECT
                (test::Issue) {
                    number
                }
            WHERE
                (((test::Issue)).(number)) = '1';

            SELECT
                test::Issue {
                    test::number
                }
            WHERE
                (((test::Issue)).(test::number)) = '1';

% OK %

            SELECT
                (test::Issue) {
                    number
                }
            WHERE
                ((test::Issue).number = '1');

            SELECT
                (test::Issue) {
                    number
                }
            WHERE
                ((test::Issue).number = '1');

            SELECT
                (test::Issue) {
                    (test::number)
                }
            WHERE
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

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=22)
    def test_edgeql_syntax_shape22(self):
        """
        SELECT Foo{
            __type__ := 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=22)
    def test_edgeql_syntax_shape23(self):
        """
        SELECT 'Foo' {
            bar := 42
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=11)
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

    def test_edgeql_syntax_shape26(self):
        """
        SELECT Issue{
            name,
            related_to *,
        };
        """

    def test_edgeql_syntax_shape27(self):
        """
        SELECT Issue{
            name,
            related_to *5,
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=25)
    def test_edgeql_syntax_shape28(self):
        """
        SELECT Issue{
            name,
            related_to *-1,
        };
        """

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

    def test_edgeql_syntax_path14(self):
        """
        SELECT User.__type__.name LIMIT 1;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_path15(self):
        """
        SELECT (42).foo;
        """

    def test_edgeql_syntax_type_interpretation01(self):
        """
        SELECT (Foo AS Bar);
        SELECT (Foo.bar AS Bar);
        SELECT (Foo.bar AS Bar).spam;
        SELECT (Foo.bar AS Bar).<ham;
        """

    def test_edgeql_syntax_map01(self):
        """
        SELECT {
            'name': 'foo',
            'description': 'bar'
        };
        SELECT {
            'name': 'baz',
        };
        SELECT {
            'first': {
                'name': 'foo'
            },
            'second': {
                'description': 'bar'
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=17)
    def test_edgeql_syntax_map02(self):
        """
        SELECT {
            'foo': {
                bar: 42
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=18)
    def test_edgeql_syntax_map03(self):
        """
        SELECT {
            'foo':= {
                bar:= 42
            }
        };
        """

    def test_edgeql_syntax_map04(self):
        """
        SELECT {'foo': 42, 'bar': 'something'};
        SELECT {'foo': 42, 'bar': 'something'}['foo'];
        SELECT ({'foo': 42, 'bar': 'something'})['foo'];

% OK %

        SELECT {'foo': 42, 'bar': 'something'};
        SELECT ({'foo': 42, 'bar': 'something'})['foo'];
        SELECT ({'foo': 42, 'bar': 'something'})['foo'];
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

    def test_edgeql_syntax_cast03(self):
        """
        SELECT
            <User {name, description}> {
                'name': 'Alice',
                'description': 'sample'
            };
        """

    def test_edgeql_syntax_cardinality01(self):
        """
        SELECT SINGLE User.name WHERE (User.name = 'special');
        INSERT User RETURNING SINGLE User{name};
        INSERT User{name:= 'foo'} RETURNING SINGLE User{name};
        UPDATE User{age:= (User.age + 10)}
            WHERE (User.name = 'foo') RETURNING SINGLE User{name};
        DELETE User WHERE (User.name = 'foo') RETURNING SINGLE User{name};
        CREATE FUNCTION spam(foo str) RETURNING SINGLE str;
        """

    def test_edgeql_syntax_with01(self):
        """
        WITH
            MODULE test,
            extra:= MODULE `lib.extra`,
            foo:= Bar.foo,
            baz:= (SELECT (extra::Foo).baz)
        SELECT Bar {
            spam,
            ham:= baz
        } WHERE (foo = 'special');
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=6, col=9)
    def test_edgeql_syntax_with02(self):
        """
        WITH
            MODULE test,
            foo:= Bar.foo,
            baz:= (SELECT Foo.baz)
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

    def test_edgeql_syntax_select01(self):
        """
        SELECT 42;
        SELECT User{name};
        SELECT User{name}
            WHERE (User.age > 42);
        SELECT User.name
            GROUP BY User.name;
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
        WHERE
            (User.age > 42)
        GROUP BY
            User.name
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
            WHERE (User.age > 42);
        WITH MODULE test
        SELECT User.name
            GROUP BY User.name;
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
        WHERE
            (User.age > 42)
        GROUP BY
            User.name
        ORDER BY
            User.name ASC
        OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select07(self):
        """
        (SELECT User.name) OFFSET 2;
        (SELECT User.name) LIMIT 2;
        (SELECT User.name) OFFSET 2 LIMIT 5;

% OK %

        SELECT User.name OFFSET 2;
        SELECT User.name LIMIT 2;
        SELECT User.name OFFSET 2 LIMIT 5;
        """

    def test_edgeql_syntax_select08(self):
        """
        WITH MODULE test
        SELECT User{name} ORDER BY User.name ASC;
        WITH MODULE test
        (SELECT User{name}) ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name ASC;
        (SELECT User{name}) ORDER BY User.name ASC;

% OK %

        WITH MODULE test
        SELECT User{name} ORDER BY User.name ASC;
        WITH MODULE test
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name ASC;
        SELECT User{name} ORDER BY User.name ASC;
        """

    def test_edgeql_syntax_select09(self):
        """
        SELECT Foo UNION SELECT Bar;
        SELECT Foo INTERSECT SELECT Bar;
        SELECT Foo EXCEPT SELECT Bar;

% OK %

        (SELECT Foo) UNION (SELECT Bar);
        (SELECT Foo) INTERSECT (SELECT Bar);
        (SELECT Foo) EXCEPT (SELECT Bar);
        """

    def test_edgeql_syntax_select10(self):
        """
        SELECT Issue {name} ORDER BY Issue.priority.name ASC NULLS FIRST;
        SELECT Issue {name} ORDER BY Issue.priority.name DESC NULLS LAST;
        """

    def test_edgeql_syntax_select11(self):
        """
        SELECT User.name OFFSET $1;
        SELECT User.name LIMIT $2;
        SELECT User.name OFFSET $1 LIMIT $2;
        """

    def test_edgeql_syntax_select12(self):
        """
        SELECT DISTINCT User.age;
        """

    def test_edgeql_syntax_insert01(self):
        """
        INSERT Foo;
        INSERT Foo RETURNING Foo;
        INSERT Foo RETURNING Foo{bar};
        INSERT Foo RETURNING SINGLE Foo;
        INSERT Foo RETURNING SINGLE Foo{bar};
        """

    def test_edgeql_syntax_insert02(self):
        """
        INSERT Foo{bar:= 42};
        INSERT Foo{bar:= 42} RETURNING Foo;
        INSERT Foo{bar:= 42} RETURNING Foo{bar};
        INSERT Foo{bar:= 42} RETURNING SINGLE Foo;
        INSERT Foo{bar:= 42} RETURNING SINGLE Foo{bar};
        """

    def test_edgeql_syntax_insert03(self):
        """
        WITH MODULE test
        INSERT Foo;
        WITH MODULE test
        INSERT Foo RETURNING Foo;
        WITH MODULE test
        INSERT Foo RETURNING Foo{bar};
        WITH MODULE test
        INSERT Foo RETURNING SINGLE Foo;
        WITH MODULE test
        INSERT Foo RETURNING SINGLE Foo{bar};
        """

    def test_edgeql_syntax_insert04(self):
        """
        WITH MODULE test
        INSERT Foo{bar:= 42};
        WITH MODULE test
        INSERT Foo{bar:= 42} RETURNING Foo;
        WITH MODULE test
        INSERT Foo{bar:= 42} RETURNING Foo{bar};
        WITH MODULE test
        INSERT Foo{bar:= 42} RETURNING SINGLE Foo;
        WITH MODULE test
        INSERT Foo{bar:= 42} RETURNING SINGLE Foo{bar};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=18)
    def test_edgeql_syntax_insert05(self):
        """
        INSERT 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=20)
    def test_edgeql_syntax_insert06(self):
        """
        INSERT Foo WHERE Foo.bar = 42;
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

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=33)
    def test_edgeql_syntax_insert11(self):
        """
        INSERT Foo RETURNING Foo, Foo.bar;
        """

    def test_edgeql_syntax_insert12(self):
        """
        INSERT Foo{
            bar:= 42,
            baz: Baz{
                spam:= 'ham'
            }
        };
        """

    def test_edgeql_syntax_insert13(self):
        """
        INSERT Foo{
            bar:= 42,
            baz:= (SELECT Baz WHERE (Baz.spam = 'ham'))
        };
        """

    def test_edgeql_syntax_insert14(self):
        """
        INSERT Foo{
            bar:= 42,
            baz: Baz{
                spam:= 'ham',
                @weight:= 2,
            }
        };
        """

    def test_edgeql_syntax_insert15(self):
        """
        INSERT Foo{
            bar:= 42,
            baz:= 'spam' {
                @weight:= 2,
            }
        };

        INSERT Foo{
            bar:= 42,
            baz:= 24 {
                @weight:= 2,
            }
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=18)
    def test_edgeql_syntax_insert16(self):
        """
        INSERT Foo{
            bar:= 42,
            baz: 'spam' {
                @weight:= 2,
            }
        };
        """

    def test_edgeql_syntax_insert17(self):
        """
        INSERT Foo{
            bar:= 42,
            baz:= (
                SELECT Baz{
                    @weight:= 2
                } WHERE (Baz.spam = 'ham')
            )
        };
        """

    def test_edgeql_syntax_insert18(self):
        """
        SELECT (INSERT Foo{bar:= 42} RETURNING Foo);
        """

    def test_edgeql_syntax_delete01(self):
        """
        DELETE Foo;
        DELETE Foo RETURNING Foo;
        DELETE Foo RETURNING Foo{bar};
        DELETE Foo RETURNING SINGLE Foo;
        DELETE Foo RETURNING SINGLE Foo{bar};
        DELETE Foo WHERE (Foo.bar = 42);
        DELETE Foo WHERE (Foo.bar = 42) RETURNING Foo;
        """

    def test_edgeql_syntax_delete02(self):
        """
        WITH MODULE test
        DELETE Foo;
        WITH MODULE test
        DELETE Foo RETURNING Foo;
        WITH MODULE test
        DELETE Foo RETURNING Foo{bar};
        WITH MODULE test
        DELETE Foo RETURNING SINGLE Foo;
        WITH MODULE test
        DELETE Foo RETURNING SINGLE Foo{bar};
        WITH MODULE test
        DELETE Foo WHERE (Foo.bar = 42);
        WITH MODULE test
        DELETE Foo WHERE (Foo.bar = 42) RETURNING Foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=18)
    def test_edgeql_syntax_delete03(self):
        """
        DELETE 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=19)
    def test_edgeql_syntax_delete04(self):
        """
        DELETE Foo{bar};
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=33)
    def test_edgeql_syntax_delete05(self):
        """
        DELETE Foo RETURNING Foo, Foo.bar;
        """

    def test_edgeql_syntax_delete06(self):
        """
        SELECT (DELETE Foo RETURNING Foo);
        """

    def test_edgeql_syntax_update01(self):
        """
        UPDATE Foo{bar:= 42};
        UPDATE Foo{bar:= 42} RETURNING Foo;
        UPDATE Foo{bar:= 42} RETURNING Foo{bar};
        UPDATE Foo{bar:= 42} RETURNING SINGLE Foo;
        UPDATE Foo{bar:= 42} RETURNING SINGLE Foo{bar};
        UPDATE Foo{bar:= 42} WHERE (Foo.bar = 24);
        UPDATE Foo{bar:= 42} WHERE (Foo.bar = 24) RETURNING Foo;
        """

    def test_edgeql_syntax_update02(self):
        """
        WITH MODULE test
        UPDATE Foo{bar:= 42};
        WITH MODULE test
        UPDATE Foo{bar:= 42} RETURNING Foo;
        WITH MODULE test
        UPDATE Foo{bar:= 42} RETURNING Foo{bar};
        WITH MODULE test
        UPDATE Foo{bar:= 42} RETURNING SINGLE Foo;
        WITH MODULE test
        UPDATE Foo{bar:= 42} RETURNING SINGLE Foo{bar};
        WITH MODULE test
        UPDATE Foo{bar:= 42} WHERE (Foo.bar = 24);
        WITH MODULE test
        UPDATE Foo{bar:= 42} WHERE (Foo.bar = 24) RETURNING Foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=16)
    def test_edgeql_syntax_update03(self):
        """
        UPDATE 42;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=19)
    def test_edgeql_syntax_update04(self):
        """
        UPDATE Foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=43)
    def test_edgeql_syntax_update05(self):
        """
        UPDATE Foo{bar:= 42} RETURNING Foo, Foo.bar;
        """

    def test_edgeql_syntax_update06(self):
        """
        SELECT (UPDATE Foo{bar:= 42} RETURNING Foo);
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
        SELECT lower(string:=User.name);
        SELECT baz(name:=User.name, for:=User.age);
        """

    def test_edgeql_syntax_function03(self):
        """
        SELECT some_agg(User.name ORDER BY User.age ASC);
        SELECT some_agg(User.name
                        WHERE (strlen(User.name) > 2)
                        ORDER BY User.age DESC);
        SELECT some_agg(User.name
                        WHERE (strlen(User.name) > 2)
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

    def test_edgeql_syntax_ddl_delta01(self):
        """
        ALTER DELTA test::d_links01_0 {
            RENAME TO test::pretty_name;
        };

% OK %

        ALTER DELTA test::d_links01_0
            RENAME TO test::pretty_name;
        """

    def test_edgeql_syntax_ddl_delta02(self):
        """
        CREATE DELTA test::d_links01_0 TO $$concept Foo$$;
        ALTER DELTA test::d_links01_0
            RENAME TO test::pretty_name;
        COMMIT DELTA test::d_links01_0;
        DROP DELTA test::d_links01_0;
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
        CREATE AGGREGATE std::sum(v std::int) RETURNING std::int;
        """

    def test_edgeql_syntax_ddl_atom01(self):
        """
        CREATE ABSTRACT ATOM std::`any`;
        CREATE ATOM std::typeref;
        CREATE ATOM std::atomref INHERITING std::typeref;
        """

    def test_edgeql_syntax_ddl_attribute01(self):
        """
        CREATE ATTRIBUTE std::paramtypes map<std::str, std::typeref>;
        """

    def test_edgeql_syntax_ddl_function01(self):
        """
        CREATE FUNCTION std::strlen(string std::str) RETURNING std::int;
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

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=30)
    def test_edgeql_syntax_ddl_linkproperty03(self):
        """
        CREATE LINK PROPERTY PROPERTY std::linkproperty {
            SET title := 'Base link property';
        };
        """
