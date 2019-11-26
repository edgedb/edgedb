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


import functools
import re

from edb import errors

from edb.testbase import lang as tb
from edb.edgeql import generate_source
from edb.edgeql.parser import parser as eql_parser


class SchemaSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s\'"()]+|(#.*?(\n|$))|SELECT')
    parser_debug_flag = 'DEBUG_ESCHEMA'
    markup_dump_lexer = 'edgeschema'
    ast_to_source = functools.partial(generate_source, unsorted=True)

    def get_parser(self, *, spec):
        return eql_parser.EdgeSDLParser()


class TestEdgeSchemaParser(SchemaSyntaxTest):
    def test_eschema_syntax_empty_01(self):
        """"""

    def test_eschema_syntax_empty_02(self):
        """# comment"""

    def test_eschema_syntax_empty_03(self):
        """


        """

    def test_eschema_syntax_empty_04(self):
        """

        # comment

        """

    def test_eschema_syntax_tabs_01(self):
        """
\tabstract type Foo {
\t\trequired property foo -> str;
\t};

\tabstract type Bar {
\t\trequired property bar -> str;
\t};
        """

    def test_eschema_syntax_tabs_02(self):
        """
\t  abstract type Foo {
\t      required property foo -> str;
};

\t  abstract type Bar {
\t      required property bar -> str;
};
        """
    def test_eschema_syntax_semicolon_01(self):
        """
        abstract type OwnedObject {
            required link owner -> User
        };

% OK %

        abstract type OwnedObject {
            required link owner -> User;
        };
        """

    def test_eschema_syntax_semicolon_02(self):
        """
        abstract type OwnedObject {
            required property tag -> str
        };

% OK %

        abstract type OwnedObject {
            required property tag -> str;
        };
        """

    def test_eschema_syntax_semicolon_03(self):
        """
        abstract type OwnedObject {
            required link owner -> User;
            required property tag -> str
        };

% OK %

        abstract type OwnedObject {
            required link owner -> User;
            required property tag -> str;
        };
        """

    def test_eschema_syntax_type_01(self):
        """type User extending builtins::NamedObject;"""

    def test_eschema_syntax_type_02(self):
        """
        abstract type OwnedObject {
            required link owner -> User;
        };
        """

    def test_eschema_syntax_type_03(self):
        """
        abstract type Text {
            required property body -> str {
                constraint max_len_value (10000);
            };
        };
        """

    def test_eschema_syntax_type_04(self):
        """
        type LogEntry extending OwnedObject, Text {
            required property spent_time -> int64;
        };
        """

    def test_eschema_syntax_type_05(self):
        """
        type LogEntry extending OwnedObject, Text {
           link start_date := (SELECT datetime::datetime_current());
        };
        """

    def test_eschema_syntax_type_06(self):
        """
        type LogEntry extending OwnedObject, Text {
            property start_date -> datetime {
               default :=
                    (SELECT datetime::datetime_current());
               title := 'Start Date';
           };
        };
        """

    def test_eschema_syntax_type_07(self):
        """
        type Issue extending `foo.bar`::NamedObject, OwnedObject, Text {
            required link number -> issue_num_t {
                readonly := true;
            };

            required link status -> Status;

            link priority -> Priority;

            multi link watchers extending orderable -> User {
                property foo extending bar -> str;
            };

            multi link time_spent_log -> LogEntry;

            link start_date := (SELECT datetime::datetime_current());

            multi link related_to -> Issue;

            property time_estimate -> int64;

            property start_date -> datetime {
               default :=
                    (SELECT datetime::datetime_current());
               title := 'Start Date';
            };

            property due_date -> datetime;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected 'unit'", line=4, col=26)
    def test_eschema_syntax_type_08(self):
        """
        type Foo {
            property time_estimate -> int64 {
                property unit {
                    default := 'minute';
                };
            };
        };
       """

    def test_eschema_syntax_type_09(self):
        """
        type LogEntry extending OwnedObject, Text {
            required link attachment -> Post | File | User;
        };
        """

    def test_eschema_syntax_type_10(self):
        """
        type `Log-Entry` extending `OwnedObject`, `Text` {
            required link attachment -> `Post` | `File` | `User`;
        };

% OK %

        type `Log-Entry` extending OwnedObject, Text {
            required link attachment -> Post | File | User;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected 'Commit'",
                  line=2, col=14)
    def test_eschema_syntax_type_11(self):
        """
        type Commit {
            required property name -> std::str;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=14)
    def test_eschema_syntax_type_12(self):
        """
        type __Foo__ {
            required property name -> std::str;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=2, col=14)
    def test_eschema_syntax_type_13(self):
        """
        type `__Foo__` {
            required property name -> std::str;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=31)
    def test_eschema_syntax_type_14(self):
        """
        type __Foo {
            required property __name__ -> std::str;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=31)
    def test_eschema_syntax_type_15(self):
        """
        type `__Foo` {
            required property `__name__` -> std::str;
        };
        """

    def test_eschema_syntax_type_16(self):
        """
        type Пример {
            required property номер -> int16;
        };
        """

    def test_eschema_syntax_type_17(self):
        """
        type Foo {
            link bar0 -> Bar {
                on target delete restrict;
            };
            link bar1 -> Bar {
                on target delete delete source;
            };
            link bar2 -> Bar {
                on target delete allow;
            };
            link bar3 -> Bar {
                on target delete deferred restrict;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "more than one 'on target delete' specification",
                  line=5, col=17)
    def test_eschema_syntax_type_18(self):
        """
        type Foo {
            link bar0 -> Bar {
                on target delete restrict;
                on target delete delete source;
            };
        };
        """

    def test_eschema_syntax_type_19(self):
        """
        type Foo {
            property foo -> str {
                default := some_func(
    1, 2, 3);
            };
        };
        """

    def test_eschema_syntax_type_22(self):
        """
        type Foo {
            property foo -> str {
                # if it's defined on the same line as :=
                # the definition must be a one-liner
                default := some_func(1, 2, 3);
            };
            property bar -> str {
                # multi-line definition with correct indentation
                default :=
                    some_func('
                    1, 2, 3');
            };
            property baz -> str {
                # multi-line definition with correct indentation
                default :=
                    $$some_func(
                    1, 2, 3)$$;
            };
        };
        """

    def test_eschema_syntax_type_23(self):
        """
        type Foo {
            single link foo -> Foo;
            multi link bar -> Bar;
            required single link baz -> Baz;
            required multi link spam -> Spam;
            overloaded required single link ham -> Ham;
            overloaded required multi link eggs -> Egg;
            overloaded link knight;
            overloaded link clinic {
                property argument -> int64;
            };
            overloaded property castle;
            overloaded property tower {
                constraint exclusive;
            };
        };
        """

    def test_eschema_syntax_type_24(self):
        """
        type Foo {
            single property foo -> str;
            multi property bar -> str;
            required single property baz -> str;
            required multi property spam -> str;
            overloaded required single property ham -> str;
            overloaded required multi property eggs -> str;
        };
        """

    def test_eschema_syntax_type_25(self):
        """
        type Foo {
            single property foo -> str
        }

        type Bar {
            multi property bar -> str
        }

        type Baz {
            required single property baz -> str
        }

        type Spam {
            required multi property spam -> str
        }

        type Ham {
            overloaded required single property ham -> str
        }

        type Eggs {
            overloaded required multi property eggs -> str
        }

% OK %

        type Foo {
            single property foo -> str;
        };

        type Bar {
            multi property bar -> str;
        };

        type Baz {
            required single property baz -> str;
        };

        type Spam {
            required multi property spam -> str;
        };

        type Ham {
            overloaded required single property ham -> str;
        };

        type Eggs {
            overloaded required multi property eggs -> str;
        };
        """

    def test_eschema_syntax_type_26(self):
        """
        type Foo {
            single property foo -> str {
                constraint max_len_value (10000)
            }

            multi property bar -> str {
                constraint max_len_value (10000)
            }
        }

% OK %

        type Foo {
            single property foo -> str {
                constraint max_len_value (10000);
            };

            multi property bar -> str {
                constraint max_len_value (10000);
            };
        };
        """

    def test_eschema_syntax_link_target_type_01(self):
        """
        type User {
            required link todo -> array<str>;
        };
        """

    def test_eschema_syntax_link_target_type_03(self):
        """
        type User {
            required link todo -> tuple<str, int64, float64>;
        };
        """

    def test_eschema_syntax_link_target_type_04(self):
        """
        type User {
            required link todo ->
                tuple<str, tuple<str, array<str>>, array<float64>>;
        };
        """

    def test_eschema_syntax_index_01(self):
        """
        type LogEntry extending OwnedObject, Text {
            required link owner -> User;
            index on (SELECT datetime::datetime_current());
        };
        """

    def test_eschema_syntax_index_02(self):
        """
        abstract link foobar {
            property foo -> str {
                title := 'Sample property';
            };
            index on (__subject__@foo);
        };
        """

    # FIXME: obscure error message
    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'prop'", line=3, col=19)
    def test_eschema_syntax_index_03(self):
        """
        scalar type foobar {
            index prop on (__source__);
        };
        """

    def test_eschema_syntax_index_04(self):
        """
        type User {
            property name -> str;
            index on (.name);
        };
        """

    def test_eschema_syntax_index_05(self):
        """
        type User {
            property name -> str;

            index on (.name) {
                annotation title := 'User name index';
            };
        };
        """

    def test_eschema_syntax_ws_01(self):
        """
type LogEntry extending    OwnedObject,    Text {

    # irrelevant comment indent
            # irrelevant comment indent
        # irrelevant comment indent

  property start_date -> datetime {


                       default := (


                        SELECT

                            datetime::datetime_current())

                ;

                       title := 'Start Date';

        };};
        """

    def test_eschema_syntax_ws_02(self):
        """
        scalar type newScalarType extending str#:

% OK %

        scalar type newScalarType extending str;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'scalar'",
                  line=4, col=9)
    def test_eschema_syntax_ws_03(self):
        """
        scalar type newScalarType0 extending str#:

        scalar type newScalarType1 extending str#:
        """

    def test_eschema_syntax_scalar_01(self):
        """
        scalar type issue_num_t extending std::sequence;
        """

    def test_eschema_syntax_scalar_02(self):
        """
        scalar type issue_num_t extending int {
            default := 42;
        };
        """

    def test_eschema_syntax_scalar_03(self):
        r"""
        scalar type basic extending int {
            delegated constraint min_value(0);
            constraint max_value(123456);
            constraint must_be_even;

            title := 'Basic ScalarType';
            default := 2;
        };
        """

    def test_eschema_syntax_scalar_04(self):
        """
        scalar type basic extending int {
            constraint min_value(0);
            constraint max_value(123456);
            delegated constraint expr on (__subject__ % 2 = 0);

            title := 'Basic ScalarType';
            default := 2;
        };
        """

    def test_eschema_syntax_scalar_05(self):
        """
        scalar type basic extending int {
            constraint expr {
                prop :=
                    (__subject__ % 2 = 0);
            };
            constraint min_value(0);
            constraint max_value(123456);

            title := 'Basic ScalarType';
            default := 2;
        };
        """

    def test_eschema_syntax_scalar_06(self):
        """
        scalar type basic extending int {
            constraint min_value(0);
            constraint max_value(123456);
            constraint expr {
                abc := (__subject__ % 2 = 0);
            };

            title := 'Basic ScalarType';
            default := 2;
        };

        scalar type inherits_default extending basic;

        abstract scalar type abstract_scalar extending int;
        """

    def test_eschema_syntax_scalar_07(self):
        """
        final scalar type none;
        """

    def test_eschema_syntax_scalar_08(self):
        """
        scalar type basic extending int {
            constraint special_constraint;
            title := 'Basic ScalarType';
            default := 2;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected ':='",
                  line=3, col=43)
    def test_eschema_syntax_scalar_09(self):
        """
        scalar type special extending int {
            constraint special_constraint := [42, 100, 9001];
        };
        """

    def test_eschema_syntax_scalar_10(self):
        """
        scalar type special extending int {
            constraint special_constraint {
                using (__subject__ % 2 = 0);
            };
            title := 'Special ScalarType';
        };
        """

    def test_eschema_syntax_scalar_11(self):
        """
        scalar type constraint_length extending str {
             constraint max_len_value(16+1, len(([1])));
        };
        """

    def test_eschema_syntax_scalar_12(self):
        """
        scalar type constraint_length extending str {
             constraint max_len_value((16+(4*2))/((4)-1), len(([1])));
        };
        """

    def test_eschema_syntax_constraint_01(self):
        """
        abstract constraint max_value(param:anytype) on (()) {
            using (__subject__ <= $param);
            errmessage := 'Maximum allowed value for {subject} is {$param}.';
        };

% OK %

        abstract constraint max_value(param:anytype) on (()) {
            using (__subject__ <= $param);
            errmessage := 'Maximum allowed value for {subject} is {$param}.';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'delegated'",
                  line=2, col=9)
    def test_eschema_syntax_constraint_02(self):
        """
        delegated constraint length {
            subject := str::len(<str>__subject__);
        };
        """

    def test_eschema_syntax_constraint_03(self):
        """
        abstract constraint max_len_value(param:anytype)
                extending max, length {
            errmessage :=
                '{subject} must be no longer than {$param} characters.';
        };
        """

    def test_eschema_syntax_constraint_04(self):
        """
        abstract constraint max_value(param:anytype) {
            using (__subject__ <= $param);
            errmessage := 'Maximum allowed value for {subject} is {$param}.';
        };

        abstract constraint length {
            subject := str::len(<str>__subject__);
        };

        abstract constraint max_len_value(param:anytype)
                extending max_value, length {
            errmessage :=
                '{subject} must be no longer than {$param} characters.';
        };
        """

    def test_eschema_syntax_constraint_05(self):
        """
        abstract constraint distance {
            subject :=
                <float64>__subject__;
        };

        abstract constraint maxldistance extending max, distance {
            errmessage := '{subject} must be no longer than {$param} meters.';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"missing type declaration.*`param`",
                  line=2, col=43)
    def test_eschema_syntax_constraint_06(self):
        """
        abstract constraint max_len_value(param) extending max, length;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'constraint'",
                  line=3, col=22)
    def test_eschema_syntax_constraint_07(self):
        """
        scalar type special extending int {
            abstract constraint length {
                subject := str::len(<str>__subject__);
            };
        };
        """

    def test_eschema_syntax_constraint_08(self):
        """
        abstract constraint foo(param:Foo) on (len(__subject__.bar))
            extending max {
                errmessage := 'bar must be no more than {$param}.';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'constraint'",
                  line=2, col=9)
    def test_eschema_syntax_constraint_09(self):
        """
        constraint foo;
        """

    def test_eschema_syntax_constraint_10(self):
        """
        scalar type foo extending str {
            constraint maxldistance {
                errmessage :=
                    '{__subject__} must be no longer than {$param} meters.';
            };

            constraint max_len_value(4);
        };
        """

    def test_eschema_syntax_property_01(self):
        """
abstract property foo {
    title := 'Sample property';
};
        """

    def test_eschema_syntax_property_02(self):
        """
        abstract property bar extending foo;
        """

    def test_eschema_syntax_property_03(self):
        """
        abstract property bar extending foo {
            title := 'Another property';
        };
        """

    def test_eschema_syntax_property_04(self):
        """
        abstract property foo {
            title := 'Sample property';
        };

        abstract property bar extending foo {
            title := 'Another property';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'property'",
                  line=2, col=9)
    def test_eschema_syntax_property_05(self):
        """
        property foo;
        """

    def test_eschema_syntax_link_01(self):
        """
        abstract link coollink;
        """

    def test_eschema_syntax_link_02(self):
        """
        abstract link coollink extending boringlink;
        """

    def test_eschema_syntax_link_03(self):
        """
        abstract link coollink {
            property foo -> int64;
        };
        """

    def test_eschema_syntax_link_04(self):
        """
        abstract link coollink {
            property foo -> int64;
            property bar -> int64;

            constraint expr {
                using (self.foo = self.bar);
            };
        };      """

    def test_eschema_syntax_link_05(self):
        """
        abstract property foo {
            title := 'Sample property';
        };

        abstract property bar extending foo {
            title := 'Another property';
        };

        abstract link coollink {
            property foo -> int64 {
                constraint min_value(0);
                constraint max_value(123456);
                constraint expr on (__subject__ % 2 = 0) {
                    title := 'aaa';
                };
                default := 2;
            };

            property bar -> int64;

            constraint expr on (self.foo = self.bar);
        };
        """

    # FIXME: should the link property be banned from being required?
    def test_eschema_syntax_link_06(self):
        """
        abstract link coollink {
            required property foo -> int64;
        };
        """

    def test_eschema_syntax_link_07(self):
        """
        abstract link time_estimate {
           property unit -> str {
               constraint my_constraint(0);
           };
        };
        """

    def test_eschema_syntax_link_08(self):
        """
        abstract link time_estimate {
           property unit -> str {
               constraint my_constraint(0, <str>(42^2));
           };
        };
        """

    def test_eschema_syntax_link_09(self):
        """
        abstract link time_estimate {
           property unit -> str{
               constraint my_constraint(')', `)`($$)$$));
           };
        };
        """

    def test_eschema_syntax_link_10(self):
        """
        abstract link coollink;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'link'",
                  line=2, col=9)
    def test_eschema_syntax_link_11(self):
        """
        link foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected '::'",
                  line=3, col=21)
    def test_eschema_syntax_link_12(self):
        """
        type Foo {
            link mod::name to std::str;
        }
        """

    def test_eschema_syntax_function_01(self):
        """
        function len() -> std::int64
            using sql function 'length';
        """

    def test_eschema_syntax_function_02(self):
        r"""
        function some_func(foo: std::int64 = 42) -> std::str
            using sql $$
                SELECT 'life';
            $$;

% OK %

        function some_func(foo: std::int64 = 42) -> std::str
            using sql $$
                SELECT 'life';
            $$;
        """

    def test_eschema_syntax_function_03(self):
        r"""
        function some_func(foo: std::int64 = 42) -> std::str
            using (
                SELECT 'life'
            );
        """

    def test_eschema_syntax_function_04(self):
        """
        function myfunc(arg1: str, arg2: str = 'DEFAULT',
                        variadic arg3: std::int64) -> set of int {
            annotation description := 'myfunc sample';
            using sql
                $$SELECT blarg;$$;
        };
        """

    def test_eschema_syntax_function_05(self):
        """
        function myfunc(arg1: str,
                        arg2: str = 'DEFAULT',
                        variadic arg3: std::int64,
                        named only arg4: std::int64,
                        named only arg5: std::int64) -> set of int
            using (
                SELECT blarg
            );
        """

    def test_eschema_syntax_function_06(self):
        """
        function some_func(foo: std::int64 = 42) -> std::str {
            initial_value := 'bad';
            using (
                SELECT 'life'
            );
        };
        """

    def test_eschema_syntax_function_07(self):
        """
        function some_func(foo: std::int64 = bar(42)) -> std::str
            using sql function 'some_other_func';
        """

    def test_eschema_syntax_function_08(self):
        """
        function some_func(foo: str = ')') -> std::str
            using sql function 'some_other_func';
        """

    def test_eschema_syntax_function_09(self):
        """
        function some_func(foo: str = $$)$$) -> std::str
            using sql function 'some_other_func';
        """

    def test_eschema_syntax_function_10(self):
        """
        function some_func(foo: str = $a1$)$a1$) -> std::str
            using sql function 'some_other_func';
        """

    def test_eschema_syntax_function_11(self):
        """
        function some_func(`(`: str = ')') -> std::str
            using sql function 'some_other_func';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"function parameters do not need a \$",
                  line=2, col=28)
    def test_eschema_syntax_function_12(self):
        """
        function some_func($`(`: str = ) ) -> std::str {
            using edgeql function 'some_other_func';
        }
        """

    def test_eschema_syntax_function_13(self):
        r"""
        function some_func(`(`:
                str = ')',
                bar: int = bar()) -> std::str
            using sql function 'some_other_func';
        """

    def test_eschema_syntax_function_15(self):
        """
        function foo() -> tuple<
                    str,
                    array<tuple<int, str>>
                >
            using sql function 'some_other_func';
        """

    def test_eschema_syntax_function_16(self):
        """
        function foo() -> tuple<
                    str,
                    array<tuple<int, `Foo:>`>>
                >
            using sql function 'some_other_func';
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected '>'",
                  line=5, col=17)
    def test_eschema_syntax_function_17(self):
        """
        function foo() -> tuple<
                    str,
                    array<tuple<int, Foo>>>
                > {
            using sql function 'some_other_func';
        };
        """

    def test_eschema_syntax_function_18(self):
        """
        function len1() -> std::int64
            using sql function 'length1';

        function len2() -> std::int64
            using sql function 'length2';

        function len3() -> std::int64
            using sql function 'length3';

        function len4() -> std::int64
            using sql function 'length4';

% OK %

        function len1() ->  std::int64
            using SQL function 'length1';
        function len2() ->  std::int64
            using SQL function 'length2';
        function len3() ->  std::int64
            using SQL function 'length3';
        function len4() ->  std::int64
            using SQL function 'length4';
        """

    def test_eschema_syntax_function_19(self):
        """
        function len1() ->  std::int64 {
            using SQL function 'length1'
        }
        function len2() ->  std::int64 {
            using SQL function 'length2'
        }
        function len3() ->  std::int64 {
            using SQL function 'length3'
        }
        function len4() ->  std::int64 {
            using SQL function 'length4'
        }


% OK %

        function len1() ->  std::int64
            using SQL function 'length1';
        function len2() ->  std::int64
            using SQL function 'length2';
        function len3() ->  std::int64
            using SQL function 'length3';
        function len4() ->  std::int64
            using SQL function 'length4';
        """

    def test_eschema_syntax_function_20(self):
        """
        function len1() ->  std::int64 {
            using SQL function 'length1'
        }
        function len2() ->  std::int64
            using SQL function 'length2';

        function len3() ->  std::int64 {
            using SQL function 'length3'
        }
        function len4() ->  std::int64
            using SQL function 'length4';


% OK %

        function len1() ->  std::int64
            using SQL function 'length1';
        function len2() ->  std::int64
            using SQL function 'length2';
        function len3() ->  std::int64
            using SQL function 'length3';
        function len4() ->  std::int64
            using SQL function 'length4';
        """

    def test_eschema_syntax_view_01(self):
        """
        view FooBaz {
            annotation description := 'Special Foo';
            using (SELECT Foo FILTER Foo.bar = 'baz');
        };
        """

    def test_eschema_syntax_view_02(self):
        """
        view FooBaz {
            using (
                SELECT Foo
                FILTER Foo.bar = 'baz'
            );
        };

% OK %

        view FooBaz := (
            SELECT Foo
            FILTER Foo.bar = 'baz'
        );
        """

    def test_eschema_syntax_view_03(self):
        """
        view FooBaz := (
            SELECT Foo
            FILTER Foo.bar = 'baz'
        );
        """

    def test_eschema_syntax_annotation_01(self):
        """
        abstract annotation foobar;
        """

    def test_eschema_syntax_annotation_03(self):
        """
        abstract annotation foobar extending baz;
        """

    def test_eschema_syntax_annotation_04(self):
        """
        abstract annotation foobar {
            title := 'Some title';
        };
        """

    def test_eschema_syntax_annotation_06(self):
        """
        abstract annotation foobar extending baz {
            title := 'Some title';
        };
        """

    def test_eschema_syntax_annotation_08(self):
        """
        abstract annotation foobar extending foo1, foo2;
        """

    def test_eschema_syntax_annotation_09(self):
        """
        abstract annotation foobar extending foo1,
    foo2;
        """

    def test_eschema_syntax_annotation_10(self):
        """
        abstract annotation foobar extending foo1,
    foo2 {
            title := 'Title';
        };
        """

    def test_eschema_syntax_annotation_11(self):
        """
        abstract annotation as extending foo;
        """

    def test_eschema_syntax_annotation_12(self):
        """
        abstract inheritable annotation foo;
        """

    def test_eschema_syntax_annotation_13(self):
        """
        abstract inheritable annotation foo extending bar;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'extending'", line=2, col=42)
    def test_eschema_syntax_annotation_14(self):
        """
        abstract annotation as extending extending foo;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'annotation'",
                  line=2, col=1)
    def test_eschema_syntax_annotation_15(self):
        """
annotation foo;
        """
