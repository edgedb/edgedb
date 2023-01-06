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
from edb.tools import test


class SchemaSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s\'"()]+|(#.*?(\n|$))|select|SELECT')
    parser_debug_flag = 'DEBUG_ESCHEMA'
    markup_dump_lexer = 'edgeschema'
    ast_to_source = functools.partial(generate_source, unsorted=True)

    @classmethod
    def get_parser(cls):
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
\tabstract type test::Foo {
\t\trequired property foo -> str;
\t};

\tabstract type test::Bar {
\t\trequired property bar -> str;
\t};
        """

    def test_eschema_syntax_tabs_02(self):
        """
\t  abstract type test::Foo {
\t      required property foo -> str;
};

\t  abstract type test::Bar {
\t      required property bar -> str;
};
        """
    def test_eschema_syntax_semicolon_01(self):
        """
        abstract type test::OwnedObject {
            required link owner -> User
        };

% OK %

        abstract type test::OwnedObject {
            required link owner -> User;
        };
        """

    def test_eschema_syntax_semicolon_02(self):
        """
        module test {
            abstract type OwnedObject {
                required property tag -> str
            }
        }

% OK %

        module test {
            abstract type OwnedObject {
                required property tag -> str;
            };
        };
        """

    def test_eschema_syntax_semicolon_03(self):
        """
        abstract type test::OwnedObject {
            required link owner -> User;
            required property tag -> str
        };

% OK %

        abstract type test::OwnedObject {
            required link owner -> User;
            required property tag -> str;
        };
        """

    def test_eschema_syntax_type_01(self):
        """type test::User extending builtins::NamedObject;"""

    def test_eschema_syntax_type_02(self):
        """
        abstract type test::OwnedObject {
            required link owner -> User;
        };
        """

    def test_eschema_syntax_type_03(self):
        """
        module test {
            abstract type Text {
                required property body -> str {
                    constraint max_len_value (10000);
                };
            };
        };
        """

    def test_eschema_syntax_type_04(self):
        """
        module test {
            type LogEntry extending OwnedObject, Text {
                required property spent_time -> int64;
            };
        };
        """

    def test_eschema_syntax_type_05(self):
        """
        module test {
            type LogEntry extending OwnedObject, Text {
               link start_date := (SELECT datetime::datetime_current());
            };
        };
        """

    def test_eschema_syntax_type_06(self):
        """
        module test {
            type LogEntry extending OwnedObject, Text {
                property start_date -> datetime {
                   default :=
                        (SELECT datetime::datetime_current());
                   title := 'Start Date';
               };
            };
        };
        """

    def test_eschema_syntax_type_07(self):
        """
        module test {
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

                property real_time_estimate {
                    using ((.time_estimate * 2));
                    annotation title := 'Ha.';
                };
                link start_date2 {
                    using (SELECT datetime::datetime_current());
                    annotation title := 'awk.';
                };
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "Unexpected 'unit'", line=5, col=30)
    def test_eschema_syntax_type_08(self):
        """
        module test {
            type Foo {
                property time_estimate -> int64 {
                    property unit {
                        default := 'minute';
                    };
                };
            };
        };
        """

    def test_eschema_syntax_type_09(self):
        """
        module test {
            type LogEntry extending OwnedObject, Text {
                required link attachment -> Post | File | User;
            };
        };
        """

    def test_eschema_syntax_type_10(self):
        """
        module test {
            type `Log-Entry` extending `OwnedObject`, `Text` {
                required link attachment -> `Post` | `File` | `User`;
            };
        };

% OK %

        module test {
            type `Log-Entry` extending OwnedObject, Text {
                required link attachment -> Post | File | User;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, "Unexpected keyword 'Commit'",
                  line=3, col=18)
    def test_eschema_syntax_type_11(self):
        """
        module test {
            type Commit {
                required property name -> std::str;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=18)
    def test_eschema_syntax_type_12(self):
        """
        module test {
            type __Foo__ {
                required property name -> std::str;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=3, col=18)
    def test_eschema_syntax_type_13(self):
        """
        module test {
            type `__Foo__` {
                required property name -> std::str;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=35)
    def test_eschema_syntax_type_14(self):
        """
        module test {
            type __Foo {
                required property __name__ -> std::str;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, line=4, col=35)
    def test_eschema_syntax_type_15(self):
        """
        module test {
            type `__Foo` {
                required property `__name__` -> std::str;
            };
        };
        """

    def test_eschema_syntax_type_16(self):
        """
        module test {
            type Пример {
                required property номер -> int16;
            };
        };
        """

    def test_eschema_syntax_type_17(self):
        """
        module test {
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
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  "more than one 'on target delete' specification",
                  line=6, col=21)
    def test_eschema_syntax_type_18(self):
        """
        module test {
            type Foo {
                link bar0 -> Bar {
                    on target delete restrict;
                    on target delete delete source;
                };
            };
        };
        """

    def test_eschema_syntax_type_19(self):
        """
        module test {
            type Foo {
                property foo -> str {
                    default := some_func(
        1, 2, 3);
                };
            };
        };
        """

    def test_eschema_syntax_type_22(self):
        r"""
        module test {
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
        };

% OK %

        module test {
            type Foo {
                property foo -> str {
                    # if it's defined on the same line as :=
                    # the definition must be a one-liner
                    default := some_func(1, 2, 3);
                };
                property bar -> str {
                    # multi-line definition with correct indentation
                    default := some_func('\n                        1, 2, 3');
                };
                property baz -> str {
                    # multi-line definition with correct indentation
                    default := 'some_func(\n                        1, 2, 3)';
                };
            };
        };
        """

    def test_eschema_syntax_type_23(self):
        """
        module test {
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
                constraint exclusive on (.asdf) except (.baz);
                index on (.asdf) except (.baz);
            };
        };
        """

    def test_eschema_syntax_type_24(self):
        """
        module test {
            type Foo {
                single property foo -> str;
                multi property bar -> str;
                required single property baz -> str;
                required multi property spam -> str;
                overloaded required single property ham -> str;
                overloaded required multi property eggs -> str;
            };
        };
        """

    def test_eschema_syntax_type_25(self):
        """
        module test {
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
        }

% OK %

        module test {
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
        };
        """

    def test_eschema_syntax_type_26(self):
        """
        module test {
            type Foo {
                single property foo -> str {
                    constraint max_len_value (10000)
                }

                multi property bar -> str {
                    constraint max_len_value (10000)
                }
            }
        }

% OK %

        module test {
            type Foo {
                single property foo -> str {
                    constraint max_len_value (10000);
                };

                multi property bar -> str {
                    constraint max_len_value (10000);
                };
            };
        };
        """

    def test_eschema_syntax_type_27(self):
        """
        type foo::Bar {
            property name -> str;
        };
        """

    def test_eschema_syntax_type_28(self):
        """
        module foo {
            type Bar {
                property name -> str;
            };
        };
        """

    @tb.must_fail(
        errors.EdgeQLSyntaxError,
        "fully-qualified name is not allowed in a module declaration",
        line=3, col=18)
    def test_eschema_syntax_type_29(self):
        """
        module foo {
            type foo::Bar {
                property name -> str;
            };
        };
        """

    def test_eschema_syntax_type_30(self):
        """
        module foo {
            type Bar {
                property name -> str;
            };
        };

        type foo::Bar2 {
            property name -> str;
        };

        module foo {
            type Bar3 {
                property name -> str;
            };
        };
        """

    def test_eschema_syntax_type_31(self):
        """
        module foo {
            type Bar {
                property name -> str;
            };
        };

        type bar::Bar {
            property name -> str;
        };

        module baz {
            type Bar {
                property name -> str;
            };
        };
        """

    def test_eschema_syntax_type_32(self):
        """
        type test::Foo {
            property lurr {
                using (20);
            };
        }
% OK %
        type test::Foo {
            property lurr := (20);
        };
        """

    def test_eschema_syntax_type_33(self):
        """
        module default {
            type Foo0 {
                property union -> str;
                link except -> Object;
            };
            type Foo1 {
                required property union -> str;
                required link except -> Object;
            };
            type Foo2 {
                optional property union -> str;
                optional link except -> Object;
            };
        };
        """

    def test_eschema_syntax_type_34(self):
        """
        module default {
            type Foo1 {
                multi property union -> str;
                multi link except -> Object;
            };
            type Foo2 {
                single property union -> str;
                single link except -> Object;
            };
        };
        """

    def test_eschema_syntax_type_35(self):
        """
        module default {
            type Foo {
                property union extending except -> str;
            };
        };
        """

    def test_eschema_syntax_type_36(self):
        """
        module default {
            type Foo {
                link union extending except -> Object {
                    property intersect -> str;
                };
            };
        };
        """

    def test_eschema_syntax_link_target_type_01(self):
        """
        module test {
            type User {
                required link todo -> array<str>;
            };
        };
        """

    def test_eschema_syntax_link_target_type_03(self):
        """
        module test {
            type User {
                required link todo -> tuple<str, int64, float64>;
            };
        };
        """

    def test_eschema_syntax_link_target_type_04(self):
        """
        module test {
            type User {
                required link todo ->
                    tuple<str, tuple<str, array<str>>, array<float64>>;
            };
        };
        """

    def test_eschema_syntax_index_01(self):
        """
        module test {
            type LogEntry extending OwnedObject, Text {
                required link owner -> User;
                index on (SELECT datetime::datetime_current());
            };
        };
        """

    def test_eschema_syntax_index_02(self):
        """
        module test {
            abstract link foobar {
                property foo -> str {
                    title := 'Sample property';
                };
                index on (__subject__@foo);
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Expected 'ON', but got 'prop' instead", line=4, col=23)
    def test_eschema_syntax_index_03(self):
        """
        module test {
            scalar type foobar {
                index prop on (__source__);
            };
        };
        """

    def test_eschema_syntax_index_04(self):
        """
        module test {
            type User {
                property name -> str;
                index on (.name);
            };
        };
        """

    def test_eschema_syntax_index_05(self):
        """
        module test {
            type User {
                property name -> str;

                index on (.name) {
                    annotation title := 'User name index';
                };
            };
        };
        """

    def test_eschema_syntax_index_06(self):
        """
        module test {
            type Foo {
                property title -> str;
                index pg::gist on (.title);
            };
        };
        """

    @test.xerror('index parameters not implemented yet')
    def test_eschema_syntax_index_07(self):
        """
        module test {
            alias eng_stop := to_json('["english_stop"]');
            alias lowercase := to_json('["lowercase"]');

            type Foo {
                property bar -> str;
                property baz -> str;
                property foo -> str;

                index myindex0() on (.bar);
                index myindex1(tok_filter := eng_stop ++ lowercase)
                    on (.baz);
                index myindex2(num := 13, val := 'ab')
                    on (.foo);
            };
        };
        """

    def test_eschema_syntax_index_08(self):
        """
        module test {
            abstract index myindex0;
        };
        """

    @test.xerror('index parameters and fallback not implemented yet')
    def test_eschema_syntax_index_09(self):
        """
        module test {
            abstract index myindex1(conf: str = 'special');
            abstract index myindex2(val: int64);
            abstract index myindex3(a : int64, b : str = 'default')
                using myindex2(val := a),
                      myindex1(conf := b),
                      myindex1;
        };
        """

    def test_eschema_syntax_index_10(self):
        """
        module test {
            abstract index myindex1 extending fts;
            abstract index myindex2(conf := 'test') extending fts;
        };
        """

    def test_eschema_syntax_ws_01(self):
        """
        module test {
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
        };
        """

    def test_eschema_syntax_ws_02(self):
        """
        scalar type test::newScalarType extending str#:

% OK %

        scalar type test::newScalarType extending str;
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'scalar'",
                  line=4, col=9)
    def test_eschema_syntax_ws_03(self):
        """
        scalar type test::newScalarType0 extending str#:

        scalar type test::newScalarType1 extending str#:
        """

    def test_eschema_syntax_scalar_01(self):
        """
        module test {
            scalar type issue_num_t extending std::sequence;
        };
        """

    def test_eschema_syntax_scalar_02(self):
        """
        module test {
            scalar type issue_num_t extending int {
                default := 42;
            };
        };
        """

    def test_eschema_syntax_scalar_03(self):
        r"""
        module test {
            scalar type basic extending int {
                delegated constraint min_value(0);
                constraint max_value(123456);
                constraint must_be_even;

                title := 'Basic ScalarType';
                default := 2;
            };
        };
        """

    def test_eschema_syntax_scalar_04(self):
        """
        module test {
            scalar type basic extending int {
                constraint min_value(0);
                constraint max_value(123456);
                delegated constraint expr on (__subject__ % 2 = 0);

                title := 'Basic ScalarType';
                default := 2;
            };
        };
        """

    def test_eschema_syntax_scalar_05(self):
        """
        module test {
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
        };
        """

    def test_eschema_syntax_scalar_06(self):
        """
        module test {
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
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'final'",
                  line=3, col=13)
    def test_eschema_syntax_scalar_07(self):
        """
        module test {
            final scalar type none;
        };
        """

    def test_eschema_syntax_scalar_08(self):
        """
        module test {
            scalar type basic extending int {
                constraint special_constraint;
                title := 'Basic ScalarType';
                default := 2;
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected ':='",
                  line=4, col=47)
    def test_eschema_syntax_scalar_09(self):
        """
        module test {
            scalar type special extending int {
                constraint special_constraint := [42, 100, 9001];
            };
        };
        """

    def test_eschema_syntax_scalar_10(self):
        """
        module test {
            scalar type special extending int {
                constraint special_constraint {
                    using (__subject__ % 2 = 0);
                };
                title := 'Special ScalarType';
            };
        };
        """

    def test_eschema_syntax_scalar_11(self):
        """
        module test {
            scalar type constraint_length extending str {
                 constraint max_len_value(16+1, len(([1])));
            };
        };
        """

    def test_eschema_syntax_scalar_12(self):
        """
        module test {
            scalar type constraint_length extending str {
                 constraint max_len_value((16+(4*2))/((4)-1), len(([1])));
            };
        };
        """

    def test_eschema_syntax_constraint_01(self):
        """
        abstract constraint test::max_value(param:anytype) on (()) {
            using (__subject__ <= $param);
            errmessage := 'Maximum allowed value for {subject} is {$param}.';
        };

% OK %

        abstract constraint test::max_value(param:anytype) on (()) {
            using (__subject__ <= $param);
            errmessage := 'Maximum allowed value for {subject} is {$param}.';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'delegated'",
                  line=3, col=13)
    def test_eschema_syntax_constraint_02(self):
        """
        module test {
            delegated constraint length {
                subject := str::len(<str>__subject__);
            };
        };
        """

    def test_eschema_syntax_constraint_03(self):
        """
        module test {
            abstract constraint max_len_value(param:anytype)
                    extending max, length {
                errmessage :=
                    '{subject} must be no longer than {$param} characters.';
            };
        };
        """

    def test_eschema_syntax_constraint_04(self):
        """
        module test {
            abstract constraint max_value(param:anytype) {
                using (__subject__ <= $param);
                errmessage :=
                    'Maximum allowed value for {subject} is {$param}.';
            };

            abstract constraint length {
                subject := str::len(<str>__subject__);
            };

            abstract constraint max_len_value(param:anytype)
                    extending max_value, length {
                errmessage :=
                    '{subject} must be no longer than {$param} characters.';
            };
        };
        """

    def test_eschema_syntax_constraint_05(self):
        """
        module test {
            abstract constraint distance {
                subject :=
                    <float64>__subject__;
            };

            abstract constraint maxldistance extending max, distance {
                errmessage :=
                    '{subject} must be no longer than {$param} meters.';
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"missing type declaration.*`param`",
                  line=3, col=47)
    def test_eschema_syntax_constraint_06(self):
        """
        module test {
            abstract constraint max_len_value(param) extending max, length;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected 'constraint'",
                  line=4, col=26)
    def test_eschema_syntax_constraint_07(self):
        """
        module test {
            scalar type special extending int {
                abstract constraint length {
                    subject := str::len(<str>__subject__);
                };
            };
        };
        """

    def test_eschema_syntax_constraint_08(self):
        """
        module test {
            abstract constraint foo(param:Foo) on (len(__subject__.bar))
                extending max {
                    errmessage := 'bar must be no more than {$param}.';
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'constraint'",
                  line=3, col=13)
    def test_eschema_syntax_constraint_09(self):
        """
        module test {
            constraint foo;
        };
        """

    def test_eschema_syntax_constraint_10(self):
        """
        module test {
            scalar type foo extending str {
                constraint maxldistance {
                    errmessage :=
                      '{__subject__} must be no longer than {$param} meters.';
                };

                constraint max_len_value(4);
            };
        };
        """

    def test_eschema_syntax_property_01(self):
        """
abstract property test::foo {
    title := 'Sample property';
};
        """

    def test_eschema_syntax_property_02(self):
        """
        module test {
            abstract property bar extending foo;
        };
        """

    def test_eschema_syntax_property_03(self):
        """
        module test {
            abstract property bar extending foo {
                title := 'Another property';
            };
        };
        """

    def test_eschema_syntax_property_04(self):
        """
        module test {
            abstract property foo {
                title := 'Sample property';
            };

            abstract property bar extending foo {
                title := 'Another property';
            };
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'property'",
                  line=3, col=13)
    def test_eschema_syntax_property_05(self):
        """
        module test {
            property foo;
        };
        """

    def test_eschema_syntax_property_06(self):
        """
        module test {
            abstract property union extending except;
        };
        """

    def test_eschema_syntax_link_01(self):
        """
        module test {
            abstract link coollink;
        };
        """

    def test_eschema_syntax_link_02(self):
        """
        module test {
            abstract link coollink extending boringlink;
        };
        """

    def test_eschema_syntax_link_03(self):
        """
        module test {
            abstract link coollink {
                property foo -> int64;
            };
        };
        """

    def test_eschema_syntax_link_04(self):
        """
        abstract link test::coollink {
            property foo -> int64;
            property bar -> int64;

            constraint expr {
                using (self.foo = self.bar);
            };
        };      """

    def test_eschema_syntax_link_05(self):
        """
        module test {
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
        };
        """

    # FIXME: should the link property be banned from being required?
    def test_eschema_syntax_link_06(self):
        """
        module test {
            abstract link coollink {
                required property foo -> int64;
            };
        };
        """

    def test_eschema_syntax_link_07(self):
        """
        module test {
            abstract link time_estimate {
               property unit -> str {
                   constraint my_constraint(0);
               };
            };
        };
        """

    def test_eschema_syntax_link_08(self):
        """
        module test {
            abstract link time_estimate {
               property unit -> str {
                   constraint my_constraint(0, <str>(42^2));
               };
            };
        };
        """

    def test_eschema_syntax_link_09(self):
        """
        module test {
            abstract link time_estimate {
               property unit -> str{
                   constraint my_constraint(')', `)`($$)$$));
               };
            };
        };

% OK %

        module test {
            abstract link time_estimate {
               property unit -> str{
                   constraint my_constraint(')', `)`(')'));
               };
            };
        };
        """

    def test_eschema_syntax_link_10(self):
        """
        module test {
            abstract link coollink;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'link'",
                  line=3, col=13)
    def test_eschema_syntax_link_11(self):
        """
        module test {
            link foo;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected '::'",
                  line=4, col=25)
    def test_eschema_syntax_link_12(self):
        """
        module test {
            type Foo {
                link mod::name to std::str;
            }
        };
        """

    def test_eschema_syntax_property_13(self):
        """
        module test {
            abstract link union extending except;
        };
        """

    def test_eschema_syntax_property_14(self):
        """
        module test {
            abstract link union extending intersect {
                property except -> str;
            };
        };
        """

    def test_eschema_syntax_function_01(self):
        """
        module test {
            function len() -> std::int64
                using sql function 'length';
        };
        """

    def test_eschema_syntax_function_02(self):
        r"""
        function test::some_func(foo: std::int64 = 42) -> std::str
            using sql $$
                SELECT 'life';
            $$;

% OK %

        function test::some_func(foo: std::int64 = 42) -> std::str
            using sql $$
                SELECT 'life';
            $$;
        """

    def test_eschema_syntax_function_03(self):
        r"""
        module test {
            function some_func(foo: std::int64 = 42) -> std::str
                using (
                    SELECT 'life'
                );
        };
        """

    def test_eschema_syntax_function_04(self):
        """
        module test {
            function myfunc(arg1: str, arg2: str = 'DEFAULT',
                            variadic arg3: std::int64) -> set of int {
                annotation description := 'myfunc sample';
                using sql
                    $$SELECT blarg;$$;
            };
        };
        """

    def test_eschema_syntax_function_05(self):
        """
        module test {
            function myfunc(arg1: str,
                            arg2: str = 'DEFAULT',
                            variadic arg3: std::int64,
                            named only arg4: std::int64,
                            named only arg5: std::int64) -> set of int
                using (
                    SELECT blarg
                );
        };
        """

    def test_eschema_syntax_function_06(self):
        """
        module test {
            function some_func(foo: std::int64 = 42) -> std::str {
                initial_value := 'bad';
                using (
                    SELECT 'life'
                );
            };
        };
        """

    def test_eschema_syntax_function_07(self):
        """
        module test {
            function some_func(foo: std::int64 = bar(42)) -> std::str
                using sql function 'some_other_func';
        };
        """

    def test_eschema_syntax_function_08(self):
        """
        module test {
            function some_func(foo: str = ')') -> std::str
                using sql function 'some_other_func';
        };
        """

    def test_eschema_syntax_function_09(self):
        """
        module test {
            function some_func(foo: str = $$)$$) -> std::str
                using sql function 'some_other_func';
        };

% OK %

        module test {
            function some_func(foo: str = ')') -> std::str
                using sql function 'some_other_func';
        };
        """

    def test_eschema_syntax_function_10(self):
        """
        module test {
            function some_func(foo: str = $a1$)$a1$) -> std::str
                using sql function 'some_other_func';
        };

% OK %

        module test {
            function some_func(foo: str = ')') -> std::str
                using sql function 'some_other_func';
        };

        """

    def test_eschema_syntax_function_11(self):
        """
        module test {
            function some_func(`(`: str = ')') -> std::str
                using sql function 'some_other_func';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"function parameters do not need a \$",
                  line=3, col=32)
    def test_eschema_syntax_function_12(self):
        """
        module test {
            function some_func($`(`: str = ) ) -> std::str {
                using edgeql function 'some_other_func';
            }
        };
        """

    def test_eschema_syntax_function_13(self):
        r"""
        module test {
            function some_func(`(`:
                    str = ')',
                    bar: int = bar()) -> std::str
                using sql function 'some_other_func';
        };
        """

    def test_eschema_syntax_function_15(self):
        """
        module test {
            function foo() -> tuple<
                        str,
                        array<tuple<int, str>>
                    >
                using sql function 'some_other_func';
        };
        """

    def test_eschema_syntax_function_16(self):
        """
        module test {
            function foo() -> tuple<
                        str,
                        array<tuple<int, `Foo:>`>>
                    >
                using sql function 'some_other_func';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected '>'",
                  line=6, col=21)
    def test_eschema_syntax_function_17(self):
        """
        module test {
            function foo() -> tuple<
                        str,
                        array<tuple<int, Foo>>>
                    > {
                using sql function 'some_other_func';
            };
        };
        """

    def test_eschema_syntax_function_18(self):
        """
        module test {
            function len1() -> std::int64
                using sql function 'length1';

            function len2() -> std::int64
                using sql function 'length2';

            function len3() -> std::int64
                using sql function 'length3';

            function len4() -> std::int64
                using sql function 'length4';
        }

% OK %

        module test {
            function len1() ->  std::int64
                using SQL function 'length1';
            function len2() ->  std::int64
                using SQL function 'length2';
            function len3() ->  std::int64
                using SQL function 'length3';
            function len4() ->  std::int64
                using SQL function 'length4';
        };
        """

    def test_eschema_syntax_function_19(self):
        """
        module test {
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
        }

% OK %

        module test {
            function len1() ->  std::int64
                using SQL function 'length1';
            function len2() ->  std::int64
                using SQL function 'length2';
            function len3() ->  std::int64
                using SQL function 'length3';
            function len4() ->  std::int64
                using SQL function 'length4';
        };
        """

    def test_eschema_syntax_function_20(self):
        """
        module test {
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
        }

% OK %

        module test {
            function len1() ->  std::int64
                using SQL function 'length1';
            function len2() ->  std::int64
                using SQL function 'length2';
            function len3() ->  std::int64
                using SQL function 'length3';
            function len4() ->  std::int64
                using SQL function 'length4';
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+b',
                  hint=r"It appears that a ',' is missing in a list of "
                       r"arguments before 'b'",
                  line=3, col=34)
    def test_eschema_syntax_function_21(self):
        """
        module test {
            function len1(a: str b: str) ->  std::str {
                using SQL function 'length1'
            }
        """

    def test_eschema_syntax_function_22(self):
        """
        module test {
            function some_func(a: str) -> std::str {
                volatility := 'Immutable';
                using sql function 'some_other_func';
            };
        };
        """

    def test_eschema_syntax_alias_01(self):
        """
        module test {
            alias FooBaz {
                annotation description := 'Special Foo';
                using (SELECT Foo FILTER Foo.bar = 'baz');
            };
        };
        """

    def test_eschema_syntax_alias_02(self):
        """
        module test {
            alias FooBaz {
                using (
                    SELECT Foo
                    FILTER Foo.bar = 'baz'
                );
            };
        }

% OK %

        module test {
            alias FooBaz := (
                SELECT Foo
                FILTER Foo.bar = 'baz'
            );
        };
        """

    def test_eschema_syntax_alias_03(self):
        """
        module test {
            alias FooBaz := (
                SELECT Foo
                FILTER Foo.bar = 'baz'
            );
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+baz',
                  hint=r"It appears that a ',' is missing in a shape "
                       r"before 'baz'",
                  line=5, col=17)
    def test_eschema_syntax_alias_04(self):
        """
        module test {
            alias FooBaz := Foo {
                val := 1
                baz := .bar + 2
            }
            );
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in a tuple "
                       r"before '2'",
                  line=3, col=32)
    def test_eschema_syntax_alias_05(self):
        """
        module test {
            alias FooBaz := (1 2);
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r'Unexpected token:.+2',
                  hint=r"It appears that a ',' is missing in an array "
                       r"before '2'",
                  line=3, col=32)
    def test_eschema_syntax_alias_06(self):
        """
        module test {
            alias FooBaz := [1 2];
        };
        """

    def test_eschema_syntax_annotation_01(self):
        """
        module test {
            abstract annotation foobar;
        };
        """

    def test_eschema_syntax_annotation_03(self):
        """
        module test {
            abstract annotation foobar extending baz;
        };
        """

    def test_eschema_syntax_annotation_04(self):
        """
        module test {
            abstract annotation foobar {
                title := 'Some title';
            };
        };
        """

    def test_eschema_syntax_annotation_06(self):
        """
        module test {
            abstract annotation foobar extending baz {
                title := 'Some title';
            };
        };
        """

    def test_eschema_syntax_annotation_08(self):
        """
        module test {
            abstract annotation foobar extending foo1, foo2;
        };
        """

    def test_eschema_syntax_annotation_09(self):
        """
        abstract annotation test::foobar extending foo1,
    foo2;
        """

    def test_eschema_syntax_annotation_10(self):
        """
        abstract annotation test::foobar extending foo1,
    foo2 {
            title := 'Title';
        };
        """

    def test_eschema_syntax_annotation_11(self):
        """
        module test {
            abstract annotation as extending foo;
        };
        """

    def test_eschema_syntax_annotation_12(self):
        """
        module test {
            abstract inheritable annotation foo;
        };
        """

    def test_eschema_syntax_annotation_13(self):
        """
        module test {
            abstract inheritable annotation foo extending bar;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError,
                  r"Unexpected keyword 'extending'", line=3, col=46)
    def test_eschema_syntax_annotation_14(self):
        """
        module test {
            abstract annotation as extending extending foo;
        };
        """

    @tb.must_fail(errors.EdgeQLSyntaxError, r"Unexpected 'annotation'",
                  line=2, col=1)
    def test_eschema_syntax_annotation_15(self):
        """
annotation test::foo;
        """

    def test_eschema_syntax_future_1(self):
        """
        using future foo;
        """

    def test_eschema_syntax_global_1(self):
        """
        module test {
            global foo -> str;
        };
        """

    def test_eschema_syntax_global_2(self):
        """
        global test::foo -> str;
        """

    def test_eschema_syntax_global_3(self):
        """
        module test {
            optional global foo := 1;
        };
        """

    def test_eschema_syntax_global_4(self):
        """
        module test {
            required global foo -> str {
                default := "lol";
                annotation title := "foo";
            };
        };
        """

    def test_eschema_syntax_global_5(self):
        """
        module test {
            required global foo {
                using (50);
                annotation title := "foo";
            };
        };
        """

    def test_eschema_syntax_policy_1(self):
        """
        module test {
            type Foo {
                access policy test
                allow all
                using (true);
            };
        };
        """

    def test_eschema_syntax_policy_2(self):
        """
        module test {
            type Foo {
                access policy test
                allow select, update, insert
                using (1 + 2 = 3);
            };
        };
        """

    def test_eschema_syntax_policy_3(self):
        """
        module test {
            type Foo {
                access policy test
                when (true)
                deny all
                using (true) {
                   annotation title := 'foo';
                };
                property x -> str;
            };
        };
        """

    def test_eschema_syntax_policy_4(self):
        """
        module test {
            type Foo {
                access policy test
                when (true)
                deny all
                using (true);
                property x -> str;
            };
        };
        """
