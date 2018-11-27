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
import unittest  # NoQA

from edb import errors

from edb.lang import _testbase as tb
from edb.lang.schema import generate_source as eschema_to_source
from edb.lang.schema.parser import parser as eschema_parser


class SchemaSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'[\s\'"()]+|(#.*?(\n|$))|SELECT')
    parser_debug_flag = 'DEBUG_ESCHEMA'
    markup_dump_lexer = 'edgeschema'
    ast_to_source = eschema_to_source

    def get_parser(self, *, spec):
        return eschema_parser.EdgeSchemaParser()


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
\tabstract type Foo:
\t\trequired property foo -> str

\tabstract type Bar:
\t\trequired property bar -> str
        """

    def test_eschema_syntax_tabs_02(self):
        """
\t  abstract type Foo:
\t      required property foo -> str

\t  abstract type Bar:
\t      required property bar -> str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Tabs used after spaces", line=2)
    def test_eschema_syntax_tabs_03(self):
        """
        \tabstract type Foo:
        \t\trequired property foo -> str

        \tabstract type Bar:
        \t\trequired property bar -> str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Inconsistent indentation", line=3)
    def test_eschema_syntax_tabs_04(self):
        """
\t\t    abstract type Foo:
\t\t\t    required property foo -> str

\t\t    abstract type Bar:
\t\t\t    required property bar -> str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Inconsistent indentation", line=5)
    def test_eschema_syntax_tabs_05(self):
        """
\t\t  abstract type Foo:
\t\t      required property foo -> str

\t      abstract type Bar:
\t          required property bar -> str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Unexpected indentation level decrease", line=6)
    def test_eschema_syntax_tabs_06(self):
        """
\t\tabstract type Foo:
\t\t\t\trequired property foo -> str

\t\tabstract type Bar:
\trequired property foo -> str
        """

    def test_eschema_syntax_tabs_07(self):
        """
type LogEntry extending OwnedObject, Text:
\tproperty start_date -> datetime:
\t\tdefault :=
\t\t\tSELECT datetime::datetime_current()
\t\ttitle := 'Start Date'
        """

    def test_eschema_syntax_tabs_08(self):
        """
type LogEntry extending OwnedObject, Text:
\tproperty start_date -> datetime:
\t\tdefault :=
\t\t\tSELECT
\t\t\t      datetime::datetime_current()
\t\ttitle := 'Start Date'
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Inconsistent indentation", line=6)
    def test_eschema_syntax_tabs_09(self):
        """
type LogEntry extending OwnedObject, Text:
\tproperty start_date -> datetime:
\t\tdefault :=
\t\t\tSELECT
\t\t         datetime::datetime_current()
\t\ttitle := 'Start Date'
        """

    def test_eschema_syntax_type_01(self):
        """type User extending builtins::NamedObject"""

    def test_eschema_syntax_type_02(self):
        """
abstract type OwnedObject:
    required link owner -> User
        """

    def test_eschema_syntax_type_03(self):
        """
        abstract type Text:
            required property body -> str:
                constraint maxlength (10000)
        """

    def test_eschema_syntax_type_04(self):
        """
type LogEntry extending OwnedObject, Text:
    required property spent_time -> int64
        """

    def test_eschema_syntax_type_05(self):
        """
type LogEntry extending OwnedObject, Text:
   link start_date := SELECT datetime::datetime_current()
        """

    def test_eschema_syntax_type_06(self):
        """
type LogEntry extending OwnedObject, Text:
    property start_date -> datetime:
       default :=
            SELECT datetime::datetime_current()
       title := 'Start Date'
        """

    def test_eschema_syntax_type_07(self):
        """
type Issue extending `foo.bar`::NamedObject, OwnedObject, Text:

    required link number -> issue_num_t:
        readonly := true

    required link status -> Status

    link priority -> Priority

    multi link watchers -> User

    multi link time_spent_log -> LogEntry

    link start_date := SELECT datetime::datetime_current()

    multi link related_to -> Issue

    property time_estimate -> int64

    property start_date -> datetime:
       default :=
            SELECT datetime::datetime_current()
       title := 'Start Date'

    property due_date -> datetime
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "illegal definition", line=4, col=9)
    def test_eschema_syntax_type_08(self):
        """
type Foo:
    property time_estimate -> int64:
        property unit:
            default := 'minute'
       """

    def test_eschema_syntax_type_09(self):
        """
type LogEntry extending OwnedObject, Text:
    required link attachment -> Post, File, User
        """

    def test_eschema_syntax_type_10(self):
        """
type `Log-Entry` extending `OwnedObject`, `Text`:
    required link attachment -> `Post`, `File`, `User`

% OK %

type `Log-Entry` extending OwnedObject, Text:
    required link attachment -> Post, File, User
        """

    @tb.must_fail(errors.SchemaSyntaxError, "Unexpected 'Commit'",
                  line=2, col=6)
    def test_eschema_syntax_type_11(self):
        """
type Commit:
    required property name -> std::str
        """

    @tb.must_fail(errors.SchemaSyntaxError, line=2, col=14)
    def test_eschema_syntax_type_12(self):
        """
        type __Foo__:
            required property name -> std::str
        """

    @tb.must_fail(errors.SchemaSyntaxError, line=2, col=14)
    def test_eschema_syntax_type_13(self):
        """
        type `__Foo__`:
            required property name -> std::str
        """

    @tb.must_fail(errors.SchemaSyntaxError, line=3, col=31)
    def test_eschema_syntax_type_14(self):
        """
        type __Foo:
            required property __name__ -> std::str
        """

    @tb.must_fail(errors.SchemaSyntaxError, line=3, col=31)
    def test_eschema_syntax_type_15(self):
        """
        type `__Foo`:
            required property `__name__` -> std::str
        """

    def test_eschema_syntax_type_16(self):
        """
        type Пример:
            required property номер -> int16
        """

    def test_eschema_syntax_type_17(self):
        """
        type Foo:
            link bar0 -> Bar:
                on target delete restrict

            link bar1 -> Bar:
                on target delete delete source

            link bar2 -> Bar:
                on target delete set empty

            link bar3 -> Bar:
                on target delete set default

            link bar4 -> Bar:
                on target delete deferred restrict
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "more than one 'on target delete' specification",
                  line=5, col=17)
    def test_eschema_syntax_type_18(self):
        """
        type Foo:
            link bar0 -> Bar:
                on target delete restrict
                on target delete delete source
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Unexpected end of line", line=4, col=38)
    def test_eschema_syntax_type_19(self):
        # testing bugs due to incorrect indentation
        """
        type Foo:
            property foo -> str:
                default := some_func(
    1, 2, 3)
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Unterminated string '", line=4, col=38)
    def test_eschema_syntax_type_20(self):
        # testing bugs due to incorrect indentation
        """
        type Foo:
            property foo -> str:
                default := some_func('
                1, 2, 3')
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unterminated string \$\$some_func\(", line=5, col=21)
    def test_eschema_syntax_type_21(self):
        # testing bugs due to incorrect indentation
        """
        type Foo:
            property foo -> str:
                default :=
                    $$some_func(
    1, 2, 3)$$
        """

    def test_eschema_syntax_type_22(self):
        """
        type Foo:
            property foo -> str:
                # if it's defined on the same line as :=
                # the definition must be a one-liner
                default := some_func(1, 2, 3)

            property bar -> str:
                # multi-line definition with correct indentation
                default :=
                    some_func('
                    1, 2, 3')

            property baz -> str:
                # multi-line definition with correct indentation
                default :=
                    $$some_func(
                    1, 2, 3)$$
        """

    def test_eschema_syntax_type_23(self):
        """
        type Foo:
            single link foo -> Foo
            multi link bar -> Bar
            required single link baz -> Baz
            required multi link spam -> Spam
            inherited required single link ham -> Ham
            inherited required multi link eggs -> Egg
        """

    def test_eschema_syntax_type_24(self):
        """
        type Foo:
            single property foo -> str
            multi property bar -> str
            required single property baz -> str
            required multi property spam -> str
            inherited required single property ham -> str
            inherited required multi property eggs -> str
        """

    def test_eschema_syntax_link_target_type_01(self):
        """
type User:
    required link todo -> array<str>
        """

    def test_eschema_syntax_link_target_type_03(self):
        """
type User:
    required link todo -> tuple<str, int64, float64>
        """

    def test_eschema_syntax_link_target_type_04(self):
        """
type User:
    required link todo -> tuple<str, tuple<str, array<str>>, array<float64>>
        """

    def test_eschema_syntax_index_01(self):
        """
type LogEntry extending OwnedObject, Text:
    required link owner -> User
    index test_index on (SELECT datetime::datetime_current())
        """

    def test_eschema_syntax_index_02(self):
        """
abstract link foobar:
    property foo:
        title := 'Sample property'

    index prop on (self@foo)
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r'illegal definition', line=3, col=5)
    def test_eschema_syntax_index_03(self):
        """
scalar type foobar:
    index prop on (self)
        """

    def test_eschema_syntax_ws_01(self):
        """
type LogEntry extending    OwnedObject,    Text:

    # irrelevant comment indent
            # irrelevant comment indent
        # irrelevant comment indent

  property start_date -> datetime:


                       default :=


                        SELECT

                            datetime::datetime_current()



                       title := 'Start Date'
        """

    def test_eschema_syntax_ws_02(self):
        """
        type LogEntry extending OwnedObject, Text:
            property start_date -> datetime:
               default :=
                    SELECT datetime::datetime_current()
               title := 'Start Date'
        """

    def test_eschema_syntax_ws_03(self):
        """     type LogEntry extending OwnedObject, Text:
                    property start_date -> datetime:
                       default :=
                            SELECT datetime::datetime_current()
                       title := 'Start Date'
        """

    def test_eschema_syntax_ws_04(self):
        """
        type LogEntry extending (
                OwnedObject,
                Text):
            property start_date -> datetime:
               default :=
                    SELECT datetime::datetime_current()
               title := 'Start Date'

% OK %

        type LogEntry extending OwnedObject, Text:
            property start_date -> datetime:
               default :=
                    SELECT datetime::datetime_current()
               title := 'Start Date'
        """

    def test_eschema_syntax_ws_05(self):
        """
        type LogEntry extending (
                OwnedObject,
                Text):
            property start_date -> datetime:
               default :=
                    SELECT
                    datetime::datetime_current()
               title := 'Start Date'
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected '\\'",
                  line=5, col=28)
    def test_eschema_syntax_ws_06(self):
        r"""
        type LogEntry extending OwnedObject, Text:
            property start_date -> datetime:
               default :=
                    SELECT \
                    datetime::datetime_current()
               title := 'Start Date'
        """

    def test_eschema_syntax_ws_07(self):
        """
        scalar type newScalarType extending str#:
        """

    def test_eschema_syntax_ws_08(self):
        """
        scalar type newScalarType0 extending str#:

        scalar type newScalarType1 extending str#:
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected indentation level increase", line=7, col=13)
    def test_eschema_syntax_ws_09(self):
        # There is no ":" at the end of the type declaration, so
        # the next declaration is supposed to be on the same level of
        # indentation.
        """
        type User extending std::Named
        # NamedObject is a standard abstract base class,
        # that provides a name link.

            # A few more optional links:
            property first_name -> str
            property last_name -> str
            property email -> str
        """

    def test_eschema_syntax_ws_10(self):
        """
                # this comment must not affect indentation

abstract link friends:
    property nickname -> str
    # this comment must not affect indentation

type Foo:
    required property foo -> str
    # this comment must not affect indentation
type Bar:
    required property bar -> str
  # this comment must not affect indentation

type Baz:
    required property baz -> str
        # this comment must not affect indentation

type Boo:
    required property boo -> str
        """

    def test_eschema_syntax_scalar_01(self):
        """
scalar type issue_num_t extending std::sequence
        """

    def test_eschema_syntax_scalar_02(self):
        """
scalar type issue_num_t extending int:
    default := 42
        """

    def test_eschema_syntax_scalar_03(self):
        r"""
scalar type basic extending int:
    delegated constraint min(0)
    constraint max(123456)
    constraint must_be_even

    title := 'Basic ScalarType'
    default := 2
        """

    def test_eschema_syntax_scalar_04(self):
        """
scalar type basic extending int:
    constraint min(0)
    constraint max(123456)
    delegated constraint expr on (__subject__ % 2 = 0)

    title := 'Basic ScalarType'
    default := 2
        """

    def test_eschema_syntax_scalar_05(self):
        """
scalar type basic extending int:
    constraint expr:
        prop :=
            __subject__ % 2 = 0
    constraint min(0)
    constraint max(123456)

    title := 'Basic ScalarType'
    default := 2
        """

    def test_eschema_syntax_scalar_06(self):
        """
scalar type basic extending int:
    constraint min(0)
    constraint max(123456)
    constraint expr:
        abc := __subject__ % 2 = 0

    title := 'Basic ScalarType'
    default := 2


scalar type inherits_default extending basic

abstract scalar type abstract_scalar extending int
        """

    def test_eschema_syntax_scalar_07(self):
        """
final scalar type none
        """

    def test_eschema_syntax_scalar_08(self):
        """
scalar type basic extending int:
    constraint special_constraint
    title := 'Basic ScalarType'
    default := 2
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected ':='",
                  line=3, col=35)
    def test_eschema_syntax_scalar_09(self):
        """
scalar type special extending int:
    constraint special_constraint := [42, 100, 9001]
        """

    def test_eschema_syntax_scalar_10(self):
        """
scalar type special extending int:
    constraint special_constraint:
        expr := __subject__ % 2 = 0
    title := 'Special ScalarType'
        """

    def test_eschema_syntax_scalar_11(self):
        """
scalar type constraint_length extending str:
     constraint maxlength(16+1, len(([1])))
        """

    def test_eschema_syntax_scalar_12(self):
        """
scalar type constraint_length extending str:
     constraint maxlength((16+(4*2))/((4)-1), len(([1])))
        """

    def test_eschema_syntax_constraint_01(self):
        """
# Test empty tuple as subject expression
abstract constraint max(param:anytype) on (()):
    expr := __subject__ <= $param
    errmessage := 'Maximum allowed value for {subject} is {$param}.'

        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"only concrete constraints can be delegated",
                  line=2, col=1)
    def test_eschema_syntax_constraint_02(self):
        """
delegated constraint length:
    subject := str::len(<str>__subject__)
        """

    def test_eschema_syntax_constraint_03(self):
        """
abstract constraint maxlength(param:anytype) extending max, length:
    errmessage := '{subject} must be no longer than {$param} characters.'
        """

    def test_eschema_syntax_constraint_04(self):
        """
abstract constraint max(param:anytype):
    expr := __subject__ <= $param
    errmessage := 'Maximum allowed value for {subject} is {$param}.'

abstract constraint length:
    subject := str::len(<str>__subject__)

abstract constraint maxlength(param:anytype) extending max, length:
    errmessage := '{subject} must be no longer than {$param} characters.'
        """

    def test_eschema_syntax_constraint_05(self):
        """
abstract constraint distance:
    subject :=
        <float64>__subject__

abstract constraint maxldistance extending max, distance:
    errmessage := '{subject} must be no longer than {$param} meters.'
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"missing type declaration.*`param`",
                  line=2, col=31)
    def test_eschema_syntax_constraint_06(self):
        """
abstract constraint maxlength(param) extending max, length
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"only top-level constraints declarations can be abstract",
                  line=3, col=5)
    def test_eschema_syntax_constraint_07(self):
        """
scalar type special extending int:
    abstract constraint length:
        subject := str::len(<str>__subject__)
        """

    def test_eschema_syntax_constraint_08(self):
        """
abstract constraint foo(param:Foo) on (len(__subject__.bar)) extending max:
    errmessage := 'bar must be no more than {$param}.'
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected 'constraint'",
                  line=2, col=1)
    def test_eschema_syntax_constraint_09(self):
        """
constraint foo
        """

    def test_eschema_syntax_constraint_10(self):
        """
type Foo:
    constraint maxldistance:
        errmessage := '{subject} must be no longer than {$param} meters.'

    constraint maxlength(4)
        """

    def test_eschema_syntax_property_01(self):
        """
abstract property foo:
    title := 'Sample property'
        """

    def test_eschema_syntax_property_02(self):
        """
abstract property bar extending foo
        """

    def test_eschema_syntax_property_03(self):
        """
abstract property bar extending foo:
    title := 'Another property'
        """

    def test_eschema_syntax_property_04(self):
        """
abstract property foo:
    title := 'Sample property'

abstract property bar extending foo:
    title := 'Another property'
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected 'property'",
                  line=2, col=1)
    def test_eschema_syntax_property_05(self):
        """
property foo
        """

    def test_eschema_syntax_link_01(self):
        """
abstract link coollink
        """

    def test_eschema_syntax_link_02(self):
        """
abstract link coollink extending boringlink
        """

    def test_eschema_syntax_link_03(self):
        """
abstract link coollink:
    property foo -> int64
        """

    def test_eschema_syntax_link_04(self):
        """
abstract link coollink:
    property foo -> int64
    property bar -> int64

    constraint expr:
        expr := self.foo = self.bar
        """

    def test_eschema_syntax_link_05(self):
        """
abstract property foo:
    title := 'Sample property'

abstract property bar extending foo:
    title := 'Another property'

abstract link coollink:
    property foo -> int64:
        constraint min(0)
        constraint max(123456)
        constraint expr on (__subject__ % 2 = 0):
            title := 'aaa'
        default := 2

    property bar -> int64

    constraint expr on (self.foo = self.bar)

        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r'link properties cannot be "required".', line=3, col=13)
    def test_eschema_syntax_link_06(self):
        """
        abstract link coollink:
            required property foo -> int64
        """

    def test_eschema_syntax_link_07(self):
        """
        abstract link time_estimate:
           property unit -> str:
               constraint my_constraint(0)
        """

    def test_eschema_syntax_link_08(self):
        """
        abstract link time_estimate:
           property unit -> str:
               constraint my_constraint(0, <str>(42^2))
        """

    def test_eschema_syntax_link_09(self):
        """
        abstract link time_estimate:
           property unit -> str:
               constraint my_constraint(')', `)`($$)$$))

% OK %

        abstract link time_estimate:
           property unit -> str:
               constraint my_constraint(')', `)`($$)$$))
        """

    def test_eschema_syntax_link_10(self):
        """
abstract link coollink
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected 'link'",
                  line=2, col=1)
    def test_eschema_syntax_link_11(self):
        """
link foo
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected '::'",
                  line=3, col=13)
    def test_eschema_syntax_link_12(self):
        """
type Foo:
    link mod::name to std::str
        """

    def test_eschema_syntax_import_01(self):
        """
        import foo

        type Bar extending foo::Foo:
            property text -> str
        """

    def test_eschema_syntax_import_02(self):
        """
        import mylib.util.foo

        type Bar extending `mylib.util.foo`::Foo:
            property text -> str
        """

    def test_eschema_syntax_import_03(self):
        """
        import foo as bar

        type Bar extending bar::Foo:
            property text -> str
        """

    def test_eschema_syntax_import_04(self):
        """
        import mylib.util.foo as bar

        type Bar extending bar::Foo:
            property text -> str
        """

    def test_eschema_syntax_import_05(self):
        """
        import mylib.util.foo as foo, mylib.special.foo as sfoo
        import (
            # ignore indentation because of parentheses
    otherlib.bar as bar,
    otherlib.ham as spam)

        type Bar extending foo::Foo, sfoo::Foo, bar::Bar, spam::Ham:
            property text -> str
        """

    def test_eschema_syntax_import_06(self):
        """
        import action.event.foo

        type Bar extending `action.event.foo`::Foo:
            property text -> str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected '\.'", line=4, col=33)
    def test_eschema_syntax_import_07(self):
        """
        import mylib.util.foo

        type Bar extending mylib.util.foo::Foo:
            property text -> str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected '\.'", line=4, col=34)
    def test_eschema_syntax_import_08(self):
        """
        import action.event.foo

        type Bar extending action.event.foo::Foo:
            property text -> str
        """

    def test_eschema_syntax_function_01(self):
        """
        function len() -> std::int64:
            from sql function: length
        """

    def test_eschema_syntax_function_02(self):
        r"""
        function some_func(foo: std::int64 = 42) -> std::str:
            from sql := "SELECT 'life';"

% OK %

        function some_func(foo: std::int64 = 42) -> std::str:
            from sql :=
                "SELECT 'life';"
        """

    def test_eschema_syntax_function_03(self):
        r"""
        function some_func(foo: std::int64 = 42) -> std::str:
            from edgeql:=
                $$
                SELECT 'life';
                $$
        """

    def test_eschema_syntax_function_04(self):
        """
        # the line continuation is just to allow long single line
        function myfunc(arg1: str, arg2: str = 'DEFAULT',
                        variadic arg3: std::int64) -> \
                        set of int:
            volatile := true
            description :=
                'myfunc sample'
            from sql :=
                $$SELECT blarg;$$
        """

    def test_eschema_syntax_function_05(self):
        """
        function myfunc(arg1: str,
                        arg2: str = 'DEFAULT',
                        variadic arg3: std::int64,
                        named only arg4: std::int64,
                        named only arg5: std::int64) -> set of int:
            from edgeql :=
                SELECT blarg
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "unexpected 'initial_value' in function definition",
                  line=3, col=13)
    def test_eschema_syntax_function_06(self):
        """
        function some_func(foo: std::int64 = 42) -> std::str:
            initial_value := 'bad'
            from edgeql :=
                SELECT 'life'
        """

    def test_eschema_syntax_function_07(self):
        """
        function some_func(foo: std::int64 = bar(42)) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_08(self):
        """
        function some_func(foo: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_09(self):
        """
        function some_func(foo: str = $$)$$) -> std::str:
            from edgeql function: some_other_func

% OK %

        function some_func(foo: str = $$)$$) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_10(self):
        """
        function some_func(foo: str = $a1$)$a1$) -> std::str:
            from edgeql function: some_other_func

% OK %

        function some_func(foo: str = $a1$)$a1$) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_11(self):
        """
        function some_func(`(`: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected '\)'",
                  line=2, col=42)
    def test_eschema_syntax_function_12(self):
        """
        function some_func($`(`: str = ) ) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_13(self):
        r"""
        function some_func(`(`:
                str = ')',
                bar: int = bar()) -> std::str:
            from edgeql function: some_other_func
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected '\)'",
                  line=4, col=24)
    def test_eschema_syntax_function_14(self):
        r"""
        function some_func($`(`:
                str
                    = )) -> std::str:
            # XXX:     ^ - this is reported as "unexpected" as opposed to the
            #              previous parentheses, this is because schema
            #              parentheses matching is done before any analysis of
            #              what's inside
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_15(self):
        """
        function foo() -> tuple<
                    str,
                    array<tuple<int, str>>
                >:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_16(self):
        """
        function foo() -> tuple<
                    str,
                    array<tuple<int, `Foo:>`>>
                >:
            from edgeql function: some_other_func
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected end of line",
                  line=4, col=44)
    def test_eschema_syntax_function_17(self):
        """
        function foo() -> tuple<
                    str,
                    array<tuple<int, Foo>>>
                >:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_view_01(self):
        """
        view FooBaz:
            expr := SELECT Foo FILTER Foo.bar = 'baz'
            description := 'Special Foo'
        """

    def test_eschema_syntax_view_02(self):
        """
        view FooBaz:
            expr :=
                SELECT Foo
                FILTER Foo.bar = 'baz'
        """

    def test_eschema_syntax_view_03(self):
        """
        view FooBaz :=
                SELECT Foo
                FILTER Foo.bar = 'baz'

% OK %

        view FooBaz:
            expr :=
                SELECT Foo
                FILTER Foo.bar = 'baz'
        """

    def test_eschema_syntax_attribute_01(self):
        """
        abstract attribute foobar
        """

    def test_eschema_syntax_attribute_03(self):
        """
        abstract attribute foobar extending baz
        """

    def test_eschema_syntax_attribute_04(self):
        """
        abstract attribute foobar:
            title := 'Some title'
        """

    def test_eschema_syntax_attribute_06(self):
        """
        abstract attribute foobar extending baz:
            title := 'Some title'
        """

    def test_eschema_syntax_attribute_08(self):
        """
        abstract attribute foobar extending (foo1, foo2)
        """

    def test_eschema_syntax_attribute_09(self):
        """
        abstract attribute foobar extending (foo1,
    foo2)
        """

    def test_eschema_syntax_attribute_10(self):
        """
        abstract attribute foobar extending (foo1,
    foo2):
            title := 'Title'
        """

    def test_eschema_syntax_attribute_11(self):
        """
        abstract attribute as extending foo
        """

    def test_eschema_syntax_attribute_12(self):
        """
        abstract inheritable attribute foo
        """

    def test_eschema_syntax_attribute_13(self):
        """
        abstract inheritable attribute foo extending bar
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected 'extending'", line=2, col=41)
    def test_eschema_syntax_attribute_14(self):
        """
        abstract attribute as extending extending foo
        """

    @tb.must_fail(errors.SchemaSyntaxError, r"Unexpected 'attribute'",
                  line=2, col=1)
    def test_eschema_syntax_attribute_15(self):
        """
attribute foo;
        """

    def test_eschema_syntax_eol_01(self):
        r"""
        abstract type \
              OwnedObject:
            required link owner -> User

        abstract type \
          OwnedObject:
            required link owner -> User

        abstract type \
  OwnedObject:
            required link owner -> User

% OK %

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User
        """

    def test_eschema_syntax_eol_02(self):
        r"""
        abstract type OwnedObject:
            required \
            link owner -> User

        abstract type OwnedObject:
            required \
                link owner -> User

        abstract type OwnedObject:
            required \
        link owner -> User

% OK %

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User
        """

    def test_eschema_syntax_eol_03(self):
        r"""
        abstract type OwnedObject:
            required link owner \
            -> User

        abstract type OwnedObject:
            required link owner \
                -> User

        abstract type OwnedObject:
            required link owner \
    -> User

% OK %

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User
        """

    def test_eschema_syntax_eol_04(self):
        r"""
        abstract type OwnedObject:
            required link owner -> \
            User

        abstract type OwnedObject:
            required link owner -> \
                User

        abstract type OwnedObject:
            required link owner -> \
        User

% OK %

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User

        abstract type OwnedObject:
            required link owner -> User
        """

    def test_eschema_syntax_eol_05(self):
        r"""
        type OwnedObject extending \
        MyObject

        type OwnedObject extending \
            MyObject

        type OwnedObject extending \
    MyObject

% OK %

        type OwnedObject extending MyObject

        type OwnedObject extending MyObject

        type OwnedObject extending MyObject
        """

    def test_eschema_syntax_eol_06(self):
        r"""
        type OwnedObject extending \
        MyObject:
            required link owner -> User

        type OwnedObject extending \
            MyObject:
            required link owner -> User

        type OwnedObject extending \
    MyObject:
            required link owner -> User

% OK %

        type OwnedObject extending MyObject:
            required link owner -> User

        type OwnedObject extending MyObject:
            required link owner -> User

        type OwnedObject extending MyObject:
            required link owner -> User
        """

    def test_eschema_syntax_eol_07(self):
        r"""
        type User:
            required link todo -> array\
                <\
          str\
          >

% OK %

        type User:
            required link todo -> array<str>
        """

    def test_eschema_syntax_eol_08(self):
        r"""
        abstract attribute \
        foobar

        abstract attribute foobar\
         extending baz

        abstract attribute \
    foobar

% OK %

        abstract attribute foobar

        abstract attribute foobar extending baz

        abstract attribute foobar
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected '2'",
                  line=3, col=9)
    def test_eschema_syntax_eol_09(self):
        r"""
        abstract attribute \
        2 foobar std::str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected character after line continuation character",
                  line=2, col=24)
    def test_eschema_syntax_eol_10(self):
        # this is intended to test trailing whitespace after "\"
        """
        abstract type \\       \

              OwnedObject:
            required link owner -> User
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  r"Unexpected character after line continuation character",
                  line=2, col=29)
    def test_eschema_syntax_eol_11(self):
        # this is intended to test trailing whitespace after "\"
        """
        abstract attribute \\   \
        foobar std::str
        """

    @tb.must_fail(errors.SchemaSyntaxError,
                  "Illegal line continuation", line=3)
    def test_eschema_syntax_eol_12(self):
        r"""
        abstract type Foo:
        \
        required link owner -> User
        """
