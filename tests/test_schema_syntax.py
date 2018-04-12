##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import unittest  # NoQA

from edgedb.lang import _testbase as tb
from edgedb.lang.schema import generate_source as eschema_to_source, error
from edgedb.lang.schema.parser import parser as eschema_parser


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
            required link body -> str:
                constraint maxlength (10000)
        """

    def test_eschema_syntax_type_04(self):
        """
type LogEntry extending OwnedObject, Text:
    required link spent_time -> int
        """

    def test_eschema_syntax_type_05(self):
        """
type LogEntry extending OwnedObject, Text:
   link start_date := SELECT datetime::current_datetime()
        """

    def test_eschema_syntax_type_06(self):
        """
type LogEntry extending OwnedObject, Text:
    link start_date -> datetime:
       default :=
            SELECT datetime::current_datetime()
       title := 'Start Date'
        """

    def test_eschema_syntax_type_07(self):
        """
type Issue extending `foo.bar`::NamedObject, OwnedObject, Text:

    required link number -> issue_num_t:
        readonly := true

    required link status -> Status

    link priority -> Priority

    link watchers -> User:
        cardinality := '**'

    link time_estimate -> int

    link time_spent_log -> LogEntry:
        cardinality := '1*'

    link start_date := SELECT datetime::current_datetime()

    link start_date -> datetime:
       default :=
            SELECT datetime::current_datetime()
       title := 'Start Date'

    link due_date -> datetime

    link related_to -> Issue:
        cardinality := '**'
        """

    def test_eschema_syntax_type_08(self):
        """
type Foo:
    link time_estimate -> int:
       link property unit:
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

    @tb.must_fail(error.SchemaSyntaxError,
                  "Unexpected token.*COMMIT",
                  line=2, col=6)
    def test_eschema_syntax_type_11(self):
        """
type Commit:
    required link name -> std::str
        """

    def test_eschema_syntax_link_target_type_01(self):
        """
type User:
    required link todo -> array<str>
        """

    def test_eschema_syntax_link_target_type_02(self):
        """
type User:
    required link dict -> map<str, str>
        """

    def test_eschema_syntax_link_target_type_03(self):
        """
type User:
    required link todo -> tuple<str, int, float>
        """

    def test_eschema_syntax_link_target_type_04(self):
        """
type User:
    required link todo -> tuple<str, map<str, array<str>>, array<float>>
        """

    def test_eschema_syntax_index_01(self):
        """
type LogEntry extending OwnedObject, Text:
    required link owner -> User
    index test_index on (SELECT datetime::current_datetime())
        """

    def test_eschema_syntax_index_02(self):
        """
link foobar:
    link property foo:
        title := 'Sample property'

    index prop on (self@foo)
        """

    @tb.must_fail(error.SchemaSyntaxError,
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

  link start_date -> datetime:


                       default :=


                        SELECT

                            datetime::current_datetime()



                       title := 'Start Date'
        """

    def test_eschema_syntax_ws_02(self):
        """
        type LogEntry extending OwnedObject, Text:
            link start_date -> datetime:
               default :=
                    SELECT datetime::current_datetime()
               title := 'Start Date'
        """

    def test_eschema_syntax_ws_03(self):
        """     type LogEntry extending OwnedObject, Text:
                    link start_date -> datetime:
                       default :=
                            SELECT datetime::current_datetime()
                       title := 'Start Date'
        """

    def test_eschema_syntax_ws_04(self):
        """
        type LogEntry extending (
                OwnedObject,
                Text):
            link start_date -> datetime:
               default :=
                    SELECT datetime::current_datetime()
               title := 'Start Date'

% OK %

        type LogEntry extending OwnedObject, Text:
            link start_date -> datetime:
               default :=
                    SELECT datetime::current_datetime()
               title := 'Start Date'
        """

    def test_eschema_syntax_ws_05(self):
        """
        type LogEntry extending (
                OwnedObject,
                Text):
            link start_date -> datetime:
               default :=
                    SELECT
                    datetime::current_datetime()
               title := 'Start Date'
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unknown token.*\\",
                  line=5, col=28)
    def test_eschema_syntax_ws_06(self):
        r"""
        type LogEntry extending OwnedObject, Text:
            link start_date -> datetime:
               default :=
                    SELECT \
                    datetime::current_datetime()
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

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unexpected token.*INDENT", line=7, col=13)
    def test_eschema_syntax_ws_09(self):
        # There is no ":" at the end of the type declaration, so
        # the next declaration is supposed to be on the same level of
        # indentation.
        """
        type User extending std::Named
        # NamedObject is a standard abstract base class,
        # that provides a name link.

            # A few more optional links:
            link first_name -> str
            link last_name -> str
            link email -> str
        """

    def test_eschema_syntax_scalar_01(self):
        """
scalar type issue_num_t extending builtins::sequence
        """

    def test_eschema_syntax_scalar_02(self):
        """
scalar type issue_num_t extending int:
    default := 42
        """

    def test_eschema_syntax_scalar_03(self):
        r"""
scalar type basic extending int:
    title := 'Basic ScalarType'
    default := 2
    delegated constraint min(0)
    constraint max(123456)
    constraint must_be_even
        """

    def test_eschema_syntax_scalar_04(self):
        """
scalar type basic extending int:

    title := 'Basic ScalarType'
    default := 2

    constraint min(0)
    constraint max(123456)
    delegated constraint expr on (__subject__ % 2 = 0)
        """

    def test_eschema_syntax_scalar_05(self):
        """
scalar type basic extending int:

    title := 'Basic ScalarType'
    default := 2

    constraint expr:
        prop :=
            __subject__ % 2 = 0
    constraint min(0)
    constraint max(123456)
        """

    def test_eschema_syntax_scalar_06(self):
        """
scalar type basic extending int:

    title := 'Basic ScalarType'
    default := 2

    constraint min(0)
    constraint max(123456)
    constraint expr:
        abc := __subject__ % 2 = 0


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
    title := 'Basic ScalarType'
    default := 2
    constraint special_constraint
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unexpected token.*:=",
                  line=3, col=35)
    def test_eschema_syntax_scalar_09(self):
        """
scalar type special extending int:
    constraint special_constraint := [42, 100, 9001]
        """

    def test_eschema_syntax_scalar_10(self):
        """
scalar type special extending int:
    title := 'Special ScalarType'
    constraint special_constraint:
        expr := __subject__ % 2 = 0
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
constraint max($param:any) on (()):
    expr := __subject__ <= $param
    errmessage := 'Maximum allowed value for {subject} is {$param}.'

        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"only specialized constraints can be delegated",
                  line=2, col=1)
    def test_eschema_syntax_constraint_02(self):
        """
delegated constraint length:
    subject := str::len(<str>__subject__)
        """

    def test_eschema_syntax_constraint_03(self):
        """
constraint maxlength($param:any) extending max, length:
    errmessage := '{subject} must be no longer than {$param} characters.'
        """

    def test_eschema_syntax_constraint_04(self):
        """
constraint max($param:any):
    expr := __subject__ <= $param
    errmessage := 'Maximum allowed value for {subject} is {$param}.'

abstract constraint length:
    subject := str::len(<str>__subject__)

constraint maxlength($param:any) extending max, length:
    errmessage := '{subject} must be no longer than {$param} characters.'
        """

    def test_eschema_syntax_constraint_05(self):
        """
constraint distance:
    subject :=
        <float>__subject__

constraint maxldistance extending max, distance:
    errmessage := '{subject} must be no longer than {$param} meters.'
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"missing type declaration.*\$param",
                  line=2, col=22)
    def test_eschema_syntax_constraint_06(self):
        """
constraint maxlength($param) extending max, length
        """

    @tb.must_fail(error.SchemaSyntaxError,
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
constraint foo($param:Foo) on (len(__subject__.bar)) extending max:
    errmessage := 'bar must be no more than {$param}.'
        """

    def test_eschema_syntax_linkproperty_01(self):
        """
link property foo:
    title := 'Sample property'
        """

    def test_eschema_syntax_linkproperty_02(self):
        """
link property bar extending foo
        """

    def test_eschema_syntax_linkproperty_03(self):
        """
link property bar extending foo:
    title := 'Another property'
        """

    def test_eschema_syntax_linkproperty_04(self):
        """
link property foo:
    title := 'Sample property'

link property bar extending foo:
    title := 'Another property'
        """

    def test_eschema_syntax_action_01(self):
        """
action ignore
        """

    def test_eschema_syntax_action_02(self):
        """
action ignore:
    title := 'Deleted'
        """

    def test_eschema_syntax_event_01(self):
        """
event self_deleted
        """

    def test_eschema_syntax_event_02(self):
        """
event self_deleted:
    title := 'Deleted'
        """

    def test_eschema_syntax_link_01(self):
        """
link coollink
        """

    def test_eschema_syntax_link_02(self):
        """
link coollink extending boringlink
        """

    def test_eschema_syntax_link_03(self):
        """
link coollink:
    link property foo -> int
        """

    def test_eschema_syntax_link_04(self):
        """
link coollink:
    link property foo -> int
    link property bar -> int

    constraint expr:
        expr := self.foo = self.bar
        """

    def test_eschema_syntax_link_05(self):
        """
link property foo:
    title := 'Sample property'

link property bar extending foo:
    title := 'Another property'

link coollink:
    link property foo -> int:
        default := 2
        constraint min(0)
        constraint max(123456)
        constraint expr on (__subject__ % 2 = 0):
            title := 'aaa'

    link property bar -> int

    constraint expr on (self.foo = self.bar)

    on self_deleted ignore

action ignore:
    title := 'Deleted'

event self_deleted:
    title := 'Deleted'

        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token.*LINKPROPERTY', line=3, col=22)
    def test_eschema_syntax_link_06(self):
        """
        link coollink:
            required link property foo -> int
        """

    def test_eschema_syntax_link_07(self):
        """
        link time_estimate:
           link property unit -> str:
               constraint my_constraint(0)
        """

    def test_eschema_syntax_link_08(self):
        """
        link time_estimate:
           link property unit -> str:
               constraint my_constraint(0, <str>(42^2))
        """

    def test_eschema_syntax_link_09(self):
        """
        link time_estimate:
           link property unit -> str:
               constraint my_constraint(')', `)`($$)$$))

% OK %

        link time_estimate:
           link property unit -> str:
               constraint my_constraint(')', `)`(')'))
        """

    def test_eschema_syntax_link_10(self):
        """
abstract link coollink
        """

    def test_eschema_syntax_import_01(self):
        """
        import foo

        type Bar extending foo::Foo:
            link text -> str
        """

    def test_eschema_syntax_import_02(self):
        """
        import mylib.util.foo

        type Bar extending `mylib.util.foo`::Foo:
            link text -> str
        """

    def test_eschema_syntax_import_03(self):
        """
        import foo as bar

        type Bar extending bar::Foo:
            link text -> str
        """

    def test_eschema_syntax_import_04(self):
        """
        import mylib.util.foo as bar

        type Bar extending bar::Foo:
            link text -> str
        """

    def test_eschema_syntax_import_05(self):
        """
        import mylib.util.foo as foo, mylib.special.foo as sfoo
        import (
            # ignore indentation because of parentheses
    otherlib.bar as bar,
    otherlib.ham as spam)

        type Bar extending foo::Foo, sfoo::Foo, bar::Bar, spam::Ham:
            link text -> str
        """

    def test_eschema_syntax_import_06(self):
        """
        import action.event.foo

        type Bar extending `action.event.foo`::Foo:
            link text -> str
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token.*DOT', line=4, col=33)
    def test_eschema_syntax_import_07(self):
        """
        import mylib.util.foo

        type Bar extending mylib.util.foo::Foo:
            link text -> str
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token.*DOT', line=4, col=34)
    def test_eschema_syntax_import_08(self):
        """
        import action.event.foo

        type Bar extending action.event.foo::Foo:
            link text -> str
        """

    def test_eschema_syntax_function_01(self):
        """
        function len() -> std::int:
            from sql function: length
        """

    def test_eschema_syntax_function_02(self):
        r"""
        function some_func($foo: std::int = 42) -> std::str:
            from sql := "SELECT 'life';"

% OK %

        function some_func($foo: std::int = 42) -> std::str:
            from sql :=
                'SELECT \'life\';'
        """

    def test_eschema_syntax_function_03(self):
        r"""
        function some_func($foo: std::int = 42) -> std::str:
            from edgeql :>
                SELECT 'life';

% OK %
        function some_func($foo: std::int = 42) -> std::str:
            from edgeql :=
                'SELECT \'life\';'
        """

    def test_eschema_syntax_function_04(self):
        """
        # the line continuation is just to allow long single line
        function myfunc($arg1: str, $arg2: str = 'DEFAULT',
                        $arg3: variadic std::int) -> \
                        set of int:
            volatile := true
            description :>
                myfunc sample
            from sql :>
                SELECT blarg;

% OK %
        function myfunc($arg1: str, $arg2: str = 'DEFAULT',
                        $arg3: variadic std::int) -> \
                        set of int:
            volatile := true
            description :=
                'myfunc sample'
            from sql :=
                'SELECT blarg;'
        """

    def test_eschema_syntax_function_05(self):
        """
        function myfunc($arg1: str,
                        $arg2: str = 'DEFAULT',
                        $arg3: variadic std::int) -> set of int:
            from edgeql :=
                SELECT blarg
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  "unexpected 'initial value' in function definition",
                  line=3, col=27)
    def test_eschema_syntax_function_06(self):
        """
        function some_func($foo: std::int = 42) -> std::str:
            initial value := 'bad'
            from edgeql :=
                SELECT 'life'
        """

    def test_eschema_syntax_function_07(self):
        """
        function some_func($foo: std::int = bar(42)) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_08(self):
        """
        function some_func($foo: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_09(self):
        """
        function some_func($foo: str = $$)$$) -> std::str:
            from edgeql function: some_other_func

% OK %

        function some_func($foo: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_10(self):
        """
        function some_func($foo: str = $a1$)$a1$) -> std::str:
            from edgeql function: some_other_func

% OK %

        function some_func($foo: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_11(self):
        """
        function some_func($`(`: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unexpected token.*RPAREN",
                  line=2, col=42)
    def test_eschema_syntax_function_12(self):
        """
        function some_func($`(`: str = ) ) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_13(self):
        r"""
        function some_func($`(`:
                str = ')',
                $bar: int = bar()) -> std::str:
            from edgeql function: some_other_func
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unexpected token.*RPAREN",
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
        function foo() -> map<
                    str,
                    array<tuple<int, str>>
                >:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function_16(self):
        """
        function foo() -> map<
                    str,
                    array<tuple<int, `Foo:>`>>
                >:
            from edgeql function: some_other_func
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unexpected token.*NL",
                  line=4, col=44)
    def test_eschema_syntax_function_17(self):
        """
        function foo() -> map<
                    str,
                    array<tuple<int, Foo>>>
                >:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_aggregate_01(self):
        """
        aggregate len() -> std::int:
            initial value := 0
            from sql function: length
        """

    def test_eschema_syntax_aggregate_02(self):
        r"""
        aggregate some_func($foo: std::int = 42) -> std::str:
            initial value := 'start'
            from sql :>
                SELECT 'life';

% OK %

        aggregate some_func($foo: std::int = 42) -> std::str:
            initial value := 'start'
            from sql :=
                'SELECT \'life\'';
        """

    def test_eschema_syntax_aggregate_03(self):
        """
        aggregate some_func($foo: std::int = 42) -> std::str:
            initial value := ''
            from edgeql :=
                SELECT 'life'
        """

    def test_eschema_syntax_aggregate_04(self):
        """
        aggregate myfunc($arg1: str, $arg2: str = 'DEFAULT',
                         $arg3: variadic std::int) -> int:
            initial value := 42
            volatile := true
            description := 'myfunc sample'
            from sql :=
                'SELECT blarg;'
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  "missing 'initial value' in aggregate definition",
                  line=2, col=9)
    def test_eschema_syntax_aggregate_05(self):
        """
        aggregate len() -> std::int:
            from sql function: length
        """

    @tb.must_fail(error.SchemaSyntaxError, r"Unexpected token:.*RPAREN",
                  line=3, col=41)
    def test_eschema_syntax_aggregate_06(self):
        """
        aggregate myfunc($arg1: str, $arg2: str = 'DEFAULT',
                         $arg3: variadic) -> int:
            initial value := 42
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

    def test_eschema_syntax_attribute_01(self):
        """
        attribute foobar std::str
        """

    def test_eschema_syntax_attribute_02(self):
        """
        attribute foobar test::mystr extending baz
        """

    def test_eschema_syntax_attribute_03(self):
        """
        attribute foobar extending baz
        """

    def test_eschema_syntax_attribute_04(self):
        """
        attribute foobar std::str:
            title := 'Some title'
        """

    def test_eschema_syntax_attribute_05(self):
        """
        attribute foobar test::mystr extending baz:
            title := 'Some title'
        """

    def test_eschema_syntax_attribute_06(self):
        """
        attribute foobar extending baz:
            title := 'Some title'
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token.*DOUBLECOLON', line=2, col=23)
    def test_eschema_syntax_attribute_07(self):
        """
        attribute test::foobar as std::str
        """

    def test_eschema_syntax_attribute_08(self):
        """
        attribute foobar extending (foo1, foo2)
        """

    def test_eschema_syntax_attribute_09(self):
        """
        attribute foobar extending (foo1,
    foo2)
        """

    def test_eschema_syntax_attribute_10(self):
        """
        attribute foobar extending (foo1,
    foo2):
            title := 'Title'
        """

    def test_eschema_syntax_attribute_11(self):
        """
        attribute as as extending foo
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token:.*EXTENDING', line=2, col=32)
    def test_eschema_syntax_attribute_12(self):
        """
        attribute as extending extending foo
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token:.*EXTENDING', line=2, col=35)
    def test_eschema_syntax_attribute_13(self):
        """
        attribute as as extending extending
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token:.*NL', line=2, col=25)
    def test_eschema_syntax_attribute_14(self):
        """
        attribute foobar
        """
