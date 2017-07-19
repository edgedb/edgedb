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
    def test_eschema_syntax_empty01(self):
        """"""

    def test_eschema_syntax_empty02(self):
        """# comment"""

    def test_eschema_syntax_empty03(self):
        """


        """

    def test_eschema_syntax_empty04(self):
        """

        # comment

        """

    def test_eschema_syntax_concept01(self):
        """concept User extending builtins::NamedObject"""

    def test_eschema_syntax_concept02(self):
        """
abstract concept OwnedObject:
    required link owner to User
        """

    def test_eschema_syntax_concept03(self):
        """
        abstract concept Text:
            required link body to str:
                constraint maxlength (10000)
        """

    def test_eschema_syntax_concept04(self):
        """
concept LogEntry extending OwnedObject, Text:
    required link spent_time to int
        """

    def test_eschema_syntax_concept05(self):
        """
concept LogEntry extending OwnedObject, Text:
   link start_date := SELECT datetime::current_datetime()
        """

    def test_eschema_syntax_concept06(self):
        """
concept LogEntry extending OwnedObject, Text:
    link start_date to datetime:
       default :=
            SELECT datetime::current_datetime()
       title: 'Start Date'
        """

    def test_eschema_syntax_concept07(self):
        """
concept Issue extending foo.bar::NamedObject, OwnedObject, Text:

    required link number to issue_num_t:
        readonly: true

    required link status to Status

    link priority to Priority

    link watchers to User:
        mapping: **

    link time_estimate to int

    link time_spent_log to LogEntry:
        mapping: 1*

    link start_date := SELECT datetime::current_datetime()

    link start_date to datetime:
       default :=
            SELECT datetime::current_datetime()
       title: 'Start Date'

    link due_date to datetime

    link related_to to Issue:
        mapping: **
        """

    def test_eschema_syntax_concept08(self):
        """
concept Foo:
    link time_estimate to int:
       linkproperty unit:
           default: 'minute'
       """

    def test_eschema_syntax_concept09(self):
        """
concept LogEntry extending OwnedObject, Text:
    required link attachment to Post, File, User
        """

    def test_eschema_syntax_concept10(self):
        """
concept `Log-Entry` extending `OwnedObject`, `Text`:
    required link attachment to `Post`, `File`, `User`

% OK %

concept `Log-Entry` extending OwnedObject, Text:
    required link attachment to Post, File, User
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  "Unexpected token.*COMMIT",
                  line=2, col=9)
    def test_eschema_syntax_concept11(self):
        """
concept Commit:
    required link name to std::str
        """

    def test_eschema_syntax_type01(self):
        """
concept User:
    required link todo to array<str>
        """

    def test_eschema_syntax_type02(self):
        """
concept User:
    required link dict to map<str, str>
        """

    def test_eschema_syntax_type03(self):
        """
concept User:
    required link todo to tuple<str, int, float>
        """

    def test_eschema_syntax_type04(self):
        """
concept User:
    required link todo to tuple<str, map<str, array<str>>, array<float>>
        """

    def test_eschema_syntax_index01(self):
        """
concept LogEntry extending OwnedObject, Text:
    required link owner to User
    index test_index := SELECT datetime::current_datetime()
        """

    def test_eschema_syntax_ws01(self):
        """
concept LogEntry extending    OwnedObject,    Text:

    # irrelevant comment indent
            # irrelevant comment indent
        # irrelevant comment indent

  link start_date to datetime:


                       default :=


                        SELECT

                            datetime::current_datetime()



                       title: 'Start Date'
        """

    def test_eschema_syntax_ws02(self):
        """
        concept LogEntry extending OwnedObject, Text:
            link start_date to datetime:
               default :=
                    SELECT datetime::current_datetime()
               title: 'Start Date'
        """

    def test_eschema_syntax_ws03(self):
        """     concept LogEntry extending OwnedObject, Text:
                    link start_date to datetime:
                       default :=
                            SELECT datetime::current_datetime()
                       title: 'Start Date'
        """

    def test_eschema_syntax_ws04(self):
        """
        concept LogEntry extending (
                OwnedObject,
                Text):
            link start_date to datetime:
               default :=
                    SELECT datetime::current_datetime()
               title: 'Start Date'

% OK %

        concept LogEntry extending OwnedObject, Text:
            link start_date to datetime:
               default :=
                    SELECT datetime::current_datetime()
               title: 'Start Date'
        """

    def test_eschema_syntax_ws05(self):
        r"""
        concept LogEntry extending (
                OwnedObject,
                Text):
            link start_date to datetime:
               default :=
                    SELECT \
                    datetime::current_datetime()
               title: 'Start Date'

% OK %

        concept LogEntry extending OwnedObject, Text:
            link start_date to datetime:
               default :=
                    SELECT datetime::current_datetime()
               title: 'Start Date'
        """

    def test_eschema_syntax_atom01(self):
        """
atom issue_num_t extending builtins::sequence
        """

    def test_eschema_syntax_atom02(self):
        """
atom issue_num_t extending int:
    default: 42
        """

    def test_eschema_syntax_atom03(self):
        """
atom basic extending int:
    title: 'Basic Atom'
    default: 2
    constraint min(0)
    constraint max(123456)
    constraint must_be_even
        """

    def test_eschema_syntax_atom04(self):
        """
atom basic extending int:

    title: 'Basic Atom'
    default: 2

    constraint min(0)
    constraint max(123456)
    constraint expr:
        subject := subject % 2 = 0
        """

    def test_eschema_syntax_atom05(self):
        """
atom basic extending int:

    title: 'Basic Atom'
    default: 2

    constraint expr:
        subject :=
            subject % 2 = 0
    constraint min(0)
    constraint max(123456)
        """

    def test_eschema_syntax_atom06(self):
        """
atom basic extending int:

    title: 'Basic Atom'
    default: 2

    constraint min(0)
    constraint max(123456)
    constraint expr:
        subject := subject % 2 = 0


atom inherits_default extending basic

abstract atom abstract_atom extending int
        """

    def test_eschema_syntax_atom07(self):
        """
final atom none
        """

    def test_eschema_syntax_atom08(self):
        """
atom basic extending int:
    title: 'Basic Atom'
    default: 2
    abstract constraint special_constraint
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unexpected token.*:=",
                  line=3, col=44)
    def test_eschema_syntax_atom09(self):
        """
atom special extending int:
    abstract constraint special_constraint := [42, 100, 9001]
        """

    def test_eschema_syntax_atom10(self):
        """
atom special extending int:
    title: 'Special Atom'
    constraint special_constraint:
        expr := subject % 2 = 0
        """

    def test_eschema_syntax_atom11(self):
        """
atom constraint_length extending str:
     constraint maxlength(16+1, len(([1])))
        """

    def test_eschema_syntax_atom12(self):
        """
atom constraint_length extending str:
     constraint maxlength((16+(4*2))/((4)-1), len(([1])))
        """

    def test_eschema_syntax_constraint01(self):
        """
constraint max($param:any):
    expr := subject <= $param
    errmessage: 'Maximum allowed value for {subject} is {param}.'

        """

    def test_eschema_syntax_constraint02(self):
        """
abstract constraint length:
    subject := str::len(<str>subject)
        """

    def test_eschema_syntax_constraint03(self):
        """
constraint maxlength($param:any) extending max, length:
    errmessage: '{$subject} must be no longer than {$param} characters.'
        """

    def test_eschema_syntax_constraint04(self):
        """
constraint max($param:any):
    expr := subject <= $param
    errmessage: 'Maximum allowed value for {subject} is {param}.'

abstract constraint length:
    subject := str::len(<str>subject)

constraint maxlength($param:any) extending max, length:
    errmessage: '{subject} must be no longer than {param} characters.'
        """

    def test_eschema_syntax_constraint05(self):
        """
abstract constraint distance:
    subject :=
        <float>subject

constraint maxldistance extending max, distance:
    errmessage: '{subject} must be no longer than {param} meters.'
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"missing type declaration.*\$param",
                  line=2, col=22)
    def test_eschema_syntax_constraint06(self):
        """
constraint maxlength($param) extending max, length
        """

    def test_eschema_syntax_linkproperty01(self):
        """
linkproperty foo:
    title: 'Sample property'
        """

    def test_eschema_syntax_linkproperty02(self):
        """
linkproperty bar extending foo
        """

    def test_eschema_syntax_linkproperty03(self):
        """
linkproperty bar extending foo:
    title: 'Another property'
        """

    def test_eschema_syntax_linkproperty04(self):
        """
linkproperty foo:
    title: 'Sample property'

linkproperty bar extending foo:
    title: 'Another property'
        """

    def test_eschema_syntax_action01(self):
        """
action ignore
        """

    def test_eschema_syntax_action02(self):
        """
action ignore:
    title: 'Deleted'
        """

    def test_eschema_syntax_event01(self):
        """
event self_deleted
        """

    def test_eschema_syntax_event02(self):
        """
event self_deleted:
    title: 'Deleted'
        """

    def test_eschema_syntax_link01(self):
        """
link coollink
        """

    def test_eschema_syntax_link02(self):
        """
link coollink extending boringlink
        """

    def test_eschema_syntax_link03(self):
        """
link coollink:
    linkproperty foo to int
        """

    def test_eschema_syntax_link04(self):
        """
link coollink:
    linkproperty foo to int
    linkproperty bar to int

    constraint expr:
        expr := self.foo = self.bar
        """

    def test_eschema_syntax_link05(self):
        """
linkproperty foo:
    title: 'Sample property'

linkproperty bar extending foo:
    title: 'Another property'

link coollink:
    linkproperty foo to int:
        default: 2
        constraint min(0)
        constraint max(123456)
        constraint expr:
            subject := subject % 2 = 0

    linkproperty bar to int

    constraint expr:
        subject := self.foo = self.bar

    on self_deleted ignore

action ignore:
    title: 'Deleted'

event self_deleted:
    title: 'Deleted'

        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r'Unexpected token.*LINKPROPERTY', line=3, col=22)
    def test_eschema_syntax_link06(self):
        """
        link coollink:
            required linkproperty foo to int
        """

    def test_eschema_syntax_link07(self):
        """
        link time_estimate:
           linkproperty unit to str:
               constraint my_constraint(0)
        """

    def test_eschema_syntax_link08(self):
        """
        link time_estimate:
           linkproperty unit to str:
               constraint my_constraint(0, <str>(42^2))
        """

    def test_eschema_syntax_link09(self):
        """
        link time_estimate:
           linkproperty unit to str:
               constraint my_constraint(')', `)`($$)$$))

% OK %

        link time_estimate:
           linkproperty unit to str:
               constraint my_constraint(')', `)`(')'))
        """

    def test_eschema_syntax_import01(self):
        """
        import foo

        concept Bar extending foo::Foo:
            link text to str
        """

    def test_eschema_syntax_import02(self):
        """
        import mylib.util.foo

        concept Bar extending mylib.util.foo::Foo:
            link text to str
        """

    def test_eschema_syntax_import03(self):
        """
        import foo as bar

        concept Bar extending bar::Foo:
            link text to str
        """

    def test_eschema_syntax_import04(self):
        """
        import mylib.util.foo as bar

        concept Bar extending bar::Foo:
            link text to str
        """

    def test_eschema_syntax_import05(self):
        """
        import mylib.util.foo as foo, mylib.special.foo as sfoo
        import (
            # ignore indentation because of parentheses
    otherlib.bar as bar,
    otherlib.ham as spam)

        concept Bar extending foo::Foo, sfoo::Foo, bar::Bar, spam::Ham:
            link text to str
        """

    def test_eschema_syntax_import06(self):
        """
        import action.event.foo

        concept Bar extending action.event.foo::Foo:
            link text to str
        """

    def test_eschema_syntax_function01(self):
        """
        function len() -> std::int:
            from sql function: length
        """

    def test_eschema_syntax_function02(self):
        """
        function some_func($foo: std::int = 42) -> std::str:
            from sql: "SELECT 'life';"

% OK %

        function some_func($foo: std::int = 42) -> std::str:
            from sql:>
                SELECT 'life';
        """

    def test_eschema_syntax_function03(self):
        """
        function some_func($foo: std::int = 42) -> std::str:
            from edgeql:>
                SELECT 'life';
        """

    def test_eschema_syntax_function04(self):
        """
        # the line continuation is just to allow long single line
        function myfunc($arg1: str, $arg2: str = 'DEFAULT',
                        *$arg3:std::int) -> \
                        set of int:
            volatile: true
            description:>
                myfunc sample
            from sql:>
                SELECT blarg;
        """

    def test_eschema_syntax_function05(self):
        """
        function myfunc($arg1: str,
                        $arg2: str = 'DEFAULT',
                        *$arg3: std::int) -> set of int:
            volatile: true
            description:>
                myfunc sample
            from sql:>
                SELECT blarg;
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  "unexpected 'initial value' in function definition",
                  line=3, col=26)
    def test_eschema_syntax_function06(self):
        """
        function some_func($foo: std::int = 42) -> std::str:
            initial value: 'bad'
            from edgeql:>
                SELECT 'life';
        """

    def test_eschema_syntax_function07(self):
        """
        function some_func($foo: std::int = bar(42)) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function08(self):
        """
        function some_func($foo: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function09(self):
        """
        function some_func($foo: str = $$)$$) -> std::str:
            from edgeql function: some_other_func

% OK %

        function some_func($foo: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function10(self):
        """
        function some_func($foo: str = $a1$)$a1$) -> std::str:
            from edgeql function: some_other_func

% OK %

        function some_func($foo: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_function11(self):
        """
        function some_func($`(`: str = ')') -> std::str:
            from edgeql function: some_other_func
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"Unexpected token.*RPAREN",
                  line=2, col=42)
    def test_eschema_syntax_function12(self):
        """
        function some_func($`(`: str = ) ) -> std::str:
            from edgeql function: some_other_func
        """

    def test_eschema_syntax_aggregate01(self):
        """
        aggregate len() -> std::int:
            initial value: 0
            from sql function: length
        """

    def test_eschema_syntax_aggregate02(self):
        """
        aggregate some_func($foo: std::int = 42) -> std::str:
            initial value: 'start'
            from sql:>
                SELECT 'life';
        """

    def test_eschema_syntax_aggregate03(self):
        """
        aggregate some_func($foo: std::int = 42) -> std::str:
            initial value: ''
            from edgeql:>
                SELECT 'life';
        """

    def test_eschema_syntax_aggregate04(self):
        """
        aggregate myfunc($arg1: str, $arg2: str = 'DEFAULT',
                         *$arg3:std::int) -> int:
            initial value: 42
            volatile: true
            description: 'myfunc sample'
            from sql:>
                SELECT blarg;
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  "missing 'initial value' in aggregate definition",
                  line=2, col=9)
    def test_eschema_syntax_aggregate05(self):
        """
        aggregate len() -> std::int:
            from sql function: length
        """

    @tb.must_fail(error.SchemaSyntaxError,
                  r"missing type declaration.*\$arg3",
                  line=3, col=27)
    def test_eschema_syntax_aggregate06(self):
        """
        aggregate myfunc($arg1: str, $arg2: str = 'DEFAULT',
                         *$arg3) -> int:
            initial value: 42
        """

    def test_eschema_syntax_view01(self):
        """
        view FooBaz:
            expr := SELECT Foo FILTER Foo.bar = 'baz'
            description: 'Special Foo'
        """

    def test_eschema_syntax_view02(self):
        """
        view FooBaz:
            expr :=
                SELECT Foo
                FILTER Foo.bar = 'baz'
        """
