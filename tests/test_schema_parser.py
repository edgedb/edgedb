##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re

from edgedb.lang.common import markup
from edgedb.lang.schema import codegen
from edgedb.lang.schema.parser import parser
from edgedb.server import _testbase as tb


class ParserTest(tb.BaseParserTest):
    re_filter = re.compile(r'[\s\'"()]+|(#.*?\n)')
    parser_cls = parser.EdgeSchemaParser

    def get_parser(self, *, spec):
        return self.__class__.parser_cls()

    def assert_equal(self, expected, result):
        expected_stripped = self.re_filter.sub('', expected).lower()
        result_stripped = self.re_filter.sub('', result).lower()

        assert expected_stripped == result_stripped, \
            '[test]expected: {}\n[test] != returned: {}'.format(
                expected, result)

    def run_test(self, *, source, spec):
        debug = bool(os.environ.get('DEBUG_ESCHEMA'))

        if debug:
            markup.dump_code(source, lexer='edgeschema')

        p = self.get_parser(spec=spec)

        esast = p.parse(source)

        if debug:
            markup.dump(esast)

        processed_src = codegen.EdgeSchemaSourceGenerator.to_source(esast)

        if debug:
            markup.dump_code(processed_src, lexer='edgeschema')

        expected_src = source

        self.assert_equal(expected_src, processed_src)


class TestEdgeSchemaParser(ParserTest):
    def test_eschema_parser_concept01(self):
        """concept User extends builtins::NamedObject"""

    def test_eschema_parser_concept02(self):
        """
abstract concept OwnedObject:
    required link owner -> User
        """

    def test_eschema_parser_concept03(self):
        """
abstract concept Text:
    required link body -> str:
        constraint maxlength:
            10000
        """

    def test_eschema_parser_concept04(self):
        """
concept LogEntry extends OwnedObject, Text:
    required link spent_time -> int
        """

    def test_eschema_parser_concept05(self):
        """
concept LogEntry extends OwnedObject, Text:
   link start_date:= SELECT datetime::current_datetime()
        """

    def test_eschema_parser_concept06(self):
        """
concept LogEntry extends OwnedObject, Text:
    link start_date -> datetime:
       default:=
            SELECT datetime::current_datetime()
       title: 'Start Date'
        """

    def test_eschema_parser_concept07(self):
        """
concept Issue extends foo.bar::NamedObject, OwnedObject, Text:

    required link number -> issue_num_t:
        readonly: true

    required link status -> Status

    link priority -> Priority

    link watchers -> User:
        mapping: **

    link time_estimate -> int

    link time_spent_log -> LogEntry:
        mapping: 1*

    link start_date:= SELECT datetime::current_datetime()

    link start_date -> datetime:
       default:=
            SELECT datetime::current_datetime()
       title: 'Start Date'

    link due_date -> datetime

    link related_to -> Issue:
        mapping: **
        """

    def test_eschema_parser_ws01(self):
        """
concept LogEntry extends    OwnedObject,    Text:

    # irrelevant comment indent
            # irrelevant comment indent
        # irrelevant comment indent

  link start_date -> datetime:


                       default:=


                        SELECT

                            datetime::current_datetime()



                       title: 'Start Date'
        """

    def test_eschema_parser_atom01(self):
        """
atom issue_num_t extends builtins::sequence
        """

    def test_eschema_parser_atom02(self):
        """
atom issue_num_t extends int:
    default: 42
        """

    def test_eschema_parser_atom03(self):
        """
atom basic extends int:
    title: "Basic Atom"
    constraint min: 0
    constraint max: 123456
    default: 2
        """

    def test_eschema_parser_atom04(self):
        """
atom basic extends int:

    title: "Basic Atom"

    constraint min: 0
    constraint max: 123456
    constraint expr:= subject % 2 = 0

    default: 2
        """

    def test_eschema_parser_atom05(self):
        """
atom basic extends int:

    title: "Basic Atom"

    constraint min: 0
    constraint max: 123456
    constraint expr:=
        subject % 2 = 0

    default: 2
        """

    def test_eschema_parser_atom06(self):
        """
atom basic extends int:

    title: "Basic Atom"

    constraint min: 0
    constraint max: 123456
    constraint expr:= subject % 2 = 0

    default: 2

atom inherits_default extends basic

abstract atom abstract_atom extends int
        """

    def test_eschema_parser_atom07(self):
        """
final atom none
        """

    def test_eschema_parser_constraint01(self):
        """
constraint max:
    expr:= subject <= $param
    errmessage: 'Maximum allowed value for {subject} is {param}.'

        """

    def test_eschema_parser_constraint02(self):
        """
abstract constraint length:
    subject:= str::len(cast(subject as str))
        """

    def test_eschema_parser_constraint03(self):
        """
constraint maxlength extends max, length:
    errmessage: '{subject} must be no longer than {param} characters.'
        """

    def test_eschema_parser_constraint04(self):
        """
constraint max:
    expr:= subject <= $param
    errmessage: 'Maximum allowed value for {subject} is {param}.'

abstract constraint length:
    subject:= str::len(cast(subject as str))

constraint maxlength extends max, length:
    errmessage: '{subject} must be no longer than {param} characters.'

        """

    def test_eschema_parser_linkproperty01(self):
        """
linkproperty foo:
    title: "Sample property"
        """

    def test_eschema_parser_linkproperty02(self):
        """
linkproperty bar extends foo
        """

    def test_eschema_parser_linkproperty03(self):
        """
linkproperty bar extends foo:
    title: "Another property"
        """

    def test_eschema_parser_linkproperty04(self):
        """
linkproperty foo:
    title: "Sample property"

linkproperty bar extends foo:
    title: "Another property"
        """

    def test_eschema_parser_action01(self):
        """
action ignore
        """

    def test_eschema_parser_action02(self):
        """
action ignore:
    title: 'Deleted'
        """

    def test_eschema_parser_event01(self):
        """
event self_deleted
        """

    def test_eschema_parser_event02(self):
        """
event self_deleted:
    title: 'Deleted'
        """

    def test_eschema_parser_link01(self):
        """
link coollink
        """

    def test_eschema_parser_link02(self):
        """
link coollink extends boringlink
        """

    def test_eschema_parser_link03(self):
        """
link coollink:
    linkproperty foo -> int
        """

    def test_eschema_parser_link04(self):
        """
link coollink:
    linkproperty foo -> int
    linkproperty bar -> int

    constraint expr := self.foo = self.bar
        """

    def test_eschema_parser_link05(self):
        """
linkproperty foo:
    title: "Sample property"

linkproperty bar extends foo:
    title: "Another property"

link coollink:
    linkproperty foo -> int:
        constraint min: 0
        constraint max: 123456
        constraint expr := subject % 2 = 0
        default: 2

    linkproperty bar -> int

    constraint expr := self.foo = self.bar

    on self_deleted ignore

action ignore:
    title: 'Deleted'

event self_deleted:
    title: 'Deleted'

        """
