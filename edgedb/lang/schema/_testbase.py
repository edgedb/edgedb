##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re

from edgedb.lang._testbase import BaseParserTest, must_fail
from edgedb.lang.common import markup
from edgedb.lang.schema import codegen
from edgedb.lang.schema.parser import parser


class ParserTest(BaseParserTest):
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
