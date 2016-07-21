##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re

from edgedb.lang import _testbase as lang_tb
from edgedb.lang.common import markup
from edgedb.lang import graphql as edge_graphql


class TranslatorTest(lang_tb.BaseParserTest):
    re_filter = re.compile(r'''[\s,]+|(\#.*?\n)''')

    def assert_equal(self, expected, result):
        expected_stripped = self.re_filter.sub('', expected).lower()
        result_stripped = self.re_filter.sub('', result).lower()

        assert expected_stripped == result_stripped, \
            '[test]expected: {}\n[test] != returned: {}'.format(
                expected, result)

    def run_test(self, *, source, spec, expected=None):
        debug = bool(os.environ.get('DEBUG_GRAPHQL'))
        if debug:
            markup.dump_code(source, lexer='graphql')

        result = edge_graphql.translate(source)

        if debug:
            markup.dump_code(result, lexer='graphql')

        expected_src = source

        self.assert_equal(expected_src, result)
