##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re

from edgedb.lang._testbase import BaseParserTest
from edgedb.lang.common import markup
from edgedb.lang.schema import codegen
from edgedb.lang.schema.parser import parser
from edgedb.lang.schema import ddl as s_ddl
from edgedb.lang.schema import declarative as s_decl
from edgedb.lang.schema import delta as s_delta
from edgedb.lang.schema import std as s_std


class BaseSchemaTest(BaseParserTest):
    re_filter = re.compile(r'[\s\'"()]+|(#.*?\n)|SELECT')

    def assert_equal(self, expected, result):
        expected_stripped = self.re_filter.sub('', expected).lower()
        result_stripped = self.re_filter.sub('', result).lower()

        assert expected_stripped == result_stripped, \
            '[test]expected: {}\n[test] != returned: {}'.format(
                expected, result)


class ParserTest(BaseSchemaTest):
    parser_cls = parser.EdgeSchemaParser

    def get_parser(self, *, spec):
        return self.__class__.parser_cls()

    def run_test(self, *, source, spec, expected=None):
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


class LoaderTest(BaseSchemaTest):
    def run_test(self, *, source, spec, expected=None):
        debug = bool(os.environ.get('DEBUG_ESCHEMA'))

        if debug:
            markup.dump_code(source, lexer='edgeschema')

        empty_schema = s_std.load_std_schema()
        loaded_schema = s_decl.parse_module_declarations([('test', source)])

        schema_diff = s_delta.delta_schemas(loaded_schema, empty_schema)
        ddl_text = s_ddl.ddl_text_from_delta(schema_diff)

        if debug:
            print(ddl_text)

        self.assert_equal(expected, ddl_text)
