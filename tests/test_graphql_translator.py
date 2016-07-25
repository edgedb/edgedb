##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re
import textwrap

from edgedb.lang import _testbase as lang_tb
from edgedb.lang.common import markup
from edgedb.lang import graphql as edge_graphql

from edgedb.lang.schema import declarative as s_decl


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
            print('\n--- GRAPHQL ---')
            markup.dump_code(textwrap.dedent(source).strip(), lexer='graphql')

        result = edge_graphql.translate(self.schema, source)

        if debug:
            print('\n--- EDGEQL ---')
            markup.dump_code(result, lexer='edgeql')

        self.assert_equal(expected, result)

    def setUp(self):
        schema_text = textwrap.dedent(self.SCHEMA)
        self.schema = s_decl.load_module_declarations(
            [('test', schema_text)])


class TestGraphQLTranslation(TranslatorTest):
    SCHEMA = r"""
        concept Group:
            required link name -> str

        concept User:
            required link name -> str
            link groups -> Group:
                mapping: **
    """

    def test_graphql_translation_01(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
        """
