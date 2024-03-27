#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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

import unittest

import edb.edgeql.quote as qlquote


class QuoteTests(unittest.TestCase):

    def test_quote_string(self):
        self.assertEqual(qlquote.quote_literal(""), "''"),
        self.assertEqual(qlquote.quote_literal("abc"), "'abc'")
        self.assertEqual(qlquote.quote_literal("\""), "'\"'")
        self.assertEqual(qlquote.quote_literal("\b"), "'\\b'")
        self.assertEqual(qlquote.quote_literal("\f"), "'\\f'")
        self.assertEqual(qlquote.quote_literal("\n"), "'\\n'")
        self.assertEqual(qlquote.quote_literal("\r"), "'\\r'")
        self.assertEqual(qlquote.quote_literal("\t"), "'\\t'")
        self.assertEqual(qlquote.quote_literal("\'"), "'\\\''")
        self.assertEqual(qlquote.quote_literal("\\"), "'\\\\'")
        self.assertEqual(qlquote.quote_literal("\\b"), "'\\\\b'")
        self.assertEqual(qlquote.quote_literal("\\f"), "'\\\\f'")
        self.assertEqual(qlquote.quote_literal("\\n"), "'\\\\n'")
        self.assertEqual(qlquote.quote_literal("\\r"), "'\\\\r'")
        self.assertEqual(qlquote.quote_literal("\\t"), "'\\\\t'")
        self.assertEqual(qlquote.quote_literal("\\\'"), "'\\\\\\\''")
        self.assertEqual(qlquote.quote_literal("\\\\"), "'\\\\\\\\'")
        self.assertEqual(qlquote.quote_literal(
            "abc\"efg\nhij\'klm\\nop"),
            "'abc\"efg\\nhij\\\'klm\\\\nop'")
