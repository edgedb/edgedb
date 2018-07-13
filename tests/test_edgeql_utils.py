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


import textwrap

from edb.lang.edgeql import utils as eql_utils
from edb.lang.schema import declarative as s_decl
from edb.lang.schema import std as s_std

from edb.lang import _testbase as tb


class TestEdgeQLUtils(tb.BaseSyntaxTest):
    SCHEMA = r"""
        abstract type NamedObject:
            required property name -> str

        type UserGroup extending NamedObject:
            link settings -> Setting:
                cardinality := '1*'

        type Setting extending NamedObject:
            required property value -> str

        type Profile extending NamedObject:
            required property value -> str

        type User extending NamedObject:
            required property active -> bool
            link groups -> UserGroup:
                cardinality := '**'
            required property age -> int64
            required property score -> float64
            link profile -> Profile:
                cardinality := '*1'
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema = s_std.load_std_schema()
        s_decl.parse_module_declarations(
            cls.schema, [('test', cls.SCHEMA)])

    def _assert_normalize_expr(self, text, expected, *,
                               anchors=None, inline_anchors=False):
        normalized = eql_utils.normalize_expr(
            text, self.__class__.schema,
            anchors=anchors, inline_anchors=inline_anchors)

        self.assertEqual(
            textwrap.dedent(normalized).strip(),
            textwrap.dedent(expected).strip()
        )

    def test_edgeql_utils_normalize_01(self):
        self._assert_normalize_expr(
            """SELECT 40 + 2""",
            """SELECT 42""",
        )

    def test_edgeql_utils_normalize_02(self):
        self._assert_normalize_expr(
            """SELECT -10""",
            """SELECT -10""",
        )

    def test_edgeql_utils_normalize_03(self):
        self._assert_normalize_expr(
            """SELECT len('a')""",
            """SELECT std::len('a')""",
        )

    def test_edgeql_utils_normalize_04(self):
        self._assert_normalize_expr(
            """WITH MODULE test SELECT User{name}""",
            """SELECT test::User { name }"""
        )

    def test_edgeql_utils_normalize_05(self):
        self._assert_normalize_expr(
            """SELECT <int64>'1'""",
            """SELECT <std::int64>'1'""",
        )

    def test_edgeql_utils_normalize_06(self):
        self._assert_normalize_expr(
            """SELECT ('aaa')[2:-1]""",
            """SELECT ('aaa')[2:-1]"""
        )

    def test_edgeql_utils_normalize_07(self):
        self._assert_normalize_expr(
            """SELECT ('aaa')[2]""",
            """SELECT ('aaa')[2]"""
        )

    def test_edgeql_utils_normalize_08(self):
        self._assert_normalize_expr(
            """SELECT 40 + 2 - 20 * +2 + (-10 / 2)""",
            """SELECT -3""",
        )

    def test_edgeql_utils_normalize_09(self):
        self._assert_normalize_expr(
            """SELECT 2 + (len('a')+1)""",
            """SELECT (3 + std::len('a'))""",
        )

        self._assert_normalize_expr(
            """SELECT (1 + len('a')) + 2""",
            """SELECT (3 + std::len('a'))""",
        )

        self._assert_normalize_expr(
            """SELECT 2 * (len('a')*1)""",
            """SELECT (2 * std::len('a'))""",
        )

        self._assert_normalize_expr(
            """SELECT (1 * len('a')) * 2""",
            """SELECT (2 * std::len('a'))""",
        )

    def test_edgeql_utils_normalize_10(self):
        self._assert_normalize_expr(
            """SELECT 1 > 2""",
            """SELECT False""",
        )

        self._assert_normalize_expr(
            """SELECT 1 = 1""",
            """SELECT True""",
        )

        self._assert_normalize_expr(
            """SELECT 1 < (1 + 1)""",
            """SELECT True""",
        )
