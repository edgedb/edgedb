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


from edgedb.lang import _testbase as tb
from edgedb.lang.graphql import ast as gqlast
from edgedb.lang.graphql.parser import parser as gql_parser


class GraphQLAstValueTest(tb.AstValueTest):
    parser_debug_flag = 'DEBUG_GRAPHQL'
    markup_dump_lexer = 'graphql'

    def get_parser(self, *, spec):
        return gql_parser.GraphQLParser()


class TestGraphQLParser(GraphQLAstValueTest):
    def test_graphql_vars_float01(self):
        self.run_test(
            source="""
            query test(
                $a: Float = 2.31e-8
                $b: Float = 2.31e-008
                $c: Float = 2.31e-08
                $d: Float = 0.231e-7
                $e: Float = 231e-10

                $f: Float = -2.31e-8
                $g: Float = -2.31e-008
                $h: Float = -2.31e-08
                $i: Float = -0.231e-7
                $j: Float = -231e-10

                $k: Float = 2.31e+8
                $l: Float = 2.31e+008
                $m: Float = 2.31e+08
                $n: Float = 0.231e+9
                $o: Float = 231e+6

                $p: Float = 2.31e8
                $q: Float = 2.31e008
                $r: Float = 2.31e08
                $s: Float = 0.231e9
                $t: Float = 231e6

            ) { id }
            """,
            expected={
                '$a': (gqlast.FloatLiteral, 2.31e-8),
                '$b': (gqlast.FloatLiteral, 2.31e-8),
                '$c': (gqlast.FloatLiteral, 2.31e-8),
                '$d': (gqlast.FloatLiteral, 2.31e-8),
                '$e': (gqlast.FloatLiteral, 2.31e-8),

                '$f': (gqlast.FloatLiteral, -2.31e-8),
                '$g': (gqlast.FloatLiteral, -2.31e-8),
                '$h': (gqlast.FloatLiteral, -2.31e-8),
                '$i': (gqlast.FloatLiteral, -2.31e-8),
                '$j': (gqlast.FloatLiteral, -2.31e-8),

                '$k': (gqlast.FloatLiteral, 2.31e+8),
                '$l': (gqlast.FloatLiteral, 2.31e+8),
                '$m': (gqlast.FloatLiteral, 2.31e+8),
                '$n': (gqlast.FloatLiteral, 2.31e+8),
                '$o': (gqlast.FloatLiteral, 2.31e+8),

                '$p': (gqlast.FloatLiteral, 2.31e+8),
                '$q': (gqlast.FloatLiteral, 2.31e+8),
                '$r': (gqlast.FloatLiteral, 2.31e+8),
                '$s': (gqlast.FloatLiteral, 2.31e+8),
                '$t': (gqlast.FloatLiteral, 2.31e+8),
            }
        )

    def test_graphql_vars_int01(self):
        self.run_test(
            source="""
            query test(
                $a: Int = 0
                $b: Int = 123
                $c: Int = -123
                $d: Int = 1234567890
            ) { id }
            """,
            expected={
                '$a': (gqlast.IntegerLiteral, 0),
                '$b': (gqlast.IntegerLiteral, 123),
                '$c': (gqlast.IntegerLiteral, -123),
                '$d': (gqlast.IntegerLiteral, 1234567890),
            }
        )

    def test_graphql_vars_str01(self):
        self.run_test(
            source=R"""
            query test(
                $a: String = "\u279b"
            ) { id }
            """,
            expected={
                '$a': (gqlast.StringLiteral, '\u279b'),
            }
        )
