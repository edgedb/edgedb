#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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


import os.path
import textwrap

from edb.testbase import lang as tb

from edb.edgeql import compiler
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser


class TestEdgeQLVolatilityInference(tb.BaseEdgeQLCompilerTest):
    """Unit tests for volatility inference."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse_query(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                modaliases={None: 'default'},
            ),
        )

        expected_volatility = qltypes.Volatility(
            textwrap.dedent(expected).strip(' \n'))
        self.assertEqual(ir.volatility, expected_volatility,
                         'unexpected volatility:\n' + source)

    def test_edgeql_ir_volatility_inference_00(self):
        """
        SELECT Card
% OK %
        Stable
        """

    def test_edgeql_ir_volatility_inference_01(self):
        """
        WITH
            foo := random()
        SELECT
            foo
% OK %
        Volatile
        """

    def test_edgeql_ir_volatility_inference_02(self):
        """
        SELECT
            Card
        FILTER
            random() > 0.9
% OK %
        Volatile
        """

    def test_edgeql_ir_volatility_inference_03(self):
        """
        SELECT
            Card
        ORDER BY
            random()
% OK %
        Volatile
        """

    def test_edgeql_ir_volatility_inference_04(self):
        """
        SELECT
            Card
        LIMIT
            <int64>random()
% OK %
        Volatile
        """

    def test_edgeql_ir_volatility_inference_05(self):
        """
        SELECT
            Card
        OFFSET
            <int64>random()
% OK %
        Volatile
        """

    def test_edgeql_ir_volatility_inference_06(self):
        """
        INSERT
            Card {
                name := 'foo',
                element := 'fire',
                cost := 1,
            }
% OK %
        Modifying
        """

    def test_edgeql_ir_volatility_inference_07(self):
        """
        UPDATE
            Card
        SET {
                name := 'foo',
        }
% OK %
        Modifying
        """

    def test_edgeql_ir_volatility_inference_08(self):
        """
        DELETE
            Card
% OK %
        Modifying
        """

    def test_edgeql_ir_volatility_inference_09(self):
        """
        with X := 1 select X
% OK %
        Immutable
        """

    def test_edgeql_ir_volatility_inference_10(self):
        """
        with X := User select X
% OK %
        Stable
        """

    def test_edgeql_ir_volatility_inference_11(self):
        """
        with X := random() select X
% OK %
        Volatile
        """

    def test_edgeql_ir_volatility_inference_12(self):
        """
        select AliasOne
% OK %
        Immutable
        """

    def test_edgeql_ir_volatility_inference_13(self):
        """
        select global GlobalOne
% OK %
        Stable
        """

    def test_edgeql_ir_volatility_inference_14(self):
        """
        select AirCard
% OK %
        Stable
        """

    def test_edgeql_ir_volatility_inference_15(self):
        """
        select global HighestCost
% OK %
        Stable
        """

    def test_edgeql_ir_volatility_inference_16(self):
        """
        select global CardsWithText
% OK %
        Stable
        """
