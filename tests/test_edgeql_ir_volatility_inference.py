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
        qltree = qlparser.parse(source)
        ir = compiler.compile_ast_to_ir(qltree, self.schema)

        expected_volatility = qltypes.Volatility(
            textwrap.dedent(expected).strip(' \n'))
        self.assertEqual(ir.volatility, expected_volatility,
                         'unexpected volatility:\n' + source)

    def test_edgeql_ir_volatility_inference_00(self):
        """
        WITH MODULE test
        SELECT Card
% OK %
        STABLE
        """

    def test_edgeql_ir_volatility_inference_01(self):
        """
        WITH
            MODULE test,
            foo := random()
        SELECT
            foo
% OK %
        VOLATILE
        """

    def test_edgeql_ir_volatility_inference_02(self):
        """
        WITH
            MODULE test
        SELECT
            Card
        FILTER
            random() > 0.9
% OK %
        VOLATILE
        """

    def test_edgeql_ir_volatility_inference_03(self):
        """
        WITH
            MODULE test
        SELECT
            Card
        ORDER BY
            random()
% OK %
        VOLATILE
        """

    def test_edgeql_ir_volatility_inference_04(self):
        """
        WITH
            MODULE test
        SELECT
            Card
        LIMIT
            <int64>random()
% OK %
        VOLATILE
        """

    def test_edgeql_ir_volatility_inference_05(self):
        """
        WITH
            MODULE test
        SELECT
            Card
        OFFSET
            <int64>random()
% OK %
        VOLATILE
        """

    def test_edgeql_ir_volatility_inference_06(self):
        """
        WITH
            MODULE test
        INSERT
            Card {
                name := 'foo',
                element := 'fire',
                cost := 1,
            }
% OK %
        VOLATILE
        """

    def test_edgeql_ir_volatility_inference_07(self):
        """
        WITH
            MODULE test
        UPDATE
            Card
        SET {
                name := 'foo',
        }
% OK %
        VOLATILE
        """

    def test_edgeql_ir_volatility_inference_08(self):
        """
        WITH
            MODULE test
        DELETE
            Card
% OK %
        VOLATILE
        """
