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

from edgedb.lang import _testbase as tb

from edgedb.lang.edgeql import compiler

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import inference as irinference


class TestEdgeQLCardinalityInference(tb.BaseEdgeQLCompilerTest):
    """Unit tests for cardinality inference."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    def run_test(self, *, source, spec, expected):
        ir = compiler.compile_to_ir(source, self.schema)

        cardinality = irinference.infer_cardinality(
            ir, set(), self.schema)
        expected_cardinality = irast.Cardinality(
            textwrap.dedent(expected).strip(' \n'))
        self.assertEqual(cardinality, expected_cardinality,
                         'unexpected cardinality:\n' + source)

    def test_edgeql_ir_card_inference_01(self):
        """
        WITH MODULE test
        SELECT Card FILTER Card.name = 'Djinn'
% OK %
        1
        """

    def test_edgeql_ir_card_inference_02(self):
        """
        WITH MODULE test
        SELECT Card FILTER 'Djinn' = Card.name
% OK %
        1
        """

    def test_edgeql_ir_card_inference_03(self):
        """
        WITH MODULE test
        SELECT Card FILTER 'foo' = 'foo' AND 'Djinn' = Card.name
% OK %
        1
        """

    def test_edgeql_ir_card_inference_04(self):
        """
        WITH MODULE test
        SELECT Card FILTER 'foo' = 'foo' OR 'Djinn' = Card.name
% OK %
        *
        """

    def test_edgeql_ir_card_inference_05(self):
        """
        WITH MODULE test
        SELECT Card FILTER Card.id = <uuid>'...'
% OK %
        1
        """

    def test_edgeql_ir_card_inference_06(self):
        """
        WITH MODULE test, C2 := DETACHED Card
        SELECT Card FILTER Card = (SELECT C2 FILTER C2.name = 'Djinn')
% OK %
        1
        """

    def test_edgeql_ir_card_inference_07(self):
        """
        WITH MODULE test
        SELECT Card LIMIT 1
% OK %
        1
        """
