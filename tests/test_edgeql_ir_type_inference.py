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
from edb.edgeql import parser as qlparser


class TestEdgeQLTypeInference(tb.BaseEdgeQLCompilerTest):
    """Unit tests for type inference."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_ir_inference.esdl')

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse_query(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                modaliases={None: 'default'},
            ),
        )

        if not expected:
            return

        # The expected type is either given for the whole query
        # (by default) or for a specific element of the top-level
        # shape. In case of the specific element the name of the shape
        # element must be given followed by ": " and then the
        # type.
        exp = textwrap.dedent(expected).strip(' \n').split(': ')

        if len(exp) == 1:
            field = None
            expected_type_name = exp[0]
        elif len(exp) == 2:
            field = exp[0].strip()
            expected_type_name = exp[1].strip()
        else:
            raise ValueError(
                f'unrecognized expected specification: {expected!r}')

        if field is not None:
            shape = ir.expr.expr.result.shape
            for el, _ in shape:
                if str(el.path_id.rptr_name()).endswith(field):
                    typeref = el.typeref
                    break
            else:
                raise AssertionError(f'shape field not found: {field!r}')

        else:
            typeref = ir.expr.typeref

        typeref = typeref.real_material_type

        self.assertEqual(str(typeref.name_hint), expected_type_name,
                         'unexpected type:\n' + source)

    def test_edgeql_ir_type_inference_00(self):
        """
        SELECT Card { name }
% OK %
        default::Card
        """

    def test_edgeql_ir_type_inference_01(self):
        """
        SELECT Card { name }
% OK %
        name: std::str
        """

    def test_edgeql_ir_type_inference_02(self):
        """
        SELECT Card UNION User
% OK %
        __derived__::(default:Card | default:User)
        """

    def test_edgeql_ir_type_inference_03(self):
        """
        SELECT {Card, User}
% OK %
        __derived__::(default:Card | default:User)
        """

    def test_edgeql_ir_type_inference_04(self):
        """
        SELECT Card if true else User
% OK %
        __derived__::(default:Card | default:User)
        """

    def test_edgeql_ir_type_inference_05(self):
        """
        SELECT Card ?? User
% OK %
        __derived__::(default:Card | default:User)
        """
