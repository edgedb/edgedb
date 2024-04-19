#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import inspect
import unittest

from edb.edgeql import ast as qlast
from edb.edgeql import tracer
from edb.edgeql.compiler import normalization
from edb.edgeql.compiler.inference import cardinality
from edb.edgeql.compiler.inference import volatility
from edb.ir import ast as irast
from edb.server.compiler import status


class TestTracer(unittest.TestCase):

    def test_tracer_dispatch(self):
        dispatcher = tracer.trace
        not_implemented = dispatcher.registry[object]

        for name, astcls in inspect.getmembers(qlast, inspect.isclass):
            if (issubclass(astcls, qlast.Expr)
                    # ignore these abstract AST nodes
                    and not astcls.__abstract_node__
                    # ignore special internal class
                    and astcls is not qlast.OptionalExpr
                    # ignore query parameters
                    and not issubclass(astcls, qlast.Parameter)
                    # ignore all config operations
                    and not issubclass(astcls, qlast.ConfigOp)):

                if dispatcher.dispatch(astcls) is not_implemented:
                    self.fail(f'trace for {name} is not implemented')

    def test_get_status_dispatch(self):
        dispatcher = status.get_status
        not_implemented = dispatcher.registry[object]

        for name, astcls in inspect.getmembers(qlast, inspect.isclass):
            # Every non-abstract Command AST node should have status
            if (issubclass(astcls, (qlast.Command, qlast.DDLCommand))
                    and not astcls.__abstract_node__):

                if dispatcher.dispatch(astcls) is not_implemented:
                    self.fail(f'get_status for {name} is not implemented')

    def test_infer_cardinality_dispatch(self):
        dispatcher = cardinality._infer_cardinality
        not_implemented = dispatcher.registry[object]

        for name, astcls in inspect.getmembers(irast, inspect.isclass):
            # Expr and Stmt need cardinality inference.
            if (issubclass(astcls, (irast.Expr, irast.Stmt,
                                    # ConfigInsert is the only config
                                    # command needing cardinality inference.
                                    irast.ConfigInsert))
                    and not astcls.__abstract_node__):

                if dispatcher.dispatch(astcls) is not_implemented:
                    self.fail(
                        f'_infer_cardinality for {name} is not implemented')

    def test_infer_volatility_dispatch(self):
        dispatcher = volatility._infer_volatility_inner
        not_implemented = dispatcher.registry[object]

        for name, astcls in inspect.getmembers(irast, inspect.isclass):
            # Expr and Stmt need cardinality inference.
            if (issubclass(astcls, (irast.Expr, irast.Stmt,
                                    # ConfigInsert is the only config
                                    # command needing volatility inference.
                                    irast.ConfigInsert))
                    and not astcls.__abstract_node__):

                if dispatcher.dispatch(astcls) is not_implemented:
                    self.fail(
                        f'_infer_volatility for {name} is not implemented')

    def test_normalize_dispatch(self):
        dispatcher = normalization.normalize
        not_implemented = dispatcher.registry[object]

        for name, astcls in inspect.getmembers(qlast, inspect.isclass):
            # Every non-DDL AST node should be normalizable
            if (issubclass(astcls, qlast.Base)
                    and not issubclass(astcls, qlast.DDL)):

                if dispatcher.dispatch(astcls) is not_implemented:
                    self.fail(f'normalize for {name} is not implemented')
