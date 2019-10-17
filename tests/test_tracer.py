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


class TestTracer(unittest.TestCase):

    def test_tracer(self):
        not_implemented = tracer.trace.registry[object]

        for name, astcls in inspect.getmembers(qlast, inspect.isclass):
            if (issubclass(astcls, qlast.Expr)
                    # ignore these abstract AST nodes
                    and not astcls.__abstract_node__
                    # ignore special internal class
                    and astcls is not qlast._Optional
                    # ignore query parameters
                    and not issubclass(astcls, qlast.Parameter)
                    # ignore all config operations
                    and not issubclass(astcls, qlast.ConfigOp)):

                if tracer.trace.dispatch(astcls) is not_implemented:
                    self.fail(f'trace for {name} is not implemented')
