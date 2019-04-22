#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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


import copy
import itertools
import typing

from edb import errors
from edb.common import ast

from . import ast as qlast


class ParameterInliner(ast.NodeTransformer):

    def __init__(self, args_map):
        super().__init__()
        self.args_map = args_map

    def visit_Path(self, node):
        if (len(node.steps) != 1 or
                not isinstance(node.steps[0], qlast.ObjectRef)):
            return node

        param_name = node.steps[0].name
        try:
            arg = self.args_map[param_name]
        except KeyError:
            return node

        arg = copy.deepcopy(arg)
        return arg

    def visit_Parameter(self, node):
        raise errors.InvalidConstraintDefinitionError(
            f'dollar-prefixed "$parameters" are not supported in constraints')


def inline_parameters(ql_expr: qlast.Base, args: typing.Dict[str, qlast.Base]):
    inliner = ParameterInliner(args)
    inliner.visit(ql_expr)


def index_parameters(ql_args: typing.List[qlast.Base], *,
                     parameters, schema):
    result = {}
    varargs = None
    variadic = parameters.find_variadic(schema)
    variadic_num = variadic.get_num(schema) if variadic else -1

    for (i, e), p in itertools.zip_longest(enumerate(ql_args),
                                           parameters.objects(schema),
                                           fillvalue=None):
        if isinstance(e, qlast.SelectQuery):
            e = e.result

        if variadic and variadic_num == i:
            assert varargs is None
            varargs = []
            result[p.get_shortname(schema)] = qlast.Array(elements=varargs)

        if varargs is not None:
            varargs.append(e)
        else:
            result[p.get_shortname(schema)] = e

    return result


class AnchorInliner(ast.NodeTransformer):

    def __init__(self, anchors):
        super().__init__()
        self.anchors = anchors

    def visit_Path(self, node):
        if not node.steps:
            return node

        step0 = node.steps[0]

        if isinstance(step0, (qlast.Subject, qlast.Source)):
            node.steps[0] = self.anchors[step0.__class__]
        elif isinstance(step0, qlast.ObjectRef) and step0.name in self.anchors:
            node.steps[0] = self.anchors[step0.name]

        return node


def inline_anchors(ql_expr: qlast.Base,
                   anchors: typing.Dict[object, qlast.Base]):
    inliner = AnchorInliner(anchors)
    inliner.visit(ql_expr)
