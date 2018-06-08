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
import typing

from edb.lang.common import ast

from . import ast as qlast
from . import codegen
from . import compiler
from . import parser


class ParameterInliner(ast.NodeTransformer):

    def __init__(self, args_map):
        super().__init__()
        self.args_map = args_map

    def visit_Parameter(self, node):
        try:
            arg = self.args_map[node.name]
        except KeyError:
            raise ValueError(
                f'could not resolve {node.name} argument') from None

        arg = copy.deepcopy(arg)
        return arg


def inline_parameters(ql_expr: qlast.Base, args: typing.Dict[str, qlast.Base]):
    inliner = ParameterInliner(args)
    inliner.visit(ql_expr)


def index_parameters(ql_args: typing.List[qlast.Base], *,
                     varparam: typing.Optional[int]=None):
    result = []
    container = result

    for i, e in enumerate(ql_args):
        if isinstance(e, qlast.SelectQuery):
            e = e.result

        if i == varparam:
            container = []
            result.append(qlast.Array(elements=container))

        container.append(e)

    return {str(i): arg for i, arg in enumerate(result)}


def normalize_tree(expr, schema, *, modaliases=None, anchors=None,
                   inline_anchors=False, arg_types=None):
    ir = compiler.compile_ast_to_ir(
        expr, schema,
        modaliases=modaliases, anchors=anchors, arg_types=arg_types)

    edgeql_tree = compiler.decompile_ir(ir, inline_anchors=inline_anchors)

    source = codegen.generate_source(edgeql_tree, pretty=False)

    return ir, edgeql_tree, source


def normalize_expr(expr, schema, *, modaliases=None, anchors=None,
                   inline_anchors=False):
    tree = parser.parse(expr, modaliases)
    _, _, expr = normalize_tree(
        tree, schema, modaliases=modaliases, anchors=anchors,
        inline_anchors=inline_anchors)

    return expr
