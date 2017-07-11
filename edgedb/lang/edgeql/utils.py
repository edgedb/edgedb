##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import copy
import typing

from edgedb.lang.common import ast

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


def index_parameters(ql_args):
    if isinstance(ql_args, qlast.SelectQuery):
        ql_args = ql_args.result

    if not isinstance(ql_args, qlast.NamedTuple):
        raise ValueError(
            'unable to unpack arguments: a named tuple was expected')

    ql_args = {e.name.name:
                    e.val.result if isinstance(e.val, qlast.SelectQuery)
                        else e.val
               for e in ql_args.elements}

    return ql_args


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
