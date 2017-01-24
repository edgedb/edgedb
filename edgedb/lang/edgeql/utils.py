##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.common import ast

from . import ast as qlast
from . import codegen
from . import compiler
from . import parser


class ParameterInliner(ast.NodeTransformer):

    def __init__(self, values):
        super().__init__()
        self.values = values

    def visit_Parameter(self, node):
        value = self.values[node.name]

        if (isinstance(value, collections.Container) and
                not isinstance(value, (str, bytes))):
            elements = [qlast.Constant(value=i) for i in value]
            new_node = qlast.Sequence(elements=elements)
        else:
            new_node = qlast.Constant(value=value)

        return new_node


def inline_parameters(edgeql_tree, values, types):
    inliner = ParameterInliner(values)
    return inliner.visit(edgeql_tree)


def normalize_tree(expr, schema, *, modaliases=None, anchors=None,
                   inline_anchors=False):
    ir = compiler.compile_ast_to_ir(
        expr, schema, modaliases=modaliases, anchors=anchors)
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
