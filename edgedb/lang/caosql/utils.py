##
# Copyright (c) 2015 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from metamagic.utils import ast

from . import ast as qlast
from . import codegen
from . import compiler
from . import decompiler
from . import parser


def inline_constants(caosql_tree, values, types):
    flt = lambda n: isinstance(n, qlast.ConstantNode) and n.index in values
    constants = ast.find_children(caosql_tree, flt)

    for constant in constants:
        value = values[constant.index]

        if (isinstance(value, collections.Container)
                and not isinstance(value, (str, bytes))):
            elements = [qlast.ConstantNode(value=i) for i in value]
            value = qlast.SequenceNode(elements=elements)

        constant.value = value


def normalize_tree(expr, schema, *, module_aliases=None, anchors=None,
                                    inline_anchors=False):
    ir = compiler.compile_to_ir(expr, schema,
                                module_aliases=module_aliases,
                                anchors=anchors)
    caosql_tree = decompiler.decompile_ir(ir, inline_anchors=inline_anchors)

    source = codegen.generate_source(caosql_tree, pretty=False)

    return ir, caosql_tree, source


def normalize_expr(expr, schema, *, module_aliases=None, anchors=None,
                                    inline_anchors=False):
    _, _, expr = normalize_tree(
        expr, schema, module_aliases=module_aliases, anchors=anchors,
                      inline_anchors=inline_anchors)

    return expr
