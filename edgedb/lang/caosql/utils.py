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
from . import parser
from . import transformer


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
    trans = transformer.CaosqlTreeTransformer(schema, module_aliases)
    revtrans = transformer.CaosqlReverseTransformer()

    caosql_tree = parser.parse(expr, module_aliases)
    ir = trans.transform(
                    caosql_tree, (), module_aliases=module_aliases,
                    anchors=anchors)
    caosql_tree = revtrans.transform(ir, inline_anchors=inline_anchors)

    source = codegen.CaosQLSourceGenerator.to_source(
                    caosql_tree, pretty=False)

    return ir, caosql_tree, source


def normalize_expr(expr, schema, *, module_aliases=None, anchors=None,
                                    inline_anchors=False):
    _, _, expr = normalize_tree(
        expr, schema, module_aliases=module_aliases, anchors=anchors,
                      inline_anchors=inline_anchors)

    return expr
