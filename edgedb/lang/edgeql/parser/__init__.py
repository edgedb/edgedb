##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .parser import EdgeQLExpressionParser, EdgeQLBlockParser
from .. import ast as qlast


def parse_fragment(expr):
    parser = EdgeQLExpressionParser()
    return parser.parse(expr)


def parse(expr, module_aliases=None):
    tree = parse_fragment(expr)

    if not isinstance(tree, qlast.StatementNode):
        tree = qlast.SelectQueryNode(result=tree)

    if module_aliases:
        nses = []
        for alias, module in module_aliases.items():
            decl = qlast.NamespaceAliasDeclNode(namespace=module, alias=alias)
            nses.append(decl)

        if tree.namespaces is None:
            tree.namespaces = nses
        else:
            tree.namespaces.extend(nses)

    return tree


def parse_block(expr):
    parser = EdgeQLBlockParser()
    return parser.parse(expr)
