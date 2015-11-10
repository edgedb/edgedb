##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .parser import CaosQLParser
from .errors import CaosQLSyntaxError

from .. import ast as qlast


def parse_fragment(expr):
    parser = CaosQLParser()
    return parser.parse(expr)


def parse(expr, module_aliases=None):
    tree = parse_fragment(expr)

    if not isinstance(tree, qlast.StatementNode):
        selnode = qlast.SelectQueryNode()
        selnode.targets = [qlast.SelectExprNode(expr=tree)]
        tree = selnode

    if module_aliases:
        nses = []
        for alias, module in module_aliases.items():
            decl = qlast.NamespaceDeclarationNode(namespace=module, alias=alias)
            nses.append(decl)

        if tree.namespaces is None:
            tree.namespaces = nses
        else:
            tree.namespaces.extend(nses)

    return tree
