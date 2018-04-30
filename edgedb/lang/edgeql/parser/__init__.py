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

    if not isinstance(tree, qlast.Statement):
        tree = qlast.SelectQuery(result=tree)

    if module_aliases:
        modaliases = []
        for alias, module in module_aliases.items():
            decl = qlast.ModuleAliasDecl(module=module, alias=alias)
            modaliases.append(decl)

        if not tree.aliases:
            tree.aliases = modaliases
        else:
            tree.aliases = modaliases + tree.aliases

    return tree


def parse_block(expr):
    parser = EdgeQLBlockParser()
    return parser.parse(expr)
