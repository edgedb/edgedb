##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .parser import GraphQLParser


def parse_fragment(expr):
    parser = GraphQLParser()
    return parser.parse(expr)


def parse(expr, module_aliases=None):
    tree = parse_fragment(expr)

    return tree
