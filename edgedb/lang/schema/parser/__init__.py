##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .parser import EdgeSchemaParser


def parse_fragment(expr):
    parser = EdgeSchemaParser()
    return parser.parse(expr)


def parse(expr, module_aliases=None):
    tree = parse_fragment(expr)

    return tree
