##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.schema import name as sn

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import codegen as qlcodegen
from edgedb.lang.edgeql import parser as qlparser


def rewrite_refs(expr, callback):
    """Rewrite class references in EdgeQL expression."""

    tree = qlparser.parse_fragment(expr)

    def _cb(node):
        if isinstance(node, qlast.ObjectRef):
            name = sn.Name(name=node.name, module=node.module)
            upd = callback(name)
            if name != upd:
                node.name = upd.name
                node.module = upd.module

    ast.find_children(tree, _cb)

    return qlcodegen.generate_source(tree, pretty=False)
