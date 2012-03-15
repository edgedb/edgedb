##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ast

from .base import serializer, serialize
from .. import elements


@serializer(handles=ast.AST)
def serialize_to_markup(node, *, ctx):
    mn = elements.lang.TreeNode(id=id(node), name=type(node).__name__)

    for fieldname, field in ast.iter_fields(node):
        mn.add_child(label=fieldname, node=serialize(field, ctx=ctx))

    return mn

