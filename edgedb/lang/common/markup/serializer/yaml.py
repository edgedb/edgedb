##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from yaml import nodes

from .base import serializer, serialize
from .. import elements


@serializer(handles=nodes.Node)
def serialize_to_markup(node, *, ctx):
    me = elements.lang.TreeNode(id=id(node), name=node.__class__.__name__)

    me.add_child(label='tag', node=elements.lang.String(str=str(node.tag)))

    value = None
    if isinstance(node.value, nodes.CollectionNode):
        children = []
        for child in node.value:
            children.append(serialize(child, ctx=ctx))
        value = elements.lang.List(items=children)
    else:
        value = serialize(node.value, ctx=ctx)

    me.add_child(label='value', node=value)
    return me
