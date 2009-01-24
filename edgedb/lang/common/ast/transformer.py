# Portions Copyright 2009 Sprymix Inc.
# Portions Copyright 2008 by Armin Ronacher.
# License: Python License

from semantix.ast.base import *
from semantix.ast.visitor import *

class NodeTransformer(NodeVisitor):
    """
    A :class:`NodeVisitor` subclass that walks the abstract syntax tree and
    allows modification of nodes.

    The `NodeTransformer` will walk the AST and use the return value of the
    visitor methods to replace or remove the old node.  If the return value of
    the visitor method is ``None``, the node will be removed from its location,
    otherwise it is replaced with the return value.  The return value may be the
    original node in which case no replacement takes place.

    Here is an example transformer that rewrites all occurrences of name lookups
    (``foo``) to ``data['foo']``::

       class RewriteName(NodeTransformer):

           def visit_Name(self, node):
               return copy_location(Subscript(
                   value=Name(id='data', ctx=Load()),
                   slice=Index(value=Str(s=node.id)),
                   ctx=node.ctx
               ), node)

    Keep in mind that if the node you're operating on has child nodes you must
    either transform the child nodes yourself or call the :meth:`generic_visit`
    method for the node first.

    For nodes that were part of a collection of statements (that applies to all
    statement nodes), the visitor may also return a list of nodes rather than
    just a single node.

    Usually you use the transformer like this::

       node = YourTransformer().visit(node)
    """

    def generic_visit(self, node):
        for field, old_value in iter_fields(node):
            old_value = getattr(node, field, None)
            if isinstance(old_value, list):
                new_values = []
                for value in old_value:
                    if isinstance(value, AST):
                        value = self.visit(value)
                        if value is None:
                            continue
                        elif not isinstance(value, AST):
                            new_values.extend(value)
                            continue
                    new_values.append(value)

                for value in new_values:
                    value.parent = node

                old_value[:] = new_values

            elif isinstance(old_value, AST):
                new_node = self.visit(old_value)
                if new_node is None:
                    delattr(node, field)
                else:
                    new_node.parent = node
                    setattr(node, field, new_node)

        return node


    def replace_child(self, child, new_child):
        if child.parent is None:
            raise ASTError('ast node does not have parent')

        node = child.parent

        for field, value in iter_fields(node):
            if isinstance(value, list):
                for i in range(0, len(value)):
                    if value[i] == child:
                        value[i] = new_child
                        return True

            elif isinstance(value, AST):
                if value == child:
                    setattr(node, field, new_child)
                    return True
