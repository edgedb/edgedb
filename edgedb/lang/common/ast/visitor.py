# Portions Copyright 2009 Sprymix Inc.
# Portions Copyright 2008 by Armin Ronacher.
# License: Python License

from semantix.ast.base import *

class NodeVisitor(object):
    """
    A node visitor base class that walks the abstract syntax tree and calls a
    visitor function for every node found.  This function may return a value
    which is forwarded by the `visit` method.

    This class is meant to be subclassed, with the subclass adding visitor
    methods.

    Per default the visitor functions for the nodes are ``'visit_'`` +
    class name of the node.  So a `TryFinally` node visit function would
    be `visit_TryFinally`.  This behavior can be changed by overriding
    the `visit` method.  If no visitor function exists for a node
    (return value `None`) the `generic_visit` visitor is used instead.

    Don't use the `NodeVisitor` if you want to apply changes to nodes during
    traversing.  For this a special visitor exists (`NodeTransformer`) that
    allows modifications.
    """

    def visit(self, node):
        method = 'visit_' + node.__class__.__name__
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        for field, value in iter_fields(node):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, AST):
                        self.visit(item)
            elif isinstance(value, AST):
                self.visit(value)

    def find_child(self, node, test_func):
        for field, value in iter_fields(node):
            if isinstance(value, list):
                for n in value:
                    if test_func(n):
                        return n

                    _n = self.find_child(n, test_func)
                    if _n is not None:
                        return _n

            elif isinstance(value, AST):
                if test_func(value):
                    return value
                else:
                    _n = self.find_child(value, test_func)
                    if _n is not None:
                        return _n

    def find_parent(self, node, test_func):
        if node.parent and test_func(node.parent):
            return node.parent
        elif not node.parent:
            return None
        return self.find_parent(node.parent, test_func)
