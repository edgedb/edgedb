##
# Portions Copyright (c) 2008-2010 Sprymix Inc.
# Portions Copyright (c) 2008 Armin Ronacher.
# All rights reserved.
#
# This code is licensed under the PSFL license.
##


from .base import *

def find_children(node, test_func, *args, force_traversal=False, **kwargs):
    visited = set()

    def _find_children(node, test_func):
        result = []

        if node in visited:
            return result
        else:
            visited.add(node)

        for field, value in iter_fields(node):
            field_spec = node._fields[field]

            if isinstance(value, (list, set, frozenset)):
                for n in value:
                    if not isinstance(n, AST):
                        continue

                    if test_func(n, *args, **kwargs):
                        result.append(n)

                    if field_spec.traverse or force_traversal:
                        _n = _find_children(n, test_func)
                        if _n is not None:
                            result.extend(_n)

            elif isinstance(value, AST):
                if test_func(value, *args, **kwargs):
                    result.append(value)

                if field_spec.traverse or force_traversal:
                    _n = _find_children(value, test_func)
                    if _n is not None:
                        result.extend(_n)
        return result

    return _find_children(node, test_func)

def find_parent(node, test_func):
    if node.parent and test_func(node.parent):
        return node.parent
    elif not node.parent:
        return None
    return find_parent(node.parent, test_func)


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

    def find_children(self, node, test_func, *args, **kwargs):
        visited = set()

        def _find_children(node, test_func):
            result = []

            if node in visited:
                return result
            else:
                visited.add(node)

            for field, value in iter_fields(node):
                if isinstance(value, (list, set)):
                    for n in value:
                        if not isinstance(n, AST):
                            continue

                        if test_func(n, *args, **kwargs):
                            result.append(n)

                        _n = _find_children(n, test_func)
                        if _n is not None:
                            result.extend(_n)

                elif isinstance(value, AST):
                    if test_func(value, *args, **kwargs):
                        result.append(value)

                    _n = _find_children(value, test_func)
                    if _n is not None:
                        result.extend(_n)
            return result

        return _find_children(node, test_func)

    def find_parent(self, node, test_func):
        if node.parent and test_func(node.parent):
            return node.parent
        elif not node.parent:
            return None
        return self.find_parent(node.parent, test_func)
