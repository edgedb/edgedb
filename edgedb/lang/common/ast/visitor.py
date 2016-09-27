##
# Portions Copyright (c) 2008-2010 MagicStack Inc.
# Portions Copyright (c) 2008 Armin Ronacher.
# All rights reserved.
#
# This code is licensed under the PSFL license.
##

from .base import *  # NOQA


class SkipNode(Exception):
    pass


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

                    try:
                        if not field_spec.hidden and test_func(
                                n, *args, **kwargs):
                            result.append(n)
                    except SkipNode:
                        continue

                    if field_spec.child_traverse or force_traversal:
                        _n = _find_children(n, test_func)
                        if _n is not None:
                            result.extend(_n)

            elif isinstance(value, AST):
                try:
                    if not field_spec.hidden and test_func(
                            value, *args, **kwargs):
                        result.append(value)
                except SkipNode:
                    continue

                if field_spec.child_traverse or force_traversal:
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


class NodeVisitor:
    """Walk the AST and call a visitor function for every node found.

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

    def __init__(self, *, context=None, memo=None):
        if memo is not None:
            self._memo = memo
        else:
            self._memo = set()
        self._context = context

    @classmethod
    def run(cls, node, **kwargs):
        visitor = cls(**kwargs)
        return visitor.visit(node)

    def container_visit(self, node):
        result = []
        for elem in node:
            if isinstance(elem, AST) or is_container(elem):
                result.append(self.visit(elem))
            else:
                result.append(elem)
        return node.__class__(result)

    def repeated_node_visit(self, node):
        return node

    def node_visit(self, node):
        if node in self._memo:
            return self.repeated_node_visit(node)
        else:
            self._memo.add(node)

        for cls in node.__class__.__mro__:
            method = 'visit_' + cls.__name__
            visitor = getattr(self, method, None)
            if visitor is not None:
                break
        else:
            visitor = self.generic_visit
        return visitor(node)

    def visit(self, node):
        if is_container(node):
            return self.container_visit(node)
        else:
            return self.node_visit(node)

    def generic_visit(self, node, *, combine_results=None):
        field_results = []

        for field, value in iter_fields(node):
            if is_container(value):
                for item in value:
                    if isinstance(item, AST):
                        res = self.visit(item)
                        if res is not None:
                            field_results.append(res)
            elif isinstance(value, AST):
                res = self.visit(value)
                if res is not None:
                    field_results.append(res)

        if combine_results is not None:
            return combine_results(field_results)
        else:
            return self.combine_field_results(field_results)

    def combine_field_results(self, results):
        return results


def nodes_equal(n1, n2):
    if type(n1) is not type(n2):
        return False

    for field, value in iter_fields(n1):
        if not n1._fields[field].hidden:
            n1v = getattr(n1, field)
            n2v = getattr(n2, field)

            if is_container(n1v):
                n1v = list(n1v)
                if is_container(n2v):
                    n2v = list(n2v)
                else:
                    return False

                if len(n1v) != len(n2v):
                    return False

                for i, item1 in enumerate(n1v):
                    try:
                        item2 = n2v[i]
                    except IndexError:
                        return False

                    if isinstance(item1, AST):
                        if not nodes_equal(item1, item2):
                            return False
                    else:
                        if item1 != item2:
                            return False

            elif isinstance(n1v, AST):
                if not nodes_equal(n1v, n2v):
                    return False

            else:
                if n1v != n2v:
                    return False

    return True
