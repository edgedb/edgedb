#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

from edb.common import typeutils

from . import base


class SkipNode(Exception):
    pass


def find_children(node, test_func, *args, force_traversal=False,
                  terminate_early=False, **kwargs):
    visited = set()

    def _find_children(node, test_func):
        result = []

        if node in visited:
            return result
        else:
            visited.add(node)

        for field, value in base.iter_fields(node, include_meta=False):
            field_spec = node._fields[field]

            if isinstance(value, (list, set, frozenset)):
                for n in value:
                    if not base.is_ast_node(n):
                        continue

                    try:
                        if not field_spec.hidden and test_func(
                                n, *args, **kwargs):
                            result.append(n)
                            if terminate_early:
                                return result
                    except SkipNode:
                        continue

                    if field_spec.child_traverse or force_traversal:
                        _n = _find_children(n, test_func)
                        if _n is not None:
                            result.extend(_n)
                            if terminate_early:
                                return result

            elif base.is_ast_node(value):
                try:
                    if not field_spec.hidden and test_func(
                            value, *args, **kwargs):
                        result.append(value)
                        if terminate_early:
                            return result
                except SkipNode:
                    continue

                if field_spec.child_traverse or force_traversal:
                    _n = _find_children(value, test_func)
                    if _n is not None:
                        result.extend(_n)
                        if terminate_early:
                            return result
        return result

    nodes = _find_children(node, test_func)
    if terminate_early:
        if nodes:
            return nodes[0]
        else:
            return None
    else:
        return nodes


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
            self._memo = {}
        self._context = context

    @property
    def memo(self):
        return self._memo

    @classmethod
    def run(cls, node, **kwargs):
        visitor = cls(**kwargs)
        return visitor.visit(node)

    def container_visit(self, node):
        result = []
        for elem in node:
            if base.is_ast_node(elem) or typeutils.is_container(elem):
                result.append(self.visit(elem))
            else:
                result.append(elem)
        return node.__class__(result)

    def repeated_node_visit(self, node):
        result = self.memo[node]
        if result is None:
            return node
        else:
            return result

    def node_visit(self, node):
        if node in self.memo:
            return self.repeated_node_visit(node)
        else:
            self.memo[node] = None

        for cls in node.__class__.__mro__:
            method = 'visit_' + cls.__name__
            visitor = getattr(self, method, None)
            if visitor is not None:
                break
        else:
            visitor = self.generic_visit
        result = visitor(node)
        self.memo[node] = result
        return result

    def visit(self, node):
        if typeutils.is_container(node):
            return self.container_visit(node)
        else:
            return self.node_visit(node)

    def generic_visit(self, node, *, combine_results=None):
        field_results = []

        for _field, value in base.iter_fields(node, include_meta=False):
            if typeutils.is_container(value):
                for item in value:
                    if base.is_ast_node(item):
                        res = self.visit(item)
                        if res is not None:
                            field_results.append(res)
            elif base.is_ast_node(value):
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

    for field, _value in base.iter_fields(n1, include_meta=False):
        if not n1._fields[field].hidden:
            n1v = getattr(n1, field)
            n2v = getattr(n2, field)

            if typeutils.is_container(n1v):
                n1v = list(n1v)
                if typeutils.is_container(n2v):
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

                    if base.is_ast_node(item1):
                        if not nodes_equal(item1, item2):
                            return False
                    else:
                        if item1 != item2:
                            return False

            elif base.is_ast_node(n1v):
                if not nodes_equal(n1v, n2v):
                    return False

            else:
                if n1v != n2v:
                    return False

    return True
