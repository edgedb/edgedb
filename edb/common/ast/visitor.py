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

from typing import (
    AbstractSet, Any, Callable, Collection, Optional, Iterable, Type, TypeVar
)

from edb.common import typeutils

from . import base


class SkipNode(Exception):
    pass


_T = TypeVar('_T')


def find_children(
    node: base.AST | Collection[base.AST],
    type: Type[_T],
    test_func: Optional[Callable[[_T], bool]] = None,
    terminate_early=False,
    extra_skips: AbstractSet[str] = frozenset(),
) -> list[_T]:
    visited = set()
    result = []

    def _find_children(node):
        if isinstance(node, (tuple, list, set, frozenset)):
            for n in node:
                if _find_children(n):
                    return True
            return False
        elif isinstance(node, dict):
            for n in node.values():
                if _find_children(n):
                    return True
            return False
        elif not base.is_ast_node(node):
            return False

        if node in visited:
            return False
        else:
            visited.add(node)

        try:
            if isinstance(node, type) and (not test_func or test_func(node)):
                result.append(node)
                if terminate_early:
                    return True
        except SkipNode:
            return False

        for field, value in base.iter_fields(node, include_meta=False):
            field_spec = node._fields[field]
            if field_spec.hidden or field_spec.name in extra_skips:
                continue

            if _find_children(value):
                return True

        return False

    _find_children(node)
    return result


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

    skip_hidden = False
    extra_skips: AbstractSet[str] = frozenset()

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

    def container_visit(self, node) -> dict[Any, Any] | Iterable[Any]:
        def _visit_element(elem):
            if base.is_ast_node(elem) or typeutils.is_container(elem):
                return self.visit(elem)
            else:
                return elem

        result: dict[Any, Any] | Iterable[Any]

        if isinstance(node, dict):
            result = {}
            for key, value in node.items():
                result[key] = _visit_element(value)

        elif isinstance(node, tuple):
            result = ()
            for elem in node:
                result += (_visit_element(elem),)

        else:
            result = []
            for elem in node:
                result.append(_visit_element(elem))

        return result

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
        elif base.is_ast_node(node):
            return self.node_visit(node)

    def generic_visit(self, node, *, combine_results=None):
        field_results = []

        for field, value in base.iter_fields(node, include_meta=False):
            field_spec = node._fields[field]
            if self.skip_hidden and field_spec.hidden:
                continue
            if field in self.extra_skips:
                continue

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
