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
from typing import *

from collections import defaultdict

from edb.common.ordered import OrderedSet


class UnresolvedReferenceError(Exception):
    pass


class CycleError(Exception):
    def __init__(self, msg: str, path: Optional[List[Any]] = None) -> None:
        super().__init__(msg)
        self.path = path


K = TypeVar('K')
V = TypeVar('V')
T = TypeVar('T')


class DepGraphEntry(Generic[K, V, T]):

    item: V
    deps: MutableSet[K]
    merge: Optional[MutableSet[K]]
    loop_control: MutableSet[K]
    extra: Optional[T]

    def __init__(
        self,
        item: V,
        deps: Optional[MutableSet[K]] = None,
        merge: Optional[MutableSet[K]] = None,
        loop_control: Optional[MutableSet[K]] = None,
        extra: Optional[T] = None,
    ) -> None:
        self.item = item
        if deps is None:
            deps = set()
        self.deps = deps
        self.merge = merge
        if loop_control is None:
            loop_control = set()
        self.loop_control = loop_control
        self.extra = extra


def sort_ex(
    graph: Mapping[K, DepGraphEntry[K, V, T]],
    *,
    allow_unresolved: bool = False,
) -> Iterator[Tuple[K, DepGraphEntry[K, V, T]]]:

    adj: Dict[K, OrderedSet[K]] = defaultdict(OrderedSet)
    loop_control: Dict[K, OrderedSet[K]] = defaultdict(OrderedSet)

    for item_name, item in graph.items():
        if item.merge is not None:
            for merge in item.merge:
                if merge in graph:
                    adj[item_name].add(merge)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            merge, item_name))

        if item.deps:
            for dep in item.deps:
                if dep in graph:
                    adj[item_name].add(dep)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            dep, item_name))

        if item.loop_control:
            for ctrl in item.loop_control:
                if ctrl in graph:
                    loop_control[item_name].add(ctrl)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            ctrl, item_name))

    visiting: OrderedSet[K] = OrderedSet()
    visited = set()
    order = []

    def visit(item: K, for_control: bool = False) -> None:
        if item in visiting:
            raise CycleError(
                f"dependency cycle between {list(visiting)[1]!r} "
                f"and {item!r}",
                path=list(visiting)[1:],
            )
        if item not in visited:
            visiting.add(item)
            for n in adj[item]:
                visit(n)
            for n in loop_control[item]:
                visit(n, for_control=True)
            if not for_control:
                order.append(item)
                visited.add(item)
            visiting.remove(item)

    for key in graph:
        visit(key)

    return ((key, graph[key]) for key in order)


def sort(
    graph: Mapping[K, DepGraphEntry[K, V, T]],
    *,
    allow_unresolved: bool = False,
) -> Iterator[V]:
    items = sort_ex(graph, allow_unresolved=allow_unresolved)
    return (i[1].item for i in items)


if TYPE_CHECKING:

    class MergeFunction(Protocol[V]):

        def __call__(
            self,
            item: V,
            parent: V,
            **kwargs: Any,
        ) -> V:
            ...


def normalize(
    graph: Mapping[K, DepGraphEntry[K, V, T]],
    merger: MergeFunction[V],
    **merger_kwargs: Any,
) -> Iterable[V]:
    merged: Dict[K, V] = {}

    for name, item in sort_ex(graph):
        merge = item.merge
        if merge:
            for m in merge:
                merger(item.item, merged[m], **merger_kwargs)

        merged.setdefault(name, item.item)

    return merged.values()
