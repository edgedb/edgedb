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
    Any,
    Generic,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Iterable,
    Iterator,
    Mapping,
    MutableSet,
    Dict,
    TYPE_CHECKING,
)

from collections import defaultdict

from edb.common.ordered import OrderedSet


class UnresolvedReferenceError(Exception):
    pass


class CycleError(Exception):
    def __init__(
        self,
        msg: str,
        item: Any,
        path: tuple[Any, ...] = (),
    ) -> None:
        super().__init__(msg)
        self.item = item
        self.path = path


K = TypeVar('K')
V = TypeVar('V')
T = TypeVar('T')


class DepGraphEntry(Generic[K, V, T]):

    #: The graph node
    item: V
    #: An optional set of dependencies for the graph node as lookup keys.
    deps: MutableSet[K]
    #: An optional set of *weak* dependencies for the graph node as
    #: lookup keys.  The difference from regular deps is that weak deps
    #: that cause cycles are ignored.  Essentially, weak deps dictate
    #: a _preference_ in order rather than a requirement.
    weak_deps: MutableSet[K]
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
        weak_deps: Optional[MutableSet[K]] = None,
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
        if weak_deps is None:
            weak_deps = set()
        self.weak_deps = weak_deps


def sort_ex(
    graph: Mapping[K, DepGraphEntry[K, V, T]],
    *,
    allow_unresolved: bool = False,
) -> Iterator[Tuple[K, DepGraphEntry[K, V, T]]]:

    adj: Dict[K, OrderedSet[K]] = defaultdict(OrderedSet)
    weak_adj: Dict[K, OrderedSet[K]] = defaultdict(OrderedSet)
    loop_control: Dict[K, OrderedSet[K]] = defaultdict(OrderedSet)

    for item_name, item in graph.items():
        if item.weak_deps:
            for dep in item.weak_deps:
                if dep in graph:
                    weak_adj[item_name].add(dep)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            dep, item_name))

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
    visiting_weak: MutableSet[K] = set()
    visited = set()
    order = []

    def visit(
        item: K,
        for_control: bool = False,
        weak_link: bool = False,
    ) -> None:
        if item in visiting:
            # Separate the matching item from the rest of the visiting
            # set for error reporting.
            vis_list = tuple(visiting - {item})
            cycle_item = item if len(vis_list) == 0 else vis_list[-1]
            raise CycleError(
                f"dependency cycle between {cycle_item!r} "
                f"and {item!r}",
                path=vis_list,
                item=item,
            )
        if item not in visited:
            visiting.add(item)
            if weak_link:
                visiting_weak.add(item)

            try:
                for n in weak_adj[item]:
                    try:
                        visit(n, weak_link=True)
                    except CycleError:
                        if len(visiting_weak) == 0:
                            pass
                        else:
                            raise
                for n in adj[item]:
                    visit(n, weak_link=weak_link)
                for n in loop_control[item]:
                    visit(n, weak_link=weak_link, for_control=True)
                if not for_control:
                    order.append(item)
                    visited.add(item)
            except CycleError:
                if len(visiting_weak) == 1:
                    pass
                else:
                    raise
            finally:
                visiting.remove(item)
                if weak_link:
                    visiting_weak.remove(item)

    for key in graph:
        visit(key)

    return ((key, graph[key]) for key in order)


def sort(
    graph: Mapping[K, DepGraphEntry[K, V, T]],
    *,
    allow_unresolved: bool = False,
) -> Tuple[V, ...]:
    items = sort_ex(graph, allow_unresolved=allow_unresolved)
    return tuple(i[1].item for i in items)


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
