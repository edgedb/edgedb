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

from collections import defaultdict

from edb.common.ordered import OrderedSet


class UnresolvedReferenceError(Exception):
    pass


class CycleError(Exception):
    def __init__(self, msg, path=None):
        super().__init__(msg)
        self.path = path


def sort(graph, *, return_record=False, allow_unresolved=False):
    adj = defaultdict(OrderedSet)
    loop_control = defaultdict(OrderedSet)

    for item_name, item in graph.items():
        if "merge" in item:
            for merge in item["merge"]:
                if merge in graph:
                    adj[item_name].add(merge)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            merge, item_name))

        if "deps" in item:
            for dep in item["deps"]:
                if dep in graph:
                    adj[item_name].add(dep)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            dep, item_name))

        if "loop-control" in item:
            for ctrl in item["loop-control"]:
                if ctrl in graph:
                    loop_control[item_name].add(ctrl)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            ctrl, item_name))

    visiting = OrderedSet()
    visited = set()
    sorted = []

    def visit(item, for_control=False):
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
                sorted.append(item)
                visited.add(item)
            visiting.remove(item)

    for item in graph:
        visit(item)

    if return_record:
        return ((item, graph[item]) for item in sorted)
    else:
        return (graph[item]["item"] for item in sorted)


def normalize(graph, merger, **merger_kwargs):
    merged = {}

    for name, item in sort(graph, return_record=True):
        merge = item.get("merge")
        if merge:
            for m in merge:
                merger(item["item"], merged[m], **merger_kwargs)

        merged.setdefault(name, item["item"])

    return merged.values()
