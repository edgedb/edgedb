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


from collections import defaultdict, OrderedDict

from edb.common.ordered import OrderedSet


class UnresolvedReferenceError(Exception):
    pass


class CycleError(Exception):
    pass


def sort(graph, return_record=False, root_only=False):
    adj = defaultdict(OrderedSet)
    radj = defaultdict(OrderedSet)

    for item_name, item in graph.items():
        if "merge" in item:
            for merge in item["merge"]:
                if merge in graph:
                    adj[item_name].add(merge)
                    radj[merge].add(item_name)
                else:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            merge, item_name))

        if "deps" in item:
            for dep in item["deps"]:
                if dep in graph:
                    adj[item_name].add(dep)
                    radj[dep].add(item_name)
                else:
                    raise UnresolvedReferenceError(
                        'reference to an undefined item {} in {}'.format(
                            dep, item_name))

    visiting = set()
    visited = set()
    sorted = []

    def visit(item):
        if item in visiting:
            raise CycleError("detected cycle on vertex {!r}".format(item))
        if item not in visited:
            visiting.add(item)
            for n in adj[item]:
                visit(n)
            sorted.append(item)
            visiting.remove(item)
            visited.add(item)

    if root_only:
        items = set(graph) - set(radj)
    else:
        items = graph

    for item in items:
        visit(item)

    if return_record:
        return ((item, graph[item]) for item in sorted)
    else:
        return (graph[item]["item"] for item in sorted)


def normalize(graph, merger, **merger_kwargs):
    merged = OrderedDict()

    for name, item in sort(graph, return_record=True):
        merge = item.get("merge")
        if merge:
            for m in merge:
                merger(item["item"], merged[m], **merger_kwargs)

        merged.setdefault(name, item["item"])

    return merged.values()
