##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from collections import defaultdict, OrderedDict

from semantix.utils.datastructures import OrderedSet


class UnresolvedReferenceError(Exception):
    pass


class CycleError(Exception):
    pass


def sort(graph, return_record=False):
    adj = defaultdict(OrderedSet)
    radj = defaultdict(OrderedSet)

    for item_name, item in graph.items():
        if "merge" in item:
            for merge in item["merge"]:
                if merge in graph:
                    adj[item_name].add(merge)
                    radj[merge].add(item_name)
                else:
                    raise UnresolvedReferenceError("reference to an undefined item %s in %s" \
                                                   % (merge, item_name))

        if "deps" in item:
            for dep in item["deps"]:
                if dep in graph:
                    adj[item_name].add(dep)
                    radj[dep].add(item_name)
                else:
                    raise UnresolvedReferenceError("reference to an undefined item %s in %s" \
                                                   % (dep, item_name))

    visiting = set()
    visited = set()
    sorted = []

    def visit(item):
        if item in visiting:
            raise CycleError("detected cycle")
        if item not in visited:
            visiting.add(item)
            for n in adj[item]:
                visit(n)
            sorted.append(item)
            visiting.remove(item)
            visited.add(item)

    for item in graph:
        visit(item)

    if return_record:
        return ((item, graph[item]) for item in sorted)
    else:
        return (graph[item]["item"] for item in sorted)


def normalize(graph, merger):
    merged = OrderedDict()

    for name, item in sort(graph, return_record=True):
        merge = item.get("merge")
        if merge:
            for m in merge:
                merger(item["item"], merged[m])

        merged.setdefault(name, item["item"])

    return merged.values()
