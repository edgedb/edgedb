class UnresolvedReferenceError(Exception):
    pass

class LoopError(Exception):
    pass

def _dfs(visited, v, a, list, root, item, parent = None, allow_loops = False):
    if visited[item]:
        if not allow_loops:
            raise LoopError("detected dependency loop: %s included from %s" % (item, parent))
        else:
            return

    visited[item] = True

    if item in a:
        for next in a[item]:
            if next != item:
                _dfs(visited, v, a, list, root, next, item, allow_loops)

    visited[item] = False

    if item not in list:
        list.append(item)

def dfs(adj, radj, root, allow_loops=False):
    visited = dict(zip(radj.keys(), (False,) * len(radj.keys())))
    list = []

    _dfs(visited, radj.keys(), adj, list, root, root, None, allow_loops)

    return list

def normalize(graph, merger, allow_loops=False, allow_unresolved=False):
    # TODO: Use tarjan algorithm here to properly determine cycles

    # Adjacency matrix
    adj = dict(zip(graph.keys(), (list() for i in range(len(graph)))))

    # Reverse adjacency matrix
    radj = dict(zip(graph.keys(), (list() for i in range(len(graph)))))

    for item_name, item in graph.items():
        if "merge" in item:
            for merge in item["merge"]:
                if merge in graph:
                    adj[item_name].append(merge)
                    radj[merge].append(item_name)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError("reference to an undefined item " + merge + " in " + item_name)

        if "deps" in item:
            for dep in item["deps"]:
                if dep in graph:
                    adj[item_name].append(dep)
                    radj[dep].append(item_name)
                elif not allow_unresolved:
                    raise UnresolvedReferenceError("reference to an undefined item " + dep + " in " + item_name)

    if not allow_loops:
        for item in graph:
            dfs(adj, radj, item)

    sorted = []

    for item_name, item_deps in radj.items():
        if len(item_deps) == 0:
            sorted += dfs(adj, radj, item_name, allow_loops)

    merged = {}

    for item_name in sorted:
        item = graph[item_name]

        if "merge" in item:
            for merge in item["merge"]:
                if merge in graph:
                    item["item"] = merger(merged[merge], item["item"])

        if item_name not in merged:
            merged[item_name] = item["item"]

    result = []
    added = {}

    for item_name in sorted:
        if item_name not in added:
            result.append(merged[item_name])
        added[item_name] = True

    return result
