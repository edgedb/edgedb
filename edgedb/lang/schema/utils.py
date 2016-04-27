##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools


def is_nontrivial_container(value):
    coll_classes = (collections.abc.Sequence, collections.abc.Set)
    trivial_classes = (str, bytes, bytearray, memoryview)
    return (isinstance(value, coll_classes) and
                not isinstance(value, trivial_classes))


def get_prototype_nearest_common_ancestor(prototypes):
    # First, find the intersection of parents
    prototypes = list(prototypes)
    first = prototypes[0].get_mro()
    common = set(first).intersection(
        *[set(c.get_mro()) for c in prototypes[1:]])
    common = sorted(common, key=lambda i: first.index(i))
    if common:
        return common[0]
    else:
        return None


def minimize_prototype_set_by_most_generic(prototypes):
    """Minimize the given prototype set by filtering out all subclasses"""

    protos = list(prototypes)
    mros = [set(p.get_mro()) for p in protos]
    count = len(protos)
    smap = itertools.starmap

    # Return only those entries that do not have other entries in their mro
    result = [
        proto for i, proto in enumerate(protos)
        if not any(smap(set.__contains__,
                        ((mros[i], protos[j])
                         for j in range(count) if j != i)))
    ]

    return result


def minimize_prototype_set_by_least_generic(prototypes):
    """Minimize the given prototype set by filtering out all superclasses"""

    protos = list(prototypes)
    mros = [set(p.get_mro()) for p in protos]
    count = len(protos)
    smap = itertools.starmap

    # Return only those entries that are not present in other entries' mro
    result = [
        proto for i, proto in enumerate(protos)
        if not any(smap(set.__contains__,
                        ((mros[j], protos[i])
                         for j in range(count) if j != i)))
    ]

    return result


def get_inheritance_map(prototypes):
    """Return a dict where values are strict subclasses of the key"""
    return {prototype: [p for p in prototypes
                        if p != prototype and p.issubclass(prototype)]
            for prototype in prototypes}


def get_full_inheritance_map(schema, prototypes):
    """Same as :func:`get_inheritance_map`, but considers full hierarchy"""

    chain = itertools.chain.from_iterable
    result = {}

    for p, descendants in get_inheritance_map(prototypes).items():
        result[p] = set(chain(d.descendants(schema) for d in descendants)) \
                        | set(descendants)

    return result
