##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools

from edgedb.lang.edgeql import ast as ql_ast

from . import name as sn
from . import objects as so


def ast_to_typeref(node: ql_ast.TypeNameNode):
    if node.subtypes:
        coll = so.Collection.get_class(node.maintype.name)

        subtypes = []
        for st in node.subtypes:
            stref = so.ClassRef(
                classname=sn.Name(module=st.module, name=st.name))
            subtypes.append(stref)

        return coll.from_subtypes(subtypes)

    mtn = sn.Name(module=node.maintype.module,
                  name=node.maintype.name)
    return so.ClassRef(classname=mtn)


def is_nontrivial_container(value):
    coll_classes = (collections.abc.Sequence, collections.abc.Set)
    trivial_classes = (str, bytes, bytearray, memoryview)
    return (isinstance(value, coll_classes) and
            not isinstance(value, trivial_classes))


def get_class_nearest_common_ancestor(classes):
    # First, find the intersection of parents
    classes = list(classes)
    first = classes[0].get_mro()
    common = set(first).intersection(
        *[set(c.get_mro()) for c in classes[1:]])
    common = sorted(common, key=lambda i: first.index(i))
    if common:
        return common[0]
    else:
        return None


def minimize_class_set_by_most_generic(classes):
    """Minimize the given Class set by filtering out all subclasses."""

    classes = list(classes)
    mros = [set(p.get_mro()) for p in classes]
    count = len(classes)
    smap = itertools.starmap

    # Return only those entries that do not have other entries in their mro
    result = [
        scls for i, scls in enumerate(classes)
        if not any(smap(set.__contains__,
                        ((mros[i], classes[j])
                         for j in range(count) if j != i)))
    ]

    return result


def minimize_class_set_by_least_generic(classes):
    """Minimize the given Class set by filtering out all superclasses."""

    classes = list(classes)
    mros = [set(p.get_mro()) for p in classes]
    count = len(classes)
    smap = itertools.starmap

    # Return only those entries that are not present in other entries' mro
    result = [
        scls for i, scls in enumerate(classes)
        if not any(smap(set.__contains__,
                        ((mros[j], classes[i])
                         for j in range(count) if j != i)))
    ]

    return result


def get_inheritance_map(classes):
    """Return a dict where values are strict subclasses of the key."""
    return {scls: [p for p in classes
                   if p != scls and p.issubclass(scls)]
            for scls in classes}


def get_full_inheritance_map(schema, classes):
    """Same as :func:`get_inheritance_map`, but considers full hierarchy."""

    chain = itertools.chain.from_iterable
    result = {}

    for p, descendants in get_inheritance_map(classes).items():
        result[p] = (set(chain(d.descendants(schema) for d in descendants)) |
                     set(descendants))

    return result
