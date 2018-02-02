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
from . import types as s_types


def ast_to_typeref(node: ql_ast.TypeName):
    if node.subtypes:
        coll = s_types.Collection.get_class(node.maintype.name)

        if issubclass(coll, s_types.Tuple):
            subtypes = collections.OrderedDict()
            named = False
            for si, st in enumerate(node.subtypes):
                if st.name:
                    named = True
                    type_name = st.name
                else:
                    type_name = str(si)

                subtypes[type_name] = ast_to_typeref(st)

            return coll.from_subtypes(subtypes, {'named': named})
        else:
            subtypes = []
            for st in node.subtypes:
                subtypes.append(ast_to_typeref(st))

            return coll.from_subtypes(subtypes)

    mtn = sn.Name(module=node.maintype.module,
                  name=node.maintype.name)

    return so.ClassRef(classname=mtn)


def typeref_to_ast(t: so.Class) -> ql_ast.TypeName:
    if not isinstance(t, s_types.Collection):
        if isinstance(t, so.ClassRef):
            name = t.classname
        else:
            name = t.name

        result = ql_ast.TypeName(
            maintype=ql_ast.ClassRef(
                module=name.module,
                name=name.name
            )
        )
    else:
        result = ql_ast.TypeName(
            maintype=ql_ast.ClassRef(
                name=t.schema_name
            ),
            subtypes=[
                typeref_to_ast(st) for st in t.get_subtypes()
            ]
        )

    return result


def resolve_typeref(ref: so.Class, schema) -> so.Class:
    if isinstance(ref, s_types.Tuple):
        if any(isinstance(st, so.ClassRef) for st in ref.get_subtypes()):
            subtypes = collections.OrderedDict()
            for st_name, st in ref.element_types.items():
                subtypes[st_name] = schema.get(st.classname)

            obj = ref.__class__.from_subtypes(
                subtypes, typemods=ref.get_typemods())
        else:
            obj = ref

    elif isinstance(ref, s_types.Collection):
        if any(isinstance(st, so.ClassRef) for st in ref.get_subtypes()):
            subtypes = []
            for st in ref.get_subtypes():
                subtypes.append(schema.get(st.classname))

            obj = ref.__class__.from_subtypes(
                subtypes, typemods=ref.get_typemods())
        else:
            obj = ref

    else:
        obj = schema.get(ref.classname)

    return obj


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
    return {scls: [p for p in classes if p != scls and p.issubclass(scls)]
            for scls in classes}


def get_full_inheritance_map(schema, classes):
    """Same as :func:`get_inheritance_map`, but considers full hierarchy."""

    chain = itertools.chain.from_iterable
    result = {}

    for p, descendants in get_inheritance_map(classes).items():
        result[p] = (set(chain(d.descendants(schema) for d in descendants)) |
                     set(descendants))

    return result


def merge_sticky_bool(ours, theirs, schema):
    if ours is not None and theirs is not None:
        result = max(ours, theirs)
    else:
        result = theirs if theirs is not None else ours

    return result


def merge_weak_bool(ours, theirs, schema):
    if ours is not None and theirs is not None:
        result = min(ours, theirs)
    else:
        result = theirs if theirs is not None else ours

    return result
