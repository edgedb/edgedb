##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import error as schema_error
from . import objects as so
from . import named


def delta_bases(old_bases, new_bases):
    dropped = frozenset(old_bases) - frozenset(new_bases)
    removed_bases = [so.PrototypeRef(prototype_name=b) for b in dropped]
    common_bases = [b for b in old_bases if b not in dropped]

    added_bases = []

    j = 0

    added_set = set()
    added_base_refs = []

    if common_bases:
        for i, base in enumerate(new_bases):
            if common_bases[j] == base:
                # Found common base, insert the accummulated
                # list of new bases and continue
                if added_base_refs:
                    ref = so.PrototypeRef(prototype_name=common_bases[j])
                    added_bases.append((added_base_refs, ('BEFORE', ref)))
                    added_base_refs = []
                j += 1
                if j >= len(common_bases):
                    break
                else:
                    continue

            # Base has been inserted at position j
            added_base_refs.append(so.PrototypeRef(prototype_name=base))
            added_set.add(base)

    # Finally, add all remaining bases to the end of the list
    tail_bases = added_base_refs + [
        so.PrototypeRef(prototype_name=b) for b in new_bases
        if b not in added_set and b not in common_bases
    ]

    if tail_bases:
        added_bases.append((tail_bases, 'LAST'))

    return tuple(removed_bases), tuple(added_bases)


class AlterInherit(sd.Command):
    astnode = qlast.AlterAddInheritNode, qlast.AlterDropInheritNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        # The base changes are handled by AlterNamedPrototype
        return None


class RebaseNamedPrototype(named.NamedPrototypeCommand):
    new_base = so.Field(tuple, default=tuple())
    removed_bases = so.Field(tuple)
    added_bases = so.Field(tuple)

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.prototype_name)

    def apply(self, schema, context):
        prototype = schema.get(self.prototype_name, type=self.prototype_class)
        bases = list(prototype.bases)
        removed_bases = {b.prototype_name for b in self.removed_bases}
        existing_bases = set()

        for b in prototype.bases:
            if b.name in removed_bases:
                bases.remove(b)
            else:
                existing_bases.add(b.name)

        index = {b.name: i for i, b in enumerate(bases)}

        for new_bases, pos in self.added_bases:
            if isinstance(pos, tuple):
                pos, ref = pos

            if pos == 'LAST':
                idx = len(bases)
            elif pos == 'FIRST':
                idx = 0
            else:
                idx = index[ref]

            bases[idx:idx] = [schema.get(b.prototype_name) for b in new_bases
                              if b.prototype_name not in existing_bases]
            index = {b.name: i for i, b in enumerate(bases)}

        prototype.bases = bases

        return prototype


def _merge_mro(obj, mros):
    result = []

    while True:
        nonempty = [mro for mro in mros if mro]
        if not nonempty:
            return result

        for mro in nonempty:
            candidate = mro[0]
            tails = [m for m in nonempty
                     if id(candidate) in {id(c) for c in m[1:]}]
            if not tails:
                break
        else:
            msg = "Could not find consistent MRO for %s" % obj.name
            raise schema_error.SchemaError(msg)

        result.append(candidate)

        for mro in nonempty:
            if mro[0] is candidate:
                del mro[0]

    return result


def compute_mro(obj):
    bases = obj.bases if obj.bases is not None else tuple()
    mros = [[obj]]

    for base in bases:
        mros.append(base.get_mro())

    return _merge_mro(obj, mros)


class InheritingPrototype(named.NamedPrototype):
    bases = so.Field(named.NamedPrototypeList,
                     default=named.NamedPrototypeList,
                     coerce=True, private=True, compcoef=0.714)

    mro = so.Field(named.NamedPrototypeList,
                   coerce=True, default=None, derived=True)

    is_abstract = so.Field(bool, default=False, private=True, compcoef=0.909)
    is_final = so.Field(bool, default=False, compcoef=0.909)

    def merge(self, obj, *, schema):
        super().merge(obj, schema=schema)
        schema.drop_inheritance_cache(obj)

    def delta(self, other, reverse=False, *, context):
        old, new = (other, self) if not reverse else (self, other)

        with context(old, new):
            delta = super().delta(other, reverse=reverse, context=context)

            if old and new:
                delta_driver = self.delta_driver

                old_base_names = old.get_base_names()
                new_base_names = new.get_base_names()

                if old_base_names != new_base_names and delta_driver.rebase:
                    removed, added = delta_bases(
                        old_base_names, new_base_names)

                    delta.add(delta_driver.rebase(
                        prototype_name=new.name,
                        prototype_class=new.__class__.get_canonical_class(),
                        removed_bases=removed,
                        added_bases=added,
                        new_base=tuple(new_base_names)))

        return delta

    def __getstate__(self):
        state = super().__getstate__()
        state['bases'] = [
            so.PrototypeRef(prototype_name=b.name)
            for b in self.bases
        ]

        return state

    def get_base_names(self):
        return self.bases.get_names()

    def get_topmost_base(self):
        return self.get_mro()[-1]

    def get_mro(self):
        return compute_mro(self)

    def issubclass(self, parent):
        if isinstance(parent, so.BasePrototype):
            mro = self.get_mro()

            if parent in mro:
                return True
            else:
                vchildren = getattr(parent, '_virtual_children', None)
                if vchildren:
                    return bool(set(vchildren) & set(mro))
                else:
                    return False
        else:
            mro = self.get_mro()

            if not isinstance(parent, tuple):
                parents = (parent,)
            else:
                parents = parent

            expanded_parents = set()

            for parent in parents:
                virt_children = getattr(parent, '_virtual_children', None)
                if virt_children:
                    expanded_parents.update(virt_children)
                expanded_parents.add(parent)

            return bool(set(expanded_parents) & set(mro))

    def get_nearest_common_descendant(self, descendants):
        descendants = list(descendants)
        candidate = descendants.pop()

        for descendant in descendants:
            if candidate.issubclass(descendant):
                continue
            elif descendant.issubclass(candidate):
                candidate = descendant
            else:
                return None

        return candidate

    def descendants(self, schema):
        return schema._get_descendants(self)

    def children(self, schema):
        return schema._get_descendants(self, max_depth=0)

    def acquire_ancestor_inheritance(self, schema):
        for base in self.bases:
            if isinstance(base, so.BasePrototype):
                self.merge(base, schema=schema)

    def update_descendants(self, schema):
        for child in self.children(schema):
            child.acquire_ancestor_inheritance(schema)
            child.update_descendants(schema)

    def finalize(self, schema, bases=None):
        super().finalize(schema, bases=bases)
        self.mro = compute_mro(self)[1:]

    @classmethod
    def get_default_base_name(self):
        return None
