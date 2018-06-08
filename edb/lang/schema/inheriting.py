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


from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import derivable
from . import error as schema_error
from . import objects as so
from . import named
from . import utils


class InheritingObjectCommand(named.NamedObjectCommand):
    def _create_finalize(self, schema, context):
        self.scls.acquire_ancestor_inheritance(schema, dctx=context)
        self.scls.update_descendants(schema)
        super()._create_finalize(schema, context)


def delta_bases(old_bases, new_bases):
    dropped = frozenset(old_bases) - frozenset(new_bases)
    removed_bases = [so.ObjectRef(classname=b) for b in dropped]
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
                    ref = so.ObjectRef(classname=common_bases[j])
                    added_bases.append((added_base_refs, ('BEFORE', ref)))
                    added_base_refs = []
                j += 1
                if j >= len(common_bases):
                    break
                else:
                    continue

            # Base has been inserted at position j
            added_base_refs.append(so.ObjectRef(classname=base))
            added_set.add(base)

    # Finally, add all remaining bases to the end of the list
    tail_bases = added_base_refs + [
        so.ObjectRef(classname=b) for b in new_bases
        if b not in added_set and b not in common_bases
    ]

    if tail_bases:
        added_bases.append((tail_bases, 'LAST'))

    return tuple(removed_bases), tuple(added_bases)


class AlterInherit(sd.Command):
    astnode = qlast.AlterAddInherit, qlast.AlterDropInherit

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        # The base changes are handled by AlterNamedObject
        return None


class CreateInheritingObject(named.CreateNamedObject, InheritingObjectCommand):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        bases = cls._classbases_from_ast(astnode, context, schema)
        if bases is not None:
            cmd.add(
                sd.AlterObjectProperty(
                    property='bases',
                    new_value=bases
                )
            )

        if getattr(astnode, 'is_abstract', False):
            cmd.add(sd.AlterObjectProperty(
                property='is_abstract',
                new_value=True
            ))

        if getattr(astnode, 'is_final', False):
            cmd.add(sd.AlterObjectProperty(
                property='is_final',
                new_value=True
            ))

        return cmd

    @classmethod
    def _classbases_from_ast(cls, astnode, context, schema):
        classname = cls._classname_from_ast(astnode, context, schema)

        modaliases = context.modaliases

        bases = so.ObjectList(
            utils.ast_to_typeref(
                qlast.TypeName(maintype=b),
                modaliases=modaliases, schema=schema)
            for b in getattr(astnode, 'bases', None) or []
        )

        mcls = cls.get_schema_metaclass()
        if not bases and classname not in mcls.get_root_classes():
            default_base = mcls.get_default_base_name()

            if default_base is not None and classname != default_base:
                bases = so.ObjectList([
                    so.ObjectRef(classname=default_base)
                ])

        return bases

    def _create_finalize(self, schema, context):
        super()._create_finalize(schema, context)
        for base in self.scls.bases:
            schema.drop_inheritance_cache(base)


class AlterInheritingObject(named.AlterNamedObject, InheritingObjectCommand):
    def _alter_begin(self, schema, context, scls):
        super()._alter_begin(schema, context, scls)

        for op in self.get_subcommands(type=RebaseNamedObject):
            op.apply(schema, context)

        scls.acquire_ancestor_inheritance(schema)

        return scls


class DeleteInheritingObject(named.DeleteNamedObject, InheritingObjectCommand):
    def _delete_finalize(self, schema, context, scls):
        super()._delete_finalize(schema, context, scls)
        schema.drop_inheritance_cache_for_child(scls)


class RebaseNamedObject(named.NamedObjectCommand):
    _delta_action = 'rebase'

    new_base = so.Field(tuple, default=tuple())
    removed_bases = so.Field(tuple)
    added_bases = so.Field(tuple)

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)

    def apply(self, schema, context):
        metaclass = self.get_schema_metaclass()
        scls = schema.get(self.classname, type=metaclass)
        bases = list(scls.bases)
        removed_bases = {b.classname for b in self.removed_bases}
        existing_bases = set()

        for b in scls.bases:
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

            bases[idx:idx] = [schema.get(b.classname) for b in new_bases
                              if b.classname not in existing_bases]
            index = {b.name: i for i, b in enumerate(bases)}

        scls.bases = bases

        return scls


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


def create_virtual_parent(schema, children, *,
                          module_name=None, minimize_by=None):
    from . import scalars as s_scalars, objtypes as s_objtypes, sources

    if len(children) == 1:
        return next(iter(children))

    if minimize_by == 'most_generic':
        children = utils.minimize_class_set_by_most_generic(children)
    elif minimize_by == 'least_generic':
        children = utils.minimize_class_set_by_least_generic(children)

    if len(children) == 1:
        return next(iter(children))

    _children = set()
    for t in children:
        if t.is_virtual:
            _children.update(t.children(schema))
        else:
            _children.add(t)

    children = list(_children)

    if module_name is None:
        module_name = children[0].name.module

    name = sources.Source.gen_virt_parent_name((t.name for t in children),
                                               module=module_name)

    target = schema.get(name, default=None)

    if target:
        schema.update_virtual_inheritance(target, children)
        return target

    seen_scalars = False
    seen_objtypes = False

    for target in children:
        if isinstance(target, s_scalars.ScalarType):
            if seen_objtypes:
                raise schema_error.SchemaError(
                    'cannot mix scalars and objects in link target list')
            seen_scalars = True
        else:
            if seen_scalars:
                raise schema_error.SchemaError(
                    'cannot mix scalars and objects in link target list')
            seen_objtypes = True

    if seen_scalars and len(children) > 1:
        target = utils.get_class_nearest_common_ancestor(children)
        if target is None:
            raise schema_error.SchemaError(
                'cannot set multiple scalar children for a link')
    else:
        base = schema.get(s_objtypes.ObjectType.get_default_base_name())
        target = s_objtypes.ObjectType(name=name, is_abstract=True,
                                       is_virtual=True, bases=[base])
        target.acquire_ancestor_inheritance(schema)
        schema.update_virtual_inheritance(target, children)

    return target


class InheritingObject(derivable.DerivableObject):
    bases = so.Field(named.NamedObjectList,
                     default=named.NamedObjectList,
                     coerce=True, inheritable=False, compcoef=0.714)

    mro = so.Field(named.NamedObjectList,
                   coerce=True, default=None, hashable=False)

    is_abstract = so.Field(bool, default=False,
                           inheritable=False, compcoef=0.909)
    is_derived = so.Field(bool, False, compcoef=0.909)
    is_final = so.Field(bool, default=False, compcoef=0.909)
    is_virtual = so.Field(bool, default=False, compcoef=0.5)

    def merge(self, obj, *, schema, dctx=None):
        super().merge(obj, schema=schema, dctx=dctx)
        schema.drop_inheritance_cache(obj)

    def delta(self, other, reverse=False, *, context):
        old, new = (other, self) if not reverse else (self, other)

        if context is None:
            context = so.ComparisonContext()

        with context(old, new):
            delta = super().delta(other, reverse=reverse, context=context)

            if old and new:
                rebase = sd.ObjectCommandMeta.get_command_class(
                    RebaseNamedObject, type(self))

                old_base_names = old.get_base_names()
                new_base_names = new.get_base_names()

                if old_base_names != new_base_names and rebase is not None:
                    removed, added = delta_bases(
                        old_base_names, new_base_names)

                    delta.add(rebase(
                        classname=new.name,
                        metaclass=new.__class__.get_canonical_class(),
                        removed_bases=removed,
                        added_bases=added,
                        new_base=tuple(new_base_names)))

        return delta

    def init_derived(self, schema, source, *qualifiers, as_copy,
                     merge_bases=None, add_to_schema=False, mark_derived=False,
                     attrs=None, dctx=None, name=None, **kwargs):
        if name is None:
            derived_name = self.get_derived_name(
                source, *qualifiers, mark_derived=mark_derived)
        else:
            derived_name = name

        if as_copy:
            derived = super().init_derived(
                schema, source, *qualifiers, as_copy=True,
                merge_bases=merge_bases, add_to_schema=add_to_schema,
                mark_derived=mark_derived, attrs=attrs, dctx=dctx, name=name,
                **kwargs)

        else:
            derived_attrs = {}

            if attrs is not None:
                derived_attrs.update(attrs)

            if not derived_attrs.get('bases'):
                derived_attrs['bases'] = [self]

            derived_attrs.pop('name', None)

            cls = type(self)
            derived = cls(name=derived_name, **derived_attrs,
                          _setdefaults_=False, _relaxrequired_=True)

        return derived

    def __getstate__(self):
        state = super().__getstate__()
        state['bases'] = [
            so.ObjectRef(classname=b.name)
            for b in self.bases
        ]

        return state

    def get_base_names(self):
        return self.bases.get_names()

    def get_topmost_concrete_base(self):
        # Get the topmost non-abstract base.
        for ancestor in reversed(self.get_mro()):
            if not ancestor.is_abstract:
                return ancestor

        raise ValueError(f'{self.name} has no non-abstract ancestors')

    def get_mro(self):
        return compute_mro(self)

    def _issubclass(self, parent):
        my_vchildren = getattr(self, '_virtual_children', None)

        if my_vchildren is None:
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
            return all(c._issubclass(parent) for c in my_vchildren)

    def issubclass(self, parent):
        if isinstance(parent, tuple):
            return any(self.issubclass(p) for p in parent)
        else:
            if parent.name == 'std::any':
                return True
            else:
                return self._issubclass(parent)

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

    def acquire_ancestor_inheritance(self, schema, bases=None, *, dctx=None):
        if bases is None:
            bases = self.bases

        for base in bases:
            self.merge(base, schema=schema, dctx=dctx)

    def update_descendants(self, schema, dctx=None):
        for child in self.children(schema):
            child.acquire_ancestor_inheritance(schema, dctx=dctx)
            child.update_descendants(schema, dctx=dctx)

    def finalize(self, schema, bases=None, *, dctx=None):
        super().finalize(schema, bases=bases, dctx=dctx)
        self.mro = compute_mro(self)[1:]
        self.acquire_ancestor_inheritance(schema, dctx=dctx)

    @classmethod
    def get_root_classes(cls):
        return tuple()

    @classmethod
    def get_default_base_name(self):
        return None
