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


from edb.lang.common import struct
from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import derivable
from . import error as schema_error
from . import objects as so
from . import named
from . import utils


class InheritingObjectCommand(named.NamedObjectCommand):
    def _create_finalize(self, schema, context):
        schema = self.scls.acquire_ancestor_inheritance(schema, dctx=context)
        schema = self.scls.update_descendants(schema)
        return super()._create_finalize(schema, context)


def delta_bases(old_bases, new_bases):
    dropped = frozenset(old_bases) - frozenset(new_bases)
    removed_bases = [so.ObjectRef(name=b) for b in dropped]
    common_bases = [b for b in old_bases if b not in dropped]

    added_bases = []

    j = 0

    added_set = set()
    added_base_refs = []

    if common_bases:
        for i, base in enumerate(new_bases):
            if common_bases[j] == base:
                # Found common base, insert the accumulated
                # list of new bases and continue
                if added_base_refs:
                    ref = so.ObjectRef(name=common_bases[j])
                    added_bases.append((added_base_refs, ('BEFORE', ref)))
                    added_base_refs = []
                j += 1
                if j >= len(common_bases):
                    break
                else:
                    continue

            # Base has been inserted at position j
            added_base_refs.append(so.ObjectRef(name=base))
            added_set.add(base)

    # Finally, add all remaining bases to the end of the list
    tail_bases = added_base_refs + [
        so.ObjectRef(name=b) for b in new_bases
        if b not in added_set and b not in common_bases
    ]

    if tail_bases:
        added_bases.append((tail_bases, 'LAST'))

    return tuple(removed_bases), tuple(added_bases)


class AlterInherit(sd.Command):
    astnode = qlast.AlterAddInherit, qlast.AlterDropInherit

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        # The base changes are handled by AlterNamedObject
        return None


class CreateInheritingObject(named.CreateNamedObject, InheritingObjectCommand):
    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        bases = cls._classbases_from_ast(schema, astnode, context)
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
    def _classbases_from_ast(cls, schema, astnode, context):
        classname = cls._classname_from_ast(schema, astnode, context)

        modaliases = context.modaliases

        bases = so.ObjectList.create(
            schema,
            [utils.ast_to_typeref(b, modaliases=modaliases, schema=schema)
             for b in getattr(astnode, 'bases', None) or []]
        )

        mcls = cls.get_schema_metaclass()
        if not bases and classname not in mcls.get_root_classes():
            default_base = mcls.get_default_base_name()

            if default_base is not None and classname != default_base:
                default_base = schema.get(default_base)
                bases = so.ObjectList.create(
                    schema,
                    [utils.reduce_to_typeref(schema, default_base)])

        return bases


class AlterInheritingObject(named.AlterNamedObject, InheritingObjectCommand):
    def _alter_begin(self, schema, context, scls):
        schema = super()._alter_begin(schema, context, scls)

        for op in self.get_subcommands(type=RebaseNamedObject):
            schema, _ = op.apply(schema, context)

        schema = scls.acquire_ancestor_inheritance(schema)

        return schema


class DeleteInheritingObject(named.DeleteNamedObject, InheritingObjectCommand):
    pass


class RebaseNamedObject(named.NamedObjectCommand):
    _delta_action = 'rebase'

    new_base = struct.Field(tuple, default=tuple())
    removed_bases = struct.Field(tuple)
    added_bases = struct.Field(tuple)

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)

    def apply(self, schema, context):
        metaclass = self.get_schema_metaclass()
        scls = schema.get(self.classname, type=metaclass)
        bases = list(scls.get_bases(schema).objects(schema))
        removed_bases = {b.get_name(schema) for b in self.removed_bases}
        existing_bases = set()

        for b in bases:
            if b.get_name(schema) in removed_bases:
                bases.remove(b)
            else:
                existing_bases.add(b.get_name(schema))

        index = {b.get_name(schema): i for i, b in enumerate(bases)}

        for new_bases, pos in self.added_bases:
            if isinstance(pos, tuple):
                pos, ref = pos

            if pos == 'LAST':
                idx = len(bases)
            elif pos == 'FIRST':
                idx = 0
            else:
                idx = index[ref]

            bases[idx:idx] = [schema.get(b.get_name(schema)) for b in new_bases
                              if b.get_name(schema) not in existing_bases]
            index = {b.get_name(schema): i for i, b in enumerate(bases)}

        schema = scls.set_field_value(schema, 'bases', bases)

        return schema, scls


def _merge_mro(schema, obj, mros):
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
            raise schema_error.SchemaError(
                f"Could not find consistent MRO for {obj.get_name(schema)}")

        result.append(candidate)

        for mro in nonempty:
            if mro[0] is candidate:
                del mro[0]

    return result


def compute_mro(schema, obj):
    bases = tuple(obj.get_bases(schema).objects(schema))
    mros = [[obj]]

    for base in bases:
        mros.append(base.compute_mro(schema))

    return _merge_mro(schema, obj, mros)


def create_virtual_parent(schema, children, *,
                          module_name=None, minimize_by=None):
    from . import scalars as s_scalars, objtypes as s_objtypes, sources

    if len(children) == 1:
        return schema, next(iter(children))

    if minimize_by == 'most_generic':
        children = utils.minimize_class_set_by_most_generic(schema, children)
    elif minimize_by == 'least_generic':
        children = utils.minimize_class_set_by_least_generic(schema, children)

    if len(children) == 1:
        return schema, next(iter(children))

    _children = set()
    for t in children:
        if t.get_is_virtual(schema):
            _children.update(t.children(schema))
        else:
            _children.add(t)

    children = list(_children)

    if module_name is None:
        module_name = children[0].get_name(schema).module

    name = sources.Source.gen_virt_parent_name(
        (t.get_name(schema) for t in children),
        module=module_name)

    target = schema.get(name, default=None)

    if target:
        return schema, target

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
        target = utils.get_class_nearest_common_ancestor(schema, children)
        if target is None:
            raise schema_error.SchemaError(
                'cannot set multiple scalar children for a link')
    else:
        base = schema.get(s_objtypes.ObjectType.get_default_base_name())
        schema, target = \
            s_objtypes.ObjectType.create_in_schema_with_inheritance(
                schema,
                name=name, is_abstract=True,
                is_virtual=True, bases=[base]
            )
        schema = target.set_field_value(
            schema, '_virtual_children',
            so.ObjectList.create(schema, children))

    return schema, target


class InheritingObject(derivable.DerivableObject):
    bases = so.SchemaField(
        so.ObjectList,
        default=so.ObjectList,
        coerce=True, inheritable=False, compcoef=0.714)

    mro = so.SchemaField(
        so.ObjectList,
        coerce=True, default=None, hashable=False)

    _virtual_children = so.SchemaField(
        so.ObjectList,
        coerce=True, default=None)

    is_abstract = so.SchemaField(
        bool,
        default=False,
        inheritable=False, compcoef=0.909)

    is_derived = so.SchemaField(
        bool,
        default=False, compcoef=0.909)

    derived_from = so.SchemaField(
        so.NamedObject,
        default=None, compcoef=0.909, inheritable=False)

    is_final = so.SchemaField(
        bool,
        default=False, compcoef=0.909)

    is_virtual = so.SchemaField(
        bool,
        default=False, compcoef=0.5)

    @classmethod
    def delta(cls, old, new, *, context, old_schema, new_schema):
        if context is None:
            context = so.ComparisonContext()

        with context(old, new):
            delta = super().delta(old, new, context=context,
                                  old_schema=old_schema,
                                  new_schema=new_schema)

            if old and new:
                rebase = sd.ObjectCommandMeta.get_command_class(
                    RebaseNamedObject, type(new))

                old_base_names = old.get_base_names(old_schema)
                new_base_names = new.get_base_names(new_schema)

                if old_base_names != new_base_names and rebase is not None:
                    removed, added = delta_bases(
                        old_base_names, new_base_names)

                    delta.add(rebase(
                        classname=new.get_name(new_schema),
                        metaclass=type(new),
                        removed_bases=removed,
                        added_bases=added,
                        new_base=tuple(new_base_names)))

        return delta

    def init_derived(self, schema, source, *qualifiers, as_copy,
                     merge_bases=None, mark_derived=False,
                     replace_original=None, attrs=None, dctx=None,
                     name=None, **kwargs):
        if name is None:
            derived_name = self.get_derived_name(
                schema, source, *qualifiers, mark_derived=mark_derived)
        else:
            derived_name = name

        if as_copy:
            schema, derived = super().init_derived(
                schema, source, *qualifiers, as_copy=True,
                merge_bases=merge_bases, mark_derived=mark_derived,
                attrs=attrs, dctx=dctx, name=name,
                replace_original=replace_original, **kwargs)

        else:
            derived_attrs = {}

            if attrs is not None:
                derived_attrs.update(attrs)

            if not derived_attrs.get('bases'):
                derived_attrs['bases'] = [self]

            derived_attrs.pop('name', None)

            existing_derived = schema.get(derived_name, default=None)
            if existing_derived is not None and replace_original is not None:
                schema = schema.mark_as_garbage(existing_derived)

            schema, derived = type(self).create_in_schema(
                schema, name=derived_name, **derived_attrs)

        return schema, derived

    def get_base_names(self, schema):
        return self.get_bases(schema).names(schema)

    def get_topmost_concrete_base(self, schema):
        # Get the topmost non-abstract base.
        for ancestor in reversed(self.compute_mro(schema)):
            if not ancestor.get_is_abstract(schema):
                return ancestor

        raise ValueError(
            f'{self.get_name(schema)} has no non-abstract ancestors')

    def compute_mro(self, schema):
        return compute_mro(schema, self)

    def _issubclass(self, schema, parent):
        my_vchildren = self.get__virtual_children(schema)

        if my_vchildren is None:
            mro = self.compute_mro(schema)
            mro = {o.id for o in mro}

            if parent.id in mro:
                return True
            elif isinstance(parent, InheritingObject):
                vchildren = parent.get__virtual_children(schema)
                if vchildren:
                    return bool(
                        {o.id for o in vchildren.objects(schema)} & mro)
                else:
                    return False
            else:
                return False
        else:
            return all(c._issubclass(schema, parent)
                       for c in my_vchildren.objects(schema))

    def issubclass(self, schema, parent):
        if isinstance(parent, tuple):
            return any(self.issubclass(schema, p) for p in parent)
        else:
            if parent.is_type() and parent.is_any():
                return True
            else:
                return self._issubclass(schema, parent)

    def descendants(self, schema):
        return schema._get_descendants(self)

    def children(self, schema):
        return schema._get_descendants(self, max_depth=0)

    def acquire_ancestor_inheritance(self, schema, bases=None, *, dctx=None):
        if bases is None:
            bases = self.get_bases(schema).objects(schema)

        schema = self.merge(*bases, schema=schema, dctx=dctx)
        return schema

    def update_descendants(self, schema, dctx=None):
        for child in self.children(schema):
            schema = child.acquire_ancestor_inheritance(schema, dctx=dctx)
            schema = child.update_descendants(schema, dctx=dctx)
        return schema

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        schema = super().finalize(
            schema, bases=bases, apply_defaults=apply_defaults,
            dctx=dctx)
        schema = self.set_field_value(
            schema, 'mro', compute_mro(schema, self)[1:])
        schema = self.acquire_ancestor_inheritance(schema, dctx=dctx)
        return schema

    def get_nearest_non_derived_parent(self, schema):
        obj = self
        while obj.get_derived_from(schema) is not None:
            obj = obj.get_derived_from(schema)
        return obj

    @classmethod
    def get_root_classes(cls):
        return tuple()

    @classmethod
    def get_default_base_name(self):
        return None

    @classmethod
    def create_in_schema_with_inheritance(cls, schema, **kwargs):
        schema, o = cls.create_in_schema(schema, **kwargs)
        schema = o.acquire_ancestor_inheritance(schema)
        return schema, o
