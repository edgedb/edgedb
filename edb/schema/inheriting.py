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


import contextlib

import immutables as immu

from edb.common import struct
from edb.edgeql import ast as qlast

from edb import errors

from . import delta as sd
from . import derivable
from . import name as sn
from . import objects as so
from . import utils


class InheritingObjectCommand(sd.ObjectCommand):

    def _apply_field_ast(self, schema, context, node, op):
        if op.source == 'inheritance':
            pass
        else:
            return super()._apply_field_ast(schema, context, node, op)

    def _create_begin(self, schema, context):
        schema = super()._create_begin(schema, context)
        inh_map = self._get_inh_map(schema, context)
        schema = self.scls.set_field_value(schema, 'field_inh_map', inh_map)
        return schema

    def _get_inh_map(self, schema, context):
        result = {}

        for op in self.get_subcommands(type=sd.AlterObjectProperty):
            result[op.property] = op.source == 'inheritance'

        return immu.Map(result)

    def inherit_fields(self, schema, context, bases, *, fields=None):
        mcls = self.get_schema_metaclass()
        scls = self.scls

        if fields is not None:
            field_names = set(scls.inheritable_fields()) & set(fields)
        else:
            field_names = scls.inheritable_fields()

        field_inh_map = scls.get_field_inh_map(schema)
        inh_map_update = {}

        for field_name in field_names:
            field = mcls.get_field(field_name)
            result = field.merge_fn(scls, bases, field_name, schema=schema)

            if not field_inh_map.get(field_name):
                ours = scls.get_explicit_field_value(schema, field_name, None)
            else:
                ours = None

            inh_map_update[field_name] = result is not None and ours is None

            if result is not None or ours is not None:
                schema = scls.set_field_value_with_delta(
                    schema, field_name, result, dctx=context,
                    source='inheritance')

        schema = scls.set_field_value(
            schema,
            'field_inh_map',
            field_inh_map.update(inh_map_update),
        )

        return schema


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


class CreateInheritingObject(InheritingObjectCommand, sd.CreateObject):
    def _create_begin(self, schema, context):
        schema = super()._create_begin(schema, context)

        if not context.canonical:
            ancestors = so.ObjectList.create(
                schema, compute_ancestors(schema, self.scls))
            schema = self.scls.set_field_value(
                schema, 'ancestors', ancestors)
            self.set_attribute_value('ancestors', ancestors)

            bases = self.get_attribute_value('bases').objects(schema)

            if context.mark_derived and len(bases) == 1:
                schema = self.scls.update(schema, {
                    'is_derived': True,
                    'derived_from': bases[0],
                })

                self.set_attribute_value('is_derived', True)
                self.set_attribute_value('derived_from', bases[0])

            if context.preserve_path_id and len(bases) == 1:
                base_name = bases[0].get_name(schema)
                schema = self.scls.set_field_value(
                    schema, 'path_id_name', base_name)
                self.set_attribute_value(
                    'path_id_name', base_name)

            if context.inheritance_merge is None or context.inheritance_merge:
                schema = self.inherit_fields(schema, context, bases)

        return schema

    def _create_refs(self, schema, context, scls, refdict):
        if not context.canonical:
            local_refs = list(self.get_subcommands(metaclass=refdict.ref_cls))
            refdict_whitelist = context.inheritance_refdicts
            if ((refdict_whitelist is None
                    or refdict.attr in refdict_whitelist)
                    and (context.inheritance_merge is None
                         or context.inheritance_merge)):
                schema = self.inherit_classref_dict(schema, context, refdict)
            for op in local_refs:
                schema, _ = op.apply(schema, context=context)
            return schema
        else:
            return super()._create_refs(schema, context, scls, refdict)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        bases = cls._classbases_from_ast(schema, astnode, context)
        if bases is not None:
            cmd.set_attribute_value('bases', bases)

        if getattr(astnode, 'is_final', False):
            cmd.set_attribute_value('is_final', True)

        return cmd

    @classmethod
    def _classbases_from_ast(cls, schema, astnode, context):
        modaliases = context.modaliases

        base_refs = [
            utils.ast_to_typeref(b, modaliases=modaliases, schema=schema)
            for b in getattr(astnode, 'bases', None) or []
        ]

        return cls._validate_base_refs(schema, base_refs, astnode, context)

    @classmethod
    def _validate_base_refs(cls, schema, base_refs, astnode, context):
        classname = cls._classname_from_ast(schema, astnode, context)

        bases = so.ObjectList.create(schema, base_refs)

        for base in bases.objects(schema):
            if base.is_type() and base.contains_any(schema):
                base_type_name = base.get_displayname(schema)
                raise errors.SchemaError(
                    f"{base_type_name!r} cannot be a parent type")

        mcls = cls.get_schema_metaclass()
        if not bases and classname not in mcls.get_root_classes():
            default_base = mcls.get_default_base_name()

            if default_base is not None and classname != default_base:
                default_base = schema.get(default_base)
                bases = so.ObjectList.create(
                    schema,
                    [utils.reduce_to_typeref(schema, default_base)])

        return bases

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'bases':
            if not isinstance(op.new_value, so.ObjectList):
                bases = so.ObjectList.create(schema, op.new_value)
            else:
                bases = op.new_value

            base_names = bases.names(schema, allow_unresolved=True)

            node.bases = [
                qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name=b.name,
                        module=b.module
                    )
                )
                for b in base_names
            ]
        elif op.property == 'ancestors':
            pass
        elif op.property == 'is_abstract':
            node.is_abstract = op.new_value
        elif op.property == 'is_final':
            node.is_final = op.new_value
        else:
            super()._apply_field_ast(schema, context, node, op)

    def inherit_classref_dict(self, schema, context, refdict):
        attr = refdict.attr

        scls = self.scls
        bases = scls.get_bases(schema)

        refs = {}

        group = sd.CommandGroup()

        for base in bases.objects(schema):
            base_refs = base.get_field_value(schema, attr)
            for k, v in base_refs.items(schema):
                if v.get_is_final(schema):
                    continue

                mcls = type(v)
                create_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.CreateObject, mcls)

                if sn.Name.is_qualified(k):
                    shortname = sn.shortname_from_fullname(sn.Name(k))
                else:
                    shortname = k

                astnode = create_cmd.as_inherited_ref_ast(
                    schema, context, shortname, v)

                fqname = create_cmd._classname_from_ast(
                    schema, astnode, context)

                if fqname not in refs:
                    refs[fqname] = (create_cmd, astnode, [v])
                else:
                    refs[fqname][2].append(v)

        for fqname, (create_cmd, astnode, parents) in refs.items():
            cmd = create_cmd.as_inherited_ref_cmd(
                schema, context, astnode, parents)

            cmd.set_attribute_value(
                refdict.backref_attr,
                so.ObjectRef(name=scls.get_name(schema)),
            )

            group.add(cmd)
            schema, _ = cmd.apply(schema, context)

        self.prepend(group)

        return schema


class AlterInheritingObject(InheritingObjectCommand, sd.AlterObject):
    def _alter_begin(self, schema, context, scls):
        schema = super()._alter_begin(schema, context, scls)

        for op in self.get_subcommands(type=RebaseInheritingObject):
            schema, _ = op.apply(schema, context)

        if not context.canonical:
            schema, props = self._get_field_updates(schema, context)
            if props:
                for child in scls.children(schema):
                    schema = self._propagate_field_alter(
                        schema, context, child, props)

        return schema

    def _propagate_field_alter(self, schema, context, scls, props):
        return schema


class DeleteInheritingObject(InheritingObjectCommand, sd.DeleteObject):

    pass


class RebaseInheritingObject(sd.ObjectCommand):
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
        scls = self.get_object(schema, context)
        self.scls = scls

        schema, props = self._get_field_updates(schema, context)
        schema = scls.update(schema, props)

        for op in self.get_subcommands(type=sd.ObjectCommand):
            schema, _ = op.apply(schema, context)

        bases = list(scls.get_bases(schema).objects(schema))
        default_base_name = scls.get_default_base_name()
        if default_base_name:
            default_base = schema.get(default_base_name)
            if bases == [default_base]:
                bases = []

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

            if pos is None or pos == 'LAST':
                idx = len(bases)
            elif pos == 'FIRST':
                idx = 0
            else:
                idx = index[ref.get_name(schema)]

            bases[idx:idx] = [self.get_object(schema, context,
                                              name=b.get_name(schema))
                              for b in new_bases
                              if b.get_name(schema) not in existing_bases]
            index = {b.get_name(schema): i for i, b in enumerate(bases)}

        if not bases:
            default = metaclass.get_default_base_name()
            if default:
                bases = [self.get_object(schema, context, name=default)]
            else:
                bases = []

        bases = so.ObjectList.create(schema, bases)
        schema = scls.set_field_value(schema, 'bases', bases)
        new_ancestors = compute_ancestors(schema, scls)
        new_ancestors = so.ObjectList.create(schema, new_ancestors)
        schema = scls.set_field_value(schema, 'ancestors', new_ancestors)

        if not self.has_attribute_value('bases'):
            self.set_attribute_value('bases', bases)

        if not self.has_attribute_value('ancestors'):
            self.set_attribute_value('ancestors', new_ancestors)

        alter_cmd = sd.ObjectCommandMeta.get_command_class(
            sd.AlterObject, metaclass)

        descendants = list(scls.descendants(schema))
        if descendants and not list(self.get_subcommands(type=alter_cmd)):
            for descendant in descendants:
                new_ancestors = compute_ancestors(schema, descendant)
                new_ancestors = so.ObjectList.create(schema, new_ancestors)
                schema = descendant.set_field_value(
                    schema, 'ancestors', new_ancestors)
                alter = alter_cmd(classname=descendant.get_name(schema))
                alter.add(sd.AlterObjectProperty(
                    property='ancestors',
                    new_value=new_ancestors,
                ))
                self.add(alter)

        schema = scls.acquire_ancestor_inheritance(schema)

        return schema, scls


def _merge_lineage(schema, obj, lineage):
    result = []

    while True:
        nonempty = [line for line in lineage if line]
        if not nonempty:
            return result

        for line in nonempty:
            candidate = line[0]
            tails = [m for m in nonempty
                     if id(candidate) in {id(c) for c in m[1:]}]
            if not tails:
                break
        else:
            name = obj.get_verbosename(schema)
            raise errors.SchemaError(
                f"Could not find consistent ancestor order for {name}"
            )

        result.append(candidate)

        for line in nonempty:
            if line[0] is candidate:
                del line[0]

    return result


def compute_lineage(schema, obj):
    bases = tuple(obj.get_bases(schema).objects(schema))
    lineage = [[obj]]

    for base in bases:
        lineage.append(compute_lineage(schema, base))

    return _merge_lineage(schema, obj, lineage)


def compute_ancestors(schema, obj):
    return compute_lineage(schema, obj)[1:]


class InheritingObject(derivable.DerivableObject):
    bases = so.SchemaField(
        so.ObjectList,
        default=so.ObjectList,
        coerce=True, inheritable=False, compcoef=0.714)

    ancestors = so.SchemaField(
        so.ObjectList,
        coerce=True, default=None, hashable=False)

    is_abstract = so.SchemaField(
        bool,
        default=False,
        inheritable=False, compcoef=0.909)

    is_derived = so.SchemaField(
        bool,
        default=False, compcoef=0.909)

    derived_from = so.SchemaField(
        so.Object,
        default=None, compcoef=0.909, inheritable=False)

    is_final = so.SchemaField(
        bool,
        default=False, compcoef=0.909)

    is_local = so.SchemaField(
        bool,
        default=False,
        inheritable=False,
        compcoef=0.909)

    field_inh_map = so.SchemaField(
        immu.Map,
        default=immu.Map(),
        inheritable=False,
        introspectable=False,
        hashable=False,
    )

    @classmethod
    def delta(cls, old, new, *, context=None, old_schema, new_schema):
        if context is None:
            context = so.ComparisonContext()

        with context(old, new):
            delta = super().delta(old, new, context=context,
                                  old_schema=old_schema,
                                  new_schema=new_schema)

            if old and new:
                rebase = sd.ObjectCommandMeta.get_command_class(
                    RebaseInheritingObject, type(new))

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

    @classmethod
    def delta_property(cls, schema, scls, delta, fname, value):
        inh_map = scls.get_field_inh_map(schema)
        delta.add(sd.AlterObjectProperty(
            property=fname, old_value=None, new_value=value,
            source='inheritance' if inh_map.get(fname) else None))

    def inheritable_fields(self):
        for fn, f in self.__class__.get_fields().items():
            if f.inheritable:
                yield fn

    def get_classref_origin(self, schema, name, attr, local_attr, classname,
                            farthest=False):
        assert self.get_field_value(schema, attr).has(schema, name)

        result = None

        if self.get_field_value(schema, local_attr).has(schema, name):
            result = self

        if not result or farthest:
            bases = compute_ancestors(schema, self)

            for c in bases:
                if c.get_field_value(schema, local_attr).has(schema, name):
                    result = c
                    if not farthest:
                        break

        if result is None:
            raise KeyError(
                'could not find {} "{}" origin'.format(classname, name))

        return result

    def derive(self, schema, source,
               *qualifiers,
               mark_derived=False,
               attrs=None, dctx=None,
               derived_name_base=None,
               inheritance_merge=True,
               preserve_path_id=None,
               refdict_whitelist=None,
               name=None, **kwargs):
        if name is None:
            derived_name = self.get_derived_name(
                schema, source, *qualifiers,
                mark_derived=mark_derived,
                derived_name_base=derived_name_base)
        else:
            derived_name = name

        if self.get_name(schema) == derived_name:
            raise errors.SchemaError(
                f'cannot derive {self!r}({derived_name}) from itself')

        derived_attrs = {}

        if attrs is not None:
            derived_attrs.update(attrs)

        derived_attrs['name'] = derived_name
        derived_attrs['bases'] = so.ObjectList.create(schema, [self])

        mcls = type(self)
        referrer_class = type(source)

        if referrer_class != mcls:
            refdict = referrer_class.get_refdict_for_class(mcls)
            reftype = referrer_class.get_field(refdict.attr).type
            refname = reftype.get_key_for_name(schema, derived_name)
            refcoll = source.get_field_value(schema, refdict.attr)
            is_alter = refcoll.has(schema, refname)
        else:
            is_alter = False

        if is_alter:
            cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.AlterObject, type(self))
        else:
            cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.CreateObject, type(self))

        cmd = cmdcls(classname=derived_name)

        for k, v in derived_attrs.items():
            cmd.set_attribute_value(k, v)

        context = sd.CommandContext(
            modaliases={},
            schema=schema,
        )

        with contextlib.ExitStack() as cstack:
            delta, parent_cmd = self._build_derive_context_stack(
                schema, context, cstack, source)

            if not inheritance_merge:
                context.current().inheritance_merge = False

            if refdict_whitelist is not None:
                context.current().inheritance_refdicts = refdict_whitelist

            if mark_derived:
                context.current().mark_derived = True

            if preserve_path_id:
                context.current().preserve_path_id = True

            parent_cmd.add(cmd)
            schema, _ = delta.apply(schema, context)

        derived = schema.get(derived_name)

        return schema, derived

    def delete(self, schema):
        source = self.get_referrer(schema)

        cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.DeleteObject, type(self))

        cmd = cmdcls(classname=self.get_name(schema))

        context = sd.CommandContext(
            modaliases={},
            schema=schema,
            disable_dep_verification=True,
        )

        with contextlib.ExitStack() as cstack:
            delta, parent_cmd = self._build_derive_context_stack(
                schema, context, cstack, source)

            parent_cmd.add(cmd)
            schema, _ = delta.apply(schema, context)

        return schema

    def _build_derive_context_stack(self, schema, context, cstack, source):
        from . import referencing

        delta = sd.DeltaRoot()

        obj = source
        object_stack = []

        if type(self) != type(source):
            object_stack.append(source)

        while obj is not None:
            if isinstance(obj, referencing.ReferencedObject):
                obj = obj.get_referrer(schema)
                object_stack.append(obj)
            else:
                obj = None

        cstack.enter_context(
            context(sd.DeltaRootContext(schema=schema, op=delta))
        )

        cmd = delta
        for obj in reversed(object_stack):
            alter_cmd_cls = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.AlterObject, type(obj))

            alter_cmd = alter_cmd_cls(classname=obj.get_name(schema))
            cmd.add(alter_cmd)
            cmd = alter_cmd

        return delta, cmd

    def get_base_names(self, schema):
        return self.get_bases(schema).names(schema)

    def get_topmost_concrete_base(self, schema):
        # Get the topmost non-abstract base.
        for ancestor in reversed(compute_lineage(schema, self)):
            if not ancestor.get_is_abstract(schema):
                return ancestor

        if not self.get_is_abstract(schema):
            return self

        raise errors.SchemaError(
            f'{self.get_verbosename(schema)} has no non-abstract ancestors')

    def get_base_for_cast(self, schema):
        if self.is_enum(schema):
            # all enums have to use std::anyenum as base type for casts
            return schema.get('std::anyenum')
        else:
            return self.get_topmost_concrete_base(schema)

    def _issubclass(self, schema, parent):
        lineage = compute_lineage(schema, self)
        return parent in lineage

    def issubclass(self, schema, parent):
        if isinstance(parent, tuple):
            return any(self.issubclass(schema, p) for p in parent)
        else:
            if parent.is_type() and parent.is_any():
                return True
            else:
                return self._issubclass(schema, parent)

    def descendants(self, schema):
        return schema.get_descendants(self)

    def children(self, schema):
        return schema.get_children(self)

    def acquire_ancestor_inheritance(self, schema, bases=None, *, dctx=None):
        if bases is None:
            bases = self.get_bases(schema).objects(schema)

        schema = self.set_field_value(
            schema, 'ancestors', compute_ancestors(schema, self))

        for field_name in self.inheritable_fields():
            field = self.__class__.get_field(field_name)
            result = field.merge_fn(self, bases, field_name, schema=schema)
            ours = self.get_explicit_field_value(schema, field_name, None)
            field_inh_map = self.get_field_inh_map(schema)
            schema = self.set_field_value(
                schema,
                'field_inh_map',
                field_inh_map.set(
                    field_name, result is not None and ours is None),
            )

            if result is not None or ours is not None:
                schema = self.set_field_value_with_delta(
                    schema, field_name, result, dctx=dctx,
                    source='inheritance')

        for obj in bases:
            for refdict in self.__class__.get_refdicts():
                # Merge Object references in each registered collection.
                #
                this_coll = self.get_explicit_field_value(
                    schema, refdict.attr, None)

                other_coll = obj.get_explicit_field_value(
                    schema, refdict.attr, None)

                if other_coll is None:
                    continue

                if not other_coll:
                    continue

                if this_coll is None:
                    schema = self.set_field_value(
                        schema, refdict.attr, other_coll)
                else:
                    updates = {v for k, v in other_coll.items(schema)
                               if not this_coll.has(schema, k)}

                    schema, this_coll = this_coll.update(schema, updates)
                    schema = self.set_field_value(
                        schema, refdict.attr, this_coll)

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
