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


from __future__ import annotations

from edb.common import struct
from edb.edgeql import ast as qlast

from edb import errors

from . import delta as sd
from . import derivable
from . import name as sn
from . import objects as so
from . import utils


class InheritingObjectCommand(sd.ObjectCommand):

    def _create_begin(self, schema, context):
        schema = super()._create_begin(schema, context)

        if not context.canonical:
            schema = self._update_inherited_fields(schema, context)

        return schema

    def _alter_begin(self, schema, context, scls):
        schema = super()._alter_begin(schema, context, scls)

        if not context.canonical:
            schema = self._update_inherited_fields(schema, context)

        return schema

    def _update_inherited_fields(self, schema, context):
        current_inh_fields = self.scls.get_inherited_fields(schema)
        new_inh_fields = self.compute_inherited_fields(schema, context)
        inherited_fields = current_inh_fields.update(new_inh_fields)
        self.set_attribute_value('inherited_fields', inherited_fields)
        schema = self.scls.set_field_value(
            schema, 'inherited_fields', inherited_fields)
        return schema

    def inherit_fields(self, schema, context, scls, bases, *, fields=None):
        mcls = self.get_schema_metaclass()

        if fields is not None:
            field_names = set(scls.inheritable_fields()) & set(fields)
        else:
            field_names = scls.inheritable_fields()

        inherited_fields = scls.get_inherited_fields(schema)
        inherited_fields_update = {}

        for field_name in field_names:
            field = mcls.get_field(field_name)
            result = field.merge_fn(scls, bases, field_name, schema=schema)

            if not inherited_fields.get(field_name):
                ours = scls.get_explicit_field_value(schema, field_name, None)
            else:
                ours = None

            inherited = result is not None and ours is None
            inherited_fields_update[field_name] = inherited

            if ((result is not None or ours is not None)
                    and result != ours):
                schema = scls.set_field_value(schema, field_name, result)
                self.set_attribute_value(field_name, result,
                                         inherited=inherited)

        inherited_fields = inherited_fields.update(inherited_fields_update)

        schema = scls.set_field_value(
            schema,
            'inherited_fields',
            inherited_fields,
        )

        self.set_attribute_value('inherited_fields', inherited_fields)

        return schema

    def get_inherited_ref_layout(self, schema, context, refdict):
        attr = refdict.attr
        bases = self.scls.get_bases(schema)
        refs = {}

        for base in bases.objects(schema):
            base_refs = base.get_field_value(schema, attr)
            for k, v in base_refs.items(schema):
                if v.get_is_final(schema):
                    continue

                mcls = type(v)
                create_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.CreateObject, mcls)

                astnode = create_cmd.as_inherited_ref_ast(
                    schema, context, k, v)

                fqname = create_cmd._classname_from_ast(
                    schema, astnode, context)

                if fqname not in refs:
                    refs[fqname] = (create_cmd, astnode, [v])
                else:
                    refs[fqname][2].append(v)

        return refs

    def get_no_longer_inherited_ref_layout(self, schema, context, refdict,
                                           present_refs):

        local_refs = self.scls.get_field_value(schema, refdict.attr)
        dropped_refs = {}
        for k, v in local_refs.items(schema):
            if not v.get_is_local(schema):
                mcls = type(v)
                create_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.CreateObject, mcls)

                astnode = create_cmd.as_inherited_ref_ast(
                    schema, context, k, v)

                fqname = create_cmd._classname_from_ast(
                    schema, astnode, context)

                if fqname not in present_refs:
                    delete_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                        sd.DeleteObject, mcls)
                    dropped_refs[fqname] = delete_cmd

        return dropped_refs

    def _recompute_inheritance(self, schema, context):
        scls = self.scls
        mcls = type(scls)

        orig_rec = context.current().enable_recursion
        context.current().enable_recursion = False

        new_ancestors = so.compute_ancestors(schema, scls)
        new_ancestors = so.ObjectList.create(schema, new_ancestors)
        schema = scls.set_field_value(schema, 'ancestors', new_ancestors)
        self.set_attribute_value('ancestors', new_ancestors)

        bases = scls.get_bases(schema).objects(schema)
        self.inherit_fields(schema, context, scls, bases)

        for refdict in mcls.get_refdicts():
            schema = self._reinherit_classref_dict(schema, context, refdict)

        context.current().enable_recursion = orig_rec

        return schema

    def _reinherit_classref_dict(self, schema, context, refdict):
        scls = self.scls
        refs = self.get_inherited_ref_layout(schema, context, refdict)
        deleted_refs = self.get_no_longer_inherited_ref_layout(
            schema, context, refdict, refs)
        group = sd.CommandGroup()

        for create_cmd, astnode, bases in refs.values():
            cmd = create_cmd.as_inherited_ref_cmd(
                schema, context, astnode, bases)

            obj = schema.get(cmd.classname, default=None)
            if obj is None:
                cmd.set_attribute_value(
                    refdict.backref_attr,
                    so.ObjectRef(name=scls.get_name(schema)),
                )

                group.add(cmd)
                schema, _ = cmd.apply(schema, context)
            else:
                existing_bases = obj.get_implicit_bases(schema)
                schema, cmd = self._rebase_ref(
                    schema, context, obj, existing_bases, bases)
                group.add(cmd)
                schema, _ = cmd.apply(schema, context)

        for fqname, delete_cmd in deleted_refs.items():
            cmd = delete_cmd(classname=fqname)
            group.add(cmd)
            schema, _ = cmd.apply(schema, context)

        self.add(group)

        return schema

    def _rebase_ref(self, schema, context, scls, old_bases, new_bases):
        old_base_names = [b.get_name(schema) for b in old_bases]
        new_base_names = [b.get_name(schema) for b in new_bases]

        removed, added = delta_bases(
            old_base_names, new_base_names)

        rebase = sd.ObjectCommandMeta.get_command_class(
            RebaseInheritingObject, type(scls))

        alter = sd.ObjectCommandMeta.get_command_class(
            sd.AlterObject, type(scls))

        new_bases_coll = so.ObjectList.create(schema, new_bases)
        schema = scls.set_field_value(schema, 'bases', new_bases_coll)
        ancestors = so.compute_ancestors(schema, scls)
        ancestors_coll = so.ObjectList.create(schema, ancestors)

        alter_cmd = alter(
            classname=scls.get_name(schema),
            metaclass=type(scls),
        )

        if rebase is not None:
            rebase_cmd = rebase(
                classname=scls.get_name(schema),
                metaclass=type(scls),
                removed_bases=removed,
                added_bases=added,
            )

            rebase_cmd.set_attribute_value(
                'bases',
                new_bases_coll,
            )

            rebase_cmd.set_attribute_value(
                'ancestors',
                ancestors_coll,
            )

            alter_cmd.add(rebase_cmd)

        alter_cmd.set_attribute_value(
            'bases',
            new_bases_coll,
        )

        alter_cmd.set_attribute_value(
            'ancestors',
            ancestors_coll,
        )

        return schema, alter_cmd

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

    def _apply_rebase_ast(self, context, node, op):
        rebase = next(iter(self.get_subcommands(type=RebaseInheritingObject)))

        dropped = rebase.removed_bases
        added = rebase.added_bases

        if dropped:
            node.commands.append(
                qlast.AlterDropInherit(
                    bases=[
                        qlast.ObjectRef(
                            module=b.classname.module,
                            name=b.classname.name
                        )
                        for b in dropped
                    ]
                )
            )

        for bases, pos in added:
            if isinstance(pos, tuple):
                pos_node = qlast.Position(
                    position=pos[0],
                    ref=qlast.ObjectRef(
                        module=pos[1].classname.module,
                        name=pos[1].classname.name))
            else:
                pos_node = qlast.Position(position=pos)

            node.commands.append(
                qlast.AlterAddInherit(
                    bases=[
                        qlast.ObjectRef(
                            module=b.classname.module,
                            name=b.classname.name
                        )
                        for b in bases
                    ],
                    position=pos_node
                )
            )


def delta_bases(old_bases, new_bases):
    dropped = frozenset(old_bases) - frozenset(new_bases)
    removed_bases = [so.ObjectRef(name=b) for b in dropped]
    common_bases = [b for b in old_bases if b not in dropped]

    added_bases = []

    j = 0

    added_set = set()
    added_base_refs = []

    if common_bases:
        for base in new_bases:
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
                schema, so.compute_ancestors(schema, self.scls))
            schema = self.scls.set_field_value(
                schema, 'ancestors', ancestors)
            self.set_attribute_value('ancestors', ancestors)

            bases_coll = self.get_attribute_value('bases')
            if bases_coll:
                bases = bases_coll.objects(schema)
            else:
                bases = ()

            if context.mark_derived:
                schema = self.scls.update(schema, {
                    'is_derived': True,
                })

                self.set_attribute_value('is_derived', True)

            if context.preserve_path_id and len(bases) == 1:
                base_name = bases[0].get_name(schema)
                schema = self.scls.set_field_value(
                    schema, 'path_id_name', base_name)
                self.set_attribute_value(
                    'path_id_name', base_name)

            if context.inheritance_merge is None or context.inheritance_merge:
                schema = self.inherit_fields(schema, context, self.scls, bases)

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

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'bases':
            mcls = self.get_schema_metaclass()
            default_base = mcls.get_default_base_name()

            if not isinstance(op.new_value, so.ObjectList):
                bases = so.ObjectList.create(schema, op.new_value)
            else:
                bases = op.new_value

            base_names = [
                b for b in bases.names(schema, allow_unresolved=True)
                if b != default_base and sn.shortname_from_fullname(b) == b
            ]

            if base_names:
                if isinstance(node, qlast.CreateObject):
                    node.bases = [
                        qlast.TypeName(
                            maintype=qlast.ObjectRef(
                                name=b.name,
                                module=b.module
                            )
                        )
                        for b in base_names
                    ]
                else:
                    node.commands.append(
                        qlast.AlterAddInherit(
                            bases=[
                                qlast.ObjectRef(
                                    module=b.module,
                                    name=b.name
                                )
                                for b in base_names
                            ],
                        )
                    )

        elif op.property == 'is_abstract':
            node.is_abstract = op.new_value
        elif op.property == 'is_final':
            node.is_final = op.new_value
        else:
            super()._apply_field_ast(schema, context, node, op)

    def inherit_classref_dict(self, schema, context, refdict):
        scls = self.scls
        refs = self.get_inherited_ref_layout(schema, context, refdict)
        group = sd.CommandGroup()

        for create_cmd, astnode, parents in refs.values():
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

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if getattr(astnode, 'bases', None):
            bases = cls._classbases_from_ast(schema, astnode, context)
            if bases is not None:
                _, added = delta_bases(
                    [], [b.get_name(schema) for b in bases.objects(schema)])

                rebase = sd.ObjectCommandMeta.get_command_class(
                    RebaseInheritingObject, cmd.get_schema_metaclass())

                rebase_cmd = rebase(
                    classname=cmd.classname,
                    removed_bases=tuple(),
                    added_bases=added,
                )

                cmd.add(rebase_cmd)

        if getattr(astnode, 'is_final', False):
            cmd.set_attribute_value('is_final', True)

        return cmd

    def _alter_begin(self, schema, context, scls):
        schema = super()._alter_begin(schema, context, scls)

        if not context.canonical:
            schema, props = self._get_field_updates(schema, context)
            if props:
                if context.enable_recursion:
                    self._propagate_field_alter(schema, context, scls, props)
                bases = scls.get_bases(schema).objects(schema)
                self.inherit_fields(schema, context, scls, bases, fields=props)

        return schema

    def _propagate_field_alter(self, schema, context, scls, props):
        alter_cmd = sd.ObjectCommandMeta.get_command_class(
            sd.AlterObject, type(scls))

        for descendant in scls.ordered_descendants(schema):
            descendant_alter = alter_cmd(classname=descendant.get_name(schema))
            descendant_alter.scls = descendant
            with descendant_alter.new_context(schema, context, descendant):
                d_bases = descendant.get_bases(schema).objects(schema)
                schema = descendant_alter.inherit_fields(
                    schema, context, descendant, d_bases, fields=props)

            droot, dcmd = descendant_alter._build_alter_cmd_stack(
                schema, context, descendant
            )

            dcmd.add(descendant_alter)

            self.update(droot.get_subcommands())


class AlterInheritingObjectFragment(InheritingObjectCommand,
                                    sd.AlterObjectFragment):
    pass


class DeleteInheritingObject(InheritingObjectCommand, sd.DeleteObject):

    pass


class RebaseInheritingObject(AlterInheritingObjectFragment):
    _delta_action = 'rebase'

    removed_bases = struct.Field(tuple)
    added_bases = struct.Field(tuple)

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)

    def apply(self, schema, context):
        scls = self.get_object(schema, context)
        self.scls = scls

        schema, props = self._get_field_updates(schema, context)
        schema = scls.update(schema, props)

        for op in self.get_subcommands(type=sd.ObjectCommand):
            schema, _ = op.apply(schema, context)

        if not context.canonical:
            bases = self._apply_base_delta(schema, context, scls)
            schema = scls.set_field_value(schema, 'bases', bases)
            self.set_attribute_value('bases', bases)

            schema = self._recompute_inheritance(schema, context)

            if context.enable_recursion:
                alter_cmd = sd.ObjectCommandMeta.get_command_class(
                    sd.AlterObject, type(scls))

                for descendant in scls.ordered_descendants(schema):
                    descendant_alter = alter_cmd(
                        classname=descendant.get_name(schema))
                    descendant_alter.scls = descendant
                    with descendant_alter.new_context(
                            schema, context, descendant):
                        schema = descendant_alter._recompute_inheritance(
                            schema, context)
                    self.add(descendant_alter)

        return schema, scls

    def _apply_base_delta(self, schema, context, scls):
        bases = list(scls.get_bases(schema).objects(schema))
        default_base_name = scls.get_default_base_name()
        if default_base_name:
            default_base = self.get_object(
                schema, context, name=default_base_name)
            if bases == [default_base]:
                bases = []
        else:
            default_base = None

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

            bases[idx:idx] = [
                self.get_object(schema, context, name=b.get_name(schema))
                for b in new_bases if b.get_name(schema) not in existing_bases
            ]
            index = {b.get_name(schema): i for i, b in enumerate(bases)}

        if not bases and default_base:
            bases = [default_base]

        return so.ObjectList.create(schema, bases)


class InheritingObject(derivable.DerivableObject):

    #: True if the object has an explicit definition and is not
    #: purely inherited.
    is_local = so.SchemaField(
        bool,
        default=False,
        inheritable=False,
        compcoef=0.909)

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

                    rebase_cmd = rebase(
                        classname=new.get_name(new_schema),
                        metaclass=type(new),
                        removed_bases=removed,
                        added_bases=added,
                    )

                    rebase_cmd.set_attribute_value(
                        'bases',
                        new._reduce_refs(
                            new_schema, new.get_bases(new_schema))[0],
                    )

                    rebase_cmd.set_attribute_value(
                        'ancestors',
                        new._reduce_refs(
                            new_schema, new.get_ancestors(new_schema))[0],
                    )

                    delta.add(rebase_cmd)

        return delta

    @classmethod
    def delta_property(cls, schema, scls, delta, fname, value):
        inherited_fields = scls.get_inherited_fields(schema)
        delta.add(sd.AlterObjectProperty(
            property=fname, old_value=None, new_value=value,
            source='inheritance' if inherited_fields.get(fname) else None))

    def inheritable_fields(self):
        for fn, f in self.__class__.get_fields().items():
            if f.inheritable:
                yield fn

    def get_base_names(self, schema):
        return self.get_bases(schema).names(schema)

    def get_topmost_concrete_base(self, schema):
        # Get the topmost non-abstract base.
        for ancestor in reversed(so.compute_lineage(schema, self)):
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

    @classmethod
    def get_root_classes(cls):
        return tuple()

    @classmethod
    def get_default_base_name(self):
        return None
