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
from typing import *

from edb.common import struct
from edb.edgeql import ast as qlast
from edb.schema import schema as s_schema

from . import delta as sd
from . import name as sn
from . import objects as so
from . import utils

if TYPE_CHECKING:
    from edb.schema import referencing as s_referencing


class InheritingObjectCommand(sd.ObjectCommand[so.InheritingObjectT]):

    def _create_begin(self,
                      schema: s_schema.Schema,
                      context: sd.CommandContext) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)

        if not context.canonical:
            inh_update = self.compute_inherited_fields(schema, context)
            schema = self._update_inherited_fields(schema, context, inh_update)

        return schema

    def _alter_begin(self,
                     schema: s_schema.Schema,
                     context: sd.CommandContext) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)  # type: ignore

        assert isinstance(schema, s_schema.Schema)
        if not context.canonical:
            inh_update = self.compute_inherited_fields(schema, context)
            schema = self._update_inherited_fields(schema, context, inh_update)

        return schema

    def _update_inherited_fields(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        update: Mapping[str, bool],
    ) -> s_schema.Schema:
        inh_fields = set(self.scls.get_inherited_fields(schema))
        for fn, inherited in update.items():
            if inherited:
                inh_fields.add(fn)
            else:
                inh_fields.discard(fn)
        self.set_attribute_value(
            'inherited_fields', frozenset(inh_fields))
        schema = self.scls.set_field_value(
            schema, 'inherited_fields', inh_fields)
        return schema

    def inherit_fields(self,
                       schema: s_schema.Schema,
                       context: sd.CommandContext,
                       scls: so.InheritingObjectT,
                       bases: Tuple[so.Object, ...],
                       *,
                       fields: Optional[Iterable[str]] = None
                       ) -> s_schema.Schema:
        mcls = self.get_schema_metaclass()

        if fields is not None:
            field_names: Iterable[str] = set(scls.inheritable_fields()) \
                & set(fields)
        else:
            field_names = scls.inheritable_fields()

        inherited_fields = scls.get_inherited_fields(schema)
        inherited_fields_update = {}

        for field_name in field_names:
            field = mcls.get_field(field_name)
            result = field.merge_fn(scls, bases, field_name, schema=schema)

            if field_name not in inherited_fields:
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

        schema = self._update_inherited_fields(
            schema, context, inherited_fields_update)

        return schema

    def get_inherited_ref_layout(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict
    ) -> Dict[
        sn.Name,
        Tuple[
            Type[
                s_referencing.CreateReferencedObject[
                    s_referencing.ReferencedObject
                ]
            ],
            qlast.ObjectDDL,
            List[so.InheritingObject],
        ],
    ]:
        from . import referencing as s_referencing

        attr = refdict.attr
        bases = self.scls.get_bases(schema)
        refs: Dict[
            sn.Name,
            Tuple[
                Type[
                    s_referencing.CreateReferencedObject[
                        s_referencing.ReferencedObject
                    ]
                ],
                qlast.ObjectDDL,
                List[so.InheritingObject],
            ],
        ] = {}

        for base in bases.objects(schema):
            base_refs = base.get_field_value(schema, attr)
            for k, v in base_refs.items(schema):
                if v.get_is_final(schema):
                    continue

                mcls = type(v)
                create_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.CreateObject, mcls)
                assert issubclass(
                    create_cmd,
                    s_referencing.CreateReferencedObject,
                )

                astnode = create_cmd.as_inherited_ref_ast(
                    schema, context, k, v)

                fqname = create_cmd._classname_from_ast(
                    schema, astnode, context)

                if fqname not in refs:
                    refs[fqname] = (create_cmd, astnode, [v])
                else:
                    refs[fqname][2].append(v)

        return refs

    def get_no_longer_inherited_ref_layout(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
        present_refs: Dict[
            sn.Name,
            Tuple[
                Type[
                    s_referencing.CreateReferencedObject[
                        s_referencing.ReferencedObject
                    ]
                ],
                qlast.ObjectDDL,
                List[so.InheritingObject],
            ],
        ],
    ) -> Dict[str, Type[sd.ObjectCommand[so.Object]]]:
        from . import referencing as s_referencing

        local_refs = self.scls.get_field_value(schema, refdict.attr)
        dropped_refs: Dict[str, Type[sd.ObjectCommand[so.Object]]] = {}
        for k, v in local_refs.items(schema):
            if not v.get_is_local(schema):
                mcls = type(v)
                create_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.CreateObject, mcls)
                assert issubclass(
                    create_cmd,
                    s_referencing.CreateReferencedObject,
                )

                astnode = create_cmd.as_inherited_ref_ast(
                    schema, context, k, v)

                fqname = create_cmd._classname_from_ast(
                    schema, astnode, context)

                if fqname not in present_refs:
                    delete_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                        sd.DeleteObject, mcls)
                    dropped_refs[fqname] = delete_cmd

        return dropped_refs

    def _recompute_inheritance(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        scls = self.scls
        mcls = type(scls)

        orig_rec = context.current().enable_recursion
        context.current().enable_recursion = False

        new_ancestors = so.ObjectList[so.InheritingObjectT].create(
            schema,
            so.compute_ancestors(schema, scls),
        )
        schema = scls.set_field_value(schema, 'ancestors', new_ancestors)
        self.set_attribute_value('ancestors', new_ancestors)

        bases = scls.get_bases(schema).objects(schema)
        self.inherit_fields(schema, context, scls, bases)

        for refdict in mcls.get_refdicts():
            schema = self._reinherit_classref_dict(schema, context, refdict)

        context.current().enable_recursion = orig_rec

        return schema

    def _reinherit_classref_dict(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> s_schema.Schema:
        from edb.schema import referencing as s_referencing

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
                cmd.set_attribute_value(refdict.backref_attr, scls)
                group.add(cmd)
                schema = cmd.apply(schema, context)
            else:
                assert isinstance(obj,
                                  s_referencing.ReferencedInheritingObject)
                existing_bases = obj.get_implicit_bases(schema)
                schema, cmd = self._rebase_ref(
                    schema, context, obj, existing_bases, bases)
                group.add(cmd)
                schema = cmd.apply(schema, context)

        for fqname, delete_cmd in deleted_refs.items():
            cmd = delete_cmd(classname=fqname)
            group.add(cmd)
            schema = cmd.apply(schema, context)

        self.add(group)

        return schema

    def _rebase_ref(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: s_referencing.ReferencedInheritingObject,
        old_bases: List[so.InheritingObjectT],
        new_bases: List[so.InheritingObjectT],
    ) -> Tuple[s_schema.Schema, AlterInheritingObject[so.InheritingObjectT]]:
        old_base_names = [b.get_name(schema) for b in old_bases]
        new_base_names = [b.get_name(schema) for b in new_bases]

        removed, added = delta_bases(
            old_base_names, new_base_names)

        rebase = sd.ObjectCommandMeta.get_command_class(
            RebaseInheritingObject, type(scls))

        alter = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.AlterObject, type(scls))
        assert issubclass(alter, AlterInheritingObject)

        new_bases_coll = so.ObjectList[so.InheritingObjectT].create(
            schema, new_bases)
        schema = scls.set_field_value(schema, 'bases', new_bases_coll)
        ancestors = so.compute_ancestors(schema, scls)
        ancestors_coll = so.ObjectList[so.InheritingObjectT].create(
            schema, ancestors)

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
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> so.ObjectList[so.InheritingObjectT]:
        modaliases = context.modaliases

        base_refs: List[so.InheritingObjectT] = []
        for b in getattr(astnode, 'bases', None) or []:
            obj = utils.ast_to_object(
                b,
                modaliases=modaliases,
                schema=schema,
                metaclass=cls.get_schema_metaclass(),
            )
            base_refs.append(cast(so.InheritingObjectT, obj))

        return cls._validate_base_refs(schema, base_refs, astnode, context)

    @classmethod
    def _validate_base_refs(
        cls,
        schema: s_schema.Schema,
        base_refs: Iterable[so.InheritingObjectT],
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> so.ObjectList[so.InheritingObjectT]:
        classname = cls._classname_from_ast(schema, astnode, context)

        bases = so.ObjectList[so.InheritingObjectT].create(schema, base_refs)
        mcls = cls.get_schema_metaclass()
        if not bases and classname not in mcls.get_root_classes():
            default_base = mcls.get_default_base_name()

            if default_base is not None and classname != default_base:
                default_base = schema.get(default_base)
                bases = so.ObjectList[so.InheritingObjectT].create(
                    schema,
                    [default_base],
                )

        return bases

    def _apply_rebase_ast(
        self,
        context: sd.CommandContext,
        node: qlast.ObjectDDL,
        op: Any
    ) -> Any:
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

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        assert isinstance(node, qlast.ObjectDDL)
        if op.property in {'is_abstract', 'is_final'}:
            node.commands.append(
                qlast.SetSpecialField(
                    name=op.property,
                    value=op.new_value
                )
            )
        elif op.property == 'bases':
            self._apply_rebase_ast(context, node, op)
        else:
            super()._apply_field_ast(schema, context, node, op)


def delta_bases(
    old_bases: Iterable[str], new_bases: Iterable[str]
) -> Tuple[
    Tuple[so.ObjectShell, ...],
    Tuple[
        Tuple[
            List[so.ObjectShell],
            Union[str, so.ObjectShell, Tuple[str, so.ObjectShell]],
        ],
        ...,
    ],
]:
    dropped = frozenset(old_bases) - frozenset(new_bases)
    removed_bases = [so.ObjectShell(name=b) for b in dropped]
    common_bases = [b for b in old_bases if b not in dropped]

    added_bases: List[
        Tuple[
            List[so.ObjectShell],
            Union[str, so.ObjectShell, Tuple[str, so.ObjectShell]],
        ]
    ] = []

    j = 0

    added_set = set()
    added_base_refs: List[so.ObjectShell] = []

    if common_bases:
        for base in new_bases:
            if common_bases[j] == base:
                # Found common base, insert the accumulated
                # list of new bases and continue
                if added_base_refs:
                    ref = so.ObjectShell(name=common_bases[j])
                    added_bases.append((added_base_refs, ('BEFORE', ref)))
                    added_base_refs = []
                j += 1
                if j >= len(common_bases):
                    break
                else:
                    continue

            # Base has been inserted at position j
            added_base_refs.append(so.ObjectShell(name=base))
            added_set.add(base)

    # Finally, add all remaining bases to the end of the list
    tail_bases = added_base_refs + [
        so.ObjectShell(name=b) for b in new_bases
        if b not in added_set and b not in common_bases
    ]

    if tail_bases:
        added_bases.append((tail_bases, 'LAST'))

    return tuple(removed_bases), tuple(added_bases)


class AlterInherit(sd.Command):
    astnode = qlast.AlterAddInherit, qlast.AlterDropInherit

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> Any:
        # The base changes are handled by AlterNamedObject
        return None


class CreateInheritingObject(
    InheritingObjectCommand[so.InheritingObjectT],
    sd.CreateObject[so.InheritingObjectT],
):

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)

        if not context.canonical:
            ancestors = so.ObjectList[so.InheritingObjectT].create(
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

    def _create_innards(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> s_schema.Schema:
        if not context.canonical:
            cmd = sd.CommandGroup()
            mcls = self.get_schema_metaclass()

            for refdict in mcls.get_refdicts():
                refdict_whitelist = context.inheritance_refdicts
                if ((refdict_whitelist is None
                        or refdict.attr in refdict_whitelist)
                        and (context.inheritance_merge is None
                             or context.inheritance_merge)):
                    cmd.add(self.inherit_classref_dict(
                        schema, context, refdict))

            self.prepend(cmd)

        result = super()._create_innards(schema, context)
        assert isinstance(result, s_schema.Schema)
        return result

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, qlast.ObjectDDL)
        bases = cls._classbases_from_ast(schema, astnode, context)
        if bases is not None:
            cmd.set_attribute_value('bases', bases)

        if getattr(astnode, 'is_final', False):
            cmd.set_attribute_value('is_final', True)

        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'bases':
            explicit_bases = self.get_explicit_bases(
                schema, context, op.new_value)

            if explicit_bases:
                if isinstance(node, qlast.CreateObject):
                    node.bases = [
                        qlast.TypeName(maintype=utils.name_to_ast_ref(b))
                        for b in explicit_bases
                    ]
                else:
                    node.commands.append(
                        qlast.AlterAddInherit(
                            bases=[
                                utils.name_to_ast_ref(b)
                                for b in explicit_bases
                            ],
                        )
                    )
        elif op.property == 'is_abstract':
            node.is_abstract = op.new_value
        elif op.property == 'is_final':
            node.is_final = op.new_value
        else:
            super()._apply_field_ast(schema, context, node, op)

    def get_explicit_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Any,
    ) -> List[str]:

        mcls = self.get_schema_metaclass()
        default_base = mcls.get_default_base_name()
        base_names: List[str]

        if isinstance(bases, so.ObjectCollectionShell):
            base_names = []
            for b in bases.items:
                assert b.name is not None
                base_names.append(b.name)
        else:
            assert isinstance(bases, so.ObjectList)
            base_names = list(bases.names(schema))

        # Filter out implicit bases
        explicit_bases = [
            b
            for b in base_names
            if (
                b != default_base
                and (
                    not isinstance(b, sn.SchemaName)
                    or sn.shortname_from_fullname(b) == b
                )
            )
        ]

        return explicit_bases

    def inherit_classref_dict(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> sd.CommandGroup:
        scls = self.scls
        refs = self.get_inherited_ref_layout(schema, context, refdict)
        group = sd.CommandGroup()

        for create_cmd, astnode, parents in refs.values():
            cmd = create_cmd.as_inherited_ref_cmd(
                schema, context, astnode, parents)

            cmd.set_attribute_value(refdict.backref_attr, scls)

            group.add(cmd)

        return group


class AlterInheritingObject(
    InheritingObjectCommand[so.InheritingObjectT],
    sd.AlterObject[so.InheritingObjectT],
):

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, AlterInheritingObject)
        assert isinstance(astnode, qlast.ObjectDDL)

        if getattr(astnode, 'bases', None):
            bases = cls._classbases_from_ast(schema, astnode, context)
            if bases is not None:
                _, added = delta_bases(
                    [], [b.get_name(schema) for b in bases.objects(schema)])

                rebase = sd.ObjectCommandMeta.get_command_class_or_die(
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

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        if not context.canonical:
            props = self.enumerate_attributes()
            if props:
                if context.enable_recursion:
                    self._propagate_field_alter(schema, context, scls, props)
                bases = scls.get_bases(schema).objects(schema)
                self.inherit_fields(schema, context, scls, bases, fields=props)

        return schema

    def _propagate_field_alter(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: so.InheritingObject,
        props: Tuple[str, ...],
    ) -> None:
        alter_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.AlterObject, type(scls))
        assert issubclass(alter_cmd, AlterInheritingObject)

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

            self.add(droot)


class AlterInheritingObjectFragment(
    InheritingObjectCommand[so.InheritingObjectT],
    sd.AlterObjectFragment,
):
    pass


class DeleteInheritingObject(
    InheritingObjectCommand[so.InheritingObjectT],
    sd.DeleteObject[so.InheritingObjectT],
):
    pass


class RebaseInheritingObject(
    AlterInheritingObjectFragment[so.InheritingObjectT],
):
    _delta_action = 'rebase'

    removed_bases = struct.Field(tuple)  # type: ignore
    added_bases = struct.Field(tuple)  # type: ignore

    def __repr__(self) -> str:
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        scls = self.scls

        assert isinstance(scls, so.InheritingObject)

        for op in self.get_subcommands(type=sd.ObjectCommand):
            schema = op.apply(schema, context)

        if not context.canonical:
            bases = self._apply_base_delta(schema, context, scls)
            schema = scls.set_field_value(schema, 'bases', bases)
            self.set_attribute_value('bases', bases)

            schema = self._recompute_inheritance(schema, context)

            if context.enable_recursion:
                alter_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.AlterObject, type(scls))
                assert issubclass(alter_cmd, AlterInheritingObject)

                for descendant in scls.ordered_descendants(schema):
                    descendant_alter = alter_cmd(
                        classname=descendant.get_name(schema))
                    descendant_alter.scls = descendant
                    with descendant_alter.new_context(
                            schema, context, descendant):
                        schema = descendant_alter._recompute_inheritance(
                            schema, context)
                    self.add(descendant_alter)

        assert isinstance(scls, so.InheritingObject)

        return schema

    def _apply_base_delta(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: so.InheritingObjectT,
    ) -> so.ObjectList[so.InheritingObjectT]:
        bases = list(scls.get_bases(schema).objects(schema))
        default_base_name = scls.get_default_base_name()
        if default_base_name:
            default_base: Optional[so.InheritingObjectT] = self.get_object(
                schema, context, name=default_base_name)
            if bases == [default_base]:
                bases = []
        else:
            default_base = None

        removed_bases = {b.name for b in self.removed_bases}
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
                idx = index[ref.name]

            bases[idx:idx] = [
                self.get_object(schema, context, name=b.name)
                for b in new_bases if b.name not in existing_bases
            ]
            index = {b.get_name(schema): i for i, b in enumerate(bases)}

        if not bases and default_base:
            bases = [default_base]

        return so.ObjectList[so.InheritingObjectT].create(schema, bases)
