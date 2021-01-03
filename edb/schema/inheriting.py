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

from edb import errors

from edb.common import struct
from edb.edgeql import ast as qlast
from edb.schema import schema as s_schema

from . import delta as sd
from . import expr as s_expr
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
        cur_inh_fields = self.scls.get_inherited_fields(schema)
        inh_fields = set(cur_inh_fields)
        for fn, inherited in update.items():
            if inherited:
                inh_fields.add(fn)
            else:
                inh_fields.discard(fn)

        if cur_inh_fields != inh_fields:
            if inh_fields:
                self.set_attribute_value(
                    'inherited_fields',
                    frozenset(inh_fields),
                    orig_value=cur_inh_fields,
                )
                schema = self.scls.set_field_value(
                    schema, 'inherited_fields', inh_fields)
            else:
                self.set_attribute_value(
                    'inherited_fields',
                    None,
                    orig_value=cur_inh_fields,
                )
                schema = self.scls.set_field_value(
                    schema, 'inherited_fields', None)

        return schema

    def compute_inherited_fields(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Dict[str, bool]:
        result = {}
        mcls = self.get_schema_metaclass()
        for op in self.get_subcommands(type=sd.AlterObjectProperty):
            field = mcls.get_field(op.property)
            if field.inheritable and not field.ephemeral:
                result[op.property] = op.new_inherited

        return result

    def inherit_fields(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Tuple[so.Object, ...],
        *,
        fields: Optional[Iterable[str]] = None,
        ignore_local: bool = False,
    ) -> s_schema.Schema:
        mcls = self.get_schema_metaclass()
        scls = self.scls

        field_names: Iterable[str]
        if fields is not None:
            field_names = set(scls.inheritable_fields()) & set(fields)
        else:
            field_names = scls.inheritable_fields()

        inherited_fields = scls.get_inherited_fields(schema)
        inherited_fields_update = {}
        deferred_complex_ops = []

        for field_name in field_names:
            ignore_local_field = ignore_local or field_name in inherited_fields
            field = mcls.get_field(field_name)

            try:
                result = field.merge_fn(
                    scls,
                    bases,
                    field_name,
                    ignore_local=ignore_local_field,
                    schema=schema,
                )
            except errors.SchemaDefinitionError as e:
                if (srcctx := self.get_attribute_source_context(field_name)):
                    e.set_source_context(srcctx)
                raise

            if not ignore_local_field:
                ours = scls.get_explicit_field_value(schema, field_name, None)
            else:
                ours = None

            inherited = result is not None and ours is None
            inherited_fields_update[field_name] = inherited

            if (
                (
                    (result is not None or ours is not None)
                    and (result != ours or inherited)
                ) or (
                    result is None and ours is None and ignore_local
                )
            ):
                if (
                    inherited
                    and not context.transient_derivation
                    and isinstance(result, s_expr.Expression)
                ):
                    result = self.compile_expr_field(
                        schema, context, field=field, value=result)
                sav = self.set_attribute_value(
                    field_name, result, inherited=inherited)
                if isinstance(sav, sd.AlterObjectProperty):
                    schema = self.scls.set_field_value(
                        schema, field_name, result)
                else:
                    # If this isn't a simple AlterObjectProperty, postpone
                    # its application to _after_ _update_inherited_fields
                    # so that the inherited_fields computation is correct,
                    # as each non-trivial AlterSpecialObjectField operation
                    # updates inherited_fields.
                    deferred_complex_ops.append(sav)

        schema = self._update_inherited_fields(
            schema, context, inherited_fields_update)

        for op in deferred_complex_ops:
            schema = op.apply(schema, context)

        return schema

    def get_inherited_ref_layout(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict
    ) -> Dict[
        sn.QualName,
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
            sn.QualName,
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

        ancestors = set(self.scls.get_ancestors(schema).objects(schema))
        for base in bases.objects(schema) + (self.scls,):
            base_refs: so.ObjectIndexBase[
                s_referencing.ReferencedInheritingObject
            ] = base.get_field_value(schema, attr)

            for k, v in base_refs.items(schema):
                if v.get_is_final(schema):
                    continue
                if base == self.scls and not v.get_is_owned(schema):
                    continue

                mcls = type(v)
                create_cmd = sd.get_object_command_class_or_die(
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
                    refs[fqname] = (create_cmd, astnode, [])

                objs = refs[fqname][2]
                if base != self.scls:
                    objs.append(v)
                elif not objs:
                    # If we are looking at refs in the base object
                    # itself, look at the bases of the ref. Any bases
                    # that we haven't seen already while looking in
                    # our object bases must be refs to into objects
                    # that have been dropped from our bases.
                    #
                    # To find which bases to keep, we traverse the
                    # base graph looking for objects with referrers in
                    # our new ancestor set.
                    work = list(reversed(v.get_bases(schema).objects(schema)))
                    while work:
                        vbase = work.pop()
                        subj = vbase.get_referrer(schema)
                        if vbase in objs:
                            continue
                        elif subj is None or subj in ancestors:
                            objs.append(vbase)
                        else:
                            work.extend(
                                reversed(
                                    vbase.get_bases(schema).objects(schema)))

        return refs

    def get_no_longer_inherited_ref_layout(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
        present_refs: AbstractSet[sn.QualName],
    ) -> Dict[sn.Name, Type[sd.ObjectCommand[so.Object]]]:
        from . import referencing as s_referencing

        local_refs = self.scls.get_field_value(schema, refdict.attr)
        dropped_refs: Dict[sn.Name, Type[sd.ObjectCommand[so.Object]]] = {}
        for k, v in local_refs.items(schema):
            if not v.get_is_owned(schema):
                mcls = type(v)
                create_cmd = sd.get_object_command_class_or_die(
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
                    delete_cmd = sd.get_object_command_class_or_die(
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
        self.set_attribute_value(
            'ancestors',
            new_ancestors,
            orig_value=scls.get_ancestors(schema),
        )
        schema = scls.set_field_value(schema, 'ancestors', new_ancestors)

        bases = scls.get_bases(schema).objects(schema)
        schema = self.inherit_fields(schema, context, bases)

        for refdict in mcls.get_refdicts():
            schema = self._reinherit_classref_dict(schema, context, refdict)

        context.current().enable_recursion = orig_rec

        return schema

    def _reinherit_classref_dict(
        self: InheritingObjectCommand[so.InheritingObjectT],
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> s_schema.Schema:
        from edb.schema import referencing as s_referencing

        scls = self.scls
        refs = self.get_inherited_ref_layout(schema, context, refdict)
        refnames = set(refs)

        obj_op: InheritingObjectCommand[so.InheritingObjectT]
        if isinstance(self, sd.AlterObjectFragment):
            obj_op = cast(InheritingObjectCommand[so.InheritingObjectT],
                          self.get_parent_op(context))
        else:
            obj_op = self

        for refalter in obj_op.get_subcommands(metaclass=refdict.ref_cls):
            if refalter.get_attribute_value('is_owned'):
                assert isinstance(refalter, sd.QualifiedObjectCommand)
                refnames.add(refalter.classname)

        deleted_refs = self.get_no_longer_inherited_ref_layout(
            schema, context, refdict, refnames)
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
                schema, cmd2 = self._rebase_ref(
                    schema, context, obj, existing_bases, bases)
                group.add(cmd2)

        for fqname, delete_cmd_cls in deleted_refs.items():
            delete_cmd = delete_cmd_cls(classname=fqname)
            group.add(delete_cmd)
            schema = delete_cmd.apply(schema, context)

        self.add(group)

        return schema

    def _rebase_ref(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: s_referencing.ReferencedInheritingObject,
        old_bases: Sequence[so.InheritingObject],
        new_bases: Sequence[so.InheritingObject],
    ) -> Tuple[s_schema.Schema, sd.Command]:
        from . import referencing as s_referencing

        old_base_names = [b.get_name(schema) for b in old_bases]
        new_base_names = [b.get_name(schema) for b in new_bases]

        removed, added = delta_bases(
            old_base_names, new_base_names)

        rebase = sd.get_object_command_class(
            RebaseInheritingObject, type(scls))

        alter_cmd_root, alter_cmd, _ = (
            scls.init_delta_branch(schema, context, sd.AlterObject))
        assert isinstance(alter_cmd, AlterInheritingObject)

        new_bases_coll = so.ObjectList.create(schema, new_bases)
        schema = scls.set_field_value(schema, 'bases', new_bases_coll)
        ancestors = so.compute_ancestors(schema, scls)
        ancestors_coll = so.ObjectList[
            s_referencing.ReferencedInheritingObject].create(schema, ancestors)

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

        schema = alter_cmd_root.apply(schema, context)

        return schema, alter_cmd_root

    @classmethod
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> List[so.ObjectShell]:
        modaliases = context.modaliases

        base_refs: List[so.ObjectShell] = []
        for b in getattr(astnode, 'bases', None) or []:
            obj = utils.ast_to_object_shell(
                b,
                modaliases=modaliases,
                schema=schema,
                metaclass=cls.get_schema_metaclass(),
            )
            base_refs.append(obj)

        classname = cls._classname_from_ast(schema, astnode, context)
        mcls = cls.get_schema_metaclass()
        if not base_refs and classname not in mcls.get_root_classes():
            default_base = mcls.get_default_base_name()

            if default_base is not None and classname != default_base:
                base_refs.append(
                    utils.ast_objref_to_object_shell(
                        utils.name_to_ast_ref(default_base),
                        metaclass=cls.get_schema_metaclass(),
                        schema=schema,
                        modaliases=modaliases,
                    )
                )

        return base_refs

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if (
            field in {'is_abstract', 'is_final'}
            and issubclass(astnode, qlast.CreateObject)
        ):
            return field
        else:
            return super().get_ast_attr_for_field(field, astnode)


BaseDelta_T = Tuple[
    Tuple[so.ObjectShell, ...],
    Tuple[
        Tuple[
            List[so.ObjectShell],
            Union[str, Tuple[str, so.ObjectShell]],
        ],
        ...,
    ],
]


def delta_bases(
    old_bases: Iterable[sn.Name],
    new_bases: Iterable[sn.Name],
) -> BaseDelta_T:
    dropped = frozenset(old_bases) - frozenset(new_bases)
    removed_bases = [so.ObjectShell(name=b) for b in dropped]
    common_bases = [b for b in old_bases if b not in dropped]

    added_bases: List[
        Tuple[
            List[so.ObjectShell],
            Union[str, Tuple[str, so.ObjectShell]],
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

    # We temporarily record information about inheritance alterations
    # here, before converting these into Rebases in AlterObject.  The
    # goal here is to encode the information in the subcommand stream,
    # so the positioning is maintained.
    added_bases = struct.Field(List[Tuple[
        so.ObjectShell,
        Optional[Union[str, Tuple[str, so.ObjectShell]]]]])
    dropped_bases = struct.Field(List[so.ObjectShell])

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astcmd: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> Any:
        added_bases = []
        dropped_bases: List[so.ObjectShell] = []

        parent_op = context.current().op
        assert isinstance(parent_op, sd.ObjectCommand)
        parent_mcls = parent_op.get_schema_metaclass()

        if isinstance(astcmd, qlast.AlterDropInherit):
            dropped_bases.extend(
                utils.ast_to_object_shell(
                    b,
                    metaclass=parent_mcls,
                    modaliases=context.modaliases,
                    schema=schema,
                )
                for b in astcmd.bases
            )

        elif isinstance(astcmd, qlast.AlterAddInherit):
            bases = [
                utils.ast_to_object_shell(
                    b,
                    metaclass=parent_mcls,
                    modaliases=context.modaliases,
                    schema=schema,
                )
                for b in astcmd.bases
            ]

            pos_node = astcmd.position
            pos: Optional[Union[str, Tuple[str, so.ObjectShell]]]
            if pos_node is not None:
                if pos_node.ref is not None:
                    ref = so.ObjectShell(
                        name=utils.ast_ref_to_name(pos_node.ref),
                        schemaclass=parent_mcls,
                    )
                    pos = (pos_node.position, ref)
                else:
                    pos = pos_node.position
            else:
                pos = None

            added_bases.append((bases, pos))

        # AlterInheritingObject will turn sequences of AlterInherit
        # into proper RebaseWhatever commands.
        return AlterInherit(
            added_bases=added_bases, dropped_bases=dropped_bases)


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

            bases_coll = self.get_resolved_attribute_value(
                'bases',
                schema=schema,
                context=context,
            )
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
                schema = self.inherit_fields(schema, context, bases)

        return schema

    def _create_innards(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> s_schema.Schema:
        if not context.canonical:
            cmd = sd.CommandGroup()
            mcls = self.get_schema_metaclass()

            for refdict in mcls.get_refdicts():
                inheritance_refdicts = context.inheritance_refdicts
                if ((inheritance_refdicts is None
                        or refdict.attr in inheritance_refdicts)
                        and (context.inheritance_merge is None
                             or context.inheritance_merge)):
                    cmd.add(self.inherit_classref_dict(
                        schema, context, refdict))

            self.prepend(cmd)

        return super()._create_innards(schema, context)

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
        else:
            super()._apply_field_ast(schema, context, node, op)

    def get_explicit_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Any,
    ) -> List[sn.Name]:

        mcls = self.get_schema_metaclass()
        default_base = mcls.get_default_base_name()
        base_names: List[sn.Name]

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
                    not isinstance(b, sn.QualName)
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


class AlterInheritingObjectOrFragment(
    InheritingObjectCommand[so.InheritingObjectT],
    sd.AlterObjectOrFragment[so.InheritingObjectT],
):

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
                bases = scls.get_bases(schema).objects(schema)
                schema = self.inherit_fields(
                    schema,
                    context,
                    bases,
                    fields=props,
                )
                if context.enable_recursion:
                    self._propagate_field_alter(schema, context, scls, props)

        return schema

    def _propagate_field_alter(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: so.InheritingObject,
        props: Tuple[str, ...],
    ) -> None:
        for descendant in scls.ordered_descendants(schema):
            d_root_cmd, d_alter_cmd, ctx_stack = descendant.init_delta_branch(
                schema, context, sd.AlterObject)

            d_bases = descendant.get_bases(schema).objects(schema)

            with ctx_stack():
                assert isinstance(d_alter_cmd, InheritingObjectCommand)
                schema = d_alter_cmd.inherit_fields(
                    schema, context, d_bases, fields=props)

            self.add(d_root_cmd)


class AlterInheritingObject(
    AlterInheritingObjectOrFragment[so.InheritingObjectT],
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

        # Collect sequences of AlterInherit commands and transform them
        # into real RebaseWhatever commands.
        added_bases = []
        dropped_bases = []
        subcmds = cmd.get_subcommands()
        for i, sub in enumerate(subcmds):
            if not isinstance(sub, AlterInherit):
                continue

            dropped_bases.extend(sub.dropped_bases)
            added_bases.extend(sub.added_bases)

            if (
                i + 1 < len(subcmds)
                and isinstance(subcmds[i + 1], AlterInherit)
            ):
                cmd.discard(sub)
                continue

            # The next command is not an AlterInherit, so it's time to
            # combine what we've seen and turn it into a rebase.

            parent_class = cmd.get_schema_metaclass()
            rebase_class = sd.get_object_command_class_or_die(
                RebaseInheritingObject, parent_class)

            cmd.replace(
                sub,
                rebase_class(
                    metaclass=parent_class,
                    classname=cmd.classname,
                    removed_bases=tuple(dropped_bases),
                    added_bases=tuple(added_bases)
                )
            )

            added_bases.clear()
            dropped_bases.clear()

        # XXX: I am not totally sure when this will come up?
        if getattr(astnode, 'bases', None):
            bases = cls._classbases_from_ast(schema, astnode, context)
            if bases is not None:
                _, added = delta_bases(
                    [],
                    [b.get_name(schema) for b in bases],
                )

                rebase = sd.get_object_command_class_or_die(
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


class AlterInheritingObjectFragment(
    AlterInheritingObjectOrFragment[so.InheritingObjectT],
    sd.AlterObjectFragment[so.InheritingObjectT],
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

        if not context.canonical:
            bases = self._apply_base_delta(schema, context, scls)
            self.set_attribute_value(
                'bases',
                bases,
                orig_value=scls.get_bases(schema),
            )
            schema = scls.set_field_value(schema, 'bases', bases)

            schema = self._recompute_inheritance(schema, context)

            if context.enable_recursion:
                for descendant in scls.ordered_descendants(schema):
                    d_root_cmd, d_alter_cmd, ctx_stack = (
                        descendant.init_delta_branch(
                            schema, context, sd.AlterObject))
                    assert isinstance(d_alter_cmd, InheritingObjectCommand)
                    with ctx_stack():
                        schema = d_alter_cmd._recompute_inheritance(
                            schema, context)
                    self.add(d_root_cmd)

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

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        assert parent_node is not None

        dropped = self._get_bases_for_ast(schema, context, self.removed_bases)

        if dropped:
            parent_node.commands.append(
                qlast.AlterDropInherit(
                    bases=[utils.typeref_to_ast(schema, b) for b in dropped],
                )
            )

        for bases, pos in self.added_bases:
            bases = self._get_bases_for_ast(schema, context, bases)
            if not bases:
                continue

            if isinstance(pos, tuple):
                pos_node = qlast.Position(
                    position=pos[0],
                    ref=utils.typeref_to_ast(schema, pos[1]),
                )
            else:
                pos_node = qlast.Position(position=pos)

            parent_node.commands.append(
                qlast.AlterAddInherit(
                    bases=[utils.typeref_to_ast(schema, b) for b in bases],
                    position=pos_node,
                )
            )

        return None

    def _get_bases_for_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Tuple[so.ObjectShell, ...],
    ) -> Tuple[so.ObjectShell, ...]:
        mcls = self.get_schema_metaclass()
        roots = set(mcls.get_root_classes())
        return tuple(b for b in bases if b.name not in roots)
