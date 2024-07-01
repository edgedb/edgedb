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
from typing import (
    Any,
    Generic,
    Optional,
    Tuple,
    Type,
    Union,
    AbstractSet,
    Iterable,
    Mapping,
    Sequence,
    Dict,
    List,
    cast,
    TYPE_CHECKING,
)

from edb import errors

from edb.common import span as edb_span
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
    def _update_inherited_fields(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        update: Mapping[str, bool],
    ) -> None:
        raise NotImplementedError

    def update_field_status(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().update_field_status(schema, context)
        inherited_status = self.compute_inherited_fields(schema, context)
        self._update_inherited_fields(schema, context, inherited_status)

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
        apply: bool = True,
    ) -> s_schema.Schema:
        from . import referencing as s_referencing

        # HACK: Don't inherit fields if the command comes from
        # expression change propagation. It shouldn't be necessary,
        # and can cause a knock-on bug: when aliases directly refer to
        # another alias, they *incorrectly* have 'expr' marked as an
        # inherited_field, which causes trouble here.
        # Fixing this in 3.x/4.x would require a schema repair, though.
        if self.from_expr_propagation:
            return schema

        mcls = self.get_schema_metaclass()
        scls = self.scls

        is_owned = (
            isinstance(scls, s_referencing.ReferencedObject)
            and scls.get_owned(schema)
        )

        field_names: Iterable[str]
        if fields is not None:
            field_names = set(scls.inheritable_fields()) & set(fields)
        else:
            field_names = set(scls.inheritable_fields())

        inherited_fields = scls.get_inherited_fields(schema)
        inherited_fields_update = {}
        deferred_complex_ops = []

        # Iterate over mcls.get_schema_fields() instead of field_names for
        # determinism reasons, and so earlier declared fields get
        # processed first.
        for field_name, field in mcls.get_schema_fields().items():
            if field_name not in field_names:
                continue

            was_inherited = field_name in inherited_fields
            ignore_local_field = ignore_local or was_inherited

            try:
                result = field.merge_fn(
                    scls,
                    bases,
                    field_name,
                    ignore_local=ignore_local_field,
                    schema=schema,
                )
            except (errors.SchemaDefinitionError, errors.SchemaError) as e:
                if (span := self.get_attribute_span(field_name)):
                    e.set_span(span)
                raise

            if not ignore_local_field:
                ours = scls.get_explicit_field_value(schema, field_name, None)
            else:
                ours = None

            inherited = result is not None and ours is None
            inherited_fields_update[field_name] = inherited

            if (
                (
                    result != ours
                    or inherited
                    or (was_inherited and not is_owned)
                ) or (
                    result is None and ours is None and ignore_local
                )
            ):
                if (
                    inherited
                    and not context.transient_derivation
                ):
                    if isinstance(result, s_expr.Expression):
                        result = self.compile_expr_field(
                            schema, context, field=field, value=result)
                    elif isinstance(result, s_expr.ExpressionDict):
                        compiled = {}
                        for k, v in result.items():
                            if not v.is_compiled():
                                v = self.compile_expr_field(
                                    schema, context, field, v)
                            compiled[k] = v
                        result = compiled
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

        self._update_inherited_fields(
            schema, context, inherited_fields_update)
        if self.has_attribute_value("inherited_fields"):
            schema = self.scls.set_field_value(
                schema,
                "inherited_fields",
                self.get_attribute_value("inherited_fields"),
            )

        # In some cases, self will be applied later
        if apply:
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
                s_referencing.CreateReferencedInheritingObject[
                    s_referencing.ReferencedInheritingObject
                ]
            ],
            qlast.ObjectDDL,
            List[s_referencing.ReferencedInheritingObject],
        ],
    ]:
        from . import referencing as s_referencing

        attr = refdict.attr
        bases = self.scls.get_bases(schema)
        refs: Dict[
            sn.QualName,
            Tuple[
                Type[
                    s_referencing.CreateReferencedInheritingObject[
                        s_referencing.ReferencedInheritingObject
                    ]
                ],
                qlast.ObjectDDL,
                List[s_referencing.ReferencedInheritingObject],
            ],
        ] = {}

        ancestors = set(self.scls.get_ancestors(schema).objects(schema))
        for base in bases.objects(schema) + (self.scls,):
            base_refs: Dict[
                sn.Name,
                s_referencing.ReferencedInheritingObject,
            ] = dict(base.get_field_value(schema, attr).items(schema))

            # Pointers can reference each other if they are computed,
            # and if they are processed in the wrong order,
            # recompiling expressions in inherit_field can break, so
            # we need to sort them by cross refs.
            # Since inherit_fields doesn't recompile expressions
            # in transient derivations, we skip the sorting there.
            if not context.transient_derivation:
                rev_refs = {v: k for k, v in base_refs.items()}
                base_refs = {
                    rev_refs[v]: v
                    for v in sd.sort_by_cross_refs(schema, base_refs.values())
                }

                # HACK: Because of issue #5661, we previously did not always
                # properly discover dependencies on __type__ in computeds.
                # This was fixed, but it may persist in existing databases.
                # Currently, expr refs are not compared when diffing schemas,
                # so a schema repair can't fix this. Thus, in addition to
                # actually fixing the bug, we hack around it by forcing
                # __type__ to sort to the front.
                # TODO: Drop this after cherry-picking.
                if (tname := sn.UnqualName('__type__')) in base_refs:
                    base_refs[tname] = base_refs.pop(tname)

            for k, v in reversed(base_refs.items()):
                if not v.should_propagate(schema):
                    continue
                if base == self.scls and not v.get_owned(schema):
                    continue

                mcls = type(v)
                create_cmd = sd.get_object_command_class_or_die(
                    sd.CreateObject, mcls)
                assert issubclass(
                    create_cmd,
                    s_referencing.CreateReferencedInheritingObject,
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
            if not v.get_owned(schema):
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

    def _fixup_inheritance_refdicts(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        # HACK?: Derived object types and pointers are created
        # with inheritance_refdicts={'pointers'}, and typically
        # don't get persisted. However, for globals and aliases,
        # they *do* get persisted, and will be altered if a parent
        # is modified. Make sure those alters are also executed
        # with a restricted inheritance_refdicts or else whether
        # things like constraints are created on derived views
        # will be ordering dependent.
        # TODO: Clean this up--maybe make it driven explicitly by
        # is_derived, always?
        if self.scls.get_is_derived(schema):
            context.current().inheritance_refdicts = {'pointers'}
            context.current().inheritance_merge = True

    def _recompute_inheritance(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        from . import ordering

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

        deleted_refs = {}
        for refdict in mcls.get_refdicts():
            if _needs_refdict(refdict, context):
                schema, deleted = self._reinherit_classref_dict(
                    schema, context, refdict)
                deleted_refs.update(deleted)

        # Finalize the deletes. We need to linearize them, since they might
        # have dependencies between them.
        root = sd.DeltaRoot()
        for fqname, delete_cmd_cls in deleted_refs.items():
            root.add(delete_cmd_cls(classname=fqname))
        root = ordering.linearize_delta(root, schema, schema)

        schema = root.apply(schema, context)
        self.update(root.get_subcommands())

        context.current().enable_recursion = orig_rec

        return schema

    def _reinherit_classref_dict(
        self: InheritingObjectCommand[so.InheritingObjectT],
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> Tuple[s_schema.Schema,
               Dict[sn.Name, Type[sd.ObjectCommand[so.Object]]]]:
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
            if refalter.get_attribute_value('owned'):
                assert isinstance(refalter, sd.QualifiedObjectCommand)
                refnames.add(refalter.classname)

        deleted_refs = self.get_no_longer_inherited_ref_layout(
            schema, context, refdict, refnames)
        group = sd.CommandGroup()

        for create_cmd, astnode, bases in refs.values():
            cmd = create_cmd.as_inherited_ref_cmd(
                schema=schema,
                context=context,
                astnode=astnode,
                bases=bases,
                referrer=scls,
            )

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
                    schema, context, obj, tuple(existing_bases), tuple(bases))
                group.add(cmd2)

        self.add(group)

        return schema, deleted_refs

    def _rebase_ref_cmd(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: s_referencing.ReferencedInheritingObject,
        old_bases: Sequence[so.InheritingObject],
        new_bases: Sequence[so.InheritingObject],
    ) -> tuple[sd.Command, Optional[sd.Command]]:
        from . import referencing as s_referencing

        old_base_names = [b.get_name(schema) for b in old_bases]
        new_base_names = [b.get_name(schema) for b in new_bases]

        removed, added = delta_bases(
            old_base_names,
            new_base_names,
            t=type(scls),
        )

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

        return alter_cmd_root, rebase_cmd

    def _rebase_ref(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: s_referencing.ReferencedInheritingObject,
        old_bases: Sequence[so.InheritingObject],
        new_bases: Sequence[so.InheritingObject],
    ) -> Tuple[s_schema.Schema, sd.Command]:
        alter_cmd_root, _ = self._rebase_ref_cmd(
            schema, context, scls, old_bases, new_bases)

        schema = alter_cmd_root.apply(schema, context)

        return schema, alter_cmd_root

    @classmethod
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> List[so.ObjectShell[so.InheritingObjectT]]:
        modaliases = context.modaliases

        base_refs = []
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
            field in {'abstract'}
            and issubclass(astnode, qlast.CreateObject)
        ):
            return field
        else:
            return super().get_ast_attr_for_field(field, astnode)


BaseDeltaItem_T = Tuple[
    List[so.ObjectShell[so.InheritingObjectT]],
    Union[str, Tuple[str, so.ObjectShell[so.InheritingObjectT]]],
]


BaseDelta_T = Tuple[
    Tuple[so.ObjectShell[so.InheritingObjectT], ...],
    Tuple[BaseDeltaItem_T[so.InheritingObjectT], ...],
]


def delta_bases(
    old_bases: Iterable[sn.Name],
    new_bases: Iterable[sn.Name],
    t: Type[so.InheritingObjectT],
) -> BaseDelta_T[so.InheritingObjectT]:
    dropped = frozenset(old_bases) - frozenset(new_bases)
    removed_bases = [so.ObjectShell(name=b, schemaclass=t) for b in dropped]
    common_bases = [b for b in old_bases if b not in dropped]

    added_bases: List[BaseDeltaItem_T[so.InheritingObjectT]] = []
    j = 0

    added_set = set()
    added_base_refs: List[so.ObjectShell[so.InheritingObjectT]] = []

    if common_bases:
        for base in new_bases:
            if common_bases[j] == base:
                # Found common base, insert the accumulated
                # list of new bases and continue
                if added_base_refs:
                    ref = so.ObjectShell(name=common_bases[j], schemaclass=t)
                    added_bases.append((added_base_refs, ('BEFORE', ref)))
                    added_base_refs = []
                j += 1
                if j >= len(common_bases):
                    break
                else:
                    continue

            # Base has been inserted at position j
            added_base_refs.append(so.ObjectShell(name=base, schemaclass=t))
            added_set.add(base)

    # Finally, add all remaining bases to the end of the list
    tail_bases = added_base_refs + [
        so.ObjectShell(name=b, schemaclass=t) for b in new_bases
        if b not in added_set and b not in common_bases
    ]

    if tail_bases:
        added_bases.append((tail_bases, 'LAST'))

    return tuple(removed_bases), tuple(added_bases)


class AlterInherit(sd.Command, Generic[so.InheritingObjectT]):
    astnode = qlast.AlterAddInherit, qlast.AlterDropInherit

    # We temporarily record information about inheritance alterations
    # here, before converting these into Rebases in AlterObject.  The
    # goal here is to encode the information in the subcommand stream,
    # so the positioning is maintained.
    added_bases = struct.Field(List[Tuple[
        List[so.ObjectShell[so.InheritingObjectT]],
        Optional[Union[str, Tuple[str, so.ObjectShell[so.InheritingObjectT]]]],
    ]])
    dropped_bases = struct.Field(List[so.ObjectShell[so.InheritingObjectT]])

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astcmd: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> Any:
        added_bases = []
        dropped_bases: List[so.ObjectShell[so.InheritingObjectT]] = []

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
            pos: Optional[
                Union[str, Tuple[str, so.ObjectShell[so.InheritingObjectT]]]
            ]
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

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        bases_coll = self.get_resolved_attribute_value(
            'bases', schema=schema, context=context)
        bases = () if bases_coll is None else bases_coll.objects(schema)
        ancestors = so.compute_lineage(schema, bases, self.get_verbosename())
        ancestors_coll = so.ObjectList[so.InheritingObjectT].create(
            schema, ancestors)
        self.set_attribute_value('ancestors', ancestors_coll.as_shell(schema))

        if context.mark_derived:
            self.set_attribute_value('is_derived', True)

        return schema

    def _update_inherited_fields(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        update: Mapping[str, bool],
    ) -> None:
        inherited_fields = {n for n, v in update.items() if v}
        if inherited_fields:
            self.set_attribute_value(
                'inherited_fields', frozenset(inherited_fields))

    def _create_begin(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)

        if not context.canonical:
            if context.inheritance_merge is None or context.inheritance_merge:
                bases_coll = self.get_resolved_attribute_value(
                    'bases', schema=schema, context=context)
                if bases_coll is not None:
                    bases = bases_coll.objects(schema)
                else:
                    bases = ()
                schema = self.inherit_fields(schema, context, bases)

        return schema

    def _create_innards(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> s_schema.Schema:
        if not context.canonical:
            cmd = sd.CommandGroup()
            mcls = self.get_schema_metaclass()

            for refdict in mcls.get_refdicts():
                if _needs_refdict(refdict, context):
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
        spans = [b.sourcectx for b in bases if b.sourcectx is not None]
        if spans:
            span = edb_span.merge_spans(spans)
        else:
            span = None
        cmd.set_attribute_value(
            'bases',
            so.ObjectCollectionShell(bases, collection_type=so.ObjectList),
            span=span,
        )

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
                    if isinstance(node, qlast.BasedOnTuple):
                        node.bases = [
                            qlast.TypeName(maintype=utils.name_to_ast_ref(b))
                            for b in explicit_bases
                        ]
                else:
                    node.commands.append(
                        qlast.AlterAddInherit(
                            bases=[
                                qlast.TypeName(
                                    maintype=utils.name_to_ast_ref(b),
                                )
                                for b in explicit_bases
                            ],
                        )
                    )
            else:
                if isinstance(node, qlast.CreateObject):
                    if isinstance(node, qlast.BasedOnTuple):
                        node.bases = []
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

        for create_cmd, astnode, bases in refs.values():
            cmd = create_cmd.as_inherited_ref_cmd(
                schema=schema,
                context=context,
                astnode=astnode,
                bases=bases,
                referrer=scls,
            )

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
            self._fixup_inheritance_refdicts(schema, context)

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
        descendant_names = [
            d.get_name(schema) for d in scls.ordered_descendants(schema)
        ]

        for descendant_name in descendant_names:
            descendant = schema.get(
                descendant_name, type=so.InheritingObject, default=None
            )
            assert descendant, '.inherit_fields caused a drop of a descendant?'

            d_root_cmd, d_alter_cmd, ctx_stack = descendant.init_delta_branch(
                schema, context, sd.AlterObject)

            d_bases = descendant.get_bases(schema).objects(schema)

            # Copy any special updates over
            if isinstance(self, sd.AlterSpecialObjectField):
                d_alter_cmd.add(self.clone(d_alter_cmd.classname))

            with ctx_stack():
                assert isinstance(d_alter_cmd, InheritingObjectCommand)
                schema = d_alter_cmd.inherit_fields(
                    schema, context, d_bases, fields=props, apply=False
                )

            self.add_caused(d_root_cmd)

    def _update_inherited_fields(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        update: Mapping[str, bool],
    ) -> None:
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
            else:
                self.set_attribute_value(
                    'inherited_fields',
                    None,
                    orig_value=cur_inh_fields,
                )

    # HACK: Recursively propagate the value of is_derived. Use to deal
    # with altering computed pointers that are aliases. We should
    # instead not have those be marked is_derived.
    def _propagate_is_derived_flat(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        val: Optional[bool],
    ) -> None:
        self.set_attribute_value('is_derived', val)
        self._propagate_field_alter(schema, context, self.scls, ('is_derived',))

        mcls = self.get_schema_metaclass()
        for refdict in mcls.get_refdicts():
            attr = refdict.attr
            if not issubclass(refdict.ref_cls, so.InheritingObject):
                continue
            for obj in self.scls.get_field_value(schema, attr).objects(schema):
                cmd = obj.init_delta_command(schema, sd.AlterObject)
                cmd._propagate_is_derived_flat(schema, context, val)
                self.add(cmd)

    def _propagate_is_derived(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        val: Optional[bool],
    ) -> None:
        self._propagate_is_derived_flat(schema, context, val)
        for descendant in self.scls.ordered_descendants(schema):
            d_root_cmd, d_alter_cmd, ctx_stack = descendant.init_delta_branch(
                schema, context, sd.AlterObject)

            with ctx_stack():
                assert isinstance(d_alter_cmd, AlterInheritingObject)
                d_alter_cmd._propagate_is_derived_flat(schema, context, val)

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
                    t=cmd.get_schema_metaclass(),
                )

                rebase = sd.get_object_command_class_or_die(
                    RebaseInheritingObject, cmd.get_schema_metaclass())

                rebase_cmd = rebase(
                    classname=cmd.classname,
                    removed_bases=tuple(),
                    added_bases=added,
                )

                cmd.add(rebase_cmd)

        return cmd


class AlterInheritingObjectFragment(
    AlterInheritingObjectOrFragment[so.InheritingObjectT],
    sd.AlterObjectFragment[so.InheritingObjectT],
):
    pass


class RenameInheritingObject(
    AlterInheritingObjectFragment[so.InheritingObjectT],
    sd.RenameObject[so.InheritingObjectT],
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

    EXTRA_INHERITED_FIELDS: set[str] = set()

    def __repr__(self) -> str:
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)

    def get_verb(self) -> str:
        # FIXME: We just say 'alter' because it is currently somewhat
        # inconsistent whether an object rebase on its own will get
        # placed in its own alter command or whether it will share one
        # with all the associated rebases of pointers.  Ideally we'd
        # say 'alter base types of', but with the current machinery it
        # would still usually say 'alter', so just always do that.
        return 'alter'

    def _alter_finalize(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> s_schema.Schema:
        schema = super()._alter_finalize(schema, context)

        if not context.canonical:
            schema = self._recompute_inheritance(schema, context)
            if context.enable_recursion:
                for descendant in self.scls.ordered_descendants(schema):
                    d_root_cmd, d_alter_cmd, ctx_stack = (
                        descendant.init_delta_branch(
                            schema, context, sd.AlterObject))
                    assert isinstance(d_alter_cmd, InheritingObjectCommand)
                    with ctx_stack():
                        d_alter_cmd._fixup_inheritance_refdicts(
                            schema, context)
                        schema = d_alter_cmd._recompute_inheritance(
                            schema, context)
                    self.add_caused(d_root_cmd)

        return schema

    def compute_inherited_fields(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Dict[str, bool]:
        result = super().compute_inherited_fields(schema, context)

        # When things like indexes and constraints that use
        # ddl_identity to define their identity are inherited, the
        # child should inherit all of those fields, even if the object
        # is owned in the child.
        # Make this happen when rebasing.
        mcls = self.get_schema_metaclass()
        new_bases = self.get_attribute_value('bases').objects(schema)

        inherit = new_bases and not new_bases[0].get_abstract(schema)

        fields = {
            field.name for field in mcls.get_fields().values()
            if field.ddl_identity and field.inheritable
        }
        fields.update(self.EXTRA_INHERITED_FIELDS)

        for field in fields:
            if (
                inherit
                and field not in result
                and bool(
                    new_bases[0].get_explicit_field_value(schema, field, None)
                )
            ):
                result[field] = True

        return result

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)

        orig_bases = self.scls.get_bases(schema)
        new_bases = self._compute_new_bases(schema, context, orig_bases)
        self.set_attribute_value(
            'bases',
            so.ObjectList[so.InheritingObjectT].create(schema, new_bases),
            orig_value=orig_bases,
        )

        return schema

    def _compute_new_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        orig_bases: so.ObjectList[so.InheritingObjectT],
    ) -> List[so.InheritingObjectT]:
        mcls = self.get_schema_metaclass()
        default_base_name = mcls.get_default_base_name()
        bases = list(orig_bases.objects(schema))
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

            if not pos or pos == 'LAST':
                idx = len(bases)
            elif pos == 'FIRST':
                idx = 0
            else:
                idx = index[ref.name]

            bases[idx:idx] = [
                self.get_object(
                    schema, context, name=b.name, sourcectx=b.sourcectx)
                for b in new_bases if b.name not in existing_bases
            ]
            index = {b.get_name(schema): i for i, b in enumerate(bases)}

        if not bases and default_base:
            bases = [default_base]

        return bases

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
                    bases=[
                        cast(qlast.TypeName, utils.typeref_to_ast(schema, b))
                        for b in dropped
                    ],
                )
            )

        for bases, pos in self.added_bases:
            bases = self._get_bases_for_ast(schema, context, bases)
            if not bases:
                continue

            if isinstance(pos, tuple):
                typ = utils.typeref_to_ast(schema, pos[1])
                assert isinstance(typ, qlast.TypeName)

                assert isinstance(typ.maintype, qlast.ObjectRef)
                pos_node = qlast.Position(
                    position=pos[0],
                    ref=typ.maintype,
                )

            else:
                pos_node = qlast.Position(position=pos)

            parent_node.commands.append(
                qlast.AlterAddInherit(
                    bases=[
                        cast(qlast.TypeName, utils.typeref_to_ast(schema, b))
                        for b in bases
                    ],
                    position=pos_node,
                )
            )

        return None

    def _get_bases_for_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Tuple[so.ObjectShell[so.InheritingObjectT], ...],
    ) -> Tuple[so.ObjectShell[so.InheritingObjectT], ...]:
        mcls = self.get_schema_metaclass()
        roots = set(mcls.get_root_classes())
        return tuple(b for b in bases if b.name not in roots)


def _needs_refdict(refdict: so.RefDict, context: sd.CommandContext) -> bool:
    inheritance_refdicts = context.inheritance_refdicts
    return (
        inheritance_refdicts is None
        or refdict.attr in inheritance_refdicts
    ) and (context.inheritance_merge is None or context.inheritance_merge)
