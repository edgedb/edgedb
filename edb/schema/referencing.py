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

import hashlib
from typing import *  # NoQA

from edb.edgeql import ast as qlast

from edb import errors

from . import delta as sd
from . import derivable
from . import inheriting
from . import objects as so
from . import schema as s_schema
from . import name as sn
from . import utils


ReferencedT = TypeVar('ReferencedT', bound='ReferencedObject')


class ReferencedObject(so.Object, derivable.DerivableObjectBase):

    def get_referrer(self, schema):
        return self.get_subject(schema)

    def delete(self, schema):
        cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.DeleteObject, type(self))

        cmd = cmdcls(classname=self.get_name(schema))

        context = sd.CommandContext(
            modaliases={},
            schema=schema,
            disable_dep_verification=True,
        )

        delta, parent_cmd = cmd._build_alter_cmd_stack(
            schema, context, self)

        parent_cmd.add(cmd)
        with context(sd.DeltaRootContext(schema=schema, op=delta)):
            schema, _ = delta.apply(schema, context)

        return schema

    def derive_ref(
        self: ReferencedT,
        schema,
        referrer,
        *qualifiers,
        mark_derived=False,
        attrs=None, dctx=None,
        derived_name_base=None,
        inheritance_merge=True,
        preserve_path_id=None,
        refdict_whitelist=None,
        name=None,
        **kwargs,
    ) -> Tuple[s_schema.Schema, ReferencedT]:
        if name is None:
            derived_name = self.get_derived_name(
                schema, referrer, *qualifiers,
                mark_derived=mark_derived,
                derived_name_base=derived_name_base)
        else:
            derived_name = name

        if self.get_name(schema) == derived_name:
            raise errors.SchemaError(
                f'cannot derive {self!r}({derived_name}) from itself')

        derived_attrs: Dict[str, object] = {}

        if attrs is not None:
            derived_attrs.update(attrs)

        derived_attrs['name'] = derived_name
        derived_attrs['bases'] = so.ObjectList.create(schema, [self])

        mcls = type(self)
        referrer_class = type(referrer)

        refdict = referrer_class.get_refdict_for_class(mcls)
        reftype = referrer_class.get_field(refdict.attr).type
        refname = reftype.get_key_for_name(schema, derived_name)
        refcoll = referrer.get_field_value(schema, refdict.attr)
        is_alter = refcoll.has(schema, refname)

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

        delta, parent_cmd = cmd._build_alter_cmd_stack(
            schema, context, self, referrer=referrer)

        with context(sd.DeltaRootContext(schema=schema, op=delta)):
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


class ReferencedInheritingObject(inheriting.InheritingObject,
                                 ReferencedObject):

    def get_implicit_bases(self, schema):
        return [
            b for b in self.get_bases(schema).objects(schema)
            if not b.generic(schema)
        ]


class ReferencedObjectCommandMeta(sd.ObjectCommandMeta):
    _transparent_adapter_subclass = True

    def __new__(mcls, name, bases, clsdct, *,
                referrer_context_class=None, **kwargs):
        cls = super().__new__(mcls, name, bases, clsdct, **kwargs)
        if referrer_context_class is not None:
            cls._referrer_context_class = referrer_context_class
        return cls


class ReferencedObjectCommandBase(sd.ObjectCommand,
                                  metaclass=ReferencedObjectCommandMeta):

    _referrer_context_class = None

    @classmethod
    def get_referrer_context_class(cls):
        if cls._referrer_context_class is None:
            raise TypeError(
                f'referrer_context_class is not defined for {cls}')
        return cls._referrer_context_class

    @classmethod
    def get_referrer_context(cls, context):
        return context.get(cls.get_referrer_context_class())


class StronglyReferencedObjectCommand(ReferencedObjectCommandBase):
    pass


class ReferencedObjectCommand(ReferencedObjectCommandBase):

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        name = super()._classname_from_ast(schema, astnode, context)

        parent_ctx = cls.get_referrer_context(context)
        if parent_ctx is not None:
            referrer_name = parent_ctx.op.classname

            try:
                base_ref = utils.ast_to_typeref(
                    qlast.TypeName(maintype=astnode.name),
                    modaliases=context.modaliases, schema=schema)
            except errors.InvalidReferenceError:
                base_name = sn.Name(name)
            else:
                base_name = base_ref.get_name(schema)

            quals = cls._classname_quals_from_ast(
                schema, astnode, base_name, referrer_name, context)
            pnn = sn.get_specialized_name(base_name, referrer_name, *quals)
            name = sn.Name(name=pnn, module=referrer_name.module)

        return name

    @classmethod
    def _classname_quals_from_ast(cls, schema, astnode, base_name,
                                  referrer_name, context):
        return ()

    @classmethod
    def _name_qual_from_exprs(cls, schema, exprs):
        m = hashlib.sha1()
        for expr in exprs:
            m.update(expr.encode())
        return m.hexdigest()

    def _get_ast_node(self, schema, context):
        subject_ctx = self.get_referrer_context(context)
        ref_astnode = getattr(self, 'referenced_astnode', None)
        if subject_ctx is not None and ref_astnode is not None:
            return ref_astnode
        else:
            if isinstance(self.astnode, (list, tuple)):
                return self.astnode[1]
            else:
                return self.astnode

    def _create_innards(self, schema, context):
        schema = super()._create_innards(schema, context)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            return schema

        referrer = referrer_ctx.scls
        referrer_cls = type(referrer)
        mcls = type(self.scls)
        refdict = referrer_cls.get_refdict_for_class(mcls)

        if refdict.backref_attr:
            # Set the back-reference on referenced object
            # to the referrer.
            schema = self.scls.set_field_value(
                schema, refdict.backref_attr, referrer)

        schema = referrer.add_classref(schema, refdict.attr, self.scls)

        if (not self.scls.get_is_final(schema)
                and isinstance(referrer, inheriting.InheritingObject)):
            if not context.canonical:
                # Propagate the creation of a new ref to descendants.
                alter_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.AlterObject, referrer_cls)

                ref_field_type = referrer_cls.get_field(refdict.attr).type
                refname = ref_field_type.get_key_for(schema, self.scls)

                if context.enable_recursion:
                    for child in referrer.children(schema):
                        alter = alter_cmd(classname=child.get_name(schema))
                        with alter.new_context(schema, context, child):
                            schema, cmd = self._propagate_ref_creation(
                                schema, context, refdict, refname, child)
                            alter.add(cmd)
                        self.add(alter)
            else:
                for op in self.get_subcommands(metaclass=referrer_cls):
                    schema, _ = op.apply(schema, context=context)

        return schema

    def _get_implicit_ref_bases(self, schema, context,
                                referrer, refdict, refname):
        if not isinstance(referrer, inheriting.InheritingObject):
            return []

        child_referrer_bases = referrer.get_bases(schema).objects(schema)
        implicit_bases = []

        for ref_base in child_referrer_bases:
            parent_coll = ref_base.get_field_value(schema, refdict.attr)
            parent_item = parent_coll.get(schema, refname, default=None)
            if (parent_item is not None
                    and not parent_item.get_is_final(schema)):
                implicit_bases.append(parent_item)

        return implicit_bases

    def _get_ref_rebase(self, schema, context, refcls, implicit_bases):
        mcls = type(self.scls)

        ref_rebase_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
            inheriting.RebaseInheritingObject, mcls)

        child_bases = refcls.get_bases(schema).objects(schema)

        default_base = refcls.get_default_base_name()
        explicit_bases = [
            b for b in child_bases
            if b.generic(schema) and b.get_name(schema) != default_base
        ]

        new_bases = implicit_bases + explicit_bases

        removed_bases, added_bases = inheriting.delta_bases(
            [b.get_name(schema) for b in child_bases],
            [b.get_name(schema) for b in new_bases],
        )

        rebase_cmd = ref_rebase_cmd(
            classname=refcls.get_name(schema),
            added_bases=added_bases,
            removed_bases=removed_bases,
        )

        return rebase_cmd

    def _propagate_ref_creation(self, schema, context,
                                refdict, refname, child):
        get_cmd = sd.ObjectCommandMeta.get_command_class_or_die
        mcls = type(self.scls)

        child_coll = child.get_field_value(schema, refdict.attr)
        existing = child_coll.get(schema, refname, None)
        if existing is not None:
            # The child already has this ref, so do a rebase.
            implicit_bases = self._get_implicit_ref_bases(
                schema, context, child, refdict, refname)
            rebase_cmd = self._get_ref_rebase(
                schema, context, existing, implicit_bases)

            ref_alter_cmd = get_cmd(sd.AlterObject, mcls)
            cmd = ref_alter_cmd(classname=existing.get_name(schema))
            cmd.add(rebase_cmd)

        else:
            ref_create_cmd = get_cmd(sd.CreateObject, mcls)

            astnode = ref_create_cmd.as_inherited_ref_ast(
                schema, context, refname, self.scls)

            cmd = ref_create_cmd.as_inherited_ref_cmd(
                schema, context, astnode, [self.scls])

            cmd.set_attribute_value(
                refdict.backref_attr,
                so.ObjectRef(name=child.get_name(schema)),
            )

            if child.get_is_derived(schema):
                # All references in a derived object must
                # also be marked as derived, to be consistent
                # with derive_subtype().
                cmd.set_attribute_value('is_derived', True)

        schema, _ = cmd.apply(schema, context)

        return schema, cmd

    def _delete_innards(self, schema, context, scls):
        schema = super()._delete_innards(schema, context, scls)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            return schema

        referrer = referrer_ctx.scls
        referrer_class = type(referrer)
        mcls = type(scls)
        refdict = referrer_class.get_refdict_for_class(mcls)
        reftype = referrer_class.get_field(refdict.attr).type
        refname = reftype.get_key_for(schema, self.scls)

        if (not context.in_deletion(offset=1)
                and not context.disable_dep_verification
                and not context.canonical):
            implicit_bases = set(self._get_implicit_ref_bases(
                schema, context, referrer, refdict, refname))

            deleted_bases = set()
            for ctx in context.stack:
                if isinstance(ctx.op, type(self)):
                    deleted_bases.add(ctx.op.scls)

            implicit_bases -= deleted_bases

            if implicit_bases:
                # Cannot remove inherited objects.
                vn = scls.get_verbosename(schema, with_parent=True)
                parents = [
                    b.get_field_value(schema, refdict.backref_attr)
                    for b in implicit_bases
                ]

                pnames = '\n- '.join(
                    p.get_verbosename(schema, with_parent=True)
                    for p in parents
                )

                raise errors.SchemaError(
                    f'cannot drop inherited {vn}',
                    context=self.source_context,
                    details=f'{vn} is inherited from:\n- {pnames}'
                )

        schema = referrer.del_classref(schema, refdict.attr, refname)

        if isinstance(referrer, inheriting.InheritingObject):
            if not context.canonical:
                alter_cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.AlterObject, referrer_class)

                for child in referrer.children(schema):
                    child_coll = child.get_field_value(schema, refdict.attr)
                    existing = child_coll.get(schema, refname, None)

                    if existing is not None:
                        alter = alter_cmd(classname=child.get_name(schema))
                        with alter.new_context(schema, context, child):
                            schema, cmd = self._propagate_ref_deletion(
                                schema, context, refdict, refname, child)
                            alter.add(cmd)
                        self.add(alter)

        return schema

    def _propagate_ref_deletion(self, schema, context,
                                refdict, refname, child):
        get_cmd = sd.ObjectCommandMeta.get_command_class_or_die
        mcls = type(self.scls)

        child_coll = child.get_field_value(schema, refdict.attr)
        existing = child_coll.get(schema, refname)

        implicit_bases = self._get_implicit_ref_bases(
            schema, context, child, refdict, refname)

        if existing.get_is_local(schema) or implicit_bases:
            # Child is either defined locally or is inherited
            # from another parent, so we need to do a rebase.
            rebase_cmd = self._get_ref_rebase(
                schema, context, existing, implicit_bases)

            ref_alter_cmd = get_cmd(sd.AlterObject, mcls)
            cmd = ref_alter_cmd(classname=existing.get_name(schema))
            cmd.add(rebase_cmd)

        else:
            # The ref in child should no longer exist.
            ref_del_cmd = get_cmd(sd.DeleteObject, mcls)
            cmd = ref_del_cmd(classname=existing.get_name(schema))

        schema, _ = cmd.apply(schema, context)

        return schema, cmd

    def _build_alter_cmd_stack(self, schema, context, scls, *, referrer=None):

        delta = sd.DeltaRoot()

        if referrer is None:
            referrer = scls.get_referrer(schema)

        obj = referrer
        object_stack = []

        if type(self) != type(referrer):
            object_stack.append(referrer)

        while obj is not None:
            if isinstance(obj, ReferencedObject):
                obj = obj.get_referrer(schema)
                object_stack.append(obj)
            else:
                obj = None

        cmd = delta
        for obj in reversed(object_stack):
            alter_cmd_cls = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.AlterObject, type(obj))

            alter_cmd = alter_cmd_cls(classname=obj.get_name(schema))
            cmd.add(alter_cmd)
            cmd = alter_cmd

        return delta, cmd


class ReferencedInheritingObjectCommand(
        ReferencedObjectCommand, inheriting.InheritingObjectCommand):

    def _create_begin(self, schema, context):
        referrer_ctx = self.get_referrer_context(context)
        implicit_bases = None

        if referrer_ctx is not None and not context.canonical:
            objcls = self.get_schema_metaclass()
            referrer = referrer_ctx.scls
            referrer_class = referrer_ctx.op.get_schema_metaclass()
            refdict = referrer_class.get_refdict_for_class(objcls)
            field = referrer_class.get_field(refdict.attr)
            coll_type = field.type

            key = coll_type.get_key_for_name(schema, self.classname)

            implicit_bases = self._get_implicit_ref_bases(
                schema, context, referrer, refdict, key)

            if implicit_bases:
                bases = self.get_attribute_value('bases')
                if bases:
                    bases = so.ObjectList.create(
                        schema,
                        implicit_bases + list(bases.objects(schema)),
                    )
                else:
                    bases = so.ObjectList.create(
                        schema,
                        implicit_bases,
                    )

                self.set_attribute_value('bases', bases)

        schema = super()._create_begin(schema, context)

        if referrer_ctx is not None and not context.canonical:
            self._validate(schema, context)

        return schema

    def _alter_begin(self, schema, context, scls):
        was_local = scls.get_is_local(schema)
        schema = super()._alter_begin(schema, context, scls)
        now_local = scls.get_is_local(schema)
        if not was_local and now_local:
            self._validate(schema, context)
        return schema

    def _validate(self, schema, context):
        implicit_bases = [
            b for b in self.scls.get_bases(schema).objects(schema)
            if not b.generic(schema)
        ]

        referrer_ctx = self.get_referrer_context(context)
        objcls = self.get_schema_metaclass()
        referrer_class = referrer_ctx.op.get_schema_metaclass()
        refdict = referrer_class.get_refdict_for_class(objcls)

        if context.declarative and self.scls.get_is_local(schema):
            if (implicit_bases
                    and refdict.requires_explicit_inherit
                    and not self.get_attribute_value('declared_overloaded')):

                ancestry = [
                    obj.get_referrer(schema) for obj in implicit_bases
                ]

                raise errors.SchemaDefinitionError(
                    f'{self.scls.get_verbosename(schema, with_parent=True)} '
                    f'must be declared using the `overloaded` keyword because '
                    f'it is defined in the following ancestor(s): '
                    f'{", ".join(a.get_shortname(schema) for a in ancestry)}',
                    context=self.source_context,
                )
            elif (not implicit_bases
                    and self.get_attribute_value('declared_overloaded')):

                raise errors.SchemaDefinitionError(
                    f'{self.scls.get_verbosename(schema, with_parent=True)}: '
                    f'cannot be declared `overloaded` as there are no '
                    f'ancestors defining it.',
                    context=self.source_context,
                )

    def _propagate_ref_op(self, schema, context, scls, cb):

        rec = context.current().enable_recursion
        context.current().enable_recursion = False

        referrer_ctx = self.get_referrer_context(context)
        referrer = referrer_ctx.scls
        referrer_class = type(referrer)
        mcls = type(scls)
        refdict = referrer_class.get_refdict_for_class(mcls)
        reftype = referrer_class.get_field(refdict.attr).type
        refname = reftype.get_key_for(schema, self.scls)

        r_alter_cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.AlterObject, referrer_class)
        alter_cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.AlterObject, mcls)

        for descendant in scls.ordered_descendants(schema):
            d_name = descendant.get_name(schema)
            d_referrer = descendant.get_referrer(schema)
            d_alter_cmd = alter_cmdcls(classname=d_name)
            r_alter_cmd = r_alter_cmdcls(
                classname=d_referrer.get_name(schema))

            with r_alter_cmd.new_context(schema, context, d_referrer):
                with d_alter_cmd.new_context(schema, context, descendant):
                    cb(d_alter_cmd, refname)

                r_alter_cmd.add(d_alter_cmd)

            schema, _ = r_alter_cmd.apply(schema, context)
            self.add(r_alter_cmd)

        context.current().enable_recursion = rec

        return schema


class CreateReferencedObject(ReferencedObjectCommand, sd.CreateObject):
    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, cls.referenced_astnode):
            objcls = cls.get_schema_metaclass()

            referrer_ctx = cls.get_referrer_context(context)
            referrer_class = referrer_ctx.op.get_schema_metaclass()
            referrer_name = referrer_ctx.op.classname
            refdict = referrer_class.get_refdict_for_class(objcls)

            cmd.set_attribute_value(
                refdict.backref_attr,
                so.ObjectRef(name=referrer_name),
            )

            cmd.set_attribute_value('is_local', True)

            if getattr(astnode, 'is_abstract', None):
                cmd.add(
                    sd.AlterObjectProperty(
                        property='is_abstract',
                        new_value=True
                    )
                )

        return cmd

    def _get_ast(self, schema, context):
        refctx = type(self).get_referrer_context(context)
        if refctx is not None and not self.get_attribute_value('is_local'):
            if context.descriptive_mode:
                astnode = super()._get_ast(schema, context)
                bases = [
                    b.get_name(schema)
                    for b in self.get_attribute_value('bases').objects(schema)
                ]
                inherited_from = []
                for b in bases:
                    if sn.shortname_from_fullname(b) == b:
                        # Not an implicit base
                        continue
                    quals = sn.quals_from_fullname(b)
                    inherited_from.append(quals[0])

                astnode.system_comment = (
                    f'inherited from {", ".join(inherited_from)}'
                )
                return astnode
            else:
                return None
        else:
            return super()._get_ast(schema, context)

    def _get_ast_node(self, schema, context):
        scls = self.get_object(schema, context)
        implicit_bases = scls.get_implicit_bases(schema)
        if implicit_bases:
            mcls = self.get_schema_metaclass()
            Alter = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.AlterObject, mcls)
            alter = Alter(classname=self.classname)
            return alter._get_ast_node(schema, context)
        else:
            return super()._get_ast_node(schema, context)

    @classmethod
    def as_inherited_ref_cmd(cls, schema, context, astnode, parents):
        cmd = cls(classname=cls._classname_from_ast(schema, astnode, context))
        cmd.set_attribute_value('name', cmd.classname)
        return cmd

    @classmethod
    def as_inherited_ref_ast(cls, schema, context, name, parent):
        astnode_cls = cls.referenced_astnode

        if sn.Name.is_qualified(name):
            nref = qlast.ObjectRef(
                name=name.name,
                module=name.module,
            )
        else:
            nref = qlast.ObjectRef(
                name=name,
                module=parent.get_shortname(schema).module,
            )

        astnode = astnode_cls(name=nref)

        return astnode


class CreateReferencedInheritingObject(CreateReferencedObject,
                                       inheriting.CreateInheritingObject):
    pass


class AlterReferencedInheritingObject(
        ReferencedInheritingObjectCommand,
        inheriting.AlterInheritingObject):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        refctx = cls.get_referrer_context(context)
        if refctx is not None:
            cmd.set_attribute_value('is_local', True)

        return cmd


class RenameReferencedInheritingObject(
        ReferencedInheritingObjectCommand,
        sd.RenameObject):

    def _rename_begin(self, schema, context, scls):
        orig_schema = schema
        schema = super()._rename_begin(schema, context, scls)

        if not context.canonical and not scls.generic(schema):
            implicit_bases = scls.get_implicit_bases(schema)
            non_renamed_bases = set(implicit_bases) - context.renamed_objs

            # This object is inherited from one or more ancestors that
            # are not renamed in the same op, and this is an error.
            if non_renamed_bases:
                bases_str = ', '.join(
                    b.get_verbosename(schema, with_parent=True)
                    for b in non_renamed_bases
                )

                verb = 'are' if len(non_renamed_bases) > 1 else 'is'
                vn = scls.get_verbosename(orig_schema)

                raise errors.SchemaDefinitionError(
                    f'cannot rename inherited {vn}',
                    details=(
                        f'{vn} is inherited from '
                        f'{bases_str}, which {verb} not being renamed'
                    ),
                    context=self.source_context,
                )

            if context.enable_recursion:
                schema = self._propagate_ref_rename(schema, context, scls)

        else:
            for op in self.get_subcommands(type=sd.ObjectCommand):
                schema, _ = op.apply(schema, context)

        return schema

    def _propagate_ref_rename(self, schema, context, scls):
        rename_cmdcls = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.RenameObject, type(scls))

        def _ref_rename(alter_cmd, refname):
            astnode = rename_cmdcls.astnode(
                new_name=qlast.ObjectRef(
                    name=refname,
                ),
            )

            rename_cmd = rename_cmdcls._rename_cmd_from_ast(
                schema, astnode, context)

            alter_cmd.add(rename_cmd)

        return self._propagate_ref_op(schema, context, scls, cb=_ref_rename)
