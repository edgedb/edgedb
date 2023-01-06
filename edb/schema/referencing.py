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

import hashlib

from edb import errors

from edb.common import struct

from edb.edgeql import ast as qlast

from . import delta as sd
from . import inheriting
from . import objects as so
from . import schema as s_schema
from . import name as sn
from . import utils


ReferencedT = TypeVar('ReferencedT', bound='ReferencedObject')
ReferencedInheritingObjectT = TypeVar('ReferencedInheritingObjectT',
                                      bound='ReferencedInheritingObject')


class ReferencedObject(so.DerivableObject):

    #: True if the object has an explicit definition and is not
    #: purely inherited.
    owned = so.SchemaField(
        bool,
        default=False,
        inheritable=False,
        compcoef=0.909,
        reflection_method=so.ReflectionMethod.AS_LINK,
        special_ddl_syntax=True,
    )

    @classmethod
    def get_verbosename_static(
        cls,
        name: sn.Name,
        *,
        parent: Optional[str] = None,
    ) -> str:
        clsname = cls.get_schema_class_displayname()
        dname = cls.get_displayname_static(name)
        sn = cls.get_shortname_static(name)
        if sn == name:
            clsname = f'abstract {clsname}'
        if parent is not None:
            return f"{clsname} '{dname}' of {parent}"
        else:
            return f"{clsname} '{dname}'"

    def get_subject(self, schema: s_schema.Schema) -> Optional[so.Object]:
        # NB: classes that inherit ReferencedObject define a `get_subject`
        # method dynamically, with `subject = SchemaField`
        raise NotImplementedError

    def get_referrer(self, schema: s_schema.Schema) -> Optional[so.Object]:
        return self.get_subject(schema)

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool = False,
    ) -> str:
        vn = super().get_verbosename(schema)
        if with_parent:
            return self.add_parent_name(vn, schema)

        return vn

    def add_parent_name(
        self,
        base_name: str,
        schema: s_schema.Schema,
    ) -> str:
        subject = self.get_subject(schema)
        if subject is not None:
            pn = subject.get_verbosename(schema, with_parent=True)
            return f'{base_name} of {pn}'

        return base_name

    def init_parent_delta_branch(
        self: ReferencedT,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        referrer: Optional[so.Object] = None,
    ) -> Tuple[
        sd.CommandGroup,
        sd.Command,
        sd.ContextStack,
    ]:
        root, parent, ctx_stack = super().init_parent_delta_branch(
            schema, context, referrer=referrer)

        if referrer is None:
            referrer = self.get_referrer(schema)

        if referrer is None:
            return root, parent, ctx_stack

        obj: Optional[so.Object] = referrer
        object_stack: List[so.Object] = [referrer]

        while obj is not None:
            if isinstance(obj, ReferencedObject):
                obj = obj.get_referrer(schema)
                if obj is not None:
                    object_stack.append(obj)
            else:
                obj = None

        cmd: sd.Command = parent
        for obj in reversed(object_stack):
            alter_cmd = obj.init_delta_command(schema, sd.AlterObject)
            ctx_stack.push(alter_cmd.new_context(schema, context, obj))
            cmd.add(alter_cmd)
            cmd = alter_cmd

        return root, cmd, ctx_stack

    def is_parent_ref(
        self,
        schema: s_schema.Schema,
        reference: so.Object,
    ) -> bool:
        """Return True if *reference* is a structural ancestor of self"""
        obj = self.get_referrer(schema)
        while obj is not None:
            if obj == reference:
                return True
            elif isinstance(obj, ReferencedObject):
                obj = obj.get_referrer(schema)
            else:
                break

        return False


class ReferencedInheritingObject(
    so.DerivableInheritingObject,
    ReferencedObject,
):

    # Indicates that the object has been declared as
    # explicitly inherited.
    declared_overloaded = so.SchemaField(
        bool,
        default=False,
        compcoef=None,
        inheritable=False,
        ephemeral=True,
    )

    def should_propagate(self, schema: s_schema.Schema) -> bool:
        """Whether this object should be propagated to subtypes of the owner"""
        return True

    def get_implicit_bases(
        self: ReferencedInheritingObjectT,
        schema: s_schema.Schema,
    ) -> List[ReferencedInheritingObjectT]:
        return [
            b for b in self.get_bases(schema).objects(schema)
            if not b.generic(schema)
        ]

    def get_implicit_ancestors(
        self: ReferencedInheritingObjectT,
        schema: s_schema.Schema,
    ) -> List[ReferencedInheritingObjectT]:
        return [
            b for b in self.get_ancestors(schema).objects(schema)
            if not b.generic(schema)
        ]

    def get_name_impacting_ancestors(
        self: ReferencedInheritingObjectT,
        schema: s_schema.Schema,
    ) -> List[ReferencedInheritingObjectT]:
        """Return ancestors that have an impact on the name of this object.

        For most types this is the same as implicit ancestors.
        (For constraints it is not.)
        """
        return self.get_implicit_ancestors(schema)

    def is_endpoint_pointer(self, schema: s_schema.Schema) -> bool:
        # overloaded by Pointer
        return False

    def as_delete_delta(
        self: ReferencedInheritingObjectT,
        *,
        schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[ReferencedInheritingObjectT]:
        del_op = super().as_delete_delta(schema=schema, context=context)

        if (
            self.get_owned(schema)
            and not self.is_generated(schema)
            and any(
                context.is_deleting(schema, ancestor)
                for ancestor in self.get_implicit_ancestors(schema)
            )
        ):
            owned_op = self.init_delta_command(schema, AlterOwned)
            owned_op.set_attribute_value('owned', False, orig_value=True)
            del_op.add(owned_op)

        return del_op

    def record_field_alter_delta(
        self: ReferencedInheritingObjectT,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[ReferencedInheritingObjectT],
        context: so.ComparisonContext,
        *,
        fname: str,
        value: Any,
        orig_value: Any,
        orig_schema: s_schema.Schema,
        orig_object: ReferencedInheritingObjectT,
        confidence: float,
    ) -> None:
        super().record_field_alter_delta(
            schema,
            delta,
            context,
            fname=fname,
            value=value,
            orig_value=orig_value,
            orig_schema=orig_schema,
            orig_object=orig_object,
            confidence=confidence,
        )

        if fname == 'name':
            if any(
                context.is_renaming(orig_schema, ancestor)
                for ancestor in orig_object.get_name_impacting_ancestors(
                    orig_schema)
            ):
                renames = delta.get_subcommands(type=sd.RenameObject)
                assert len(renames) == 1
                rename = renames[0]
                rename.set_annotation('implicit_propagation', True)

    def derive_ref(
        self: ReferencedInheritingObjectT,
        schema: s_schema.Schema,
        referrer: so.QualifiedObject,
        *qualifiers: str,
        mark_derived: bool = False,
        attrs: Optional[Dict[str, Any]] = None,
        dctx: Optional[sd.CommandContext] = None,
        derived_name_base: Optional[sn.Name] = None,
        inheritance_merge: bool = True,
        inheritance_refdicts: Optional[AbstractSet[str]] = None,
        transient: bool = False,
        name: Optional[sn.QualName] = None,
        **kwargs: Any,
    ) -> Tuple[s_schema.Schema, ReferencedInheritingObjectT]:
        if name is None:
            derived_name = self.get_derived_name(
                schema,
                referrer,
                *qualifiers,
                mark_derived=mark_derived,
                derived_name_base=derived_name_base,
            )
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
        existing = refcoll.get(schema, refname, default=None)

        cmdcls = sd.AlterObject if existing is not None else sd.CreateObject
        cmd: sd.ObjectCommand[ReferencedInheritingObjectT] = (
            sd.get_object_delta_command(
                objtype=type(self),
                cmdtype=cmdcls,  # type: ignore[arg-type]
                schema=schema,
                name=derived_name,
            )
        )

        for k, v in derived_attrs.items():
            cmd.set_attribute_value(k, v)

        if existing is not None:
            new_bases = derived_attrs['bases']
            old_bases = existing.get_bases(schema)

            if new_bases != old_bases:
                assert isinstance(new_bases, so.ObjectList)
                removed_bases, added_bases = inheriting.delta_bases(
                    [b.get_name(schema) for b in old_bases.objects(schema)],
                    [b.get_name(schema) for b in new_bases.objects(schema)],
                    t=type(self),
                )

                rebase_cmd = sd.get_object_delta_command(
                    objtype=type(self),
                    cmdtype=inheriting.RebaseInheritingObject,
                    schema=schema,
                    name=derived_name,
                    added_bases=added_bases,
                    removed_bases=removed_bases,
                )

                cmd.add(rebase_cmd)

        context = sd.CommandContext(modaliases={}, schema=schema)
        delta, parent_cmd, _ = self.init_parent_delta_branch(
            schema, context, referrer=referrer)
        root = sd.DeltaRoot()
        root.add(delta)

        with context(sd.DeltaRootContext(schema=schema, op=root)):
            if not inheritance_merge:
                context.current().inheritance_merge = False

            if inheritance_refdicts is not None:
                context.current().inheritance_refdicts = (
                    inheritance_refdicts)

            if mark_derived:
                context.current().mark_derived = True

            if transient:
                context.current().transient_derivation = True

            parent_cmd.add(cmd)
            schema = delta.apply(schema, context)

        derived = schema.get(derived_name, type=type(self))

        return schema, derived


class ReferencedObjectCommandBase(sd.QualifiedObjectCommand[ReferencedT]):

    _referrer_context_class: ClassVar[Optional[
        Type[sd.ObjectCommandContext[so.Object]]
    ]] = None

    #: Whether the referenced command represents a "strong" reference,
    #: i.e. the one that must not be broken out of the enclosing parent
    #: command when doing dependency reorderings.
    is_strong_ref = struct.Field(bool, default=False)

    def __init_subclass__(
        cls,
        *,
        referrer_context_class: Optional[
            Type[sd.ObjectCommandContext[so.Object]]
        ] = None,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)
        if referrer_context_class is not None:
            cls._referrer_context_class = referrer_context_class

    @classmethod
    def get_referrer_context_class(
        cls,
    ) -> Type[sd.ObjectCommandContext[so.Object]]:
        if cls._referrer_context_class is None:
            raise TypeError(
                f'referrer_context_class is not defined for {cls}')
        return cls._referrer_context_class

    @classmethod
    def get_referrer_context(
        cls,
        context: sd.CommandContext,
    ) -> Optional[sd.ObjectCommandContext[so.Object]]:
        """Get the context of the command for the referring object, if any.

        E.g. for a `create/alter/etc concrete link` command this would
        be the context of the `create/alter/etc type` command.
        """
        ctxcls = cls.get_referrer_context_class()
        return context.get(ctxcls)

    @classmethod
    def get_referrer_context_or_die(
        cls,
        context: sd.CommandContext,
    ) -> sd.ObjectCommandContext[so.Object]:
        ctx = cls.get_referrer_context(context)
        if ctx is None:
            raise RuntimeError(f'no referrer context for {cls}')
        return ctx

    def get_top_referrer_op(
        self,
        context: sd.CommandContext,
    ) -> Optional[sd.ObjectCommand[so.Object]]:
        op: sd.ObjectCommand[so.Object] = self  # type: ignore
        while True:
            if not isinstance(op, ReferencedObjectCommandBase):
                break
            ctx = op.get_referrer_context(context)
            if ctx is None:
                break
            op = ctx.op
        return op


class ReferencedObjectCommand(ReferencedObjectCommandBase[ReferencedT]):

    @classmethod
    def _classname_from_ast_and_referrer(
        cls,
        schema: s_schema.Schema,
        referrer_name: sn.QualName,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext
    ) -> sn.QualName:
        base_ref = utils.ast_to_object_shell(
            astnode.name,
            modaliases=context.modaliases,
            schema=schema,
            metaclass=cls.get_schema_metaclass(),
        )

        base_name = sn.shortname_from_fullname(base_ref.name)
        quals = cls._classname_quals_from_ast(
            schema, astnode, base_name, referrer_name, context)
        pnn = sn.get_specialized_name(base_name, str(referrer_name), *quals)
        return sn.QualName(name=pnn, module=referrer_name.module)

    @classmethod
    def _classname_from_ast(cls,
                            schema: s_schema.Schema,
                            astnode: qlast.NamedDDL,
                            context: sd.CommandContext
                            ) -> sn.QualName:
        name = super()._classname_from_ast(schema, astnode, context)

        parent_ctx = cls.get_referrer_context(context)
        if parent_ctx is not None:
            assert isinstance(parent_ctx.op, sd.QualifiedObjectCommand)
            referrer_name = context.get_referrer_name(parent_ctx)
            name = cls._classname_from_ast_and_referrer(
                schema, referrer_name, astnode, context
            )

        assert isinstance(name, sn.QualName)
        return name

    @classmethod
    def _classname_from_name(
        cls,
        name: sn.QualName,
        referrer_name: sn.QualName,
    ) -> sn.QualName:
        base_name = sn.shortname_from_fullname(name)
        quals = cls._classname_quals_from_name(name)
        pnn = sn.get_specialized_name(base_name, str(referrer_name), *quals)
        return sn.QualName(name=pnn, module=referrer_name.module)

    @classmethod
    def _classname_quals_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        base_name: sn.Name,
        referrer_name: sn.QualName,
        context: sd.CommandContext,
    ) -> Tuple[str, ...]:
        return ()

    @classmethod
    def _classname_quals_from_name(
        cls,
        name: sn.QualName,
    ) -> Tuple[str, ...]:
        return ()

    @classmethod
    def _name_qual_from_exprs(cls,
                              schema: s_schema.Schema,
                              exprs: Iterable[str]) -> str:
        m = hashlib.sha1()
        for expr in exprs:
            m.update(expr.encode())
        return m.hexdigest()

    def _get_ast_node(self,
                      schema: s_schema.Schema,
                      context: sd.CommandContext
                      ) -> Type[qlast.DDLOperation]:
        subject_ctx = self.get_referrer_context(context)
        ref_astnode: Optional[Type[qlast.DDLOperation]] = (
            getattr(self, 'referenced_astnode', None))
        if subject_ctx is not None and ref_astnode is not None:
            return ref_astnode
        else:
            if isinstance(self.astnode, (list, tuple)):
                return self.astnode[1]
            else:
                return self.astnode


class CreateReferencedObject(
    ReferencedObjectCommand[ReferencedT],
    sd.CreateObject[ReferencedT],
):

    referenced_astnode: ClassVar[Type[qlast.ObjectDDL]]

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, cls.referenced_astnode):
            objcls = cls.get_schema_metaclass()

            referrer_ctx = cls.get_referrer_context_or_die(context)
            referrer_class = referrer_ctx.op.get_schema_metaclass()
            referrer_name = context.get_referrer_name(referrer_ctx)
            refdict = referrer_class.get_refdict_for_class(objcls)

            cmd.set_attribute_value(
                refdict.backref_attr,
                so.ObjectShell(
                    name=referrer_name,
                    schemaclass=referrer_class,
                ),
            )

            cmd.set_attribute_value('owned', True)

            if getattr(astnode, 'abstract', None):
                cmd.set_attribute_value('abstract', True)

        return cmd

    def _get_ast_node(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Type[qlast.DDLOperation]:
        # Render CREATE as ALTER in DDL if the created referenced object is
        # implicitly inherited from parents.
        scls = self.get_object(schema, context)
        assert isinstance(scls, ReferencedInheritingObject)
        implicit_bases = scls.get_implicit_bases(schema)
        if (
            implicit_bases
            and not context.declarative
            and not self.ast_ignore_ownership()
        ):
            alter = scls.init_delta_command(schema, sd.AlterObject)
            return alter._get_ast_node(schema, context)
        else:
            return super()._get_ast_node(schema, context)

    @classmethod
    def as_inherited_ref_cmd(
        cls,
        *,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        astnode: qlast.ObjectDDL,
        bases: List[ReferencedT],
        referrer: so.Object,
    ) -> sd.ObjectCommand[ReferencedT]:
        cmd = cls(classname=cls._classname_from_ast(schema, astnode, context))
        cmd.set_attribute_value('name', cmd.classname)
        cmd.set_attribute_value(
            'bases', so.ObjectList.create(schema, bases).as_shell(schema))
        return cmd

    @classmethod
    def as_inherited_ref_ast(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refname: sn.Name,
        parent: ReferencedObject,
    ) -> qlast.ObjectDDL:
        nref = cls.get_inherited_ref_name(schema, context, parent, refname)
        astnode_cls = cls.referenced_astnode
        astnode = astnode_cls(name=nref)  # type: ignore
        assert isinstance(astnode, qlast.ObjectDDL)
        return astnode

    @classmethod
    def get_inherited_ref_name(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        parent: ReferencedObject,
        refname: sn.Name,
    ) -> qlast.ObjectRef:
        ref = utils.name_to_ast_ref(refname)
        if ref.module is None:
            ref.module = parent.get_shortname(schema).module
        return ref

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            referrer = referrer_ctx.scls
            referrer_cls = type(referrer)
            mcls = type(self.scls)
            refdict = referrer_cls.get_refdict_for_class(mcls)
            schema = referrer.add_classref(schema, refdict.attr, self.scls)
        return schema


class DeleteReferencedObjectCommand(
    ReferencedObjectCommand[ReferencedT],
    sd.DeleteObject[ReferencedT],
):

    def _delete_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_innards(schema, context)
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            referrer = referrer_ctx.scls
            schema = self._delete_ref(schema, context, referrer)
        return schema

    def _delete_ref(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        referrer: so.Object,
    ) -> s_schema.Schema:

        scls = self.scls
        referrer_class = type(referrer)
        mcls = type(scls)
        refdict = referrer_class.get_refdict_for_class(mcls)
        reftype = referrer_class.get_field(refdict.attr).type
        refname = reftype.get_key_for(schema, self.scls)

        return referrer.del_classref(schema, refdict.attr, refname)


class ReferencedInheritingObjectCommand(
    ReferencedObjectCommand[ReferencedInheritingObjectT],
    inheriting.InheritingObjectCommand[ReferencedInheritingObjectT],
):

    def _get_implicit_ref_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        referrer: so.InheritingObject,
        referrer_field: str,
        fq_name: sn.QualName,
    ) -> List[ReferencedInheritingObjectT]:

        ref_field_type = type(referrer).get_field(referrer_field).type
        assert isinstance(referrer, so.QualifiedObject)
        child_referrer_bases = referrer.get_bases(schema).objects(schema)
        implicit_bases = []

        for ref_base in child_referrer_bases:
            fq_name_in_child = self._classname_from_name(
                fq_name, ref_base.get_name(schema))
            refname = ref_field_type.get_key_for_name(schema, fq_name_in_child)
            parent_coll = ref_base.get_field_value(schema, referrer_field)
            parent_item = parent_coll.get(schema, refname, default=None)
            if (
                parent_item is not None
                and parent_item.should_propagate(schema)
                and not context.is_deleting(parent_item)
            ):
                implicit_bases.append(parent_item)

        return implicit_bases

    def get_ref_implicit_base_delta(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refcls: ReferencedInheritingObjectT,
        implicit_bases: List[ReferencedInheritingObjectT],
    ) -> inheriting.BaseDelta_T[ReferencedInheritingObjectT]:
        child_bases = refcls.get_bases(schema).objects(schema)

        default_base = refcls.get_default_base_name()
        explicit_bases = [
            b for b in child_bases
            if b.generic(schema) and b.get_name(schema) != default_base
        ]

        new_bases = implicit_bases + explicit_bases
        return inheriting.delta_bases(
            [b.get_name(schema) for b in child_bases],
            [b.get_name(schema) for b in new_bases],
            t=type(refcls),
        )

    def _validate(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext
    ) -> None:
        scls = self.scls
        implicit_bases = [
            b for b in scls.get_bases(schema).objects(schema)
            if not b.generic(schema)
        ]

        referrer_ctx = self.get_referrer_context_or_die(context)
        objcls = self.get_schema_metaclass()
        referrer_class = referrer_ctx.op.get_schema_metaclass()
        refdict = referrer_class.get_refdict_for_class(objcls)

        if context.declarative and scls.get_owned(schema):
            if (implicit_bases
                    and refdict.requires_explicit_overloaded
                    and not self.get_attribute_value('declared_overloaded')):

                ancestry = []

                for obj in implicit_bases:
                    bref = obj.get_referrer(schema)
                    assert bref is not None
                    ancestry.append(bref)

                alist = ", ".join(
                    str(a.get_shortname(schema)) for a in ancestry
                )
                raise errors.SchemaDefinitionError(
                    f'{self.scls.get_verbosename(schema, with_parent=True)} '
                    f'must be declared using the `overloaded` keyword because '
                    f'it is defined in the following ancestor(s): {alist}',
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

    def get_implicit_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Any,
    ) -> List[sn.QualName]:

        mcls = self.get_schema_metaclass()
        default_base = mcls.get_default_base_name()

        if isinstance(bases, so.ObjectCollectionShell):
            base_names = [b.get_name(schema) for b in bases.items]
        elif isinstance(bases, so.ObjectList):
            base_names = list(bases.names(schema))
        else:
            # assume regular iterable of shells
            base_names = [b.get_name(schema) for b in bases]

        # Filter out explicit bases
        implicit_bases = [
            b
            for b in base_names
            if (
                b != default_base
                and isinstance(b, sn.QualName)
                and sn.shortname_from_fullname(b) != b
            )
        ]

        return implicit_bases

    def _propagate_ref_op(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: ReferencedInheritingObject,
        cb: Callable[[sd.ObjectCommand[so.Object], sn.Name], None]
    ) -> None:
        for ctx in reversed(context.stack):
            if (
                isinstance(ctx.op, sd.ObjectCommand)
                and ctx.op.get_annotation('implicit_propagation')
            ):
                return

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx:
            referrer = referrer_ctx.scls
            referrer_class = type(referrer)
            mcls = type(scls)
            refdict = referrer_class.get_refdict_for_class(mcls)
            reftype = referrer_class.get_field(refdict.attr).type
            refname = reftype.get_key_for(schema, self.scls)
        else:
            refname = self.scls.get_name(schema)

        for descendant in scls.ordered_descendants(schema):
            d_alter_root, d_alter_cmd, ctx_stack = (
                descendant.init_delta_branch(schema, context, sd.AlterObject))
            d_alter_cmd.set_annotation('implicit_propagation', True)

            with ctx_stack():
                cb(d_alter_cmd, refname)

            self.add_caused(d_alter_root)

    def _propagate_ref_field_alter_in_inheritance(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field_name: str,
        require_inheritance_consistency: bool = True,
    ) -> None:
        """Validate and propagate a field alteration to children.

        This method also performs consistency checks against base objects
        to ensure that the new value matches that of the parents.
        """
        scls = self.scls

        currently_altered = context.change_log[type(scls), field_name]
        currently_altered.add(scls)

        if require_inheritance_consistency:
            implicit_bases = scls.get_implicit_bases(schema)
            non_altered_bases = []

            value = scls.get_field_value(schema, field_name)
            for base in {
                    x for x in implicit_bases if x not in currently_altered}:
                base_value = base.get_field_value(schema, field_name)

                if isinstance(value, so.SubclassableObject):
                    if not value.issubclass(schema, base_value):
                        non_altered_bases.append(base)
                else:
                    if value != base_value:
                        non_altered_bases.append(base)

            # This object is inherited from one or more ancestors that
            # are not altered in the same op, and this is an error.
            if non_altered_bases:
                bases_str = ', '.join(
                    b.get_verbosename(schema, with_parent=True)
                    for b in non_altered_bases
                )

                vn = scls.get_verbosename(schema, with_parent=True)
                desc = self.get_friendly_description(
                    schema=schema,
                    object_desc=f'inherited {vn}',
                )

                raise errors.SchemaDefinitionError(
                    f'cannot {desc}',
                    details=(
                        f'{vn} is inherited from '
                        f'{bases_str}'
                    ),
                    context=self.source_context,
                )

        value = self.get_attribute_value(field_name)

        def _propagate(
            alter_cmd: sd.ObjectCommand[so.Object],
            refname: sn.Name,
        ) -> None:
            assert isinstance(alter_cmd, sd.QualifiedObjectCommand)
            s_t: sd.ObjectCommand[ReferencedInheritingObjectT]
            if isinstance(self, sd.AlterSpecialObjectField):
                s_t = self.clone(alter_cmd.classname)
            else:
                s_t = type(self)(classname=alter_cmd.classname)
            orig_value = scls.get_explicit_field_value(
                schema, field_name, default=None)
            s_t.set_attribute_value(
                field_name,
                value,
                orig_value=orig_value,
                inherited=True,
            )
            alter_cmd.add(s_t)

        self._propagate_ref_op(schema, context, scls, cb=_propagate)

    def _drop_owned_refs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> s_schema.Schema:

        scls = self.scls
        refs = scls.get_field_value(schema, refdict.attr)

        ref: ReferencedInheritingObject
        for ref in refs.objects(schema):
            inherited = ref.get_implicit_bases(schema)
            if inherited and ref.get_owned(schema):
                alter = ref.init_delta_command(schema, sd.AlterObject)
                alter.set_attribute_value('owned', False, orig_value=True)
                schema = alter.apply(schema, context)
                self.add(alter)
            elif (
                # drop things that aren't owned and aren't inherited
                not inherited
                # endpoint pointers are special because they aren't marked as
                # inherited even though they basically are
                and not ref.is_endpoint_pointer(schema)
            ):
                drop_ref = ref.init_delta_command(schema, sd.DeleteObject)
                self.add(drop_ref)

        return schema


class CreateReferencedInheritingObject(
    CreateReferencedObject[ReferencedInheritingObjectT],
    inheriting.CreateInheritingObject[ReferencedInheritingObjectT],
    ReferencedInheritingObjectCommand[ReferencedInheritingObjectT],
):

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        refctx = type(self).get_referrer_context(context)
        if refctx is not None:
            if self.get_attribute_value('from_alias'):
                return None

            elif (
                not self.get_attribute_value('owned')
                and not self.ast_ignore_ownership()
            ):
                if context.descriptive_mode:
                    astnode = super()._get_ast(
                        schema,
                        context,
                        parent_node=parent_node,
                    )
                    assert astnode is not None

                    inherited_from = [
                        sn.quals_from_fullname(b)[0]
                        for b in self.get_implicit_bases(
                            schema,
                            context,
                            self.get_attribute_value('bases'),
                        )
                    ]

                    astnode.system_comment = (
                        f'inherited from {", ".join(inherited_from)}'
                    )

                    return astnode
                else:
                    return None

            else:
                astnode = super()._get_ast(
                    schema, context, parent_node=parent_node)

                if context.declarative:
                    scls = self.get_object(schema, context)
                    assert isinstance(scls, ReferencedInheritingObject)
                    implicit_bases = scls.get_implicit_bases(schema)
                    objcls = self.get_schema_metaclass()
                    referrer_class = refctx.op.get_schema_metaclass()
                    refdict = referrer_class.get_refdict_for_class(objcls)
                    if refdict.requires_explicit_overloaded and implicit_bases:
                        assert isinstance(astnode, qlast.CreateConcretePointer)
                        astnode.declared_overloaded = True

                return astnode
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        referrer_ctx = self.get_referrer_context(context)
        implicit_bases = None

        if referrer_ctx is not None and not context.canonical:
            objcls = self.get_schema_metaclass()
            referrer = referrer_ctx.scls

            if isinstance(referrer, so.InheritingObject):
                referrer_class = referrer_ctx.op.get_schema_metaclass()
                refdict = referrer_class.get_refdict_for_class(objcls)

                implicit_bases = self._get_implicit_ref_bases(
                    schema, context, referrer, refdict.attr, self.classname)

                if implicit_bases:
                    bases = self.get_attribute_value('bases')
                    if bases:
                        res_bases = cast(
                            List[ReferencedInheritingObjectT],
                            self.resolve_obj_collection(bases, schema))
                        bases = so.ObjectList.create(
                            schema,
                            implicit_bases + [
                                b for b in res_bases
                                if b not in implicit_bases
                            ],
                        )
                    else:
                        bases = so.ObjectList.create(
                            schema,
                            implicit_bases,
                        )

                    self.set_attribute_value('bases', bases.as_shell(schema))

                if referrer.get_is_derived(schema):
                    self.set_attribute_value('is_derived', True)

        return super()._create_begin(schema, context)

    def _create_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if (
            not context.canonical
            and context.enable_recursion
            and (referrer_ctx := self.get_referrer_context(context))
            and isinstance(referrer := referrer_ctx.scls, so.InheritingObject)
            and self.scls.should_propagate(schema)
        ):
            # Propagate the creation of a new ref to
            # descendants of our referrer.
            self._propagate_ref_creation(schema, context, referrer)

        return super()._create_innards(schema, context)

    def _create_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_finalize(schema, context)
        if not context.canonical:
            referrer_ctx = self.get_referrer_context(context)
            if referrer_ctx is not None:
                self._validate(schema, context)

        return schema

    def _propagate_ref_creation(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        referrer: so.InheritingObject,
    ) -> None:

        get_cmd = sd.get_object_command_class_or_die

        mcls = type(self.scls)
        referrer_cls = type(referrer)
        ref_create_cmd = get_cmd(sd.CreateObject, mcls)
        ref_alter_cmd = get_cmd(sd.AlterObject, mcls)
        ref_rebase_cmd = get_cmd(inheriting.RebaseInheritingObject, mcls)
        assert issubclass(ref_create_cmd, CreateReferencedInheritingObject)
        assert issubclass(ref_rebase_cmd, RebaseReferencedInheritingObject)
        refdict = referrer_cls.get_refdict_for_class(mcls)
        parent_fq_refname = self.scls.get_name(schema)

        for child in referrer.children(schema):
            if not child.allow_ref_propagation(schema, context, refdict):
                continue

            alter_root, alter, ctx_stack = child.init_delta_branch(
                schema, context, sd.AlterObject)

            with ctx_stack():
                # This is needed to get the correct inherited name which will
                # either be created or rebased.
                ref_field_type = type(child).get_field(refdict.attr).type
                refname = ref_field_type.get_key_for_name(
                    schema, parent_fq_refname)

                astnode = ref_create_cmd.as_inherited_ref_ast(
                    schema, context, refname, self.scls)
                fq_name = self._classname_from_ast(schema, astnode, context)

                # We cannot check for ref existence in this child at this
                # time, because it might get created in a sibling branch
                # of the delta tree.  Instead, generate a command group
                # containing Alter(if_exists) and Create(if_not_exists)
                # to postpone that check until the application time.
                ref_create = ref_create_cmd.as_inherited_ref_cmd(
                    schema=schema,
                    context=context,
                    astnode=astnode,
                    bases=[self.scls],
                    referrer=child,
                )
                assert isinstance(ref_create, sd.CreateObject)
                ref_create.if_not_exists = True

                # Copy any special updates over
                for special in self.get_subcommands(
                        type=sd.AlterSpecialObjectField):
                    ref_create.add(special.clone(ref_create.classname))

                ref_create.set_attribute_value(refdict.backref_attr, child)

                if child.get_is_derived(schema):
                    # All references in a derived object must
                    # also be marked as derived, to be consistent
                    # with derive_subtype().
                    ref_create.set_attribute_value('is_derived', True)

                ref_alter = ref_alter_cmd(classname=fq_name, if_exists=True)
                ref_alter.add(ref_rebase_cmd(
                    classname=fq_name,
                    implicit=True,
                    added_bases=(),
                    removed_bases=(),
                ))

                alter.add(ref_alter)
                alter.add(ref_create)

            self.add_caused(alter_root)


class AlterReferencedInheritingObject(
    ReferencedInheritingObjectCommand[ReferencedInheritingObjectT],
    inheriting.AlterInheritingObject[ReferencedInheritingObjectT],
):

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.get_attribute_value('from_alias'):
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> AlterReferencedInheritingObject[ReferencedInheritingObjectT]:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        refctx = cls.get_referrer_context(context)
        # When a referenced object is altered it becomes "owned"
        # by the referrer, _except_ when either an explicit
        # SET OWNED/DROP OWNED subcommand is present, or
        # _all_ subcommands are `RESET` subcommands.
        if (
            refctx is not None
            and qlast.get_ddl_field_command(astnode, 'owned') is None
            and (
                not cmd.get_subcommands()
                or not all(
                    (
                        isinstance(scmd, sd.AlterObjectProperty)
                        and scmd.new_value is None
                    )
                    for scmd in cmd.get_subcommands()
                )
            )
        ):
            cmd.set_attribute_value('owned', True)

        assert isinstance(cmd, AlterReferencedInheritingObject)
        return cmd

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_finalize(schema, context)
        scls = self.scls
        was_owned = scls.get_owned(context.current().original_schema)
        now_owned = scls.get_owned(schema)
        if not was_owned and now_owned:
            self._validate(schema, context)
        return schema


class RebaseReferencedInheritingObject(
    ReferencedInheritingObjectCommand[ReferencedInheritingObjectT],
    inheriting.RebaseInheritingObject[ReferencedInheritingObjectT],
):

    implicit = struct.Field(bool, default=False)

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:

        if not context.canonical and self.implicit:
            mcls = self.get_schema_metaclass()
            refctx = self.get_referrer_context_or_die(context)
            referrer = refctx.scls
            assert isinstance(referrer, so.InheritingObject)
            refdict = type(referrer).get_refdict_for_class(mcls)

            implicit_bases = self._get_implicit_ref_bases(
                schema,
                context,
                referrer=referrer,
                referrer_field=refdict.attr,
                fq_name=self.classname,
            )

            scls = self.get_object(schema, context)
            removed_bases, added_bases = self.get_ref_implicit_base_delta(
                schema,
                context,
                scls,
                implicit_bases=implicit_bases,
            )

            self.added_bases = added_bases
            self.removed_bases = removed_bases

        return super().apply(schema, context)

    def _get_bases_for_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Tuple[so.ObjectShell[ReferencedInheritingObjectT], ...],
    ) -> Tuple[so.ObjectShell[ReferencedInheritingObjectT], ...]:
        bases = super()._get_bases_for_ast(schema, context, bases)
        implicit_bases = set(self.get_implicit_bases(schema, context, bases))
        return tuple(b for b in bases if b.name not in implicit_bases)


class RenameReferencedInheritingObject(
    ReferencedInheritingObjectCommand[ReferencedInheritingObjectT],
    inheriting.RenameInheritingObject[ReferencedInheritingObjectT],
):

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        if not context.canonical and not scls.generic(schema):
            referrer_ctx = self.get_referrer_context_or_die(context)
            referrer_class = referrer_ctx.op.get_schema_metaclass()
            mcls = self.get_schema_metaclass()
            refdict = referrer_class.get_refdict_for_class(mcls)
            reftype = referrer_class.get_field(refdict.attr).type

            orig_ref_fqname = scls.get_name(orig_schema)
            orig_ref_lname = reftype.get_key_for_name(schema, orig_ref_fqname)

            new_ref_fqname = scls.get_name(schema)
            new_ref_lname = reftype.get_key_for_name(schema, new_ref_fqname)

            # Distinguish between actual local name change and fully-qualified
            # name change due to structural parent rename.
            if orig_ref_lname != new_ref_lname:
                implicit_bases = scls.get_implicit_bases(schema)
                non_renamed_bases = {
                    x for x in implicit_bases if x not in context.renamed_objs}
                # This object is inherited from one or more ancestors that
                # are not renamed in the same op, and this is an error.
                if non_renamed_bases:
                    bases_str = ', '.join(
                        b.get_verbosename(schema, with_parent=True)
                        for b in non_renamed_bases
                    )

                    verb = 'are' if len(non_renamed_bases) > 1 else 'is'
                    vn = scls.get_verbosename(orig_schema, with_parent=True)

                    raise errors.SchemaDefinitionError(
                        f'cannot rename inherited {vn}',
                        details=(
                            f'{vn} is inherited from '
                            f'{bases_str}, which {verb} not being renamed'
                        ),
                        context=self.source_context,
                    )

            self._propagate_ref_rename(schema, context, scls)

        return schema

    def _propagate_ref_rename(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: ReferencedInheritingObject
    ) -> None:
        rename_cmdcls = sd.get_object_command_class_or_die(
            sd.RenameObject, type(scls))

        def _ref_rename(alter_cmd: sd.Command, refname: sn.Name) -> None:
            astnode = rename_cmdcls.astnode(  # type: ignore
                new_name=utils.name_to_ast_ref(refname),
            )

            rename_cmd = rename_cmdcls._rename_cmd_from_ast(
                schema, astnode, context)

            alter_cmd.add(rename_cmd)

        self._propagate_ref_op(schema, context, scls, cb=_ref_rename)

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.get_annotation('implicit_propagation'):
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)


class DeleteReferencedInheritingObject(
    DeleteReferencedObjectCommand[ReferencedInheritingObjectT],
    inheriting.DeleteInheritingObject[ReferencedInheritingObjectT],
    ReferencedInheritingObjectCommand[ReferencedInheritingObjectT],
):

    def _delete_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if (
            not context.canonical
            and (referrer_ctx := self.get_referrer_context(context))
            and isinstance(referrer := referrer_ctx.scls, so.InheritingObject)
            and self.scls.should_propagate(schema)
        ):
            self._propagate_ref_deletion(schema, context, referrer)

        return super()._delete_innards(schema, context)

    def _propagate_ref_deletion(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        referrer: so.InheritingObject,
    ) -> None:
        scls = self.scls
        self_name = scls.get_name(schema)
        referrer_class = type(referrer)
        mcls = type(scls)
        refdict = referrer_class.get_refdict_for_class(mcls)
        reftype = referrer_class.get_field(refdict.attr).type

        if (
            not context.in_deletion(offset=1)
            and not context.disable_dep_verification
        ):
            implicit_bases = set(self._get_implicit_ref_bases(
                schema, context, referrer, refdict.attr, self_name))

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

        # Sort the children by reverse inheritance order amongst them.
        # So if we are T and have children A and B and A <: B, we want to
        # process A first, since we need to rebase it away from T, and then
        # dropping A will also drop B.
        for child in reversed(
            sd.sort_by_inheritance(schema, referrer.children(schema))
        ):
            assert isinstance(child, so.QualifiedObject)
            child_coll = child.get_field_value(schema, refdict.attr)
            fq_refname_in_child = self._classname_from_name(
                self_name,
                child.get_name(schema),
            )
            child_refname = reftype.get_key_for_name(
                schema, fq_refname_in_child)
            existing = child_coll.get(schema, child_refname, None)

            if existing is not None:
                alter_root, alter_leaf, ctx_stack = (
                    existing.init_parent_delta_branch(
                        schema, context, referrer=child))
                with ctx_stack():
                    cmd = self._propagate_child_ref_deletion(
                        schema, context, refdict, child, existing)
                    alter_leaf.add(cmd)
                self.add_caused(alter_root)

    def _propagate_child_ref_deletion(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
        child: so.InheritingObject,
        child_ref: ReferencedInheritingObjectT,
    ) -> sd.Command:
        name = child_ref.get_name(schema)
        implicit_bases = self._get_implicit_ref_bases(
            schema, context, child, refdict.attr, name)

        cmd: sd.Command

        if child_ref.get_owned(schema) or implicit_bases:
            # Child is either defined locally or is inherited
            # from another parent, so we need to do a rebase.
            removed_bases, added_bases = self.get_ref_implicit_base_delta(
                schema, context, child_ref, implicit_bases)

            rebase_cmd = child_ref.init_delta_command(
                schema,
                inheriting.RebaseInheritingObject,
                added_bases=added_bases,
                removed_bases=removed_bases,
            )

            cmd = child_ref.init_delta_command(schema, sd.AlterObject)
            cmd.add(rebase_cmd)
        else:
            # The ref in child should no longer exist.
            cmd = child_ref.init_delta_command(schema, sd.DeleteObject)

        return cmd

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        refctx = type(self).get_referrer_context(context)
        if (
            refctx is not None
            and not self.get_orig_attribute_value('owned')
        ):
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)


class AlterOwned(
    ReferencedInheritingObjectCommand[ReferencedInheritingObjectT],
    inheriting.AlterInheritingObjectFragment[ReferencedInheritingObjectT],
    sd.AlterSpecialObjectField[ReferencedInheritingObjectT],
):

    _delta_action = 'alterowned'

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        orig_owned = scls.get_owned(orig_schema)
        owned = scls.get_owned(schema)

        if (
            orig_owned != owned
            and not owned
            and not context.canonical
        ):
            implicit_bases = scls.get_implicit_bases(schema)
            if not implicit_bases:
                # ref isn't actually inherited, so cannot be un-owned
                vn = scls.get_verbosename(schema, with_parent=True)
                sn = type(scls).get_schema_class_displayname().upper()
                raise errors.InvalidDefinitionError(
                    f'cannot drop owned {vn}, as it is not inherited, '
                    f'use DROP {sn} instead',
                    context=self.source_context,
                )

            # DROP OWNED requires special handling: the object in question
            # must revert all modification made on top of inherited attributes.
            bases = scls.get_bases(schema).objects(schema)
            schema = self.inherit_fields(
                schema,
                context,
                bases,
                ignore_local=True,
            )

            for refdict in type(scls).get_refdicts():
                schema = self._drop_owned_refs(schema, context, refdict)

        return schema
