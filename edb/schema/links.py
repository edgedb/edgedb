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

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb import errors

from . import abc as s_abc
from . import constraints
from . import delta as sd
from . import indexes
from . import inheriting
from . import lproperties
from . import name as sn
from . import objects as so
from . import pointers
from . import referencing
from . import sources
from . import utils

if TYPE_CHECKING:
    from . import objtypes as s_objtypes
    from . import schema as s_schema


LinkTargetDeleteAction = qltypes.LinkTargetDeleteAction


def merge_actions(
    target: so.InheritingObject,
    sources: List[so.Object],
    field_name: str,
    *,
    schema: s_schema.Schema,
) -> Any:
    ours = target.get_explicit_local_field_value(schema, field_name, None)
    if ours is None:
        current = None
        current_from = None

        for source in sources:
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs is not None:
                if current is None:
                    current = theirs
                    current_from = source
                elif current != theirs:
                    target_source = target.get_source(schema)
                    current_from_source = current_from.get_source(schema)
                    source_source = source.get_source(schema)

                    tgt_repr = (
                        f'{target_source.get_displayname(schema)}.'
                        f'{target.get_displayname(schema)}'
                    )
                    cf_repr = (
                        f'{current_from_source.get_displayname(schema)}.'
                        f'{current_from.get_displayname(schema)}'
                    )
                    other_repr = (
                        f'{source_source.get_displayname(schema)}.'
                        f'{source.get_displayname(schema)}'
                    )

                    raise errors.SchemaError(
                        f'cannot implicitly resolve the '
                        f'`on target delete` action for '
                        f'{tgt_repr!r}: it is defined as {current} in '
                        f'{cf_repr!r} and as {theirs} in {other_repr!r}; '
                        f'to resolve, declare `on target delete` '
                        f'explicitly on {tgt_repr!r}'
                    )
        return current
    else:
        return ours


class Link(sources.Source, pointers.Pointer, s_abc.Link,
           qlkind=qltypes.SchemaObjectClass.LINK):

    on_target_delete = so.SchemaField(
        LinkTargetDeleteAction,
        default=LinkTargetDeleteAction.RESTRICT,
        coerce=True,
        compcoef=0.9,
        merge_fn=merge_actions)

    def is_link_property(self, schema: s_schema.Schema) -> bool:
        return False

    def is_property(self, schema: s_schema.Schema) -> bool:
        return False

    def scalar(self) -> bool:
        return False

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return bool([p for p in self.get_pointers(schema).objects(schema)
                     if not p.is_special_pointer(schema)])

    def compare(
        self,
        other: so.Object,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> float:
        if not isinstance(other, Link):
            if isinstance(other, pointers.Pointer):
                return 0.0
            else:
                raise NotImplementedError()

        return super().compare(
            other, our_schema=our_schema,
            their_schema=their_schema, context=context)

    def set_target(
        self,
        schema: s_schema.Schema,
        target: s_objtypes.ObjectType,
    ) -> s_schema.Schema:
        schema = super().set_target(schema, target)
        tgt_prop = self.getptr(schema, 'target')
        assert tgt_prop is not None
        schema = tgt_prop.set_target(schema, target)
        return schema

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.Name, ...]:
        return (
            sn.Name(module='std', name='link'),
            sn.Name(module='schema', name='__type__'),
        )

    @classmethod
    def get_default_base_name(self) -> sn.Name:
        return sn.Name('std::link')


class DerivedLink(pointers.Pointer, sources.Source):
    pass


class LinkSourceCommandContext(sources.SourceCommandContext):
    pass


class LinkSourceCommand(inheriting.InheritingObjectCommand[Link]):
    pass


class LinkCommandContext(pointers.PointerCommandContext,
                         constraints.ConsistencySubjectCommandContext,
                         lproperties.PropertySourceContext,
                         indexes.IndexSourceCommandContext):
    pass


class LinkCommand(lproperties.PropertySourceCommand,
                  pointers.PointerCommand,
                  schema_metaclass=Link, context_class=LinkCommandContext,
                  referrer_context_class=LinkSourceCommandContext):

    def _set_pointer_type(
        self,
        schema: s_schema.Schema,
        astnode: qlast.CreateConcreteLink,
        context: sd.CommandContext,
        target_ref: Union[so.Object, so.ObjectShell],
    ) -> None:
        assert astnode.target is not None
        slt = SetLinkType(classname=self.classname, type=target_ref)
        slt.set_attribute_value(
            'target', target_ref, source_context=astnode.target.context)
        self.add(slt)

    def _apply_refs_fields_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        refdict: so.RefDict,
    ) -> None:
        if issubclass(refdict.ref_cls, pointers.Pointer):
            for op in self.get_subcommands(metaclass=refdict.ref_cls):
                pname = sn.shortname_from_fullname(op.classname)
                if pname.name not in {'source', 'target'}:
                    self._append_subcmd_ast(schema, node, op, context)
        else:
            super()._apply_refs_fields_ast(schema, context, node, refdict)

    def _validate_pointer_def(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        """Check that link definition is sound."""
        super()._validate_pointer_def(schema, context)

        scls = self.scls
        assert isinstance(scls, Link)

        if not scls.get_is_local(schema):
            return

        target = scls.get_target(schema)
        assert target is not None

        if not target.is_object_type():
            srcctx = self.get_attribute_source_context('target')
            raise errors.InvalidLinkTargetError(
                f'invalid link target, expected object type, got '
                f'{target.get_schema_class_displayname()}',
                context=srcctx,
            )

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        node = super()._get_ast(schema, context, parent_node=parent_node)
        # __type__ link is special, and while it exists on every object
        # it does not have a defined default in the schema (and therefore
        # it isn't marked as required.)  We intervene here to mark all
        # __type__ links required when rendering for SDL/TEXT.
        if context.declarative and node is not None:
            assert isinstance(node, (qlast.CreateConcreteLink,
                                     qlast.CreateLink))
            if node.name.name == '__type__':
                node.is_required = True
        return node


class CreateLink(
    LinkCommand,
    referencing.CreateReferencedInheritingObject[Link],
):
    astnode = [qlast.CreateConcreteLink, qlast.CreateLink]
    referenced_astnode = qlast.CreateConcreteLink

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        if isinstance(astnode, qlast.CreateConcreteLink):
            cmd._process_create_or_alter_ast(schema, astnode, context)
        else:
            # this is an abstract property then
            if cmd.get_attribute_value('default') is not None:
                raise errors.SchemaDefinitionError(
                    f"'default' is not a valid field for an abstract link",
                    context=astnode.context)
        assert isinstance(cmd, sd.Command)
        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        objtype = self.get_referrer_context(context)

        if op.property == 'required':
            # Due to how SDL is processed the underlying AST may be an
            # AlterConcreteLink, which requires different handling.
            if isinstance(node, qlast.CreateConcreteLink):
                assert isinstance(op.new_value, bool)
                node.is_required = op.new_value
            else:
                node.commands.append(
                    qlast.SetSpecialField(
                        name='required',
                        value=op.new_value,
                    )
                )
        elif op.property == 'cardinality':
            node.cardinality = op.new_value
        elif op.property == 'target' and objtype:
            # Due to how SDL is processed the underlying AST may be an
            # AlterConcreteLink, which requires different handling.
            if isinstance(node, qlast.CreateConcreteLink):
                if not node.target:
                    expr = self.get_attribute_value('expr')
                    if expr is not None:
                        node.target = expr.qlast
                    else:
                        t = op.new_value
                        assert isinstance(t, (so.Object, so.ObjectShell))
                        node.target = utils.typeref_to_ast(schema, t)
            else:
                assert isinstance(op.new_value, (so.Object, so.ObjectShell))
                node.commands.append(
                    qlast.SetLinkType(
                        type=utils.typeref_to_ast(schema, op.new_value)
                    )
                )
        elif op.property == 'on_target_delete':
            node.commands.append(qlast.OnTargetDelete(cascade=op.new_value))
        else:
            super()._apply_field_ast(schema, context, node, op)

    def inherit_classref_dict(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> sd.CommandGroup:
        cmd = super().inherit_classref_dict(schema, context, refdict)

        if refdict.attr != 'pointers':
            return cmd

        parent_ctx = self.get_referrer_context(context)
        if parent_ctx is None:
            return cmd

        base_prop_name = sn.Name('std::source')
        s_name = sn.get_specialized_name(
            sn.Name('__::source'), self.classname)
        src_prop_name = sn.Name(name=s_name,
                                module=self.classname.module)

        src_prop = lproperties.CreateProperty(
            classname=src_prop_name,
            metaclass=lproperties.Property
        )
        src_prop.set_attribute_value('name', src_prop_name)
        src_prop.set_attribute_value(
            'bases',
            so.ObjectList.create(schema, [schema.get(base_prop_name)]),
        )
        src_prop.set_attribute_value(
            'source',
            self.scls,
        )
        src_prop.set_attribute_value(
            'target',
            parent_ctx.op.scls,
        )
        src_prop.set_attribute_value('required', True)
        src_prop.set_attribute_value('readonly', True)
        src_prop.set_attribute_value('is_final', True)
        src_prop.set_attribute_value('is_local', True)
        src_prop.set_attribute_value('cardinality', qltypes.Cardinality.ONE)

        cmd.prepend(src_prop)

        base_prop_name = sn.Name('std::target')
        s_name = sn.get_specialized_name(
            sn.Name('__::target'), self.classname)
        tgt_prop_name = sn.Name(name=s_name,
                                module=self.classname.module)

        tgt_prop = lproperties.CreateProperty(
            classname=tgt_prop_name,
            metaclass=lproperties.Property
        )

        tgt_prop.set_attribute_value('name', tgt_prop_name)
        tgt_prop.set_attribute_value(
            'bases',
            so.ObjectList.create(schema, [schema.get(base_prop_name)]),
        )
        tgt_prop.set_attribute_value(
            'source',
            self.scls,
        )
        tgt_prop.set_attribute_value(
            'target',
            self.get_attribute_value('target'),
        )
        tgt_prop.set_attribute_value('required', False)
        tgt_prop.set_attribute_value('readonly', True)
        tgt_prop.set_attribute_value('is_final', True)
        tgt_prop.set_attribute_value('is_local', True)
        tgt_prop.set_attribute_value('cardinality', qltypes.Cardinality.ONE)

        cmd.prepend(tgt_prop)

        return cmd


class RenameLink(LinkCommand, sd.RenameObject):
    pass


class RebaseLink(LinkCommand, inheriting.RebaseInheritingObject[Link]):
    pass


class SetLinkType(pointers.SetPointerType,
                  schema_metaclass=Link,
                  referrer_context_class=LinkSourceCommandContext):

    astnode = qlast.SetLinkType


class SetTargetDeletePolicy(sd.Command):
    astnode = qlast.OnTargetDelete

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.AlterObjectProperty:
        return sd.AlterObjectProperty(
            property='on_target_delete'
        )

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.OnTargetDelete)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd.new_value = astnode.cascade
        return cmd


class AlterLink(
    LinkCommand,
    referencing.AlterReferencedInheritingObject[Link],
):
    astnode = [qlast.AlterLink, qlast.AlterConcreteLink]
    referenced_astnode = qlast.AlterConcreteLink

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> referencing.AlterReferencedInheritingObject[Link]:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        if isinstance(astnode, qlast.CreateConcreteLink):
            cmd._process_create_or_alter_ast(schema, astnode, context)
        assert isinstance(cmd, referencing.AlterReferencedInheritingObject)
        return cmd


class DeleteLink(
    LinkCommand,
    referencing.DeleteReferencedInheritingObject[Link],
):
    astnode = [qlast.DropLink, qlast.DropConcreteLink]
    referenced_astnode = qlast.DropConcreteLink

    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: so.Object,
    ) -> List[sd.Command]:
        assert isinstance(scls, Link)
        commands = list(super()._canonicalize(schema, context, scls))
        target = scls.get_target(schema)

        # A link may only target an alias only inside another alias,
        # which means that the target alias must be dropped along
        # with this link.
        if (target is not None
                and target.is_view(schema)
                and target.get_alias_is_persistent(schema)):

            Cmd = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.DeleteObject, type(target))

            del_cmd = Cmd(classname=target.get_name(schema))
            subcmds = del_cmd._canonicalize(schema, context, target)
            del_cmd.update(subcmds)
            commands.append(del_cmd)

        return commands
