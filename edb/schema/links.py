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


import typing

from edb.edgeql import ast as qlast

from edb import errors

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


LinkTargetDeleteAction = qlast.LinkTargetDeleteAction


def merge_actions(target: so.Object, sources: typing.List[so.Object],
                  field_name: str, *, schema) -> object:
    ours = target.get_explicit_field_value(schema, field_name, None)
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


class Link(sources.Source, pointers.Pointer):

    schema_class_displayname = 'link'

    spectargets = so.SchemaField(
        so.ObjectSet,
        default=so.ObjectSet,
        coerce=True)

    on_target_delete = so.SchemaField(
        LinkTargetDeleteAction,
        default=None, coerce=True, compcoef=0.9,
        merge_fn=merge_actions)

    def init_std_props(self, schema, *, mark_derived=False, dctx=None):

        src_n = sn.Name('std::source')
        pointers = self.get_pointers(schema)

        if not pointers.has(schema, 'source'):
            source_pbase = schema.get(src_n)
            schema, source_p = source_pbase.derive(
                schema, self, self.get_source(schema),
                mark_derived=mark_derived, dctx=dctx)

            schema = self.add_pointer(schema, source_p)

        tgt_n = sn.Name('std::target')
        if not pointers.has(schema, 'target'):
            target_pbase = schema.get(tgt_n)
            schema, target_p = target_pbase.derive(
                schema, self, self.get_target(schema),
                mark_derived=mark_derived, dctx=dctx)

            schema = self.add_pointer(schema, target_p)

        return schema

    def init_derived(self, schema, source, *qualifiers,
                     mark_derived=False, dctx=None,
                     init_props=True, **kwargs):

        schema, ptr = super().init_derived(
            schema, source, *qualifiers,
            mark_derived=mark_derived,
            dctx=dctx, **kwargs)

        if init_props:
            schema = ptr.init_std_props(schema, mark_derived=mark_derived)

        return schema, ptr

    def is_link_property(self, schema):
        return False

    def is_property(self, schema):
        return False

    def scalar(self):
        return False

    def has_user_defined_properties(self, schema):
        return bool([p for p in self.get_pointers(schema).objects(schema)
                     if not p.is_special_pointer(schema)])

    def compare(self, other, *, our_schema, their_schema, context=None):
        if not isinstance(other, Link):
            if isinstance(other, pointers.Pointer):
                return 0.0
            else:
                return NotImplemented

        return super().compare(
            other, our_schema=our_schema,
            their_schema=their_schema, context=context)

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        schema = super().finalize(
            schema, bases=bases, apply_defaults=apply_defaults,
            dctx=dctx)

        if not self.generic(schema) and apply_defaults:
            if self.get_on_target_delete(schema) is None:
                schema = self.set_field_value(
                    schema,
                    'on_target_delete',
                    LinkTargetDeleteAction.RESTRICT)

                if dctx is not None:
                    from . import delta as sd

                    dctx.current().op.add(sd.AlterObjectProperty(
                        property='on_target_delete',
                        new_value=self.get_on_target_delete(schema),
                        source='default'
                    ))

        return schema

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='link'),
            sn.Name(module='schema', name='__type__'),
        )

    @classmethod
    def get_default_base_name(self):
        return sn.Name('std::link')


class DerivedLink(pointers.Pointer, sources.Source):
    pass


class LinkSourceCommandContext(sources.SourceCommandContext):
    pass


class LinkSourceCommand(inheriting.InheritingObjectCommand):
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
    pass


class CreateLink(LinkCommand, referencing.CreateReferencedInheritingObject):
    astnode = [qlast.CreateConcreteLink, qlast.CreateLink]
    referenced_astnode = qlast.CreateConcreteLink

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        from . import objtypes as s_objtypes

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.CreateConcreteLink):
            cmd.add(
                sd.AlterObjectProperty(
                    property='required',
                    new_value=astnode.is_required
                )
            )

            cmd.add(
                sd.AlterObjectProperty(
                    property='cardinality',
                    new_value=astnode.cardinality
                )
            )

            # "source" attribute is set automatically as a refdict back-attr
            parent_ctx = context.get(LinkSourceCommandContext)
            source_name = parent_ctx.op.classname
            target_type = None

            # FIXME: this is an approximate solution
            targets = qlast.get_targets(astnode.target)

            if len(targets) > 1:
                cmd.add(
                    sd.AlterObjectProperty(
                        property='spectargets',
                        new_value=so.ObjectList([
                            utils.ast_to_typeref(
                                t, modaliases=context.modaliases,
                                schema=schema)
                            for t in targets
                        ])
                    )
                )

                target_name = sources.Source.gen_virt_parent_name(
                    (sn.Name(module=t.maintype.module, name=t.maintype.name)
                     for t in targets),
                    module=source_name.module
                )

                target = so.ObjectRef(name=target_name)

                create_virt_parent = s_objtypes.CreateObjectType(
                    classname=target_name,
                    metaclass=s_objtypes.ObjectType
                )

                create_virt_parent.update((
                    sd.AlterObjectProperty(
                        property='bases',
                        new_value=so.ObjectList([
                            so.ObjectRef(name=sn.Name(
                                module='std', name='Object'
                            ))
                        ])
                    ),
                    sd.AlterObjectProperty(
                        property='name',
                        new_value=target_name
                    ),
                    sd.AlterObjectProperty(
                        property='is_virtual',
                        new_value=True
                    )
                ))

                delta_ctx = context.get(sd.DeltaRootContext)

                for cc in delta_ctx.op.get_subcommands(
                        type=s_objtypes.CreateObjectType):
                    if cc.classname == create_virt_parent.classname:
                        break
                else:
                    delta_ctx.op.add(create_virt_parent)
            else:
                target_expr = targets[0]
                if isinstance(target_expr, qlast.TypeName):
                    target = utils.ast_to_typeref(
                        target_expr, modaliases=context.modaliases,
                        schema=schema)
                else:
                    # computable
                    target = cmd._parse_computable(
                        target_expr, schema, context)

            if (isinstance(target, so.ObjectRef) and
                    target.name == source_name):
                # Special case for loop links.  Since the target
                # is the same as the source, we know it's a proper
                # type.
                pass
            else:
                if target_type is None:
                    target_type = utils.resolve_typeref(target, schema=schema)

                if not isinstance(target_type, s_objtypes.ObjectType):
                    raise errors.InvalidLinkTargetError(
                        f'invalid link target, expected object type, got '
                        f'{target_type.__class__.__name__}',
                        context=astnode.target.context
                    )

            cmd.add(
                sd.AlterObjectProperty(
                    property='target',
                    new_value=target
                )
            )

            base_prop_name = sn.Name('std::source')
            s_name = sn.get_specialized_name(base_prop_name, cmd.classname)
            src_prop_name = sn.Name(name=s_name,
                                    module=cmd.classname.module)

            src_prop = lproperties.CreateProperty(
                classname=src_prop_name,
                metaclass=lproperties.Property
            )
            src_prop.update((
                sd.AlterObjectProperty(
                    property='name',
                    new_value=src_prop_name
                ),
                sd.AlterObjectProperty(
                    property='bases',
                    new_value=[
                        so.ObjectRef(
                            name=base_prop_name
                        )
                    ]
                ),
                sd.AlterObjectProperty(
                    property='source',
                    new_value=so.ObjectRef(
                        name=cmd.classname
                    )
                ),
                sd.AlterObjectProperty(
                    property='target',
                    new_value=so.ObjectRef(
                        name=source_name
                    )
                ),
                sd.AlterObjectProperty(
                    property='required',
                    new_value=True
                ),
                sd.AlterObjectProperty(
                    property='readonly',
                    new_value=True
                ),
            ))

            cmd.add(src_prop)

            base_prop_name = sn.Name('std::target')
            s_name = sn.get_specialized_name(base_prop_name, cmd.classname)
            tgt_prop_name = sn.Name(name=s_name,
                                    module=cmd.classname.module)

            tgt_prop = lproperties.CreateProperty(
                classname=tgt_prop_name,
                metaclass=lproperties.Property
            )
            tgt_prop.update((
                sd.AlterObjectProperty(
                    property='name',
                    new_value=tgt_prop_name
                ),
                sd.AlterObjectProperty(
                    property='bases',
                    new_value=[
                        so.ObjectRef(
                            name=base_prop_name
                        )
                    ]
                ),
                sd.AlterObjectProperty(
                    property='source',
                    new_value=so.ObjectRef(
                        name=cmd.classname
                    )
                ),
                sd.AlterObjectProperty(
                    property='target',
                    new_value=target
                ),
                sd.AlterObjectProperty(
                    property='required',
                    new_value=False
                ),
                sd.AlterObjectProperty(
                    property='readonly',
                    new_value=True
                ),
            ))

            cmd.add(tgt_prop)

            cls._parse_default(cmd)

        return cmd

    def _get_ast_node(self, context):
        objtype = context.get(LinkSourceCommandContext)

        if objtype:
            return qlast.CreateConcreteLink
        else:
            return qlast.CreateLink

    def _apply_field_ast(self, schema, context, node, op):
        objtype = context.get(LinkSourceCommandContext)

        if op.property == 'is_derived':
            pass
        elif op.property == 'spectargets':
            if op.new_value:
                node.target = qlast.union_targets(
                    [t.name for t in op.new_value])
        elif op.property == 'default':
            self._encode_default(schema, context, node, op)
        elif op.property == 'required':
            node.is_required = op.new_value
        elif op.property == 'cardinality':
            node.cardinality = op.new_value
        elif op.property == 'source':
            pass
        elif op.property == 'target' and objtype:
            if not node.target:
                t = op.new_value
                node.target = utils.typeref_to_ast(schema, t)
        else:
            super()._apply_field_ast(schema, context, node, op)

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        objtype = context.get(LinkSourceCommandContext)

        if not objtype:
            for op in self.get_subcommands(type=indexes.SourceIndexCommand):
                self._append_subcmd_ast(schema, node, op, context)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(schema, node, op, context)


class RenameLink(LinkCommand, sd.RenameObject):
    pass


class RebaseLink(LinkCommand, inheriting.RebaseInheritingObject):
    pass


class SetTargetDeletePolicy(sd.Command):
    astnode = qlast.OnTargetDelete

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        return sd.AlterObjectProperty(property='on_target_delete')

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd.new_value = astnode.cascade
        return cmd


class AlterTarget(sd.Command):
    astnode = qlast.AlterTarget

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        return sd.AlterObjectProperty(property='target')

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        from . import objtypes as s_objtypes

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        parent_ctx = context.get(LinkSourceCommandContext)
        source_name = parent_ctx.op.classname

        targets = qlast.get_targets(astnode.target)

        if len(targets) > 1:
            alter_ptr_ctx = context.get(pointers.PointerCommandContext)

            alter_ptr_ctx.op.add(
                sd.AlterObjectProperty(
                    property='spectargets',
                    new_value=so.ObjectList([
                        so.ObjectRef(
                            name=sn.Name(
                                module=t.module,
                                name=t.name
                            )
                        )
                        for t in targets
                    ])
                )
            )

            target_name = sources.Source.gen_virt_parent_name(
                (sn.Name(module=t.module, name=t.name)
                 for t in targets),
                module=source_name.module
            )

            target = so.ObjectRef(name=target_name)

            create_virt_parent = s_objtypes.CreateObjectType(
                classname=target_name,
                metaclass=s_objtypes.ObjectType
            )

            create_virt_parent.update((
                sd.AlterObjectProperty(
                    property='name',
                    new_value=target_name
                ),
                sd.AlterObjectProperty(
                    property='is_virtual',
                    new_value=True
                ),
                sd.AlterObjectProperty(
                    property='is_derived',
                    new_value=True
                )
            ))

            delta_ctx = context.get(sd.DeltaRootContext)

            for cc in delta_ctx.op(s_objtypes.CreateObjectType):
                if cc.classname == create_virt_parent.classname:
                    break
            else:
                delta_ctx.op.add(create_virt_parent)
        else:
            target = targets[0]

        cmd.new_value = target

        return cmd


class AlterLink(LinkCommand, sd.AlterObject):
    astnode = [qlast.AlterLink, qlast.AlterConcreteLink]
    referenced_astnode = qlast.AlterConcreteLink

    def _get_ast_node(self, context):
        objtype = context.get(LinkSourceCommandContext)

        if objtype:
            return qlast.AlterConcreteLink
        else:
            return qlast.AlterLink

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        objtype = context.get(LinkSourceCommandContext)

        if not objtype:
            for op in self.get_subcommands(type=indexes.SourceIndexCommand):
                self._append_subcmd_ast(schema, node, op, context)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(schema, node, op, context)

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'spectargets':
            if op.new_value:
                node.commands.append(qlast.AlterTarget(
                    targets=[
                        qlast.ObjectRef(name=t.classname.name,
                                        module=t.classname.module)
                        for t in op.new_value
                    ]
                ))
        elif op.property == 'target':
            if op.new_value:
                node.commands.append(qlast.AlterTarget(
                    targets=[
                        qlast.ObjectRef(
                            name=op.new_value.classname.name,
                            module=op.new_value.classname.module)
                    ]
                ))
        elif op.property == 'source':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)


class DeleteLink(LinkCommand, sd.DeleteObject):
    astnode = [qlast.DropLink, qlast.DropConcreteLink]
    referenced_astnode = qlast.DropConcreteLink

    def _get_ast_node(self, context):
        objtype = context.get(LinkSourceCommandContext)

        if objtype:
            return qlast.DropConcreteLink
        else:
            return qlast.DropLink

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        objtype = context.get(LinkSourceCommandContext)

        for op in self.get_subcommands(type=lproperties.PropertyCommand):
            self._append_subcmd_ast(schema, node, op, context)

        if not objtype:
            for op in self.get_subcommands(type=indexes.SourceIndexCommand):
                self._append_subcmd_ast(schema, node, op, context)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(schema, node, op, context)
