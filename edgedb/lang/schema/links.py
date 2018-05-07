##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast

from . import constraints
from . import database as s_db
from . import delta as sd
from . import error as s_err
from . import indexes
from . import inheriting
from . import lproperties
from . import name as sn
from . import named
from . import objects as so
from . import pointers
from . import policy
from . import referencing
from . import sources
from . import utils


class Link(sources.Source, pointers.Pointer):
    _type = 'link'

    spectargets = so.Field(named.NamedObjectSet, named.NamedObjectSet,
                           coerce=True)

    @classmethod
    def get_special_pointers(cls):
        return (sn.Name('std::source'),
                sn.Name('std::target'),
                sn.Name('std::linkid'),)

    def init_std_props(self, schema, *, mark_derived=False,
                       add_to_schema=False, dctx=None):

        src_n = sn.Name('std::source')
        if src_n not in self.pointers:
            source_pbase = schema.get(src_n)
            source_p = source_pbase.derive(
                schema, self, self.source, mark_derived=mark_derived,
                add_to_schema=add_to_schema, dctx=dctx)

            self.add_pointer(source_p)

        tgt_n = sn.Name('std::target')
        if tgt_n not in self.pointers:
            target_pbase = schema.get(tgt_n)
            target_p = target_pbase.derive(
                schema, self, self.target, mark_derived=mark_derived,
                add_to_schema=add_to_schema, dctx=dctx)

            self.add_pointer(target_p)

    def init_derived(self, schema, source, *qualifiers,
                     mark_derived=False, add_to_schema=False,
                     dctx=None, init_props=True, **kwargs):

        ptr = super().init_derived(
            schema, source, *qualifiers, mark_derived=mark_derived,
            add_to_schema=add_to_schema, dctx=dctx, **kwargs)

        if init_props:
            ptr.init_std_props(schema, mark_derived=mark_derived,
                               add_to_schema=add_to_schema)

        return ptr

    def is_link_property(self):
        return False

    def scalar(self):
        return False

    def has_user_defined_properties(self):
        return bool([p for p in self.pointers.values()
                     if not p.is_special_pointer()])

    def compare(self, other, context=None):
        if not isinstance(other, Link):
            if isinstance(other, pointers.Pointer):
                return 0.0
            else:
                return NotImplemented

        return super().compare(other, context=context)

    def copy(self):
        result = super().copy()
        result.source = self.source
        result.target = self.target
        result.default = self.default

        return result

    def finalize(self, schema, bases=None, *, dctx=None):
        super().finalize(schema, bases=bases, dctx=dctx)

        if not self.generic() and self.cardinality is None:
            self.cardinality = pointers.PointerCardinality.ManyToOne

            if dctx is not None:
                from . import delta as sd

                dctx.current().op.add(sd.AlterObjectProperty(
                    property='cardinality',
                    new_value=self.cardinality,
                    source='default'
                ))

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


class LinkSourceCommand(referencing.ReferencingObjectCommand):
    pass


class LinkCommandContext(pointers.PointerCommandContext,
                         constraints.ConsistencySubjectCommandContext,
                         policy.InternalPolicySubjectCommandContext,
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
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        from edgedb.lang.edgeql import utils as ql_utils
        from edgedb.lang.ir import ast as irast
        from edgedb.lang.ir import inference as ir_inference
        from edgedb.lang.ir import utils as ir_utils
        from . import objtypes as s_objtypes

        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if isinstance(astnode, qlast.CreateConcreteLink):
            cmd.add(
                sd.AlterObjectProperty(
                    property='required',
                    new_value=astnode.is_required
                )
            )

            # "source" attribute is set automatically as a refdict back-attr
            parent_ctx = context.get(LinkSourceCommandContext)
            source_name = parent_ctx.op.classname
            target_type = None

            if len(astnode.targets) > 1:
                cmd.add(
                    sd.AlterObjectProperty(
                        property='spectargets',
                        new_value=so.ObjectList([
                            utils.ast_to_typeref(
                                t, modaliases=context.modaliases,
                                schema=schema)
                            for t in astnode.targets
                        ])
                    )
                )

                target_name = sources.Source.gen_virt_parent_name(
                    (sn.Name(module=t.maintype.module, name=t.maintype.name)
                     for t in astnode.targets),
                    module=source_name.module
                )

                target = so.ObjectRef(classname=target_name)

                create_virt_parent = s_objtypes.CreateObjectType(
                    classname=target_name,
                    metaclass=s_objtypes.ObjectType
                )

                create_virt_parent.update((
                    sd.AlterObjectProperty(
                        property='bases',
                        new_value=so.ObjectList([
                            so.ObjectRef(classname=sn.Name(
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

                alter_db_ctx = context.get(s_db.DatabaseCommandContext)

                for cc in alter_db_ctx.op.get_subcommands(
                        type=s_objtypes.CreateObjectType):
                    if cc.classname == create_virt_parent.classname:
                        break
                else:
                    alter_db_ctx.op.add(create_virt_parent)
            else:
                target_expr = astnode.targets[0]
                if isinstance(target_expr, qlast.TypeName):
                    target = utils.ast_to_typeref(
                        target_expr, modaliases=context.modaliases,
                        schema=schema)
                else:
                    # computable
                    source = schema.get(source_name, default=None)
                    if source is None:
                        raise s_err.SchemaDefinitionError(
                            f'cannot define link computables in CREATE TYPE',
                            hint='Perform a CREATE TYPE without the link '
                                 'followed by ALTER TYPE defining the '
                                 'computable',
                            context=target_expr.context
                        )

                    ir, _, target_expr = ql_utils.normalize_tree(
                        target_expr, schema,
                        anchors={qlast.Source: source})

                    try:
                        target_type = ir_utils.infer_type(ir, schema)
                    except edgeql.EdgeQLError as e:
                        raise s_err.SchemaDefinitionError(
                            'could not determine the result type of '
                            'computable expression',
                            context=target_expr.context) from e

                    target = utils.reduce_to_typeref(target_type)

                    cmd.add(
                        sd.AlterObjectProperty(
                            property='default',
                            new_value=target_expr
                        )
                    )

                    cmd.add(
                        sd.AlterObjectProperty(
                            property='computable',
                            new_value=True
                        )
                    )

                    singletons = {
                        irast.PathId(source)
                    }

                    cardinality = ir_inference.infer_cardinality(
                        ir, singletons, schema)

                    if cardinality == qlast.Cardinality.ONE:
                        link_card = pointers.PointerCardinality.ManyToOne
                    else:
                        link_card = pointers.PointerCardinality.ManyToMany

                    cmd.add(
                        sd.AlterObjectProperty(
                            property='cardinality',
                            new_value=link_card
                        )
                    )

            if (isinstance(target, so.ObjectRef) and
                    target.classname == source_name):
                # Special case for loop links.  Since the target
                # is the same as the source, we know it's a proper
                # type.
                pass
            else:
                if target_type is None:
                    target_type = utils.resolve_typeref(target, schema=schema)

                if not isinstance(target_type, s_objtypes.ObjectType):
                    raise s_err.SchemaDefinitionError(
                        f'invalid link target, expected object type, got '
                        f'{target_type.__class__.__name__}',
                        context=astnode.targets[0].context
                    )

            cmd.add(
                sd.AlterObjectProperty(
                    property='target',
                    new_value=target
                )
            )

            base_prop_name = sn.Name('std::source')
            s_name = lproperties.Property.get_specialized_name(
                base_prop_name, cmd.classname)
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
                            classname=base_prop_name
                        )
                    ]
                ),
                sd.AlterObjectProperty(
                    property='source',
                    new_value=so.ObjectRef(
                        classname=cmd.classname
                    )
                ),
                sd.AlterObjectProperty(
                    property='target',
                    new_value=so.ObjectRef(
                        classname=source_name
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
            s_name = lproperties.Property.get_specialized_name(
                base_prop_name, cmd.classname)
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
                            classname=base_prop_name
                        )
                    ]
                ),
                sd.AlterObjectProperty(
                    property='source',
                    new_value=so.ObjectRef(
                        classname=cmd.classname
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

    def _apply_field_ast(self, context, node, op):
        objtype = context.get(LinkSourceCommandContext)

        if op.property == 'is_derived':
            pass
        elif op.property == 'spectargets':
            if op.new_value:
                node.targets = [
                    qlast.ObjectRef(name=t.classname.name,
                                    module=t.classname.module)
                    for t in op.new_value
                ]
        elif op.property == 'default':
            self._encode_default(context, node, op)
        elif op.property == 'required':
            node.is_required = op.new_value
        elif op.property == 'source':
            pass
        elif op.property == 'search':
            if op.new_value:
                v = qlast.Constant(value=str(op.new_value.weight))
                self._set_attribute_ast(context, node, 'search_weight', v)
        elif op.property == 'target' and objtype:
            if not node.targets:
                t = op.new_value
                node.targets = [utils.typeref_to_ast(t)]
        else:
            super()._apply_field_ast(context, node, op)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        objtype = context.get(LinkSourceCommandContext)

        if not objtype:
            for op in self.get_subcommands(type=indexes.SourceIndexCommand):
                self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)


class RenameLink(LinkCommand, named.RenameNamedObject):
    pass


class RebaseLink(LinkCommand, inheriting.RebaseNamedObject):
    pass


class AlterTarget(sd.Command):
    astnode = qlast.AlterTarget

    @classmethod
    def _cmd_from_ast(cls, astnode, context, schema):
        return sd.AlterObjectProperty(property='target')

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        from . import objtypes as s_objtypes

        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        parent_ctx = context.get(LinkSourceCommandContext)
        source_name = parent_ctx.op.classname

        if len(astnode.targets) > 1:
            alter_ptr_ctx = context.get(pointers.PointerCommandContext)

            alter_ptr_ctx.op.add(
                sd.AlterObjectProperty(
                    property='spectargets',
                    new_value=so.ObjectList([
                        so.ObjectRef(
                            classname=sn.Name(
                                module=t.module,
                                name=t.name
                            )
                        )
                        for t in astnode.targets
                    ])
                )
            )

            target_name = sources.Source.gen_virt_parent_name(
                (sn.Name(module=t.module, name=t.name)
                 for t in astnode.targets),
                module=source_name.module
            )

            target = so.ObjectRef(classname=target_name)

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

            alter_db_ctx = context.get(s_db.DatabaseCommandContext)

            for cc in alter_db_ctx.op(s_objtypes.CreateObjectType):
                if cc.classname == create_virt_parent.classname:
                    break
            else:
                alter_db_ctx.op.add(create_virt_parent)
        else:
            target = so.ObjectRef(
                classname=sn.Name(
                    module=astnode.targets[0].module,
                    name=astnode.targets[0].name
                )
            )

        cmd.new_value = target

        return cmd


class AlterLink(LinkCommand, named.AlterNamedObject):
    astnode = [qlast.AlterLink, qlast.AlterConcreteLink]
    referenced_astnode = qlast.AlterConcreteLink

    def _get_ast_node(self, context):
        objtype = context.get(LinkSourceCommandContext)

        if objtype:
            return qlast.AlterConcreteLink
        else:
            return qlast.AlterLink

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        objtype = context.get(LinkSourceCommandContext)

        if not objtype:
            for op in self.get_subcommands(type=indexes.SourceIndexCommand):
                self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
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
        elif op.property == 'search':
            if op.new_value:
                v = qlast.Constant(value=str(op.new_value.weight))
                self._set_attribute_ast(context, node, 'search_weight', v)
            else:
                self._drop_attribute_ast(context, node, 'search_weight')
        else:
            super()._apply_field_ast(context, node, op)


class DeleteLink(LinkCommand, named.DeleteNamedObject):
    astnode = [qlast.DropLink, qlast.DropConcreteLink]
    referenced_astnode = qlast.DropConcreteLink

    def _get_ast_node(self, context):
        objtype = context.get(LinkSourceCommandContext)

        if objtype:
            return qlast.DropConcreteLink
        else:
            return qlast.DropLink

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        objtype = context.get(LinkSourceCommandContext)

        for op in self.get_subcommands(type=lproperties.PropertyCommand):
            self._append_subcmd_ast(node, op, context)

        if not objtype:
            for op in self.get_subcommands(type=indexes.SourceIndexCommand):
                self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)
