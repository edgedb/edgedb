##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.functional import hybridmethod

from metamagic.caos.lang.caosql import ast as qlast

from . import atoms
from . import constraints
from . import delta as sd
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import pointers
from . import policy
from . import sources


class LinkPropertySourceContext(sources.SourceCommandContext):
    pass


class LinkPropertyCommandContext(pointers.PointerCommandContext,
                                 constraints.ConsistencySubjectCommandContext):
    pass


class LinkPropertyCommand(pointers.PointerCommand):
    context_class = LinkPropertyCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return LinkProperty


class CreateLinkProperty(LinkPropertyCommand, named.CreateNamedPrototype):
    astnode = [qlast.CreateConcreteLinkPropertyNode,
               qlast.CreateLinkPropertyNode]

    @classmethod
    def _protobases_from_ast(cls, astnode, context):
        proto_name = '{}.{}'.format(astnode.name.module, astnode.name.name)

        if isinstance(astnode, qlast.CreateConcreteLinkPropertyNode):
            nname = LinkProperty.normalize_name(proto_name)

            bases = so.PrototypeList([
                so.PrototypeRef(
                    prototype_name=sn.Name(
                        module=nname.module,
                        name=nname.name
                    )
                )
            ])
        else:
            bases = super()._protobases_from_ast(astnode, context)
            if not bases:
                if proto_name != 'metamagic.caos.builtins.link_property':
                    bases = so.PrototypeList([
                        so.PrototypeRef(
                            prototype_name=sn.Name(
                                module='metamagic.caos.builtins',
                                name='link_property'
                            )
                        )
                    ])

        return bases

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        if isinstance(astnode, qlast.CreateConcreteLinkPropertyNode):
            target = getattr(astnode, 'target', None)

            cmd.add(
                sd.AlterPrototypeProperty(
                    property='required',
                    new_value=astnode.is_required
                )
            )

            parent_ctx = context.get(LinkPropertySourceContext)
            source_name = parent_ctx.op.prototype_name

            cmd.add(
                sd.AlterPrototypeProperty(
                    property='source',
                    new_value=so.PrototypeRef(
                        prototype_name=source_name
                    )
                )
            )

            cmd.add(
                sd.AlterPrototypeProperty(
                    property='target',
                    new_value=so.PrototypeRef(
                        prototype_name=sn.Name(
                            module=target.module,
                            name=target.name
                        )
                    )
                )
            )

            cls._parse_default(cmd)

        return cmd

    def _get_ast_node(self, context):
        link = context.get(LinkPropertySourceContext)
        if link:
            return qlast.CreateConcreteLinkPropertyNode
        else:
            return qlast.CreateLinkPropertyNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
        link = context.get(LinkPropertySourceContext)

        if op.property == 'is_derived':
            pass
        elif op.property == 'default':
            self._encode_default(context, node, op)
        elif op.property == 'required':
            node.is_required = op.new_value
        elif op.property == 'source':
            pass
        elif op.property == 'target' and link:
            t = op.new_value.prototype_name
            node.target = qlast.PrototypeRefNode(name=t.name, module=t.module)
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context):
        prop = named.CreateNamedPrototype.apply(self, schema, context)

        link = context.get(LinkPropertySourceContext)

        if link:
            prop.source = link.proto

        with context(LinkPropertyCommandContext(self, prop)):
            for op in self(atoms.AtomCommand):
                op.apply(schema, context=context)

            prop.acquire_ancestor_inheritance(schema)

            if link:
                link.proto.add_pointer(prop)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

        return prop


class RenameLinkProperty(LinkPropertyCommand, named.RenameNamedPrototype):
    def apply(self, schema, context):
        result = super().apply(schema, context)

        if not result.generic():
            link_ctx = context.get(LinkPropertySourceContext)
            assert link_ctx, "LinkProperty commands must be run in " + \
                             "Link context"

            norm = LinkProperty.normalize_name

            link = link_ctx.proto
            own = link.own_pointers.pop(norm(self.prototype_name), None)
            if own:
                link.own_pointers[norm(self.new_name)] = own

            for child in link.children(schema):
                ptr = child.pointers.pop(norm(self.prototype_name), None)
                if ptr is not None:
                    child.pointers[norm(self.new_name)] = ptr

            inherited = link.pointers.pop(norm(self.prototype_name), None)
            if inherited is not None:
                link.pointers[norm(self.new_name)] = inherited

        return result


class RebaseLinkProperty(LinkPropertyCommand,
                         inheriting.RebaseNamedPrototype):
    pass


class AlterLinkProperty(LinkPropertyCommand, named.AlterNamedPrototype):
    astnode = [qlast.AlterConcreteLinkPropertyNode,
               qlast.AlterLinkPropertyNode]

    def _get_ast_node(self, context):
        concept = context.get(LinkPropertySourceContext)

        if concept:
            return qlast.AlterConcreteLinkPropertyNode
        else:
            return qlast.AlterLinkPropertyNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'target':
            if op.new_value:
                node.commands.append(qlast.AlterTargetNode(
                    targets=[
                        qlast.PrototypeRefNode(
                            name=op.new_value.prototype_name.name,
                            module=op.new_value.prototype_name.module)
                    ]
                ))
        elif op.property == 'source':
            pass
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()
        with context(LinkPropertyCommandContext(self, None)):
            prop = super().apply(schema, context)

            for op in self(atoms.AtomCommand):
                op.apply(schema, context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

            for op in self(inheriting.RebaseNamedPrototype):
                op.apply(schema, context)

        return prop


class DeleteLinkProperty(LinkPropertyCommand, named.DeleteNamedPrototype):
    astnode = [qlast.DropConcreteLinkPropertyNode,
               qlast.DropLinkPropertyNode]

    def _get_ast_node(self, context):
        concept = context.get(LinkPropertySourceContext)

        if concept:
            return qlast.DropConcreteLinkPropertyNode
        else:
            return qlast.DropLinkPropertyNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def apply(self, schema, context):
        prop = super().apply(schema, context)

        link = context.get(LinkPropertySourceContext)

        with context(LinkPropertyCommandContext(self, prop)):
            for op in self(atoms.AtomCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

        if link:
            link.proto.del_pointer(prop, schema)

        return prop


class TypeProperty(pointers.Pointer):
    pass


class LinkProperty(pointers.Pointer):
    _type = 'link_property'

    delta_driver = sd.DeltaDriver(
        create=CreateLinkProperty,
        alter=AlterLinkProperty,
        rebase=RebaseLinkProperty,
        rename=RenameLinkProperty,
        delete=DeleteLinkProperty
    )

    def derive(self, schema, source, target=None, **kwargs):
        if target is None:
            target = self.target

        ptr = super().derive(schema, source, target, **kwargs)

        if ptr.normal_name() == 'metamagic.caos.builtins.source':
            ptr.target = source.source

        if ptr.normal_name() == 'metamagic.caos.builtins.target':
            ptr.target = source.target

        return ptr

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2):
        if ptr.is_endpoint_pointer():
            return t1
        else:
            return super().merge_targets(schema, ptr, t1, t2)

    def merge_specialized(self, schema, other, relaxed=False):
        if isinstance(other, LinkProperty):
            self.required = max(self.required, other.required)
            self.merge_defaults(other)
        return self

    def get_metaclass(self, proto_schema):
        from metamagic.caos.link import LinkPropertyMeta
        return LinkPropertyMeta

    def atomic(self):
        assert not self.generic(), \
               "atomicity is not determined for generic pointers"
        return isinstance(self.target, atoms.Atom)

    def singular(self, direction=pointers.PointerDirection.Outbound):
        return True

    def get_exposed_behaviour(self):
        return pointers.PointerExposedBehaviour.FirstItem

    @hybridmethod
    def copy(scope, obj=None):
        if isinstance(scope, LinkProperty):
            obj = scope
            cls = obj.__class__.get_canonical_class()
        else:
            cls = scope = scope.get_canonical_class()

        result = super(cls, cls).copy(obj)

        result.source = obj.source
        result.target = obj.target
        result.default = obj.default[:]
        return result
