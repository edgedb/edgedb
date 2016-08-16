##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.functional import hybridmethod

from edgedb.lang.edgeql import ast as qlast

from . import atoms
from . import constraints
from . import delta as sd
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import pointers
from . import policy
from . import referencing
from . import sources


class LinkPropertySourceContext(sources.SourceCommandContext):
    pass


class LinkPropertySourceCommand(sd.PrototypeCommand):
    def _create_innards(self, schema, context):
        for op in self(LinkPropertyCommand):
            op.apply(schema, context=context)

        super()._create_innards(schema, context)

    def _alter_innards(self, schema, context, prototype):
        for op in self(LinkPropertyCommand):
            op.apply(schema, context=context)

        super()._alter_innards(schema, context, prototype)

    def _delete_innards(self, schema, context, prototype):
        super()._delete_innards(schema, context, prototype)

        for op in self(LinkPropertyCommand):
            op.apply(schema, context=context)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(LinkPropertyCommand):
            self._append_subcmd_ast(node, op, context)


class LinkPropertyCommandContext(pointers.PointerCommandContext,
                                 constraints.ConsistencySubjectCommandContext):
    pass


class LinkPropertyCommand(pointers.PointerCommand):
    context_class = LinkPropertyCommandContext
    referrer_context_class = LinkPropertySourceContext

    @classmethod
    def _get_prototype_class(cls):
        return LinkProperty


class CreateLinkProperty(LinkPropertyCommand,
                         referencing.CreateReferencedPrototype):
    astnode = [qlast.CreateConcreteLinkPropertyNode,
               qlast.CreateLinkPropertyNode]

    referenced_astnode = qlast.CreateConcreteLinkPropertyNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

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


class RenameLinkProperty(LinkPropertyCommand, named.RenameNamedPrototype):
    pass


class RebaseLinkProperty(LinkPropertyCommand,
                         inheriting.RebaseNamedPrototype):
    pass


class AlterLinkProperty(LinkPropertyCommand,
                        inheriting.AlterInheritingPrototype):
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


class DeleteLinkProperty(LinkPropertyCommand,
                         inheriting.DeleteInheritingPrototype):
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

    def derive(self, schema, source, target=None, attrs=None, **kwargs):
        if target is None:
            target = self.target

        ptr = super().derive(schema, source, target, attrs=attrs, **kwargs)

        if ptr.normal_name() == 'std::source':
            ptr.target = source.source

        if ptr.normal_name() == 'std::target':
            ptr.target = source.target

        return ptr

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2):
        if ptr.is_endpoint_pointer():
            return t1
        else:
            return super().merge_targets(schema, ptr, t1, t2)

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
        result.default = obj.default
        return result

    @classmethod
    def get_default_base_name(self):
        return 'std::linkproperty'
