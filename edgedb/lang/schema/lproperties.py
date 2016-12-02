##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import hybrid

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


class LinkPropertySourceCommand(sd.ClassCommand):
    def _create_innards(self, schema, context):
        for op in self.get_subcommands(type=LinkPropertyCommand):
            op.apply(schema, context=context)

        super()._create_innards(schema, context)

    def _alter_innards(self, schema, context, scls):
        for op in self.get_subcommands(type=LinkPropertyCommand):
            op.apply(schema, context=context)

        super()._alter_innards(schema, context, scls)

    def _delete_innards(self, schema, context, scls):
        super()._delete_innards(schema, context, scls)

        for op in self.get_subcommands(type=LinkPropertyCommand):
            op.apply(schema, context=context)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self.get_subcommands(type=LinkPropertyCommand):
            self._append_subcmd_ast(node, op, context)


class LinkPropertyCommandContext(pointers.PointerCommandContext,
                                 constraints.ConsistencySubjectCommandContext):
    pass


class LinkPropertyCommand(pointers.PointerCommand):
    context_class = LinkPropertyCommandContext
    referrer_context_class = LinkPropertySourceContext

    @classmethod
    def _get_metaclass(cls):
        return LinkProperty


class CreateLinkProperty(LinkPropertyCommand,
                         referencing.CreateReferencedClass):
    astnode = [qlast.CreateConcreteLinkPropertyNode,
               qlast.CreateLinkPropertyNode]

    referenced_astnode = qlast.CreateConcreteLinkPropertyNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if isinstance(astnode, qlast.CreateConcreteLinkPropertyNode):
            target = getattr(astnode, 'target', None)

            cmd.add(
                sd.AlterClassProperty(
                    property='required',
                    new_value=astnode.is_required
                )
            )

            parent_ctx = context.get(LinkPropertySourceContext)
            source_name = parent_ctx.op.classname

            cmd.add(
                sd.AlterClassProperty(
                    property='source',
                    new_value=so.ClassRef(
                        classname=source_name
                    )
                )
            )

            cmd.add(
                sd.AlterClassProperty(
                    property='target',
                    new_value=so.ClassRef(
                        classname=sn.Name(
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

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
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
            t = op.new_value.classname
            node.target = qlast.ClassRefNode(name=t.name, module=t.module)
        else:
            super()._apply_field_ast(context, node, op)


class RenameLinkProperty(LinkPropertyCommand, named.RenameNamedClass):
    pass


class RebaseLinkProperty(LinkPropertyCommand,
                         inheriting.RebaseNamedClass):
    pass


class AlterLinkProperty(LinkPropertyCommand,
                        inheriting.AlterInheritingClass):
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

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'target':
            if op.new_value:
                node.commands.append(qlast.AlterTargetNode(
                    targets=[
                        qlast.ClassRefNode(
                            name=op.new_value.classname.name,
                            module=op.new_value.classname.module)
                    ]
                ))
        elif op.property == 'source':
            pass
        else:
            super()._apply_field_ast(context, node, op)


class DeleteLinkProperty(LinkPropertyCommand,
                         inheriting.DeleteInheritingClass):
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

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)


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

        if ptr.shortname == 'std::source':
            ptr.target = source.source

        if ptr.shortname == 'std::target':
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

    @hybrid.method
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
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='linkproperty'),
        )

    @classmethod
    def get_default_base_name(self):
        return 'std::linkproperty'
