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
from . import database as s_db
from . import delta as sd
from edgedb.lang.common import enum
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


class LinkSearchWeight(enum.StrEnum):
    A = 'A'
    B = 'B'
    C = 'C'
    D = 'D'


class LinkMapping(enum.StrEnum):
    OneToOne = '11'
    OneToMany = '1*'
    ManyToOne = '*1'
    ManyToMany = '**'

    def __and__(self, other):
        if not isinstance(other, LinkMapping):
            return NotImplemented

        if self == LinkMapping.OneToOne:
            return self
        elif other == LinkMapping.OneToOne:
            return other
        elif self == LinkMapping.OneToMany:
            if other == LinkMapping.ManyToOne:
                err = 'mappings %r and %r are mutually incompatible'
                raise ValueError(err % (self, other))
            return self
        elif self == LinkMapping.ManyToOne:
            if other == LinkMapping.OneToMany:
                err = 'mappings %r and %r are mutually incompatible'
                raise ValueError(err % (self, other))
            return self
        else:
            return other

    def __or__(self, other):
        if not isinstance(other, LinkMapping):
            return NotImplemented
        # We use the fact that '*' is less than '1'
        return self.__class__(min(self[0], other[0]) + min(self[1], other[1]))

    @classmethod
    def merge_values(cls, ours, theirs, schema):
        if ours and theirs and ours != theirs:
            result = ours & theirs
        elif not ours and theirs:
            result = theirs
        else:
            result = ours

        return result


class LinkSearchConfiguration(so.Class):
    weight = so.Field(LinkSearchWeight, default=None, compcoef=0.9,
                      introspectable=False)


class LinkSourceCommandContext(sources.SourceCommandContext):
    pass


class LinkSourceCommand(sd.ClassCommand):
    def _create_innards(self, schema, context):
        super()._create_innards(schema, context)

        for op in self(LinkCommand):
            op.apply(schema, context=context)

    def _alter_innards(self, schema, context, scls):
        super()._alter_innards(schema, context, scls)

        for op in self(LinkCommand):
            op.apply(schema, context=context)

    def _delete_innards(self, schema, context, scls):
        super()._delete_innards(schema, context, scls)

        for op in self(LinkCommand):
            op.apply(schema, context=context)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(LinkCommand):
            self._append_subcmd_ast(node, op, context)


class LinkCommandContext(pointers.PointerCommandContext,
                         constraints.ConsistencySubjectCommandContext,
                         policy.InternalPolicySubjectCommandContext,
                         lproperties.LinkPropertySourceContext,
                         indexes.IndexSourceCommandContext):
    pass


class LinkCommand(lproperties.LinkPropertySourceCommand,
                  pointers.PointerCommand):
    context_class = LinkCommandContext
    referrer_context_class = LinkSourceCommandContext

    @classmethod
    def _get_metaclass(cls):
        return Link


class CreateLink(LinkCommand, referencing.CreateReferencedClass):
    astnode = [qlast.CreateConcreteLinkNode, qlast.CreateLinkNode]
    referenced_astnode = qlast.CreateConcreteLinkNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        from . import concepts

        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if isinstance(astnode, qlast.CreateConcreteLinkNode):
            cmd.add(
                sd.AlterClassProperty(
                    property='required',
                    new_value=astnode.is_required
                )
            )

            # "source" attribute is set automatically as a refdict back-attr
            parent_ctx = context.get(LinkSourceCommandContext)
            source_name = parent_ctx.op.classname

            for ap in cmd(sd.AlterClassProperty):
                if ap.property == 'search_weight':
                    ap.property = 'search'
                    ap.new_value = LinkSearchConfiguration(
                        weight=LinkSearchWeight(
                            ap.new_value
                        )
                    )
                    break

            if len(astnode.targets) > 1:
                cmd.add(
                    sd.AlterClassProperty(
                        property='spectargets',
                        new_value=so.ClassList([
                            so.ClassRef(
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

                target = so.ClassRef(classname=target_name)

                create_virt_parent = concepts.CreateConcept(
                    classname=target_name,
                    metaclass=concepts.Concept
                )

                create_virt_parent.update((
                    sd.AlterClassProperty(
                        property='name',
                        new_value=target_name
                    ),
                    sd.AlterClassProperty(
                        property='is_virtual',
                        new_value=True
                    ),
                    sd.AlterClassProperty(
                        property='is_derived',
                        new_value=True
                    )
                ))

                alter_db_ctx = context.get(s_db.DatabaseCommandContext)

                for cc in alter_db_ctx.op(concepts.CreateConcept):
                    if cc.classname == create_virt_parent.classname:
                        break
                else:
                    alter_db_ctx.op.add(create_virt_parent)
            else:
                target = so.ClassRef(
                    classname=sn.Name(
                        module=astnode.targets[0].module,
                        name=astnode.targets[0].name
                    )
                )

            cmd.add(
                sd.AlterClassProperty(
                    property='target',
                    new_value=target
                )
            )

            base_prop_name = sn.Name('std::source')
            s_name = lproperties.LinkProperty.generate_specialized_name(
                cmd.classname, base_prop_name)
            src_prop_name = sn.Name(name=s_name,
                                    module=cmd.classname.module)

            src_prop = lproperties.CreateLinkProperty(
                classname=src_prop_name,
                metaclass=lproperties.LinkProperty
            )
            src_prop.update((
                sd.AlterClassProperty(
                    property='name',
                    new_value=src_prop_name
                ),
                sd.AlterClassProperty(
                    property='bases',
                    new_value=[
                        so.ClassRef(
                            classname=base_prop_name
                        )
                    ]
                ),
                sd.AlterClassProperty(
                    property='source',
                    new_value=so.ClassRef(
                        classname=cmd.classname
                    )
                ),
                sd.AlterClassProperty(
                    property='target',
                    new_value=so.ClassRef(
                        classname=source_name
                    )
                ),
                sd.AlterClassProperty(
                    property='required',
                    new_value=True
                ),
                sd.AlterClassProperty(
                    property='readonly',
                    new_value=True
                ),
                sd.AlterClassProperty(
                    property='loading',
                    new_value='eager'
                )
            ))

            cmd.add(src_prop)

            base_prop_name = sn.Name('std::target')
            s_name = lproperties.LinkProperty.generate_specialized_name(
                cmd.classname, base_prop_name)
            tgt_prop_name = sn.Name(name=s_name,
                                    module=cmd.classname.module)

            tgt_prop = lproperties.CreateLinkProperty(
                classname=tgt_prop_name,
                metaclass=lproperties.LinkProperty
            )
            tgt_prop.update((
                sd.AlterClassProperty(
                    property='name',
                    new_value=tgt_prop_name
                ),
                sd.AlterClassProperty(
                    property='bases',
                    new_value=[
                        so.ClassRef(
                            classname=base_prop_name
                        )
                    ]
                ),
                sd.AlterClassProperty(
                    property='source',
                    new_value=so.ClassRef(
                        classname=cmd.classname
                    )
                ),
                sd.AlterClassProperty(
                    property='target',
                    new_value=target
                ),
                sd.AlterClassProperty(
                    property='required',
                    new_value=False
                ),
                sd.AlterClassProperty(
                    property='readonly',
                    new_value=True
                ),
                sd.AlterClassProperty(
                    property='loading',
                    new_value='eager'
                )
            ))

            cmd.add(tgt_prop)

            cls._parse_default(cmd)

        return cmd

    def _get_ast_node(self, context):
        concept = context.get(LinkSourceCommandContext)

        if concept:
            return qlast.CreateConcreteLinkNode
        else:
            return qlast.CreateLinkNode

    def _apply_field_ast(self, context, node, op):
        concept = context.get(LinkSourceCommandContext)

        if op.property == 'is_derived':
            pass
        elif op.property == 'spectargets':
            if op.new_value:
                node.targets = [
                    qlast.ClassRefNode(name=t.classname.name,
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
                v = qlast.ConstantNode(value=str(op.new_value.weight))
                self._set_attribute_ast(context, node, 'search_weight', v)
        elif op.property == 'target' and concept:
            if not node.targets:
                t = op.new_value.classname
                node.targets = [
                    qlast.ClassRefNode(name=t.name, module=t.module)
                ]
        else:
            super()._apply_field_ast(context, node, op)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        concept = context.get(LinkSourceCommandContext)

        for op in self(lproperties.LinkPropertyCommand):
            name = op.metaclass.normalize_name(op.classname)
            if name not in {'std::source',
                            'std::target'}:
                self._append_subcmd_ast(node, op, context)

        if not concept:
            for op in self(indexes.SourceIndexCommand):
                self._append_subcmd_ast(node, op, context)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)


class RenameLink(LinkCommand, named.RenameNamedClass):
    pass


class RebaseLink(LinkCommand, inheriting.RebaseNamedClass):
    pass


class AlterTarget(sd.Command):
    astnode = qlast.AlterTargetNode

    @classmethod
    def _cmd_from_ast(cls, astnode, context, schema):
        return sd.AlterClassProperty(property='target')

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        from . import concepts

        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        parent_ctx = context.get(LinkSourceCommandContext)
        source_name = parent_ctx.op.classname

        if len(astnode.targets) > 1:
            alter_ptr_ctx = context.get(pointers.PointerCommandContext)

            alter_ptr_ctx.op.add(
                sd.AlterClassProperty(
                    property='spectargets',
                    new_value=so.ClassList([
                        so.ClassRef(
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

            target = so.ClassRef(classname=target_name)

            create_virt_parent = concepts.CreateConcept(
                classname=target_name,
                metaclass=concepts.Concept
            )

            create_virt_parent.update((
                sd.AlterClassProperty(
                    property='name',
                    new_value=target_name
                ),
                sd.AlterClassProperty(
                    property='is_virtual',
                    new_value=True
                ),
                sd.AlterClassProperty(
                    property='is_derived',
                    new_value=True
                )
            ))

            alter_db_ctx = context.get(s_db.DatabaseCommandContext)

            for cc in alter_db_ctx.op(concepts.CreateConcept):
                if cc.classname == create_virt_parent.classname:
                    break
            else:
                alter_db_ctx.op.add(create_virt_parent)
        else:
            target = so.ClassRef(
                classname=sn.Name(
                    module=astnode.targets[0].module,
                    name=astnode.targets[0].name
                )
            )

        cmd.new_value = target

        return cmd


class AlterLink(LinkCommand, named.AlterNamedClass):
    astnode = [qlast.AlterLinkNode, qlast.AlterConcreteLinkNode]
    referenced_astnode = qlast.AlterConcreteLinkNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if isinstance(astnode, qlast.AlterConcreteLinkNode):
            for ap in cmd(sd.AlterClassProperty):
                if ap.property == 'search_weight':
                    ap.property = 'search'
                    if ap.new_value is not None:
                        ap.new_value = LinkSearchConfiguration(
                            weight=LinkSearchWeight(
                                ap.new_value
                            )
                        )
                    break

        return cmd

    def _get_ast_node(self, context):
        concept = context.get(LinkSourceCommandContext)

        if concept:
            return qlast.AlterConcreteLinkNode
        else:
            return qlast.AlterLinkNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        concept = context.get(LinkSourceCommandContext)

        for op in self(lproperties.LinkPropertyCommand):
            self._append_subcmd_ast(node, op, context)

        if not concept:
            for op in self(indexes.SourceIndexCommand):
                self._append_subcmd_ast(node, op, context)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'spectargets':
            if op.new_value:
                node.commands.append(qlast.AlterTargetNode(
                    targets=[
                        qlast.ClassRefNode(name=t.classname.name,
                                           module=t.classname.module)
                        for t in op.new_value
                    ]
                ))
        elif op.property == 'target':
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
        elif op.property == 'search':
            if op.new_value:
                v = qlast.ConstantNode(value=str(op.new_value.weight))
                self._set_attribute_ast(context, node, 'search_weight', v)
            else:
                self._drop_attribute_ast(context, node, 'search_weight')
        else:
            super()._apply_field_ast(context, node, op)


class DeleteLink(LinkCommand, named.DeleteNamedClass):
    astnode = [qlast.DropLinkNode, qlast.DropConcreteLinkNode]
    referenced_astnode = qlast.DropConcreteLinkNode

    def _get_ast_node(self, context):
        concept = context.get(LinkSourceCommandContext)

        if concept:
            return qlast.DropConcreteLinkNode
        else:
            return qlast.DropLinkNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        concept = context.get(LinkSourceCommandContext)

        for op in self(lproperties.LinkPropertyCommand):
            self._append_subcmd_ast(node, op, context)

        if not concept:
            for op in self(indexes.SourceIndexCommand):
                self._append_subcmd_ast(node, op, context)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)


class Link(pointers.Pointer, sources.Source):
    _type = 'link'

    spectargets = so.Field(named.NamedClassSet, named.NamedClassSet,
                           coerce=True)

    mapping = so.Field(LinkMapping, default=None,
                       compcoef=0.833, coerce=True)

    exposed_behaviour = so.Field(pointers.PointerExposedBehaviour,
                                 default=None, compcoef=0.98)

    search = so.Field(LinkSearchConfiguration, default=None, compcoef=0.909)

    delta_driver = sd.DeltaDriver(
        create=CreateLink,
        alter=AlterLink,
        rebase=RebaseLink,
        rename=RenameLink,
        delete=DeleteLink
    )

    @classmethod
    def get_pointer_class(cls):
        return lproperties.LinkProperty

    @classmethod
    def get_special_pointers(cls):
        return (sn.Name('std::linkid'),
                sn.Name('std::source'),
                sn.Name('std::target'))

    def get_exposed_behaviour(self):
        if self.exposed_behaviour is not None:
            return self.exposed_behaviour
        else:
            if self.mapping in {LinkMapping.OneToOne, LinkMapping.ManyToOne}:
                return pointers.PointerExposedBehaviour.FirstItem
            else:
                return pointers.PointerExposedBehaviour.Set

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

    def singular(self, direction=pointers.PointerDirection.Outbound):
        if direction == pointers.PointerDirection.Outbound:
            return self.mapping in \
                (LinkMapping.OneToOne, LinkMapping.ManyToOne)
        else:
            return self.mapping in \
                (LinkMapping.OneToOne, LinkMapping.OneToMany)

    def atomic(self):
        assert not self.generic(), \
            "atomicity is not determined for generic links"
        return isinstance(self.target, atoms.Atom)

    def has_user_defined_properties(self):
        return bool([p for p in self.pointers.values()
                     if not p.is_special_pointer()])

    def compare(self, other, context=None):
        if not isinstance(other, Link):
            if isinstance(other, pointers.BasePointer):
                return 0.0
            else:
                return NotImplemented

        return super().compare(other, context=context)

    @hybridmethod
    def copy(scope, obj=None):
        if isinstance(scope, Link):
            obj = scope
            cls = obj.__class__.get_canonical_class()
        else:
            cls = scope = scope.get_canonical_class()

        result = super(cls, cls).copy(obj)

        result.source = obj.source
        result.target = obj.target
        result.default = obj.default

        return result

    def finalize(self, schema, bases=None, *, dctx=None):
        super().finalize(schema, bases=bases, dctx=dctx)

        if not self.generic() and self.mapping is None:
            self.mapping = LinkMapping.ManyToOne

            if dctx is not None:
                from . import delta as sd

                dctx.current().op.add(sd.AlterClassProperty(
                    property='mapping',
                    new_value=self.mapping,
                    source='default'
                ))

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='link'),
            sn.Name(module='schema', name='__class__'),
        )

    @classmethod
    def get_default_base_name(self):
        return sn.Name('std::link')
