##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.functional import hybridmethod

from metamagic.caos.caosql import ast as qlast

from . import atoms
from . import constraints
from . import delta as sd
from . import enum
from . import indexes
from . import inheriting
from . import lproperties
from . import name as sn
from . import named
from . import objects as so
from . import pointers
from . import policy
from . import realm
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


class LinkSearchConfiguration(so.BasePrototype):
    weight = so.Field(LinkSearchWeight, default=None, compcoef=0.9)


class LinkSourceCommandContext(sources.SourceCommandContext):
    pass


class LinkCommandContext(pointers.PointerCommandContext,
                         constraints.ConsistencySubjectCommandContext,
                         policy.InternalPolicySubjectCommandContext,
                         lproperties.LinkPropertySourceContext):
    pass


class LinkCommand(pointers.PointerCommand):
    context_class = LinkCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Link


class CreateLink(LinkCommand, named.CreateNamedPrototype):
    astnode = [qlast.CreateConcreteLinkNode, qlast.CreateLinkNode]

    @classmethod
    def _protobases_from_ast(cls, astnode, context):
        proto_name = '{}.{}'.format(astnode.name.module, astnode.name.name)

        if isinstance(astnode, qlast.CreateConcreteLinkNode):
            nname = Link.normalize_name(proto_name)

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
                if proto_name != 'metamagic.caos.builtins.link':
                    bases = so.PrototypeList([
                        so.PrototypeRef(
                            prototype_name=sn.Name(
                                module='metamagic.caos.builtins',
                                name='link'
                            )
                        )
                    ])

        return bases

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        from . import concepts

        cmd = super()._cmd_tree_from_ast(astnode, context)

        if isinstance(astnode, qlast.CreateConcreteLinkNode):
            cmd.add(
                sd.AlterPrototypeProperty(
                    property='required',
                    new_value=astnode.is_required
                )
            )

            parent_ctx = context.get(LinkSourceCommandContext)
            source_name = parent_ctx.op.prototype_name

            cmd.add(
                sd.AlterPrototypeProperty(
                    property='source',
                    new_value=so.PrototypeRef(
                        prototype_name=source_name
                    )
                )
            )

            for ap in cmd(sd.AlterPrototypeProperty):
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
                    sd.AlterPrototypeProperty(
                        property='spectargets',
                        new_value=so.PrototypeList([
                            so.PrototypeRef(
                                prototype_name=sn.Name(
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

                target = so.PrototypeRef(prototype_name=target_name)

                create_virt_parent = concepts.CreateConcept(
                    prototype_name=target_name,
                    prototype_class=concepts.Concept
                )

                create_virt_parent.update((
                    sd.AlterPrototypeProperty(
                        property='name',
                        new_value=target_name
                    ),
                    sd.AlterPrototypeProperty(
                        property='is_virtual',
                        new_value=True
                    ),
                    sd.AlterPrototypeProperty(
                        property='is_derived',
                        new_value=True
                    )
                ))

                alter_realm_ctx = context.get(realm.RealmCommandContext)

                for cc in alter_realm_ctx.op(concepts.CreateConcept):
                    if cc.prototype_name == create_virt_parent.prototype_name:
                        break
                else:
                    alter_realm_ctx.op.add(create_virt_parent)
            else:
                target = so.PrototypeRef(
                    prototype_name=sn.Name(
                        module=astnode.targets[0].module,
                        name=astnode.targets[0].name
                    )
                )

            cmd.add(
                sd.AlterPrototypeProperty(
                    property='target',
                    new_value=target
                )
            )

            base_prop_name = sn.Name('metamagic.caos.builtins.source')
            s_name = lproperties.LinkProperty.generate_specialized_name(
                        cmd.prototype_name, base_prop_name)
            src_prop_name = sn.Name(name=s_name,
                                    module=cmd.prototype_name.module)

            src_prop = lproperties.CreateLinkProperty(
                prototype_name=src_prop_name,
                prototype_class=lproperties.LinkProperty
            )
            src_prop.update((
                sd.AlterPrototypeProperty(
                    property='name',
                    new_value=src_prop_name
                ),
                sd.AlterPrototypeProperty(
                    property='bases',
                    new_value=[
                        so.PrototypeRef(
                            prototype_name=base_prop_name
                        )
                    ]
                ),
                sd.AlterPrototypeProperty(
                    property='source',
                    new_value=so.PrototypeRef(
                        prototype_name=cmd.prototype_name
                    )
                ),
                sd.AlterPrototypeProperty(
                    property='target',
                    new_value=so.PrototypeRef(
                        prototype_name=source_name
                    )
                ),
                sd.AlterPrototypeProperty(
                    property='required',
                    new_value=True
                ),
                sd.AlterPrototypeProperty(
                    property='readonly',
                    new_value=True
                ),
                sd.AlterPrototypeProperty(
                    property='loading',
                    new_value='eager'
                )
            ))

            cmd.add(src_prop)

            base_prop_name = sn.Name('metamagic.caos.builtins.target')
            s_name = lproperties.LinkProperty.generate_specialized_name(
                        cmd.prototype_name, base_prop_name)
            tgt_prop_name = sn.Name(name=s_name,
                                    module=cmd.prototype_name.module)

            tgt_prop = lproperties.CreateLinkProperty(
                prototype_name=tgt_prop_name,
                prototype_class=lproperties.LinkProperty
            )
            tgt_prop.update((
                sd.AlterPrototypeProperty(
                    property='name',
                    new_value=tgt_prop_name
                ),
                sd.AlterPrototypeProperty(
                    property='bases',
                    new_value=[
                        so.PrototypeRef(
                            prototype_name=base_prop_name
                        )
                    ]
                ),
                sd.AlterPrototypeProperty(
                    property='source',
                    new_value=so.PrototypeRef(
                        prototype_name=cmd.prototype_name
                    )
                ),
                sd.AlterPrototypeProperty(
                    property='target',
                    new_value=target
                ),
                sd.AlterPrototypeProperty(
                    property='required',
                    new_value=False
                ),
                sd.AlterPrototypeProperty(
                    property='readonly',
                    new_value=True
                ),
                sd.AlterPrototypeProperty(
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
                    qlast.PrototypeRefNode(name=t.prototype_name.name,
                                           module=t.prototype_name.module)
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
                t = op.new_value.prototype_name
                node.targets = [
                    qlast.PrototypeRefNode(name=t.name, module=t.module)
                ]
        else:
            super()._apply_field_ast(context, node, op)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        concept = context.get(LinkSourceCommandContext)

        for op in self(lproperties.LinkPropertyCommand):
            name = op.prototype_class.normalize_name(op.prototype_name)
            if name not in {'metamagic.caos.builtins.source',
                            'metamagic.caos.builtins.target'}:
                self._append_subcmd_ast(node, op, context)

        if not concept:
            for op in self(indexes.SourceIndexCommand):
                self._append_subcmd_ast(node, op, context)

        for op in self(constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self(policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        result = named.CreateNamedPrototype.apply(self, schema, context)

        concept = context.get(LinkSourceCommandContext)
        if concept:
            result.source = concept.proto
            concept.proto.add_pointer(result)
            pointer_name = result.normal_name()
            for child in concept.proto.descendants(schema):
                if pointer_name not in child.own_pointers:
                    child.pointers[pointer_name] = result

        with context(LinkCommandContext(self, result)):
            result.acquire_ancestor_inheritance(schema)

            for op in self(atoms.AtomCommand):
                op.apply(schema, context=context)

            for op in self(lproperties.LinkPropertyCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

            for op in self(indexes.SourceIndexCommand):
                op.apply(schema, context=context)

            for op in self(policy.PolicyCommand):
                op.apply(schema, context)

        result.acquire_ancestor_inheritance(schema)

        return result


class RenameLink(LinkCommand, named.RenameNamedPrototype):
    def apply(self, schema, context):
        result = super().apply(schema, context)

        if not result.generic():
            concept = context.get(LinkSourceCommandContext)
            assert concept, "Link commands must be run in concept context"

            norm = Link.normalize_name

            own = concept.proto.own_pointers.pop(
                    norm(self.prototype_name), None)
            if own:
                concept.proto.own_pointers[norm(self.new_name)] = own

            inherited = concept.proto.pointers.pop(
                            norm(self.prototype_name), None)
            if inherited is not None:
                concept.proto.pointers[norm(self.new_name)] = inherited

        return result


class RebaseLink(LinkCommand, inheriting.RebaseNamedPrototype):
    pass


class AlterTarget(sd.Command):
    astnode = qlast.AlterTargetNode

    @classmethod
    def _cmd_from_ast(cls, astnode, context):
        return sd.AlterPrototypeProperty(property='target')

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        from . import concepts

        cmd = super()._cmd_tree_from_ast(astnode, context)

        parent_ctx = context.get(LinkSourceCommandContext)
        source_name = parent_ctx.op.prototype_name

        if len(astnode.targets) > 1:
            alter_ptr_ctx = context.get(pointers.PointerCommandContext)

            alter_ptr_ctx.op.add(
                sd.AlterPrototypeProperty(
                    property='spectargets',
                    new_value=so.PrototypeList([
                        so.PrototypeRef(
                            prototype_name=sn.Name(
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

            target = so.PrototypeRef(prototype_name=target_name)

            create_virt_parent = concepts.CreateConcept(
                prototype_name=target_name,
                prototype_class=concepts.Concept
            )

            create_virt_parent.update((
                sd.AlterPrototypeProperty(
                    property='name',
                    new_value=target_name
                ),
                sd.AlterPrototypeProperty(
                    property='is_virtual',
                    new_value=True
                ),
                sd.AlterPrototypeProperty(
                    property='is_derived',
                    new_value=True
                )
            ))

            alter_realm_ctx = context.get(realm.RealmCommandContext)

            for cc in alter_realm_ctx.op(concepts.CreateConcept):
                if cc.prototype_name == create_virt_parent.prototype_name:
                    break
            else:
                alter_realm_ctx.op.add(create_virt_parent)
        else:
            target = so.PrototypeRef(
                prototype_name=sn.Name(
                    module=astnode.targets[0].module,
                    name=astnode.targets[0].name
                )
            )

        cmd.new_value = target

        return cmd


class AlterLink(LinkCommand, named.AlterNamedPrototype):
    astnode = [qlast.AlterLinkNode, qlast.AlterConcreteLinkNode]

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        if isinstance(astnode, qlast.AlterConcreteLinkNode):
            for ap in cmd(sd.AlterPrototypeProperty):
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
                        qlast.PrototypeRefNode(name=t.prototype_name.name,
                                               module=t.prototype_name.module)
                        for t in op.new_value
                    ]
                ))
        elif op.property == 'target':
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
        elif op.property == 'search':
            if op.new_value:
                v = qlast.ConstantNode(value=str(op.new_value.weight))
                self._set_attribute_ast(context, node, 'search_weight', v)
            else:
                self._drop_attribute_ast(context, node, 'search_weight')
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()
        with context(LinkCommandContext(self, None)):
            link = super().apply(schema, context)

            for op in self(inheriting.RebaseNamedPrototype):
                op.apply(schema, context)

            link.acquire_ancestor_inheritance(schema)

            for op in self(atoms.AtomCommand):
                op.apply(schema, context)

            for op in self(lproperties.LinkPropertyCommand):
                op.apply(schema, context=context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

            for op in self(indexes.SourceIndexCommand):
                op.apply(schema, context=context)

            for op in self(policy.PolicyCommand):
                op.apply(schema, context)

        return link


class DeleteLink(LinkCommand, named.DeleteNamedPrototype):
    astnode = [qlast.DropLinkNode, qlast.DropConcreteLinkNode]

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

    def apply(self, schema, context):
        link = super().apply(schema, context)

        with context(LinkCommandContext(self, link)):
            for op in self(atoms.AtomCommand):
                op.apply(schema, context)

            for op in self(constraints.ConstraintCommand):
                op.apply(schema, context=context)

            for op in self(indexes.SourceIndexCommand):
                op.apply(schema, context=context)

            for op in self(lproperties.LinkPropertyCommand):
                op.apply(schema, context=context)

            for op in self(policy.PolicyCommand):
                op.apply(schema, context)

        concept = context.get(LinkSourceCommandContext)
        if concept:
            concept.proto.del_pointer(link, schema)

        return link


class Link(pointers.Pointer, sources.Source):
    _type = 'link'

    spectargets = so.Field(named.NamedPrototypeSet, named.NamedPrototypeSet,
                           coerce=True)

    mapping = so.Field(LinkMapping, default=LinkMapping.OneToOne,
                       compcoef=0.833)
    exposed_behaviour = so.Field(pointers.PointerExposedBehaviour, default=None,
                                 compcoef=0.98)

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
        return (sn.Name('metamagic.caos.builtins.linkid'),
                sn.Name('metamagic.caos.builtins.source'),
                sn.Name('metamagic.caos.builtins.target'))

    def get_exposed_behaviour(self):
        if self.exposed_behaviour is not None:
            return self.exposed_behaviour
        else:
            if self.mapping in {LinkMapping.OneToOne, LinkMapping.ManyToOne}:
                return pointers.PointerExposedBehaviour.FirstItem
            else:
                return pointers.PointerExposedBehaviour.Set

    def derive(self, schema, source, target=None, *,
                     mark_derived=False, add_to_schema=False, **kwargs):
        if target is None:
            target = self.target

        ptr = super().derive(schema, source, target,
                             mark_derived=mark_derived,
                             add_to_schema=add_to_schema, **kwargs)

        src_n = sn.Name('metamagic.caos.builtins.source')
        if src_n not in ptr.pointers:
            source_pbase = schema.get(src_n)
            source_p = source_pbase.get_derived(
                            schema, ptr, ptr.source,
                            mark_derived=mark_derived,
                            add_to_schema=add_to_schema)

            ptr.add_pointer(source_p)

        tgt_n = sn.Name('metamagic.caos.builtins.target')
        if tgt_n not in ptr.pointers:
            target_pbase = schema.get(tgt_n)
            target_p = target_pbase.get_derived(
                            schema, ptr, ptr.target,
                            mark_derived=mark_derived,
                            add_to_schema=add_to_schema)

            ptr.add_pointer(target_p)

        return ptr

    def finalize(self, schema, bases=None):
        super().finalize(schema, bases=bases)
        self._clear_caches()

    def merge_specialized(self, schema, other, relaxed=False):
        if isinstance(other, Link):
            self.readonly = max(self.readonly, other.readonly)
            self.required = max(self.required, other.required)

            if relaxed:
                self.mapping = self.mapping | other.mapping
            else:
                self.mapping = self.mapping & other.mapping

            if self.exposed_behaviour is None:
                self.exposed_behaviour = other.exposed_behaviour

            if self.search is None:
                self.search = other.search

            self.merge_defaults(other)

        return self

    def get_metaclass(self, proto_schema):
        from metamagic.caos.link import LinkMeta
        return LinkMeta

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
        result.default = obj.default[:]

        return result
