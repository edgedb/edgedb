##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import astmatch as qlastmatch

from . import delta as sd
from . import derivable
from . import name as sn
from . import named
from . import objects as so
from . import primary
from . import referencing


class AttributeCommandContext(sd.PrototypeCommandContext):
    pass


class AttributeCommand:
    context_class = AttributeCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Attribute


class _EnumASTExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            binop = qlastmatch.BinOpNode(op=qlast.IN)
            binop.left = qlastmatch.PathNode(
                steps=[
                    qlastmatch.PathStepNode(expr='value')
                ]
            )
            binop.right = qlastmatch.group('enum', qlastmatch.SequenceNode())

            self.pattern = binop

        return self.pattern

    def match(self, tree):
        m = qlastmatch.match(self.get_pattern(), tree)
        if m:
            sequence = m.enum[0].node
            return [el.value for el in sequence.elements]
        else:
            return None


class CreateAttribute(named.CreateNamedPrototype, AttributeCommand):
    astnode = qlast.CreateAttributeNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        if astnode.type.subtype:
            eltype = astnode.type.subtype.maintype
        else:
            eltype = None

        typ = so.SchemaType(
            main_type=astnode.type.maintype,
            element_type=eltype
        )

        if astnode.constraint is not None:
            pattern = _EnumASTExpr()
            values = pattern.match(astnode.constraint)
            if values is None:
                raise ValueError('unexpected attribute constraint expression')

            constr = so.SchemaTypeConstraintEnum(data=values)
            typ.constraints = so.SchemaTypeConstraintSet((constr,))

        cmd.add(
            sd.AlterPrototypeProperty(
                property='type',
                new_value=typ
            )
        )

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'type':
            tp = op.new_value
            tnn = qlast.TypeNameNode(maintype=tp.main_type)
            if tp.element_type:
                tnn.subtype = qlast.TypeNameNode(maintype=tp.element_type)
            node.type = tnn

            is_enum = lambda c: c.__class__.__name__ == \
                                    'SchemaTypeConstraintEnum'

            if tp.constraints:
                expr = None

                for constraint in tp.constraints:
                    if is_enum(constraint):
                        l = qlast.PathNode(steps=[
                            qlast.PathStepNode(expr='value')
                        ])

                        r = qlast.SequenceNode(elements=[
                            qlast.ConstantNode(value=v)
                            for v in constraint.data
                        ])

                        cexpr = qlast.BinOpNode(
                                    left=l, op=qlast.IN, right=r)
                    else:
                        msg = 'unexpected schema type constraint: {!r}'
                        raise ValueError(msg.format(constraint))

                    if expr is None:
                        expr = cexpr
                    else:
                        expr = qlast.BinOpNode(
                                    left=expr, op=qlast.AND, right=cexpr)

                node.constraint = expr
        else:
            super()._apply_field_ast(context, node, op)


class RenameAttribute(named.RenameNamedPrototype, AttributeCommand):
    pass


class AlterAttribute(named.AlterNamedPrototype, AttributeCommand):
    pass


class DeleteAttribute(named.DeleteNamedPrototype, AttributeCommand):
    astnode = qlast.DropAttributeNode


class AttributeSubjectCommandContext:
    pass


class AttributeValueCommandContext(sd.PrototypeCommandContext):
    pass


class AttributeValueCommand(sd.PrototypeCommand):
    context_class = AttributeValueCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return AttributeValue

    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        propname = super()._protoname_from_ast(astnode, context)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.prototype_name

        pnn = AttributeValue.generate_specialized_name(
            subject_name, sn.Name(propname)
        )

        pn = sn.Name(name=pnn, module=subject_name.module)

        return pn

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '.' + propname

        if '.' not in propname:
            return sd.AlterPrototypeProperty._cmd_tree_from_ast(
                        astnode, context)
        else:
            return super()._cmd_tree_from_ast(astnode, context)

    def add_attribute(self, attribute, parent, schema):
        parent.add_attribute(attribute, replace=True)

    def delete_attribute(self, attribute_class, parent, schema):
        parent.del_attribute(attribute_class, schema)


class CreateAttributeValue(AttributeValueCommand, named.CreateNamedPrototype):
    astnode = qlast.CreateAttributeValueNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '.' + propname

        if '.' not in propname:
            return sd.AlterPrototypeProperty._cmd_tree_from_ast(
                        astnode, context)

        cmd = super()._cmd_tree_from_ast(astnode, context)
        propname = AttributeValue.normalize_name(cmd.prototype_name)

        val = astnode.value
        if isinstance(val, qlast.ConstantNode):
            value = val.value
        elif isinstance(val, qlast.SequenceNode):
            value = tuple(v.value for v in val.elements)
        else:
            msg = 'unexpected value type in AttributeValue: {!r}'
            raise ValueError(msg.format(val))

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.prototype_name

        cmd.update((
            sd.AlterPrototypeProperty(
                property='subject',
                new_value=so.PrototypeRef(prototype_name=subject_name)
            ),
            sd.AlterPrototypeProperty(
                property='attribute',
                new_value=so.PrototypeRef(prototype_name=propname)
            ),
            sd.AlterPrototypeProperty(
                property='value',
                new_value=value
            )
        ))

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'value':
            node.value = qlast.ConstantNode(value=op.new_value)
        elif op.property == 'is_derived':
            pass
        elif op.property == 'attribute':
            pass
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        with context(AttributeValueCommandContext(self, None)):
            name = AttributeValue.normalize_name(
                        self.prototype_name)
            attribute = attrsubj.proto.local_attributes.get(name)
            if attribute is None:
                attribute = super().apply(schema, context)
                self.add_attribute(attribute, attrsubj.proto, schema)
            else:
                attribute = named.AlterNamedPrototype.apply(
                                self, schema, context)

            return attribute


class RenameAttributeValue(AttributeValueCommand, named.RenameNamedPrototype):
    def apply(self, schema, context):
        result = super().apply(schema, context)

        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        norm = AttributeValue.normalize_name

        own = attrsubj.proto.local_attributes.pop(
                norm(self.prototype_name), None)
        if own:
            attrsubj.proto.local_attributes[norm(self.new_name)] = own

        inherited = attrsubj.proto.attributes.pop(
                        norm(self.prototype_name), None)
        if inherited is not None:
            attrsubj.proto.attributes[norm(self.new_name)] = inherited

        return result


class AlterAttributeValue(AttributeValueCommand, named.AlterNamedPrototype):
    astnode = qlast.AlterAttributeValueNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        for op in self(sd.AlterPrototypeProperty):
            if op.property == 'value':
                node.value = qlast.ConstantNode(value=op.new_value)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'is_derived':
            pass
        elif op.property == 'attribute':
            pass
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        with context(AttributeValueCommandContext(self, None)):
            return super().apply(schema, context)


class DeleteAttributeValue(AttributeValueCommand, named.DeleteNamedPrototype):
    astnode = qlast.DropAttributeValueNode

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        self.delete_attribute(self.prototype_name, attrsubj.proto, schema)

        return super().apply(schema, context)


class Attribute(primary.Prototype):
    _type = 'attribute'

    type = so.Field(so.SchemaType, compcoef=0.909)

    delta_driver = sd.DeltaDriver(
        create=CreateAttribute,
        alter=AlterAttribute,
        rename=RenameAttribute,
        delete=DeleteAttribute
    )


class AttributeValue(derivable.DerivablePrototype):
    _type = 'attribute-value'

    subject = so.Field(named.NamedPrototype, compcoef=1.0)
    attribute = so.Field(Attribute, compcoef=0.429)
    value = so.Field(object, compcoef=0.909)

    delta_driver = sd.DeltaDriver(
        create=CreateAttributeValue,
        alter=AlterAttributeValue,
        rename=RenameAttributeValue,
        delete=DeleteAttributeValue
    )

    def __str__(self):
        return '<{}: {}={!r} at 0x{:x}>'.format(
                    self.__class__.__name__,
                    self.attribute.name if self.attribute else '<nil>',
                    self.value, id(self))

    __repr__ = __str__


class AttributeSubject(referencing.ReferencingPrototype):
    attributes = referencing.RefDict(ref_cls=AttributeValue, compcoef=0.909)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._attr_name_cache = None

    def add_attribute(self, attribute, replace=False):
        self.add_protoref('attributes', attribute, replace=replace)
        self._attr_name_cache = None

    def del_attribute(self, attribute_name, proto_schema):
        self.del_protoref('attributes', attribute_name, proto_schema)

    def delta_all_attributes(self, old, new, delta, context):
        oldattributes = old.local_attributes if old else {}
        newattributes = new.local_attributes if new else {}

        self.delta_attributes(oldattributes, newattributes, delta, context)

    def get_attribute(self, name):
        value = None

        try:
            value = self.attributes[name]
        except KeyError:
            if self._attr_name_cache is None:
                self._attr_name_cache = self._build_attr_name_cache()

            try:
                value = self._attr_name_cache[name]
            except KeyError:
                pass

        return value

    def _build_attr_name_cache(self):
        _attr_name_cache = {}
        ambiguous = set()

        for an, attr in self.attributes.items():
            if an.name in _attr_name_cache:
                ambiguous.add(an.name)
            _attr_name_cache[an.name] = attr

        for amb in ambiguous:
            del _attr_name_cache[amb]

        return _attr_name_cache

    @classmethod
    def delta_attributes(cls, set1, set2, delta, context=None):
        oldattributes = set(set1)
        newattributes = set(set2)

        for attribute in oldattributes - newattributes:
            d = set1[attribute].delta(None, reverse=True, context=context)
            delta.add(d)

        for attribute in newattributes - oldattributes:
            d = set2[attribute].delta(None, context=context)
            delta.add(d)

        for attribute in newattributes & oldattributes:
            oldattr = set1[attribute]
            newattr = set2[attribute]

            if newattr.compare(oldattr, context=context) != 1.0:
                d = newattr.delta(oldattr, context=context)
                delta.add(d)
