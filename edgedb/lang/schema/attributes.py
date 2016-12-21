##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import derivable
from . import name as sn
from . import named
from . import objects as so
from . import primary
from . import referencing


class AttributeCommandContext(sd.ClassCommandContext):
    pass


class AttributeCommand(sd.ClassCommand):
    context_class = AttributeCommandContext

    @classmethod
    def _get_metaclass(cls):
        return Attribute


class CreateAttribute(AttributeCommand, named.CreateNamedClass):
    astnode = qlast.CreateAttributeNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if astnode.type.subtypes:
            coll = so.Collection.get_class(astnode.type.maintype.name)

            subtypes = []
            for st in astnode.type.subtypes:
                stref = so.ClassRef(
                    classname=sn.Name(module=st.module, name=st.name))
                subtypes.append(stref)

            typ = coll.from_subtypes(subtypes)
        else:
            mtn = sn.Name(module=astnode.type.maintype.module,
                          name=astnode.type.maintype.name)
            typ = so.ClassRef(classname=mtn)

        cmd.add(
            sd.AlterClassProperty(
                property='type',
                new_value=typ
            )
        )

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'type':
            tp = op.new_value
            if isinstance(tp, so.Collection):
                maintype = tp.schema_name
                stt = tp.get_subtypes()

                for st in stt:
                    eltype = qlast.ClassRefNode(module=st.module, name=st.name)
                tnn = qlast.TypeNameNode(
                    maintype=maintype,
                    subtypes=[eltype])
            else:
                tnn = qlast.TypeNameNode(maintype=tp)

            node.type = tnn

        else:
            super()._apply_field_ast(context, node, op)


class RenameAttribute(AttributeCommand, named.RenameNamedClass):
    pass


class AlterAttribute(AttributeCommand, named.AlterNamedClass):
    pass


class DeleteAttribute(AttributeCommand, named.DeleteNamedClass):
    astnode = qlast.DropAttributeNode


class AttributeSubjectCommandContext:
    pass


class AttributeSubjectCommand(sd.ClassCommand):
    def _create_innards(self, schema, context):
        super()._create_innards(schema, context)

        for op in self.get_subcommands(type=AttributeValueCommand):
            op.apply(schema, context=context)

    def _alter_innards(self, schema, context, scls):
        super()._alter_innards(schema, context, scls)

        for op in self.get_subcommands(type=AttributeValueCommand):
            op.apply(schema, context=context)

    def _delete_innards(self, schema, context, scls):
        super()._delete_innards(schema, context, scls)

        for op in self.get_subcommands(type=AttributeValueCommand):
            op.apply(schema, context=context)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self.get_subcommands(type=AttributeValueCommand):
            self._append_subcmd_ast(node, op, context)


class AttributeValueCommandContext(sd.ClassCommandContext):
    pass


class AttributeValueCommand(sd.ClassCommand):
    context_class = AttributeValueCommandContext

    @classmethod
    def _get_metaclass(cls):
        return AttributeValue

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        propname = super()._classname_from_ast(astnode, context, schema)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        pnn = AttributeValue.get_specialized_name(
            sn.Name(propname), subject_name
        )

        pn = sn.Name(name=pnn, module=subject_name.module)

        return pn

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        if '::' not in propname:
            return sd.AlterClassProperty._cmd_tree_from_ast(
                astnode, context)
        else:
            return super()._cmd_tree_from_ast(astnode, context, schema)

    def add_attribute(self, attribute, parent, schema):
        parent.add_attribute(attribute, replace=True)

    def delete_attribute(self, attribute_class, parent, schema):
        parent.del_attribute(attribute_class, schema)


class CreateAttributeValue(AttributeValueCommand, named.CreateNamedClass):
    astnode = qlast.CreateAttributeValueNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        if '::' not in propname:
            return sd.AlterClassProperty._cmd_tree_from_ast(
                astnode, context, schema)

        cmd = super()._cmd_tree_from_ast(astnode, context, schema)
        propname = AttributeValue.get_shortname(cmd.classname)

        val = astnode.value
        if isinstance(val, qlast.ConstantNode):
            value = val.value
        elif isinstance(val, qlast.SequenceNode):
            value = tuple(v.value for v in val.elements)
        else:
            msg = 'unexpected value type in AttributeValue: {!r}'
            raise ValueError(msg.format(val))

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        cmd.update((
            sd.AlterClassProperty(
                property='subject',
                new_value=so.ClassRef(classname=subject_name)
            ),
            sd.AlterClassProperty(
                property='attribute',
                new_value=so.ClassRef(classname=propname)
            ),
            sd.AlterClassProperty(
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
            name = AttributeValue.get_shortname(
                self.classname)
            attribute = attrsubj.scls.local_attributes.get(name)
            if attribute is None:
                attribute = super().apply(schema, context)
                self.add_attribute(attribute, attrsubj.scls, schema)
            else:
                attribute = named.AlterNamedClass.apply(
                    self, schema, context)

            return attribute


class RenameAttributeValue(AttributeValueCommand, named.RenameNamedClass):
    def apply(self, schema, context):
        result = super().apply(schema, context)

        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        norm = AttributeValue.get_shortname

        own = attrsubj.scls.local_attributes.pop(
            norm(self.classname), None)
        if own:
            attrsubj.scls.local_attributes[norm(self.new_name)] = own

        inherited = attrsubj.scls.attributes.pop(
            norm(self.classname), None)
        if inherited is not None:
            attrsubj.scls.attributes[norm(self.new_name)] = inherited

        return result


class AlterAttributeValue(AttributeValueCommand, named.AlterNamedClass):
    astnode = qlast.AlterAttributeValueNode

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        for op in self(sd.AlterClassProperty):
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


class DeleteAttributeValue(AttributeValueCommand, named.DeleteNamedClass):
    astnode = qlast.DropAttributeValueNode

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        self.delete_attribute(self.classname, attrsubj.scls, schema)

        return super().apply(schema, context)


class Attribute(primary.PrimaryClass):
    _type = 'attribute'

    type = so.Field(so.Class, compcoef=0.909)

    delta_driver = sd.DeltaDriver(
        create=CreateAttribute,
        alter=AlterAttribute,
        rename=RenameAttribute,
        delete=DeleteAttribute
    )


class AttributeValue(derivable.DerivableClass):
    _type = 'attribute-value'

    subject = so.Field(named.NamedClass, compcoef=1.0)
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


class AttributeSubject(referencing.ReferencingClass):
    attributes = referencing.RefDict(ref_cls=AttributeValue, compcoef=0.909)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._attr_name_cache = None

    def add_attribute(self, attribute, replace=False):
        self.add_classref('attributes', attribute, replace=replace)
        self._attr_name_cache = None

    def del_attribute(self, attribute_name, schema):
        self.del_classref('attributes', attribute_name, schema)

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
