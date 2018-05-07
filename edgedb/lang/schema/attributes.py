##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import referencing
from . import types as s_types
from . import utils


class Attribute(inheriting.InheritingObject):
    _type = 'attribute'

    type = so.Field(so.Object, compcoef=0.909)


class AttributeValue(inheriting.InheritingObject):
    _type = 'attribute-value'

    subject = so.Field(named.NamedObject, compcoef=1.0, default=None,
                       inheritable=False)
    attribute = so.Field(Attribute, compcoef=0.429)
    value = so.Field(object, compcoef=0.909)

    def __str__(self):
        return '<{}: {}={!r} at 0x{:x}>'.format(
            self.__class__.__name__,
            self.attribute.name if self.attribute else '<nil>',
            self.value, id(self))

    __repr__ = __str__


class AttributeSubject(referencing.ReferencingObject):
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


class AttributeCommandContext(sd.ObjectCommandContext):
    pass


class AttributeCommand(sd.ObjectCommand, schema_metaclass=Attribute,
                       context_class=AttributeCommandContext):
    pass


class CreateAttribute(AttributeCommand, named.CreateNamedObject):
    astnode = qlast.CreateAttribute

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        cmd.add(
            sd.AlterObjectProperty(
                property='type',
                new_value=utils.ast_to_typeref(
                    astnode.type, modaliases=context.modaliases,
                    schema=schema)
            )
        )

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'type':
            tp = op.new_value
            if isinstance(tp, s_types.Collection):
                maintype = tp.schema_name
                stt = tp.get_subtypes()

                for st in stt:
                    eltype = qlast.ObjectRef(module=st.module, name=st.name)
                tnn = qlast.TypeName(
                    maintype=maintype,
                    subtypes=[eltype])
            else:
                tnn = qlast.TypeName(maintype=tp)

            node.type = tnn

        else:
            super()._apply_field_ast(context, node, op)


class RenameAttribute(AttributeCommand, named.RenameNamedObject):
    pass


class AlterAttribute(AttributeCommand, named.AlterNamedObject):
    pass


class DeleteAttribute(AttributeCommand, named.DeleteNamedObject):
    astnode = qlast.DropAttribute


class AttributeSubjectCommandContext:
    pass


class AttributeSubjectCommand(referencing.ReferencingObjectCommand):
    pass


class AttributeValueCommandContext(sd.ObjectCommandContext):
    pass


class AttributeValueCommand(sd.ObjectCommand, schema_metaclass=AttributeValue,
                            context_class=AttributeValueCommandContext):
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
            return sd.AlterObjectProperty._cmd_tree_from_ast(
                astnode, context, schema)
        else:
            return super()._cmd_tree_from_ast(astnode, context, schema)

    def add_attribute(self, attribute, parent, schema):
        parent.add_attribute(attribute, replace=True)

    def delete_attribute(self, attribute_class, parent, schema):
        parent.del_attribute(attribute_class, schema)


class CreateAttributeValue(AttributeValueCommand, named.CreateNamedObject):
    astnode = qlast.CreateAttributeValue

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        if '::' not in propname:
            return sd.AlterObjectProperty._cmd_tree_from_ast(
                astnode, context, schema)

        cmd = super()._cmd_tree_from_ast(astnode, context, schema)
        propname = AttributeValue.get_shortname(cmd.classname)

        val = astnode.value
        if isinstance(val, qlast.Constant):
            value = val.value
        elif isinstance(val, qlast.Tuple):
            value = tuple(v.value for v in val.elements)
        else:
            msg = 'unexpected value type in AttributeValue: {!r}'
            raise ValueError(msg.format(val))

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        cmd.update((
            sd.AlterObjectProperty(
                property='subject',
                new_value=so.ObjectRef(classname=subject_name)
            ),
            sd.AlterObjectProperty(
                property='attribute',
                new_value=so.ObjectRef(classname=propname)
            ),
            sd.AlterObjectProperty(
                property='value',
                new_value=value
            )
        ))

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'value':
            node.value = qlast.Constant(value=op.new_value)
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
                attribute = named.AlterNamedObject.apply(
                    self, schema, context)

            return attribute


class RenameAttributeValue(AttributeValueCommand, named.RenameNamedObject):
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


class AlterAttributeValue(AttributeValueCommand, named.AlterNamedObject):
    astnode = qlast.AlterAttributeValue

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        for op in self(sd.AlterObjectProperty):
            if op.property == 'value':
                node.value = qlast.Constant(value=op.new_value)

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


class DeleteAttributeValue(AttributeValueCommand, named.DeleteNamedObject):
    astnode = qlast.DropAttributeValue

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        self.delete_attribute(self.classname, attrsubj.scls, schema)

        return super().apply(schema, context)
