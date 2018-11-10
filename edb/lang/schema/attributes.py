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


from edb.lang.edgeql import ast as qlast

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
    attributes_refs = referencing.RefDict(
        attr='attributes',
        local_attr='own_attributes',
        ref_cls=AttributeValue)

    attributes = so.Field(so.ObjectDict,
                          inheritable=False, ephemeral=True, coerce=True,
                          default=so.ObjectDict, hashable=False)
    own_attributes = so.Field(so.ObjectDict, compcoef=0.909,
                              inheritable=False, ephemeral=True, coerce=True,
                              default=so.ObjectDict)

    def get_attributes(self, schema):
        return so.ObjectDictView(schema, self.attributes)

    def get_own_attributes(self, schema):
        return so.ObjectDictView(schema, self.own_attributes)

    def add_attribute(self, schema, attribute, replace=False):
        schema = self.add_classref(
            schema, 'attributes', attribute, replace=replace)
        return schema

    def del_attribute(self, schema, attribute_name):
        return self.del_classref(schema, 'attributes_refs', attribute_name)

    def rename_attribute(self, schema, attribute_name, new_attribute_name):
        norm = AttributeValue.get_shortname

        if self.own_attributes:
            own = self.own_attributes.pop(norm(attribute_name), None)
            if own:
                self.own_attributes[norm(new_attribute_name)] = own

        inherited = self.attributes.pop(norm(attribute_name), None)
        if inherited is not None:
            self.attributes[norm(new_attribute_name)] = inherited

        return schema, self


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
        return parent.add_attribute(schema, attribute, replace=True)

    def delete_attribute(self, attribute_class, parent, schema):
        return parent.del_attribute(schema, attribute_class)


class CreateAttributeValue(AttributeValueCommand, named.CreateNamedObject):
    astnode = qlast.CreateAttributeValue

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        from edb.lang.edgeql import compiler as qlcompiler

        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        if '::' not in propname:
            return sd.AlterObjectProperty._cmd_tree_from_ast(
                astnode, context, schema)

        cmd = super()._cmd_tree_from_ast(astnode, context, schema)
        propname = AttributeValue.get_shortname(cmd.classname)

        val = astnode.value
        if isinstance(val, qlast.BaseConstant):
            value = qlcompiler.evaluate_ast_to_python_val(
                val, schema=schema)

        elif isinstance(astnode.value, qlast.Tuple):
            value = tuple(
                qlcompiler.evaluate_ast_to_python_val(
                    el.value, schema=schema)
                for el in astnode.value.elements
            )

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
            node.value = qlast.BaseConstant.from_python(op.new_value)
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
                schema, attribute = super().apply(schema, context)
                schema = self.add_attribute(attribute, attrsubj.scls, schema)
            else:
                schema, attribute = named.AlterNamedObject.apply(
                    self, schema, context)

            return schema, attribute


class RenameAttributeValue(AttributeValueCommand, named.RenameNamedObject):
    def apply(self, schema, context):
        schema, result = super().apply(schema, context)

        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        schema, _ = attrsubj.scls.rename_attribute(
            schema, self.classname, self.new_name)
        return schema, result


class AlterAttributeValue(AttributeValueCommand, named.AlterNamedObject):
    astnode = qlast.AlterAttributeValue

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        for op in self(sd.AlterObjectProperty):
            if op.property == 'value':
                node.value = qlast.BaseConstant.from_python(op.new_value)

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

        schema = self.delete_attribute(self.classname, attrsubj.scls, schema)

        return super().apply(schema, context)
