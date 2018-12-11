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

from . import abc as s_abc
from . import delta as sd
from . import inheriting
from . import name as sn
from . import objects as so
from . import referencing
from . import types as s_types
from . import utils


class Attribute(inheriting.InheritingObject):

    type = so.SchemaField(
        s_types.Type,
        compcoef=0.909)


class AttributeValue(inheriting.InheritingObject):

    subject = so.SchemaField(
        so.Object, compcoef=1.0, default=None, inheritable=False)

    attribute = so.SchemaField(
        Attribute, compcoef=0.429)

    value = so.SchemaField(
        str, compcoef=0.909)

    def __str__(self):
        return '<{}: at 0x{:x}>'.format(self.__class__.__name__, id(self))

    __repr__ = __str__


class AttributeSubject(referencing.ReferencingObject):
    attributes_refs = referencing.RefDict(
        attr='attributes',
        local_attr='own_attributes',
        ref_cls=AttributeValue)

    attributes = so.SchemaField(
        so.ObjectIndexByShortname,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByShortname, hashable=False)

    own_attributes = so.SchemaField(
        so.ObjectIndexByShortname, compcoef=0.909,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByShortname)

    def add_attribute(self, schema, attribute, replace=False):
        schema = self.add_classref(
            schema, 'attributes', attribute, replace=replace)
        return schema

    def del_attribute(self, schema, attribute_name):
        shortname = sn.shortname_from_fullname(attribute_name)
        return self.del_classref(schema, 'attributes', shortname)


class AttributeCommandContext(sd.ObjectCommandContext):
    pass


class AttributeCommand(sd.ObjectCommand, schema_metaclass=Attribute,
                       context_class=AttributeCommandContext):
    pass


class CreateAttribute(AttributeCommand, sd.CreateObject):
    astnode = qlast.CreateAttribute

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        cmd.add(
            sd.AlterObjectProperty(
                property='type',
                new_value=utils.ast_to_typeref(
                    astnode.type, modaliases=context.modaliases,
                    schema=schema)
            )
        )

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'type':
            tp = op.new_value
            if isinstance(tp, s_abc.Collection):
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
            super()._apply_field_ast(schema, context, node, op)


class AlterAttribute(AttributeCommand, sd.AlterObject):
    pass


class DeleteAttribute(AttributeCommand, sd.DeleteObject):
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
    def _classname_from_ast(cls, schema, astnode, context):
        propname = super()._classname_from_ast(schema, astnode, context)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        pnn = sn.get_specialized_name(
            sn.Name(propname), subject_name
        )

        pn = sn.Name(name=pnn, module=subject_name.module)

        return pn

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        if '::' not in propname:
            return sd.AlterObjectProperty._cmd_tree_from_ast(
                schema, astnode, context)
        else:
            return super()._cmd_tree_from_ast(schema, astnode, context)

    def add_attribute(self, schema, attribute, parent):
        return parent.add_attribute(schema, attribute, replace=True)

    def del_attribute(self, schema, attribute_class, parent):
        return parent.del_attribute(schema, attribute_class)


class CreateAttributeValue(AttributeValueCommand, sd.CreateObject):
    astnode = qlast.CreateAttributeValue

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        from edb.lang.edgeql import compiler as qlcompiler

        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        if '::' not in propname:
            return sd.AlterObjectProperty._cmd_tree_from_ast(
                schema, astnode, context)

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        propname = sn.shortname_from_fullname(cmd.classname)

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
        subject = parent_ctx.scls
        attr = schema.get(propname)

        cmd.update((
            sd.AlterObjectProperty(
                property='subject',
                new_value=utils.reduce_to_typeref(schema, subject)
            ),
            sd.AlterObjectProperty(
                property='attribute',
                new_value=utils.reduce_to_typeref(schema, attr)
            ),
            sd.AlterObjectProperty(
                property='value',
                new_value=value
            )
        ))

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'value':
            node.value = qlast.BaseConstant.from_python(op.new_value)
        elif op.property == 'is_derived':
            pass
        elif op.property == 'attribute':
            pass
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        with context(AttributeValueCommandContext(self, None)):
            name = sn.shortname_from_fullname(self.classname)
            attrs = attrsubj.scls.get_own_attributes(schema)
            attribute = attrs.get(schema, name, None)
            if attribute is None:
                schema, attribute = super().apply(schema, context)
                schema = self.add_attribute(schema, attribute, attrsubj.scls)
            else:
                schema, attribute = sd.AlterObject.apply(
                    self, schema, context)

            return schema, attribute


class AlterAttributeValue(AttributeValueCommand, sd.AlterObject):
    astnode = qlast.AlterAttributeValue

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)
        for op in self(sd.AlterObjectProperty):
            if op.property == 'value':
                node.value = qlast.BaseConstant.from_python(op.new_value)

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'is_derived':
            pass
        elif op.property == 'attribute':
            pass
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        with context(AttributeValueCommandContext(self, None)):
            return super().apply(schema, context)


class DeleteAttributeValue(AttributeValueCommand, sd.DeleteObject):
    astnode = qlast.DropAttributeValue

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        schema = self.del_attribute(schema, self.classname, attrsubj.scls)

        return super().apply(schema, context)
