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


import typing

from edb import errors

from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import inheriting
from . import name as sn
from . import objects as so
from . import utils


class Attribute(inheriting.InheritingObject):
    # Attributes cannot be renamed, so make sure the name
    # has low compcoef.
    name = so.SchemaField(
        sn.Name, inheritable=False, compcoef=0.2)

    inheritable = so.SchemaField(
        bool, default=False, compcoef=0.2)


class AttributeValue(inheriting.InheritingObject):

    subject = so.SchemaField(
        so.Object, compcoef=1.0, default=None, inheritable=False)

    attribute = so.SchemaField(
        Attribute, compcoef=0.429)

    value = so.SchemaField(
        str, compcoef=0.909)

    inheritable = so.SchemaField(
        bool, default=False, compcoef=0.2)

    def __str__(self):
        return '<{}: at 0x{:x}>'.format(self.__class__.__name__, id(self))

    __repr__ = __str__


class AttributeSubject(so.Object):
    attributes_refs = so.RefDict(
        attr='attributes',
        local_attr='own_attributes',
        non_inheritable_attr='non_inheritable_attributes',
        ref_cls=AttributeValue)

    attributes = so.SchemaField(
        so.ObjectIndexByShortname,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByShortname, hashable=False)

    own_attributes = so.SchemaField(
        so.ObjectIndexByShortname, compcoef=0.909,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByShortname)

    non_inheritable_attributes = so.SchemaField(
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

    def get_attribute(self, schema, name: str) -> typing.Optional[str]:
        attrval = self.get_attributes(schema).get(schema, name, None)
        return attrval.get_value(schema) if attrval is not None else None

    def set_attribute(self, schema, attr: Attribute, value: str):
        attrname = attr.get_name(schema)
        existing = self.get_own_attributes(schema).get(schema, attrname, None)
        if existing is None:
            existing = self.get_non_inheritable_attributes(schema).get(
                schema, attrname, None)
        if existing is None:
            my_name = self.get_name(schema)
            ann = sn.get_specialized_name(attrname, my_name)
            an = sn.Name(name=ann, module=my_name.module)
            schema, av = AttributeValue.create_in_schema(
                schema, name=an, value=value,
                subject=self, attribute=attr,
                inheritable=attr.get_inheritable(schema))
            schema = self.add_attribute(schema, av)
        else:
            schema, updated = existing.set_field_value('value', value)
            schema = self.add_attribute(schema, updated, replace=True)

        return schema


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
        cmd.update((
            sd.AlterObjectProperty(
                property='inheritable',
                new_value=astnode.inheritable,
            ),
        ))

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'inheritable':
            node.inheritable = op.new_value
        else:
            super()._apply_field_ast(schema, context, node, op)


class AlterAttribute(AttributeCommand, sd.AlterObject):
    pass


class DeleteAttribute(AttributeCommand, sd.DeleteObject):
    astnode = qlast.DropAttribute


class AttributeSubjectCommandContext:
    pass


class AttributeSubjectCommand(inheriting.InheritingObjectCommand):
    pass


class AttributeValueCommandContext(sd.ObjectCommandContext):
    pass


class AttributeValueCommand(sd.ObjectCommand, schema_metaclass=AttributeValue,
                            context_class=AttributeValueCommandContext):
    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        nqname = cls._get_ast_name(schema, astnode, context)
        if astnode.name.module:
            propname = sn.Name(module=astnode.name.module, name=nqname)
        else:
            propname = nqname

        try:
            attr = schema.get(propname, module_aliases=context.modaliases)
        except errors.InvalidReferenceError as e:
            raise errors.InvalidReferenceError(
                str(e), context=astnode.context) from None

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        pnn = sn.get_specialized_name(attr.get_name(schema), subject_name)
        pn = sn.Name(name=pnn, module=subject_name.module)

        return pn

    def add_attribute(self, schema, attribute, parent):
        return parent.add_attribute(schema, attribute, replace=True)

    def del_attribute(self, schema, attribute_class, parent):
        return parent.del_attribute(schema, attribute_class)


class CreateAttributeValue(AttributeValueCommand, sd.CreateObject):
    astnode = qlast.CreateAttributeValue

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        from edb.lang.edgeql import compiler as qlcompiler

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        propname = sn.shortname_from_fullname(cmd.classname)

        value = qlcompiler.evaluate_ast_to_python_val(
            astnode.value, schema=schema)

        if not isinstance(value, str):
            raise ValueError(
                f'unexpected value type in AttributeValue: {value!r}')

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname
        attr = schema.get(propname)

        cmd.update((
            sd.AlterObjectProperty(
                property='subject',
                new_value=so.ObjectRef(name=subject_name),
            ),
            sd.AlterObjectProperty(
                property='attribute',
                new_value=utils.reduce_to_typeref(schema, attr)
            ),
            sd.AlterObjectProperty(
                property='value',
                new_value=value
            ),
            sd.AlterObjectProperty(
                property='inheritable',
                new_value=attr.get_inheritable(schema),
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
        elif op.property == 'inheritable':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        with context(AttributeValueCommandContext(schema, self, None)):
            name = sn.shortname_from_fullname(self.classname)
            attrs = attrsubj.scls.get_own_attributes(schema)
            attribute = attrs.get(schema, name, None)
            if attribute is None:
                attrs = attrsubj.scls.get_non_inheritable_attributes(schema)
                attribute = attrs.get(schema, name, None)

            if attribute is None:
                schema, attribute = super().apply(schema, context)
                schema = self.add_attribute(schema, attribute, attrsubj.scls)
            else:
                schema, attribute = sd.AlterObject.apply(self, schema, context)

            return schema, attribute


class DeleteAttributeValue(AttributeValueCommand, sd.DeleteObject):
    astnode = qlast.DropAttributeValue

    def apply(self, schema, context):
        attrsubj = context.get(AttributeSubjectCommandContext)
        assert attrsubj, "Attribute commands must be run in " + \
                         "AttributeSubject context"

        schema = self.del_attribute(schema, self.classname, attrsubj.scls)

        return super().apply(schema, context)
