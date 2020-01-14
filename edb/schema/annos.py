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


from __future__ import annotations

from typing import *  # NoQA

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import delta as sd
from . import inheriting
from . import name as sn
from . import referencing
from . import objects as so
from . import utils

if TYPE_CHECKING:
    from . import schema as s_schema


class Annotation(inheriting.InheritingObject,
                 qlkind=qltypes.SchemaObjectClass.ANNOTATION):
    # Annotations cannot be renamed, so make sure the name
    # has low compcoef.
    name = so.SchemaField(
        sn.Name, inheritable=False, compcoef=0.2)

    inheritable = so.SchemaField(
        bool, default=False, compcoef=0.2)

    def get_verbosename(self,
                        schema: s_schema.Schema,
                        *,
                        with_parent: bool=False) -> str:
        vn = super().get_verbosename(schema)
        return f"abstract {vn}"


class AnnotationValue(referencing.ReferencedInheritingObject):

    subject = so.SchemaField(
        so.Object, compcoef=1.0, default=None, inheritable=False)

    annotation = so.SchemaField(
        Annotation, compcoef=0.429)

    value = so.SchemaField(
        str, compcoef=0.909)

    inheritable = so.SchemaField(
        bool, default=False, compcoef=0.2)

    def __str__(self) -> str:
        return '<{}: at 0x{:x}>'.format(self.__class__.__name__, id(self))

    __repr__ = __str__

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'annotation'

    def get_verbosename(self,
                        schema: s_schema.Schema,
                        *,
                        with_parent: bool=False) -> str:
        vn = super().get_verbosename(schema)
        if with_parent:
            subject = self.get_subject(schema)
            assert subject is not None
            pvn = subject.get_verbosename(schema, with_parent=True)
            return f'{vn} of {pvn}'
        else:
            return vn


class AnnotationSubject(so.Object):
    annotations_refs = so.RefDict(
        attr='annotations',
        ref_cls=AnnotationValue)

    annotations = so.SchemaField(
        so.ObjectIndexByShortname,
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.909,
        default=so.ObjectIndexByShortname)

    def add_annotation(self,
                       schema: s_schema.Schema,
                       annotation: AnnotationValue,
                       replace: bool = False) -> s_schema.Schema:
        schema = self.add_classref(
            schema, 'annotations', annotation, replace=replace)
        return schema

    def del_annotation(self,
                       schema: s_schema.Schema,
                       annotation_name: str) -> s_schema.Schema:
        shortname = sn.shortname_from_fullname(annotation_name)
        return self.del_classref(schema, 'annotations', shortname)

    def get_annotation(self,
                       schema: s_schema.Schema,
                       name: str) -> Optional[str]:
        attrval = self.get_annotations(schema).get(schema, name, None)
        return attrval.get_value(schema) if attrval is not None else None

    def set_annotation(self,
                       schema: s_schema.Schema,
                       attr: Annotation,
                       value: str) -> s_schema.Schema:
        attrname = attr.get_name(schema)
        existing = self.get_annotations(schema).get(schema, attrname, None)
        if existing is None:
            my_name = self.get_name(schema)
            ann = sn.get_specialized_name(attrname, my_name)
            an = sn.Name(name=ann, module=my_name.module)
            schema, av = AnnotationValue.create_in_schema(
                schema, name=an, value=value,
                subject=self, annotation=attr,
                inheritable=attr.get_inheritable(schema))
            schema = self.add_annotation(schema, av)
        else:
            schema, updated = existing.set_field_value('value', value)
            schema = self.add_annotation(schema, updated, replace=True)

        return schema


class AnnotationCommandContext(sd.ObjectCommandContext):
    pass


class AnnotationCommand(sd.ObjectCommand, schema_metaclass=Annotation,
                        context_class=AnnotationCommandContext):
    pass


class CreateAnnotation(AnnotationCommand, sd.CreateObject):
    astnode = qlast.CreateAnnotation

    @classmethod
    def _cmd_tree_from_ast(cls: sd.ObjectCommandMeta,
                           schema: s_schema.Schema,
                           astnode: qlast.CreateAnnotation,
                           context: sd.CommandContext) -> CreateAnnotation:
        cmd = super()._cmd_tree_from_ast(schema,  # type: ignore
                                         astnode,
                                         context)

        cmd.set_attribute_value('inheritable', astnode.inheritable)
        cmd.set_attribute_value('is_abstract', True)

        assert isinstance(cmd, CreateAnnotation)
        return cmd

    def _apply_field_ast(self,
                         schema: s_schema.Schema,
                         context: sd.CommandContext,
                         node: qlast.CreateAnnotation,
                         op: sd.AlterObjectProperty) -> None:
        if op.property == 'inheritable':
            node.inheritable = op.new_value
        else:
            super()._apply_field_ast(schema, context, node, op)


class AlterAnnotation(AnnotationCommand, sd.AlterObject):
    pass


class DeleteAnnotation(AnnotationCommand, sd.DeleteObject):
    astnode = qlast.DropAnnotation


class AnnotationSubjectCommandContext:
    pass


class AnnotationSubjectCommand(sd.ObjectCommand):
    pass


class AnnotationValueCommandContext(sd.ObjectCommandContext):
    pass


class AnnotationValueCommand(
        referencing.ReferencedInheritingObjectCommand,
        schema_metaclass=AnnotationValue,
        context_class=AnnotationValueCommandContext,
        referrer_context_class=AnnotationSubjectCommandContext):

    def add_annotation(self,
                       schema: s_schema.Schema,
                       annotation: AnnotationValue,
                       parent: AnnotationSubject) -> s_schema.Schema:
        return parent.add_annotation(schema, annotation, replace=True)

    def del_annotation(self,
                       schema: s_schema.Schema,
                       annotation_name: str,
                       parent: AnnotationSubject) -> s_schema.Schema:
        return parent.del_annotation(schema, annotation_name)


class CreateAnnotationValue(AnnotationValueCommand,
                            referencing.CreateReferencedInheritingObject):
    astnode = qlast.CreateAnnotationValue
    referenced_astnode = qlast.CreateAnnotationValue

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.CreateAnnotationValue,
        context: sd.CommandContext
    ) -> CreateAnnotationValue:
        from edb.edgeql import compiler as qlcompiler

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        propname = sn.shortname_from_fullname(cmd.classname)

        value = qlcompiler.evaluate_ast_to_python_val(
            astnode.value, schema=schema)

        if not isinstance(value, str):
            raise ValueError(
                f'unexpected value type in AnnotationValue: {value!r}')

        attr = schema.get(propname)

        assert isinstance(attr, Annotation)

        cmd.update((
            sd.AlterObjectProperty(
                property='annotation',
                new_value=utils.reduce_to_typeref(schema, attr)
            ),
            sd.AlterObjectProperty(
                property='value',
                new_value=value
            ),
            sd.AlterObjectProperty(
                property='inheritable',
                new_value=attr.get_inheritable(schema),
            ),
            sd.AlterObjectProperty(
                property='is_final',
                new_value=not attr.get_inheritable(schema),
            ),
        ))

        assert isinstance(cmd, CreateAnnotationValue)
        return cmd

    def _apply_field_ast(self,
                         schema: s_schema.Schema,
                         context: sd.CommandContext,
                         node: qlast.CreateAnnotationValue,
                         op: sd.AlterObjectProperty) -> None:
        if op.property == 'value':
            node.value = qlast.BaseConstant.from_python(op.new_value)
        else:
            super()._apply_field_ast(schema, context, node, op)


class AlterAnnotationValue(AnnotationValueCommand,
                           referencing.AlterReferencedInheritingObject):

    astnode = qlast.AlterAnnotationValue
    referenced_astnode = qlast.AlterAnnotationValue

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.AlterAnnotationValue,
        context: sd.CommandContext
    ) -> AlterAnnotationValue:
        from edb.edgeql import compiler as qlcompiler

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        value = qlcompiler.evaluate_ast_to_python_val(
            astnode.value, schema=schema)

        if not isinstance(value, str):
            raise ValueError(
                f'unexpected value type in AnnotationValue: {value!r}')

        cmd.update((
            sd.AlterObjectProperty(
                property='value',
                new_value=value
            ),
        ))

        assert isinstance(cmd, AlterAnnotationValue)

        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.AlterAnnotationValue,
        op: sd.AlterObjectProperty
    ) -> None:
        if op.property == 'value':
            node.value = qlast.BaseConstant.from_python(op.new_value)
        else:
            super()._apply_field_ast(schema, context, node, op)


class DeleteAnnotationValue(AnnotationValueCommand,
                            inheriting.DeleteInheritingObject):

    astnode = qlast.DropAnnotationValue
