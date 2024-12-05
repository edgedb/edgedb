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

from typing import (
    AbstractSet,
    Any,
    Callable,
    Optional,
    Type,
    TypeVar,
    cast,
    TYPE_CHECKING,
)

import json

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import delta as sd
from . import name as sn
from . import referencing
from . import objects as so
from . import utils

if TYPE_CHECKING:
    from . import schema as s_schema


class AnnotationValue(
    referencing.ReferencedInheritingObject,
    qlkind=qltypes.SchemaObjectClass.ANNOTATION,
    reflection=so.ReflectionMethod.AS_LINK,
    reflection_link='annotation',
    data_safe=True,
):

    subject = so.SchemaField(
        so.Object, compcoef=1.0, default=None, inheritable=False)

    # N.B: This is really an Annotation, and we even patch it up below
    # to be one, but we can't reference it here because it hasn't been
    # declared. (And the tricks used for this sort of thing elsewhere
    # don't work for reflection AS_LINK)
    annotation = so.SchemaField(
        so.Object, compcoef=0.429, ddl_identity=True)

    value = so.SchemaField(
        str, compcoef=0.909)

    def get_annotation(self, schema: s_schema.Schema) -> Annotation:
        return self.get_field_value(  # type: ignore[no-any-return]
            schema, 'annotation')

    def should_propagate(self, schema: s_schema.Schema) -> bool:
        return self.get_annotation(schema).get_inheritable(schema)

    def __str__(self) -> str:
        return '<{}: at 0x{:x}>'.format(self.__class__.__name__, id(self))

    __repr__ = __str__

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'annotation'

    def get_verbosename(
        self, schema: s_schema.Schema, *, with_parent: bool = False
    ) -> str:
        vn = super().get_verbosename(schema)
        if with_parent:
            subject = self.get_subject(schema)
            assert subject is not None
            pvn = subject.get_verbosename(schema, with_parent=True)
            return f'{vn} of {pvn}'
        else:
            return vn


T = TypeVar("T")


class AnnotationSubject(so.Object):

    annotations_refs = so.RefDict(
        attr='annotations',
        ref_cls=AnnotationValue)

    annotations = so.SchemaField(
        so.ObjectIndexByShortname[AnnotationValue],
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.909,
        default=so.DEFAULT_CONSTRUCTOR)

    def get_annotation(
        self,
        schema: s_schema.Schema,
        name: sn.QualName,
    ) -> Optional[str]:
        attrval = self.get_annotations(schema).get(schema, name, None)
        return attrval.get_value(schema) if attrval is not None else None

    def must_get_annotation(
        self,
        schema: s_schema.Schema,
        name: sn.QualName,
    ) -> str:
        annotation_text = self.get_annotation(schema, name)
        if annotation_text is None:
            vn = self.get_verbosename(schema, with_parent=True)
            raise errors.SchemaDefinitionError(
                f"annotation {name} on {vn} is not set")

        return annotation_text

    def get_json_annotation(
        self,
        schema: s_schema.Schema,
        name: sn.QualName,
        t: Callable[[Any], T],
    ) -> Optional[T]:
        annotation_text = self.get_annotation(schema, name)
        if annotation_text is None:
            return None
        else:
            try:
                value = json.loads(annotation_text)
            except Exception:
                vn = self.get_verbosename(schema, with_parent=True)
                raise errors.SchemaDefinitionError(
                    f"annotation {name} on {vn} is not set to "
                    f"a valid JSON value")

            try:
                return t(value)
            except Exception as e:
                vn = self.get_verbosename(schema, with_parent=True)
                raise errors.SchemaDefinitionError(
                    f"annotation {name} on {vn} is not set to "
                    f"JSON containing a valid value of type {t}: {e}"
                )

    def must_get_json_annotation(
        self,
        schema: s_schema.Schema,
        name: sn.QualName,
        t: Callable[[Any], T],
    ) -> T:
        value = self.get_json_annotation(schema, name, t)
        if value is None:
            vn = self.get_verbosename(schema, with_parent=True)
            raise errors.SchemaDefinitionError(
                f"annotation {name} is not set on {vn}"
            )
        else:
            return value


class Annotation(
    so.QualifiedObject,
    so.InheritingObject,
    AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.ANNOTATION,
    data_safe=True,
):

    inheritable = so.SchemaField(
        bool, default=False, compcoef=0.2)

    def get_verbosename(
        self, schema: s_schema.Schema, *, with_parent: bool = False
    ) -> str:
        vn = super().get_verbosename(schema)
        return f"abstract {vn}"


# HACK?: Fix up annotation field in AnnotationValue to have the proper type
AnnotationValue.get_field('annotation').type = Annotation


class AnnotationSubjectCommandContext:
    pass


class AnnotationSubjectCommand(sd.ObjectCommand[so.Object_T]):
    pass


class AnnotationCommandContext(sd.ObjectCommandContext[Annotation],
                               AnnotationSubjectCommandContext):
    pass


class AnnotationCommand(sd.QualifiedObjectCommand[Annotation],
                        AnnotationSubjectCommand[Annotation],
                        context_class=AnnotationCommandContext):

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if field in {'abstract', 'inheritable'}:
            return field
        else:
            return super().get_ast_attr_for_field(field, astnode)


class CreateAnnotation(AnnotationCommand, sd.CreateObject[Annotation]):
    astnode = qlast.CreateAnnotation

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> CreateAnnotation:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, qlast.CreateAnnotation)

        cmd.set_attribute_value('inheritable', astnode.inheritable)
        cmd.set_attribute_value('abstract', True)

        assert isinstance(cmd, CreateAnnotation)
        return cmd


class RenameAnnotation(AnnotationCommand, sd.RenameObject[Annotation]):

    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: Annotation,
    ) -> None:
        super()._canonicalize(schema, context, scls)

        # AnnotationValues have names derived from the abstract
        # annotations. We unfortunately need to go update their names.
        annot_vals = cast(
            AbstractSet[AnnotationValue],
            schema.get_referrers(
                scls, scls_type=AnnotationValue, field_name='annotation'))

        for ref in annot_vals:
            if ref.get_implicit_bases(schema):
                # This annotation value is inherited, and presumably
                # the rename in parent will propagate.
                continue
            ref_name = ref.get_name(schema)
            quals = list(sn.quals_from_fullname(ref_name))
            new_ref_name = sn.QualName(
                name=sn.get_specialized_name(self.new_name, *quals),
                module=ref_name.module,
            )

            self.add(self.init_rename_branch(
                ref,
                new_ref_name,
                schema=schema,
                context=context,
            ))


class AlterAnnotation(AnnotationCommand, sd.AlterObject[Annotation]):
    astnode = qlast.AlterAnnotation


class DeleteAnnotation(AnnotationCommand, sd.DeleteObject[Annotation]):
    astnode = qlast.DropAnnotation


class AnnotationValueCommandContext(sd.ObjectCommandContext[AnnotationValue]):
    pass


class AnnotationValueCommand(
    referencing.ReferencedInheritingObjectCommand[AnnotationValue],
    context_class=AnnotationValueCommandContext,
    referrer_context_class=AnnotationSubjectCommandContext,
):

    def _deparse_name(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: sn.Name,
    ) -> qlast.ObjectRef:
        ref = super()._deparse_name(schema, context, name)
        # Clear `itemclass`
        ref.itemclass = None
        return ref

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        parent_ctx = cls.get_referrer_context_or_die(context)
        assert isinstance(parent_ctx.op, sd.QualifiedObjectCommand)
        referrer_name = context.get_referrer_name(parent_ctx)
        base_ref = utils.ast_to_object_shell(
            astnode.name,
            modaliases=context.modaliases,
            schema=schema,
            metaclass=Annotation,
        )

        base_name = base_ref.name
        quals = cls._classname_quals_from_ast(
            schema, astnode, base_name, referrer_name, context)
        pnn = sn.get_specialized_name(base_name, str(referrer_name), *quals)
        return sn.QualName(name=pnn, module=referrer_name.module)

    def populate_ddl_identity(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().populate_ddl_identity(schema, context)
        if not isinstance(self, sd.CreateObject):
            anno = self.scls.get_annotation(schema)
        else:
            annoname = sn.shortname_from_fullname(self.classname)
            anno = schema.get(annoname, type=Annotation)
        self.set_ddl_identity('annotation', anno)
        return schema


class CreateAnnotationValue(
    AnnotationValueCommand,
    referencing.CreateReferencedInheritingObject[AnnotationValue],
):
    astnode = qlast.CreateAnnotationValue
    referenced_astnode = qlast.CreateAnnotationValue

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext
    ) -> CreateAnnotationValue:
        assert isinstance(astnode, qlast.CreateAnnotationValue)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, CreateAnnotationValue)
        annoname = sn.shortname_from_fullname(cmd.classname)

        value, ir = qlcompiler.evaluate_ast_to_python_val_and_ir(
            astnode.value, schema=schema)

        if ir.stype.get_name(schema) != sn.QualName('std', 'str'):
            vn = ir.stype.get_verbosename(schema)
            raise errors.InvalidValueError(
                f"annotation values must be 'std::str', got {vn}",
                span=astnode.value.span,
            )

        anno = utils.ast_objref_to_object_shell(
            utils.name_to_ast_ref(annoname),
            metaclass=Annotation,
            modaliases=context.modaliases,
            schema=schema,
        )

        cmd.set_attribute_value('annotation', anno)
        cmd.set_attribute_value('value', value)

        return cmd

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        anno = self.get_ddl_identity('annotation')
        assert anno is not None
        self.set_attribute_value('internal', True)
        return schema

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'value':
            assert isinstance(op.new_value, str)
            assert isinstance(node, (
                qlast.CreateAnnotationValue, qlast.AlterAnnotationValue))
            node.value = qlast.Constant.string(op.new_value)
        else:
            super()._apply_field_ast(schema, context, node, op)


class AlterAnnotationValueOwned(
    referencing.AlterOwned[AnnotationValue],
    AnnotationValueCommand,
    field='owned',
    referrer_context_class=AnnotationSubjectCommandContext,
):
    pass


class AlterAnnotationValue(
    AnnotationValueCommand,
    referencing.AlterReferencedInheritingObject[AnnotationValue],
):

    astnode = qlast.AlterAnnotationValue
    referenced_astnode = qlast.AlterAnnotationValue

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext
    ) -> AlterAnnotationValue:
        assert isinstance(
            astnode,
            (qlast.CreateAnnotationValue, qlast.AlterAnnotationValue),
        )
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, AlterAnnotationValue)

        if astnode.value is not None:
            value, ir = qlcompiler.evaluate_ast_to_python_val_and_ir(
                astnode.value, schema=schema)

            if ir.stype.get_name(schema) != sn.QualName('std', 'str'):
                vn = ir.stype.get_verbosename(schema)
                raise errors.InvalidValueError(
                    f"annotation values must be 'std::str', got {vn}",
                    span=astnode.value.span,
                )

            cmd.set_attribute_value(
                'value',
                value,
            )

        annoname = sn.shortname_from_fullname(cmd.classname)
        anno = utils.ast_objref_to_object_shell(
            utils.name_to_ast_ref(annoname),
            metaclass=Annotation,
            modaliases=context.modaliases,
            schema=schema,
        )
        cmd.set_attribute_value('annotation', value=anno, orig_value=anno)

        return cmd

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if (
            not self.has_attribute_value('value')
            and not self.has_attribute_value('owned')
        ):
            return None
        # Skip AlterObject's _get_ast, because we *don't* want to
        # filter out things without subcommands!
        return sd.ObjectCommand._get_ast(
            self, schema, context, parent_node=parent_node)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty
    ) -> None:
        assert isinstance(node, qlast.AlterAnnotationValue)

        if op.property == 'value':
            assert isinstance(op.new_value, str)
            node.value = qlast.Constant.string(op.new_value)
        else:
            super()._apply_field_ast(schema, context, node, op)


class RebaseAnnotationValue(
    AnnotationValueCommand,
    referencing.RebaseReferencedInheritingObject[AnnotationValue],
):
    pass


class RenameAnnotationValue(
    AnnotationValueCommand,
    referencing.RenameReferencedInheritingObject[AnnotationValue],
):
    pass


class DeleteAnnotationValue(
    AnnotationValueCommand,
    referencing.DeleteReferencedInheritingObject[AnnotationValue],
):

    astnode = qlast.DropAnnotationValue

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext
    ) -> DeleteAnnotationValue:
        assert isinstance(astnode, qlast.DropAnnotationValue)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, DeleteAnnotationValue)
        annoname = sn.shortname_from_fullname(cmd.classname)
        anno = utils.ast_objref_to_object_shell(
            utils.name_to_ast_ref(annoname),
            metaclass=Annotation,
            modaliases=context.modaliases,
            schema=schema,
        )

        cmd.set_attribute_value('annotation', value=None, orig_value=anno)

        return cmd

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        anno = self.get_ddl_identity('annotation')
        assert anno is not None
        self.set_attribute_value(
            'annotation',
            value=None,
            orig_value=anno,
        )
        return schema
