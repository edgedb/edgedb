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

from edb import edgeql
from edb import errors
from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as sn
from . import objects as so
from . import referencing


class Index(referencing.ReferencedInheritingObject, s_anno.AnnotationSubject):

    subject = so.SchemaField(so.Object)

    expr = so.SchemaField(
        s_expr.Expression, coerce=True, compcoef=0.909)

    # Text representation of the original expression that's been
    # parsed and re-generated, but not normalized.
    origexpr = so.SchemaField(
        str, default=None, coerce=True, compcoef=0.909, allow_ddl_set=True)

    def __repr__(self):
        cls = self.__class__
        return '<{}.{} {!r} at 0x{:x}>'.format(
            cls.__module__, cls.__name__, self.id, id(self))

    __str__ = __repr__

    def get_displayname(self, schema) -> str:
        expr = self.get_expr(schema)
        return expr.origtext


class IndexableSubject(inheriting.InheritingObject):
    indexes_refs = so.RefDict(
        attr='indexes',
        ref_cls=Index)

    indexes = so.SchemaField(
        so.ObjectIndexByUnqualifiedName,
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.909,
        default=so.ObjectIndexByUnqualifiedName)

    def add_index(self, schema, index):
        return self.add_classref(schema, 'indexes', index)


class IndexSourceCommandContext:
    pass


class IndexSourceCommand(inheriting.InheritingObjectCommand):
    pass


class IndexCommandContext(sd.ObjectCommandContext,
                          s_anno.AnnotationSubjectCommandContext):
    pass


class IndexCommand(referencing.ReferencedInheritingObjectCommand,
                   schema_metaclass=Index,
                   context_class=IndexCommandContext,
                   referrer_context_class=IndexSourceCommandContext):

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = referrer_ctx.op.classname

            shortname = sn.Name(
                module='__',
                name=astnode.name.name,
            )

            quals = cls._classname_quals_from_ast(
                schema, astnode, shortname, referrer_name, context)

            name = sn.Name(
                module=referrer_name.module,
                name=sn.get_specialized_name(
                    shortname,
                    referrer_name,
                    *quals,
                ),
            )
        else:
            name = super()._classname_from_ast(schema, astnode, context)

        return name

    @classmethod
    def _classname_quals_from_ast(cls, schema, astnode, base_name,
                                  referrer_name, context):
        expr_text = None

        # check if "origexpr" field is already set
        for node in astnode.commands:
            if isinstance(node, qlast.SetField):
                if node.name == 'origexpr':
                    expr_text = node.value.value
                    break

        if expr_text is None:
            # if not, then use the origtext directly from the expression
            expr = s_expr.Expression.from_ast(
                astnode.expr, schema, context.modaliases)
            expr_text = expr.origtext

        name = (cls._name_qual_from_exprs(schema, (expr_text,)),)

        return name

    def get_object(self, schema, context, *, name=None):
        try:
            return super().get_object(schema, context, name=name)
        except errors.InvalidReferenceError:
            referrer_ctx = self.get_referrer_context(context)
            referrer = referrer_ctx.scls
            expr = self.get_attribute_value('expr')
            raise errors.InvalidReferenceError(
                f"index {expr.origtext!r} does not exist on "
                f"{referrer.get_verbosename(schema)}"
            ) from None


class CreateIndex(IndexCommand, referencing.CreateReferencedInheritingObject):
    astnode = qlast.CreateIndex
    referenced_astnode = qlast.CreateIndex

    def _create_begin(self, schema, context):
        # First we need to apply the command as is in order to get
        # access to the up-to-date state of the index.
        schema = super()._create_begin(schema, context)
        index = self.scls

        expr = self.get_attribute_value('expr')
        origexpr = index.get_field_value(schema, 'origexpr')
        # Check if the index doesn't have "origexpr" explicitly specified.
        if origexpr is None:
            # Add annotation with the original expression.
            self.set_attribute_value('origexpr', expr.origtext)
            # Because we already called super()._create_begin, we need
            # to manually reflect the attribute change into and actual
            # field change.
            schema = index.set_field_value(
                schema, 'origexpr', expr.origtext)
        else:
            # If we have an explicit "origexpr", set the "origtext"
            # value of expr.
            expr_dict = expr.__getstate__()
            expr_dict['origtext'] = origexpr
            newexpr = s_expr.Expression(**expr_dict)
            self.set_attribute_value('expr', newexpr)

        return schema

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        cmd.set_attribute_value(
            'expr',
            s_expr.Expression.from_ast(
                astnode.expr, schema, context.modaliases),
        )

        return cmd

    @classmethod
    def as_inherited_ref_ast(cls, schema, context, name, parent):
        astnode_cls = cls.referenced_astnode
        expr = parent.get_expr(schema)
        if expr is not None:
            expr_ql = edgeql.parse_fragment(expr.origtext)
        else:
            expr_ql = None

        return astnode_cls(
            name=qlast.ObjectRef(
                name=name,
                module=parent.get_shortname(schema).module,
            ),
            expr=expr_ql,
        )

    def _apply_field_ast(self, schema, context, node, op):
        if context.descriptive_mode:
            # When generating AST for DESCRIBE AS TEXT, we want
            # to use the original user-specified and unmangled
            # expression to render the index definition.
            # The mangled actual 'expr' needs to be omitted.
            if op.property == 'origexpr':
                node.expr = edgeql.parse_fragment(op.new_value)
                return
            elif op.property == 'expr':
                return
        else:
            # In all other DESCRIBE modes we want the 'origexpr'
            # to be there as a 'SET origexpr := ...' command.
            # The mangled 'expr' should be the main expression that
            # the index is defined on.
            if op.property == 'origexpr':
                node.commands.append(
                    qlast.SetField(
                        name='origexpr',
                        value=qlast.RawStringConstant.from_python(
                            op.new_value),
                    )
                )
                return
            elif op.property == 'expr':
                node.expr = op.new_value.qlast
                return

        super()._apply_field_ast(schema, context, node, op)

    def compile_expr_field(self, schema, context, field, value):
        if field.name == 'expr':
            parent_ctx = context.get_ancestor(IndexSourceCommandContext, self)
            subject_name = parent_ctx.op.classname
            subject = schema.get(subject_name, default=None)
            if not isinstance(subject, s_abc.Pointer):
                singletons = [subject]
                path_prefix_anchor = qlast.Subject
            else:
                singletons = []
                path_prefix_anchor = None

            return type(value).compiled(
                value,
                schema=schema,
                modaliases=context.modaliases,
                parent_object_type=self.get_schema_metaclass(),
                anchors={qlast.Subject: subject},
                path_prefix_anchor=path_prefix_anchor,
                singletons=singletons,
            )
        else:
            return super().compile_expr_field(schema, context, field, value)


class RenameIndex(IndexCommand, referencing.RenameReferencedInheritingObject):
    pass


class AlterIndex(IndexCommand, referencing.AlterReferencedInheritingObject):
    astnode = qlast.AlterIndex


class DeleteIndex(IndexCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropIndex

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        cmd.set_attribute_value(
            'expr',
            s_expr.Expression.from_ast(
                astnode.expr, schema, context.modaliases),
        )

        return cmd


class RebaseIndex(IndexCommand,
                  inheriting.RebaseInheritingObject):
    pass
