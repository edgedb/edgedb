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


from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import referencing
from . import utils as s_utils


class SourceIndex(inheriting.InheritingObject):
    _type = 'index'

    subject = so.SchemaField(so.NamedObject)

    expr = so.SchemaField(
        s_expr.ExpressionText, coerce=True, compcoef=0.909)

    def __repr__(self):
        cls = self.__class__
        return '<{}.{} {!r} at 0x{:x}>'.format(
            cls.__module__, cls.__name__, self.id, id(self))

    __str__ = __repr__


class IndexableSubject(referencing.ReferencingObject):
    indexes_refs = referencing.RefDict(
        attr='indexes',
        local_attr='own_indexes',
        ref_cls=SourceIndex)

    indexes = so.SchemaField(
        so.ObjectIndexByShortname,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByShortname, hashable=False)

    own_indexes = so.SchemaField(
        so.ObjectIndexByShortname, compcoef=0.909,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByShortname)

    def add_index(self, schema, index):
        return self.add_classref(schema, 'indexes', index)


class IndexSourceCommandContext:
    pass


class IndexSourceCommand(referencing.ReferencingObjectCommand):
    pass


class SourceIndexCommandContext(sd.ObjectCommandContext):
    pass


class SourceIndexCommand(referencing.ReferencedObjectCommand,
                         schema_metaclass=SourceIndex,
                         context_class=SourceIndexCommandContext,
                         referrer_context_class=IndexSourceCommandContext):
    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        idx_name = SourceIndex.get_specialized_name(
            sn.Name(name=astnode.name.name, module=subject_name),
            subject_name
        )

        return sn.Name(name=idx_name, module=subject_name.module)


class CreateSourceIndex(SourceIndexCommand, named.CreateNamedObject):
    astnode = qlast.CreateIndex

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        parent_ctx = context.get(sd.CommandContextToken)
        subject = parent_ctx.scls

        cmd.update((
            sd.AlterObjectProperty(
                property='subject',
                new_value=s_utils.reduce_to_typeref(schema, subject)
            ),
            sd.AlterObjectProperty(
                property='expr',
                new_value=s_expr.ExpressionText(
                    edgeql.generate_source(astnode.expr, pretty=False))
            )
        ))

        return cmd

    def _create_begin(self, schema, context):
        return named.CreateNamedObject._create_begin(
            self, schema, context)

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)
        node.name.module = ''

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'expr':
            node.expr = op.new_value
        elif op.property == 'is_derived':
            pass
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameSourceIndex(SourceIndexCommand, named.RenameNamedObject):
    pass


class AlterSourceIndex(SourceIndexCommand, named.AlterNamedObject):
    pass


class DeleteSourceIndex(SourceIndexCommand, named.DeleteNamedObject):
    astnode = qlast.DropIndex
