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


from edb import edgeql
from edb.edgeql import ast as qlast

from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as sn
from . import objects as so
from . import referencing
from . import utils as s_utils


class Index(inheriting.InheritingObject):

    subject = so.SchemaField(so.Object)

    expr = so.SchemaField(
        s_expr.Expression, coerce=True, compcoef=0.909,
        allow_ddl_set=True)

    def __repr__(self):
        cls = self.__class__
        return '<{}.{} {!r} at 0x{:x}>'.format(
            cls.__module__, cls.__name__, self.id, id(self))

    __str__ = __repr__


class IndexableSubject(inheriting.InheritingObject):
    indexes_refs = so.RefDict(
        attr='indexes',
        local_attr='own_indexes',
        ref_cls=Index)

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


class IndexSourceCommand(inheriting.InheritingObjectCommand):
    pass


class IndexCommandContext(sd.ObjectCommandContext):
    pass


class IndexCommand(referencing.ReferencedObjectCommand,
                   schema_metaclass=Index,
                   context_class=IndexCommandContext,
                   referrer_context_class=IndexSourceCommandContext):
    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        idx_name = sn.get_specialized_name(
            sn.Name(name=astnode.name.name, module=subject_name),
            subject_name
        )

        return sn.Name(name=idx_name, module=subject_name.module)


class CreateIndex(IndexCommand, sd.CreateObject):
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
                new_value=s_expr.Expression(
                    text=edgeql.generate_source(astnode.expr, pretty=False))
            )
        ))

        return cmd

    def _create_begin(self, schema, context):
        return sd.CreateObject._create_begin(
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


class RenameIndex(IndexCommand, sd.RenameObject):
    pass


class AlterIndex(IndexCommand, sd.AlterObject):
    pass


class DeleteIndex(IndexCommand, sd.DeleteObject):
    astnode = qlast.DropIndex
