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
from . import expr
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import referencing


class SourceIndex(inheriting.InheritingObject):
    _type = 'index'

    subject = so.Field(so.NamedObject)
    expr = so.Field(str, compcoef=0.909)

    def __repr__(self):
        cls = self.__class__
        return '<{}.{} {!r} {!r} at 0x{:x}>'.format(
            cls.__module__, cls.__name__, self.name, self.expr, id(self))

    __str__ = __repr__


class IndexableSubject(referencing.ReferencingObject):
    indexes = referencing.RefDict(ref_cls=SourceIndex, compcoef=0.909)

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
    def _classname_from_ast(cls, astnode, context, schema):
        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        idx_name = SourceIndex.get_specialized_name(
            sn.Name(name=astnode.name.name, module=subject_name),
            subject_name
        )

        return sn.Name(name=idx_name, module=subject_name.module)

    def _create_begin(self, schema, context):
        return inheriting.InheritingObjectCommand._create_begin(
            self, schema, context)


class CreateSourceIndex(SourceIndexCommand, named.CreateNamedObject):
    astnode = qlast.CreateIndex

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        cmd.update((
            sd.AlterObjectProperty(
                property='subject',
                new_value=so.ObjectRef(classname=subject_name)
            ),
            sd.AlterObjectProperty(
                property='expr',
                new_value=expr.ExpressionText(
                    edgeql.generate_source(astnode.expr, pretty=False))
            )
        ))

        return cmd

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        node.name.module = ''

    def _apply_field_ast(self, context, node, op):
        if op.property == 'expr':
            node.expr = op.new_value
        elif op.property == 'is_derived':
            pass
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(context, node, op)


class RenameSourceIndex(SourceIndexCommand, named.RenameNamedObject):
    pass


class AlterSourceIndex(SourceIndexCommand, named.AlterNamedObject):
    pass


class DeleteSourceIndex(SourceIndexCommand, named.DeleteNamedObject):
    astnode = qlast.DropIndex
