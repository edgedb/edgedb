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

from typing import *

from edgedb import scram

from edb import errors
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import inheriting
from . import objects as so

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class Role(so.GlobalObject, so.InheritingObject,
           s_anno.AnnotationSubject, qlkind=qltypes.SchemaObjectClass.ROLE):

    is_superuser = so.SchemaField(
        bool,
        default=False,
        inheritable=False)

    password = so.SchemaField(
        str,
        default=None,
        allow_ddl_set=True,
        inheritable=False)


class RoleCommandContext(
        sd.ObjectCommandContext[Role],
        s_anno.AnnotationSubjectCommandContext):
    pass


class RoleCommand(sd.GlobalObjectCommand,
                  inheriting.InheritingObjectCommand,
                  s_anno.AnnotationSubjectCommand,
                  schema_metaclass=Role,
                  context_class=RoleCommandContext):

    @classmethod
    def _process_role_body(
        cls,
        cmd: sd.Command,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> None:
        password = cmd.get_attribute_value('password')
        if password is not None:
            salted_password = scram.build_verifier(password)
            cmd.set_attribute_value('password', salted_password)

    @classmethod
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> so.ObjectList[so.InheritingObject]:
        result = []
        for b in getattr(astnode, 'bases', None) or []:
            result.append(
                schema.get_global(cls.get_schema_metaclass(), b.maintype.name)
            )

        return so.ObjectList.create(schema, result)


class CreateRole(RoleCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateRole

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.CreateRole)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if not astnode.superuser and not context.testmode:
            raise errors.EdgeQLSyntaxError(
                'missing required SUPERUSER qualifier',
                context=astnode.context,
            )

        cmd.set_attribute_value('is_superuser', astnode.superuser)
        cls._process_role_body(cmd, schema, astnode, context)
        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'is_superuser':
            node.superuser = op.new_value
            return

        super()._apply_field_ast(schema, context, node, op)


class RebaseRole(RoleCommand, inheriting.RebaseInheritingObject):
    pass


class RenameRole(RoleCommand, sd.RenameObject):
    pass


class AlterRole(RoleCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterRole

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cls._process_role_body(cmd, schema, astnode, context)
        return cmd


class DeleteRole(RoleCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropRole
