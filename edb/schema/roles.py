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
from . import utils

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class Role(
    so.GlobalObject,
    so.InheritingObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.ROLE,
    data_safe=True,
):

    superuser = so.SchemaField(
        bool,
        default=False,
        inheritable=False)

    password = so.SchemaField(
        str,
        default=None,
        allow_ddl_set=True,
        inheritable=False)

    password_hash = so.SchemaField(
        str,
        default=None,
        allow_ddl_set=True,
        ephemeral=True,
        inheritable=False)


class RoleCommandContext(
        sd.ObjectCommandContext[Role],
        s_anno.AnnotationSubjectCommandContext):
    pass


class RoleCommand(
    sd.GlobalObjectCommand[Role],
    inheriting.InheritingObjectCommand[Role],
    s_anno.AnnotationSubjectCommand[Role],
    context_class=RoleCommandContext,
):

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
            if cmd.get_attribute_value('password_hash') is not None:
                raise errors.EdgeQLSyntaxError(
                    'cannot specify both `password` and `password_hash` in'
                    ' the same statement',
                    context=astnode.context,
                )
            salted_password = scram.build_verifier(password)
            cmd.set_attribute_value('password', salted_password)

        password_hash = cmd.get_attribute_value('password_hash')
        if password_hash is not None:
            try:
                scram.parse_verifier(password_hash)
            except ValueError as e:
                raise errors.InvalidValueError(
                    e.args[0],
                    context=astnode.context)
            cmd.set_attribute_value('password', password_hash)

    @classmethod
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> List[so.ObjectShell]:
        result = []
        for b in getattr(astnode, 'bases', None) or []:
            result.append(utils.ast_objref_to_object_shell(
                b.maintype,
                metaclass=Role,
                schema=schema,
                modaliases=context.modaliases,
            ))

        return result


class CreateRole(RoleCommand, inheriting.CreateInheritingObject[Role]):
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

        cmd.set_attribute_value('superuser', astnode.superuser)
        cls._process_role_body(cmd, schema, astnode, context)
        return cmd

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if (
            field == 'superuser'
            and issubclass(astnode, qlast.CreateRole)
        ):
            return 'superuser'
        else:
            return super().get_ast_attr_for_field(field, astnode)


class RebaseRole(RoleCommand, inheriting.RebaseInheritingObject[Role]):
    pass


class RenameRole(RoleCommand, sd.RenameObject[Role]):
    pass


class AlterRole(RoleCommand, inheriting.AlterInheritingObject[Role]):
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


class DeleteRole(RoleCommand, inheriting.DeleteInheritingObject[Role]):
    astnode = qlast.DropRole
