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

from edgedb import scram

from edb.edgeql import ast as qlast

from . import annotations
from . import delta as sd
from . import inheriting
from . import objects as so


class Role(so.GlobalObject, inheriting.InheritingObject,
           annotations.AnnotationSubject):

    allow_login = so.SchemaField(
        bool,
        default=False,
        allow_ddl_set=True,
        inheritable=False)

    is_superuser = so.SchemaField(
        bool,
        default=False,
        allow_ddl_set=True,
        inheritable=False)

    password = so.SchemaField(
        str,
        default=None,
        allow_ddl_set=True,
        inheritable=False)


class RoleCommandContext(
        sd.ObjectCommandContext,
        annotations.AnnotationSubjectCommandContext):
    pass


class RoleCommand(sd.GlobalObjectCommand,
                  inheriting.InheritingObjectCommand,
                  annotations.AnnotationSubjectCommand,
                  schema_metaclass=Role,
                  context_class=RoleCommandContext):

    @classmethod
    def _process_role_body(cls, cmd, schema, astnode, context):
        password = cmd.get_attribute_value('password')
        if password is not None:
            salted_password = scram.build_verifier(password)
            cmd.set_attribute_value('password', salted_password)

    @classmethod
    def _classbases_from_ast(cls, schema, astnode, context):
        result = []
        for b in getattr(astnode, 'bases', None) or []:
            result.append(
                schema.get_global(cls.get_schema_metaclass(), b.maintype.name)
            )

        return so.ObjectList.create(schema, result)


class CreateRole(RoleCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateRole

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cls._process_role_body(cmd, schema, astnode, context)
        return cmd


class RebaseRole(RoleCommand, inheriting.RebaseInheritingObject):
    pass


class RenameRole(RoleCommand, sd.RenameObject):
    pass


class AlterRole(RoleCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterRole

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cls._process_role_body(cmd, schema, astnode, context)
        return cmd


class DeleteRole(RoleCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropRole
