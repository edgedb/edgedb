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

from edb import errors

from edb.common import struct

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.schema import defines as s_def

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import objects as so
from . import schema as s_schema

from typing import cast


class Branch(
    so.ExternalObject,
    s_anno.AnnotationSubject,
    s_abc.Branch,
    qlkind=qltypes.SchemaObjectClass.DATABASE,
    data_safe=False,
):
    pass


class BranchCommandContext(sd.ObjectCommandContext[Branch]):
    pass


class BranchCommand(
    sd.ExternalObjectCommand[Branch],
    context_class=BranchCommandContext,
):

    def _validate_name(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        name = self.get_attribute_value('name')
        if len(str(name)) > s_def.MAX_NAME_LENGTH:
            span = self.get_attribute_span('name')
            raise errors.SchemaDefinitionError(
                f'Branch names longer than {s_def.MAX_NAME_LENGTH} '
                f'characters are not supported',
                span=span,
            )


class CreateBranch(BranchCommand, sd.CreateExternalObject[Branch]):

    astnode = qlast.CreateDatabase
    template = struct.Field(str, default=None)
    branch_type = struct.Field(
        qlast.BranchType, default=qlast.BranchType.EMPTY)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> CreateBranch:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, CreateBranch)

        assert isinstance(astnode, qlast.CreateDatabase)
        if astnode.template is not None:
            cmd.template = astnode.template.name

        if (
            astnode.branch_type == qlast.BranchType.TEMPLATE
            and not context.testmode
        ):
            raise errors.EdgeQLSyntaxError(
                f'unexpected TEMPLATE',
                span=astnode.span,
            )

        cmd.branch_type = astnode.branch_type

        return cmd

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        # no call to super().validate_create() as we don't want to enforce
        # rules that hold for any other schema objects
        self._validate_name(schema, context)


class AlterBranch(BranchCommand, sd.AlterExternalObject[Branch]):
    astnode = qlast.AlterDatabase

    def validate_alter(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_alter(schema, context)
        self._validate_name(schema, context)


class DropBranch(BranchCommand, sd.DeleteExternalObject[Branch]):
    astnode = qlast.DropDatabase

    def _validate_legal_command(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super()._validate_legal_command(schema, context)
        if self.classname.name in s_def.EDGEDB_SPECIAL_DBS:
            raise errors.ExecutionError(
                f"database {self.classname.name!r} cannot be dropped"
            )


class RenameBranch(BranchCommand, sd.RenameObject[Branch]):
    # databases are ExternalObjects, so they might not be properly
    # present in the schema, so we can't do a proper rename.
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        scls = self.get_parent_op(context).scls
        self.scls = cast(Branch, scls)
        return schema
