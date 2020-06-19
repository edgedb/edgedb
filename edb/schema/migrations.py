#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


"""Implementation of MIGRATION objects."""


from __future__ import annotations
from typing import *

import base64
import hashlib

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import qltypes
from edb._edgeql_rust import tokenize as qltokenize

from . import abc as s_abc
from . import delta as sd
from . import objects as so
from . import utils as s_utils

if TYPE_CHECKING:
    from . import schema as s_schema


class Migration(
    so.Object,
    s_abc.Migration,
    qlkind=qltypes.SchemaObjectClass.MIGRATION,
):

    parents = so.SchemaField(
        so.ObjectList["Migration"],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True,
    )

    message = so.SchemaField(
        str,
        default=None,
        allow_ddl_set=True,
    )

    script = so.SchemaField(
        str,
    )


class MigrationCommandContext(sd.ObjectCommandContext[Migration]):
    pass


class MigrationCommand(sd.ObjectCommand[Migration],
                       schema_metaclass=Migration,
                       context_class=MigrationCommandContext):
    pass


class CreateMigration(MigrationCommand, sd.CreateObject[Migration]):

    astnode = qlast.CreateMigration

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> CreateMigration:
        assert isinstance(astnode, qlast.CreateMigration)

        if astnode.commands:
            text = ';\n'.join(
                qlcodegen.generate_source(stmt)
                for stmt in astnode.commands
            ) + ';'
        else:
            text = ''

        tokenstream = ''.join(token.text() for token in qltokenize(text))
        if astnode.parent is None:
            parent = None
        else:
            parent = s_utils.ast_objref_to_object_shell(
                astnode.parent,
                metaclass=Migration,
                schema=schema,
                modaliases={},
            )
            tokenstream = f'PARENT: {astnode.parent.name}\n{tokenstream}'

        hashsum = hashlib.sha256(tokenstream.encode()).digest()
        name = f'm1{base64.b32encode(hashsum).decode().strip("=").lower()}'

        cmd = cls(classname=name)
        cmd.set_attribute_value('script', text)
        cmd.set_attribute_value('builtin', False)
        if parent is not None:
            cmd.set_attribute_value('parents', [parent])

        if astnode.auto_diff is not None:
            cmd.canonical = True

        return cmd

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> CreateMigration:
        assert isinstance(astnode, qlast.CreateMigration)

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if astnode.auto_diff is not None:
            for subcmd in list(cmd.get_subcommands()):
                if not isinstance(subcmd, sd.AlterObjectProperty):
                    cmd.discard(subcmd)
            for subcmd in astnode.auto_diff.get_subcommands():
                cmd.add(subcmd)

        assert isinstance(cmd, CreateMigration)

        return cmd


class AlterMigration(MigrationCommand, sd.AlterObject[Migration]):

    astnode = qlast.AlterMigration


class DeleteMigration(MigrationCommand, sd.DeleteObject[Migration]):

    astnode = qlast.DropMigration
