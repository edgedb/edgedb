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

from edb import errors
from edb.common import debug

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

        if astnode.name is not None:
            specified_name = astnode.name.name
        else:
            specified_name = None

        parent_migration = schema.get_last_migration()

        parent: Optional[so.ObjectShell]

        if astnode.parent is None:
            if parent_migration is not None:
                parent = parent_migration.as_shell(schema)
            else:
                parent = None
        elif parent_migration is None:
            if astnode.parent.name.lower() == 'initial':
                parent = None
            else:
                raise errors.SchemaDefinitionError(
                    f'specified migration parent does not exist',
                    context=astnode.parent.context,
                )
        else:
            parent = s_utils.ast_objref_to_object_shell(
                astnode.parent,
                metaclass=Migration,
                schema=schema,
                modaliases={},
            )

            actual_parent_name = parent_migration.get_name(schema)
            if parent.name != actual_parent_name:
                raise errors.SchemaDefinitionError(
                    f'specified migration parent is not the most '
                    f'recent migration, expected {actual_parent_name!r}',
                    context=astnode.parent.context,
                )

        if parent is not None:
            parent_name = parent.name
        else:
            parent_name = 'initial'

        stmt_text = f'CREATE MIGRATION ONTO {parent_name}\n{{'

        if astnode.commands:
            ddl_text = ';\n'.join(
                qlcodegen.generate_source(stmt)
                for stmt in astnode.commands
            ) + ';'
        else:
            ddl_text = ''

        stmt_text += f'{ddl_text}\n}}'

        tokenstream = b'\x00'.join(
            token.text().encode('utf-8') for token in qltokenize(stmt_text)
        )

        hashsum = hashlib.sha256(tokenstream).digest()
        name = f'm1{base64.b32encode(hashsum).decode().strip("=").lower()}'

        if specified_name is not None and name != specified_name:
            raise errors.SchemaDefinitionError(
                f'specified migration name does not match the name derived '
                f'from the migration contents: {specified_name!r}, expected '
                f'{name!r}',
                context=astnode.name.context,
            )

        cmd = cls(classname=name)
        cmd.set_attribute_value('script', ddl_text)
        cmd.set_attribute_value('builtin', False)
        if parent is not None:
            cmd.set_attribute_value('parents', [parent])

        if (
            astnode.auto_diff is not None
            and not debug.flags.migrations_via_ddl
        ):
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

        if (
            astnode.auto_diff is not None
            and not debug.flags.migrations_via_ddl
        ):
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
