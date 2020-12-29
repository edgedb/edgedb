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

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import qltypes
from edb.edgeql import hasher as qlhasher
from edb.edgeql import parser as qlparser

from . import abc as s_abc
from . import delta as sd
from . import name as sn
from . import objects as so
from . import utils as s_utils

if TYPE_CHECKING:
    from . import schema as s_schema


class Migration(
    so.Object,
    s_abc.Migration,
    qlkind=qltypes.SchemaObjectClass.MIGRATION,
    data_safe=False,
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


class MigrationCommand(
    sd.ObjectCommand[Migration],
    context_class=MigrationCommandContext,
):
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
                    f'recent migration, expected {str(actual_parent_name)!r}',
                    context=astnode.parent.context,
                )

        if parent is not None:
            parent_name = parent.name
        else:
            parent_name = sn.UnqualName(name='initial')

        hasher = qlhasher.Hasher.start_migration(str(parent_name))
        if astnode.body.context is not None:
            # This is an explicitly specified CREATE MIGRATION
            src_start = astnode.body.context.start.pointer
            src_end = astnode.body.context.end.pointer
            # XXX: Workaround the rust lexer issue of returning
            # byte token offsets instead of character offsets.
            buffer = astnode.context.buffer.encode('utf-8')
            ddl_text = buffer[src_start:src_end].decode('utf-8')
        elif astnode.body.commands:
            # An implicit CREATE MIGRATION produced by START MIGRATION
            ddl_text = ';\n'.join(
                qlcodegen.generate_source(stmt)
                for stmt in astnode.body.commands
            ) + ';'
        else:
            ddl_text = ''

        hasher.add_source(ddl_text)
        name = hasher.make_migration_id()

        if specified_name is not None and name != specified_name:
            raise errors.SchemaDefinitionError(
                f'specified migration name does not match the name derived '
                f'from the migration contents: {specified_name!r}, expected '
                f'{name!r}',
                context=astnode.name.context,
            )

        cmd = cls(classname=sn.UnqualName(name))
        cmd.set_attribute_value('script', ddl_text)
        cmd.set_attribute_value('builtin', False)
        cmd.set_attribute_value('internal', False)
        if parent is not None:
            cmd.set_attribute_value('parents', [parent])

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

        if astnode.body.commands and not astnode.metadata_only:
            for subastnode in astnode.body.commands:
                subcmd = sd.compile_ddl(schema, subastnode, context=context)
                if subcmd is not None:
                    cmd.add(subcmd)

        assert isinstance(cmd, CreateMigration)

        return cmd

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        node = super()._get_ast(schema, context, parent_node=parent_node)
        assert isinstance(node, qlast.CreateMigration)
        node.metadata_only = True
        return node

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        assert isinstance(node, qlast.CreateMigration)
        if op.property == 'script':
            node.script = op.new_value
            node.body = qlast.MigrationBody(
                commands=qlparser.parse_block(op.new_value)
            )
        elif op.property == 'parents':
            if op.new_value and (items := op.new_value.items):
                assert len(items) == 1
                parent = next(iter(items))
                node.parent = s_utils.name_to_ast_ref(parent.get_name(schema))
        else:
            super()._apply_field_ast(schema, context, node, op)


class AlterMigration(MigrationCommand, sd.AlterObject[Migration]):

    astnode = qlast.AlterMigration


class DeleteMigration(MigrationCommand, sd.DeleteObject[Migration]):

    astnode = qlast.DropMigration
