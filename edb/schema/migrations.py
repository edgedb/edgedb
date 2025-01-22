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
from typing import Optional, TYPE_CHECKING

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser
import edb._edgeql_parser as ql_parser

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

    generated_by = so.SchemaField(
        str,
        default=None,
        allow_ddl_set=True,
    )

    script = so.SchemaField(
        str,
    )

    sdl = so.SchemaField(
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

        parent: Optional[so.ObjectShell[Migration]]

        if parent_migration is not None:
            parent = parent_migration.as_shell(schema)
            parent_name = str(parent.name)
        else:
            parent = None
            parent_name = 'initial'

        if astnode.parent is not None:
            parent_name = astnode.parent.name

        hasher = ql_parser.Hasher.start_migration(parent_name)
        if astnode.body.text is not None:
            # This is an explicitly specified CREATE MIGRATION
            ddl_text = astnode.body.text
        elif astnode.body.commands:
            # An implicit CREATE MIGRATION produced by START MIGRATION
            ddl_text = ';\n'.join(
                qlcodegen.generate_source(stmt, uppercase=True)
                for stmt in [*astnode.commands, *astnode.body.commands]
            ) + ';'
        else:
            ddl_text = ''

        hasher.add_source(ddl_text)
        name = hasher.make_migration_id()

        sdl_text: Optional[str] = astnode.target_sdl

        if specified_name is not None and name != specified_name:
            raise errors.SchemaDefinitionError(
                f'specified migration name does not match the name derived '
                f'from the migration contents: {specified_name!r}, expected '
                f'{name!r}',
                span=astnode.name.span,
            )

        if specified_name is not None and schema.has_migration(specified_name):
            # Note: it's not possible to have duplicate migration without
            # `specified_name`. Because new one will be based onto the new
            # parent (and you can't specify parent without a name).
            raise errors.DuplicateMigrationError(
                f'migration {name!r} is already applied',
                span=astnode.name.span,
            )

        if astnode.parent is not None:
            if parent_migration is None:
                if astnode.parent.name.lower() != 'initial':
                    raise errors.SchemaDefinitionError(
                        f'specified migration parent does not exist',
                        span=astnode.parent.span,
                    )
            else:
                astnode_parent = s_utils.ast_objref_to_object_shell(
                    astnode.parent,
                    metaclass=Migration,
                    schema=schema,
                    modaliases={},
                )

                actual_parent_name = parent_migration.get_name(schema)
                if astnode_parent.name != actual_parent_name:
                    raise errors.SchemaDefinitionError(
                        f'specified migration parent is not the most recent '
                        f'migration, expected {str(actual_parent_name)!r}',
                        span=astnode.parent.span,
                    )

        cmd = cls(classname=sn.UnqualName(name))
        cmd.set_attribute_value('script', ddl_text)
        cmd.set_attribute_value('sdl', sdl_text)
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

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        from . import ddl as s_ddl

        new_schema = super().apply(schema, context)

        if (
            context.store_migration_sdl
            and not self.get_attribute_value('sdl')
        ):
            # If target sdl was not known in advance, compute it now.
            new_sdl: str = s_ddl.sdl_text_from_schema(new_schema)
            new_schema = self.scls.set_field_value(new_schema, 'sdl', new_sdl)
            self.set_attribute_value('sdl', new_sdl)

        return new_schema

    def apply_subcommands(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        assert not self.get_prerequisites() and not self.get_caused()
        # Renames shouldn't persist between commands in a migration script.
        context.renames.clear()
        for op in self.get_subcommands(
            include_prerequisites=False,
            include_caused=False,
        ):
            if not isinstance(op, sd.AlterObjectProperty):
                schema = op.apply(schema, context=context)
                context.renames.clear()
        return schema

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
            block, _ = qlparser.parse_migration_body_block(op.new_value)
            node.body = qlast.NestedQLBlock(
                commands=block.commands,
                text=op.new_value,
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


def get_ordered_migrations(
    schema: s_schema.Schema,
) -> list[Migration]:
    '''Get all the migrations, in order.

    It would be nice if our toposort could do this for us, but
    toposort is implemented recursively, and it would be a pain to
    change that.

    '''
    output = []
    mig = schema.get_last_migration()
    while mig:
        output.append(mig)

        parents = mig.get_parents(schema).objects(schema)
        assert len(parents) <= 1, "only one parent supported currently"
        mig = parents[0] if parents else None

    output.reverse()

    return output
