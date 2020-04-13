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

from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import delta as sd
from . import objects as so

if TYPE_CHECKING:
    from . import schema as s_schema


class Migration(so.Object, s_abc.Migration):

    parents = so.SchemaField(
        so.ObjectList['Migration'],
        default=so.DEFAULT_CONSTRUCTOR, coerce=True, inheritable=False)

    target = so.SchemaField(
        qlast.Schema,
        inheritable=False, default=None, introspectable=False)

    # type ignore below, because this class is redefining a new member
    # with the same name
    delta = so.SchemaField(  # type: ignore
        sd.DeltaRoot,
        default=None,
        coerce=True, inheritable=False, introspectable=False)


class MigrationCommandContext(sd.ObjectCommandContext[Migration]):
    pass


class MigrationCommand(sd.ObjectCommand[Migration],
                       schema_metaclass=Migration,
                       context_class=MigrationCommandContext):
    pass


class CreateMigration(MigrationCommand, sd.CreateObject[Migration]):
    astnode = qlast.CreateMigration

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, qlast.CreateMigration)
        if astnode.target is not None:
            cmd.set_attribute_value('target', astnode.target)

        return cmd


class AlterMigration(MigrationCommand):
    astnode = qlast.AlterMigration


class DeleteMigration(MigrationCommand):
    astnode = qlast.DropMigration


class CommitMigration(MigrationCommand):
    astnode = qlast.CommitMigration

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        migration = schema.get(self.classname, type=Migration)
        delta = migration.get_delta(schema)
        assert delta is not None
        schema = delta.apply(schema, context)
        return schema


class GetMigration(MigrationCommand):
    astnode = qlast.GetMigration

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        return schema
