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


class Database(
    so.ExternalObject,
    s_anno.AnnotationSubject,
    s_abc.Database,
    qlkind=qltypes.SchemaObjectClass.DATABASE,
    data_safe=False,
):
    pass


class DatabaseCommandContext(sd.ObjectCommandContext[Database]):
    pass


class DatabaseCommand(
    sd.ExternalObjectCommand[Database],
    context_class=DatabaseCommandContext,
):

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)

        # Validate that the database name is fewer than 64 characters
        name = self.get_attribute_value('name')
        if len(str(name)) > s_def.MAX_NAME_LENGTH:
            source_context = self.get_attribute_source_context('name')
            raise errors.SchemaDefinitionError(
                f'Database names longer than {s_def.MAX_NAME_LENGTH} '
                f'characters are not supported',
                context=source_context,
            )

        return schema


class CreateDatabase(DatabaseCommand, sd.CreateExternalObject[Database]):

    astnode = qlast.CreateDatabase
    template = struct.Field(str, default=None)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> CreateDatabase:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, CreateDatabase)

        assert isinstance(astnode, qlast.CreateDatabase)
        if astnode.template is not None:
            if not context.testmode:
                raise errors.EdgeQLSyntaxError(
                    f'unexpected {astnode.template.name!r}',
                    context=astnode.template.context,
                )
            cmd.template = astnode.template.name

        return cmd


class AlterDatabase(DatabaseCommand, sd.AlterObject[Database]):
    astnode = qlast.AlterDatabase


class DropDatabase(DatabaseCommand, sd.DeleteExternalObject[Database]):
    astnode = qlast.DropDatabase
