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

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.schema import defines as s_def

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import objects as so
from . import schema as s_schema


class Database(so.GlobalObject, s_anno.AnnotationSubject, s_abc.Database,
               qlkind=qltypes.SchemaObjectClass.DATABASE):
    pass


class DatabaseCommandContext(sd.ObjectCommandContext):
    pass


class DatabaseCommand(sd.GlobalObjectCommand, schema_metaclass=Database,
                      context_class=DatabaseCommandContext):

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


class CreateDatabase(DatabaseCommand, sd.CreateObject):
    astnode = qlast.CreateDatabase


class AlterDatabase(DatabaseCommand, sd.AlterObject):
    astnode = qlast.AlterDatabase


class DropDatabase(DatabaseCommand, sd.DeleteObject):
    astnode = qlast.DropDatabase
