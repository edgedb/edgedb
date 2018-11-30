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


from edb.lang.common import struct
from edb.lang.edgeql import ast as qlast

from . import abc as s_abc
from . import delta as sd
from . import modules
from . import objects as so


class Database(so.Object, s_abc.Database):

    # Override 'name' to str type, since databases don't have
    # fully-qualified names.
    name = so.SchemaField(str)


class DatabaseCommandContext(sd.CommandContextToken):
    pass


class DatabaseCommand(sd.ObjectCommand, schema_metaclass=Database,
                      context_class=DatabaseCommandContext):
    pass


class CreateDatabase(DatabaseCommand):
    name = struct.Field(str, None)
    astnode = qlast.CreateDatabase

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        return cls(name=astnode.name.name)


class AlterDatabase(DatabaseCommand):
    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        with context(DatabaseCommandContext(self)):
            mods = []

            for op in self.get_subcommands(type=modules.CreateModule):
                schema, mod = op.apply(schema, context)
                mods.append(mod)

            for op in self.get_subcommands(type=modules.AlterModule):
                schema, mod = op.apply(schema, context)
                mods.append(mod)

            for op in self:
                if not isinstance(op, (modules.CreateModule,
                                       modules.AlterModule)):
                    schema, _ = op.apply(schema, context)

        return schema, None


class DropDatabase(DatabaseCommand):
    name = struct.Field(str, None)
    astnode = qlast.DropDatabase

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        return cls(name=astnode.name.name)
