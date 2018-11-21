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

from . import delta as sd
from . import links as s_links
from . import modules
from . import named
from . import objects as so
from . import objtypes as s_objtypes


class Database(named.NamedObject):
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

            for link in schema.get_objects(type=s_links.Link):
                link_target = link.get_target(schema)
                if link_target and not isinstance(link_target, so.Object):
                    schema = link.set_field_value(
                        schema, 'target', schema.get(link_target))

                schema = link.acquire_ancestor_inheritance(schema)
                schema = link.finalize(schema)

            for objtype in schema.get_objects(type=s_objtypes.BaseObjectType):
                schema = objtype.acquire_ancestor_inheritance(schema)
                schema = objtype.finalize(schema)

        return schema, None


class DropDatabase(DatabaseCommand):
    name = struct.Field(str, None)
    astnode = qlast.DropDatabase

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        return cls(name=astnode.name.name)
