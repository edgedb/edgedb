##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib

from edgedb.lang.edgeql import ast as qlast


from . import delta as sd
from . import modules
from . import named
from . import objects as so


class Database(named.NamedClass):
    # Override 'name' to str type, since databases don't have
    # fully-qualified names.
    name = so.Field(str)


class DatabaseCommandContext(sd.CommandContextToken):
    pass


class DatabaseCommand(sd.ClassCommand, schema_metaclass=Database,
                      context_class=DatabaseCommandContext):
    pass


class CreateDatabase(DatabaseCommand):
    name = so.Field(str, None)
    astnode = qlast.CreateDatabase

    @classmethod
    def _cmd_from_ast(cls, astnode, context, schema):
        return cls(name=astnode.name.name, metaclass=Database)


class AlterDatabase(DatabaseCommand):
    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        with context(DatabaseCommandContext(self)):
            mods = []

            for op in self.get_subcommands(type=modules.CreateModule):
                mods.append(op.apply(schema, context))

            for op in self.get_subcommands(type=modules.AlterModule):
                mods.append(op.apply(schema, context))

            for mod in mods:
                for imported in mod.imports:
                    if not schema.has_module(imported):
                        try:
                            impmod = importlib.import_module(imported)
                        except ImportError:
                            # Reference to a non-schema external module,
                            # which might have disappeared.
                            pass
                        else:
                            schema.add_module(impmod)

            for op in self:
                if not isinstance(op, (modules.CreateModule,
                                       modules.AlterModule)):
                    op.apply(schema, context)

            for link in schema.get_objects(type='link'):
                if link.target and not isinstance(link.target,
                                                  so.Class):
                    link.target = schema.get(link.target)

                link.acquire_ancestor_inheritance(schema)
                link.finalize(schema)

            for link in schema.get_objects(type='computable'):
                if link.target and not isinstance(link.target,
                                                  so.Class):
                    link.target = schema.get(link.target)

            for concept in schema.get_objects(type='concept'):
                concept.acquire_ancestor_inheritance(schema)
                concept.finalize(schema)


class DropDatabase(DatabaseCommand):
    name = so.Field(str, None)
    astnode = qlast.DropDatabase

    @classmethod
    def _cmd_from_ast(cls, astnode, context, schema):
        return cls(name=astnode.name.name, metaclass=Database)
