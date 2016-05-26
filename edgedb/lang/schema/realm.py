##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib

from . import delta as sd
from . import modules
from . import name as sn
from . import objects as so
from . import schema as ss


class RealmCommandContext(sd.CommandContextToken):
    pass


class AlterRealm(sd.Command):
    module = so.Field(sn.Name, None)
    context_class = RealmCommandContext

    def apply(self, schema, context=None):
        context = context or sd.CommandContext()

        with context(RealmCommandContext(self)):
            mods = []

            for op in self(modules.CreateModule):
                mods.append(op.apply(schema, context))

            for op in self(modules.AlterModule):
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

            for link in schema(type='link'):
                if link.target and not isinstance(link.target, so.BasePrototype):
                    link.target = schema.get(link.target)

                link.acquire_ancestor_inheritance(schema)
                link.finalize(schema)

            for link in schema(type='computable'):
                if link.target and not isinstance(link.target, so.BasePrototype):
                    link.target = schema.get(link.target)

            for concept in schema(type='concept'):
                concept.acquire_ancestor_inheritance(schema)
                concept.finalize(schema)
