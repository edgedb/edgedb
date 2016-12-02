##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.server.pgsql import common

from . import ddl


class Extension:
    def __init__(self, name, schema='edgedb'):
        self.name = name
        self.schema = schema

    def get_extension_name(self):
        return self.name

    async def code(self, context):
        name = common.quote_ident(self.get_extension_name())
        schema = common.quote_ident(self.schema)
        return 'CREATE EXTENSION {} WITH SCHEMA {}'.format(name, schema)

    @classmethod
    async def init_extension(cls, db):
        pass

    @classmethod
    async def reset_connection(cls, connection):
        pass


class CreateExtension(ddl.DDLOperation):
    def __init__(
            self, extension, *, conditions=None, neg_conditions=None,
            priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.extension = extension
        self.opid = extension.name

    async def code(self, context):
        return await self.extension.code(context)

    async def execute(self, context):
        await super().execute(context)
        await self.extension.init_extension(context.db)
