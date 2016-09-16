##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from .. import common
from . import base
from . import ddl


class Database(base.DBObject):
    def __init__(self, name):
        super().__init__()
        self.name = name


class DatabaseExists(base.Condition):
    def __init__(self, name):
        self.name = name

    async def code(self, context):
        code = '''SELECT
                        typname
                    FROM
                        pg_catalog.pg_database db
                    WHERE
                        datname = $1'''
        return code, self.name


class CreateDatabase(ddl.SchemaObjectOperation):
    def __init__(
            self, db, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            db.name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.db = db

    async def code(self, context):
        code = 'CREATE DATABASE {} WITH TEMPLATE=edgedb0'.format(
            common.quote_ident(self.db.name))
        return code


class DropDatabase(ddl.SchemaObjectOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

    async def code(self, context):
        return 'DROP DATABASE {}'.format(common.quote_ident(self.name))
