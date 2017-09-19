##
# Copyright (c) 2008-present MagicStack Inc.
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
        self.add_metadata('edgedb', True)

    def get_type(self):
        return 'DATABASE'

    def get_id(self):
        return common.quote_ident(self.name)


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


class CreateDatabase(ddl.CreateObject):
    def __init__(
            self, db, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            db.name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.object = db

    async def code(self, context):
        return f'CREATE DATABASE {self.object.get_id()} WITH TEMPLATE=edgedb0'


class DropDatabase(ddl.SchemaObjectOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

    async def code(self, context):
        return 'DROP DATABASE {}'.format(common.quote_ident(self.name))
