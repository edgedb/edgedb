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


from .. import common
from . import base
from . import ddl


class Database(base.DBObject):
    def __init__(self, name, owner=None):
        super().__init__()
        self.name = name
        self.owner = owner
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
        extra = ''
        if self.object.owner:
            extra += f' OWNER={self.object.owner}'
        return (f'CREATE DATABASE {self.object.get_id()} '
                f'WITH TEMPLATE=edgedb0 {extra}')


class DropDatabase(ddl.SchemaObjectOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

    async def code(self, context):
        return 'DROP DATABASE {}'.format(common.quote_ident(self.name))
