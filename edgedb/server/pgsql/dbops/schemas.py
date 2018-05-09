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


class SchemaExists(base.Condition):
    def __init__(self, name):
        self.name = name

    async def code(self, context):
        return (
            'SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1',
            [self.name])


class CreateSchema(ddl.DDLOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.name = name
        self.opid = name
        self.neg_conditions.add(SchemaExists(self.name))

    async def code(self, context):
        return 'CREATE SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<edgedb.sync.%s %s>' % (self.__class__.__name__, self.name)


class RenameSchema(ddl.SchemaObjectOperation):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    async def code(self, context):
        return '''ALTER SCHEMA {} RENAME TO {}'''.format(
            common.quote_ident(self.name),
            common.quote_ident(self.new_name)), []


class DropSchema(ddl.DDLOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name

    async def code(self, context):
        return 'DROP SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<edgedb.sync.%s %s>' % (self.__class__.__name__, self.name)
