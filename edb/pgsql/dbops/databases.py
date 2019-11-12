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

import textwrap

from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base
from . import ddl


class Database(base.DBObject):
    def __init__(self, name, owner=None):
        super().__init__()
        self.name = name
        self.owner = owner

    def get_type(self):
        return 'DATABASE'

    def get_id(self):
        return qi(self.name)

    def is_shared(self) -> bool:
        return True

    def get_oid(self) -> base.Query:
        qry = textwrap.dedent(f'''\
            SELECT
                'pg_database'::regclass::oid AS classoid,
                pg_database.oid AS objectoid,
                0
            FROM
                pg_database
            WHERE
                datname = {ql(self.name)}
        ''')

        return base.Query(text=qry)


class DatabaseExists(base.Condition):
    def __init__(self, name):
        self.name = name

    def code(self, block: base.PLBlock) -> str:
        return textwrap.dedent(f'''\
            SELECT
                typname
            FROM
                pg_catalog.pg_database AS db
            WHERE
                datname = {ql(self.name)}
        ''')


class CreateDatabase(ddl.CreateObject):
    def __init__(
            self, db, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            db.name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.object = db

    def code(self, block: base.PLBlock) -> str:
        extra = ''
        if self.object.owner:
            extra += f' OWNER={qi(self.object.owner)}'
        return (f'CREATE DATABASE {self.object.get_id()} '
                f'WITH TEMPLATE=edgedb0 {extra}')


class DropDatabase(ddl.SchemaObjectOperation):

    def code(self, block: base.PLBlock) -> str:
        return f'DROP DATABASE {qi(self.name)}'
