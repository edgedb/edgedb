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


class SchemaExists(base.Condition):
    def __init__(self, name):
        self.name = name

    def code(self, block: base.PLBlock) -> str:
        return textwrap.dedent(f'''\
            SELECT
                oid
            FROM
                pg_catalog.pg_namespace
            WHERE
                nspname = {ql(self.name)}
        ''')


class CreateSchema(ddl.DDLOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.name = name
        self.opid = name

    def code(self, block: base.PLBlock) -> str:
        return f'CREATE SCHEMA {qi(self.name)}'

    def __repr__(self):
        return '<edb.sync.%s %s>' % (self.__class__.__name__, self.name)


class RenameSchema(ddl.SchemaObjectOperation):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    def code(self, block: base.PLBlock) -> str:
        return f'ALTER SCHEMA {qi(self.name)} RENAME TO {qi(self.new_name)}'


class DropSchema(ddl.DDLOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.name = name

    def code(self, block: base.PLBlock) -> str:
        return f'DROP SCHEMA {qi(self.name)}'

    def __repr__(self):
        return '<edb.sync.%s %s>' % (self.__class__.__name__, self.name)
