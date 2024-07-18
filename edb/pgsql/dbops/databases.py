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
from typing import Any, Optional, Mapping

import textwrap

from ..common import quote_ident as qi
from ..common import quote_literal as ql
from ..common import versioned_schema as V

from . import base
from . import ddl


class Database(base.DBObject):
    def __init__(
        self,
        name: str,
        *,
        owner: Optional[str] = None,
        is_template: bool = False,
        encoding: Optional[str] = None,
        lc_collate: Optional[str] = None,
        lc_ctype: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(metadata=metadata)
        self.name = name
        self.owner = owner
        self.is_template = is_template
        self.encoding = encoding
        self.lc_collate = lc_collate
        self.lc_ctype = lc_ctype

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


class DatabaseWithTenant(Database):
    def __init__(
        self,
        name: str,
    ) -> None:
        super().__init__(name=name)

    def get_id(self):
        dyn_db = f"{V('edgedb')}.get_database_backend_name({ql(self.name)})"
        return f"' || quote_ident({dyn_db}) || '"

    def get_oid(self) -> base.Query:
        qry = textwrap.dedent(f'''\
            SELECT
                'pg_database'::regclass::oid AS classoid,
                pg_database.oid AS objectoid,
                0
            FROM
                pg_database
            WHERE
                datname =
                  {V("edgedb")}.get_database_backend_name({ql(self.name)})
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


class CreateDatabase(ddl.CreateObject, ddl.NonTransactionalDDLOperation):

    def __init__(self, object, *, template: str | None, **kwargs):
        super().__init__(object, **kwargs)
        self.template = template

    def code(self, block: base.PLBlock) -> str:
        extra = ''

        if self.object.owner:
            extra += f' OWNER={ql(self.object.owner)}'
        if self.object.is_template:
            extra += f' IS_TEMPLATE = TRUE'
        if self.template:
            extra += f' TEMPLATE={ql(self.template)}'
        if self.object.encoding:
            extra += f' ENCODING={ql(self.object.encoding)}'
        if self.object.lc_collate:
            extra += f' LC_COLLATE={ql(self.object.lc_collate)}'
        if self.object.lc_ctype:
            extra += f' LC_CTYPE={ql(self.object.lc_ctype)}'

        return (f'CREATE DATABASE {self.object.get_id()} {extra}')


class DropDatabase(ddl.SchemaObjectOperation,
                   ddl.NonTransactionalDDLOperation):

    def code(self, block: base.PLBlock) -> str:
        return f'DROP DATABASE {qi(self.name)}'


class RenameDatabase(ddl.AlterObject,
                     ddl.NonTransactionalDDLOperation):
    def __init__(self, object, *, old_name: str, **kwargs):
        super().__init__(object, **kwargs)
        self.old_name = old_name

    def code(self, block: base.PLBlock) -> str:
        return (
            f'ALTER DATABASE {qi(self.old_name)} '
            f'RENAME TO {self.object.get_id()}'
        )
