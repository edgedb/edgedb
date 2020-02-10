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
from typing import *

import textwrap

from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base
from . import ddl


class Role(base.DBObject):
    def __init__(
        self,
        name: str,
        *,
        allow_login: bool = False,
        allow_createdb: bool = False,
        allow_createrole: bool = False,
        password: Optional[str] = None,
        is_superuser: bool = False,
        membership: Optional[Iterable[str]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__()
        self.name = name
        self.is_superuser = is_superuser
        self.allow_login = allow_login
        self.allow_createdb = allow_createdb
        self.allow_createrole = allow_createrole
        self.password = password
        self.membership = membership
        self.metadata = metadata

    def get_type(self):
        return 'ROLE'

    def get_id(self):
        return qi(self.name)


class RoleExists(base.Condition):
    def __init__(self, name):
        self.name = name

    def code(self, block: base.PLBlock) -> str:
        return textwrap.dedent(f'''\
            SELECT
                rolname
            FROM
                pg_catalog.pg_roles
            WHERE
                rolname = {ql(self.name)}
        ''')


class RoleCommand:

    def _render(self):
        attrs = []

        attrmap = {
            'is_superuser': 'SUPERUSER',
            'allow_login': 'LOGIN',
            'allow_createdb': 'CREATEDB',
            'allow_createrole': 'CREATEROLE',
        }

        for objattr, stmtattr in attrmap.items():
            if getattr(self.object, objattr):
                attrs.append(stmtattr)
            else:
                attrs.append(f'NO{stmtattr}')

        if self.object.password:
            attrs.append(f'PASSWORD {ql(self.object.password)}')
        else:
            attrs.append('PASSWORD NULL')

        return f'ROLE {self.object.get_id()} {" ".join(attrs)}'


class CreateRole(ddl.CreateObject, RoleCommand):
    def __init__(
            self, role, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            role.name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.object = role

    def code(self, block: base.PLBlock) -> str:
        if self.object.membership:
            roles = ', '.join(qi(m) for m in self.object.membership)
            membership = f'IN ROLE {roles}'
        else:
            membership = ''
        return f'CREATE {self._render()} {membership}'


class AlterRole(ddl.AlterObject, RoleCommand):
    def __init__(
            self, role, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            role.name, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.object = role

    def code(self, block: base.PLBlock) -> str:
        return f'ALTER {self._render()}'


class DropRole(ddl.SchemaObjectOperation):

    def code(self, block: base.PLBlock) -> str:
        return f'DROP ROLE {qi(self.name)}'


class AlterRoleAddMember(ddl.SchemaObjectOperation):

    def __init__(
            self, name, member, *, conditions=None,
            neg_conditions=None, priority=0):
        super().__init__(name, conditions=conditions,
                         neg_conditions=neg_conditions, priority=priority)
        self.member = member

    def code(self, block: base.PLBlock) -> str:
        return f'GRANT {qi(self.name)} TO {qi(self.member)}'


class AlterRoleDropMember(ddl.SchemaObjectOperation):

    def __init__(
            self, name, member, *, conditions=None,
            neg_conditions=None, priority=0):
        super().__init__(name, conditions=conditions,
                         neg_conditions=neg_conditions, priority=priority)
        self.member = member

    def code(self, block: base.PLBlock) -> str:
        return f'REVOKE {qi(self.name)} FROM {qi(self.member)}'
