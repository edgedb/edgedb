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
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Union,
    TypeAlias,
)

import json
import textwrap

from ..common import quote_ident as qi
from ..common import quote_literal as ql


from . import base
from . import ddl


RoleName: TypeAlias = str


class Role(base.DBObject):
    def __init__(
        self,
        name: RoleName,
        *,
        allow_login: Union[bool, base.NotSpecifiedT] = base.NotSpecified,
        allow_createdb: Union[bool, base.NotSpecifiedT] = base.NotSpecified,
        allow_createrole: Union[bool, base.NotSpecifiedT] = base.NotSpecified,
        password: Union[None, str, base.NotSpecifiedT] = base.NotSpecified,
        superuser: Union[bool, base.NotSpecifiedT] = base.NotSpecified,
        membership: Optional[Iterable[str]] = None,
        members: Optional[Iterable[str]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(metadata=metadata)
        self.name = name
        self.superuser = superuser
        self.allow_login = allow_login
        self.allow_createdb = allow_createdb
        self.allow_createrole = allow_createrole
        self.password = password
        self.membership = membership
        self.members = members

    def get_type(self) -> str:
        return 'ROLE'

    def get_id(self) -> str:
        return qi(self.name)


class SingleRole(Role):
    def __init__(
        self,
        *,
        password: Union[None, str, base.NotSpecifiedT] = base.NotSpecified,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__('current_user', password=password)
        self.single_role_metadata = metadata

    def get_id(self) -> str:
        return self.name


class RoleExists(base.Condition):
    def __init__(self, name: RoleName):
        self.name = name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                rolname
            FROM
                pg_catalog.pg_roles
            WHERE
                rolname = {ql(self.name)}
        ''')


class RoleCommand:

    object: Role

    def _role(self) -> str:
        return f'ROLE {self.object.get_id()}'

    def _attrs(self) -> str:
        attrs = []

        attrmap = {
            'superuser': 'SUPERUSER',
            'allow_login': 'LOGIN',
            'allow_createdb': 'CREATEDB',
            'allow_createrole': 'CREATEROLE',
        }

        for objattr, stmtattr in attrmap.items():
            attr = getattr(self.object, objattr)
            if attr is base.NotSpecified:
                continue
            elif attr:
                attrs.append(stmtattr)
            else:
                attrs.append(f'NO{stmtattr}')

        if self.object.password is None:
            attrs.append('PASSWORD NULL')
        elif self.object.password is not base.NotSpecified:
            attrs.append(f'PASSWORD {ql(self.object.password)}')

        return " ".join(attrs)


class CreateRole(ddl.CreateObject, RoleCommand):

    def code(self) -> str:
        if self.object.membership:
            roles = ', '.join(qi(str(m)) for m in self.object.membership)
            membership = f'IN ROLE {roles}'
        else:
            membership = ''
        if self.object.members:
            roles = ', '.join(qi(str(m)) for m in self.object.members)
            members = f'ROLE {roles}'
        else:
            members = ''
        return f'CREATE {self._role()} {self._attrs()} {membership} {members}'


class AlterRole(ddl.AlterObject, RoleCommand):

    def code(self) -> str:
        attrs = self._attrs()
        if attrs:
            return f'ALTER {self._role()} {attrs}'
        else:
            return ''

    def generate_extra(self, block: base.PLBlock) -> None:
        from .. import trampoline

        super().generate_extra(block)
        if getattr(self.object, 'single_role_metadata', None):
            value = json.dumps(self.object.single_role_metadata)
            query = base.Query(trampoline.fixup_query(
                f'''
                UPDATE edgedbinstdata_VER.instdata
                SET json = {ql(value)}::jsonb
                WHERE key = 'single_role_metadata'
                '''
            ))
            block.add_command(query.code_with_block(block))


class DropRole(ddl.SchemaObjectOperation):

    def code(self) -> str:
        return f'DROP ROLE {qi(self.name)}'


class AlterRoleAddMember(ddl.SchemaObjectOperation):

    def __init__(
        self,
        name: RoleName,
        member: str,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions
        )
        self.member = member

    def code(self) -> str:
        return f'GRANT {qi(self.name)} TO {qi(self.member)}'


class AlterRoleDropMember(ddl.SchemaObjectOperation):

    def __init__(
        self,
        name: RoleName,
        member: str,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions
        )
        self.member = member

    def code(self) -> str:
        return f'REVOKE {qi(self.name)} FROM {qi(self.member)}'


class AlterRoleAddMembership(ddl.SchemaObjectOperation):

    def __init__(
        self,
        name: RoleName,
        membership: Iterable[str],
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ):
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions
        )
        self.membership = membership

    def code(self) -> str:
        roles = ', '.join(qi(m) for m in self.membership)
        return f'GRANT {roles} TO {qi(self.name)}'
