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
    Sequence,
    TypeAlias,
)

import textwrap

from ..common import qname as qn
from ..common import quote_literal as ql
from ..common import quote_type as qt

from . import base
from . import constraints
from . import ddl


DomainName: TypeAlias = tuple[str, ...]


class DomainExists(base.Condition):
    def __init__(self, name: DomainName):
        self.name = name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                domain_name
            FROM
                information_schema.domains
            WHERE
                domain_schema = {ql(self.name[0])}
                AND domain_name = {ql(self.name[1])}
        ''')


class Domain(base.DBObject):

    def __init__(
        self,
        name: DomainName,
        *,
        base: str | DomainName,
        constraints: Sequence[DomainConstraint] = (),
        metadata: Optional[Mapping[str, Any]] = None
    ):
        self.constraints = tuple(constraints)
        self.base = base
        self.name = name
        super().__init__(metadata=metadata)


class CreateDomain(ddl.SchemaObjectOperation):
    def __init__(
        self,
        domain: Domain,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(
            domain.name, conditions=conditions, neg_conditions=neg_conditions
        )
        self.domain = domain

    def code_with_block(self, block: base.PLBlock) -> str:
        extra: list[str] = []
        for constraint in self.domain.constraints:
            extra.append(constraint.constraint_code(block))

        return textwrap.dedent(f'''\
            CREATE DOMAIN {qn(*self.domain.name)}
            AS {qt(self.domain.base)}
            {' '.join(extra) if extra else ''}
        ''').strip()


class AlterDomain(ddl.DDLOperation):
    def __init__(
        self,
        name: DomainName,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.name = name

    def prefix_code(self) -> str:
        return f'ALTER DOMAIN {qn(*self.name)}'

    def __repr__(self) -> str:
        return '<edb.sync.%s %s>' % (self.__class__.__name__, self.name)


class AlterDomainAlterDefault(AlterDomain):
    def __init__(
        self,
        name: DomainName,
        default: Optional[str]
    ) -> None:
        super().__init__(name)
        self.default = default

    def code(self) -> str:
        code = self.prefix_code()
        if self.default is None:
            code += ' DROP DEFAULT '
        else:
            if self.default is not None:
                value = ql(str(self.default))
            else:
                value = 'None'
            code += f' SET DEFAULT {value}'
        return code


class AlterDomainAlterNull(AlterDomain):
    def __init__(self, name: DomainName, null: bool) -> None:
        super().__init__(name)
        self.null = null

    def code(self) -> str:
        code = self.prefix_code()
        if self.null:
            code += ' DROP NOT NULL '
        else:
            code += ' SET NOT NULL '
        return code


class AlterDomainAlterConstraint(AlterDomain):
    def __init__(
        self,
        name: DomainName,
        constraint: DomainConstraint,
        *,
        conditions: Optional[Iterable[str | base.Condition]] = None,
        neg_conditions: Optional[Iterable[str | base.Condition]] = None,
    ) -> None:
        super().__init__(
            name, conditions=conditions, neg_conditions=neg_conditions)
        self._constraint = constraint


class DomainConstraint(constraints.Constraint):
    def get_subject_type(self) -> str:
        return 'DOMAIN'

    def constraint_code(self, block: base.PLBlock) -> str:
        raise NotImplementedError()


class DomainCheckConstraint(DomainConstraint):

    def __init__(
        self,
        domain_name: DomainName,
        constraint_name: Optional[str] = None,
        *,
        expr: base.Query | str,
    ) -> None:
        super().__init__(domain_name, constraint_name=constraint_name)
        self.expr = expr

    def constraint_code(self, block: base.PLBlock) -> str:
        if isinstance(self.expr, base.Query):
            assert self.expr.type
            var = block.declare_var(self.expr.type)
            indent = len(var) + 5
            expr_text = textwrap.indent(self.expr.text, ' ' * indent).strip()
            block.add_command(f'{var} := ({expr_text})')

            code = f"'CHECK (' || {var} || ')'"
            code = base.PLExpression(code)

        else:
            code = f'CHECK ({self.expr})'

        return code


class AlterDomainAddConstraint(AlterDomainAlterConstraint):
    def code_with_block(self, block: base.PLBlock) -> str:
        code = self.prefix_code()
        constr_name = self._constraint.constraint_name()
        constr_code = self._constraint.constraint_code(block)
        if isinstance(constr_code, base.PLExpression):
            code = (f"EXECUTE {ql(code)} || ' ADD CONSTRAINT {constr_name} ' "
                    f"|| {constr_code}")
        else:
            code += f' ADD CONSTRAINT {constr_name} {constr_code}'
        return code

    def generate_extra(self, block: base.PLBlock) -> None:
        return self._constraint.generate_extra(block)


class AlterDomainDropConstraint(AlterDomainAlterConstraint):
    def code(self) -> str:
        code = super().prefix_code()
        code += f' DROP CONSTRAINT {self._constraint.constraint_name()}'
        return code


class DropDomain(ddl.SchemaObjectOperation):
    def code(self) -> str:
        return f'DROP DOMAIN {qn(*self.name)}'
