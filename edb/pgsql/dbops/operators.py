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
from ..common import quote_type as qt

from . import base
from . import ddl


class CreateOperatorAlias(ddl.SchemaObjectOperation):
    def __init__(
        self,
        *,
        name,
        args,
        base_operator,
        operator_args,
        negator=None,
        commutator=None,
        procedure=None,
        **kwargs,
    ):
        super().__init__(name=name, **kwargs)
        self.args = args
        self.operator = base_operator
        self.operator_args = operator_args
        self.procedure = procedure
        self.commutator = commutator
        self.negator = negator

    def code_with_block(self, block: base.PLBlock) -> str:
        oper_var = block.declare_var(('pg_catalog', 'pg_operator%ROWTYPE'))
        oper_cond = []

        oper_name = f'{qi(self.name[0])}.{self.name[1]}'

        if self.args[0] is not None:
            left_type_desc = qt(self.args[0])
            left_type = f"', LEFTARG = {left_type_desc}'"
            oper_cond.append(
                f'o.oprleft = {ql(qt(self.operator_args[0]))}::regtype')
        else:
            left_type_desc = 'NONE'
            left_type = "''"
            oper_cond.append(f'o.oprleft = 0')

        if self.args[1] is not None:
            right_type_desc = qt(self.args[1])
            right_type = f"', RIGHTARG = {right_type_desc}'"
            oper_cond.append(
                f'o.oprright = {ql(qt(self.operator_args[1]))}::regtype')
        else:
            right_type_desc = 'NONE'
            right_type = "''"
            oper_cond.append(f'o.oprright = 0')

        oper_desc = (
            f'{qi(self.operator[0])}.{self.operator[1]} ('
            f'{left_type_desc}, {right_type_desc})'
        ).strip()

        if self.commutator:
            commutator_name = f'{qi(self.commutator[0])}.{self.commutator[1]}'
            commutator_decl = textwrap.indent(textwrap.dedent(f'''\
                ', COMMUTATOR = OPERATOR({commutator_name})'
            '''), ' ' * 8).strip()
            commutator_cond = 'TRUE'
        else:
            commutator_decl = textwrap.indent(textwrap.dedent(f'''\
                ', COMMUTATOR = ' || (
                    SELECT edgedb.raise(
                        NULL::text,
                        'invalid_object_definition',
                        msg => (
                            'missing required commutator for operator '
                            || {ql(oper_name)}
                        )
                    )
                )
            '''), ' ' * 8).strip()
            commutator_cond = 'FALSE'

        if self.negator:
            negator_name = f'{qi(self.negator[0])}.{self.negator[1]}'
            negator_decl = textwrap.indent(textwrap.dedent(f'''\
                ', NEGATOR = OPERATOR({negator_name})'
            '''), ' ' * 8).strip()
            negator_cond = 'TRUE'
        else:
            negator_decl = textwrap.indent(textwrap.dedent(f'''\
                ', NEGATOR = ' || (
                    SELECT edgedb.raise(
                        NULL::text,
                        'invalid_object_definition',
                        msg => (
                            'missing required negator for operator '
                            || {ql(oper_name)}
                        )
                    )
                )
            '''), ' ' * 8).strip()
            negator_cond = 'FALSE'

        def _get_op_field(field, oid):
            return textwrap.indent(textwrap.dedent(f'''\
                (CASE WHEN {oid} != 0 THEN
                 ', {field} = ' || (
                    SELECT
                        'OPERATOR('
                        || quote_ident(nspname) || '.' || oprname
                        || ')'
                    FROM
                        pg_operator o
                        INNER JOIN pg_namespace ns
                            ON (o.oprnamespace = ns.oid)
                    WHERE o.oid = {oid}
                 )
                 ELSE ''
                 END)
            '''), ' ' * 8).strip()

        code = textwrap.dedent('''\
            SELECT
                o.*
            INTO
                {oper}
            FROM
                pg_operator o
                INNER JOIN pg_namespace ns
                    ON (o.oprnamespace = ns.oid)
            WHERE
                o.oprname = {oper_name}
                AND ns.nspname = {oper_schema}
                AND {oper_cond}
            ;

            IF NOT FOUND THEN
                RAISE
                    'SQL operator does not exist: %',
                    {oper_desc}
                    USING ERRCODE = 'undefined_function';
            END IF;

            EXECUTE
                'CREATE OPERATOR {name} ('
                || 'PROCEDURE = ' || {procedure}
                || {left_type}
                || {right_type}
                || {commutator}
                || {negator}
                || {restrict}
                || {join}
                || {hashes}
                || {merges}
                || ')'
                ;
        ''').format_map({
            'name': oper_name,
            'oper': oper_var,
            'procedure': (ql(self.procedure) if self.procedure
                          else f'{oper_var}.oprcode::text'),
            'oper_schema': ql(self.operator[0]),
            'oper_name': ql(self.operator[1]),
            'oper_cond': ' AND '.join(oper_cond),
            'oper_desc': ql(oper_desc),
            'left_type': left_type,
            'right_type': right_type,
            'commutator': (
                f"(CASE WHEN {oper_var}.oprcom != 0 OR {commutator_cond} THEN "
                f"{commutator_decl} "
                f"ELSE '' END)"
            ),
            'negator': (
                f"(CASE WHEN {oper_var}.oprnegate != 0 OR {negator_cond} THEN "
                f"{negator_decl} "
                f"ELSE '' END)"
            ),
            'restrict': (
                f"(CASE WHEN {oper_var}.oprrest != 0 THEN "
                f"', RESTRICT = ' || {oper_var}.oprrest::text "
                f"ELSE '' END)"
            ),
            'join': (
                f"(CASE WHEN {oper_var}.oprjoin != 0 THEN "
                f"', JOIN = ' || {oper_var}.oprjoin::text "
                f"ELSE '' END)"
            ),
            'hashes': (
                f"(CASE WHEN {oper_var}.oprcanhash THEN "
                f"', HASHES ' "
                f"ELSE '' END)"
            ),
            'merges': (
                f"(CASE WHEN {oper_var}.oprcanmerge THEN "
                f"', MERGES ' "
                f"ELSE '' END)"
            ),
        })
        return code.strip()


class CreateOperator(ddl.SchemaObjectOperation):
    def __init__(self, *, name, args, procedure, **kwargs):
        super().__init__(name=name, **kwargs)
        self.args = args
        self.procedure = procedure

    def code(self) -> str:
        if self.args[0] is not None:
            left_type_desc = qt(self.args[0])
            left_type = f", LEFTARG = {left_type_desc}"
        else:
            left_type = ""

        if self.args[1] is not None:
            right_type_desc = qt(self.args[1])
            right_type = f", RIGHTARG = {right_type_desc}"
        else:
            right_type = ""

        return textwrap.dedent(f'''\
            CREATE OPERATOR {qi(self.name[0])}.{self.name[1]} (
                PROCEDURE = {self.procedure}
                {left_type}
                {right_type}
            );
        ''')


class DropOperator(ddl.SchemaObjectOperation):
    def __init__(self, *, name, args, **kwargs):
        super().__init__(name=name, **kwargs)
        self.args = args

    def code(self) -> str:
        if self.args[0] is not None:
            left_type = qt(self.args[0])
        else:
            left_type = 'NONE'

        if self.args[1] is not None:
            right_type = qt(self.args[1])
        else:
            right_type = 'NONE'

        return textwrap.dedent(f'''\
            DROP OPERATOR {qi(self.name[0])}.{self.name[1]} (
                {left_type}, {right_type}
            );
        ''')
