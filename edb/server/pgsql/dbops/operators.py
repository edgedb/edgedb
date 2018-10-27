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


import textwrap

from ..common import quote_ident as qi
from ..common import quote_literal as ql
from ..common import quote_type as qt

from . import base
from . import ddl


class CreateOperatorAlias(ddl.SchemaObjectOperation):
    def __init__(self, *, name, args, operator, **kwargs):
        super().__init__(name=name, **kwargs)
        self.args = args
        self.operator = operator

    def code(self, block: base.PLBlock) -> str:
        oper_var = block.declare_var(('pg_catalog', 'pg_operator%ROWTYPE'))
        oper_cond = []

        if self.args[0] is not None:
            left_type_desc = qt(self.args[0])
            left_type = f"', LEFTARG = {left_type_desc}'"
            oper_cond.append(f'o.oprleft = {ql(qt(self.args[0]))}::regtype')
        else:
            left_type = "''"
            left_type_desc = 'NONE'

        if self.args[1] is not None:
            right_type_desc = qt(self.args[1])
            right_type = f"', RIGHTARG ={right_type_desc}'"
            oper_cond.append(f'o.oprright = {ql(qt(self.args[1]))}::regtype')
        else:
            right_type = "''"
            right_type_desc = 'NONE'

        oper_desc = (
            f'{qi(self.operator[0])}.{self.operator[1]} ('
            f'{left_type_desc}, {right_type_desc})'
        ).strip()

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
                || 'PROCEDURE = ' || {oper}.oprcode::text
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
            'name': f'{qi(self.name[0])}.{self.name[1]}',
            'oper': oper_var,
            'oper_schema': ql(self.operator[0]),
            'oper_name': ql(self.operator[1]),
            'oper_cond': ' AND '.join(oper_cond),
            'oper_desc': ql(oper_desc),
            'left_type': left_type,
            'right_type': right_type,
            'commutator': _get_op_field('COMMUTATOR', oper_var + '.oprcom'),
            'negator': _get_op_field('NEGATOR', oper_var + '.oprnegate'),
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


class DropOperator(ddl.SchemaObjectOperation):
    def __init__(self, *, name, args, **kwargs):
        super().__init__(name=name, **kwargs)
        self.args = args

    def code(self, block: base.PLBlock) -> str:
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
