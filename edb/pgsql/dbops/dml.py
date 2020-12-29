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

from ..common import qname as qn
from ..common import quote_ident as qi
from ..common import quote_type as qt

from . import base


class DMLOperation(base.Command):
    pass


class Insert(DMLOperation):
    def __init__(
            self, table, records, returning=None, *, conditions=None,
            neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.table = table
        self.records = records
        self.returning = returning

    def code(self, block: base.PLBlock) -> str:
        cols = [(c.name, c.type)
                for c in self.table.iter_columns(writable_only=True)]

        placeholders = []

        if isinstance(self.records, base.Query):
            values_expr = '({})'.format(self.records.text)
        else:
            for row in self.records:
                placeholder_row = []
                for col, coltype in cols:
                    val = getattr(row, col, None)
                    if val and isinstance(val, base.Query):
                        cast = f'::{qt(val.type)}' if val.type else ''
                        placeholder_row.append(f'({val.text.strip()}){cast}')
                    elif val is base.Default:
                        placeholder_row.append('DEFAULT')
                    else:
                        val = base.encode_value(val)
                        placeholder_row.append(f'{val}::{coltype}')

                values_row = textwrap.indent(
                    ',\n'.join(placeholder_row), '    ')
                placeholders.append(f'(\n{values_row}\n)')

            values_expr = 'VALUES\n' + ',\n'.join(placeholders)

        col_list = textwrap.indent(',\n'.join(qi(c[0]) for c in cols), '    ')
        cols_s = f'(\n{col_list}\n)' if cols else ''
        code = f'INSERT INTO {qn(*self.table.name)} {cols_s}\n{values_expr}'
        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return code

    def __repr__(self):
        if isinstance(self.records, base.Query):
            vals = self.records.text
        else:
            rows = (', '.join('{}={!r}'.format(c, v) for c, v in row.items())
                    for row in self.records)
            vals = ', '.join('({})'.format(r) for r in rows)
        return '<{} {} ({}) priority={}>'.format(
            self.__class__.__name__, self.table.name, vals, self.priority)


class Update(DMLOperation):
    def __init__(
            self, table, record, condition, returning=None, *,
            include_children=True, priority=0, table_alias=None):
        super().__init__(priority=priority)

        self.table = table
        self.table_alias = table_alias
        self.record = record
        self.fields = [f for f, v in record.items() if v is not base.Default]
        self.condition = condition
        self.returning = returning
        self.cols = {
            c.name: c.type
            for c in self.table.iter_columns(writable_only=True)
        }
        self.include_children = include_children

    def code(self, block: base.PLBlock) -> str:
        placeholders = []

        for f in self.fields:
            val = getattr(self.record, f)

            if val is base.Default:
                continue

            if isinstance(val, base.Query):
                expr = val.text
                if not expr.startswith('('):
                    expr = '({})'.format(expr)
            elif isinstance(val, base.Default):
                expr = 'DEFAULT'
            else:
                expr = f'{base.encode_value(val)}::{self.cols[f]}'

            placeholders.append(f'{qi(f)} = {expr}')

        if self.condition:
            cond = []
            for condval in self.condition:
                if len(condval) == 3:
                    field, op, value = condval
                else:
                    field, value = condval
                    op = '='

                field = qi(field)

                if value is None:
                    cond.append(f'{field} IS NULL')
                else:
                    if isinstance(value, base.Query):
                        expr = value.text
                        cond.append(f'{field} {op} {expr}')
                    else:
                        cond.append(f'{field} {op} {base.encode_value(value)}')

            where = 'WHERE ' + ' AND '.join(cond)
        else:
            where = ''

        tabname = qn(*self.table.name)
        if not self.include_children:
            tabname = 'ONLY {}'.format(tabname)

        if self.table_alias:
            tabname = f'{tabname} AS {qi(self.table_alias)}'

        code = 'UPDATE {} SET {} {}'.format(
            tabname, ', '.join(placeholders), where)

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return code

    def __repr__(self):
        expr = ','.join(
            '%s=%s' % (f, getattr(self.record, f)) for f in self.fields)
        where = ','.join('%s=%s' % (c[0], c[1])
                         for c in self.condition) if self.condition else ''
        return '<%s %s %s (%s) priority=%s>' % (
            self.__class__.__name__, self.table.name, expr, where,
            self.priority)


class Delete(DMLOperation):
    def __init__(self, table, condition, *, include_children=True, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.condition = condition
        self.include_children = include_children

    def code(self, block: base.PLBlock) -> str:
        where = ' AND '.join(f'{qi(c[0])} = {base.encode_value(c[1])}'
                             for c in self.condition)

        tabname = qn(*self.table.name)
        if not self.include_children:
            tabname = f'ONLY {tabname}'

        return f'DELETE FROM {tabname} WHERE {where}'

    def __repr__(self):
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition)
        return '<edb.sync.%s %s (%s)>' % (
            self.__class__.__name__, self.table.name, where)
