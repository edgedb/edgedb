##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import re

from .. import common
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

    async def code(self, context):
        cols = [(c.name, c.type)
                for c in self.table.iter_columns(writable_only=True)]

        vals = []
        placeholders = []
        i = 1

        if isinstance(self.records, base.Query):
            vals.extend(self.records.params)
            qtext = re.sub(
                r'\$(\d+)', lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1),
                self.records.text)
            values_expr = '({})'.format(qtext)
        else:
            for row in self.records:
                placeholder_row = []
                for col, coltype in cols:
                    val = getattr(row, col, None)
                    if val and isinstance(val, base.Query):
                        vals.extend(val.params)
                        qtext = re.sub(
                            r'\$(\d+)',
                            lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1),
                            val.text)
                        placeholder_row.append(
                            '(%s)%s' % (
                                qtext, '::{}'.format(val.type)
                                if val.type is not None else ''))
                        i += len(val.params)
                    elif val is base.Default:
                        placeholder_row.append('DEFAULT')
                    else:
                        vals.append(val)
                        placeholder_row.append('$%d::%s' % (i, coltype))
                        i += 1
                placeholders.append('(%s)' % ','.join(placeholder_row))

            values_expr = 'VALUES {}'.format(','.join(placeholders))

        qi = common.quote_ident
        code = 'INSERT INTO {} {} {}'.format(
            common.qname(*self.table.name),
            '(' + ','.join(qi(c[0]) for c in cols) + ')' if cols else '',
            values_expr)

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        if isinstance(self.records, base.Query):
            vals = self.records.text
        else:
            rows = (', '.join('{}={!r}'.format(c, v) for c, v in row.items())
                    for row in self.records)
            vals = ', '.join('({})'.format(r) for r in rows)
        return '<edgedb.sync.{} {} ({})>'.format(
            self.__class__.__name__, self.table.name, vals)


class Update(DMLOperation):
    def __init__(
            self, table, record, condition, returning=None, *,
            include_children=True, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.record = record
        self.fields = [f for f, v in record.items() if v is not base.Default]
        self.condition = condition
        self.returning = returning
        self.cols = {
            c.name: c.type
            for c in self.table.iter_columns(writable_only=True)
        }
        self.include_children = include_children

    async def code(self, context):
        e = common.quote_ident

        placeholders = []
        vals = []

        i = 1
        for f in self.fields:
            val = getattr(self.record, f)

            if val is base.Default:
                continue

            if isinstance(val, base.Query):
                expr = re.sub(
                    r'\$(\d+)',
                    lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1), val.text)
                if not expr.startswith('('):
                    expr = '({})'.format(expr)
                i += len(val.params)
                vals.extend(val.params)
            elif isinstance(val, base.Default):
                expr = 'DEFAULT'
            else:
                expr = '$%d::%s' % (i, self.cols[f])
                i += 1
                vals.append(val)

            placeholders.append('%s = %s' % (e(f), expr))

        if self.condition:
            cond = []
            for condval in self.condition:
                if len(condval) == 3:
                    field, op, value = condval
                else:
                    field, value = condval
                    op = '='

                field = e(field)

                if value is None:
                    cond.append('%s IS NULL' % field)
                else:
                    if isinstance(value, base.Query):
                        expr = re.sub(
                            r'\$(\d+)',
                            lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1),
                            value.text)
                        cond.append('{} {} {}'.format(field, op, expr))
                        i += len(value.params)
                        vals.extend(value.params)
                    else:
                        cond.append('%s %s $%d' % (field, op, i))
                        vals.append(value)
                        i += 1

            where = 'WHERE ' + ' AND '.join(cond)
        else:
            where = ''

        tabname = common.qname(*self.table.name)
        if not self.include_children:
            tabname = 'ONLY {}'.format(tabname)

        code = 'UPDATE {} SET {} {}'.format(
            tabname, ', '.join(placeholders), where)

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        expr = ','.join(
            '%s=%s' % (f, getattr(self.record, f)) for f in self.fields)
        where = ','.join('%s=%s' % (c[0], c[1])
                         for c in self.condition) if self.condition else ''
        return '<edgedb.sync.%s %s %s (%s)>' % (
            self.__class__.__name__, self.table.name, expr, where)


class Merge(Update):
    async def code(self, context):
        code = await super().code(context)

        if not self.returning:
            if self.condition:
                cols = (common.quote_ident(c[0]) for c in self.condition)
                returning = ','.join(cols)
            else:
                returning = '*'

            code = (code[0] + ' RETURNING %s' % returning, code[1])
        return code

    async def execute(self, context):
        result = await super().execute(context)

        if not result:
            op = Insert(self.table, records=[self.record])
            result = await op.execute(context)

        return result


class Delete(DMLOperation):
    def __init__(self, table, condition, *, include_children=True, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.condition = condition
        self.include_children = include_children

    async def code(self, context):
        e = common.quote_ident
        where = ' AND '.join(
            '%s = $%d' % (e(c[0]), i + 1)
            for i, c in enumerate(self.condition))

        tabname = common.qname(*self.table.name)
        if not self.include_children:
            tabname = 'ONLY {}'.format(tabname)
        code = 'DELETE FROM %s WHERE %s' % (tabname, where)

        vals = [c[1] for c in self.condition]

        return (code, vals)

    def __repr__(self):
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition)
        return '<edgedb.sync.%s %s (%s)>' % (
            self.__class__.__name__, self.table.name, where)
