##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql
import re

from .. import common
from . import base


class DMLOperation(base.Command):
    pass


class Insert(DMLOperation):
    def __init__(self, table, records, returning=None, *, conditions=None, neg_conditions=None,
                                                                           priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.table = table
        self.records = records
        self.returning = returning

    def code(self, context):
        cols = [(c.name, c.type) for c in self.table.columns(writable_only=True)]
        l = len(cols)

        vals = []
        placeholders = []
        i = 1
        for row in self.records:
            placeholder_row = []
            for col, coltype in cols:
                val = getattr(row, col, None)
                if val and isinstance(val, base.Query):
                    vals.extend(val.params)
                    qtext = re.sub(r'\$(\d+)', lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1), val.text)
                    placeholder_row.append('(%s)::%s' % (qtext, val.type))
                    i += len(val.params)
                elif val is base.Default:
                    placeholder_row.append('DEFAULT')
                else:
                    vals.append(val)
                    placeholder_row.append('$%d::%s' % (i, coltype))
                    i += 1
            placeholders.append('(%s)' % ','.join(placeholder_row))

        code = 'INSERT INTO %s (%s) VALUES %s' % \
                (common.qname(*self.table.name),
                 ','.join(common.quote_ident(c[0]) for c in cols),
                 ','.join(placeholders))

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        vals = (('(%s)' % ', '.join('%s=%r' % (col, v) for col, v in row.items())) for row in self.records)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, ', '.join(vals))


class Update(DMLOperation):
    def __init__(self, table, record, condition, returning=None, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.record = record
        self.fields = [f for f, v in record.items() if v is not base.Default]
        self.condition = condition
        self.returning = returning
        self.cols = {c.name: c.type for c in self.table.columns(writable_only=True)}


    def code(self, context):
        e = common.quote_ident

        placeholders = []
        vals = []

        i = 1
        for f in self.fields:
            val = getattr(self.record, f)

            if val is base.Default:
                continue

            if isinstance(val, base.Query):
                expr = re.sub(r'\$(\d+)', lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1), val.text)
                i += len(val.params)
                vals.extend(val.params)
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
                    cond.append('%s %s $%d' % (field, op, i))
                    vals.append(value)
                    i += 1

            where = 'WHERE ' + ' AND '.join(cond)
        else:
            where = ''

        code = 'UPDATE %s SET %s %s' % \
                (common.qname(*self.table.name), ', '.join(placeholders), where)

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        expr = ','.join('%s=%s' % (f, getattr(self.record, f)) for f in self.fields)
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition) if self.condition else ''
        return '<caos.sync.%s %s %s (%s)>' % (self.__class__.__name__, self.table.name, expr, where)


class Merge(Update):
    def code(self, context):
        code = super().code(context)
        if self.condition:
            cols = (common.quote_ident(c[0]) for c in self.condition)
            returning = ','.join(cols)
        else:
            returning = '*'

        code = (code[0] + ' RETURNING %s' % returning, code[1])
        return code

    def execute(self, context):
        result = super().execute(context)

        if not result:
            op = Insert(self.table, records=[self.record])
            result = op.execute(context)

        return result


class Delete(DMLOperation):
    def __init__(self, table, condition, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.condition = condition

    def code(self, context):
        e = common.quote_ident
        where = ' AND '.join('%s = $%d' % (e(c[0]), i + 1) for i, c in enumerate(self.condition))

        code = 'DELETE FROM %s WHERE %s' % (common.qname(*self.table.name), where)

        vals = [c[1] for c in self.condition]

        return (code, vals)

    def __repr__(self):
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, where)


class CopyFrom(DMLOperation):
    def __init__(self, table, producer, *, format='text', priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.producer = producer
        self.format = format

    def code(self, context):
        code = 'COPY %s FROM STDIN WITH (FORMAT "%s")' % (common.qname(*self.table.name), self.format)
        return code, ()

    def execute(self, context):
        code, vars = self.code(context)
        receive_stmt = context.db.prepare(code)
        receiver = postgresql.copyman.StatementReceiver(receive_stmt)

        cm = postgresql.copyman.CopyManager(self.producer, receiver)
        cm.run()
