##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .. import common
from . import base
from . import ddl


class TriggerExists(base.Condition):
    def __init__(self, trigger_name, table_name):
        self.trigger_name = trigger_name
        self.table_name = table_name

    def code(self, context):
        code = '''SELECT
                        tg.tgname
                    FROM
                        pg_catalog.pg_trigger tg
                        INNER JOIN pg_catalog.pg_class tab ON (tab.oid = tg.tgrelid)
                        INNER JOIN pg_catalog.pg_namespace ns ON (ns.oid = tab.relnamespace)
                    WHERE
                        tab.relname = $3 AND ns.nspname = $2 AND tg.tgname = $1
                '''

        return code, (self.trigger_name,) + self.table_name


class CreateTrigger(ddl.DDLOperation):
    def __init__(self, trigger_name, *, table_name, events, timing='after', granularity='row',
                                                            procedure, condition=None, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.table_name = table_name
        self.events = events
        self.timing = timing
        self.granularity = granularity
        self.procedure = procedure
        self.condition = condition

    def code(self, context):
        return '''CREATE TRIGGER %(trigger_name)s %(timing)s %(events)s ON %(table_name)s
                  FOR EACH %(granularity)s %(condition)s EXECUTE PROCEDURE %(procedure)s
               ''' % {
                      'trigger_name': common.quote_ident(self.trigger_name),
                      'timing': self.timing,
                      'events': ' OR '.join(self.events),
                      'table_name': common.qname(*self.table_name),
                      'granularity': self.granularity,
                      'condition': ('WHEN (%s)' % self.condition) if self.condition else '',
                      'procedure': '%s()' % common.qname(*self.procedure)
                     }


class CreateConstraintTrigger(CreateTrigger):
    def __init__(self, trigger_name, *, table_name, events, procedure, condition=None,
                                        conditions=None, neg_conditions=None, priority=0):

        super().__init__(trigger_name=trigger_name, table_name=table_name, events=events,
                         procedure=procedure, condition=condition,
                         conditions=conditions, neg_conditions=neg_conditions, priority=priority)

    def code(self, context):
        return '''CREATE CONSTRAINT TRIGGER %(trigger_name)s %(timing)s %(events)s
                  ON %(table_name)s
                  FOR EACH %(granularity)s %(condition)s EXECUTE PROCEDURE %(procedure)s
               ''' % {
                      'trigger_name': common.quote_ident(self.trigger_name),
                      'timing': self.timing,
                      'events': ' OR '.join(self.events),
                      'table_name': common.qname(*self.table_name),
                      'granularity': self.granularity,
                      'condition': ('WHEN (%s)' % self.condition) if self.condition else '',
                      'procedure': '%s()' % common.qname(*self.procedure)
                     }


class AlterTriggerRenameTo(ddl.DDLOperation):
    def __init__(self, *, trigger_name, new_trigger_name, table_name, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.new_trigger_name = new_trigger_name
        self.table_name = table_name

    def code(self, context):
        return 'ALTER TRIGGER %s ON %s RENAME TO %s' % \
                (common.quote_ident(self.trigger_name), common.qname(*self.table_name),
                 common.quote_ident(self.new_trigger_name))


class DropTrigger(ddl.DDLOperation):
    def __init__(self, trigger_name, *, table_name, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.table_name = table_name

    def code(self, context):
        return 'DROP TRIGGER %(trigger_name)s ON %(table_name)s' % \
                {'trigger_name': common.quote_ident(self.trigger_name),
                 'table_name': common.qname(*self.table_name)}

