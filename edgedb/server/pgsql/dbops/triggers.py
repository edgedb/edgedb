##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import json

from .. import common
from ..datasources import introspection

from . import base
from . import ddl
from . import tables


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


class Trigger(base.DBObject):
    def __init__(self, name, *, table_name, events, timing='after',
                       granularity='row', procedure, condition=None,
                       is_constraint=False, inherit=False):
        self.name = name
        self.table_name = table_name
        self.events = events
        self.timing = timing
        self.granularity = granularity
        self.procedure = procedure
        self.condition = condition
        self.is_constraint = is_constraint
        self.inherit = inherit

        if is_constraint and granularity != 'row':
            msg = 'invalid granularity for constraint trigger: {}'\
                      .format(granularity)
            raise ValueError(msg)

    def get_type(self):
        return 'TRIGGER'

    def get_id(self):
        return '{} ON {}'.format(common.quote_ident(self.name),
                                 common.qname(*self.table_name))

    def get_oid(self):
        qry = '''
            SELECT
                'pg_trigger'::regclass::oid,
                pg_trigger.oid,
                0
            FROM
                pg_trigger
                INNER JOIN pg_class ON tgrelid = pg_class.oid
                INNER JOIN pg_namespace ON relnamespace = pg_namespace.oid
            WHERE
                tgname = $1
                AND nspname = $2
                AND relname = $3
        '''
        params = (self.name,) + self.table_name

        return base.Query(text=qry, params=params)

    @classmethod
    def from_introspection(cls, table_name, trigger_data):
        (id, name, proc, constraint, granularity, timing,
         events, definition, metadata) = trigger_data

        if metadata:
            metadata = json.loads(metadata.decode('utf-8'))

        condition = None
        if definition:
            # A rather dumb bracket parser.  It'll choke
            # on any expression containing quoted or escaped
            # brackets.  The alternative is to fire up a
            # full SQL parser, which'll be utterly slow.
            #
            when_off = definition.find('WHEN (')
            if when_off != -1:
                pos = when_off + 6
                brackets = 1
                while brackets:
                    if definition[pos] == ')':
                        brackets -= 1
                    elif definition[pos] == '(':
                        brackets += 1
                    pos += 1

                condition = definition[when_off+6:pos-1]

        trg = cls(name=name, table_name=table_name,
                  events=events, timing=timing,
                  granularity=granularity, procedure=proc,
                  condition=condition,
                  is_constraint=bool(constraint),
                  inherit=True)

        return trg

    def copy(self):
        return self.__class__(
            name=self.name,
            table_name=self.table_name,
            events=self.events,
            timing=self.timing,
            granularity=self.granularity,
            procedure=self.procedure,
            condition=self.condition,
            is_constraint=self.is_constraint,
            inherit=self.inherit
        )

    def __repr__(self):
        return '<{mod}.{cls} {name} ON {table_name} {timing} {events}>'\
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        name=self.name,
                        table_name=common.qname(*self.table_name),
                        timing=self.timing,
                        events=' OR '.join(self.events))


class CreateTrigger(ddl.DDLOperation):
    def __init__(self, trigger, **kwargs):
        super().__init__(**kwargs)

        self.trigger = trigger

    def code(self, context):
        return '''
            CREATE {constr}TRIGGER {trigger_name} {timing} {events}
                   ON {table_name}
                   FOR EACH {granularity} {condition}
                   EXECUTE PROCEDURE {procedure}
        '''.format(
            constr='CONSTRAINT ' if self.trigger.is_constraint else '',
            trigger_name=common.quote_ident(self.trigger.name),
            timing=self.trigger.timing,
            events=' OR '.join(self.trigger.events),
            table_name=common.qname(*self.trigger.table_name),
            granularity=self.trigger.granularity,
            condition=('WHEN ({})'.format(self.trigger.condition)
                       if self.trigger.condition else ''),
            procedure='{}()'.format(common.qname(*self.trigger.procedure))
        )

    def extra(self, context):
        if self.trigger.inherit:
            ops = []

            # Propagade trigger to all current descendants.
            # Future descendats will receive the trigger via
            # the DDL trigger below.
            #
            ds = introspection.tables.TableDescendants(context.db)
            descendants = ds.fetch(schema_name=self.trigger.table_name[0],
                                   table_name=self.trigger.table_name[1],
                                   max_depth=1)

            for dschema, dname, *_ in descendants:
                trigger = self.trigger.copy()
                trigger.table_name = (dschema, dname)
                cr_trg = self.__class__(trigger)
                ops.append(cr_trg)

            mdata = ddl.UpdateMetadata(self.trigger, {'ddl:inherit': True})
            ops.append(mdata)

            return ops

    def __repr__(self):
        return '<{mod}.{cls} {trigger!r}>' \
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        trigger=self.trigger)


class AlterTriggerRenameTo(ddl.DDLOperation):
    def __init__(self, trigger, *, new_trigger_name, **kwargs):
        super().__init__(**kwargs)

        self.trigger = trigger
        self.new_trigger_name = new_trigger_name

    def code(self, context):
        return 'ALTER TRIGGER %s ON %s RENAME TO %s' % \
                (common.quote_ident(self.trigger.name),
                 common.qname(*self.trigger.table_name),
                 common.quote_ident(self.new_trigger_name))

    def extra(self, context):
        if self.trigger.inherit:
            ops = []

            # Propagade trigger rename to all current descendants.
            #
            ds = introspection.tables.TableDescendants(context.db)
            descendants = ds.fetch(schema_name=self.trigger.table_name[0],
                                   table_name=self.trigger.table_name[1],
                                   max_depth=1)

            for dschema, dname, *_ in descendants:
                trigger = self.trigger.copy()
                trigger.table_name = (dschema, dname)
                rn_trg = self.__class__(trigger,
                                        new_trigger_name=self.new_trigger_name)
                ops.append(rn_trg)

            return ops

    def __repr__(self):
        return '<{mod}.{cls} {trigger!r} TO {new_name}>' \
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        trigger=self.trigger,
                        new_name=self.new_trigger_name)


class DropTrigger(ddl.DDLOperation):
    def __init__(self, trigger, **kwargs):
        super().__init__(**kwargs)
        self.trigger = trigger

    def code(self, context):
        return 'DROP TRIGGER %(trigger_name)s ON %(table_name)s' % \
                {'trigger_name': common.quote_ident(self.trigger.name),
                 'table_name': common.qname(*self.trigger.table_name)}

    def extra(self, context):
        if self.trigger.inherit:
            ops = []

            # Propagade trigger drop to all current descendants.
            #
            ds = introspection.tables.TableDescendants(context.db)
            descendants = ds.fetch(schema_name=self.trigger.table_name[0],
                                   table_name=self.trigger.table_name[1],
                                   max_depth=1)

            for dschema, dname, *_ in descendants:
                trigger = self.trigger.copy()
                trigger.table_name = (dschema, dname)
                rn_trg = self.__class__(trigger)
                ops.append(rn_trg)

            return ops

    def __repr__(self):
        return '<{mod}.{cls} {trigger!r}>' \
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        trigger=self.trigger)


class DisableTrigger(ddl.DDLOperation):
    def __init__(self, trigger, *, self_only=False, **kwargs):
        super().__init__(**kwargs)
        self.trigger = trigger
        self.self_only = self_only

    def code(self, context):
        return 'ALTER TABLE{only} {table_name} DISABLE TRIGGER {trigger_name}'\
                .format(trigger_name=common.quote_ident(self.trigger.name),
                        table_name=common.qname(*self.trigger.table_name),
                        only=' ONLY' if self.self_only else '')

    def __repr__(self):
        return '<{mod}.{cls} {trigger!r}>' \
                .format(mod=self.__class__.__module__,
                        cls=self.__class__.__name__,
                        trigger=self.trigger)


class DDLTriggerBase:
    @classmethod
    def get_inherited_triggers(cls, db, table_name, bases):
        bases = ['{}.{}'.format(*base) for base in bases]

        tc = introspection.tables.TableTriggers(db)
        trig_records = tc.fetch(table_list=bases)

        triggers = []
        for row in trig_records:
            for r in row['triggers']:
                trg = Trigger.from_introspection(table_name, r)
                triggers.append(trg)

        return triggers


class DDLTriggerCreateTable(ddl.DDLTrigger, DDLTriggerBase):
    operations = tables.CreateTable,

    @classmethod
    def after(cls, context, op):
        # Apply inherited triggers

        triggers = cls.get_inherited_triggers(context.db, op.table.name,
                                              op.table.bases)
        if triggers:
            cmd = base.CommandGroup()
            cmd.add_commands([CreateTrigger(trg) for trg in triggers])
            return cmd


class DDLTriggerAlterTable(ddl.DDLTrigger, DDLTriggerBase):
    operations = tables.AlterTable,

    @classmethod
    def after(cls, context, op):
        dropped_parents = []
        added_parents = []
        ops = []

        for cmd in op.commands:
            if isinstance(cmd, (tuple, list)):
                cmd = cmd[0]

            if isinstance(cmd, tables.AlterTableAddParent):
                added_parents.append(cmd.parent_name)
            elif isinstance(cmd, tables.AlterTableDropParent):
                dropped_parents.append(cmd.parent_name)

        if dropped_parents:
            triggers_to_drop = cls.get_inherited_triggers(
                                context.db, op.name, dropped_parents)
        else:
            triggers_to_drop = []

        if added_parents:
            triggers_to_add = cls.get_inherited_triggers(
                                context.db, op.name, added_parents)
        else:
            triggers_to_add = []

        if triggers_to_drop:
            for trg in triggers_to_drop:
                trg = trg.copy()
                trg.table_name = op.name
                ops.append(DropTrigger(trg))

        if triggers_to_add:
            for trg in triggers_to_add:
                trg = trg.copy()
                trg.table_name = op.name
                ops.append(CreateTrigger(trg))

        if ops:
            grp = base.CommandGroup()
            grp.add_commands(ops)
            return grp

        return None
