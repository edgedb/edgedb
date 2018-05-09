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

    async def code(self, context):
        code = '''
            SELECT
                tg.tgname
            FROM
                pg_catalog.pg_trigger tg
                INNER JOIN pg_catalog.pg_class tab
                    ON (tab.oid = tg.tgrelid)
                INNER JOIN pg_catalog.pg_namespace ns
                    ON (ns.oid = tab.relnamespace)
            WHERE
                tab.relname = $3 AND ns.nspname = $2 AND tg.tgname = $1
        '''

        return code, (self.trigger_name, ) + self.table_name


class Trigger(tables.InheritableTableObject):
    def __init__(
            self, name, *, table_name, events, timing='after',
            granularity='row', procedure, condition=None, is_constraint=False,
            inherit=False, metadata=None):
        super().__init__(inherit=inherit, metadata=metadata)

        self.name = name
        self.table_name = table_name
        self.events = events
        self.timing = timing
        self.granularity = granularity
        self.procedure = procedure
        self.condition = condition
        self.is_constraint = is_constraint

        if is_constraint and granularity != 'row':
            msg = 'invalid granularity for ' \
                  'constraint trigger: {}'.format(granularity)
            raise ValueError(msg)

    def rename(self, new_name):
        self.name = new_name

    def get_type(self):
        return 'TRIGGER'

    def get_id(self):
        return '{} ON {}'.format(
            common.quote_ident(self.name), common.qname(*self.table_name))

    def get_oid(self):
        qry = '''
            SELECT
                'pg_trigger'::regclass::oid AS classoid,
                pg_trigger.oid AS objectoid,
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
        params = (self.name, ) + self.table_name

        return base.Query(text=qry, params=params)

    @classmethod
    def from_introspection(cls, table_name, trigger_data):
        (
            id, name, proc, constraint, granularity, timing, events,
            definition, metadata) = trigger_data

        if metadata:
            metadata = json.loads(metadata)
        else:
            metadata = {}

        condition = None
        if definition:
            # A rather dumb bracket parser.  It'll choke
            # on any expression containing quoted or escaped
            # brackets.  The alternative is to fire up a
            # full SQL parser, which will be utterly slow.
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

                condition = definition[when_off + 6:pos - 1]

        trg = cls(
            name=name, table_name=table_name, events=events, timing=timing,
            granularity=granularity, procedure=proc, condition=condition,
            is_constraint=bool(constraint), metadata=metadata)

        return trg

    def copy(self):
        return self.__class__(
            name=self.name, table_name=self.table_name, events=self.events,
            timing=self.timing, granularity=self.granularity,
            procedure=self.procedure, condition=self.condition,
            is_constraint=self.is_constraint, metadata=self.metadata.copy())

    def __repr__(self):
        return \
            '<{mod}.{cls} {name} ON {table_name} {timing} {events}>'.format(
                mod=self.__class__.__module__,
                cls=self.__class__.__name__,
                name=self.name,
                table_name=common.qname(*self.table_name),
                timing=self.timing,
                events=' OR '.join(self.events))


class CreateTrigger(tables.CreateInheritableTableObject):
    def __init__(self, object, *, conditional=False, **kwargs):
        super().__init__(object, **kwargs)
        self.trigger = object
        if conditional:
            self.neg_conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name))

    async def code(self, context):
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
            granularity=self.trigger.granularity, condition=(
                'WHEN ({})'.format(self.trigger.condition)
                if self.trigger.condition else ''),
            procedure='{}()'.format(common.qname(*self.trigger.procedure)))


class AlterTriggerRenameTo(tables.RenameInheritableTableObject):
    def __init__(self, object, *, conditional=False, **kwargs):
        super().__init__(object, **kwargs)
        self.trigger = object
        if conditional:
            self.conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name))

    async def code(self, context):
        return 'ALTER TRIGGER {} ON {} RENAME TO {}'.format(
            common.quote_ident(self.trigger.name),
            common.qname(*self.trigger.table_name),
            common.quote_ident(self.new_name))


class DropTrigger(tables.DropInheritableTableObject):
    def __init__(self, object, *, conditional=False, **kwargs):
        super().__init__(object, **kwargs)
        self.trigger = object
        if conditional:
            self.conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name))

    async def code(self, context):
        return 'DROP TRIGGER {trigger_name} ON {table_name}'.format(
            trigger_name=common.quote_ident(self.trigger.name),
            table_name=common.qname(*self.trigger.table_name))


class DisableTrigger(ddl.DDLOperation):
    def __init__(self, trigger, *, self_only=False, **kwargs):
        super().__init__(**kwargs)
        self.trigger = trigger
        self.self_only = self_only

    async def code(self, context):
        return \
            'ALTER TABLE{only} {table_name} ' \
            'DISABLE TRIGGER {trigger_name}'.format(
                trigger_name=common.quote_ident(self.trigger.name),
                table_name=common.qname(*self.trigger.table_name),
                only=' ONLY' if self.self_only else '')

    def __repr__(self):
        return '<{mod}.{cls} {trigger!r}>'.format(
            mod=self.__class__.__module__,
            cls=self.__class__.__name__,
            trigger=self.trigger)


class DDLTriggerBase:
    @classmethod
    async def get_inherited_triggers(cls, db, table_name, bases):
        bases = ['{}.{}'.format(*base.name) for base in bases]

        trig_records = await introspection.tables.fetch_triggers(
            db, table_list=bases, inheritable_only=True)

        triggers = []
        for row in trig_records:
            for r in row['triggers']:
                trg = Trigger.from_introspection(table_name, r)
                trg.add_metadata('ddl:inherited', True)
                triggers.append(trg)

        return triggers


class DDLTriggerCreateTable(
        ddl.DDLTrigger, tables.CreateTableDDLTriggerMixin, DDLTriggerBase):
    operations = tables.CreateTable,

    @classmethod
    async def after(cls, context, op):
        # Apply inherited triggers
        return await cls.apply_inheritance(
            context, op, cls.get_inherited_triggers, CreateTrigger)


class DDLTriggerAlterTable(
        ddl.DDLTrigger, tables.AlterTableDDLTriggerMixin, DDLTriggerBase):
    operations = tables.AlterTable,

    @classmethod
    async def after(cls, context, op):
        return await cls.apply_inheritance(
            context, op, cls.get_inherited_triggers, CreateTrigger,
            DropTrigger)
