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

import json
import textwrap
from typing import *

from ..common import qname as qn
from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base
from . import ddl
from . import tables


class TriggerExists(base.Condition):
    def __init__(self, trigger_name, table_name):
        self.trigger_name = trigger_name
        self.table_name = table_name

    def code(self, block: base.PLBlock) -> str:
        return textwrap.dedent(f'''\
            SELECT
                tg.tgname
            FROM
                pg_catalog.pg_trigger tg
                INNER JOIN pg_catalog.pg_class tab
                    ON (tab.oid = tg.tgrelid)
                INNER JOIN pg_catalog.pg_namespace ns
                    ON (ns.oid = tab.relnamespace)
            WHERE
                tab.relname = {ql(self.table_name[1])}
                AND ns.nspname = {ql(self.table_name[0])}
                AND tg.tgname = {ql(self.trigger_name)}
        ''')


class Trigger(tables.InheritableTableObject):
    def __init__(
            self, name, *, table_name, events, timing='after',
            granularity='row', procedure, condition=None, is_constraint=False,
            deferred=False, inherit=False, metadata=None):
        super().__init__(inherit=inherit, metadata=metadata)

        self.name = name
        self.table_name = table_name
        self.events = events
        self.timing = timing
        self.granularity = granularity
        self.procedure = procedure
        self.condition = condition
        self.is_constraint = is_constraint
        self.deferred = deferred

        if is_constraint and granularity != 'row':
            msg = 'invalid granularity for ' \
                  'constraint trigger: {}'.format(granularity)
            raise ValueError(msg)

        if deferred and not is_constraint:
            raise ValueError('only constraint triggers can be deferred')

    def rename(self, new_name):
        self.name = new_name

    def get_type(self):
        return 'TRIGGER'

    def get_id(self):
        return f'{qi(self.name)} ON {qn(*self.table_name)}'

    def get_oid(self):
        qry = textwrap.dedent(f'''\
            SELECT
                'pg_trigger'::regclass::oid AS classoid,
                pg_trigger.oid AS objectoid,
                0
            FROM
                pg_trigger
                INNER JOIN pg_class ON tgrelid = pg_class.oid
                INNER JOIN pg_namespace ON relnamespace = pg_namespace.oid
            WHERE
                tgname = {ql(self.name)}
                AND nspname = {ql(self.table_name[0])}
                AND relname = {ql(self.table_name[1])}
        ''')

        return base.Query(text=qry)

    def declare_pl_desc_var(self, block: base.PLBlock) -> str:
        desc_var = block.declare_var(('edgedb', 'intro_trigger_desc_t'))
        events = ', '.join(ql(e) for e in self.events)

        code = textwrap.dedent(f'''\
            {desc_var}.table_name := ARRAY[
                {ql(self.table_name[0])}, {ql(self.table_name[1])}];
            {desc_var}.name := {ql(self.name)};
            {desc_var}.proc := ARRAY[
                {ql(self.procedure[0])}, {ql(self.procedure[1])}];
            {desc_var}.is_constraint := {self.is_constraint};
            {desc_var}.deferred := {self.deferred};
            {desc_var}.granularity := {ql(self.granularity)};
            {desc_var}.timing := {ql(self.timing)};
            {desc_var}.events := ARRAY[{events}]::text[];
            {desc_var}.condition := {ql(self.condition) if self.condition
                                     else 'NULL'};
            {desc_var}.metadata := {ql(json.dumps(self.metadata))};
        ''')

        block.add_command(code)

        return desc_var

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
            is_constraint=self.is_constraint, deferred=self.deferred,
            metadata=self.metadata.copy())

    def __repr__(self):
        return \
            '<{mod}.{cls} {name} ON {table_name} {timing} {events}>'.format(
                mod=self.__class__.__module__,
                cls=self.__class__.__name__,
                name=self.name,
                table_name=qn(*self.table_name),
                timing=self.timing,
                events=' OR '.join(self.events))


class CreateTrigger(tables.CreateInheritableTableObject):
    def __init__(self, object, *, conditional=False, **kwargs):
        super().__init__(object, **kwargs)
        self.trigger = object
        if conditional:
            self.neg_conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name))

    def code(self, block: base.PLBlock) -> str:
        return textwrap.dedent('''\
            CREATE {constr}TRIGGER {trigger_name} {timing} {events}
                   ON {table_name}
                   {deferred}
                   FOR EACH {granularity} {condition}
                   EXECUTE PROCEDURE {procedure}
        ''').format(
            constr='CONSTRAINT ' if self.trigger.is_constraint else '',
            trigger_name=qi(self.trigger.name),
            timing=self.trigger.timing,
            events=' OR '.join(self.trigger.events),
            table_name=qn(*self.trigger.table_name),
            deferred=('DEFERRABLE INITIALLY DEFERRED'
                      if self.trigger.deferred else ''),
            granularity=self.trigger.granularity, condition=(
                'WHEN ({})'.format(self.trigger.condition)
                if self.trigger.condition else ''),
            procedure='{}()'.format(qn(*self.trigger.procedure)))

    @classmethod
    def pl_code(cls, desc_var: str, block: base.PLBlock) -> str:
        constr = (
            f"(CASE WHEN {desc_var}.is_constraint IS NOT NULL"
            f" THEN 'CONSTRAINT ' ELSE '' END)"
        )
        table_name = (
            f"(quote_ident({desc_var}.table_name[1])"
            f" || '.' || quote_ident({desc_var}.table_name[2]))"
        )
        events = (
            f"(SELECT string_agg(upper(e), ' OR ')"
            f" FROM unnest({desc_var}.events) AS e)"
        )

        cond_var = block.declare_var('text', 'cond')

        condition_code = textwrap.dedent(f"""\
            {cond_var} := COALESCE(
                {desc_var}.condition,
                edgedb._parse_trigger_condition({desc_var}.definition)
            );
        """)

        deferrability = (
            f"(CASE WHEN {desc_var}.deferred"
            f" THEN ' DEFERRABLE INITIALLY DEFERRED' ELSE '' END)"
        )

        condition = (
            f"(CASE WHEN {cond_var} IS NOT NULL "
            f"THEN 'WHEN (' || {cond_var} || ')' "
            f"ELSE '' END)"
        )

        procedure = (
            f"(quote_ident({desc_var}.proc[1])"
            f" || '.' || quote_ident({desc_var}.proc[2]) || '()')"
        )

        return condition_code + textwrap.dedent(f'''\
            EXECUTE
                'CREATE ' || {constr}
                || 'TRIGGER ' || quote_ident({desc_var}.name)
                || ' ' || upper({desc_var}.timing) || ' '
                || {events}
                || ' ON ' || {table_name}
                || {deferrability}
                || ' FOR EACH ' || upper({desc_var}.granularity) || ' '
                || {condition}
                || ' EXECUTE PROCEDURE ' || {procedure}
                ;
            EXECUTE
                'COMMENT ON TRIGGER ' || quote_ident({desc_var}.name)
                || ' ON ' || {table_name} || ' IS '
                || quote_literal({desc_var}.metadata::text)
                ;
        ''')


class AlterTriggerRenameTo(tables.RenameInheritableTableObject):
    def __init__(self, object, *, conditional=False, **kwargs):
        super().__init__(object, **kwargs)
        self.trigger = object
        if conditional:
            self.conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name))

    def code(self, block: base.PLBlock) -> str:
        return (f'ALTER TRIGGER {qi(self.trigger.name)} '
                f'ON {qn(*self.trigger.table_name)} '
                f'RENAME TO {qi(self.new_name)}')

    def pl_code(self, desc_var: str, block: base.PLBlock) -> str:
        table_name = (
            f"(quote_ident({desc_var}.table_name[1])"
            f" || '.' || quote_ident({desc_var}.table_name[2]))"
        )

        return textwrap.dedent(f'''\
            EXECUTE
                'ALTER TRIGGER ' || quote_ident({desc_var}.name)
                || ' ON ' || {table_name}
                || ' RENAME TO {qi(self.new_name)}';
        ''')


class DropTrigger(tables.DropInheritableTableObject):
    def __init__(self, object, *, conditional=False, **kwargs):
        super().__init__(object, **kwargs)
        self.trigger = object
        if conditional:
            self.conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name))

    def code(self, block: base.PLBlock) -> str:
        return (f'DROP TRIGGER {qi(self.trigger.name)} '
                f'ON {qn(*self.trigger.table_name)}')

    @classmethod
    def pl_code(cls, desc_var: str, block: base.PLBlock) -> str:
        table_name = (
            f"(quote_ident({desc_var}.table_name[1])"
            f" || '.' || quote_ident({desc_var}.table_name[2]))"
        )

        return textwrap.dedent(f'''\
            EXECUTE
                'DROP TRIGGER ' || quote_ident({desc_var}.name)
                || 'ON ' || {table_name}
                ;
        ''')


class DisableTrigger(ddl.DDLOperation):
    def __init__(self, trigger, *, self_only=False, **kwargs):
        super().__init__(**kwargs)
        self.trigger = trigger
        self.self_only = self_only

    def code(self, block: base.PLBlock) -> str:
        only = ' ONLY' if self.self_only else ''
        return (f'ALTER TABLE{only} {qn(*self.trigger.table_name)} '
                f'DISABLE TRIGGER {qi(self.trigger.name)}')

    def __repr__(self):
        return '<{mod}.{cls} {trigger!r}>'.format(
            mod=self.__class__.__module__,
            cls=self.__class__.__name__,
            trigger=self.trigger)


class DDLTriggerBase:
    @classmethod
    def get_inherited_triggers(
        cls,
        block: base.PLBlock,
        bases: List[Tuple[str, str]]
    ) -> Tuple[str, str]:
        bases = [ql('{}.{}'.format(*base.name)) for base in bases]
        var = block.declare_var(
            ('edgedb', 'intro_trigger_desc_t'), 't', shared=True)

        return var, textwrap.dedent(f'''\
            SELECT DISTINCT ON (triggers.name)
                triggers.*
            FROM
                edgedb.introspect_triggers(
                    table_list := ARRAY[{', '.join(bases)}]::text[],
                    inheritable_only := TRUE,
                    include_inherited := TRUE
                ) AS triggers
        ''')


class DDLTriggerCreateTable(
        ddl.DDLTrigger, tables.CreateTableDDLTriggerMixin, DDLTriggerBase):
    operations = tables.CreateTable,

    @classmethod
    def generate_after(cls, block, op):
        # Apply inherited triggers
        return cls.apply_inheritance(
            block, op, cls.get_inherited_triggers, CreateTrigger,
            comment='Trigger inheritance propagation on type table creation.')


class DDLTriggerAlterTable(
        ddl.DDLTrigger, tables.AlterTableDDLTriggerMixin, DDLTriggerBase):
    operations = (tables.AlterTableAddParent, tables.AlterTableDropParent)

    @classmethod
    def generate_after(cls, block, op):
        return cls.apply_inheritance(
            block, op, cls.get_inherited_triggers, CreateTrigger, DropTrigger,
            comment='Trigger inheritance propagation on type table alter.')
