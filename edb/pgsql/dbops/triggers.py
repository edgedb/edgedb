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
from typing import (
    Any,
    Mapping,
    Optional,
    TypeAlias,
)

from ...common import enum as s_enum
from ..common import qname as qn
from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base
from . import ddl
from . import tables


TriggerName: TypeAlias = str


class TriggerTiming(s_enum.StrEnum):
    Before = 'before'
    After = 'after'


class TriggerGranularity(s_enum.StrEnum):
    Row = 'row'


class TriggerExists(base.Condition):
    def __init__(
        self,
        trigger_name: TriggerName,
        table_name: tables.TableName
    ) -> None:
        self.trigger_name = trigger_name
        self.table_name = table_name

    def code(self) -> str:
        return textwrap.dedent(
            f'''\
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
        '''
        )


class Trigger(tables.InheritableTableObject):
    def __init__(
        self,
        name: TriggerName,
        *,
        table_name: tables.TableName,
        events: tuple[str, ...],
        timing: TriggerTiming = TriggerTiming.After,
        granularity: TriggerGranularity = TriggerGranularity.Row,
        procedure,
        condition=None,
        is_constraint: bool = False,
        deferred: bool = False,
        inherit: bool = False,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
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

        if is_constraint and granularity != TriggerGranularity.Row:
            msg = 'invalid granularity for ' 'constraint trigger: {}'.format(
                granularity
            )
            raise ValueError(msg)

        if deferred and not is_constraint:
            raise ValueError('only constraint triggers can be deferred')

    def get_type(self) -> str:
        return 'TRIGGER'

    def get_id(self) -> str:
        return f'{qi(self.name)} ON {qn(*self.table_name)}'

    def get_oid(self) -> base.Query:
        qry = textwrap.dedent(
            f'''\
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
        '''
        )

        return base.Query(text=qry)

    def copy(self) -> Trigger:
        return self.__class__(
            name=self.name,
            table_name=self.table_name,
            events=self.events,
            timing=self.timing,
            granularity=self.granularity,
            procedure=self.procedure,
            condition=self.condition,
            is_constraint=self.is_constraint,
            deferred=self.deferred,
            metadata=(
                self.metadata.copy() if self.metadata is not None else None
            ),
        )

    def __repr__(self) -> str:
        return '<{mod}.{cls} {name} ON {table_name} {timing} {events}>'.format(
            mod=self.__class__.__module__,
            cls=self.__class__.__name__,
            name=self.name,
            table_name=qn(*self.table_name),
            timing=self.timing,
            events=' OR '.join(self.events),
        )


class CreateTrigger(ddl.CreateObject):
    def __init__(
        self,
        object: Trigger,
        *,
        conditional: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(object, **kwargs)
        self.trigger = object
        if conditional:
            self.neg_conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name)
            )

    def code(self) -> str:
        return textwrap.dedent(
            '''\
            CREATE {constr}TRIGGER {trigger_name} {timing} {events}
                   ON {table_name}
                   {deferred}
                   FOR EACH {granularity} {condition}
                   EXECUTE PROCEDURE {procedure}
        '''
        ).format(
            constr='CONSTRAINT ' if self.trigger.is_constraint else '',
            trigger_name=qi(self.trigger.name),
            timing=self.trigger.timing,
            events=' OR '.join(self.trigger.events),
            table_name=qn(*self.trigger.table_name),
            deferred=(
                'DEFERRABLE INITIALLY DEFERRED'
                if self.trigger.deferred
                else ''
            ),
            granularity=self.trigger.granularity,
            condition=(
                f'WHEN ({self.trigger.condition})'
                if self.trigger.condition
                else ''
            ),
            procedure=f'{qn(*self.trigger.procedure)}()',
        )


class DropTrigger(ddl.DropObject):
    def __init__(
        self,
        object: Trigger,
        *,
        conditional: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(object, **kwargs)
        self.trigger = object
        self.conditional = conditional
        if conditional:
            self.conditions.add(
                TriggerExists(self.trigger.name, self.trigger.table_name)
            )

    def code(self) -> str:
        ifexists = ' IF EXISTS' if self.conditional else ''
        return (
            f'DROP TRIGGER{ifexists} {qi(self.trigger.name)} '
            f'ON {qn(*self.trigger.table_name)}'
        )


class DisableTrigger(ddl.DDLOperation):
    def __init__(
        self,
        trigger: Trigger,
        *,
        self_only: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.trigger = trigger
        self.self_only = self_only

    def code(self) -> str:
        only = ' ONLY' if self.self_only else ''
        return (
            f'ALTER TABLE{only} {qn(*self.trigger.table_name)} '
            f'DISABLE TRIGGER {qi(self.trigger.name)}'
        )

    def __repr__(self) -> str:
        return '<{mod}.{cls} {trigger!r}>'.format(
            mod=self.__class__.__module__,
            cls=self.__class__.__name__,
            trigger=self.trigger,
        )


class EnableTrigger(ddl.DDLOperation):
    def __init__(
        self,
        trigger: Trigger,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.trigger = trigger

    def code(self) -> str:
        return (
            f'ALTER TABLE {qn(*self.trigger.table_name)} '
            f'ENABLE TRIGGER {qi(self.trigger.name)}'
        )

    def __repr__(self) -> str:
        return '<{mod}.{cls} {trigger!r}>'.format(
            mod=self.__class__.__module__,
            cls=self.__class__.__name__,
            trigger=self.trigger,
        )
