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

from edb.server import defines

from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base


class DDLTriggerMeta(type):
    _triggers = {}
    _trigger_cache = {}

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)
        if cls.operations:
            for op in cls.operations:
                try:
                    triggers = mcls._triggers[op]
                except KeyError:
                    triggers = mcls._triggers[op] = []

                triggers.append(cls)
                mcls._trigger_cache.clear()

        return cls

    @classmethod
    def get_triggers(mcls, opcls):
        try:
            triggers = mcls._trigger_cache[opcls]
        except KeyError:
            triggers = set()

            for cls in opcls.__mro__:
                try:
                    trg = mcls._triggers[cls]
                except KeyError:
                    pass
                else:
                    triggers.update(trg)

            mcls._trigger_cache[opcls] = triggers

        return triggers


class DDLTrigger(metaclass=DDLTriggerMeta):
    operations = None

    @classmethod
    def generate_before(cls, block, op):
        pass

    @classmethod
    def generate_after(cls, block, op):
        pass


class DDLOperation(base.Command):
    def generate(self, block: base.PLBlock) -> None:
        triggers = DDLTriggerMeta.get_triggers(self.__class__)

        if not block.disable_ddl_triggers:
            for trigger in triggers:
                trigger.generate_before(block, self)

        super().generate(block)

        if not block.disable_ddl_triggers:
            for trigger in triggers:
                trigger.generate_after(block, self)


class SchemaObjectOperation(DDLOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.name = name
        self.opid = name

    def __repr__(self):
        return '<edb.sync.%s %s>' % (self.__class__.__name__, self.name)


class Comment(DDLOperation):
    def __init__(self, object, text, **kwargs):
        super().__init__(**kwargs)

        self.object = object
        self.text = text

    def code(self, block: base.PLBlock) -> str:
        object_type = self.object.get_type()
        object_id = self.object.get_id()

        code = 'COMMENT ON {type} {id} IS {text}'.format(
            type=object_type, id=object_id,
            text=ql(self.text))

        return code


class ReassignOwned(DDLOperation):
    def __init__(self, old_role, new_role, **kwargs):
        super().__init__(**kwargs)
        self.old_role = old_role
        self.new_role = new_role

    def qi(self, ident: str) -> str:
        if ident.upper() in ('CURRENT_USER', 'SESSION_USER'):
            return ident
        else:
            return qi(ident)

    def code(self, block: base.PLBlock) -> str:
        return (
            f'REASSIGN OWNED BY {self.qi(self.old_role)} '
            f'TO {self.qi(self.new_role)}'
        )


class GetMetadata(base.Command):
    def __init__(self, object):
        super().__init__()
        self.object = object

    def code(self, block: base.PLBlock) -> str:
        oid = self.object.get_oid()
        is_shared = self.object.is_shared()
        if isinstance(oid, base.Query):
            qry = oid.text
            objoid = block.declare_var('oid')
            classoid = block.declare_var('oid')
            objsubid = block.declare_var('oid')
            block.add_command(
                qry + f' INTO {objoid}, {classoid}, {objsubid}')
        else:
            objoid, classoid, objsubid = oid

        prefix = f'E{ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)}'

        return textwrap.dedent(f'''\
            SELECT
                CASE WHEN substr(
                    description, 1, char_length({prefix})) = {prefix}
                THEN substr(description, char_length({prefix}) + 1)::jsonb
                ELSE '{{}}'::jsonb
                END
             FROM
                {'pg_shdescription' if is_shared else 'pg_description'}
             WHERE
                objoid = {objoid}
                AND classoid = {classoid}
                {f'AND objsubid = {objsubid}' if not is_shared else ''}
        ''')


class PutMetadata(DDLOperation):
    def __init__(self, object, metadata, **kwargs):
        super().__init__(**kwargs)
        self.object = object
        self.metadata = metadata

    def __repr__(self):
        return \
            '<{mod}.{cls} {object!r} {metadata!r}>'.format(
                mod=self.__class__.__module__,
                cls=self.__class__.__name__,
                object=self.object,
                metadata=self.metadata)


class SetMetadata(PutMetadata):
    def code(self, block: base.PLBlock) -> str:
        metadata = self.metadata

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
        desc = ql(json.dumps(metadata))
        comment = f'E{prefix} || {desc}'

        return textwrap.dedent(f'''\
            EXECUTE 'COMMENT ON {object_type} {object_id} IS ' ||
                quote_literal({comment});
        ''')


class UpdateMetadata(PutMetadata):
    def code(self, block: base.PLBlock) -> str:
        metadata_qry = GetMetadata(self.object).code(block)
        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
        json_v = block.declare_var('jsonb')
        upd_v = block.declare_var('text')
        meta_v = block.declare_var('jsonb')
        block.add_command(f'{json_v} := ({metadata_qry});')
        upd_metadata = ql(json.dumps(self.metadata))
        block.add_command(f'{meta_v} := {upd_metadata}::jsonb')

        block.add_command(textwrap.dedent(f'''\
            IF {json_v} IS NOT NULL THEN
                {upd_v} := E{prefix} || ({json_v} || {upd_metadata})::text;
            ELSE
                {upd_v} := E{prefix} || {upd_metadata}::text;
            END IF;
        '''))

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        return textwrap.dedent(f'''\
            IF {upd_v} IS NOT NULL THEN
                EXECUTE 'COMMENT ON {object_type} {object_id} IS ' ||
                    quote_literal({upd_v});
            END IF;
        ''')


class CreateObject(SchemaObjectOperation):
    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = SetMetadata(self.object, self.object.metadata)
            block.add_command(mdata.code(block))


class RenameObject(SchemaObjectOperation):
    def __init__(self, object, *, new_name, **kwargs):
        super().__init__(name=object.name, **kwargs)
        self.object = object
        self.altered_object = object.copy()
        self.altered_object.rename(new_name)
        self.new_name = new_name

    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = UpdateMetadata(
                self.altered_object, self.altered_object.metadata)
            block.add_command(mdata.code(block))


class AlterObject(SchemaObjectOperation):
    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = SetMetadata(self.object, self.object.metadata)
            block.add_command(mdata.code(block))
