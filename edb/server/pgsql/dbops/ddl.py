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
    async def before(cls, context, op):
        pass

    @classmethod
    async def after(cls, context, op):
        pass


class DDLOperation(base.Command):
    async def execute(self, context):
        triggers = DDLTriggerMeta.get_triggers(self.__class__)

        for trigger in triggers:
            cmd = await trigger.before(context, self)
            if cmd:
                await cmd.execute(context)

        result = await super().execute(context)

        for trigger in triggers:
            cmd = await trigger.after(context, self)
            if cmd:
                await cmd.execute(context)

        return result


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

    async def code(self, context):
        object_type = self.object.get_type()
        object_id = self.object.get_id()

        code = 'COMMENT ON {type} {id} IS {text}'.format(
            type=object_type, id=object_id,
            text=common.quote_literal(self.text))

        return code


class ReassignOwned(DDLOperation):
    def __init__(self, old_role, new_role, **kwargs):
        super().__init__(**kwargs)
        self.old_role = old_role
        self.new_role = new_role

    async def code(self, context):
        return (f'REASSIGN OWNED BY {common.quote_ident(self.old_role)} '
                f'TO {common.quote_ident(self.new_role)}')


class GetMetadata(base.Command):
    def __init__(self, object):
        super().__init__()
        self.object = object

    async def code(self, context):
        code = '''
            SELECT
                substr(description, 5)::json
             FROM
                pg_description
             WHERE
                objoid = $1 AND classoid = $2 AND objsubid = $3
                AND substr(description, 1, 4) = '$CMR'
        '''

        oid = self.object.get_oid()
        if isinstance(oid, base.Command):
            oid = (await oid.execute(context))[0]

        return code, oid

    async def _execute(self, context, code, vars):
        result = await super()._execute(context, code, vars)

        if result:
            result = result[0][0]
        else:
            result = None

        return result


class PutMetadata(DDLOperation):
    def __init__(self, object, metadata, **kwargs):
        super().__init__(**kwargs)
        self.object = object
        self.metadata = metadata

    async def _execute(self, context, code, vars):
        metadata = self.metadata
        desc = '$CMR{}'.format(json.dumps(metadata))

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        code = 'COMMENT ON {type} {id} IS {text}'.format(
            type=object_type, id=object_id,
            text=common.quote_literal(desc))

        result = await base.Query(code).execute(context)

        return result

    def __repr__(self):
        return \
            '<{mod}.{cls} {object!r} {metadata!r}>'.format(
                mod=self.__class__.__module__,
                cls=self.__class__.__name__,
                object=self.object,
                metadata=self.metadata)


class SetMetadata(PutMetadata):
    async def _execute(self, context, code, vars):
        metadata = self.metadata
        desc = '$CMR{}'.format(json.dumps(metadata))

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        code = 'COMMENT ON {type} {id} IS {text}'.format(
            type=object_type, id=object_id,
            text=common.quote_literal(desc))

        result = await base.Query(code).execute(context)

        return result


class UpdateMetadata(PutMetadata):
    async def _execute(self, context, code, vars):
        metadata = await GetMetadata(self.object).execute(context)

        if metadata is None:
            metadata = {}

        metadata.update(self.metadata)

        desc = '$CMR{}'.format(json.dumps(metadata))

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        code = 'COMMENT ON {type} {id} IS {text}'.format(
            type=object_type, id=object_id,
            text=common.quote_literal(desc))

        result = await base.Query(code).execute(context)

        return result


class CreateObject(SchemaObjectOperation):
    async def extra(self, context):
        ops = await super().extra(context)

        if self.object.metadata:
            if ops is None:
                ops = []

            mdata = SetMetadata(self.object, self.object.metadata)
            ops.append(mdata)

        return ops


class RenameObject(SchemaObjectOperation):
    def __init__(self, object, *, new_name, **kwargs):
        super().__init__(name=object.name, **kwargs)
        self.object = object
        self.altered_object = object.copy()
        self.altered_object.rename(new_name)
        self.new_name = new_name

    async def extra(self, context):
        ops = await super().extra(context)

        if self.altered_object.metadata:
            if ops is None:
                ops = []

            mdata = UpdateMetadata(
                self.altered_object, self.altered_object.metadata)
            ops.append(mdata)

        return ops


class AlterObject(SchemaObjectOperation):
    async def extra(self, context):
        ops = await super().extra(context)

        if self.object.metadata:
            if ops is None:
                ops = []

            mdata = UpdateMetadata(self.object, self.object.metadata)
            ops.append(mdata)

        return ops
