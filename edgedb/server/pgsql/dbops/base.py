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


import base64
import hashlib

from edgedb.lang.common import markup
from edgedb.lang.common import debug


def pack_name(name, prefix_length=0):
    """Pack a potentially long name into Postgres' 63 char limit."""
    name = str(name)
    if len(name) > 63 - prefix_length:
        hash = base64.b64encode(hashlib.md5(name.encode()).digest()).decode(
        ).rstrip('=')
        name = name[:prefix_length] + hash + ':' + name[-(
            63 - prefix_length - 1 - len(hash)):]
    return name


class BaseCommand(metaclass=markup.MarkupCapableMeta):
    async def get_code_and_vars(self, context):
        code = await self.code(context)
        assert code is not None
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = None

        return code, vars

    async def execute(self, context):
        code, vars = await self.get_code_and_vars(context)

        if code:
            extra = await self.extra(context)
            extra_before = extra_after = None

            if isinstance(extra, dict):
                extra_before = extra.get('before')
                extra_after = extra.get('after')
            else:
                extra_after = extra

            if extra_before:
                for cmd in extra_before:
                    await cmd.execute(context)

            if debug.flags.delta_execute:
                debug.header('Executing DDL')
                debug.print(repr(self))
                debug.print('CODE:', code)
                debug.print('VARS:', vars)

            stmt = await context.db.prepare(code)
            result = await stmt.fetch(*vars)

            if extra_after:
                for cmd in extra_after:
                    await cmd.execute(context)
            return result

    @classmethod
    def as_markup(cls, self, *, ctx):
        return markup.elements.lang.TreeNode(name=repr(self))

    def dump(self):
        return str(self)

    async def code(self, context):
        return ''

    async def extra(self, context, *args, **kwargs):
        return None


class Command(BaseCommand):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        self.opid = id(self)
        self.conditions = conditions or set()
        self.neg_conditions = neg_conditions or set()
        self.priority = priority

    async def execute(self, context):
        ok = (
            await self.check_conditions(context, self.conditions, True) and
            await self.check_conditions(context, self.neg_conditions, False)
        )

        result = None
        if ok:
            code, vars = await self.get_code_and_vars(context)
            result = await self.execute_code(context, code, vars)
        return result

    async def get_extra_commands(self, context):
        extra = await self.extra(context)
        extra_before = extra_after = None

        if isinstance(extra, dict):
            extra_before = extra.get('before')
            extra_after = extra.get('after')
        else:
            extra_after = extra

        return extra_before, extra_after

    async def execute_code(self, context, code, vars):
        extra_before, extra_after = await self.get_extra_commands(context)

        if extra_before:
            for cmd in extra_before:
                await cmd.execute(context)

        result = await self._execute(context, code, vars)

        if extra_after:
            for cmd in extra_after:
                await cmd.execute(context)

        return result

    async def _execute(self, context, code, vars):
        if debug.flags.delta_execute:
            debug.print(repr(self), '\n')
            debug.print('CODE:', code)
            debug.print('VARS:', vars)

        stmt = await context.db.prepare(code)

        if vars is None:
            vars = []

        result = await stmt.fetch(*vars)
        return result

    async def check_conditions(self, context, conditions, positive):
        result = True
        if conditions:
            for condition in conditions:
                result = await condition.execute(context)

                if bool(result) ^ positive:
                    result = False
                    break
            else:
                result = True

        return result


class CommandGroup(Command):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.commands = []

    def add_command(self, cmd):
        self.commands.append(cmd)

    def add_commands(self, cmds):
        self.commands.extend(cmds)

    async def _execute(self, context, code, vars):
        if code:
            result = await super()._execute(context, code, vars)
        else:
            result = await self.execute_commands(context)

        return result

    async def execute_commands(self, context):
        result = []

        for c in self.commands:
            result.append(await c.execute(context))

        return result

    def get_code(self, context):
        return None

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.commands:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node

    def __iter__(self):
        return iter(self.commands)

    def __call__(self, typ):
        return filter(lambda i: isinstance(i, typ), self.commands)

    def __len__(self):
        return len(self.commands)


class CompositeCommandGroup(CommandGroup):
    async def code(self, context):
        if self.commands:
            prefix_code = self.prefix_code(context)
            subcommands_code = await self.subcommands_code(context)

            if subcommands_code:
                return prefix_code + ' ' + subcommands_code
        return False

    def prefix_code(self):
        return ''

    async def subcommands_code(self, context):
        cmds = []
        for cmd in self.commands:
            if isinstance(cmd, tuple):
                cond = True
                if cmd[1]:
                    cond = cond and await \
                        self.check_conditions(context, cmd[1], True)
                if cmd[2]:
                    cond = cond and await \
                        self.check_conditions(context, cmd[2], False)
                if cond:
                    cmds.append(await cmd[0].code(context))
            else:
                cmds.append(await cmd.code(context))
        if cmds:
            return ', '.join(cmds)
        else:
            return False

    async def execute_commands(self, context):
        # Sub-commands are always executed as part of code()
        return None

    async def extra(self, context):
        extra = {}
        for cmd in self.commands:
            if isinstance(cmd, tuple):
                cmd = cmd[0]
            cmd_extra = await cmd.extra(context, self)
            if cmd_extra:
                if isinstance(cmd_extra, dict):
                    extra_before = cmd_extra.get('before')
                    extra_after = cmd_extra.get('after')
                else:
                    extra_before = []
                    extra_after = cmd_extra

                if extra_before:
                    try:
                        extra["before"].extend(extra_before)
                    except KeyError:
                        extra["before"] = extra_before

                if extra_after:
                    try:
                        extra["after"].extend(extra_after)
                    except KeyError:
                        extra["after"] = extra_after

        return extra


class Condition(BaseCommand):
    async def execute(self, context):
        code, vars = await self.get_code_and_vars(context)

        stmt = await context.db.prepare(code)
        return await stmt.fetch(*vars)


class Echo(Command):
    def __init__(
            self, msg, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self._msg = msg

    async def code(self, context):
        return None, ()

    async def execute_code(self, context, code, vars):
        print(self._msg)


class Query(Command):
    def __init__(self, text, params=(), type=None):
        super().__init__()
        self.text = text
        self.params = params
        self.type = type

    async def code(self, context):
        return self.text, self.params

    def __repr__(self):
        return '<Query {!r} {!r}>'.format(self.text, self.params)


class DefaultMeta(type):
    def __bool__(cls):
        return False

    def __repr__(self):
        return '<DEFAULT>'

    __str__ = __repr__


class Default(metaclass=DefaultMeta):
    pass


class DBObject:
    def __init__(self, *, metadata=None):
        self.metadata = metadata

    def add_metadata(self, key, value):
        if self.metadata is None:
            self.metadata = {}

        self.metadata[key] = value

    def get_metadata(self, key):
        if self.metadata is None:
            return None
        else:
            return self.metadata.get(key)


class InheritableDBObject(DBObject):
    def __init__(self, *, inherit=False, **kwargs):
        super().__init__(**kwargs)
        if inherit:
            self.add_metadata('ddl:inherit', inherit)

    @property
    def inherit(self):
        return self.get_metadata('ddl:inherit') or False
