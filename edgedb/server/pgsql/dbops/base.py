##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import base64
import hashlib

from metamagic.utils import markup
from metamagic.utils.debug import debug


def pack_name(name, prefix_length=0):
    """Pack a potentially long name into Postgres' 63 char limit"""

    name = str(name)
    if len(name) > 63 - prefix_length:
        hash = base64.b64encode(hashlib.md5(name.encode()).digest()).decode().rstrip('=')
        name = name[:prefix_length] + hash + ':' + name[-(63 - prefix_length - 1 - len(hash)):]
    return name


@markup.serializer.serializer(method='as_markup')
class BaseCommand:
    def get_code_and_vars(self, context):
        code = self.code(context)
        assert code is not None
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = None

        return code, vars

    @debug
    def execute(self, context):
        code, vars = self.get_code_and_vars(context)

        if code:
            extra = self.extra(context)
            extra_before = extra_after = None

            if isinstance(extra, dict):
                extra_before = extra.get('before')
                extra_after = extra.get('after')
            else:
                extra_after = extra

            if extra_before:
                for cmd in extra_before:
                    cmd.execute(context)

            """LOG [caos.delta.execute] Sync command code:
            print(code, vars)
            """

            """LINE [caos.delta.execute] EXECUTING
            repr(self)
            """
            result = context.db.prepare(code)(*vars)

            if extra_after:
                for cmd in extra_after:
                    cmd.execute(context)
            return result

    @classmethod
    def as_markup(cls, self, *, ctx):
        return markup.elements.lang.TreeNode(name=repr(self))

    def dump(self):
        return str(self)

    def code(self, context):
        return ''

    def extra(self, context, *args, **kwargs):
        return None


class Command(BaseCommand):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        self.opid = id(self)
        self.conditions = conditions or set()
        self.neg_conditions = neg_conditions or set()
        self.priority = priority

    def execute(self, context):
        ok = self.check_conditions(context, self.conditions, True) and \
             self.check_conditions(context, self.neg_conditions, False)

        result = None
        if ok:
            code, vars = self.get_code_and_vars(context)
            result = self.execute_code(context, code, vars)
        return result

    def get_extra_commands(self, context):
        extra = self.extra(context)
        extra_before = extra_after = None

        if isinstance(extra, dict):
            extra_before = extra.get('before')
            extra_after = extra.get('after')
        else:
            extra_after = extra

        return extra_before, extra_after

    @debug
    def execute_code(self, context, code, vars):
        extra_before, extra_after = self.get_extra_commands(context)

        if extra_before:
            for cmd in extra_before:
                cmd.execute(context)

        """LOG [caos.delta.execute] Sync command code:
        print(code, vars)
        """

        """LINE [caos.delta.execute] EXECUTING
        repr(self)
        """

        result = self._execute(context, code, vars)

        """LINE [caos.delta.execute] EXECUTION RESULT
        repr(result)
        """

        if extra_after:
            for cmd in extra_after:
                cmd.execute(context)

        return result

    def _execute(self, context, code, vars):
        if vars is not None:
            result = context.db.prepare(code)(*vars)
        else:
            result = context.db.execute(code)

        return result

    @debug
    def check_conditions(self, context, conditions, positive):
        result = True
        if conditions:
            for condition in conditions:
                result = condition.execute(context)

                """LOG [caos.delta.execute] Sync command condition result:
                print('actual:', bool(result), 'expected:', positive)
                """

                if bool(result) ^ positive:
                    result = False
                    break
            else:
                result = True

        return result


class CommandGroup(Command):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.commands = []

    def add_command(self, cmd):
        self.commands.append(cmd)

    def add_commands(self, cmds):
        self.commands.extend(cmds)

    def _execute(self, context, code, vars):
        if code:
            result = super()._execute(context, code, vars)
        else:
            result = self.execute_commands(context)

        return result

    def execute_commands(self, context):
        return [c.execute(context) for c in self.commands]

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
    def code(self, context):
        if self.commands:
            prefix_code = self.prefix_code(context)
            subcommands_code = self.subcommands_code(context)

            if subcommands_code:
                return prefix_code + ' ' + subcommands_code
        return False

    def prefix_code(self):
        return ''

    def subcommands_code(self, context):
        cmds = []
        for cmd in self.commands:
            if isinstance(cmd, tuple):
                cond = True
                if cmd[1]:
                    cond = cond and self.check_conditions(context, cmd[1], True)
                if cmd[2]:
                    cond = cond and self.check_conditions(context, cmd[2], False)
                if cond:
                    cmds.append(cmd[0].code(context))
            else:
                cmds.append(cmd.code(context))
        if cmds:
            return ', '.join(cmds)
        else:
            return False

    def execute_commands(self, context):
        # Sub-commands are always executed as part of code()
        return None

    def extra(self, context):
        extra = {}
        for cmd in self.commands:
            if isinstance(cmd, tuple):
                cmd = cmd[0]
            cmd_extra = cmd.extra(context, self)
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
    def execute(self, context):
        code, vars = self.get_code_and_vars(context)

        """LOG [caos.delta.execute] Sync command condition:
        print(code, vars)
        """

        return context.db.prepare(code)(*vars)


class Echo(Command):
    def __init__(self, msg, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self._msg = msg

    def code(self, context):
        return None, ()

    def execute_code(self, context, code, vars):
        print(self._msg)


class Query(Command):
    def __init__(self, text, params=(), type=None):
        super().__init__()
        self.text = text
        self.params = params
        self.type = type

    def code(self, context):
        return self.text, self.params


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
