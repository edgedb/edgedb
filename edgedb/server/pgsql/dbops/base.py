##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import markup
from semantix.utils.debug import debug


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
            """LOG [caos.delta.execute] Sync command code:
            print(code, vars)
            """

            """LINE [caos.delta.execute] EXECUTING
            repr(self)
            """
            result = context.db.prepare(code)(*vars)
            extra = self.extra(context)
            if extra:
                for cmd in extra:
                    cmd.execute(context)
            return result

    @classmethod
    def as_markup(cls, self, *, ctx):
        return markup.serialize(str(self))

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

    @debug
    def execute_code(self, context, code, vars):
        """LOG [caos.delta.execute] Sync command code:
        print(code, vars)
        """

        """LINE [caos.delta.execute] EXECUTING
        repr(self)
        """

        if vars is not None:
            result = context.db.prepare(code)(*vars)
        else:
            result = context.db.execute(code)

        """LINE [caos.delta.execute] EXECUTION RESULT
        repr(result)
        """

        extra = self.extra(context)
        if extra:
            for cmd in extra:
                cmd.execute(context)

        return result

    @debug
    def check_conditions(self, context, conditions, positive):
        result = True
        if conditions:
            for condition in conditions:
                code, vars = condition.get_code_and_vars(context)

                """LOG [caos.delta.execute] Sync command condition:
                print(code, vars)
                """

                result = context.db.prepare(code)(*vars)

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

    def execute(self, context):
        result = None
        ok = self.check_conditions(context, self.conditions, True) and \
             self.check_conditions(context, self.neg_conditions, False)

        if ok:
            code, vars = self.get_code_and_vars(context)
            if code:
                result = self.execute_code(context, code, vars)
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
        extra = []
        for cmd in self.commands:
            if isinstance(cmd, tuple):
                cmd = cmd[0]
            cmd_extra = cmd.extra(context, self)
            if cmd_extra:
                extra.extend(cmd_extra)

        return extra


class Condition(BaseCommand):
    pass


class Query:
    def __init__(self, text, params, type):
        self.text = text
        self.params = params
        self.type = type


class DefaultMeta(type):
    def __bool__(cls):
        return False

    def __repr__(self):
        return '<DEFAULT>'

    __str__ = __repr__


class Default(metaclass=DefaultMeta):
    pass


class DBObject:
    pass
