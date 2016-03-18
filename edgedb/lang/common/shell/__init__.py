##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import os
import sys

from metamagic.exceptions import MetamagicError
from metamagic import bootstrap

from metamagic.utils import datastructures
from metamagic.utils import debug, config

from . import reqs


class CommandMeta(config.ConfigurableMeta):
    main_command = None

    def __new__(cls, clsname, bases, dct, *, name, commands=None, expose=False, requires=None):
        return super().__new__(cls, clsname, bases, dct)

    def __init__(cls, clsname, bases, dct, *, name, commands=None, expose=False, requires=None):
        super().__init__(clsname, bases, dct)

        if name == '__main__':
            if CommandMeta.main_command:
                if issubclass(cls, CommandMeta.main_command):
                    CommandMeta.main_command = cls
                elif not issubclass(CommandMeta.main_command, cls):
                    raise MetamagicError('Main command is already defined by {} which {} does not' \
                                        ' subclass from'.format(CommandMeta.main_command, cls))
            else:
                CommandMeta.main_command = cls

        cls._command_name = name
        if hasattr(cls, '_commands'):
            cls._commands = cls._commands.copy()
            if commands:
                cls._commands.update(commands)
        else:
            cls._commands = datastructures.OrderedIndex(commands, key=lambda i: i._command_name)
        cls._command_requirements = requires

        if expose and not issubclass(cls, CommandMeta.main_command):
            CommandMeta.main_command._commands.add(cls)


class CommandBase(metaclass=CommandMeta, name=None):
    def check_requirements(self, args):
        if self.__class__._command_requirements:
            try:
                for requirement in self.__class__._command_requirements:
                    requirement(args)
            except reqs.UnsatisfiedRequirementError as e:
                err = '{}: {}'.format(self.__class__._command_name, e)
                raise reqs.UnsatisfiedRequirementError(err) from None

    def __init__(self, subparsers):
        self.parser = self.get_parser(subparsers)
        if self.__doc__:
            self.parser.description = self.__doc__

    def create_parser(self, subparsers, **kwargs):
        return subparsers.add_parser(self.__class__._command_name)

    def get_parser(self, subparsers, **kwargs):
        return self.create_parser(subparsers, **kwargs)


class CommandGroup(CommandBase, name=None):
    def __init__(self, subparsers):
        super().__init__(subparsers)

        self._command_submap = {}
        for cmdcls in self:
            cmd = cmdcls(self.parser)
            self._command_submap[cmd._command_name] = cmd

    def get_parser(self, subparsers):
        parser = self.create_parser(subparsers)
        action = parser.add_subparsers(dest=self.__class__._command_name + '_subcommand',
                                       title='{} subcommands'.format(self.__class__._command_name))
        action.required = True
        return action

    def __call__(self, args, unknown_args):
        self.check_requirements(args)
        command = getattr(args, self.__class__._command_name + '_subcommand')
        next_command = self._command_submap[command]

        if isinstance(next_command, Command):
            if unknown_args:
                next_command.handle_unknown_args(unknown_args)
            return next_command(args)

        return next_command(args, unknown_args)

    def __iter__(self):
        for cmd in self.__class__._commands:
            yield cmd


class Command(CommandBase, name=None):
    def __call__(self, args):
        raise NotImplementedError

    def handle_unknown_args(self, args):
        self.parser.error('unrecognized arguments: {}'.format(' '.join(args)))


class MainCommand(CommandGroup, name='__main__'):
    colorize = config.cvalue('auto',
                             type=str,
                             validator=lambda value: value in ('auto', 'on', 'off'),
                             doc='default commands color output setting value ' \
                                 '[auto, on, off]')

    def create_parser(self, parser):
        bootstrap.init_early_args(parser)
        return parser

    def __call__(self, args, unknown_args):
        if args.debug:
            debug.channels.update(args.debug)

        with config.inline({'metamagic.utils.shell.MainCommand.colorize': args.color}):
            result = super().__call__(args, unknown_args)

        return result

    @classmethod
    def main(cls, argv):
        parser = argparse.ArgumentParser(prog=os.path.basename(argv[0]))

        cmd = cls(parser)
        args, unknown_args = parser.parse_known_args(argv[1:])

        return cmd(args, unknown_args)
