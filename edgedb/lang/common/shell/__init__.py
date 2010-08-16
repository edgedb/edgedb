##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import os
import sys

from semantix import SemantixError

from semantix.utils import datastructures
from semantix.utils import debug, config
from semantix.utils.io import terminal

from . import reqs


class CommandMeta(type):
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
                    raise SemantixError(('Main command is already defined by %s which %s does not'
                                         ' subclass from') % (CommandMeta.main_command, cls))
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
    def check_requirements(self):
        if self.__class__._command_requirements:
            try:
                for requirement in self.__class__._command_requirements:
                    requirement()
            except reqs.UnsatisfiedRequirementError as e:
                err = '%s: %s' % (self.__class__._command_name, e)
                raise reqs.UnsatisfiedRequirementError(err) from e


class CommandGroup(CommandBase, name=None):
    def __init__(self, subparsers):
        parser = self.get_parser(subparsers)
        self._command_submap = {}

        for cmdcls in self:
            cmd = cmdcls(parser)
            self._command_submap[cmd._command_name] = cmd

    def get_parser(self, subparsers):
        parser = self.create_parser(subparsers)
        return parser.add_subparsers(dest=self.__class__._command_name + '_subcommand')

    def create_parser(self, subparsers):
        return subparsers.add_parser(self.__class__._command_name)

    def __call__(self, args):
        self.check_requirements()
        self._command_submap[getattr(args, self.__class__._command_name + '_subcommand')](args)

    def __iter__(self):
        for cmd in self.__class__._commands:
            yield cmd


class Command(CommandBase, name=None):
    def __init__(self, subparsers):
        self.get_parser(subparsers)

    def get_parser(self, subparsers, **kwargs):
        parser = subparsers.add_parser(self.__class__._command_name, **kwargs)
        return parser

    def __call__(self, args):
        raise NotImplementedError


@config.configurable
class MainCommand(CommandGroup, name='__main__'):
    colorize = config.cvalue('auto',
                             type=str,
                             validator=lambda value: value in ('auto', 'always', 'never'),
                             doc='default commands color output setting value [auto, always, never]')

    def create_parser(self, parser):
        parser.add_argument('--color', choices=('auto', 'always', 'never'), default=self.colorize)
        parser.add_argument('-d', '--debug', dest='debug', action='append')
        parser.add_argument('--profile', dest='profile')
        return parser

    def __call__(self, args):
        config.set_value('semantix.utils.shell.MainCommand.colorize', args.color)
        color = None if args.color == 'auto' else args.color == 'always'
        term = terminal.Terminal(sys.stdout.fileno(), colors=color)
        args.color = term.has_colors()

        if args.debug:
            debug.channels.update(args.debug)

        try:
            if args.profile:
                import cProfile
                ep = super().__call__
                cProfile.runctx('ep(args)', globals=globals(), locals=locals(), filename=args.profile)
            else:
                super().__call__(args)
        except SemantixError as e:
            raise

        return 0

    @classmethod
    def main(cls, argv):
        parser = argparse.ArgumentParser(prog=os.path.basename(argv[0]))

        cmd = cls(parser)
        args = parser.parse_args(argv[1:])
        return cmd(args)
