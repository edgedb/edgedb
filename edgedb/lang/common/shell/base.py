##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class CommandMeta(type):
    exposed = set()

    def __new__(cls, name, bases, dct, expose=False):
        return super(CommandMeta, cls).__new__(cls, name, bases, dct)

    def __init__(cls, name, bases, dct, expose=False):
        super(CommandMeta, cls).__init__(name, bases, dct)
        if expose:
            CommandMeta.exposed.add(cls)


class CommandGroup(metaclass=CommandMeta):
    def __init__(self, subparsers):
        parser = self.get_parser(subparsers)
        self.submap = {}

        for cmdcls in self:
            cmd = cmdcls(parser)
            self.submap[cmd.name] = cmd

    def get_parser(self, subparsers):
        parser = subparsers.add_parser(self.__class__.name)
        return parser.add_subparsers(dest=self.__class__.name + '_subcommand')

    def __call__(self, args):
        self.submap[getattr(args, self.name + '_subcommand')](args)

    def __iter__(self):
        for cmd in self.commands:
            yield cmd


class Command(metaclass=CommandMeta):
    def __init__(self, subparsers):
        self.get_parser(subparsers)

    def get_parser(self, subparsers):
        parser = subparsers.add_parser(self.__class__.name)
        return parser

    def __call__(self, args):
        pass
