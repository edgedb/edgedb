##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import os

from semantix.utils.shell.base import CommandMeta, Command, CommandGroup


def main(argv):
    parser = argparse.ArgumentParser(prog=os.path.basename(argv[0]))
    subparsers = parser.add_subparsers(dest='main_subcommand', title='subcommands')
    submap = {}

    for cmdgroup in CommandMeta.exposed:
        c = cmdgroup(subparsers)
        submap[c.name] = c

    args = parser.parse_args(argv[1:])
    submap[args.main_subcommand](args)

    return 0
