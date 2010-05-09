##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys

import py
from semantix.utils import shell


class TestCommand(shell.Command, name='test', expose=True):
    def get_parser(self, subparsers):
        parser = super().get_parser(subparsers)

        parser.add_argument('--keep-going', action='store_true', default=False)
        parser.add_argument('--color', action='store_true', default=True)
        parser.add_argument('--no-color', dest='color', action='store_false')
        parser.add_argument('--no-magic', dest='magic', action='store_false', default=True)
        parser.add_argument('--skip', dest='skipped', action='append')
        parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)
        parser.add_argument('tests', nargs='*')

        return parser

    def __call__(self, args):
        test_args = []

        test_args.extend(('-p', 'semantix', '-s'))

        if args.debug:
            test_args.extend('--semantix-debug=%s' % d for d in args.debug)
            test_args.append('--capture=no')

        if args.tests:
            test_args.extend('--tests=%s' % t for t in args.tests)
            test_args.extend(('-k', 'testmask'))

        if args.skipped:
            test_args.extend('--skip-tests=%s' % i for i in args.skipped)

        if args.color:
            test_args.append('--colorize')

        if not args.magic:
            test_args.append('--nomagic')

        if not args.keep_going:
            test_args.append('-x')

        if args.verbose:
            test_args.append('-v')

        path = os.path.dirname(os.path.abspath(__file__))

        # This ugliness is required due to py.test braindead plugin lookup: there is
        # no way to specify a plugin with full package path, only a name _suffix_
        sys.path.insert(0, path)
        py.test.cmdline.main(test_args)
        sys.path.remove(path)
