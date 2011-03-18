##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys

import py
from semantix.utils import shell
from semantix.utils.test import Exceptions as ExceptionConfig


class TestCommand(shell.Command, name='test', expose=True):
    def get_parser(self, subparsers, **kwargs):
        parser = super().get_parser(subparsers, description='Collect and execute project tests.')

        parser.add_argument('-i', '--pdb', dest='pdb', action='store_true',
                            help='start PDB (python debugger) on error', default=False)
        parser.add_argument('-s', '--shell', dest='shell', action='store_true',
                            help='start the interactive python shell on error', default=False)
        parser.add_argument('--skip', dest='skipped', action='append', metavar='SKIP_PATTERN',
                            help='a pattern specifying tests to be excluded from the test run')
        parser.add_argument('--keep-going', action='store_true', default=False,
                            help='do not stop at the first failed test')
        parser.add_argument('--no-magic', dest='magic', action='store_false', default=True,
                            help='don\'t reinterpret asserts, no traceback cutting')
        parser.add_argument('--no-assert', dest='asserts', action='store_false', default=True,
                            help='disable python assert expression reinterpretation')
        parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False,
                            help='enable verbose output')
        parser.add_argument('--tb', dest='traceback_style', default=ExceptionConfig.traceback_style)
        parser.add_argument('tests', nargs='*',
                            help=('a pattern specifying tests to be included in the test run; '
                                  'if not specified, all tests will be run'))

        return parser

    def __call__(self, args):
        if tuple(py.__version__.split('.')) < ('1', '4'):
            print('error: sx test requires at least py-1.4.0 and pytest-2.0.0', file=sys.stderr)
            return 1

        test_args = []

        plugins = ['-p', 'semantix.utils.test.pytest_semantix']
        test_args.extend(plugins)

        test_args.append('-s')

        if args.shell:
            test_args.append('--shell')

        if args.pdb:
            test_args.append('--pdb')

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

        if not args.asserts:
            test_args.append('--no-assert')

        if not args.keep_going:
            test_args.append('-x')

        if args.verbose:
            test_args.append('-v')

        test_args.append('--traceback-style=%s' % args.traceback_style)

        return py.test.cmdline.main(test_args)
