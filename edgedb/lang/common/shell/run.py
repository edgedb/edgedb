##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import contextlib
import os
import imp
from semantix.utils.functional import contextlib as sx_contextlib
from semantix.utils import shell, helper


class RunCommand(shell.Command, name='run', expose=True):
    def get_parser(self, subparsers, **kwargs):
        parser = super().get_parser(subparsers, description='Run python script in semantix context.')

        parser.add_argument('-C', '--config', dest='configs', action='append',
                            help="configuration objects to load in specified order")
        parser.add_argument('--callable', dest='callable', default='main',
                            help='name of function/callable to execute, default to "main(*args)"')
        parser.add_argument('file', help=('path to a python script to be executed'))
        parser.add_argument('--with-debug-logger', dest='debug_logger',
                            action='store_true', default=False,
                            help='installs a debug logger that dumps semantix errors to stdout')
        parser.add_argument('args', nargs='*', help='script arguments')

        return parser

    def __call__(self, args):
        path = os.path.abspath(args.file)

        if not os.path.exists(path):
            raise ValueError('path not found: %r' % path)

        mod_name = os.path.splitext(os.path.split(path)[-1])[0]
        mod = imp.load_source(mod_name, path)

        try:
            callable = getattr(mod, args.callable)
        except AttributeError:
            raise ValueError('unable to run file %r: has no callable %r defined' % \
                             (path, args.callable))

        if args.debug_logger:
            import logging
            from semantix.utils.test.pytest_semantix import LoggingPrintHandler
            logging.getLogger("semantix").addHandler(LoggingPrintHandler(args.color))
            logging.getLogger("semantix").setLevel(logging.INFO)

        if args.configs:
            contexts = []
            for config_name in args.configs:
                contexts.append(helper.get_object(config_name))
        else:
            @contextlib.contextmanager
            def dummy_context():
                yield
            contexts = [dummy_context()]

        with sx_contextlib.nested(*contexts):
            return callable(*args.args)
