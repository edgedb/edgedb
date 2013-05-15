##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import os
import imp
import inspect

from metamagic.utils import shell


class RunCommand(shell.Command, name='run', expose=True):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app_kwargs = None

    def get_parser(self, subparsers, **kwargs):
        parser = super().get_parser(subparsers, description='Run python script in metamagic context.')

        parser.add_argument('--callable', dest='callable', default='main',
                            help='name of function/callable to execute, default to "main(*args)"')
        parser.add_argument('file', help=('path to a python script to be executed'))
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

        kwargs = {}
        if self.app_kwargs:
            kwargs_parser = self._build_callable_args_subparser(callable)
            kwargs = kwargs_parser.parse_args(self.app_kwargs)
            kwargs = dict(kwargs._get_kwargs())

        return callable(*args.args, **kwargs)

    def handle_unknown_args(self, args):
        self.app_kwargs = args

    def _build_callable_args_subparser(self, callable):
        sig = inspect.signature(callable)

        parser = argparse.ArgumentParser(usage='{!r} callable accepts following arguments: {}'.\
                                               format(callable.__name__, sig))

        kwonly = [param for param in sig.parameters.values() if param.kind == param.KEYWORD_ONLY]

        for param in kwonly:
            kwargs = {}
            name = '--{}'.format(param.name)
            type_ = None
            default = None

            type_ = param.annotation
            if type_ is not param.empty:
                kwargs['type'] = type_

            default = param.default
            if default is not param.empty:
                kwargs['default'] = default

            if type_ is bool and default is False:
                kwargs['action'] = 'store_true'
                kwargs.pop('type')

            parser.add_argument(name, **kwargs)

        return parser
