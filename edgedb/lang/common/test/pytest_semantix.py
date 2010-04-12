##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import py
import re
import logging

from semantix.utils.debug import highlight
from semantix.utils.io import terminal


setattr(logging, '_semantix_logging_running', True)

def _logging_on():
    setattr(logging, '_semantix_logging_running', True)
def _logging_off():
    setattr(logging, '_semantix_logging_running', False)

setattr(logging, '_logging_on', _logging_on)
setattr(logging, '_logging_off', _logging_off)


class LoggingPrintHandler(logging.Handler):
    def __init__(self, colorize, *args, **kwargs):
        self.colorize = colorize
        super().__init__(*args, **kwargs)

    def emit(self, record):
        if getattr(logging, '_semantix_logging_running'):
            if self.colorize:
                print(terminal.colorize('LOGGER', 'white', 'red'), record)
            else:
                print('LOGGER', record)


test_patterns = []
test_skipped_patterns = []

class PyTestColorizerPatcher:
    target = None
    old_method = None

    @classmethod
    def patch(cls):
        cls.target = __import__('py._code.code', None, None, ['FormattedExcinfo']).FormattedExcinfo
        cls.old_method = cls.target.get_source

        def get_source(self, *args, **kwargs):
            lst = cls.old_method(self, *args, **kwargs)
            return highlight('\n'.join(lst), 'python').split('\n')

        cls.target.get_source = get_source

    @classmethod
    def unpatch(cls):
        if cls.old_method and cls.target:
            cls.target.get_source = cls.old_method
            cls.old_method = None


def pytest_addoption(parser):
    parser.addoption("--semantix-debug", dest="semantix_debug", action="append")
    parser.addoption("--tests", dest="test_patterns", action="append")
    parser.addoption("--skip-tests", dest="test_skipped_patterns", action="append")

    group = parser.getgroup("terminal reporting")
    group._addoption('--colorize', default=False, action='store_true', dest='colorize')


def pytest_configure(config):
    global test_patterns, test_skipped_patterns, semantix_debug

    if config.option.colorize:
        PyTestColorizerPatcher.patch()
        logging.getLogger("semantix").addHandler(LoggingPrintHandler(True))
    else:
        logging.getLogger("semantix").addHandler(LoggingPrintHandler(False))

    patterns = []
    tp = config.getvalue('test_patterns')
    if tp:
        for t in tp:
            patterns.extend(t.split(","))
        test_patterns = [re.compile(p) for p in patterns]

    patterns = []
    tp = config.getvalue('test_skipped_patterns')
    if tp:
        for t in tp:
            patterns.extend(t.split(","))
        test_skipped_patterns = [re.compile(p) for p in patterns]

    sd = config.getvalue('semantix_debug')
    if sd:
        debug = []

        for d in sd:
            debug.extend(d.split(","))

        import semantix.utils.debug
        semantix.utils.debug.enabled = True
        semantix.utils.debug.channels.update(debug)


def pytest_unconfigure(config):
    if config.option.colorize:
        PyTestColorizerPatcher.unpatch()


def pytest_pycollect_makeitem(__multicall__, collector, name, obj):
    item = __multicall__.execute()
    result = item

    if isinstance(item, py.test.collect.Function):
        func = item.obj
        name = func.__name__

        if name.startswith('test_'):
            name = name[5:]

        if test_patterns:
            for p in test_patterns:
                if test_skipped_patterns:
                    for ip in test_skipped_patterns:
                        if ip.match(name):
                            return

                if p.match(name):
                    func = getattr(func, '__func__', func)
                    setattr(func, 'testmask', py.test.mark.Marker('testmask'))
                    break

        elif test_skipped_patterns:
            for p in test_skipped_patterns:
                if p.match(name):
                    return

    return result
