##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import py
import re
import sys
import logging
import contextlib

from semantix.exceptions import SemantixError, excepthook, _iter_contexts
from semantix.utils.debug import highlight
from semantix.utils.io import terminal


setattr(logging, '_semantix_logging_running', True)

def _logging_on():
    setattr(logging, '_semantix_logging_running', True)
def _logging_off():
    setattr(logging, '_semantix_logging_running', False)

setattr(logging, '_logging_on', _logging_on)
setattr(logging, '_logging_off', _logging_off)


@contextlib.contextmanager
def logging_off():
    logging._logging_off()
    yield
    logging._logging_on()


class ShellInvoker:
    def pytest_runtest_makereport(self, item, call):
        if call.excinfo:
            import code, traceback
            from semantix.utils import helper

            tb = call.excinfo._excinfo[2]
            while tb.tb_next:
                print(tb.tb_frame.f_locals)
                tb = tb.tb_next

            frame = tb.tb_frame

            dct = { '__frame__' : frame , '__excinfo__' : call.excinfo._excinfo }
            dct.update(frame.f_globals)
            dct.update(frame.f_locals)

            shell = code.InteractiveConsole(locals=dct)
            message = "Exception occurred: entering python shell.\n\nTraceback:\n"
            message += ''.join(traceback.format_stack(frame))

            try:
                code_ctx = helper.dump_code_context(frame.f_code.co_filename,
                                                    tb.tb_lineno, dump_range=6)

                message += '\n\nCode Context (%s:%d):\n' % (frame.f_code.co_filename, tb.tb_lineno)
                message += code_ctx

            except Exception:
                pass

            message += '\n\nException: %s' % call.excinfo._excinfo[1]

            message += '\n\nLocals: %.500r' % frame.f_locals
            message += '\n\nCurrent frame and exception are stored in the "__frame__" and ' \
                       '"__excinfo__" variables.'
            message += '\nGood luck.\n'

            try:
                import readline
            except ImportError:
                pass

            shell.interact(message)


class LoggingPrintHandler(logging.Handler):
    def __init__(self, colorize, *args, **kwargs):
        self.colorize = colorize
        super().__init__(*args, **kwargs)

    def emit(self, record):
        if getattr(logging, '_semantix_logging_running'):
            if self.colorize:
                level = record.levelname
                level_color = 'blue'
                if level == 'ERROR':
                    level_color = 'red'

                print(terminal.colorize(level, 'white', level_color), os.getpid(),
                      record.getMessage())
            else:
                print(record.levelname, os.getpid(), record.getMessage())

            if record.exc_info:
                excepthook(*record.exc_info)


test_patterns = []
test_skipped_patterns = []
traceback_style = 'long'


BaseReprExceptionInfo = __import__('py._code.code', None, None, ['ReprExceptionInfo']).ReprExceptionInfo

class ReprExceptionInfo(BaseReprExceptionInfo):
    def __init__(self, einfos):
        super().__init__(*einfos[-1][:2])
        self.einfos = einfos
        self.term = terminal.Terminal(fd=sys.stderr, colors=True)

    def toterminal(self, tw):
        for tb, crash, contexts in reversed(self.einfos):
            tb.toterminal(tw)

            tw.sep('~', 'Semantix Exception Contexts')
            if contexts:
                for context in contexts:
                    context.print(self.term)

        for name, content, sep in self.sections:
            tw.sep(sep, name)
            tw.line(content)


BaseExceptionInfo = __import__('py._code.code', None, None, ['ExceptionInfo']).ExceptionInfo

class ExceptionInfo(BaseExceptionInfo):
    def getrepr(self, showlocals=False, style='long', abspath=False, tbfilter=True, funcargs=False):
        global traceback_style
        style = traceback_style
        return super(showlocals=showlocals, style=style, abspath=abspath, tbfilter=tbfilter,
                     funcargs=funcargs)


class PyTestPatcher:
    target = None
    old_get_source = None
    old_repr_excinfo = None

    @classmethod
    def patch(cls):
        cls.target = __import__('py._code.code', None, None, ['FormattedExcinfo']).FormattedExcinfo
        cls.target2 = __import__('py._code.code', None, None, ['ExceptionInfo']).ExceptionInfo
        cls.old_get_source = cls.target.get_source
        cls.old_getrepr = cls.target2.getrepr

        def get_source(self, *args, **kwargs):
            lst = cls.old_get_source(self, *args, **kwargs)
            return highlight('\n'.join(lst), 'python').split('\n')

        cls.target.get_source = get_source

        cls.old_repr_excinfo = cls.target.repr_excinfo

        def repr_excinfo(self, excinfo):
            einfos = []

            einfo = ExceptionInfo(excinfo._excinfo)

            while einfo:
                contexts = _iter_contexts(einfo.value)
                einfos.append((self.repr_traceback(einfo), einfo._getreprcrash(), contexts))
                if einfo.value.__cause__:
                    cause = einfo.value.__cause__
                    einfo = ExceptionInfo((type(cause), cause, cause.__traceback__))
                else:
                    einfo = None

            return ReprExceptionInfo(einfos)

        def getrepr(self, showlocals=False,
                    style='long',
                    abspath=False, tbfilter=True, funcargs=False):
            global traceback_style
            style = traceback_style
            return cls.old_getrepr(self, showlocals=showlocals, style=style, abspath=abspath,
                                   tbfilter=tbfilter, funcargs=funcargs)

        cls.target.repr_excinfo = repr_excinfo
        cls.target2.getrepr = getrepr


    @classmethod
    def unpatch(cls):
        if cls.old_get_source and cls.target:
            cls.target.get_source = cls.old_get_source
            cls.target.repr_excinfo = cls.old_repr_excinfo
            cls.old_get_source = None
            cls.old_repr_excinfo = None
        if cls.old_getrepr and cls.target2:
            cls.target2.getrepr = cls.old_getrepr
            cls.old_getrepr = None


def pytest_addoption(parser):
    parser.addoption("--semantix-debug", dest="semantix_debug", action="append")
    parser.addoption("--tests", dest="test_patterns", action="append")
    parser.addoption("--skip-tests", dest="test_skipped_patterns", action="append")
    parser.addoption("--shell", dest="shell_on_ex", action="store_true", default=False)
    parser.addoption('--traceback-style', dest="traceback_style", default="extended")

    group = parser.getgroup("terminal reporting")
    group._addoption('--colorize', default=False, action='store_true', dest='colorize')


def pytest_configure(config):
    global test_patterns, test_skipped_patterns, semantix_debug, traceback_style

    if config.option.shell_on_ex:
        config.pluginmanager.register(ShellInvoker(), 'shell')

    if config.option.colorize:
        PyTestPatcher.patch()
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

    tbs = config.getvalue('traceback_style')
    if tbs:
        traceback_style = tbs

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
        PyTestPatcher.unpatch()


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
                    if hasattr(result, 'keywords'):
                        result.keywords['testmask'] = True
                    break

        elif test_skipped_patterns:
            for p in test_skipped_patterns:
                if p.match(name):
                    return

    return result
