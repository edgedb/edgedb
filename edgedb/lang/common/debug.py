##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import inspect
import re
import ast
import contextlib

from semantix.utils.functional import decorate, callable


enabled = __debug__
channels = set()

class DebugDecoratorParseError(Exception): pass


def _indent_code(source, absolute=None, relative=None):
    def _calc_tab_size(str):
        count = 0
        for i in str:
            if i == ' ':
                count += 1
            else:
                break
        return count

    tab_size = min(_calc_tab_size(line) for line in source.split('\n') if line.strip())

    if relative is not None:
        absolute = tab_size + relative

    if absolute < 0:
        absolute = 0

    if absolute is not None:
        source = '\n'.join([(' ' * absolute) + line[tab_size:] \
                          for line in source.split('\n')])

    return source

def _set_location(node, lineno):
    if 'lineno' in node._attributes:
        node.lineno = lineno

    for c in ast.iter_child_nodes(node):
        _set_location(c, lineno)
    return node

class debug(object):
    active = False

    def __new__(cls, func):
        if cls.active or not enabled:
            return func

        cls.active = True

        source = _indent_code(inspect.getsource(func), absolute=0)
        sourceloc = inspect.getsourcelines(func)[1]
        orig_file = inspect.getsourcefile(func)

        func_start = source.find('\ndef ') + 1
        sourceloc += source[:func_start].count('\n')
        source = source[func_start:]

        tree = ast.parse(source, filename=orig_file)
        ast.increment_lineno(tree, sourceloc - 1)

        class Transformer(ast.NodeTransformer):

            pattern = re.compile(r'''(?P<type>LOG|LINE)
                                     \s+ \[ \s* (?P<tags> [\w\.]+ (?:\s* , \s* [\w+\.]+)* ) \s* \]
                                     (?P<title>.*)
                                  ''', re.X)

            def visit_Expr(self, node):
                if isinstance(node.value, ast.Str):
                    if node.value.s.startswith('LOG') or node.value.s.startswith('LINE'):
                        m = Transformer.pattern.match(node.value.s)
                        if m:
                            type = m.group('type').strip()
                            title = m.group('title').strip()
                            tags = {t.strip() for t in m.group('tags').split(',')}

                            comment = node.value.s.split('\n')

                            # Str().lineno is for the _last_ line of the string.
                            # We want to use the first.
                            lineno = node.lineno - len(comment) + 1

                            text = 'import semantix.utils.debug, semantix.utils.helper, os as _os_\n' \
                                   'if semantix.utils.debug.channels & %r:\n' \
                                   '    pass\n' % tags

                            if title:
                                if type == 'LOG':
                                    text += '    print(semantix.utils.helper.dump_header(%r))\n' % title
                                else:
                                    text += '    print(_os_.getpid(), %r, %s)' % (title, ', '.join(comment[1:]))

                            code = ast.parse(text.rstrip(), filename=orig_file)
                            code = ast.fix_missing_locations(code)
                            _set_location(code, lineno)

                            if type == 'LOG' and len(comment) > 1:
                                ctext = _indent_code('\n'.join(comment[1:]), absolute=0)
                                ccode = ast.parse(ctext, filename=orig_file)

                                ast.increment_lineno(ccode, lineno)

                                # Prepend the custom code to the If block body
                                code.body[1].body.extend(ccode.body)

                            return code.body
                        else:
                            raise DebugDecoratorParseError('invalid debug decorator syntax')
                return node

        tree = Transformer().visit(tree)
        code = compile(tree, orig_file if orig_file else '<string>', 'exec')

        _locals = {}
        exec(code, func.__globals__, _locals)

        new_func = _locals[func.__name__]
        cls.active = False

        decorate(new_func, func)

        return new_func


def debug_channels(*channels):
    class DebugContext:
        def __init__(self, *channels):
            self.channels = set(channels)

        def __enter__(self):
            global channels
            self.change = self.channels - channels
            channels.update(self.change)

        def __exit__(self, exc_type, exc_value, tb):
            global channels
            channels -= self.change

    return DebugContext(*channels)


def highlight(code, lang=None):
    try:
        from pygments import highlight as h
        from pygments.lexers import get_lexer_by_name
        from pygments.formatters import TerminalFormatter
    except ImportError:
        return code

    return h(code, get_lexer_by_name(lang), TerminalFormatter(bg='dark', style='native'))


class ErrorExpected(Exception): pass


@contextlib.contextmanager
def assert_raises(exception_cls, *, cause=None, error_re=None):
    try:
        yield
    except exception_cls as ex:
        if cause is not None:
            assert isinstance(ex.__cause__, cause)

        if error_re is not None:
            err_re = re.compile('%s' % error_re)

            assert err_re.search(ex.args[0]) or \
                   (cause is not None and err_re.search(ex.__cause__.args[0]))

    except Exception as ex:
        raise ErrorExpected('%s was expected to be raised, got %s' % \
                                    (exception_cls.__name__, ex.__class__.__name__)) from ex

    else:
        raise ErrorExpected('%s was expected to be raised' % exception_cls.__name__)


def timeit(target):
    """
    Utility function to simplify writing performance benchmarks.

    Usage:

    from semantix.util.debug import timeit

    1) in a "with" statement:
        >>> with timeit('long list'):
        ...     result = ''.join(str(i) for i in range(0, 10**5))
        ...
        long list, in 0.101 seconds

    2) as a decorator with some message:
        >>> @timeit('long list generator')
        ... def test(length):
        ...     return ''.join(str(i) for i in range(0, length))
        ...
        >>> list = test(10**5)
        long list generator, in 0.099 seconds

    3) as a simple decorator:
        >>> @timeit
        ... def test(length):
        ...     return ''.join(str(i) for i in range(0, length))
        ...
        >>> list = test(10**5)
        <function test at 0x71f978>, in 0.098 seconds
    """

    import time

    class Timer:
        def __init__(self, message):
            self.message = message

        def __enter__(self):
            self.started = time.time()

        def __exit__(self, exc_type, exc_value, tb):
            print("%s, in %.3f seconds" % (self.message, time.time() - self.started))

        def decorate(self, func):
            def new_func(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)
            return new_func

        def __call__(self, *args):
            if len(args) == 1 and callable(args[0]):
                return self.decorate(args[0])
            else:
                raise Exception("Invalid arguments")

    if target and isinstance(target, str):
        return Timer(target)
    elif target and callable(target):
        return Timer(repr(target)).decorate(target)
    else:
        raise Exception("Invalid arguments")
