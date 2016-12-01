##
# Copyright (c) 2008-2010, 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""A collection of useful debugging routines."""

import functools
import inspect
import re
import os
import ast


def _init_debug_channels():
    channels = set()

    for k, v in os.environ.items():
        if k.startswith('EDGEDB_DEBUG_') and v:
            channel = k[len('EDGEDB_DEBUG_'):].lower().replace('_', '.')
            channels.add(channel)

    return channels


channels = _init_debug_channels()


class DebugDecoratorParseError(Exception):
    pass


def _indent_code(source, absolute=None, relative=None):
    def _calc_tab_size(str):
        count = 0
        for i in str:
            if i == ' ':
                count += 1
            else:
                break
        return count

    tab_sizes = tuple(
        (_calc_tab_size(line) for line in source.split('\n') if line.strip()))
    if tab_sizes:
        tab_size = min(tab_sizes)
    else:
        tab_size = 0

    if relative is not None:
        absolute = tab_size + relative

    if absolute < 0:
        absolute = 0

    if absolute is not None:
        source = '\n'.join([(' ' * absolute) + line[tab_size:]
                           for line in source.split('\n')])

    return source


def _set_location(node, lineno):
    if 'lineno' in node._attributes:
        node.lineno = lineno

    for c in ast.iter_child_nodes(node):
        _set_location(c, lineno)
    return node


def _dump_header(title):
    return '\n' + '=' * 80 + '\n' + title + '\n' + '=' * 80 + '\n'


class debug:
    enabled = bool(channels)
    active = False

    def __new__(cls, func):
        if cls.active or not cls.enabled:
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

            pattern = re.compile(r'''
                (?P<type>LOG|LINE)
                \s+ \[ \s* (?P<tags> [\w\.]+ (?:\s* , \s* [\w+\.]+)* ) \s* \]
                (?P<title>.*)
            ''', re.X)

            def visit_Expr(self, node):
                if isinstance(node.value, ast.Str):
                    if node.value.s.startswith(
                            'LOG') or node.value.s.startswith('LINE'):
                        m = Transformer.pattern.match(node.value.s)
                        if m:
                            type = m.group('type').strip()
                            title = m.group('title').strip()
                            tags = {
                                t.strip()
                                for t in m.group('tags').split(',')
                            }

                            comment = node.value.s.split('\n')

                            # Str().lineno is for the _last_ line of the
                            # string. We want to use the first.
                            lineno = node.lineno - len(comment) + 1

                            text = (
                                'import edgedb.lang.common.debug, '
                                'os as _os_\n'
                                'if edgedb.lang.common.debug.channels & %r:\n'
                                '    pass\n'
                            ) % tags

                            if title:
                                if type == 'LOG':
                                    text += (
                                        '    print(edgedb.lang.common.debug'
                                        '._dump_header(%r))\n'
                                    ) % title
                                else:
                                    text += (
                                        '    print(_os_.getpid(), %r, %s)'
                                    ) % (title, ', '.join(comment[1:]))

                            code = ast.parse(text.rstrip(), filename=orig_file)
                            code = ast.fix_missing_locations(code)
                            _set_location(code, lineno)

                            if type == 'LOG' and len(comment) > 1:
                                ctext = _indent_code(
                                    '\n'.join(comment[1:]), absolute=0)
                                ccode = ast.parse(ctext, filename=orig_file)

                                ast.increment_lineno(ccode, lineno)

                                # Prepend the custom code to the If block body
                                code.body[1].body.extend(ccode.body)

                            return code.body
                        else:
                            raise DebugDecoratorParseError(
                                'invalid debug decorator syntax')
                return node

        tree = Transformer().visit(tree)
        code = compile(tree, orig_file if orig_file else '<string>', 'exec')

        _locals = {}
        exec(code, func.__globals__, _locals)

        new_func = _locals[func.__name__]
        cls.active = False

        new_func = functools.wraps(func)(new_func)
        return new_func
