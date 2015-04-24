##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ast

import linecache
import inspect
import types

from importkit.context import SourcePoint, SourceContext
from importkit.exceptions import SourceErrorContext


def source_context_from_frame(frame):
    try:
        filename = inspect.getabsfile(frame)
    except TypeError:
        return None

    file_source = linecache.getlines(filename, frame.f_globals)

    line_no = frame.f_lineno
    name = frame.f_code.co_name

    if line_no > 0:
        offset = sum(len(l) for l in file_source[:line_no])
    else:
        offset = 0

    start = SourcePoint(line_no, None, offset)
    end = None

    context = SourceContext(name, file_source, start, end, filename=filename)
    return context


class SourceErrorContext(SourceErrorContext):
    def __init__(self, source_context):
        if inspect.isframe(source_context):
            self.source_context = source_context_from_frame(source_context)
        else:
            self.source_context = source_context


def resolve(expr, globals):
    tree = ast.parse(expr, '<{}>'.format(expr))
    expr = tree.body[0].value

    def _resolve(expr, globals):
        node = expr

        if isinstance(node, ast.Name):
            return globals[node.id]

        if isinstance(node, ast.Attribute):
            value = _resolve(node.value, globals)
            return getattr(value, node.attr)

        if isinstance(node, ast.Subscript):
            value = _resolve(node.value, globals)
            node = node.slice
            if isinstance(node, ast.Index):
                node = node.value
                if isinstance(node, ast.Num):
                    return value[node.n]
                if isinstance(node, ast.Str):
                    return value[node.s]

        raise TypeError('unsupported ast node {}'.format(node))

    return _resolve(expr, globals)
