##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ast

import linecache
import inspect

from semantix.utils.lang.context import SourcePoint, SourceContext
from semantix.utils import markup


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


class SourceErrorContext(markup.MarkupExceptionContext):
    def __init__(self, source_context):
        if inspect.isframe(source_context):
            self.source_context = source_context_from_frame(source_context)
        else:
            self.source_context = source_context

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        if self.source_context:
            tbp = me.lang.TracebackPoint(name=self.source_context.name,
                                         lineno=self.source_context.start.line,
                                         filename=self.source_context.filename or '<unknown>')
            tbp.load_source(lines=self.source_context.buffer)
        else:
            tbp = me.doc.Text(text='Unknown source context')

        return me.lang.ExceptionContext(title=self.title, body=[tbp])


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
