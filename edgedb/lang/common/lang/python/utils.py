##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


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
        self.source_context = source_context

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        if inspect.isframe(self.source_context):
            source_context = source_context_from_frame(self.source_context)
        else:
            source_context = self.source_context

        if source_context:
            tbp = me.lang.TracebackPoint(name=source_context.name,
                                         lineno=source_context.start.line,
                                         filename=source_context.filename or '<unknown>')
            tbp.load_source(lines=source_context.buffer)
        else:
            tbp = me.doc.Text(text='Unknown source context')

        return me.lang.ExceptionContext(title=self.title, body=[tbp])
