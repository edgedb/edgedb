##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import inspect

from semantix.utils.lang.context import SourcePoint, SourceContext
from semantix.utils import markup
from semantix.utils.lang.import_.finder import Finder


def get_frame_module(frame):
    try:
        file = inspect.getabsfile(frame)
    except TypeError:
        return None

    return Finder.get_module_by_filename(file)


def source_context_from_frame(frame):
    frame_module = get_frame_module(frame)

    if not frame_module:
        return None

    file_source = inspect.getsourcelines(frame_module)[0]
    line_no = frame.f_lineno
    name = frame_module.__name__

    if line_no > 0:
        offset = sum(len(l) for l in file_source[:line_no])
    else:
        offset = 0

    start = SourcePoint(line_no, None, offset)
    end = None

    context = SourceContext(name, file_source, start, end)
    return context


class SourceErrorContext(markup.MarkupExceptionContext):
    def __init__(self, source_context):
        self.source_context = source_context

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        if self.source_context:
            tbp = me.lang.TracebackPoint(name=self.source_context.name,
                                         lineno=self.source_context.start.line)
            tbp.load_source(lines=self.source_context.buffer)
        else:
            tbp = me.doc.Text(text='Unknown source context')

        return me.lang.ExceptionContext(title=self.title, body=[tbp])
