##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import inspect

from semantix.exceptions import ExceptionContext
from semantix.utils.datastructures import xvalue
from semantix.utils.lang.meta import SourcePoint, SourceContext
from semantix.utils import helper
from semantix.utils.lang.import_ import Importer


def get_frame_module(frame):
    try:
        file = inspect.getabsfile(frame)
    except TypeError:
        return None

    return Importer.get_module_by_filename(file)


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


class SourceErrorContext(ExceptionContext):
    def __init__(self, source_context):
        self.source_context = source_context

    def render(self):
        if inspect.isframe(self.source_context):
            source_context = source_context_from_frame(self.source_context)
        else:
            source_context = self.source_context

        chunks = []

        if source_context:
            chunks.append(xvalue('%s:\n' % source_context.name, fg='white', opts=('bold',)))
            lines = source_context.buffer
            lineno = source_context.start.line
            chunks.extend(helper.format_code_context(lines, lineno, colorize=True))
        else:
            chunks.append('Unknown source context')

        return chunks
