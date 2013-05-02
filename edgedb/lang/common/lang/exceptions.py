##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic import exceptions
from metamagic.utils import markup


class SourceErrorContext(markup.MarkupExceptionContext):
    def __init__(self, source_context):
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


class LanguageError(exceptions.MetamagicError):
    def __init__(self, msg, *, context=None, **kwargs):
        super().__init__(msg, **kwargs)
        exceptions._add_context(self, SourceErrorContext(source_context=context))


class UnresolvedError(LanguageError):
    pass
