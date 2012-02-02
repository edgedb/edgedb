##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import elements, serializer, renderers
from .serializer import serialize
from .serializer import base as _base_serializer
from semantix.exceptions import ExceptionContext as _ExceptionContext
from semantix.utils import abc


@serializer.serializer(method='as_markup')
class MarkupExceptionContext(_ExceptionContext, metaclass=abc.AbstractMeta):
    @abc.abstractclassmethod
    def as_markup(cls, *, ctx):
        pass


def _serialize(obj, trim=True):
    ctx = _base_serializer.Context(trim=trim)
    return serialize(obj, ctx=ctx)


def dumps(obj, header=None, trim=True):
    markup = _serialize(obj, trim=trim)
    if header is not None:
        markup = elements.doc.Section(title=header, body=[markup])
    return renderers.terminal.renders(markup)


def _dump(markup, header, file):
    if header is not None:
        markup = elements.doc.Section(title=header, body=[markup])
    renderers.terminal.render(markup, file=file)


def dump(obj, *, header=None, file=None, trim=True):
    markup = _serialize(obj, trim=trim)
    _dump(markup, header, file)


def dump_code(code:str, *, lexer='python', header=None, file=None):
    markup = serializer.serialize_code(code, lexer=lexer)
    _dump(markup, header, file)
