##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import abc
import sys

from edgedb.lang.common import exceptions
from . import elements, serializer, renderers
from .serializer import serialize
from .serializer import base as _base_serializer


class MarkupCapableMeta(type):

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)
        if 'as_markup' in dct:
            serializer.serializer.register(cls)(getattr(cls, 'as_markup'))
        return cls


class MarkupExceptionContextMeta(MarkupCapableMeta, abc.ABCMeta):
    pass


class MarkupExceptionContext(exceptions.ExceptionContext,
                             metaclass=MarkupExceptionContextMeta):

    @abc.abstractclassmethod
    def as_markup(cls, *, ctx):
        pass


def _serialize(obj, trim=True, kwargs=None):
    ctx = _base_serializer.Context(trim=trim, kwargs=kwargs)
    try:
        return serialize(obj, ctx=ctx)
    finally:
        ctx.reset()


def dumps(obj, header=None, trim=True):
    markup = _serialize(obj, trim=trim)
    if header is not None:
        markup = elements.doc.Section(title=header, body=[markup])
    return renderers.terminal.renders(markup)


def _dump(markup, header, file):
    if header is not None:
        markup = elements.doc.Section(title=header, body=[markup])
    renderers.terminal.render(markup, file=file)


def dump(obj, file=None, trim=True, marker=None, **kwargs):
    for kw in kwargs:
        if kw[0] != '_':
            raise TypeError(f'unknown keyword argument {kw!r}')

    if marker:
        markup = elements.doc.Marker(text=marker)
        renderers.terminal.render(markup, file=file, ensure_newline=False)

    markup = _serialize(obj, trim=trim, kwargs=kwargs)
    _dump(markup, None, file)


def dump_code(code: str, *, lexer='python', header=None, file=None):
    markup = serializer.serialize_code(code, lexer=lexer)
    _dump(markup, header, file)


def dump_callstack(f=None, *, limit=None, header=None, file=None, trim=True):
    if f is None:
        try:
            raise ZeroDivisionError
        except ZeroDivisionError:
            f = sys.exc_info()[2].tb_frame.f_back

    if limit is None:
        limit = getattr(sys, 'tracebacklimit', None)

    result = []
    i = 0
    start_frame = f

    ctx = _base_serializer.Context(trim=trim)

    while f is not None and (limit is None or i < limit):
        result.append(_base_serializer.serialize_callstack_point(f, ctx=ctx))
        f = f.f_back
        i += 1

    result.reverse()
    markup = elements.lang.Traceback(items=result, id=id(start_frame))
    _dump(markup, header, file)
