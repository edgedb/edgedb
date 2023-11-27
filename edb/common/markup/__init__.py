#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import abc
import sys

from edb.common import exceptions
from . import elements, serializer, renderers
from .serializer import serialize
from .serializer import base as _base_serializer
from .serializer.base import Context  # noqa
from .elements.base import Markup  # noqa


class MarkupCapableMixin:

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'as_markup'):
            serializer.serializer.register(cls)(cls.as_markup)


class MarkupExceptionContext(
    exceptions.ExceptionContext,
    MarkupCapableMixin,
):

    @abc.abstractclassmethod  # type: ignore
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


def dump(*objs, file=None, trim=True, marker=None, **kwargs):
    for obj in objs:
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
