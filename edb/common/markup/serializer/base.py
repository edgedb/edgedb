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

from typing import TypeVar

import collections
import collections.abc
import decimal
import functools
import types
import weakref

from .. import elements

from edb.common import exceptions
from edb.common.markup.format import xrepr
from edb.common import debug

from . import settings

#: Maximum level of nested structures that we can serialize.
#: If we reach it - we'll just stop traversing the objects
#: tree at that point and yield 'elements.base.OverflowBarier'
#:
OVERFLOW_BARIER = 100  # XXX Configurable?

#: Maximum number of total 'serialize' calls.
#: If we reach it - we'll just stop traversing the objects
#: tree at that point and yield 'elements.base.OverflowBarier'
#:
RUN_OVERFLOW_BARIER = 5000  # XXX Configurable?

__all__ = 'serialize',


T = TypeVar('T')


def no_ref_detect(func: T) -> T:
    """Serializer decorated with ``no_ref_detect`` will be executed without
    prior checking the memo if object was already serialized"""

    func.no_ref_detect = True  # type: ignore
    return func


@functools.singledispatch
def serializer(obj, *, ctx):
    """Markup serializers dispatcher"""
    raise NotImplementedError


class Context:
    """Markup serialization context.  Holds the ``memo`` set, which
    is used to avoid serializing objects that already have been serialized,
    and ``depth`` - recursion depth"""

    def __init__(self, trim=True, kwargs=None):
        self.reset()
        self.trim = trim
        self.kwargs = kwargs or {}
        if settings.censor_sensitive_vars:
            self.censor_set = set(settings.censor_list)
        else:
            self.censor_set = set()

    def censored(self, key):
        return key in self.censor_set

    def reset(self):
        self.memo = set()
        self.keep_alive = []
        self.level = 0
        self.run_cnt = 0


def serialize(obj, *, ctx):
    """Serialize arbitrary python object to Markup elements"""

    tobj = type(obj)

    sr = serializer.dispatch(tobj)
    if sr is serializer:
        raise LookupError(f'unable to find serializer for object {obj!r}')

    if (sr is serialize_unknown_object and
            hasattr(tobj, '__dataclass_fields__')):
        sr = serialize_dataclass

    ctx.level += 1
    ctx.run_cnt += 1
    try:
        if ctx.level >= OVERFLOW_BARIER or ctx.run_cnt >= RUN_OVERFLOW_BARIER:
            return elements.base.OverflowBarier()

        ref_detect = True
        try:
            # Was the serializer decorated with ``@no_ref_detect``?
            #
            ref_detect = not sr.no_ref_detect
        except AttributeError:
            pass

        if ref_detect:
            # OK, so if we've already serialized obj, don't do that again, just
            # return ``markup.Ref`` element.
            #
            obj_id = id(obj)
            if obj_id in ctx.memo:
                return elements.lang.Ref(ref=obj_id, refname=repr(obj))
            else:
                ctx.memo.add(obj_id)
                ctx.keep_alive.append(obj)

        try:
            return sr(obj, ctx=ctx)
        except Exception as ex:
            return elements.base.SerializationError(
                text=str(ex), cls='{}.{}'.format(
                    ex.__class__.__module__, ex.__class__.__name__))
    finally:
        ctx.level -= 1


@no_ref_detect
def _serialize_traceback_point(
    obj,
    frame,
    lineno,
    *,
    ctx,
    include_source=True,
    source_window_size=2,
    include_locals=False,
    point_cls=elements.lang.TracebackPoint,
):
    assert source_window_size >= 0

    name = frame.f_code.co_name
    filename = frame.f_code.co_filename

    locals = None
    if include_locals or debug.flags.print_locals:
        locals = serialize(dict(frame.f_locals), ctx=ctx)

    if filename.startswith('.'):
        frame_fn = frame.f_globals.get('__file__')
        if frame_fn and frame_fn.endswith(filename[2:]):
            filename = frame_fn

    point = point_cls(
        name=name, lineno=lineno, filename=filename, locals=locals, id=id(obj))

    if include_source:
        point.load_source(window=source_window_size)

    return point


@no_ref_detect
def serialize_traceback_point(
    obj,
    *,
    ctx,
    include_source=True,
    source_window_size=2,
    include_locals=False,
    point_cls=elements.lang.TracebackPoint,
):
    assert isinstance(obj, types.TracebackType)

    return _serialize_traceback_point(
        obj, obj.tb_frame, obj.tb_lineno, ctx=ctx,
        include_source=include_source, source_window_size=source_window_size,
        include_locals=include_locals, point_cls=point_cls)


@no_ref_detect
def serialize_callstack_point(
    obj,
    *,
    ctx,
    include_source=True,
    source_window_size=2,
    include_locals=False,
    point_cls=elements.lang.TracebackPoint,
):
    assert isinstance(obj, types.FrameType)

    return _serialize_traceback_point(
        obj, obj, obj.f_lineno, ctx=ctx, include_source=include_source,
        source_window_size=source_window_size, include_locals=include_locals,
        point_cls=point_cls)


@serializer.register(types.TracebackType)
def serialize_traceback(obj, *, ctx):
    result = []

    current = obj
    while current is not None:
        result.append(serialize_traceback_point(current, ctx=ctx))
        current = current.tb_next

    return elements.lang.Traceback(items=result, id=id(obj))


@serializer.register(BaseException)
def serialize_exception(obj, *, ctx):
    cause = context = None
    if obj.__cause__ is not None and obj.__cause__ is not obj:
        cause = serialize(obj.__cause__, ctx=ctx)
    elif (
            not obj.__suppress_context__ and obj.__context__ is not None and
            obj.__context__ is not obj):
        context = serialize(obj.__context__, ctx=ctx)

    details_context = None
    contexts = []
    for ex_context in exceptions.iter_contexts(obj):
        if isinstance(ex_context, exceptions.DefaultExceptionContext):
            details_context = ex_context
        else:
            contexts.append(serialize(ex_context, ctx=ctx))

    obj_traceback = obj.__traceback__
    if obj_traceback:
        traceback = elements.lang.ExceptionContext(
            title='Traceback', body=[serialize(obj_traceback, ctx=ctx)])

        if isinstance(obj, SyntaxError):
            point = elements.lang.TracebackPoint(
                name='<parser>', lineno=obj.lineno, colno=obj.offset,
                filename=obj.filename or '<buffer>')
            point.load_source()
            traceback.body[0].items.append(point)

        contexts.append(traceback)

    if details_context is not None:
        contexts.append(serialize(details_context, ctx=ctx))

    markup = elements.lang.Exception(
        class_module=obj.__class__.__module__,
        classname=obj.__class__.__name__, msg=str(obj), contexts=contexts,
        cause=cause, context=context, id=id(obj))

    if isinstance(obj, BaseExceptionGroup):
        markup = elements.doc.Section(
            body=[
                markup,
                elements.doc.Section(
                    title='Grouped exceptions',
                    body=[
                        elements.doc.SubNode(body=serializer(sub, ctx=ctx))
                        for sub in obj.exceptions
                    ]
                )
            ],
        )

    return markup


@serializer.register(exceptions.ExceptionContext)
def serialize_generic_exception_context(obj, *, ctx):
    msg = 'No markup serializer for {!r} context'.format(obj)
    return elements.lang.ExceptionContext(
        title=obj.title, body=[elements.doc.Text(text=msg)])


@serializer.register(exceptions.DefaultExceptionContext)
def serialize_default_exception_context(obj, *, ctx):
    body = []

    if obj.details:
        txt = elements.doc.Text(text='Details: {}'.format(obj.details))
        body.append(elements.doc.Section(body=[txt]))

    if obj.hint:
        txt = elements.doc.Text(text='Hint: {}'.format(obj.hint))
        body.append(elements.doc.Section(body=[txt]))

    return elements.lang.ExceptionContext(title=obj.title, body=body)


@serializer.register(type(None))
@no_ref_detect
def serialize_none(obj, *, ctx):
    return elements.lang.Constants.none


@serializer.register(bool)
@no_ref_detect
def serialize_bool(obj, *, ctx):
    if obj:
        return elements.lang.Constants.true
    else:
        return elements.lang.Constants.false


@serializer.register(int)
@serializer.register(float)
@serializer.register(decimal.Decimal)
@no_ref_detect
def serialize_number(obj, *, ctx):
    return elements.lang.Number(num=obj)


@serializer.register(str)
@no_ref_detect
def serialize_str(obj, *, ctx):
    return elements.lang.String(str=obj)


@serializer.register(collections.UserList)
@serializer.register(list)
@serializer.register(tuple)
@serializer.register(collections.abc.Set)
@serializer.register(weakref.WeakSet)
@serializer.register(set)
@serializer.register(frozenset)
@no_ref_detect
def serialize_sequence(obj, *, ctx, trim_at=100):
    els = []
    cnt = 0
    trim = ctx.trim

    if isinstance(obj, tuple):
        brackets = "()"
    elif isinstance(obj,
                    (collections.abc.Set, weakref.WeakSet, set, frozenset)):
        brackets = "{}"
    else:
        brackets = "[]"

    for cnt, item in enumerate(obj):
        els.append(serialize(item, ctx=ctx))
        if trim and cnt >= trim_at:
            break
    return elements.lang.List(
        items=els, id=id(obj), brackets=brackets,
        trimmed=(trim and cnt >= trim_at))


@serializer.register(dict)
@serializer.register(collections.abc.Mapping)
@no_ref_detect
def serialize_mapping(obj, *, ctx, trim_at=100):
    map = collections.OrderedDict()
    cnt = 0
    trim = ctx.trim
    for cnt, (key, value) in enumerate(obj.items()):
        if not isinstance(key, str):
            key = repr(key)
        if ctx.censored(key) and value is not None:
            value = '********'
        map[key] = serialize(value, ctx=ctx)
        if trim and cnt >= trim_at:
            break
    return elements.lang.Dict(
        items=map, id=id(obj), trimmed=(trim and cnt >= trim_at))


def serialize_dataclass(obj, *, ctx):
    fields = type(obj).__dataclass_fields__

    node = elements.lang.TreeNode(
        id=id(obj),
        name=f'{type(obj).__name__}')

    for fieldname, field in fields.items():
        try:
            val = getattr(obj, fieldname)
        except AttributeError:
            continue

        if not field.repr:
            continue

        node.add_child(
            label=fieldname,
            node=serialize(val, ctx=ctx))

    return node


@serializer.register(object)
@no_ref_detect
def serialize_unknown_object(obj, *, ctx):
    return elements.lang.Object(
        id=id(obj), class_module=type(obj).__module__,
        classname=type(obj).__name__, repr=xrepr(obj, max_len=200))


def _serialize_known_object(obj, attrs, *, ctx):
    map = collections.OrderedDict()
    for attr in attrs:
        map[attr] = serialize(getattr(obj, attr, None), ctx=ctx)
    return elements.lang.Object(
        id=id(obj), class_module=obj.__class__.__module__,
        classname=obj.__class__.__name__, attributes=map)
