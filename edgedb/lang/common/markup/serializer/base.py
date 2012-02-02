##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
import decimal
import collections
import weakref

from semantix.utils.functional.dispatch import TypeDispatcher
from .. import elements

from semantix import exceptions
from semantix.utils.helper import xrepr


#: Maximum level of nested structures that we can serialize.
#: If we reach it - we'll just stop traversing the objects
#: tree at that point and yield 'elements.base.OverflowBarier'
#:
OVERFLOW_BARIER = 100


__all__ = 'serialize',


def no_ref_detect(func):
    """Serializer decorated with ``no_ref_detect`` will be executed wihout
    prior checking the memo if object was already serialized"""

    func.no_ref_detect = True
    return func


class serializer(TypeDispatcher):
    """Markup serializers dispatcher"""


class Context:
    """Markup serialization context.  Holds the ``memo`` set, which
    is used to avoid serializing objects that already have been serialized,
    and ``depth`` - recursion depth"""

    def __init__(self, trim=True):
        self.memo = set()
        self.trim = trim
        self.level = 0


def serialize(obj, *, ctx=None):
    """Serialize arbitrary python object to Markup elements"""

    try:
        # Find serialization function
        #
        sr = serializer.get_handler(type=type(obj))
    except LookupError:
        raise LookupError('unable to find serializer for object {!r}'.format(obj))

    if ctx is None:
        # No context?  Perhaps, this is a top-level call to ``serialize``.
        # Initialize empty context.
        #
        ctx = Context()

    ctx.level += 1
    try:
        if ctx.level >= OVERFLOW_BARIER:
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
                refname = '{}.{}'.format(type(obj).__module__, type(obj).__name__)
                return elements.lang.Ref(ref=obj_id, refname=refname)
            else:
                ctx.memo.add(obj_id)

        return sr(obj, ctx=ctx)
    finally:
        ctx.level -= 1


@no_ref_detect
def serialize_traceback_point(obj, *, ctx, include_source=True, source_window_size=2,
                              include_locals=False, point_cls=elements.lang.TracebackPoint):

    assert type(obj) is types.TracebackType
    assert source_window_size >= 0

    lineno = obj.tb_lineno
    name = obj.tb_frame.f_code.co_name
    filename = obj.tb_frame.f_code.co_filename

    locals = None
    if include_locals:
        locals = serialize(dict(obj.tb_frame.f_locals), ctx=ctx)

    if filename.startswith('.'):
        frame_fn = obj.tb_frame.f_globals.get('__file__')
        if frame_fn and frame_fn.endswith(filename[2:]):
            filename = frame_fn

    point = point_cls(name=name, lineno=lineno, filename=filename,
                      locals=locals, id=id(obj))

    if include_source:
        point.load_source(window=source_window_size)

    return point


@serializer(handles=types.TracebackType)
def serialize_traceback(obj, *, ctx):
    result = []

    current = obj
    while current is not None:
        result.append(serialize_traceback_point(current, ctx=ctx))
        current = current.tb_next

    return elements.lang.Traceback(items=result, id=id(obj))


@serializer(handles=BaseException)
def serialize_exception(obj, *, ctx):
    try:
        # Serializing an exception to markup maybe a very time-wise
        # expensive operation.
        #
        # However, it should be safe to cache results, as we serialize
        # exceptions just before they get printed or logged, after the
        # point of adding new contexts.
        #
        return obj.__sx_markup_cached__
    except AttributeError:
        pass

    cause = context = None
    if obj.__cause__ is not None and obj.__cause__ is not obj:
        cause = serialize(obj.__cause__, ctx=ctx)
    elif obj.__context__ is not None and obj.__context__ is not obj:
        context = serialize(obj.__context__, ctx=ctx)

    details_context = None
    contexts = []
    for ex_context in exceptions._iter_contexts(obj):
        if isinstance(ex_context, exceptions.DefaultExceptionContext):
            details_context = ex_context
        else:
            contexts.append(serialize(ex_context, ctx=ctx))

    if obj.__traceback__:
        traceback = elements.lang.ExceptionContext(title='Traceback',
                                                   body=[serialize(obj.__traceback__, ctx=ctx)])
        contexts.append(traceback)

    if details_context is not None:
        contexts.append(serialize(details_context, ctx=ctx))

    markup = elements.lang.Exception(class_module=obj.__class__.__module__,
                                     class_name=obj.__class__.__name__,
                                     msg=str(obj),
                                     contexts=contexts,
                                     cause=cause,
                                     context=context,
                                     id=id(obj))

    obj.__sx_markup_cached__ = markup
    return markup


@serializer(handles=exceptions.ExceptionContext)
def serialize_generic_exception_context(obj, *, ctx):
    msg = 'No markup serializer for {!r} context'.format(obj)
    return elements.lang.ExceptionContext(title=obj.title,
                                          body=[elements.doc.Text(text=msg)])


@serializer(handles=exceptions.DefaultExceptionContext)
def serialize_default_exception_context(obj, *, ctx):
    body = []

    if obj.details:
        body.append(elements.doc.Text(text='Details: {}'.format(obj.details)))

    if obj.hint:
        body.append(elements.doc.Text(text='Hint: {}'.format(obj.hint)))

    return elements.lang.ExceptionContext(title=obj.title, body=body)


@serializer(handles=type(None))
@no_ref_detect
def serialize_none(obj, *, ctx):
    return elements.lang.Constants.none


@serializer(handles=bool)
@no_ref_detect
def serialize_bool(obj, *, ctx):
    if obj:
        return elements.lang.Constants.true
    else:
        return elements.lang.Constants.false


@serializer(handles=(int, float, decimal.Decimal))
@no_ref_detect
def serialize_number(obj, *, ctx):
    return elements.lang.Number(num=obj)


@serializer(handles=str)
@no_ref_detect
def serialize_str(obj, *, ctx):
    return elements.lang.String(str=obj)


@serializer(handles=(collections.UserList, list, tuple, collections.Set,
                     weakref.WeakSet, set, frozenset))
def serialize_sequence(obj, *, ctx, trim_at=100):
    els = []
    cnt = 0
    trim = ctx.trim
    for cnt, item in enumerate(obj):
        els.append(serialize(item, ctx=ctx))
        if trim and cnt >= trim_at:
            break
    return elements.lang.List(items=els, id=id(obj), trimmed=(trim and cnt >= trim_at))


@serializer(handles=(dict, collections.Mapping))
def serialize_mapping(obj, *, ctx, trim_at=100):
    map = collections.OrderedDict()
    cnt = 0
    trim = ctx.trim
    for cnt, (key, value) in enumerate(obj.items()):
        if not isinstance(key, str):
            key = repr(key)
        map[key] = serialize(value, ctx=ctx)
        if trim and cnt >= trim_at:
            break
    return elements.lang.Dict(items=map, id=id(obj), trimmed=(trim and cnt >= trim_at))


@serializer(handles=object)
@no_ref_detect
def serialize_uknown_object(obj, *, ctx):
    return elements.lang.Object(id=id(obj),
                                class_module=type(obj).__module__,
                                class_name=type(obj).__name__,
                                repr=xrepr(obj, max_len=100))


def _serialize_known_object(obj, attrs, *, ctx):
    map = collections.OrderedDict()
    for attr in attrs:
        map[attr] = serialize(getattr(obj, attr, None), ctx=ctx)
    return elements.lang.Object(id=id(obj),
                                class_module=obj.__class__.__module__,
                                class_name=obj.__class__.__name__,
                                attributes=map)
