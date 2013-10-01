##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import contextlib
import time

from metamagic.utils import markup
from metamagic.utils import config


class _root(config.Configurable):
    _pointer = config.cvalue(None)


class Trace:
    caption = None

    def __init__(self, *, info=None):
        self._parent = None
        self._traces = []
        self._cfg = None
        self._info = info
        self._entered_at = self._exited_at = None

    def _get_caption(self):
        if self.caption is not None:
            return self.caption
        return '{}.{}'.format(self.__class__.__module__,
                              self.__class__.__name__)

    @property
    def parent(self):
        return self._parent

    def set_info(self, val):
        self._info = val

    def __enter__(self):
        path = '{}.{}.{}'.format(_root.__module__, _root.__name__, '_pointer')

        self._parent = _root._pointer
        if self._parent:
            self._parent._traces.append(self)

        self._cfg = config.inline({path: self})
        self._cfg.__enter__()

        self._entered_at = time.perf_counter()

        return self

    def __exit__(self, *exc):
        self._exited_at = time.perf_counter()
        self._cfg.__exit__(*exc)
        self._cfg = None

    def __mm_serialize__(self):
        return {
            'class': '{}.{}'.format(self.__class__.__module__, self.__class__.__name__),
            'caption': self._get_caption(),
            'id': id(self),
            'info': self._info,
            'entered_at': self._entered_at,
            'exited_at': self._exited_at,
            'traces': self._traces
        }


class TraceNop:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def set_info(self, val):
        pass


def is_tracing():
    return _root._pointer is not None


@contextlib.contextmanager
def if_tracing(trace_cls, **kwargs):
    if is_tracing():
        trace = trace_cls(**kwargs)
    else:
        trace = TraceNop()

    with trace as ctx:
        yield ctx


@markup.serializer.serializer(handles=Trace)
def _serialize_to_markup(tr, *, ctx):
    caption = tr._get_caption()
    node = markup.elements.lang.TreeNode(id=id(tr), name=caption)

    node.add_child(label='cost', node=markup.serialize(tr._exited_at - tr._entered_at, ctx=ctx))
    if tr._info:
        node.add_child(label='info', node=markup.serialize(tr._info, ctx=ctx))

    for idx, ch in enumerate(tr._traces):
        node.add_child(node=markup.serialize(ch, ctx=ctx))

    return node
