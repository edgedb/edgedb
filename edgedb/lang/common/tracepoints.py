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
    merge_descendants = False
    merge_same_id_only = False

    def __init__(self, *, info=None, id=None):
        self._parent = None
        self._traces = []
        self._cfg = None
        self._info = info
        self._entered_at = self._exited_at = None
        self._num = 1
        self._id = id

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

    def _merge_traces(self):
        if not self._traces:
            return

        traces = [self._traces[0]]
        for trace in self._traces[1:]:
            if trace.merge_descendants and type(trace) is type(traces[-1]) and \
                    (not trace.merge_same_id_only or
                            (trace._id == traces[-1]._id and trace._id is not None)):

                traces[-1]._exited_at = trace._exited_at
                traces[-1]._num += trace._num
                traces[-1]._traces.extend(trace._traces)
                traces[-1]._merge_traces()
            else:
                traces.append(trace)

        self._traces = traces

    def __exit__(self, *exc):
        self._exited_at = time.perf_counter()
        self._cfg.__exit__(*exc)
        self._cfg = None

        if self._traces:
            self._merge_traces()

            traces = self._traces
            if len(traces) == 1 and self.merge_descendants and type(self) is type(traces[0]) and \
                        (not self.merge_same_id_only or
                                (self._id == traces[0]._id and self._id is not None)):

                self._num += traces[0]._num
                self._traces.extend(traces[0]._traces)
                self._merge_traces()
                traces = []

            self._traces = traces

    def __mm_serialize__(self):
        return {
            'class': '{}.{}'.format(self.__class__.__module__, self.__class__.__name__),
            'caption': self._get_caption(),
            'id': id(self),
            'info': self._info,
            'num': self._num,
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
