##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import contextlib
from time import perf_counter

from metamagic.utils import markup, config
from metamagic.utils.localcontext import HEAD as _HEAD


def _get_local_trace():
    return _HEAD.get('__mm_tracepoints__')

def _set_local_trace(trace):
    return _HEAD.set('__mm_tracepoints__', trace)


class TraceMeta(config.ConfigurableMeta):
    def __new__(mcls, name, bases, dct):
        if '__slots__' not in dct:
            dct['__slots__'] = ()

        # To avoid possible slots-layout conflicts (and there is no real need
        # to have multiple inheritance for traces anyways)
        assert len(bases) <= 1, 'multiple inheritance is not supported for traces'

        return super().__new__(mcls, name, bases, dct)


class Trace(metaclass=TraceMeta):
    __slots__ = ('_traces', '_info', '_entered_at',
                 '_exited_at', '_num', '_id', '_root', '_extras',
                 '_parent', '_prev_trace')

    caption = None
    merge_descendants = False
    merge_same_id_only = False

    measure_overhead = config.cvalue(False, type=bool)

    def __init__(self, *, info=None, id=None, __parent__=None):
        self._root = None
        self._traces = None
        self._info = info
        self._entered_at = self._exited_at = None
        self._num = 1
        self._id = id
        self._parent = __parent__

    def _get_caption(self):
        if self.caption is not None:
            return self.caption
        return '{}.{}'.format(self.__class__.__module__,
                              self.__class__.__name__)

    def set_info(self, val):
        self._info = val

    def __enter__(self):
        _start = perf_counter()

        if self._parent:
            parent = self._parent
            self._parent = None
        else:
            parent = _get_local_trace()

        if parent:
            if parent._traces is None:
                parent._traces = []
            parent._traces.append(self)
            if parent._root is not None:
                root = self._root = parent._root
            else:
                root = self._root = parent
        else:
            # root tracepoint
            self._extras = {
                'measure_overhead': self.measure_overhead,
                'overhead': 0.0,
                'count': 0
            }
            root = None

        self._prev_trace = _get_local_trace()
        _set_local_trace(self)

        self._entered_at = _start

        if root is not None and root._extras['measure_overhead']:
            _extras = root._extras
            _extras['overhead'] += perf_counter() - _start
            _extras['count'] += 1

        return self

    def _merge_traces(self):
        if not self._traces:
            return

        merged_traces = None

        traces = [self._traces[0]]
        for trace in self._traces[1:]:
            prev_trace = traces[-1]
            if (trace.merge_descendants
                    and trace.__class__ is prev_trace.__class__
                    and (not trace.merge_same_id_only
                            or (trace._id == prev_trace._id and trace._id is not None))):

                duration = ((prev_trace._exited_at - prev_trace._entered_at) +
                            (trace._exited_at - trace._entered_at))

                prev_trace._exited_at = trace._exited_at
                prev_trace._entered_at = trace._exited_at - duration
                prev_trace._num += trace._num

                if trace._traces is not None:
                    if prev_trace._traces is None:
                        prev_trace._traces = trace._traces
                    else:
                        prev_trace._traces.extend(trace._traces)

                if merged_traces is None:
                    merged_traces = set()

                merged_traces.add(prev_trace)

            else:
                traces.append(trace)

        if merged_traces is not None:
            for trace in merged_traces:
                trace._merge_traces()

        self._traces = traces

    def __exit__(self, *exc):
        _start = self._exited_at = perf_counter()

        _set_local_trace(self._prev_trace)
        self._prev_trace = None

        cls = self.__class__
        id = self._id

        if self._traces is not None:
            self._merge_traces()

            if self.merge_descendants:
                while True:
                    traces = self._traces
                    if traces is not None and len(traces) == 1:
                        trace = traces[0]
                        if (cls is trace.__class__
                                and (not self.merge_same_id_only
                                        or (id == trace._id and id is not None))):

                            self._num += trace._num
                            self._traces = trace._traces

                            if not self._traces:
                                self._traces = None
                                break
                        else:
                            break
                    else:
                        break

        _root = self._root
        if _root is not None and _root._extras['measure_overhead']:
            _root._extras['overhead'] += perf_counter() - _start

    def __mm_serialize__(self):
        dct = {
            'caption': self._get_caption(),
            'id': id(self),
            'info': self._info,
            'num': self._num,
            'entered_at': self._entered_at,
            'exited_at': self._exited_at,
            'traces': self._traces
        }

        try:
            dct['extras'] = self._extras
        except AttributeError:
            pass

        return dct


class TraceNop:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def set_info(self, val):
        pass

_nop = TraceNop()


def is_tracing():
    return _get_local_trace() is not None


@contextlib.contextmanager
def if_tracing(trace_cls, **kwargs):
    trace = _get_local_trace()

    if trace is not None:
        trace = trace_cls(__parent__=trace, **kwargs)
    else:
        trace = _nop

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
