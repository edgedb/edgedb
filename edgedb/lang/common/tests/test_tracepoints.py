##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import tracepoints


def test_utils_tracepoints_slots():
    class Foo(tracepoints.Trace):
        pass

    assert hasattr(Foo, '__slots__')
    assert not hasattr(Foo(), '__dict__')


def test_utils_tracepoints_1():
    class Foo(tracepoints.Trace):
        pass
    class Bar(tracepoints.Trace):
        pass

    root = Foo()
    with root:
        sum(i for i in range(100))
        with Bar():
            sum(i for i in range(100))

    assert root._entered_at < root._exited_at
    assert len(root._traces) == 1
    assert isinstance(root._traces[0], Bar)
    assert root._traces[0]._entered_at > root._entered_at
    assert root._traces[0]._entered_at < root._exited_at
    assert root._traces[0]._exited_at < root._exited_at


def test_utils_tracepoints_2():
    assert not tracepoints.is_tracing()

    class Foo(tracepoints.Trace):
        pass
    class Bar(tracepoints.Trace):
        pass

    with tracepoints.if_tracing(Foo) as f:
        assert not tracepoints.is_tracing()
        assert f is tracepoints.TraceNop

    root = Foo()
    with root:
        assert tracepoints.is_tracing()
        with tracepoints.if_tracing(Bar) as f:
            assert isinstance(f, Bar)
            assert tracepoints.is_tracing()
