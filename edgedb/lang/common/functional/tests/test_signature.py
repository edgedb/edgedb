##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.functional.signature import signature, BindError
from semantix.utils.debug import assert_raises


def call(func, *args, **kwargs):
    sig = signature(func)
    ba = sig.bind(*args, **kwargs)
    return func(*ba.args, **ba.kwargs)


class TestUtilsSignature(object):
    def test_utils_signature_empty(self):
        def test():
            return 'spam'

        assert call(test) == 'spam'

    def test_utils_signature_1(self):
        def test(*args, **kwargs):
            return args, kwargs

        assert call(test) == ((), {})
        assert call(test, 1) == ((1,), {})
        assert call(test, 1, 2) == ((1, 2), {})
        assert call(test, foo='bar') == ((), {'foo': 'bar'})
        assert call(test, 1, foo='bar') == ((1,), {'foo': 'bar'})
        assert call(test, 1, 2, foo='bar') == ((1, 2), {'foo': 'bar'})

    def test_utils_signature_args(self):
        def test(a, b, c):
            return a + b + c

        assert call(test, 1, 2, 3) == 6

        with assert_raises(BindError, error_re='too many positional arguments'):
            call(test, 1, 2, 3, 4)

        with assert_raises(BindError, error_re="'b' parameter"):
            call(test, 1)

        with assert_raises(BindError, error_re="'a' parameter"):
            call(test)

    def test_utils_signature_varargs_order(self):
        def test(*args):
            assert args == (1, 2, 3)
        call(test, 1, 2, 3)

    def test_utils_signature_2(self):
        def test(a, b, c, *args):
            return a + b + c + sum(args)
        assert call(test, 1, 2, 3, 4, 5) == 15

    def test_utils_signature_4(self):
        def test(*args):
            return sum(args)
        assert call(test, 1, 2, 3, 4, 5) == 15

    def test_utils_signature_5(self):
        def test(**kwargs):
            assert kwargs == {'foo': 'bar', 'spam': 'ham'}
        call(test, foo='bar', spam='ham')

    def test_utils_signature_6(self):
        def test(**kwargs):
            assert kwargs == {'foo': 'bar', 'spam': 'ham'}
        call(test, foo='bar', spam='ham')

    def test_utils_signature_7(self):
        def test(a, b, **kwargs):
            assert a == 1
            assert b == 2
            assert kwargs == {'foo': 'bar', 'spam': 'ham'}

        call(test, 1, 2, foo='bar', spam='ham')
        call(test, a=1, b=2, foo='bar', spam='ham')
        call(test, 1, b=2, foo='bar', spam='ham')

    def test_utils_signature_8(self):
        def test(a, b, c=3, **kwargs):
            assert a == 1
            assert b == 2
            assert kwargs == {'foo': 'bar', 'kwc': c}

        call(test, 1, 2, foo='bar', kwc=3)
        call(test, 1, 2, 4, foo='bar', kwc=4)
        call(test, 1, 2, c=5, foo='bar', kwc=5)
        call(test, 1, c=6, b=2, foo='bar', kwc=6)
        call(test, b=2, foo='bar', kwc=7, a=1, c=7)

    def test_utils_signature_9(self):
        def test(a, b, c=3, *args):
            assert (a, b, c, args) == (1, 2, 3, (4, 5))

        call(test, 1, 2, 3, 4, 5)

        with assert_raises(BindError, error_re='multiple values for keyword argument \'c\''):
            call(test, 1, 2, 4, 5, c=3)

    def test_utils_signature_10(self):
        def test(*, foo):
            assert foo == 1

        with assert_raises(BindError, error_re='too many positional arguments'):
            call(test, 1)

        call(test, foo=1)

    def test_utils_signature_11(self):
        def test(foo, *, bar):
            assert foo == 1
            assert bar == 2

        call(test, 1, bar=2)
        call(test, bar=2, foo=1)

        with assert_raises(BindError, error_re='too many positional arguments'):
            call(test, 1, 2)

        with assert_raises(BindError, error_re='too many positional arguments'):
            call(test, 1, 2, bar=2)

        with assert_raises(BindError, error_re='too many keyword arguments'):
            call(test, 1, bar=2, spam='ham')

        with assert_raises(BindError, error_re='\'bar\' parameter lacking default value'):
            call(test, 1)

    def test_utils_signature_12(self):
        def test(foo, *, bar, **bin):
            assert foo == 1
            assert bar == 2
            return bin

        assert call(test, 1, bar=2, spam='ham') == {'spam': 'ham'}

    def test_utils_signature_13(self):
        def test(a, b, c:str='1'):
            return a+b+c
        assert call(test, '1', '2', c='3') == '123'

    def test_utils_signature_obj(self):
        def test(a, b:int, c, d:'foo'=1, *args:1, k=1, v:'bar', x=False, **y:2) -> 42: pass

        s = signature(test)
        assert s.return_annotation == 42

        assert len(s.args) == 4
        assert s.args[0].name == 'a' and not hasattr(s.args[0], 'default') \
                                                and not hasattr(s.args[0], 'annotation')
        assert s.args[1].name == 'b' and not hasattr(s.args[1], 'default') \
                                                and s.args[1].annotation is int
        assert s.args[2].name == 'c' and not hasattr(s.args[2], 'default') \
                                                and not hasattr(s.args[2], 'annotation')
        assert s.args[3].name == 'd' and s.args[3].default == 1 \
                                                and s.args[3].annotation == 'foo'

        assert s['b'] == s.args[1]

        assert s.vararg
        assert s.vararg.name == 'args'
        assert s.vararg.annotation == 1

        assert len(s.kwonlyargs) == 3
        assert s.kwonlyargs[0].name == 'k' and s.kwonlyargs[0].default == 1 \
                                                and not hasattr(s.kwonlyargs[0], 'annotation')
        assert s.kwonlyargs[1].name == 'v' and not hasattr(s.kwonlyargs[1], 'default') \
                                                and s.kwonlyargs[1].annotation == 'bar'

        assert s.varkwarg
        assert s.varkwarg.name == 'y'
        assert s.varkwarg.annotation == 2
