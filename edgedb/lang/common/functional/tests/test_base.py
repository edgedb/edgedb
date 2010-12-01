##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import functional
from semantix.utils.debug import assert_raises
from semantix.exceptions import SemantixError


class TestUtilsFunctional(object):
    def test_utils_functional_decorator(self):
        class dec1(functional.Decorator):
            def __call__(self, *args, **kwargs):
                return self._func_(*args, **kwargs) + 1

        @dec1
        def test1(a, b=None):
            return a + (b if b else 0)

        assert test1(1, 2) == 4 and test1(1) == 2
        assert hasattr(test1, '__name__') and test1.__name__ == 'test1'


        class Test1:
            BASE = 100

            def __init__(self, base):
                self.base = base

            @dec1
            def test(self, a):
                return a + self.base

            @classmethod
            @dec1
            def test2(cls, a):
                return cls.BASE + a


        t1 = Test1(200)
        assert t1.test(2) == 203
        assert t1.test(2) == 203
        assert t1.test(2) == 203
        assert Test1.test2(3) == 104

        assert hasattr(t1.test, '__name__') and t1.test.__name__ == 'test'
        assert hasattr(Test1.test, '__name__') and Test1.test.__name__ == 'test'

        assert hasattr(t1.test2, '__name__') and t1.test2.__name__ == 'test2'
        assert hasattr(Test1.test2, '__name__') and Test1.test2.__name__ == 'test2'


        class dec2(dec1):
            def instance_call(self, instance, a):
                return self._func_(instance, a*2)

        class Test2(Test1):
            @dec2
            def test(self, a):
                return super().test(a*2) * 10

        t2 = Test2(50)
        assert t2.test(10) == 910

        class dec3(dec1):
            def class_call(self, cls, a):
                return self._func_(cls, a*2)

        class Test3(Test2):
            @dec3
            def test2(cls, a):
                return super().test2(a*2) * 20


        t2 = Test2(50)
        assert t2.test(10) == 910
        assert Test3.test2(10) == 2820

        assert hasattr(Test3.test2, '__name__') and Test3.test2.__name__ == 'test2'


        class dec4(functional.Decorator):
            def __call__(self): pass

        with assert_raises(SemantixError, error_re='does not support any arguments'):
            @dec4(1)
            def test(): pass


        CHK = 0
        class dec5(functional.Decorator):
            def handle_args(self, foo=None, *, bar=None):
                nonlocal CHK
                if foo:
                    assert foo == 42
                if bar:
                    assert bar == 100500
                CHK += 1
            def __call__(self):
                return self._func_()

        @dec5(42)
        def test(): return 42
        assert test() == 42
        assert CHK == 1

        @dec5(42, bar=100500)
        def test(): return 43
        assert test() == 43
        assert CHK == 2

        @dec5()
        def test(): return 44
        assert test() == 44
        assert CHK == 2

    def test_utils_functional_callable(self):
        assert functional.callable(functional.callable)

        class foo:
            def __call__(self): pass
        class bar(foo):
            pass
        assert functional.callable(bar())

        assert functional.callable(object)
        assert not functional.callable(object())
        assert not functional.callable(functional)
        assert not functional.callable(42)


        class P(property):
            def __get__(self, obj, cls):
                1/0
        class C(object):
            __call__ = P()
        assert functional.callable(C())

    def test_utils_functional_cachedproperty(self):
        CHK = 0

        class Test:
            def __init__(self, val):
                self.value = val

            @functional.cachedproperty
            def square(self):
                nonlocal CHK
                CHK += 1
                return self.value ** 2

        t = Test(10)
        assert t.square + t.square + t.square == 300
        assert CHK == 1

        t2 = Test(20)
        assert t2.square == 400 and t.square == 100
