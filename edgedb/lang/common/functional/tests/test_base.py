##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
import functools

from metamagic.utils import functional
from metamagic.utils.debug import assert_raises
from metamagic.exceptions import MetamagicError
from metamagic.utils.functional.tests import base


class TestUtilsFunctional(object):
    def test_utils_functional_inclass(self):
        def dec(expect):
            def wrap(obj, expect=expect):
                assert functional.in_class() is expect
                return obj
            return wrap

        def do_test(dec):
            @dec(False)
            class foo:
                @dec(True)
                def bar(self):
                    @dec(False)
                    def bar2():
                        pass

                    @dec(False)
                    class foo2:
                        @dec(True)
                        def bar3(self): pass

                        @dec(True)
                        class test():
                            @dec(True)
                            def bar4(self): pass

            foo().bar()

            def test():
                __locals__ = True
                __module__ = True
                dec(False)(123)
            test()

        do_test(dec)
        do_test(lambda arg: dec(arg))

    def test_utils_functional_decorator(self):
        class dec1(functional.Decorator):
            def __call__(self, *args, **kwargs):
                return self.__wrapped__(*args, **kwargs) + 1

        @dec1
        def test1(a, b=None):
            '''spam'''
            return a + (b if b else 0)

        assert test1(1, 2) == 4 and test1(1) == 2
        assert hasattr(test1, '__name__') and test1.__name__ == 'test1'
        assert test1.__doc__ == 'spam'

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
                return self.__wrapped__(instance, a*2)

        class Test2(Test1):
            @dec2
            def test(self, a):
                return super().test(a*2) * 10

        t2 = Test2(50)
        assert t2.test(10) == 910

        class dec3(dec1):
            def class_call(self, cls, a):
                return self.__wrapped__(cls, a*2)

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

        with assert_raises(MetamagicError, error_re='does not support any arguments'):
            @dec4(1)
            def test(): pass


        CHK = 0
        TOT = 0
        class dec5(functional.Decorator):
            @classmethod
            def decorate(cls, func, *args, **kwargs):
                nonlocal TOT
                TOT += 1
            def handle_args(self, foo=None, *, bar=None):
                nonlocal CHK
                if foo:
                    assert foo == 42
                if bar:
                    assert bar == 100500
                CHK += 1
            def __call__(self):
                return self.__wrapped__()

        @dec5(42)
        def test(): return 42
        assert TOT == 1
        assert test() == 42
        assert CHK == 1

        @dec5(42, bar=100500)
        def test(): return 43
        assert TOT == 2
        assert test() == 43
        assert CHK == 2

        @dec5()
        def test(): return 44
        assert TOT == 3
        assert test() == 44
        assert CHK == 2

        @dec5
        def test(): return 45
        assert TOT == 4
        assert test() == 45
        assert CHK == 2


        CHK = 0
        class dec6(functional.Decorator):
            @classmethod
            def decorate(cls, func, a=None, *, b=None):
                nonlocal CHK
                assert isinstance(func, types.FunctionType) and func.__name__ == 'test'
                if a:
                    assert a == 42
                if b:
                    assert b == 100500
                CHK += 1
                return lambda: func() + 1

        @dec6(42)
        def test(): return 7
        assert CHK == 1
        assert test() == 8

        @dec6(42, b=100500)
        @base.wrap
        def test(): return 7
        assert CHK == 2
        assert test() == 8

        @dec6()
        @base.wrap
        def test(): return 8
        assert CHK == 3
        assert test() == 9

        @dec6
        @base.wrap
        def test(): return 8
        assert CHK == 4
        assert test() == 9


        dec7 = functools.partial(dec6, b=100500)

        @dec7()
        @base.wrap
        def test(): return 8
        assert CHK == 5
        assert test() == 9

        @dec7
        @base.wrap
        def test(): return 8
        assert CHK == 6
        assert test() == 9

    def test_utils_functional_hybridmethod(self):
        class C1:
            @functional.hybridmethod
            def method(scope, param):
                """Method documentation"""
                if isinstance(scope, C1):
                    return param * 2
                elif isinstance(scope, type) and issubclass(scope, C1):
                    return param * 3
                else:
                    assert False

            def __bool__(self):
                return False

        assert C1.method(10) == 30
        assert C1().method(10) == 20

        assert C1.method.__name__ == 'method'
        assert C1.method.__doc__ == 'Method documentation'

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
