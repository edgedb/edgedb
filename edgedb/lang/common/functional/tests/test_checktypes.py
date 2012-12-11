##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types as std_types
import sys
import warnings
import functools

from metamagic.utils import functional
from metamagic.utils.functional import types
from metamagic.utils.debug import assert_raises


class TestUtilsFunctionalChecktypes:
    def test_utils_functional_checktypes_checker_get(self):
        checker = types.Checker.get(int)
        assert isinstance(checker, types.TypeChecker)

        checker = types.Checker.get((str, bytes))
        assert isinstance(checker, types.TupleChecker)


    def test_utils_functional_checktypes_warning(self):
        with warnings.catch_warnings(record=True) as w:
            @functional.checktypes
            def tmp(a, b):
                return b*a

            assert len(w) == 1
            assert issubclass(w[-1].category, UserWarning)
            assert isinstance(tmp, std_types.FunctionType)


    def test_utils_functional_checktypes_custom_checker(self):
        class DictChecker(types.Checker):
            __slots__ = ('key',)

            def __init__(self, key):
                super().__init__()
                self.key = key

            def check(self, value, value_name):
                if not isinstance(value, dict) or self.key not in value:
                    raise TypeError()

        @functional.checktypes
        def tmp(a:DictChecker('foo')):
            return True

        assert tmp({'foo': True})
        with assert_raises(TypeError):
            tmp({'bar': True})


    def test_utils_functional_checktypes_class_methods(self):
        class T:
            @classmethod
            @functional.checktypes
            def tmp1(cls, foo:int):
                return foo * 3

            @staticmethod
            @functional.checktypes
            def tmp2(foo:int):
                return foo * 4

            @functional.checktypes
            def tmp0(self, foo:float, bar:int) -> float:
                return foo + bar

        assert T.tmp1(3) == 9
        with assert_raises(TypeError):
            T.tmp1(3.0)

        assert T.tmp2(3) == 12
        with assert_raises(TypeError):
            T.tmp2(3.0)

        t1, t2 = T(), T()
        assert t1.tmp0(3.0, 4) == 7.0 and t2.tmp0(3.0, 5) == 8.0
        with assert_raises(TypeError):
            t1.tmp0(4, 4.0)

        assert isinstance(t1.tmp0, std_types.MethodType)
        assert t1.tmp0.__func__ is t2.tmp0.__func__


    def test_utils_functional_checktypes_class_decorator(self):
        class dec(functional.Decorator):
            def __call__(self, *args, **kwargs):
                return self.__wrapped__(*args, **kwargs) + 1

        class dec2(functional.Decorator):
            def __call__(self, *args, **kwargs):
                return self.__wrapped__(*args, **kwargs) + 10

        with warnings.catch_warnings(record=True) as w:
            @functional.checktypes
            class T:
                _property = 123

                @classmethod
                def tmp1(cls, foo:int):
                    return foo * 3

                @staticmethod
                def tmp2(foo:int):
                    return foo * 4

                def tmp0(self, foo:float, bar:int) -> float:
                    return foo + bar

                def tmp(self):
                    pass

                @functional.hybridmethod
                def tmp4(it, a:bytes) -> int:
                    return 3

                @dec
                @dec2
                def tmp5(self, *, a:bytes=None) -> int:
                    return self._property

                @classmethod
                @dec
                @dec2
                def tmp6(cls, *, a:int=None) -> int:
                    return cls._property * a

            assert not len(w)

        assert T._property == 123

        assert T.tmp1(3) == 9
        with assert_raises(TypeError):
            T.tmp1(3.0)

        assert T.tmp2(3) == 12
        with assert_raises(TypeError):
            T.tmp2(3.0)

        assert T.tmp4(b'') == 3
        with assert_raises(TypeError):
            T.tmp4('')

        t1, t2 = T(), T()
        assert t1.tmp0(3.0, 4) == 7.0 and t2.tmp0(3.0, 5) == 8.0
        with assert_raises(TypeError):
            t1.tmp0(4, 4.0)
        assert isinstance(t1.tmp0, std_types.MethodType)
        assert t1.tmp0.__func__ is t2.tmp0.__func__

        assert t1.tmp4(b'') == 3
        with assert_raises(TypeError):
            t1.tmp4('')

        t1 = T()
        assert t1.tmp5(a=b'') == 134
        with assert_raises(TypeError):
            t1.tmp5(a='')
        with assert_raises(TypeError):
            t1.tmp5('')

        assert T.tmp6(a=10) == 1241
        assert t1.tmp5(a=b'') == 134

    def test_utils_functional_checktypes_lambda(self):
        @functional.checktypes
        def tmp1(a:lambda arg: arg > 0 and isinstance(arg, int)):
            return a**a

        with assert_raises(TypeError):
            tmp1(-1)
        with assert_raises(TypeError):
            tmp1(0.1)
        assert tmp1(3) == 27

    def test_utils_functional_checktypes_validation(self):
        @functional.checktypes
        def tmp1(a, b:bytes=b'', d=3) -> bytes:
            return b*a
        with assert_raises(TypeError):
            tmp1(2, 4)
        assert tmp1(3, b'123') == 3 * b'123'


        @functional.checktypes
        def tmp2(a, b, d=3) -> bytes:
            return b*a
        assert tmp2(2, b'1') == b'11'
        with assert_raises(TypeError):
            tmp2(2, 4)

        @functional.checktypes
        def tmp4(a, b, d=3) -> (bytes, str):
            return b*a
        assert tmp4(2, b'1') == b'11'
        assert tmp4(2, '1') == '11'
        with assert_raises(TypeError):
            tmp4(2, 4)


        @functional.checktypes
        def tmp3(a:int, *, b:int=1, d:bytes=b'a') -> bytes:
            return (a + b) * d
        assert tmp3(1, b=2) == b'aaa'
        with assert_raises(TypeError):
            tmp3(1, 2)
        with assert_raises(TypeError):
            tmp3(2.0, 4)
        with assert_raises(TypeError):
            tmp3(2.0, b=4)
        with assert_raises(TypeError):
            tmp3(2.0)
        with assert_raises(TypeError):
            tmp3(d='a')


        @functional.checktypes
        def tmp5(a:int, *, b:int=1, d:bytes=b'a') -> tuple:
            return (a, b, d)
        assert tmp5(1, b=None, d=b'a') == (1, None, b'a')


        @functional.checktypes
        def tmp6() -> tuple:
            return
        assert tmp6() is None


        try:
            @functional.checktypes
            def tmp3(a:int, *, b:int='a', d:bytes=b'a') -> bytes:
                pass
        except TypeError:
            pass
        else:
            assert False

        try:
            @functional.checktypes
            def tmp3(a:int, *, b:int=3, d:bytes=None) -> bytes:
                pass
        except TypeError:
            assert False

        try:
            @functional.checktypes
            def tmp3(a:int, b:int='a', d:bytes=b'a') -> bytes:
                pass
        except TypeError:
            pass
        else:
            assert False
