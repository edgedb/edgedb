##
# Copyright (c) 2010-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common import abc
from edgedb.lang.common.debug import assert_raises


class TestAbc:
    def test_utils_abc_method(self):
        class A(metaclass=abc.AbstractMeta):
            @abc.abstractmethod
            def tmp(self):
                pass

        class B(A):
            pass

        with assert_raises(TypeError, error_re='instantiate abstract'):
            B()

        class C(A):
            def tmp(self):
                pass

        C()

    def test_utils_abc_attr(self):
        class A(metaclass=abc.AbstractMeta):
            foo = abc.abstractattribute()

        with assert_raises(TypeError, error_re='instantiate abstract'):
            A()

        class B(A):
            foo = 1

        assert B().foo == 1

        class C(A):
            def __init__(self):
                self.foo = 2

        with assert_raises(TypeError, error_re='instantiate abstract'):
            C()

        class D(A):
            foo = 3
            bar = abc.abstractattribute(doc='spam')

        assert D.bar.__doc__ == 'spam'

        class E(D):
            bar = 4

        assert E().foo == 3 and E().bar == 4
