##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import abc
from semantix.utils.debug import assert_raises


class TestAbc:
    def test_utils_abc(self):
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
