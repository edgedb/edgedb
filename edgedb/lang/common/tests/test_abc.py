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
