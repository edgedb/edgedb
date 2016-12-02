##
# Copyright (c) 2010-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest

from edgedb.lang.common import abc


class AbcTests(unittest.TestCase):

    def test_common_abc_method(self):
        class A(metaclass=abc.AbstractMeta):
            @abc.abstractmethod
            def tmp(self):
                pass

        class B(A):
            pass

        with self.assertRaisesRegex(TypeError, 'instantiate abstract'):
            B()

        class C(A):
            def tmp(self):
                pass

        C()
