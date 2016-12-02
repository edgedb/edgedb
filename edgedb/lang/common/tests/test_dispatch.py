##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import inspect
import unittest

from edgedb.lang.common import dispatch


class DispatchTests(unittest.TestCase):

    def test_typedispatch_basics(self):
        # Step 1. Test basics.
        #

        class test(dispatch.TypeDispatcher):
            pass

        @test(handles=int)
        def foo():
            pass

        # decorator returned function obj back
        assert inspect.isfunction(foo)

        @test(handles=(float, str))
        def bar():
            pass

        @test(handles=bool)
        class Spam:
            pass

        assert test.get_handler(int) is foo
        assert test.get_handler(float) is bar
        assert test.get_handler(str) is bar
        assert test.get_handler(bool) is Spam

        # Step 2. Test that dispatchers don't overlap.
        #

        class test2(dispatch.TypeDispatcher):
            pass

        with self.assertRaises(LookupError):
            test2.get_handler(int)

        @test2(handles=int)
        def ham():
            pass

        assert test2.get_handler(int) is ham

    def test_typedispatch_classmethod(self):
        class test(dispatch.TypeDispatcher):
            pass

        @test(handles=int)
        def foo():
            return 'foo'

        @test(method='test')
        class bar:
            @classmethod
            def test(cls):
                return 'bar::test'

        # decorator returned class
        assert isinstance(bar, type)

        class baz(bar):
            @classmethod
            def test(cls):
                return 'baz::test'

        assert test.get_handler(int)() == 'foo'
        assert test.get_handler(bar)() == 'bar::test'
        assert test.get_handler(baz)() == 'baz::test'

    def test_typedispatch_no_instance(self):
        class test(dispatch.TypeDispatcher):
            pass

        with self.assertRaises(TypeError):
            test()
