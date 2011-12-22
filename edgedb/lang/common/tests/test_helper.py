##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import helper, functional
from semantix.utils.debug import assert_raises


class Foo:
    def bar(self):
        pass

    def _staticbar():
        pass
    staticbar = staticmethod(_staticbar)


    def _classbar(cls):
        pass
    classbar = classmethod(_classbar)

    saved = (_staticbar, _classbar)
    del _classbar
    del _staticbar

    @functional.checktypes
    def typesbar(self) -> int:
        pass


class TestHelper:
    def foo():
        pass

    def test_utils_helper_get_func_class(self):
        assert not hasattr(Foo, '_staticbar')
        assert not hasattr(Foo, '_classbar')

        assert helper.get_function_class(self.foo) is self.__class__

        assert helper.get_function_class(Foo.bar) is Foo
        assert helper.get_function_class(Foo.staticbar) is Foo
        assert helper.get_function_class(Foo.saved[0]) is Foo
        assert helper.get_function_class(Foo.classbar) is Foo
        assert helper.get_function_class(Foo.saved[1]) is Foo
        assert helper.get_function_class(Foo.typesbar.__wrapped__) is Foo

        foo = Foo()
        assert helper.get_function_class(foo.bar) is Foo
        assert helper.get_function_class(foo.staticbar) is Foo
        assert helper.get_function_class(foo.saved[0]) is Foo
        assert helper.get_function_class(foo.classbar) is Foo
        assert helper.get_function_class(foo.saved[1]) is Foo
        assert helper.get_function_class(foo.typesbar.__wrapped__) is Foo

    def test_utils_helper_xrepr(self):
        a = '1234567890'

        assert helper.xrepr(a) == repr(a)

        assert helper.xrepr(a, max_len=5) == "''..."
        assert helper.xrepr(a, max_len=7) == "'12'..."
        assert helper.xrepr(a, max_len=12) == repr(a)

        assert repr(repr) == '<built-in function repr>'

        assert helper.xrepr(repr) == repr(repr)
        assert helper.xrepr(repr, max_len=10) == '<built>...'
        assert helper.xrepr(repr, max_len=100) == repr(repr)

        assert len(helper.xrepr(repr, max_len=10)) == 10
