##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import contextlib

from metamagic.utils.functional import contextlib as sx_contextlib
from metamagic.utils.debug import assert_raises


class TestUtilsContextLib:
    def test_utils_contextlib_nested_ok(self):
        TEST = ''

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'
            yield
            TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            yield
            TEST += 'o2'

        with sx_contextlib.nested(context1(), context2()):
            TEST += 'b'

        assert TEST == 'i1i2bo2o1'

    def test_utils_contextlib_nested_error_in_body_unhandled(self):
        TEST = ''

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'

            try:
                yield
            finally:
                TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            try:
                yield
            finally:
                TEST += 'o2'

        with assert_raises(ValueError):
            with sx_contextlib.nested(context1(), context2()):
                raise ValueError('a')

        assert TEST == 'i1i2o2o1'

    def test_utils_contextlib_nested_error_in_body_handled(self):
        TEST = ''

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'

            try:
                yield
            except:
                TEST += 'e1'
            else:
                TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            try:
                yield
            except:
                TEST += 'e2'
            else:
                TEST += 'o2'

        with sx_contextlib.nested(context1(), context2()):
            raise ValueError('a')

        TEST += ' | '

        with context1(), context2():
            raise ValueError('a')

        assert TEST == 'i1i2e2o1 | i1i2e2o1'

    def test_utils_contextlib_nested_error_in_enter(self):
        TEST = ''

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'

            try:
                yield
            except:
                TEST += 'e1'
                raise
            else:
                TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            raise ValueError
            try:
                yield
            except:
                TEST += 'e2'
                raise
            else:
                TEST += 'o2'

        with assert_raises(ValueError):
            with sx_contextlib.nested(context1(), context2()):
                TEST += 'b'

        TEST += ' | '

        with assert_raises(ValueError):
            with context1(), context2():
                TEST += 'b'

        assert TEST == 'i1i2e1 | i1i2e1'

    def test_utils_contextlib_nested_error_in_enter_inhibited(self):
        TEST = ''

        @contextlib.contextmanager
        def context0():
            nonlocal TEST
            TEST += 'i0'

            try:
                yield
            except:
                TEST += 'e0'
            else:
                TEST += 'o0'

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'

            try:
                yield
            except:
                TEST += 'e1'
            else:
                TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            raise ValueError
            try:
                yield
            except:
                TEST += 'e2'
                raise
            else:
                TEST += 'o2'

        with assert_raises(RuntimeError, error_re='inhibited error in nested __enter__'):
            with sx_contextlib.nested(context0(), context1(), context2()):
                TEST += 'b'

    def test_utils_contextlib_nested_error_in_enter_and_exit(self):
        TEST = ''

        @contextlib.contextmanager
        def context0():
            nonlocal TEST
            TEST += 'i0'

            try:
                yield
            except:
                TEST += 'e0'
                raise
            else:
                TEST += 'o0'

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'

            try:
                yield
            except:
                TEST += 'e1'
                raise TypeError
            else:
                TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            raise ValueError
            try:
                yield
            except:
                TEST += 'e2'
                raise
            else:
                TEST += 'o2'

        with assert_raises(TypeError):
            with sx_contextlib.nested(context0(), context1(), context2()):
                TEST += 'b'

    def test_utils_contextlib_nested_error_in_exit(self):
        TEST = ''

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'

            try:
                yield
            except:
                TEST += 'e1'
                raise
            else:
                TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            try:
                yield
            except:
                TEST += 'e2'
                raise
            else:
                raise ValueError
                TEST += 'o2'

        with assert_raises(ValueError):
            with sx_contextlib.nested(context1(), context2()):
                TEST += 'b'

        TEST += ' | '

        with assert_raises(ValueError):
            with context1(), context2():
                TEST += 'b'

        assert TEST == 'i1i2be1 | i1i2be1'

    def test_utils_contextlib_nested_error_in_exit_inhibited(self):
        TEST = ''

        @contextlib.contextmanager
        def context1():
            nonlocal TEST
            TEST += 'i1'

            try:
                yield
            except:
                TEST += 'e1'
            else:
                TEST += 'o1'

        @contextlib.contextmanager
        def context2():
            nonlocal TEST
            TEST += 'i2'
            try:
                yield
            except:
                TEST += 'e2'
                raise
            else:
                raise ValueError
                TEST += 'o2'

        with sx_contextlib.nested(context1(), context2()):
            TEST += 'b'

        TEST += ' | '

        with context1(), context2():
            TEST += 'b'

        assert TEST == 'i1i2be1 | i1i2be1'
