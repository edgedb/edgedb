##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.debug import assert_raises, ErrorExpected


class TestAssertRaises:
    def test_utils_debug_assert_raises(self):
        with assert_raises(ValueError):
            # Simple case
            int('a')

        with assert_raises(Exception, cause=ValueError):
            # Cascaded case
            try:
                int('a')
            except Exception as ex:
                raise Exception from ex

        with assert_raises(TypeError):
            # Test other exception
            set((list(),))

        with assert_raises(TypeError, error_re='unhashable type'):
            # Test for an error substring
            set((list(),))

        for base in (10, 16):
            # Substring is a regexp
            with assert_raises(ValueError,
                               error_re='invalid literal for int\(\) with base \d+'):
                int('g', base)

        with assert_raises(ErrorExpected):
            # Let it test itself :) int(10) does not raise
            with assert_raises(ValueError):
                int(10)

        with assert_raises(AssertionError):
            # Remember -- error_re is a regular expression, so parens must be quoted
            with assert_raises(ValueError, error_re='invalid literal for int() with base \d+'):
                int('g')
