##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import logging

from semantix.utils.debug import assert_raises, ErrorExpected, assert_logs, \
                                 assert_shorter_than, assert_longer_than


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

        with assert_raises(Exception, context=ValueError):
            # Cascaded case
            try:
                int('a')
            except Exception as ex:
                raise Exception

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

        with assert_raises(ErrorExpected):
            # Remember -- error_re is a regular expression, so parens must be quoted
            with assert_raises(ValueError, error_re='invalid literal for int() with base \d+'):
                int('g')

        with assert_raises(Exception, cause=ValueError, error_re='invalid literal for int()'):
            # Cascaded case
            try:
                int('a')
            except Exception as ex:
                raise Exception from ex

        with assert_raises(Exception, context=ValueError, error_re='invalid literal for int()'):
            # Cascaded case
            try:
                int('a')
            except Exception as ex:
                raise Exception

        with assert_raises(ErrorExpected, error_re='''Exception with cause ValueError was ''' \
                                                   '''expected to be raised with cause message ''' \
                                                   '''that matches 'invalid exception', got ''' \
                                                   '''"invalid literal'''):

            with assert_raises(Exception, cause=ValueError, error_re='invalid exception'):
                # Cascaded case
                try:
                    int('a')
                except Exception as ex:
                    raise Exception('foo bar') from ex

        class Ex(Exception):
            pass

        with assert_raises(Ex, attrs={'foo': 'bar'}):
            ex = Ex()
            ex.foo = 'bar'
            raise ex

        with assert_raises(ErrorExpected):
            with assert_raises(Ex, attrs={'foo': 'bar'}):
                ex = Ex()
                ex.bar = 'bar'
                raise ex

        with assert_raises(ErrorExpected):
            with assert_raises(Ex, attrs={'foo': 'bar'}):
                ex = Ex()
                ex.foo = 'foo'
                raise ex


    def test_utils_debug_assert_logs(self):
        logger = logging.getLogger('semantix.tests.debug')

        with assert_raises(AssertionError,
                           error_re="no expected message matching 'spam' was logged"):
            with assert_logs('spam'):
                pass

        with assert_raises(AssertionError,
                           error_re="no expected message matching 'spam' was logged"):
            with assert_logs('spam'):
                logger.debug('ham')

        with assert_raises(AssertionError,
                           error_re="no expected message matching 'spam' on logger " \
                                                                        "'foo.bar' was logged"):
            with assert_logs('spam', logger_re='foo.bar'):
                logger.debug('spam')

        with assert_logs('spam'):
            logger.debug('spam')

        with assert_logs('spam', logger_re='semantix.tests.debug'):
            logger.debug('spam')

    def test_utils_debug_assert_shorter_than(self):
        import time

        with assert_raises(AssertionError, error_re='block was expected'):
            with assert_shorter_than(0.1):
                time.sleep(0.2)

        with assert_shorter_than(0.01):
            1 + 2

        with assert_shorter_than(1):
            1 + 2

    def test_utils_debug_assert_longer_than(self):
        import time

        with assert_longer_than(0.1):
            time.sleep(0.2)

        with assert_raises(AssertionError, error_re='block was expected'):
            with assert_longer_than(0.01):
                1 + 2
