##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys
import random
import functools
import operator
import logging

from metamagic.utils.lang import javascript
from metamagic.utils.debug import debug_logger_off
from metamagic.utils import resource


def no_jsc_cache(func):
    def wrapper(*args, **kwargs):
        old = javascript.Loader._cache_magic
        javascript.Loader._cache_magic = random.randint(0, 10**6)
        try:
            return func(*args, **kwargs)
        finally:
            javascript.Loader._cache_magic = old

    functools.update_wrapper(wrapper, func)
    return wrapper


def clean_sys_modules(func):
    def wrapper(*args, **kwargs):
        mods = sys.modules
        func(*args, **kwargs)

        # in case of exception - we don't recover sys.modules; that's intentional
        sys.modules = mods

    functools.update_wrapper(wrapper, func)
    return wrapper


_deps = lambda res: [sys.modules[im] for im in res.__sx_imports__]


class TestUtilsLangJSImport:
    @clean_sys_modules
    @no_jsc_cache
    def test_utils_lang_js_import_1(self):
        with debug_logger_off():
            from metamagic.utils.lang.javascript.tests.testimport import foo

        d = _deps(foo)
        assert len(d) == 3
        assert d[0].__name__.endswith('bar')
        assert d[1].__name__.endswith('inner')
        assert d[2].__name__.endswith('spam')

        m = sys.modules['metamagic.utils.lang.javascript.tests.testimport.inner.ham']
        d = _deps(m)
        assert len(d)  == 1
        assert d[0].__name__.endswith('outer')

        m = sys.modules['metamagic.utils.lang.javascript.tests.testimport']
        d = _deps(m)
        assert len(d)  == 1
        assert d[0].__name__.endswith('outer')

        assert isinstance(foo, javascript.JavaScriptModule)

    @clean_sys_modules
    @no_jsc_cache
    def test_utils_lang_js_import_2(self):
        with debug_logger_off():
            from metamagic.utils.lang.javascript.tests.testimport import foo

        mods = resource.ResourceBucket.get_import_list((foo,))
        mods = [m.__name__ for m in mods]

        assert mods == ['metamagic.utils.lang.javascript.tests.testimport.outer',
                        'metamagic.utils.lang.javascript.tests.testimport',
                        'metamagic.utils.lang.javascript.tests.testimport.bar',
                        'metamagic.utils.lang.javascript.tests.testimport.inner.ham',
                        'metamagic.utils.lang.javascript.tests.testimport.inner',
                        'metamagic.utils.lang.javascript.tests.testimport.spam',
                        'metamagic.utils.lang.javascript.tests.testimport.foo']
