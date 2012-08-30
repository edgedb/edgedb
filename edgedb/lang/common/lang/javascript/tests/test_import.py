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

from semantix.utils.lang import javascript
from semantix.utils.debug import debug_logger_off


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


_deps = lambda res: list(res.__sx_resource_deps__.items())


class TestUtilsLangJSImport:
    @clean_sys_modules
    @no_jsc_cache
    def test_utils_lang_js_import_1(self):
        with debug_logger_off():
            from semantix.utils.lang.javascript.tests.testimport import foo

        d = _deps(foo)
        assert len(d) == 3
        assert d[0][0].__name__.endswith('bar')
        assert d[1][0].__name__.endswith('inner')
        assert d[2][0].__name__.endswith('spam')

        m = sys.modules['semantix.utils.lang.javascript.tests.testimport.inner.ham']
        d = _deps(m)
        assert len(d)  == 1
        assert d[0][0].__name__.endswith('outer')

        m = sys.modules['semantix.utils.lang.javascript.tests.testimport']
        d = _deps(m)
        assert len(d)  == 1
        assert d[0][0].__name__.endswith('outer')

        assert isinstance(foo, javascript.JavaScriptModule)

    @clean_sys_modules
    @no_jsc_cache
    def test_utils_lang_js_import_2(self):
        with debug_logger_off():
            from semantix.utils.lang.javascript.tests.testimport import foo

        mods = []
        for mod in type(foo)._list_resources(foo):
            mods.append(mod.__name__)

        assert mods == ['semantix.utils.lang.javascript.tests.testimport.outer',
                        'semantix.utils.lang.javascript.tests.testimport',
                        'semantix.utils.lang.javascript.tests.testimport.bar',
                        'semantix.utils.lang.javascript.tests.testimport.inner.ham',
                        'semantix.utils.lang.javascript.tests.testimport.inner',
                        'semantix.utils.lang.javascript.tests.testimport.spam',
                        'semantix.utils.lang.javascript.tests.testimport.foo']
