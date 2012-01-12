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

from semantix.utils.lang import javascript


def no_jsc_cache(func):
    def wrapper(*args, **kwargs):
        old = javascript.Loader.CACHE_MAGIC_BASE
        javascript.Loader.CACHE_MAGIC_BASE = random.randint(0, 10**6)
        try:
            return func(*args, **kwargs)
        finally:
            javascript.Loader.CACHE_MAGIC_BASE = old

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


_deps = operator.attrgetter('__sx_js_module_deps__')


class TestUtilsLangJSImport:
    @clean_sys_modules
    @no_jsc_cache
    def test_utils_lang_js_import_1(self):
        from semantix.utils.lang.javascript.tests.testimport import foo

        d = _deps(foo)
        assert len(d) == 2
        assert d[0].__name__.endswith('bar')
        assert d[1].__name__.endswith('inner')

        m = sys.modules['semantix.utils.lang.javascript.tests.testimport.inner.ham']
        d = _deps(m)
        assert len(d)  == 1
        assert d[0].__name__.endswith('outer')

        m = sys.modules['semantix.utils.lang.javascript.tests.testimport']
        d = _deps(m)
        assert len(d)  == 1
        assert d[0].__name__.endswith('outer')

        assert isinstance(foo, javascript.JavaScriptModule)

    @clean_sys_modules
    @no_jsc_cache
    def test_utils_lang_js_import_2(self):
        from semantix.utils.lang.javascript.tests.testimport import foo

        mods = []
        for mod in foo.list_deps_modules():
            mods.append(mod.__name__)

        assert mods == ['semantix.utils.lang.javascript.tests.testimport.bar',
                        'semantix.utils.lang.javascript.tests.testimport.outer',
                        'semantix.utils.lang.javascript.tests.testimport.inner.ham',
                        'semantix.utils.lang.javascript.tests.testimport.inner',
                        'semantix.utils.lang.javascript.tests.testimport.foo']
