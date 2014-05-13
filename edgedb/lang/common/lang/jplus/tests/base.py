##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import contextlib
import subprocess
import sys
import tempfile

from metamagic.utils import resource, functional
from metamagic.utils.markup import dump, dump_code

from .. import transpiler, parser

from metamagic.utils import config

from metamagic.utils.debug import debug, assert_raises, debug_logger_off
from metamagic.utils.lang.javascript.tests import base as js_base_test
from metamagic.utils.lang.javascript.codegen import JavascriptSourceGenerator

from metamagic.utils.datastructures import OrderedSet

from metamagic.utils.lang import runtimes
from metamagic.utils.lang import jplus, javascript
from metamagic.utils.lang.jplus.support import builtins as builtins_js

from metamagic.node.targets import Target


@contextlib.contextmanager
def no_bytecode():
    old = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        yield
    finally:
        sys.dont_write_bytecode = old


@contextlib.contextmanager
def nop():
    yield


def transpiler_opts(**kwargs):
    def wrap(func):
        func.transpiler_kwargs = kwargs
        return func
    return wrap


def expected_fail(*args, **kwargs):
    def wrap(func):
        func.expected_fail = (args, kwargs)
        return func
    return wrap


class BaseJPlusTestMeta(js_base_test.JSFunctionalTestMeta):
    doc_prefix = 'JS+'

    TEST_TPL_START = ''
    TEST_TPL_END = ''

    @classmethod
    def load_test_module_from_source(mcls, modname, source):
        loader = jplus.BufferLoader(modname, source.encode('utf-8'),
                                    jplus.Language)

        with debug_logger_off(), no_bytecode():
            module = loader.load_module(modname)
            js_mod = runtimes.load_module_for_runtime(
                        module.__name__, javascript.JavaScriptWebRuntime)

        return js_mod

    @classmethod
    @debug
    def do_test(mcls, source, name=None, data=None, transpiler_kwargs=None):
        if source.startswith(mcls.doc_prefix):
            source = source[len(mcls.doc_prefix)+1:]
        expected = ''

        if '%%' in source:
            source, expected = source.split('%%')
            expected = expected.strip()

        if transpiler_kwargs:
            ctx = {'metamagic.utils.lang.jplus.transpiler.Transpiler.' + key: value
                        for key, value in transpiler_kwargs.items()}
            ctx = config.inline(ctx)
        else:
            ctx = nop()

        with ctx:
            super(BaseJPlusTestMeta, mcls).do_test(source, name=name,
                                                   data=data,
                                                   expected_output=expected)

    @classmethod
    def make_test(mcls, meth, doc):
        def do_test(self, meth=meth, name=meth.__name__, source=doc, mcls=mcls):
            try:
                ef = meth.expected_fail
            except AttributeError:
                ef = None

            transpiler_kwargs = getattr(meth, 'transpiler_kwargs', None)

            if ef:
                with assert_raises(*ef[0], **ef[1]):
                    mcls.do_test(source, name=meth.__name__, transpiler_kwargs=transpiler_kwargs)
            else:
                mcls.do_test(source, name=meth.__name__, transpiler_kwargs=transpiler_kwargs)

        functional.decorate(do_test, meth)
        return do_test


class BaseJPlusTest(metaclass=BaseJPlusTestMeta):
    pass
