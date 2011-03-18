##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import subprocess

from semantix.utils import ast, debug
import semantix.utils.lang.javascript.parser.jsparser as jsp
from semantix.utils.lang.javascript.codegen import JavascriptSourceGenerator


class MetaTestJavascript(type):
    @debug.debug
    def do_test(src):
        jsparser = jsp.JSParser()

        result = subprocess.getoutput("v8 -e %r" % src)

        """LOG [js.parse] Test Source
        print(src)
        """

        """LOG [js.parse] Expected Result
        print(result)
        """

        tree = jsparser.parse(src)
        """LOG [js.parse] Test Source AST
        print(ast.dump.pretty_dump(tree, colorize=True))
        """

        processed_src = JavascriptSourceGenerator.to_source(tree)
        """LOG [js.parse] Test Processed Source
        print(processed_src)
        """

        processed = subprocess.getoutput("v8 -e %r" % processed_src)
        assert processed == result

    def __new__(cls, name, bases, dct):
        for testname, testfunc in dct.items():
            if testname.startswith('test_') and getattr(testfunc, '__doc__', None):
                dct[testname] = lambda self, src=testfunc.__doc__: MetaTestJavascript.do_test(src)
                dct[testname].__name__ = testname

        return super().__new__(cls, name, bases, dct)


class TestJavaScriptParsing(metaclass=MetaTestJavascript):
    def test_utils_lang_javascript_1(self):
        """print(1+2);"""

    def test_utils_lang_javascript_2(self):
        """print(1+a);"""



