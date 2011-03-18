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

        result = subprocess.getoutput("v8 -e '%s'" % src)

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

        processed = subprocess.getoutput("v8 -e '%s'" % processed_src)
        assert processed == result

    def __new__(cls, name, bases, dct):
        for testname, testfunc in dct.items():
            if testname.startswith('test_') and getattr(testfunc, '__doc__', None):
                dct[testname] = lambda self, src=testfunc.__doc__: MetaTestJavascript.do_test(src)
                dct[testname].__name__ = testname

        return super().__new__(cls, name, bases, dct)


class TestJavaScriptParsing(metaclass=MetaTestJavascript):
    def test_utils_lang_javascript_0(self):
        """print(1);"""

    def test_utils_lang_javascript_1(self):
        """print(1+2);"""

    def test_utils_lang_javascript_2(self):
        """print(1+a);"""

    def test_utils_lang_javascript_3(self):
        """var a,b,c=[,,7,,,8];print(1,2,3,4,"aa",a,b,c);"""

    def test_utils_lang_javascript_4(self):
        """var a = {do : 1}; print(a.do);"""

    def test_utils_lang_javascript_5(self):
        """var a = {get while () {}}; print(a.while);"""

    def test_utils_lang_javascript_6(self):
        """var a = {set get (a) {}}; print(a.get);"""

    def test_utils_lang_javascript_7(self):
        """var a = {do : 1, get while () {}, set get (a) {}}; print(a.do, a.while, a.get);"""

    def test_utils_lang_javascript_8(self):
        """var a = 3; print(-a++);"""

    def test_utils_lang_javascript_9(self):
        """var a = 3;
print(---a);"""

    def test_utils_lang_javascript_10(self):
        """var a = 3; print(a << 2, a >>> 1);"""

    def test_utils_lang_javascript_11(self):
        """var a = -4; print(a >> 1);"""
