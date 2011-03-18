##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import subprocess

from semantix.utils import ast, debug, functional
import semantix.utils.lang.javascript.parser.jsparser as jsp
from semantix.utils.lang.javascript.codegen import JavascriptSourceGenerator


def jxfail(*args, **kwargs):
    def wrap(func):
        setattr(func, 'xfail', (args, kwargs))
        return func
    return wrap


class MetaTestJavascript(type):
    @debug.debug
    def do_test(src):
        jsparser = jsp.JSParser()
        tmp = open('test.js', 'w')
        tmp.write(src)
        tmp.close()
        result = subprocess.getoutput("v8 %s" % tmp.name)
        #os.unlink(tmp.name)

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

        #os.unlink(tmp.name)
        tmp = open('test.js', 'w')
        tmp.write(processed_src)
        tmp.close()
        processed = subprocess.getoutput("v8 %s" % tmp.name)
        os.unlink(tmp.name)
        assert processed == result

    def __new__(cls, name, bases, dct):
        for testname, testfunc in dct.items():
            if testname.startswith('test_') and getattr(testfunc, '__doc__', None):
                if hasattr(testfunc, 'xfail'):
                    def do_test(self, func=testfunc):
                        with debug.assert_raises(*func.xfail[0], **func.xfail[1]):
                            cls.do_test(func.__doc__)
                    dct[testname] = do_test

                else:
                    dct[testname] = lambda self, src=testfunc.__doc__: cls.do_test(src)

                dct[testname].__name__ = testname

        return super().__new__(cls, name, bases, dct)
