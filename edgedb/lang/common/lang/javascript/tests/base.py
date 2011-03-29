##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import subprocess
import tempfile
import py.test

from semantix.utils import ast, debug, functional, config
import semantix.utils.lang.javascript.parser.jsparser as jsp
from semantix.utils.lang.javascript.codegen import JavascriptSourceGenerator


def jxfail(*args, **kwargs):
    def wrap(func):
        setattr(func, 'xfail', (args, kwargs))
        return func
    return wrap


class MetaJSParserTest(type, metaclass=config.ConfigurableMeta):
    v8_executable = config.cvalue('v8', type=str, doc='path to v8 executable')
    v8_found = None

    @debug.debug
    def do_test(src):
        jsparser = jsp.JSParser()

        with tempfile.NamedTemporaryFile('w') as file:
            file.write(src)
            file.flush()

            result = subprocess.getoutput("%s %s" % (MetaJSParserTest.v8_executable, file.name))

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

            if len(src) > len(processed_src):
                processed_src += (len(src) - len(processed_src) + 1) * ' '

            file.seek(0)
            file.write(processed_src)
            file.flush()

            processed = subprocess.getoutput("%s %s" % (MetaJSParserTest.v8_executable, file.name))

            """LOG [js.parse] Processed Result
            print(result)
            """

        assert processed.replace(' ', '') == result.replace(' ', '')

    def __new__(cls, name, bases, dct):
        if MetaJSParserTest.v8_found is None:
            result = subprocess.getoutput('%s %s' % (cls.v8_executable,
                                                     """-e 'print("semantix")'"""))
            MetaJSParserTest.v8_found = result != 'semantix'

        for testname, testfunc in dct.items():
            if testname.startswith('test_') and getattr(testfunc, '__doc__', None):
                if hasattr(testfunc, 'xfail'):
                    def do_test(self, func=testfunc):
                        with debug.assert_raises(*func.xfail[0], **func.xfail[1]):
                            cls.do_test(func.__doc__)
                    dct[testname] = do_test

                else:
                    dct[testname] = lambda self, src=testfunc.__doc__: cls.do_test(src)
                    dct[testname] = py.test.mark.skipif('%s' % cls.v8_found)(dct[testname])

                dct[testname].__name__ = testname

        return super().__new__(cls, name, bases, dct)
