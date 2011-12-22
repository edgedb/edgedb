##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re
import subprocess
import tempfile
import py.test

from semantix.utils import ast, debug, functional, config, markup
from semantix.utils.debug import _indent_code, highlight
import semantix.utils.lang.javascript.parser.jsparser as jsp
from semantix.utils.lang.javascript.codegen import JavascriptSourceGenerator
from semantix.utils.lang.javascript.ast import ForEachNode


def jxfail(*args, **kwargs):
    def wrap(func):
        setattr(func, 'xfail', (args, kwargs))
        return func
    return wrap

def flags(**args):
    def wrap(func):
        setattr(func, 'flags', args)
        return func
    return wrap

# useful for filtering output
#
filter = re.compile(r'(\s|\'|\"|\\|;)+')


class MetaJSParserTest_Base(type, metaclass=config.ConfigurableMeta):
    # flag to be used for disabling tests
    #
    skipif = False

    @debug.debug
    def do_test(src, flags):
        jsparser = jsp.JSParser(**flags)

        """LOG [js.extra.parse] Test Source
        print(highlight(src, 'javascript'))
        """

        tree = jsparser.parse(src)
        """LOG [js.extra.parse] Test Source AST (no checking for now)
        markup.dump(tree)
        """

        processed_src = JavascriptSourceGenerator.to_source(tree)
        """LOG [js.extra.parse] Test Processed Source
        print(highlight(processed_src, 'javascript'))
        """

        assert filter.sub('', src) == filter.sub('', processed_src)

    def __new__(cls, name, bases, dct):
        for testname, testfunc in dct.items():
            if testname.startswith('test_') and getattr(testfunc, '__doc__', None):
                flags = getattr(testfunc, 'flags', {})

                if hasattr(testfunc, 'xfail'):
                    def do_test(self, func=testfunc, flags=flags):
                        with debug.assert_raises(*func.xfail[0], **func.xfail[1]):
                            cls.do_test(func.__doc__, flags)
                    dct[testname] = do_test

                else:
                    dct[testname] = lambda self, src=testfunc.__doc__, flags=flags: \
                                                                    cls.do_test(src, flags)
                    dct[testname] = py.test.mark.skipif('%s' % cls.skipif)(dct[testname])

                dct[testname].__name__ = testname

        return super().__new__(cls, name, bases, dct)


class MetaJSParserTest_Functional(MetaJSParserTest_Base):
    v8_executable = config.cvalue('v8', type=str, doc='path to v8 executable')
    v8_found = None

    @debug.debug
    def do_test(src, flags):
        jsparser = jsp.JSParser(**flags)

        with tempfile.NamedTemporaryFile('w') as file:
            file.write(src)
            file.flush()

            result = subprocess.getoutput("%s %s" %
                                          (MetaJSParserTest_Functional.v8_executable, file.name))

            """LOG [js.parse] Test Source
            print(src)
            """

            """LOG [js.parse] Expected Result
            print(result)
            """
            tree = jsparser.parse(src)
            """LOG [js.parse] Test Source AST
            markup.dump(tree)
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

            processed = subprocess.getoutput("%s %s" %
                                             (MetaJSParserTest_Functional.v8_executable, file.name))

            """LOG [js.parse] Processed Result
            print(result)
            """

        assert processed.replace(' ', '') == result.replace(' ', '')

    def __new__(cls, name, bases, dct):
        if MetaJSParserTest_Functional.v8_found is None:
            result = subprocess.getoutput('%s %s' % (cls.v8_executable,
                                                     """-e 'print("semantix")'"""))
            MetaJSParserTest_Functional.v8_found = result == 'semantix'
            MetaJSParserTest_Functional.skipif = not MetaJSParserTest_Functional.v8_found

        return super().__new__(cls, name, bases, dct)

