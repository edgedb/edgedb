##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import subprocess
import tempfile
import py.test
import importlib

from semantix.utils import debug, functional, config, markup, resource
import semantix.utils.lang.javascript.parser.jsparser as jsp
from semantix.utils.lang.javascript.codegen import JavascriptSourceGenerator
from semantix.utils.lang.javascript import Loader as JSLoader, BaseJavaScriptModule, \
                                           Language as JSLanguage, JavaScriptModule


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


class BaseJSFunctionalTestMeta(type, metaclass=config.ConfigurableMeta):
    v8_executable = config.cvalue('v8', type=str, doc='path to v8 executable')
    v8_found = None

    # flag to be used for disabling tests
    #
    skipif = False


    def __new__(mcls, name, bases, dct):
        if mcls.v8_found is None:
            result = subprocess.getoutput('{} {}'.format(mcls.v8_executable,
                                                         """-e 'print("semantix")'"""))
            mcls.v8_found = result == 'semantix'
            mcls.skipif = not mcls.v8_found

        return super().__new__(mcls, name, bases, dct)


class JSFunctionalTestMeta(BaseJSFunctionalTestMeta):
    TEST_TPL_START = '''
    ;(function() {
        'use strict';

        // %from semantix.utils.lang.javascript.tests import assert
    '''

    TEST_TPL_END = '''
        print('OK');
    })();
    ''';

    def __new__(mcls, name, bases, dct):
        for meth_name, meth in dct.items():
            if not meth_name.startswith('test_'):
                continue

            doc = getattr(meth, '__doc__', '')
            if not doc.startswith('JS\n'):
                continue

            dct[meth_name] = py.test.mark.skipif(str(mcls.skipif))(mcls.make_test(meth, doc))

        return super().__new__(mcls, name, bases, dct)

    @classmethod
    def run_v8(mcls, imports, bootstrap, source):
        with tempfile.NamedTemporaryFile('w') as file:
            file.write(bootstrap)
            file.write(source)
            file.flush()

            result = subprocess.getoutput('{} {}'.format(mcls.v8_executable, file.name))
            if result != 'OK':
                print()
                markup.dump(imports, header='Imports', trim=False)
                markup.dump_code(source, lexer='javascript', header='Test Source')
                markup.dump_header('Test Execution Trace')
                print(result)

            assert result =='OK'

    @classmethod
    def make_test(mcls, meth, doc):
        def do_test(self, meth=meth, source=doc, mcls=mcls):
            source = source[3:]
            source = mcls.TEST_TPL_START + source + mcls.TEST_TPL_END

            # XXX heads up, ugly hacks ahead
            module = BaseJavaScriptModule('tmp_mod_' + meth.__name__)
            module.__file__ = '<tmp>'

            loader = JSLoader(module.__name__, '', JSLanguage)
            with debug.debug_logger_off():
                imports = loader.code_from_source(module, source.encode('utf-8'), log=False)

            deps = []
            for dep_name, dep_weak in imports:
                with debug.debug_logger_off():
                    dep = importlib.import_module(dep_name)
                deps.extend(resource.Resource._list_resources(dep))

            imports = []
            bootstrap = []
            for dep in deps:
                if isinstance(dep, JavaScriptModule):
                    imports.append(dep.__name__)
                    with open(dep.__file__, 'rt') as dep_f:
                        bootstrap.append(dep_f.read())

            mcls.run_v8(imports, '\n;\n'.join(bootstrap), source)

        functional.decorate(do_test, meth)
        return do_test


class JSFunctionalTest(metaclass=JSFunctionalTestMeta):
    pass


class MetaJSParserTest_Base(type, metaclass=config.ConfigurableMeta):
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


class MetaJSParserTest_Functional(BaseJSFunctionalTestMeta, MetaJSParserTest_Base):
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
