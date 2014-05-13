##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import re
import subprocess
import tempfile
import importlib

from metamagic.utils.datastructures import OrderedSet
from metamagic.utils import debug, functional, config, markup, resource
from metamagic.node.targets import Target
from metamagic.utils.lang import loader as lang_loader
from metamagic import json, test
import metamagic.utils.lang.javascript.parser.jsparser as jsp
from metamagic.utils.lang.javascript.codegen import JavascriptSourceGenerator
from metamagic.utils.lang.javascript import BufferLoader as JSBufferLoader, BaseJavaScriptModule, \
                                           Language as JSLanguage, JavaScriptModule, \
                                           VirtualJavaScriptResource


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
    v8_executable = config.cvalue('node', type=str, doc='path to nodejs executable')
    v8_found = None

    # flag to be used for disabling tests
    #
    skipif = False

    def __new__(mcls, name, bases, dct):
        if mcls.v8_found is None:
            result = subprocess.getoutput('{} {}'.format(mcls.v8_executable,
                                                         """-e 'console.log("metamagic")'"""))
            mcls.v8_found = result == 'metamagic'
            mcls.skipif = not mcls.v8_found

        return super().__new__(mcls, name, bases, dct)


class JSFunctionalTestMeta(BaseJSFunctionalTestMeta):
    doc_prefix = 'JS'

    TEST_TPL_START = '''
    ;(function($$$global) {
        'use strict';

        function dump(obj) {
            print(JSON.stringify(obj, null, '  '));
        }

        // %from metamagic.utils.lang.javascript.tests import assert

        var $$$ = {};
        for (var i in $$$global) {
            $$$[i] = true;
        }
    '''

    TEST_TPL_END = '''
        for (var i in $$$global) {
            if (!$$$[i]) {
                delete $$$global[i];
            }
        }

        print('OK');
    }).call(this, this);
    ''';

    TEST_HTML = '''<!DOCTYPE html>
    <html>
        <body>
            <script type="text/javascript"><!--
            {source}
            //-->
            </script>
        </body>
    </html>
    ''';

    @classmethod
    def parse_test(mcls, source, base_deps=None, name=None, pre='', post=''):
        if source.startswith(mcls.doc_prefix):
            source = source[len(mcls.doc_prefix)+1:]

        source = mcls.TEST_TPL_START + pre + source + post + mcls.TEST_TPL_END

        modname = 'tmp_mod_' + (name or 'test_js')

        module = mcls.load_test_module_from_source(modname, source)

        deps = Target.get_import_list((module,))
        deps.discard(module)
        return module, deps

    @classmethod
    def load_test_module_from_source(mcls, modname, source):
        loader = JSBufferLoader(modname, source.encode('utf-8'), JSLanguage)

        with debug.debug_logger_off():
            module = loader.load_module(modname)

        return module

    @classmethod
    def get_source(mcls, module):
        if isinstance(module, JavaScriptModule):
            with open(module.__file__, 'rt') as mod_f:
                return mod_f.read()
        elif (isinstance(module, resource.VirtualFile) and
                                    isinstance(module, BaseJavaScriptModule)):
            return module.__sx_resource_get_source__().decode('utf-8')

    @classmethod
    def compile_boostrap(mcls, deps):
        bootstrap = []
        for dep in deps:
            source = mcls.get_source(dep)
            if source:
                bootstrap.append(source)

        return bootstrap

    @classmethod
    def do_test(mcls, source, name=None, data=None, *, expected_output='OK'):
        module, deps = mcls.parse_test(source, name=name)

        js_source = mcls.get_source(module)
        bootstrap = mcls.compile_boostrap(deps)

        if data is not None:
            bootstrap.append('var $ = ' + json.dumps(data) + ';');

        mcls.run_v8(deps, '\n;\n'.join(bootstrap), js_source,
                    expected_output=expected_output)

    @classmethod
    def _gen_html(mcls, cls, dct):
        all_sources = []
        all_deps = OrderedSet()

        for meth_name, meth in dct.items():
            if not meth_name.startswith('test_'):
                continue

            doc = getattr(meth, '__doc__', '')
            if not doc or not doc.startswith('{}\n'.format(mcls.doc_prefix)):
                continue

            source, deps = mcls.parse_test(doc, pre='''
                document.write('<div style="color:green;margin-top: 20px">{}</div>');
            '''.format(meth_name))
            all_sources.append(source)
            all_deps.update(deps)

        if not all_sources:
            return

        all_sources.insert(0, '''
            (function() {
                window.print = function() {
                    var args = Array.prototype.slice.call(arguments);
                    document.write(args.join(' '));
                    document.write('<br/>');
                }
                assert.fail = window.print;
            })();
        ''');

        imports, boostrap = mcls.compile_boostrap(all_deps)

        source = '\n;\n'.join(boostrap) + '\n//TESTS\n' + '\n\n'.join(all_sources)

        with open(cls.__name__ + '.html', 'wt') as f:
            f.write(mcls.TEST_HTML.format(source=source))

    def __new__(mcls, name, bases, dct):
        for meth_name, meth in dct.items():
            if not meth_name.startswith('test_'):
                continue

            doc = getattr(meth, '__doc__', '')
            if not doc or not doc.startswith('{}\n'.format(mcls.doc_prefix)):
                continue

            dct[meth_name] = test.skipif(mcls.skipif)(mcls.make_test(meth, doc))

        cls = super().__new__(mcls, name, bases, dct)

        gen_html = 'js.tests.genhtml' in debug.channels
        if gen_html:
            mcls._gen_html(cls, dct)

        return cls

    @classmethod
    def run_v8(mcls, imports, bootstrap, source, expected_output='OK'):
        with tempfile.NamedTemporaryFile('w') as file:
            file.write('(function() {\n\n');
            file.write('function print() { console.log.apply(console, arguments); };\n\n');
            file.write(bootstrap)
            file.write(source)
            file.write('\n\n}).call(global);'); #nodejs: to fix 'this' to point to the global ns
            file.flush()

            result = subprocess.getoutput('{} {}'.format(mcls.v8_executable, file.name)).strip()
            if result != expected_output:
                print()
                markup.dump(imports, header='Imports', trim=False)
                markup.dump_code(source, lexer='javascript', header='Test Source')
                markup.dump_header('Test Execution Trace')
                print(result)

            assert result == expected_output, \
                   'expected {!r} != result {!r}'.format(expected_output, result)

    @classmethod
    def make_test(mcls, meth, doc):
        def do_test(self, name=meth.__name__, source=doc, mcls=mcls):
            mcls.do_test(source, name=meth.__name__)
        functools.update_wrapper(do_test, meth)
        return do_test


class JSFunctionalTest(metaclass=JSFunctionalTestMeta):
    def run_js_test(self, source, data=None):
        mcls = type(type(self))
        if getattr(mcls, 'skipif', False):
            test.skip("no js backend")
        mcls.do_test(source, data=data)


class MetaJSParserTest_Base(type, metaclass=config.ConfigurableMeta):
    skipif = False

    @debug.debug
    def do_test(src, flags):
        jsparser = jsp.JSParser(**flags)

        """LOG [js.extra.parse] Test Source
        markup.dump_code(src, lexer='javascript')
        """

        tree = jsparser.parse(src)
        """LOG [js.extra.parse] Test Source AST (no checking for now)
        markup.dump(tree)
        """

        processed_src = JavascriptSourceGenerator.to_source(tree)
        """LOG [js.extra.parse] Test Processed Source
        markup.dump_code(processed_src, lexer='javascript')
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
                    functools.update_wrapper(do_test, dct[testname])
                    dct[testname] = do_test

                else:
                    dct[testname] = lambda self, src=testfunc.__doc__, flags=flags: \
                                                                    cls.do_test(src, flags)
                    functools.update_wrapper(dct[testname], testfunc)

                    dct[testname] = test.skipif(cls.skipif)(dct[testname])

                dct[testname].__name__ = testname

        return super().__new__(cls, name, bases, dct)


class MetaJSParserTest_Functional(BaseJSFunctionalTestMeta, MetaJSParserTest_Base):
    @debug.debug
    def do_test(src, flags):
        jsparser = jsp.JSParser(**flags)

        with tempfile.NamedTemporaryFile('w') as file:
            file.write('function print(){console.log.apply(console, arguments)};\n' + src)
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
            file.write('function print(){console.log.apply(console, arguments)};\n' + processed_src)
            file.flush()

            processed = subprocess.getoutput("%s %s" %
                                             (MetaJSParserTest_Functional.v8_executable, file.name))

            """LOG [js.parse] Processed Result
            print(result)
            """

        assert processed.replace(' ', '') == result.replace(' ', '')
