##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import os
import unittest

from edgedb.lang.common import markup, context


def must_fail(*args, **kwargs):
    def wrap(func):
        _set_spec(func, 'must_fail', (args, kwargs))
        return func
    return wrap


def _set_spec(func, name, attrs):
    try:
        spec = func.test_spec
    except AttributeError:
        spec = func.test_spec = {}

    assert name not in spec
    spec[name] = attrs


class ParserTestMeta(type(unittest.TestCase)):
    def __new__(mcls, name, bases, dct):
        for attr, meth in tuple(dct.items()):
            if attr.startswith('test_') and meth.__doc__:

                @functools.wraps(meth)
                def wrapper(self, meth=meth, doc=meth.__doc__):
                    spec = getattr(meth, 'test_spec', {})
                    spec['test_name'] = meth.__name__

                    if doc:
                        output = error = None

                        source, _, output = doc.partition('\n% OK %')

                        if not output:
                            source, _, error = doc.partition('\n% ERROR %')

                            if not error:
                                output = None
                            else:
                                output = error
                    else:
                        source = output = None

                    self._run_test(source=source, spec=spec, expected=output)

                dct[attr] = wrapper

        return super().__new__(mcls, name, bases, dct)


class BaseParserTest(unittest.TestCase, metaclass=ParserTestMeta):
    parser_debug_flag = ''

    def get_parser(self, *, spec):
        raise NotImplementedError

    def _run_test(self, *, source, spec=None, expected=None):
        if spec and 'must_fail' in spec:
            with self.assertRaises(*spec['must_fail'][0]) as cm:
                return self.run_test(source=source, spec=spec,
                                     expected=expected)

            if cm.exception:
                exc = cm.exception
                for key, val in spec['must_fail'][1].items():
                    if getattr(exc, key) != val:
                        raise exc
            return

        else:
            return self.run_test(source=source, spec=spec, expected=expected)

    def run_test(self, *, source, spec, expected=None):
        raise NotImplementedError


class BaseSyntaxTest(BaseParserTest):
    re_filter = None
    ast_to_source = None
    markup_dump_lexer = None

    def assert_equal(self, expected, result, *, re_filter=None):
        if re_filter is None:
            re_filter = self.re_filter

        if re_filter is not None:
            expected_stripped = re_filter.sub('', expected).lower()
            result_stripped = re_filter.sub('', result).lower()
        else:
            expected_stripped = expected.lower()
            result_stripped = result.lower()

        assert expected_stripped == result_stripped, \
            '[test]expected: {}\n[test] != returned: {}'.format(
                expected, result)

    def run_test(self, *, source, spec, expected=None):
        debug = bool(os.environ.get(self.parser_debug_flag))
        if debug:
            markup.dump_code(source, lexer=self.markup_dump_lexer)

        p = self.get_parser(spec=spec)

        inast = p.parse(source)

        if debug:
            markup.dump(inast)

        # make sure that the AST has context
        #
        context.ContextValidator().visit(inast)

        processed_src = self.ast_to_source(inast)

        if debug:
            markup.dump_code(processed_src, lexer=self.markup_dump_lexer)

        expected_src = source if expected is None else expected

        self.assert_equal(expected_src, processed_src)


class AstValueTest(BaseParserTest):
    def run_test(self, *, source, spec=None, expected=None):
        debug = bool(os.environ.get(self.parser_debug_flag))
        if debug:
            markup.dump_code(source, lexer=self.markup_dump_lexer)

        p = self.get_parser(spec=spec)

        inast = p.parse(source)

        if debug:
            markup.dump(inast)

        for var in inast.definitions[0].variables:
            asttype, val = expected[var.name]
            self.assertIsInstance(var.value, asttype)
            self.assertEqual(var.value.value, val)
