##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import os
import re
import unittest

from edgedb.lang.common import markup, context

from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast

from edgedb.lang.schema import ddl as s_ddl
from edgedb.lang.schema import declarative as s_decl
from edgedb.lang.schema import delta as sd
from edgedb.lang.schema import deltas as s_deltas  # noqa
from edgedb.lang.schema import std as s_std


def must_fail(exc_type, exc_msg_re=None, **kwargs):
    """A decorator to ensure that the test fails with a specific exception.

    If exc_msg_re is passed, assertRaisesRegex will be used to match the
    exception message.

    Example:

        @must_fail(EdgeQLSyntaxError,
                   'non-default argument follows', line=2, col=61)
        def test_edgeql_syntax_1(self):
            ...
    """
    def wrap(func):
        args = (exc_type,)
        if exc_msg_re is not None:
            args += (exc_msg_re,)

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


class DocTestMeta(type(unittest.TestCase)):
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


class BaseDocTest(unittest.TestCase, metaclass=DocTestMeta):
    parser_debug_flag = ''
    re_filter = None

    def _run_test(self, *, source, spec=None, expected=None):
        if spec and 'must_fail' in spec:
            spec_args, spec_kwargs = spec['must_fail']

            if len(spec_args) == 1:
                assertRaises = self.assertRaises
            else:
                assertRaises = self.assertRaisesRegex

            with assertRaises(*spec_args) as cm:
                return self.run_test(source=source, spec=spec,
                                     expected=expected)

            if cm.exception:
                exc = cm.exception
                for attr_name, expected_val in spec_kwargs.items():
                    val = getattr(exc, attr_name)
                    if val != expected_val:
                        raise AssertionError(
                            f'must_fail: attribute {attr_name!r} is '
                            f'{val} (expected is {expected_val!r})') from exc
        else:
            return self.run_test(source=source, spec=spec, expected=expected)

    def run_test(self, *, source, spec, expected=None):
        raise NotImplementedError

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


class BaseSyntaxTest(BaseDocTest):
    ast_to_source = None
    markup_dump_lexer = None

    def get_parser(self, *, spec):
        raise NotImplementedError

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


class AstValueTest(BaseDocTest):
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


class BaseEdgeQLCompilerTest(BaseDocTest):
    SCHEMA = None

    @classmethod
    def setUpClass(cls):
        cls.schema = cls.load_schemas()

    @classmethod
    def load_schemas(cls):
        script = cls.get_schema_script()
        statements = edgeql.parse_block(script)

        schema = s_std.load_std_schema()
        schema = s_std.load_graphql_schema(schema)

        for stmt in statements:
            if isinstance(stmt, qlast.Delta):
                # CREATE/APPLY MIGRATION
                ddl_plan = s_ddl.cmd_from_ddl(stmt, schema=schema)

            elif isinstance(stmt, qlast.DDL):
                # CREATE/DELETE/ALTER (FUNCTION, TYPE, etc)
                ddl_plan = s_ddl.delta_from_ddl(stmt, schema=schema)

            else:
                raise ValueError(
                    f'unexpected {stmt!r} in compiler setup script')

            context = sd.CommandContext()
            ddl_plan.apply(schema, context)

        return schema

    @classmethod
    def get_schema_script(cls):
        if cls.SCHEMA is None:
            raise ValueError(
                'compiler test cases must define at least one'
                ' SCHEMA attribute')

        # Always create the test module.
        script = 'CREATE MODULE test;'

        # look at all SCHEMA entries and potentially create multiple modules
        #
        for name, val in cls.__dict__.items():
            m = re.match(r'^SCHEMA(?:_(\w+))?', name)
            if m:
                module_name = (m.group(1) or 'test').lower().replace(
                    '__', '.')

                with open(val, 'r') as sf:
                    schema = sf.read()

                if module_name != 'test':
                    script += f'\nCREATE MODULE {module_name};'

                script += f'\nCREATE MIGRATION {module_name}::d1'
                script += f' TO eschema $${schema}$$;'
                script += f'\nCOMMIT MIGRATION {module_name}::d1;'

        return script.strip(' \n')


class BaseSchemaTest(BaseDocTest):
    def run_test(self, *, source, spec, expected=None):
        schema = s_std.load_std_schema()
        schema = s_std.load_graphql_schema(schema)
        s_decl.parse_module_declarations(schema, [('test', source)])
