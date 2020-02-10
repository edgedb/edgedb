#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations
from typing import *

import functools
import os
import re
import unittest

from edb.common import context
from edb.common import devmode
from edb.common import markup

from edb import edgeql
from edb.edgeql import ast as qlast
from edb.edgeql import parser as qlparser

from edb.server import defines

from edb.schema import ddl as s_ddl
from edb.schema import delta as sd
from edb.schema import migrations as s_migrations  # noqa
from edb.schema import schema as s_schema
from edb.schema import std as s_std


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

    def assert_equal(
        self,
        expected,
        result,
        *,
        re_filter: Optional[str] = None,
        message: Optional[str] = None
    ) -> None:
        if re_filter is None:
            re_filter = self.re_filter

        if re_filter is not None:
            expected_stripped = re_filter.sub('', expected).lower()
            result_stripped = re_filter.sub('', result).lower()
        else:
            expected_stripped = expected.lower()
            result_stripped = result.lower()

        self.assertEqual(
            expected_stripped,
            result_stripped,
            (f'{message if message else ""}' +
                f'\nexpected:\n{expected}\nreturned:\n{result}')
        )


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


_std_schema = None


def _load_std_schema():
    global _std_schema
    if _std_schema is None:
        std_dirs_hash = devmode.hash_dirs(s_std.CACHE_SRC_DIRS)
        schema = None

        if devmode.is_in_dev_mode():
            schema = devmode.read_dev_mode_cache(
                std_dirs_hash, 'transient-stdschema.pickle')

        if schema is None:
            schema = s_schema.Schema()
            for modname in s_schema.STD_LIB + ('stdgraphql',):
                schema = s_std.load_std_module(schema, modname)

        if devmode.is_in_dev_mode():
            devmode.write_dev_mode_cache(
                schema, std_dirs_hash, 'transient-stdschema.pickle')

        _std_schema = schema

    return _std_schema


class BaseSchemaTest(BaseDocTest):
    SCHEMA = None

    @classmethod
    def setUpClass(cls):
        script = cls.get_schema_script()
        if script is not None:
            cls.schema = cls.run_ddl(_load_std_schema(), script)

    @classmethod
    def run_ddl(cls, schema, ddl, default_module=defines.DEFAULT_MODULE_ALIAS):
        statements = edgeql.parse_block(ddl)

        current_schema = schema
        target_schema = None

        for stmt in statements:
            if isinstance(stmt, qlast.CreateMigration):
                # CREATE MIGRATION
                if target_schema is None:
                    target_schema = _load_std_schema()

                ddl_plan = s_ddl.cmd_from_ddl(
                    stmt, schema=current_schema,
                    modaliases={None: default_module},
                    testmode=True)

                ddl_plan = s_ddl.compile_migration(
                    ddl_plan, target_schema, current_schema)

            elif isinstance(stmt, qlast.Delta):
                # APPLY MIGRATION
                migration_cmd = s_ddl.cmd_from_ddl(
                    stmt, schema=current_schema,
                    modaliases={None: default_module},
                    testmode=True)
                migration = current_schema.get_global(
                    s_migrations.Migration, migration_cmd.classname)
                ddl_plan = migration.get_delta(current_schema)

            elif isinstance(stmt, qlast.DDL):
                # CREATE/DELETE/ALTER (FUNCTION, TYPE, etc)
                ddl_plan = s_ddl.delta_from_ddl(
                    stmt, schema=current_schema,
                    modaliases={None: default_module},
                    testmode=True)

            else:
                raise ValueError(
                    f'unexpected {stmt!r} in compiler setup script')

            context = sd.CommandContext()
            context.testmode = True
            current_schema = ddl_plan.apply(current_schema, context)

        return current_schema

    @classmethod
    def load_schema(cls, source: str, modname: str='test') -> s_schema.Schema:
        target = qlparser.parse_sdl(f'module {modname} {{ {source} }}')
        decls = [
            # The target is a Schema with a single module block. We
            # want to extract the declarations from it.
            (modname, target.declarations[0].declarations)
        ]

        schema = _load_std_schema()
        return s_ddl.apply_sdl(
            decls, target_schema=schema, current_schema=schema)

    @classmethod
    def get_schema_script(cls):
        script = ''

        # look at all SCHEMA entries and potentially create multiple modules
        schema = []
        for name, val in cls.__dict__.items():
            m = re.match(r'^SCHEMA(?:_(\w+))?', name)
            if m:
                module_name = (m.group(1) or 'test').lower().replace(
                    '__', '.')

                if '\n' in val:
                    # Inline schema source
                    module = val
                else:
                    with open(val, 'r') as sf:
                        module = sf.read()

                schema.append(f'\nmodule {module_name} {{ {module} }}')

        script += f'\nCREATE MIGRATION test_migration'
        script += f' TO {{ {"".join(schema)} }};'
        script += f'\nCOMMIT MIGRATION test_migration;'

        return script.strip(' \n')


class BaseSchemaLoadTest(BaseSchemaTest):
    def run_test(self, *, source, spec, expected=None):
        self.load_schema(source)


class BaseEdgeQLCompilerTest(BaseSchemaTest):
    @classmethod
    def get_schema_script(cls):
        script = super().get_schema_script()
        if not script:
            raise ValueError(
                'compiler test cases must define at least one '
                'schema in the SCHEMA[_MODNAME] class attribute.')
        return script
