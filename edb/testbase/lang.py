# mypy: ignore-errors

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

import typing
import functools
import os
import re
import unittest

from edb.common import context
from edb.common import debug
from edb.common import devmode
from edb.common import markup

from edb import buildmeta
from edb import errors
from edb import edgeql
from edb.edgeql import ast as qlast
from edb.edgeql import parser as qlparser
from edb.edgeql import qltypes

from edb.server import defines
from edb.server import compiler as edbcompiler

from edb.schema import ddl as s_ddl
from edb.schema import delta as sd
from edb.schema import migrations as s_migrations  # noqa
from edb.schema import name as sn
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
from edb.schema import std as s_std
from edb.schema import utils as s_utils


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
    re_filter: Optional[typing.Pattern[str]] = None

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
    ast_to_source: Optional[Any] = None
    markup_dump_lexer: Optional[str] = None

    @classmethod
    def get_parser(cls):
        raise NotImplementedError

    def run_test(self, *, source, spec, expected=None):
        debug = bool(os.environ.get(self.parser_debug_flag))
        if debug:
            markup.dump_code(source, lexer=self.markup_dump_lexer)

        p = self.get_parser()

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


class TestCasesSetup:
    def __init__(self, parsers: List[qlparser.EdgeQLParserBase]) -> None:
        self.parsers = parsers


def get_test_cases_setup(
    cases: Iterable[unittest.TestCase],
) -> Optional[TestCasesSetup]:
    parsers: List[qlparser.EdgeQLParserBase] = []

    for case in cases:
        if not hasattr(case, 'get_parser'):
            continue

        parser = case.get_parser()
        if not parser:
            continue

        parsers.append(parser)

    if not parsers:
        return None
    else:
        return TestCasesSetup(parsers)


def run_test_cases_setup(setup: TestCasesSetup, jobs: int) -> None:
    qlparser.preload(
        parsers=setup.parsers,
        allow_rebuild=True,
        paralellize=jobs > 1,
    )


class AstValueTest(BaseDocTest):
    def run_test(self, *, source, spec=None, expected=None):
        debug = bool(os.environ.get(self.parser_debug_flag))
        if debug:
            markup.dump_code(source, lexer=self.markup_dump_lexer)

        p = self.get_parser()

        inast = p.parse(source)

        if debug:
            markup.dump(inast)

        for var in inast.definitions[0].variables:
            asttype, val = expected[var.name]
            self.assertIsInstance(var.value, asttype)
            self.assertEqual(var.value.value, val)


_std_schema = None
_refl_schema = None
_schema_class_layout = None


def _load_std_schema():
    global _std_schema
    if _std_schema is None:
        std_dirs_hash = buildmeta.hash_dirs(s_std.CACHE_SRC_DIRS)
        schema = None

        if devmode.is_in_dev_mode():
            schema = buildmeta.read_data_cache(
                std_dirs_hash, 'transient-stdschema.pickle')

        if schema is None:
            schema = s_schema.FlatSchema()
            for modname in [*s_schema.STD_SOURCES, sn.UnqualName('_testmode')]:
                schema = s_std.load_std_module(schema, modname)
            schema, _ = s_std.make_schema_version(schema)
            schema, _ = s_std.make_global_schema_version(schema)

        if devmode.is_in_dev_mode():
            buildmeta.write_data_cache(
                schema, std_dirs_hash, 'transient-stdschema.pickle')

        _std_schema = schema

    return _std_schema


def _load_reflection_schema():
    global _refl_schema
    global _schema_class_layout

    if _refl_schema is None:
        std_dirs_hash = buildmeta.hash_dirs(s_std.CACHE_SRC_DIRS)

        cache = None
        if devmode.is_in_dev_mode():
            cache = buildmeta.read_data_cache(
                std_dirs_hash, 'transient-reflschema.pickle')

        if cache is not None:
            reflschema, classlayout = cache
        else:
            std_schema = _load_std_schema()
            reflection = s_refl.generate_structure(std_schema)
            classlayout = reflection.class_layout
            context = sd.CommandContext()
            context.stdmode = True
            reflschema = reflection.intro_schema_delta.apply(
                std_schema, context)

            if devmode.is_in_dev_mode():
                buildmeta.write_data_cache(
                    (reflschema, classlayout),
                    std_dirs_hash,
                    'transient-reflschema.pickle',
                )

        _refl_schema = reflschema
        _schema_class_layout = classlayout

    return _refl_schema, _schema_class_layout


def new_compiler():
    std_schema = _load_std_schema()
    refl_schema, layout = _load_reflection_schema()

    return edbcompiler.new_compiler(
        std_schema=std_schema,
        reflection_schema=refl_schema,
        schema_class_layout=layout,
    )


class BaseSchemaTest(BaseDocTest):
    DEFAULT_MODULE = 'default'
    SCHEMA: Optional[str] = None

    schema: s_schema.Schema

    @classmethod
    def setUpClass(cls):
        script = cls.get_schema_script()
        if script is not None:
            cls.schema = cls.run_ddl(_load_std_schema(), script)
        else:
            cls.schema = _load_std_schema()

    @classmethod
    def run_ddl(cls, schema, ddl, default_module=defines.DEFAULT_MODULE_ALIAS):
        statements = edgeql.parse_block(ddl)

        current_schema = schema
        target_schema = None
        migration_schema = None
        migration_target = None
        migration_script = []

        for stmt in statements:
            if isinstance(stmt, qlast.StartMigration):
                # START MIGRATION
                if target_schema is None:
                    target_schema = _load_std_schema()

                migration_target = s_ddl.apply_sdl(
                    stmt.target,
                    base_schema=target_schema,
                    current_schema=current_schema,
                    testmode=True,
                )

                migration_schema = current_schema

                ddl_plan = None

            elif isinstance(stmt, qlast.PopulateMigration):
                # POPULATE MIGRATION
                if migration_target is None:
                    raise errors.QueryError(
                        'unexpected POPULATE MIGRATION:'
                        ' not currently in a migration block',
                        context=stmt.context,
                    )

                migration_diff = s_ddl.delta_schemas(
                    migration_schema,
                    migration_target,
                )

                if debug.flags.delta_plan:
                    debug.header('Populate Migration Diff')
                    debug.dump(migration_diff, schema=schema)

                new_ddl = s_ddl.ddlast_from_delta(
                    migration_schema,
                    migration_target,
                    migration_diff,
                )

                migration_script.extend(new_ddl)

                if debug.flags.delta_plan:
                    debug.header('Populate Migration DDL AST')
                    text = []
                    for cmd in new_ddl:
                        debug.dump(cmd)
                        text.append(edgeql.generate_source(cmd, pretty=True))
                    debug.header('Populate Migration DDL Text')
                    debug.dump_code(';\n'.join(text) + ';')

            elif isinstance(stmt, qlast.DescribeCurrentMigration):
                # This is silly, and we don't bother doing all the work,
                # but try to catch when doing the JSON thing wouldn't work.
                if stmt.language is qltypes.DescribeLanguage.JSON:
                    guided_diff = s_ddl.delta_schemas(
                        migration_schema,
                        migration_target,
                        generate_prompts=True,
                    )
                    s_ddl.statements_from_delta(
                        schema,
                        migration_target,
                        guided_diff,
                    )

            elif isinstance(stmt, qlast.CommitMigration):
                if migration_target is None:
                    raise errors.QueryError(
                        'unexpected COMMIT MIGRATION:'
                        ' not currently in a migration block',
                        context=stmt.context,
                    )

                last_migration = current_schema.get_last_migration()
                if last_migration:
                    last_migration_ref = s_utils.name_to_ast_ref(
                        last_migration.get_name(current_schema),
                    )
                else:
                    last_migration_ref = None

                create_migration = qlast.CreateMigration(
                    body=qlast.NestedQLBlock(commands=migration_script),
                    parent=last_migration_ref,
                )

                ddl_plan = s_ddl.delta_from_ddl(
                    create_migration,
                    schema=migration_schema,
                    modaliases={None: default_module},
                    testmode=True,
                )

                if debug.flags.delta_plan:
                    debug.header('Delta Plan')
                    debug.dump(ddl_plan, schema=schema)

                migration_schema = None
                migration_target = None
                migration_script = []

            elif isinstance(stmt, qlast.DDL):
                if migration_target is not None:
                    migration_script.append(stmt)
                    ddl_plan = None
                else:
                    ddl_plan = s_ddl.delta_from_ddl(
                        stmt,
                        schema=current_schema,
                        modaliases={None: default_module},
                        testmode=True,
                    )

                    if debug.flags.delta_plan:
                        debug.header('Delta Plan')
                        debug.dump(ddl_plan, schema=schema)
            else:
                raise ValueError(
                    f'unexpected {stmt!r} in compiler setup script')

            if ddl_plan is not None:
                context = sd.CommandContext()
                context.testmode = True
                current_schema = ddl_plan.apply(current_schema, context)

        return current_schema

    @classmethod
    def load_schema(
            cls, source: str, modname: Optional[str]=None) -> s_schema.Schema:
        if not modname:
            modname = cls.DEFAULT_MODULE
        sdl_schema = qlparser.parse_sdl(f'module {modname} {{ {source} }}')
        schema = _load_std_schema()
        return s_ddl.apply_sdl(
            sdl_schema,
            base_schema=schema,
            current_schema=schema,
        )

    @classmethod
    def get_schema_script(cls):
        script = ''

        # look at all SCHEMA entries and potentially create multiple modules
        schema = []
        for name, val in cls.__dict__.items():
            m = re.match(r'^SCHEMA(?:_(\w+))?', name)
            if m:
                module_name = (m.group(1)
                               or 'default').lower().replace('__', '.')

                if '\n' in val:
                    # Inline schema source
                    module = val
                else:
                    with open(val, 'r') as sf:
                        module = sf.read()

                schema.append(f'\nmodule {module_name} {{ {module} }}')

        if schema:
            script += f'\nSTART MIGRATION'
            script += f' TO {{ {"".join(schema)} }};'
            script += f'\nPOPULATE MIGRATION;'
            script += f'\nCOMMIT MIGRATION;'

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
