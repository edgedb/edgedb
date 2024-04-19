#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


import os.path

from edb.testbase import server as tb


class DumpTestCaseMixin:

    async def ensure_schema_data_integrity(self, include_data=True):
        async for tx in self._run_and_rollback_retrying():
            async with tx:
                await self._ensure_schema_integrity()
                if include_data:
                    await self._ensure_data_integrity()

    async def _ensure_schema_integrity(self):
        # Check that index exists
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr,
                    },
                    properties: {
                        name,
                        default,
                    } FILTER .name != 'id',
                } FILTER .name = 'default::Łukasz';
            ''',
            [
                {
                    'name': 'default::Łukasz',
                    'indexes': [{
                        'expr': '.`Ł🤞`'
                    }],
                }
            ]
        )

        # Check that scalar types exist
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT (
                    SELECT ScalarType {
                        name,
                    } FILTER .name LIKE 'default%'
                ).name;
            ''',
            {
                'default::你好',
                'default::مرحبا',
                'default::🚀🚀🚀',
            }
        )

        # Check that abstract constraint exists
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Constraint {
                    name,
                } FILTER .name LIKE 'default%' AND .abstract;
            ''',
            [
                {'name': 'default::🚀🍿'},
            ]
        )

        # Check that abstract constraint was applied properly
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Constraint {
                    name,
                    params: {
                        @value
                    } FILTER .num > 0
                }
                FILTER
                    .name = 'default::🚀🍿' AND
                    NOT .abstract AND
                    Constraint.<constraints[IS ScalarType].name =
                        'default::🚀🚀🚀';
            ''',
            [
                {
                    'name': 'default::🚀🍿',
                    'params': [
                        {'@value': '100'}
                    ]
                },
            ]
        )

    async def _ensure_data_integrity(self):
        await self.assert_query_result(
            r'''
                SELECT A {
                    `s p A m 🤞`: {
                        `🚀`,
                        c100,
                        c101 := `💯`(`🙀` := .`🚀` + 1)
                    }
                }
            ''',
            [
                {
                    's p A m 🤞': {
                        '🚀': 42,
                        'c100': 58,
                        'c101': 57,
                    }
                }
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT Łukasz {
                    `Ł🤞`,
                    `Ł💯`: {
                        @`🙀🚀🚀🚀🙀`,
                        @`🙀مرحبا🙀`,
                        `s p A m 🤞`: {
                            `🚀`,
                            c100,
                            c101 := `💯`(`🙀` := .`🚀` + 1)
                        }
                    }
                } ORDER BY .`Ł💯` EMPTY LAST
            ''',
            [
                {
                    'Ł🤞': 'simple 🚀',
                    'Ł💯': {
                        '@🙀🚀🚀🚀🙀': None,
                        '@🙀مرحبا🙀': None,
                        's p A m 🤞': {
                            '🚀': 42,
                            'c100': 58,
                            'c101': 57,
                        }
                    }
                },
                {
                    'Ł🤞': '你好🤞',
                    'Ł💯': None,
                },
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT `💯💯💯`::`🚀🙀🚀`('Łink prop 🙀مرحبا🙀');
            ''',
            [
                'Łink prop 🙀مرحبا🙀Ł🙀',
            ]
        )

        # Check that annotation exists
        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Function {
                    name,
                    annotations: {
                        name,
                        @value
                    },
                } FILTER .name = 'default::💯';
            ''',
            [
                {
                    'name': 'default::💯',
                    'annotations': [{
                        'name': 'default::🍿',
                        '@value': 'fun!🚀',
                    }]
                }
            ]
        )

        # Check the default value
        await self.con.execute(r'INSERT Łukasz')
        await self.assert_query_result(
            r'''
                SELECT Łukasz {
                    `Ł🤞`,
                } FILTER NOT EXISTS .`Ł💯`;
            ''',
            [
                # We had one before and expect one more now.
                {'Ł🤞': '你好🤞'},
                {'Ł🤞': '你好🤞'},
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT count(schema::Migration) >= 2
            ''',
            [True],
        )


class TestDump02(tb.StableDumpTestCase, DumpTestCaseMixin):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump02_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump02_setup.edgeql')

    TEARDOWN = '''
        CONFIGURE CURRENT DATABASE RESET allow_dml_in_functions;
    '''

    @classmethod
    def get_setup_script(cls):
        script = (
            'CONFIGURE CURRENT DATABASE SET allow_dml_in_functions := true;\n'
        )
        return script + super().get_setup_script()

    async def test_dump02_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump02_branch_schema(self):
        await self.check_branching(
            include_data=False,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump02_branch_data(self):
        await self.check_branching(
            include_data=True,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDump02Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dump02',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    @classmethod
    def tearDownClass(cls):
        try:
            cls.loop.run_until_complete(cls.con.execute('''
                CONFIGURE CURRENT DATABASE RESET allow_dml_in_functions;
            '''))
        finally:
            super().tearDownClass()
