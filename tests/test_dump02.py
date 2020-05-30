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
import tempfile

from edb.testbase import server as tb


class TestDump02(tb.QueryTestCase, tb.CLITestCaseMixin):

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump02_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump02_setup.edgeql')

    ISOLATED_METHODS = False
    SERIALIZED = True

    async def test_dump02_basic(self):
        await self.ensure_schema_data_integrity()

    async def test_dump02_dump_restore(self):
        assert type(self).__name__.startswith('Test')
        # The name of the database created for this test case by
        # the test runner:
        dbname = f'{type(self).__name__[4:].lower()}'

        with tempfile.NamedTemporaryFile() as f:
            self.run_cli('-d', dbname, 'dump', f.name)

            await self.con.execute(f'CREATE DATABASE `💯{dbname}_restored`')
            try:
                self.run_cli('-d', f'💯{dbname}_restored', 'restore', f.name)
                con2 = await self.connect(database=f'💯{dbname}_restored')
            except Exception:
                await self.con.execute(f'DROP DATABASE `💯{dbname}_restored`')
                raise

        oldcon = self.con
        self.__class__.con = con2
        try:
            await self.ensure_schema_data_integrity()
        finally:
            self.__class__.con = oldcon
            await con2.aclose()
            await self.con.execute(f'DROP DATABASE `💯{dbname}_restored`')

    async def ensure_schema_data_integrity(self):
        tx = self.con.transaction()
        await tx.start()
        try:
            await self._ensure_schema_data_integrity()
        finally:
            await tx.rollback()

    async def _ensure_schema_data_integrity(self):
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
                } FILTER .name LIKE 'default%' AND .is_abstract;
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
                    NOT .is_abstract AND
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
