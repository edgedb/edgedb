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

import edgedb

from edb.testbase import server as tb


class DumpTestCaseMixin:

    async def ensure_schema_data_integrity(self):
        tx = self.con.transaction()
        await tx.start()
        try:
            await self._ensure_schema_data_integrity()
        finally:
            await tx.rollback()

    async def _ensure_schema_data_integrity(self):
        # Validate access policies
        await self.assert_query_result(
            r'''
            SELECT schema::ObjectType {access_policies: {name}}
            FILTER .name = 'default::Test2';
            ''',
            [
                {'access_policies': [{'name': 'test'}]},
            ],
        )

        # Validate globals
        await self.assert_query_result(
            r'''
            SELECT schema::Global {
                name, tgt := .target.name, required, default
            }
            ORDER BY .name
            ''',
            [
                {
                    'name': 'default::bar',
                    'tgt': 'std::int64',
                    'required': True,
                    'default': '-1',
                },
                {
                    'name': 'default::baz',
                    'tgt': 'default::baz',
                    'required': False,
                    'default': None,
                },
                {
                    'name': 'default::foo',
                    'tgt': 'std::str',
                    'required': False,
                    'default': None,
                },
            ],
        )

        # Test that on source delete all work correctly still
        await self.con.execute(r'DELETE SourceA FILTER .name = "s0"')

        await self.assert_query_result(
            r'''
            SELECT TargetA {name}
            FILTER .name = 't0';
            ''',
            [],
        )

        # Should trigger a cascade that then causes a link policy error
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'prohibited by link target policy'):
            async with self.con.transaction():
                await self.con.execute(r'DELETE SourceA FILTER .name = "s1"')

        # Shouldn't delete anything
        await self.con.execute(r'DELETE SourceA FILTER .name = "s3"')
        await self.assert_query_result(
            r'''
            SELECT TargetA {name}
            FILTER .name = 't2';
            ''',
            [{'name': 't2'}],
        )

        # But deleting the last reamining one should
        await self.con.execute(r'DELETE SourceA FILTER .name = "s4"')
        await self.assert_query_result(
            r'''
            SELECT TargetA {name}
            FILTER .name = 't2';
            ''',
            [],
        )


class TestDump04(tb.StableDumpTestCase, DumpTestCaseMixin):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump04_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump04_setup.edgeql')

    STABLE_DUMP = False

    async def test_dump04_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDump04Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dump04',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    pass
