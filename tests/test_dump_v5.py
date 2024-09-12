#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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
        tx = self.con.transaction()
        await tx.start()
        try:
            await self._ensure_schema_integrity()
            if include_data:
                await self._ensure_data_integrity()
        finally:
            await tx.rollback()

    async def _ensure_schema_integrity(self):
        # Validate branch-level config
        await self.assert_query_result(
            r'''
                select cfg::Config.extensions[is ext::pgvector::Config]
                                  .ef_search
            ''',
            [
                5
            ]
        )

        await self.assert_query_result(
            r'''
                select ext::pgvector::Config.ef_search
                filter ext::pgvector::Config.cfg is cfg::DatabaseConfig
            ''',
            [
                5
            ]
        )

    async def _ensure_data_integrity(self):
        await self.assert_query_result(
            r'''
                select count(L2)
            ''',
            [
                999
            ]
        )

        await self.assert_query_result(
            r'''
                with x := range_unpack(range(1, 1000))
                select all(
                    L2.vec in <v3>[x % 10, std::math::ln(x), x / 7 % 13]
                )
            ''',
            [
                True
            ]
        )


class TestDumpV5(tb.StableDumpTestCase, DumpTestCaseMixin):
    EXTENSIONS = ["pgvector", "ai"]
    BACKEND_SUPERUSER = True

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump_v5_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump_v5_setup.edgeql')

    async def test_dump_v5_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump_v5_branch_schema(self):
        await self.check_branching(
            include_data=False,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump_v5_branch_data(self):
        await self.check_branching(
            include_data=True,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDumpV5Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dumpv5',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    BACKEND_SUPERUSER = True
