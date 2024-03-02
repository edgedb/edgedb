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

    async def ensure_schema_data_integrity(self):
        # We can't use _retrying here, since the sequences won't get reset.
        # Hopefully this won't be a problem.
        async with self._run_and_rollback():
            await self._ensure_schema_data_integrity()

    async def _ensure_schema_data_integrity(self):
        await self.assert_query_result(
            r'''
                SELECT Test {
                    array_of_tuples,
                    tuple_of_arrays,
                    seq,
                } FILTER .name = 'test01'
            ''',
            [
                {
                    'array_of_tuples': [
                        [1, '2', 3],
                        [4, '5', 6],
                    ],
                    'tuple_of_arrays': [
                        '1',
                        ['2', '3'],
                        [4, 5, ['6']],
                    ],
                    'seq': 1,
                },
            ]
        )

        result = await self.con.query_single(
            "SELECT sequence_next(INTROSPECT TYPEOF Test.seq)"
        )
        self.assertEqual(result, 2)

        result = await self.con.query_single(
            "SELECT sequence_next(INTROSPECT MyPristineSeq)"
        )
        self.assertEqual(result, 1)


class TestDump03(tb.StableDumpTestCase, DumpTestCaseMixin):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump03_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump03_setup.edgeql')

    async def test_dump03_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump03_zbranch_data(self):
        await self.check_branching(
            include_data=True,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDump03Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dump03',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    pass
