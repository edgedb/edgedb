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
        dbname = type(self).__name__[4:].lower()

        with tempfile.NamedTemporaryFile() as f:
            self.run_cli('-d', dbname, 'dump', f.name)

            await self.con.execute(f'CREATE DATABASE {dbname}_restored')
            try:
                self.run_cli('-d', f'{dbname}_restored', 'restore', f.name)
                con2 = await self.connect(database=f'{dbname}_restored')
            except Exception:
                await self.con.execute(f'DROP DATABASE {dbname}_restored')
                raise

        oldcon = self.con
        self.__class__.con = con2
        try:
            await self.ensure_schema_data_integrity()
        finally:
            self.__class__.con = oldcon
            await con2.aclose()
            await self.con.execute(f'DROP DATABASE {dbname}_restored')

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
