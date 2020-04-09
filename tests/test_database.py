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


import edgedb

from edb.schema import defines as s_def
from edb.testbase import server as tb


class TestDatabase(tb.ConnectedTestCase):
    async def test_database_create_01(self):
        await self.con.execute('CREATE DATABASE mytestdb;')

        try:
            conn = await self.connect(database='mytestdb')

            dbname = await conn.fetchall('SELECT sys::get_current_database();')
            self.assertEqual(dbname, ['mytestdb'])

            with self.assertRaisesRegex(edgedb.ExecutionError,
                                        r'cannot drop the currently open '
                                        r'database'):
                await conn.execute('DROP DATABASE mytestdb;')

            with self.assertRaisesRegex(edgedb.ExecutionError,
                                        r'database "mytestdb" is being '
                                        r'accessed by other users'):
                await self.con.execute('DROP DATABASE mytestdb;')

            await conn.aclose()
        finally:
            await self.con.execute('DROP DATABASE mytestdb;')

    async def test_database_create_02(self):
        with self.assertRaisesRegex(edgedb.SchemaDefinitionError,
                                    r'Database names longer than \d+ '
                                    r'characters are not supported'):
            await self.con.execute(
                f'CREATE DATABASE mytestdb_{"x" * s_def.MAX_NAME_LENGTH};')
