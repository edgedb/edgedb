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
    TRANSACTION_ISOLATION = False
    PARALLELISM_GRANULARITY = 'suite'

    async def test_database_create_01(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        await self.con.execute('CREATE DATABASE mytestdb;')

        try:
            conn = await self.connect(database='mytestdb')

            dbname = await conn.query('SELECT sys::get_current_database();')
            self.assertEqual(dbname, ['mytestdb'])

            with self.assertRaisesRegex(
                    edgedb.ExecutionError,
                    r'cannot drop the currently open database'):
                await conn.execute('DROP DATABASE mytestdb;')

            with self.assertRaisesRegex(
                    edgedb.ExecutionError,
                    r'''database ["']mytestdb["'] is being '''
                    r'''accessed by other users'''):
                await self.con.execute('DROP DATABASE mytestdb;')

            await conn.aclose()
        finally:
            await tb.drop_db(self.con, 'mytestdb')

    async def test_database_create_02(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'Database names longer than \d+ '
                r'characters are not supported'):
            await self.con.execute(
                f'CREATE DATABASE mytestdb_{"x" * s_def.MAX_NAME_LENGTH};')

    async def test_database_create_03(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        await self.con.execute('CREATE DATABASE databasename;')

        try:
            with self.assertRaisesRegex(
                    edgedb.DuplicateDatabaseDefinitionError,
                    r'database "databasename" already exists'):
                await self.con.execute('CREATE DATABASE databasename;')
        finally:
            await tb.drop_db(self.con, 'databasename')

    async def test_database_create_04(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        # create database name that conflicts with names in schema
        await self.con.execute('CREATE DATABASE range;')

        conn = await self.connect(database='range')

        res = await conn.query('select range(5, 10)')
        self.assertEqual(res, [edgedb.Range(5, 10)])

        await conn.aclose()

        await tb.drop_db(self.con, 'range')

    async def test_database_drop_01(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        with self.assertRaisesRegex(
                edgedb.UnknownDatabaseError,
                r'database "databasename" does not exist'):
            await self.con.execute('DROP DATABASE databasename;')

    async def test_database_drop_recreate(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        with self.assertRaises(edgedb.UnknownDatabaseError):
            await self.con.execute('DROP DATABASE test_db_drop;')

        await self.con.execute('CREATE DATABASE test_db_drop;')
        try:
            conn = await self.connect(database='test_db_drop')

            try:
                dbname = await conn.query(
                    'SELECT sys::get_current_database();')
                self.assertEqual(dbname, ['test_db_drop'])
            finally:
                await conn.aclose()

        finally:
            await self.con.execute('DROP DATABASE test_db_drop;')

    async def test_database_non_exist_template(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        with self.assertRaises(edgedb.UnknownDatabaseError):
            await self.con.execute('CREATE DATABASE _dummy FROM test_tpl')

        await self.con.execute('CREATE DATABASE test_tpl;')
        try:
            conn = await self.connect(database='test_tpl')

            try:
                dbname = await conn.query(
                    'SELECT sys::get_current_database();')
                self.assertEqual(dbname, ['test_tpl'])
            finally:
                await conn.aclose()

        finally:
            await self.con.execute('DROP DATABASE test_tpl;')
