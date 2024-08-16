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


import asyncio
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

            dbname = await conn.query('SELECT sys::get_current_branch();')
            self.assertEqual(dbname, ['mytestdb'])

            with self.assertRaisesRegex(
                    edgedb.ExecutionError,
                    r'cannot drop the currently open database'):
                await conn.execute('DROP DATABASE mytestdb;')

            with self.assertRaisesRegex(
                    edgedb.ExecutionError,
                    r'''branch ["']mytestdb["'] is being '''
                    r'''accessed by other users'''):
                await self.con.execute('DROP DATABASE mytestdb;')

            await conn.aclose()
        finally:
            await tb.drop_db(self.con, 'mytestdb')

    async def test_database_create_02(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'Branch names longer than \d+ '
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
                    r'branch "databasename" already exists'):
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
                r'branch "databasename" does not exist'):
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
            await tb.drop_db(self.con, 'test_db_drop')

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
            await tb.drop_db(self.con, 'test_tpl')

    async def test_branch_create_01(self):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        await self.con.execute('CREATE EMPTY BRANCH mytestdb;')

        try:
            conn = await self.connect(database='mytestdb')

            dbname = await conn.query('SELECT sys::get_current_database();')
            self.assertEqual(dbname, ['mytestdb'])

            with self.assertRaisesRegex(
                    edgedb.ExecutionError,
                    r'cannot drop the currently open database'):
                await conn.execute('DROP BRANCH mytestdb;')

            with self.assertRaisesRegex(
                    edgedb.ExecutionError,
                    r'''branch ["']mytestdb["'] is being '''
                    r'''accessed by other users'''):
                await self.con.execute('DROP BRANCH mytestdb;')

            await conn.aclose()
        finally:
            await tb.drop_db(self.con, 'mytestdb')

    async def test_branch_create_02(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'Branch names longer than \d+ '
                r'characters are not supported'):
            await self.con.execute(
                f'CREATE EMPTY BRANCH mytestdb_{"x" * s_def.MAX_NAME_LENGTH};')

    async def test_branch_create_03(self):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        await self.con.execute('CREATE EMPTY BRANCH databasename;')

        try:
            with self.assertRaisesRegex(
                    edgedb.DuplicateDatabaseDefinitionError,
                    r'branch "databasename" already exists'):
                await self.con.execute('CREATE EMPTY BRANCH databasename;')
        finally:
            await tb.drop_db(self.con, 'databasename')

    async def test_branch_create_04(self):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        # create branch name that conflicts with names in schema
        await self.con.execute('CREATE EMPTY BRANCH range;')

        conn = await self.connect(database='range')

        res = await conn.query('select range(5, 10)')
        self.assertEqual(res, [edgedb.Range(5, 10)])

        await conn.aclose()

        await tb.drop_db(self.con, 'range')

    async def test_branch_drop_01(self):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        with self.assertRaisesRegex(
                edgedb.UnknownDatabaseError,
                r'branch "databasename" does not exist'):
            await self.con.execute('DROP BRANCH databasename;')

    async def test_branch_drop_recreate(self):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        with self.assertRaises(edgedb.UnknownDatabaseError):
            await self.con.execute('DROP BRANCH test_db_drop;')

        await self.con.execute('CREATE EMPTY BRANCH test_db_drop;')
        try:
            conn = await self.connect(database='test_db_drop')

            try:
                dbname = await conn.query(
                    'SELECT sys::get_current_database();')
                self.assertEqual(dbname, ['test_db_drop'])
            finally:
                await conn.aclose()

        finally:
            await tb.drop_db(self.con, 'test_db_drop')

    async def _test_branch_drop_disconnect(
        self, *, with_transaction, with_query
    ):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        await self.con.execute('CREATE EMPTY BRANCH test_db_disconnect;')
        conn = await self.connect(database='test_db_disconnect')
        sleeping = None

        try:
            if with_transaction:
                await conn.query('START TRANSACTION')

            dbname = await conn.query(
                'SELECT sys::get_current_database();')
            self.assertEqual(dbname, ['test_db_disconnect'])

            if with_query:
                await conn.query('select sys::_sleep(0)')
                sleeping = asyncio.create_task(
                    conn.query('select sys::_sleep(3)')
                )
                await asyncio.sleep(1)

            # Drop branch while the frontend connection is active
            await self.con.execute('''
                DROP BRANCH test_db_disconnect FORCE
            ''')

            if with_query:
                try:
                    await sleeping
                except edgedb.EdgeDBError:
                    pass
                sleeping = None

            # The frontend connection should be closed by the server now
            self.assertTrue(conn.is_closed())
        finally:
            if sleeping:
                try:
                    await sleeping
                except Exception:
                    pass

            await conn.aclose()
            try:
                await tb.drop_db(self.con, 'test_db_disconnect')
            except edgedb.UnknownDatabaseError:
                pass

    async def test_branch_drop_disconnect_01(self):
        await self._test_branch_drop_disconnect(
            with_transaction=False, with_query=False,
        )

    async def test_branch_drop_disconnect_02(self):
        await self._test_branch_drop_disconnect(
            with_transaction=True, with_query=False,
        )

    async def test_branch_drop_disconnect_03(self):
        await self._test_branch_drop_disconnect(
            with_transaction=False, with_query=True,
        )

    async def test_branch_drop_disconnect_04(self):
        await self._test_branch_drop_disconnect(
            with_transaction=True, with_query=True,
        )

    async def test_branch_rename_disconnect(self):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        await self.con.execute('CREATE EMPTY BRANCH test_db_rename;')
        conn = await self.connect(database='test_db_rename')

        try:
            dbname = await conn.query(
                'SELECT sys::get_current_database();')
            self.assertEqual(dbname, ['test_db_rename'])

            # Drop branch while the frontend connection is active
            await self.con.execute('''
                ALTER BRANCH test_db_rename FORCE
                RENAME TO test_db_rename2
            ''')

            # The frontend connection should be closed by the server now
            self.assertTrue(conn.is_closed())

            conn2 = await self.connect(database='test_db_rename2')
            dbname = await conn2.query(
                'SELECT sys::get_current_database();')
            self.assertEqual(dbname, ['test_db_rename2'])

        finally:
            await conn.aclose()
            await conn2.aclose()
            try:
                await tb.drop_db(self.con, 'test_db_rename')
            except edgedb.UnknownDatabaseError:
                await tb.drop_db(self.con, 'test_db_rename2')

    async def test_branch_non_exist_template(self):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        with self.assertRaises(edgedb.UnknownDatabaseError):
            await self.con.execute('CREATE DATA BRANCH _dummy FROM test_tpl')

        await self.con.execute('CREATE EMPTY BRANCH test_tpl;')
        try:
            conn = await self.connect(database='test_tpl')

            try:
                dbname = await conn.query(
                    'SELECT sys::get_current_database();')
                self.assertEqual(dbname, ['test_tpl'])
            finally:
                await conn.aclose()

        finally:
            await tb.drop_db(self.con, 'test_tpl')

    async def test_branch_rename_01(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        await self.con.execute('CREATE EMPTY BRANCH mytestdb;')

        name = 'mytestdb'
        conn = None
        try:
            res_old = await self.con.query('''
                SELECT sys::Branch.id filter sys::Branch.name = <str>$0
            ''', name)

            await self.con.execute('''
                ALTER BRANCH mytestdb RENAME TO mytestdb2;
            ''')
            name = 'mytestdb2'

            res_new = await self.con.query('''
                SELECT sys::Branch.id filter sys::Branch.name = <str>$0
            ''', name)
            self.assertEqual(res_old, res_new)

            conn = await self.connect(database=name)

            dbname = await conn.query('SELECT sys::get_current_database();')
            self.assertEqual(dbname, [name])

        finally:
            if conn:
                await conn.aclose()
            await tb.drop_db(self.con, name)

    async def test_branch_alias(self):
        if not self.has_create_database:
            self.skipTest("create database is not supported by the backend")

        name = 'mydbalias'
        await self.con.execute(f'CREATE EMPTY BRANCH {name};')

        try:
            res_old = await self.con.query('''
                SELECT sys::Database.id filter sys::Database.name = <str>$0
            ''', name)
            res_new = await self.con.query('''
                SELECT sys::Branch.id filter sys::Branch.name = <str>$0
            ''', name)
            self.assertEqual(res_old, res_new)

        finally:
            await tb.drop_db(self.con, name)
