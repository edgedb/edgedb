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

from edb.testbase import server as tb

try:
    import asyncpg
except ImportError:
    pass


class TestSQLDataModificationLanguage(tb.SQLQueryTestCase):

    def setUp(self):
        self.stran = self.scon.transaction()
        self.loop.run_until_complete(self.stran.start())
        super().setUp()

    def tearDown(self):
        try:
            self.loop.run_until_complete(self.stran.rollback())
        finally:
            super().tearDown()

    SETUP = [
        """
        create type User;

        create type Document {
          create property title: str {
            create rewrite insert using (.title ++ ' (new)');
            create rewrite update using (.title ++ ' (updated)');
          };
          create multi property keywords: str;

          create link owner: User {
            create property is_author: bool;
          };
          create multi link shared_with: User {
            create property can_edit: bool;
          };
        };

        create type Log {
          create property line: str;
        };

        create type Base {
          create property prop: str {
            create constraint exclusive;
          }
        };

        create type Child extending Base {
          create trigger log_insert_each after insert for each do (
            insert Log { line := 'inserted each ' ++ __new__.prop }
          );
          create trigger log_insert_all after insert for all do (
            insert Log { line := 'inserted all' }
          );
        };
    """
    ]

    async def test_sql_dml_insert_01(self):
        # base case
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Meeting report')
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assertEqual(res, [['Meeting report (new)']])

    async def test_sql_dml_insert_02(self):
        # when columns are not specified, all columns are expected,
        # in alphabetical order:
        # id, __type__, owner, title
        with self.assertRaisesRegex(
            asyncpg.DataError,
            "cannot assign to link '__type__': it is protected",
            # TODO: positions are hard to recover since we don't even know which
            # DML stmt this error is originating from
            # position="30",
        ):
            await self.scon.execute(
                '''
                INSERT INTO "Document" VALUES (NULL, NULL, NULL, 'Report')
                '''
            )
            res = await self.squery_values('SELECT title FROM "Document"')
            self.assertEqual(res, [['Report (new)']])

    async def test_sql_dml_insert_03(self):
        # multiple rows at once
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Report'), ('Briefing')
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assert_data_shape(
            res, tb.bag([['Report (new)'], ['Briefing (new)']])
        )

    async def test_sql_dml_insert_04(self):
        # using arbitrary query instead of VALUES
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title)
            SELECT c FROM (
                SELECT 'Report', 1 UNION ALL SELECT 'Briefing', 2
            ) t(c, x)
            WHERE x >= 2
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assert_data_shape(res, tb.bag([['Briefing (new)']]))

    async def test_sql_dml_insert_05(self):
        # insert link
        await self.scon.execute('INSERT INTO "User" DEFAULT VALUES;')
        await self.scon.execute(
            'INSERT INTO "Document" (owner_id) SELECT id FROM "User" LIMIT 1'
        )
        res = await self.squery_values('SELECT owner_id FROM "Document"')
        self.assert_shape(res, rows=1, columns=1)

        # insert multiple
        await self.scon.execute('INSERT INTO "User" DEFAULT VALUES;')
        await self.scon.execute(
            'INSERT INTO "Document" (owner_id) SELECT id FROM "User"'
        )
        res = await self.squery_values('SELECT owner_id FROM "Document"')
        self.assert_shape(res, rows=3, columns=1)

        # insert a null link
        await self.scon.execute(
            'INSERT INTO "Document" (owner_id) VALUES (NULL)'
        )

        # insert multiple, with nulls
        await self.scon.execute(
            '''
            INSERT INTO "Document" (owner_id) VALUES
                ((SELECT id from "User" LIMIT 1)),
                (NULL)
            '''
        )

    async def test_sql_dml_insert_06(self):
        # insert in a subquery: syntax error
        with self.assertRaisesRegex(
            asyncpg.PostgresSyntaxError,
            'syntax error at or near "INTO"',
            position="61",
        ):
            await self.scon.execute(
                '''
                SELECT * FROM (
                    INSERT INTO "Document" (title) VALUES ('Meeting report')
                )
                '''
            )

    async def test_sql_dml_insert_07(self):
        # insert in a CTE
        await self.scon.execute(
            '''
            WITH a AS (
                INSERT INTO "Document" (title) VALUES ('Meeting report')
            )
            SELECT * FROM a
            '''
        )

    async def test_sql_dml_insert_08(self):
        # insert in a CTE: invalid PostgreSQL
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            'WITH clause containing a data-modifying statement must be at '
            'the top level',
            position="98",
        ):
            await self.scon.execute(
                '''
                WITH a AS (
                    WITH b AS (
                        INSERT INTO "Document" (title) VALUES ('Meeting report')
                    )
                    SELECT * FROM b
                )
                SELECT * FROM a
                '''
            )

    async def test_sql_dml_insert_09(self):
        # insert with a CTE
        await self.scon.execute(
            '''
            WITH a AS (
                SELECT 'Report' as t UNION ALL SELECT 'Briefing'
            )
            INSERT INTO "Document" (title) SELECT * FROM a
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assertEqual(res, tb.bag([['Report (new)'], ['Briefing (new)']]))

    async def test_sql_dml_insert_10(self):
        # two inserts
        await self.scon.execute(
            '''
            WITH a AS (
                INSERT INTO "Document" (title) VALUES ('Report')
                RETURNING title as t
            )
            INSERT INTO "Document" (title) SELECT t || ' - copy' FROM a
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assert_data_shape(
            res, tb.bag([['Report (new)'], ['Report (new) - copy (new)']])
        )

    async def test_sql_dml_insert_11(self):
        await self.scon.execute(
            '''
            WITH a AS (
                INSERT INTO "Document" (title) VALUES ('Report')
            )
            INSERT INTO "Document" (title) VALUES ('Briefing')
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assert_data_shape(
            res, tb.bag([['Report (new)'], ['Briefing (new)']])
        )

    async def test_sql_dml_insert_12(self):
        # returning
        await self.scon.execute('INSERT INTO "User" DEFAULT VALUES;')
        res = await self.scon.fetch(
            '''
            INSERT INTO "Document" (title, owner_id)
            SELECT 'Meeting Report', id FROM "User" LIMIT 1
            RETURNING id, owner_id, LOWER(title) as my_title
            '''
        )
        self.assert_shape(res, rows=1, columns=["id", "owner_id", "my_title"])
        first = res[0]
        self.assertEqual(first[2], 'meeting report (new)')

    async def test_sql_dml_insert_13(self):
        # returning sublink
        await self.scon.execute('INSERT INTO "User" DEFAULT VALUES;')
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title, owner_id)
            SELECT 'Report', id FROM "User" LIMIT 1
            '''
        )

        res = await self.squery_values(
            '''
            INSERT INTO "Document" as subject (title, owner_id)
            VALUES ('Report', NULL), ('Briefing', (SELECT id FROM "User"))
            RETURNING (
                SELECT COUNT(*) FROM "User" WHERE "User".id = owner_id
            ),
            (
                SELECT COUNT(*) FROM "User"
            ),
            (
                SELECT COUNT(*) FROM "Document" AS d
                WHERE subject.title = d.title
            )
            '''
        )
        self.assertEqual(
            res,
            tb.bag(
                [
                    [0, 1, 1],
                    [1, 1, 0],
                ]
            ),
        )

    async def test_sql_dml_insert_14(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            'invalid input syntax for type uuid',
        ):
            await self.scon.execute(
                '''
                INSERT INTO "Document" (title, owner_id)
                VALUES
                    ('Briefing', 'bad uuid')
                '''
            )

    async def test_sql_dml_insert_15(self):
        with self.assertRaisesRegex(
            asyncpg.exceptions.CardinalityViolationError,
            "'default::User' with id '[0-9a-f-]+' does not exist",
        ):
            await self.scon.execute(
                '''
                INSERT INTO "Document" (title, owner_id)
                VALUES
                    ('Report', '343a6c20-2e3b-11ef-8798-ebce402e7d3f')
                '''
            )

    async def test_sql_dml_insert_16(self):
        with self.assertRaisesRegex(
            asyncpg.exceptions.CannotCoerceError,
            'cannot cast type boolean to uuid',
        ):
            await self.scon.execute(
                '''
                INSERT INTO "Document" (title, owner_id)
                VALUES ('Briefing', FALSE)
                '''
            )

    async def test_sql_dml_insert_17(self):
        # default values

        await self.scon.execute(
            '''
            INSERT INTO "Document" DEFAULT VALUES;
            '''
        )

        await self.scon.execute(
            '''
            INSERT INTO "Document" (id, title) VALUES (DEFAULT, 'Report');
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assert_data_shape(res, tb.bag([[None], ['Report (new)']]))

        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            'DEFAULT keyword is supported only when '
            'used for a column in all rows',
        ):
            await self.scon.execute(
                '''
                INSERT INTO "Document" (title) VALUES ('Report'), (DEFAULT);
                '''
            )

    async def test_sql_dml_insert_18(self):
        res = await self.scon.fetch(
            '''
            WITH
                a as (INSERT INTO "Child" (prop) VALUES ('a')),
                b as (INSERT INTO "Child" (prop) VALUES ('b_0'), ('b_1'))
            SELECT line FROM "Log" ORDER BY line;
            '''
        )
        # changes to the database are not visible in the same query
        self.assert_shape(res, 0, 0)

        # so we need to re-select
        res = await self.squery_values('SELECT line FROM "Log" ORDER BY line;')
        self.assertEqual(
            res,
            [
                ["inserted all"],
                ["inserted each a"],
                ["inserted each b_0"],
                ["inserted each b_1"],
            ],
        )

    async def test_sql_dml_insert_19(self):
        # exclusive on base, then insert into base and child
        with self.assertRaisesRegex(
            asyncpg.ExclusionViolationError,
            'duplicate key value violates unique constraint '
            '"[0-9a-f-]+;schemaconstr"',
        ):
            await self.scon.execute(
                '''
                WITH
                    a as (INSERT INTO "Base" (prop) VALUES ('a')),
                    b as (INSERT INTO "Child" (prop) VALUES ('a'))
                SELECT 1
                '''
            )

    async def test_sql_dml_insert_20(self):
        # CommandComplete tag (inserted rows) with no RETURNING

        query = '''
            INSERT INTO "Document" (title) VALUES ('Report'), ('Briefing');
        '''

        # extended (binary) protocol, because fetch
        res = await self.scon.fetch(query)
        # actually, no DataRows are returned, but asyncpg returns [] anyway
        self.assert_shape(res, 0, 0)

        # simple (text) protocol
        res = await self.scon.execute(query)
        self.assertEqual(res, 'INSERT 0 2')

        # extended (binary) protocol because we used args
        query = '''
            INSERT INTO "Document" (title) VALUES ($1), ($2);
        '''
        res = await self.scon.execute(query, 'Report', 'Briefing')
        self.assertEqual(res, 'INSERT 0 2')

    async def test_sql_dml_insert_21(self):
        # CommandComplete tag (inserted rows) with RETURNING

        query = '''
            INSERT INTO "Document" (title) VALUES ('Report'), ('Briefing')
            RETURNING id as my_id;
        '''

        res = await self.scon.fetch(query)
        self.assert_shape(res, rows=2, columns=["my_id"])

        # simple (text) protocol
        res = await self.scon.execute(query)
        self.assertEqual(res, 'INSERT 0 2')

        # extended (binary) protocol because we used args
        query = '''
            INSERT INTO "Document" (title) VALUES ($1), ($2)
            RETURNING id as my_id;
        '''
        res = await self.scon.execute(query, 'Report', 'Briefing')
        self.assertEqual(res, 'INSERT 0 2')
