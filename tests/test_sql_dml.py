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
from edb.tools import test

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
    """
    ]

    async def test_sql_dml_insert0_1(self):
        # base case
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Meeting report')
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assertEqual(res, [['Meeting report (new)']])

    async def test_sql_dml_insert0_2(self):
        # when columns are not specified, all columns are expected,
        # in alphabetical order:
        # id, __type__, owner, title
        with self.assertRaisesRegex(
            asyncpg.DataError,
            "cannot assign to link '__type__': it is protected",
            position="30",
        ):
            await self.scon.execute(
                '''
                INSERT INTO "Document" VALUES (NULL, NULL, NULL, 'Report')
                '''
            )
            res = await self.squery_values('SELECT title FROM "Document"')
            self.assertEqual(res, [['Report (new)']])

    async def test_sql_dml_insert0_3(self):
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

    async def test_sql_dml_insert0_4(self):
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

    async def test_sql_dml_insert0_5(self):
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

    async def test_sql_dml_insert0_6(self):
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

    async def test_sql_dml_insert0_7(self):
        # insert in a CTE
        await self.scon.execute(
            '''
            WITH a AS (
                INSERT INTO "Document" (title) VALUES ('Meeting report')
            )
            SELECT * FROM a
            '''
        )

    async def test_sql_dml_insert0_8(self):
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

    async def test_sql_dml_insert0_9(self):
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
        self.assertEqual(
            res, tb.bag([['Report (new)'], ['Report (new) - copy (new)']])
        )

    async def test_sql_dml_insert_11(self):
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

    async def test_sql_dml_insert_12(self):
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

    @test.skip('bug 7471 closes connection')
    async def test_sql_dml_insert_13(self):
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

    @test.skip('bug 7471 closes connection')
    async def test_sql_dml_insert_14(self):
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

    @test.skip('bug 7471 closes connection')
    async def test_sql_dml_insert_15(self):
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

    async def test_sql_dml_insert_16(self):
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
