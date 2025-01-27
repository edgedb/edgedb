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

import uuid

from edb.testbase import server as tb
from edb.tools import test

try:
    import asyncpg
except ImportError:
    pass


class TestSQLDataModificationLanguage(tb.SQLQueryTestCase):

    SETUP = [
        """
        create type User;
        create type Asdf { create link user -> User };

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

        create type Post {
          create property title: str {
            set default := 'untitled';
          };
          create property created_at: datetime;
          create property content: str {
            set default := 'This page intentionally left blank';
          }
        };

        create global y: str;
        create type Globals {
          create property gy: str {
            set default := global y;
          };
        };

        create type Document2 {
            create required property title: str;
            create link owner: User;
        };

        create type Numbered {
            create required property num_id: int64;
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

        await self.scon.execute("SET LOCAL allow_user_specified_id TO TRUE")
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
            "object type default::User with id '[0-9a-f-]+' does not exist",
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

    async def test_sql_dml_insert_17a(self):
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

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Report2'), (DEFAULT);
            '''
        )
        res = await self.squery_values('SELECT title FROM "Document"')
        self.assert_data_shape(
            res,
            tb.bag([
                [None],
                [None],
                ['Report (new)'],
                ['Report2 (new)'],
            ]),
        )

        await self.scon.execute(
            '''
            INSERT INTO "Post" (title) VALUES ('post'), (DEFAULT);
            '''
        )
        res = await self.squery_values('SELECT title FROM "Post"')
        self.assert_data_shape(
            res,
            tb.bag([
                ['post'],
                ['untitled'],
            ]),
        )

    async def test_sql_dml_insert_17b(self):
        # more default values
        await self.scon.execute(
            '''
            INSERT INTO "Post" (id, title, content) VALUES
              (DEFAULT, 'foo', 'bar'),
              (DEFAULT, 'post', DEFAULT),
              (DEFAULT, DEFAULT, 'content'),
              (DEFAULT, DEFAULT, DEFAULT);
            '''
        )
        res = await self.squery_values('SELECT title, content FROM "Post"')
        self.assert_data_shape(
            res,
            tb.bag([
                ['foo', 'bar'],
                ['post', 'This page intentionally left blank'],
                ['untitled', 'content'],
                ['untitled', 'This page intentionally left blank'],
            ]),
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

    async def test_sql_dml_insert_22(self):
        # insert into link table

        query = '''
            INSERT INTO "Document" (title) VALUES ('Report'), ('Briefing')
            RETURNING id;
        '''
        documents = await self.squery_values(query)

        query = '''
            WITH
            u1 AS (INSERT INTO "User" DEFAULT VALUES RETURNING id),
            u2 AS (INSERT INTO "User" DEFAULT VALUES RETURNING id)
            SELECT id from u1 UNION ALL SELECT id from u2
        '''
        users = await self.squery_values(query)

        res = await self.scon.execute(
            '''
            INSERT INTO "Document.shared_with" (source, target)
            VALUES ($1, $2)
            ''',
            documents[0][0],
            users[0][0],
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.scon.execute(
            '''
            INSERT INTO "Document.shared_with" (source, target)
            VALUES ($1, $2), ($1, $3)
            ''',
            documents[1][0],
            users[0][0],
            users[1][0],
        )
        self.assertEqual(res, 'INSERT 0 2')

    async def test_sql_dml_insert_24(self):
        # insert into link table, link properties

        documents = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            '''
        )
        users = await self.squery_values(
            '''
            INSERT INTO "User" DEFAULT VALUES RETURNING id
            '''
        )

        res = await self.scon.execute(
            '''
            WITH t(doc, usr) as (VALUES ($1::uuid, $2::uuid))
            INSERT INTO "Document.shared_with" (source, target, can_edit)
            SELECT doc, usr, TRUE FROM t
            ''',
            documents[0][0],
            users[0][0],
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.squery_values(
            'SELECT can_edit FROM "Document.shared_with"'
        )
        self.assertEqual(res, [[True]])

    async def test_sql_dml_insert_25(self):
        # insert into link table, returning

        documents = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            '''
        )
        users = await self.squery_values(
            '''
            INSERT INTO "User" DEFAULT VALUES RETURNING id
            '''
        )

        res = await self.squery_values(
            '''
            INSERT INTO "Document.shared_with"
            VALUES ($1, $2, FALSE)
            RETURNING source, target, not can_edit
            ''',
            documents[0][0],
            users[0][0],
        )
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], documents[0][0])
        self.assertEqual(res[0][1], users[0][0])
        self.assertEqual(res[0][2], True)

    async def test_sql_dml_insert_26(self):
        # insert into single link table

        documents = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            '''
        )
        users = await self.squery_values(
            '''
            INSERT INTO "User" DEFAULT VALUES RETURNING id
            '''
        )

        res = await self.squery_values(
            '''
            INSERT INTO "Document.owner"
            VALUES ($1, $2, FALSE)
            RETURNING source, target, not is_author
            ''',
            documents[0][0],
            users[0][0],
        )
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], documents[0][0])
        self.assertEqual(res[0][1], users[0][0])
        self.assertEqual(res[0][2], True)

    async def test_sql_dml_insert_27(self):
        with self.assertRaisesRegex(
            asyncpg.PostgresError,
            'column source is required when inserting into link tables',
        ):
            await self.squery_values(
                '''
                INSERT INTO "Document.shared_with" (target, can_edit)
                VALUES ('uuid 1'::uuid, FALSE)
                ''',
            )
        with self.assertRaisesRegex(
            asyncpg.PostgresError,
            'column target is required when inserting into link tables',
        ):
            await self.squery_values(
                '''
                INSERT INTO "Document.shared_with" (source, can_edit)
                VALUES ('uuid 1'::uuid, FALSE)
                ''',
            )

    async def test_sql_dml_insert_28(self):
        documents = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            '''
        )
        res = await self.squery_values(
            '''
            INSERT INTO "Document.keywords"
            VALUES ($1, 'notes')
            RETURNING source, target
            ''',
            documents[0][0],
        )
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], documents[0][0])
        self.assertEqual(res[0][1], 'notes')

        res = await self.scon.execute(
            '''
            INSERT INTO "Document.keywords" (source, target)
            VALUES ($1, 'priority'), ($1, 'recent')
            ''',
            documents[0][0],
        )
        self.assertEqual(res, 'INSERT 0 2')

    async def test_sql_dml_insert_29(self):
        res = await self.scon.execute(
            '''
            INSERT INTO "User" (id) VALUES (DEFAULT), (DEFAULT)
            '''
        )
        self.assertEqual(res, 'INSERT 0 2')

    async def test_sql_dml_insert_30(self):
        # params
        users = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id;'
        )
        res = await self.squery_values(
            '''
            INSERT INTO "Document" (title, owner_id)
            VALUES ('Report', $1), ('Briefing', NULL)
            RETURNING title, owner_id
            ''',
            users[0][0],
        )
        self.assertEqual(
            res,
            [
                ['Report (new)', users[0][0]],
                ['Briefing (new)', None],
            ],
        )

    async def test_sql_dml_insert_31(self):
        res = await self.squery_values(
            '''
            WITH u as (
                INSERT INTO "User" (id) VALUES (DEFAULT), (DEFAULT) RETURNING id
            )
            INSERT INTO "Document" (title, owner_id)
            SELECT 'Report', u.id FROM u
            RETURNING title, owner_id
            '''
        )
        self.assertEqual(
            res, [['Report (new)', res[0][1]], ['Report (new)', res[1][1]]]
        )

    async def test_sql_dml_insert_32(self):
        with self.assertRaisesRegex(
            asyncpg.PostgresError,
            'cannot write into table "columns"',
        ):
            await self.squery_values(
                '''
                INSERT INTO information_schema.columns DEFAULT VALUES
                '''
            )

    async def test_sql_dml_insert_33(self):
        # TODO: error message should say `owner_id` not `owner`
        with self.assertRaisesRegex(
            asyncpg.PostgresError,
            'Expected 2 columns \\(title, owner\\), but got 1',
        ):
            await self.squery_values(
                '''
                INSERT INTO "Document" (title, owner_id)
                VALUES ('Report'), ('Report'), ('Briefing')
                '''
            )

    async def test_sql_dml_insert_34(self):
        id1 = uuid.uuid4()
        id2 = uuid.uuid4()
        id3 = uuid.uuid4()
        id4 = uuid.uuid4()
        id5 = uuid.uuid4()

        await self.scon.execute("SET LOCAL allow_user_specified_id TO TRUE")

        res = await self.squery_values(
            f'''
            INSERT INTO "Document" (id)
            VALUES ($1), ('{id2}')
            RETURNING id
            ''',
            id1,
        )
        self.assertEqual(res, [[id1], [id2]])

        res = await self.squery_values(
            f'''
            INSERT INTO "Document" (id)
            SELECT id FROM (VALUES ($1::uuid), ('{id4}')) t(id)
            RETURNING id
            ''',
            id3,
        )
        self.assertEqual(res, [[id3], [id4]])

        res = await self.squery_values(
            f'''
            INSERT INTO "Document" (id)
            VALUES ($1)
            RETURNING id
            ''',
            id5,
        )
        self.assertEqual(res, [[id5]])

    async def test_sql_dml_insert_35(self):
        with self.assertRaisesRegex(
            asyncpg.exceptions.DataError,
            "cannot assign to property 'id'",
        ):
            res = await self.squery_values(
                f'''
                INSERT INTO "Document" (id) VALUES ($1) RETURNING id
                ''',
                uuid.uuid4(),
            )

        await self.scon.execute('SET LOCAL allow_user_specified_id TO TRUE')
        id = uuid.uuid4()
        res = await self.squery_values(
            f'''
            INSERT INTO "Document" (id) VALUES ($1) RETURNING id
            ''',
            id,
        )
        self.assertEqual(res, [[id]])

    async def test_sql_dml_insert_36(self):
        [user] = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id'
        )

        res = await self.scon.execute(
            '''
            WITH d AS (
              INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            )
            INSERT INTO "Document.shared_with" (source, target, can_edit)
            SELECT d.id, $1, TRUE FROM d
            ''',
            user[0],
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.squery_values(
            'SELECT can_edit FROM "Document.shared_with"'
        )
        self.assertEqual(res, [[True]])

    async def test_sql_dml_insert_37(self):
        [doc] = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            '''
        )

        res = await self.scon.execute(
            '''
            WITH u AS (
              INSERT INTO "User" DEFAULT VALUES RETURNING id
            )
            INSERT INTO "Document.shared_with" (source, target, can_edit)
            SELECT $1, u.id, TRUE FROM u
            ''',
            doc[0],
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.squery_values(
            'SELECT can_edit FROM "Document.shared_with"'
        )
        self.assertEqual(res, [[True]])

    async def test_sql_dml_insert_38(self):
        res = await self.scon.execute(
            '''
            WITH
            d AS (
              INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            ),
            u AS (
              INSERT INTO "User" DEFAULT VALUES RETURNING id
            )
            INSERT INTO "Document.shared_with" (source, target, can_edit)
            SELECT d.id, u.id, TRUE FROM d, u
            '''
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.squery_values(
            'SELECT can_edit FROM "Document.shared_with"'
        )
        self.assertEqual(res, [[True]])

    async def test_sql_dml_insert_39(self):
        res = await self.scon.execute(
            '''
            WITH d AS (
              INSERT INTO "User" DEFAULT VALUES RETURNING id
            )
            INSERT INTO "Asdf" (user_id)
            SELECT d.id FROM d
            ''',
        )
        self.assertEqual(res, 'INSERT 0 1')

    async def test_sql_dml_insert_40(self):
        await self.squery_values(
            f'''
            INSERT INTO "User" DEFAULT VALUES
            '''
        )

        res = await self.scon.execute(
            f'''
            INSERT INTO "Document2" (title, owner_id)
            VALUES ('Raven', (select id FROM "User" LIMIT 1))
            '''
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.squery_values(
            '''
            SELECT title FROM "Document2"
            '''
        )
        self.assertEqual(res, [['Raven']])

    async def test_sql_dml_insert_41(self):
        res = await self.squery_values(
            f'''
            WITH
            u1 as (
                INSERT INTO "User" DEFAULT VALUES RETURNING id
            )
            INSERT INTO "Document" (owner_id, title)
            VALUES ((SELECT id FROM u1), 'hello')
            RETURNING title
            '''
        )
        self.assertEqual(res, [['hello (new)']])

    async def test_sql_dml_insert_42(self):
        await self.scon.execute("SET LOCAL allow_user_specified_id TO TRUE")

        uuid1 = uuid.uuid4()
        uuid2 = uuid.uuid4()
        res = await self.scon.execute(
            f'''
            WITH
            u1 as (
                INSERT INTO "User" DEFAULT VALUES RETURNING id, 'hello' as x
            ),
            u2 as (
                INSERT INTO "User" (id) VALUES ($1), ($2)
                RETURNING id, 'world' as y
            )
            INSERT INTO "Document" (owner_id, title)
            VALUES
                ((SELECT id FROM u1), (SELECT x FROM u1)),
                ((SELECT id FROM u2 LIMIT 1), (SELECT y FROM u2 LIMIT 1)),
                (
                    (SELECT id FROM u2 OFFSET 1 LIMIT 1),
                    (SELECT y FROM u2 OFFSET 1 LIMIT 1)
                )
            ''',
            uuid1,
            uuid2,
        )
        self.assertEqual(res, 'INSERT 0 3')
        res = await self.squery_values(
            '''
            SELECT title, owner_id FROM "Document"
            '''
        )
        res[0][1] = None  # first uuid is generated and unknown at this stage
        self.assertEqual(
            res,
            [
                ['hello (new)', None],
                ['world (new)', uuid1],
                ['world (new)', uuid2],
            ],
        )

    async def test_sql_dml_insert_43(self):
        await self.scon.execute("SET LOCAL allow_user_specified_id TO TRUE")

        doc_id = uuid.uuid4()
        user_id = uuid.uuid4()
        res = await self.scon.execute(
            '''
            WITH
                d AS (INSERT INTO "Document" (id) VALUES ($1)),
                u AS (INSERT INTO "User" (id) VALUES ($2)),
                dsw AS (
                    INSERT INTO "Document.shared_with" (source, target)
                    VALUES ($1, $2)
                )
                INSERT INTO "Document.keywords" VALUES ($1, 'top-priority')
            ''',
            doc_id,
            user_id,
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.squery_values(
            '''
            SELECT source, target FROM "Document.shared_with"
            '''
        )
        self.assertEqual(res, [[doc_id, user_id]])

    async def test_sql_dml_insert_44(self):
        # Test that RETURNING supports "Table".col format
        res = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Test returning')
            RETURNING "Document".id
            '''
        )
        docid = res[0][0]
        res = await self.squery_values('SELECT id, title FROM "Document"')
        self.assertEqual(res, [[docid, 'Test returning (new)']])

    async def test_sql_dml_insert_45(self):
        # Test that properties ending in _id work.
        res = await self.scon.execute(
            '''
            INSERT INTO "Numbered" (num_id) VALUES (10)
            '''
        )
        res = await self.squery_values('SELECT num_id FROM "Numbered"')
        self.assertEqual(res, [[10]])

    async def test_sql_dml_delete_01(self):
        # delete, inspect CommandComplete tag

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title)
            VALUES ('Report'), ('Report'), ('Briefing')
            '''
        )
        res = await self.scon.execute(
            '''
            DELETE FROM "Document"
            WHERE title = 'Report (new)'
            ''',
        )
        self.assertEqual(res, 'DELETE 2')

    async def test_sql_dml_delete_02(self):
        # delete with returning clause, inspect CommandComplete tag

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title)
            VALUES ('Report'), ('Report'), ('Briefing')
            '''
        )
        res = await self.scon.execute(
            '''
            DELETE FROM "Document"
            WHERE title = 'Report (new)'
            RETURNING title
            ''',
        )
        self.assertEqual(res, 'DELETE 2')

    async def test_sql_dml_delete_03(self):
        # delete with returning clause

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Report'), ('Briefing')
            '''
        )
        res = await self.squery_values(
            '''
            DELETE FROM "Document"
            RETURNING title
            ''',
        )
        self.assertEqual(res, [['Report (new)'], ['Briefing (new)']])

    async def test_sql_dml_delete_04(self):
        # delete with using clause

        users = await self.squery_values(
            '''
            INSERT INTO "User" (id) VALUES (DEFAULT), (DEFAULT) RETURNING id
            '''
        )
        await self.squery_values(
            '''
            WITH u(id) as (VALUES ($1), ($2))
            INSERT INTO "Document" (title, owner_id)
            SELECT 'Report', u.id FROM u
            RETURNING title, owner_id
            ''',
            str(users[0][0]),
            str(users[1][0]),
        )

        res = await self.squery_values(
            '''
            DELETE FROM "Document"
            USING "User" u
            WHERE "Document".owner_id = u.id AND title = 'Report (new)'
            RETURNING title, owner_id
            ''',
        )
        self.assertEqual(
            res,
            [
                ['Report (new)', res[0][1]],
                ['Report (new)', res[1][1]],
            ],
        )

    async def test_sql_dml_delete_05(self):
        # delete where current of
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            'not supported: CURRENT OF',
        ):
            await self.scon.execute(
                '''
                DELETE FROM tasks WHERE CURRENT OF c_tasks;
                ''',
            )

    async def test_sql_dml_delete_06(self):
        # delete returning *

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Report')
            '''
        )
        res = await self.squery_values(
            '''
            DELETE FROM "Document" RETURNING *
            ''',
        )
        self.assertEqual(res, [[res[0][0], res[0][1], None, 'Report (new)']])
        self.assertIsInstance(res[0][0], uuid.UUID)
        self.assertIsInstance(res[0][1], uuid.UUID)

    async def test_sql_dml_delete_07(self):
        # delete with CTEs

        await self.scon.execute(
            '''
            INSERT INTO "User" DEFAULT VALUES
            '''
        )
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title, owner_id)
            VALUES
              ('Report', NULL),
              ('Briefing', (SELECT id FROM "User" LIMIT 1))
            '''
        )
        res = await self.squery_values(
            '''
            WITH
              users as (SELECT id FROM "User"),
              not_owned as (
                SELECT d.id
                FROM "Document" d
                LEFT JOIN users u ON d.owner_id = u.id
                WHERE u.id IS NULL
              )
            DELETE FROM "Document"
            USING not_owned
            WHERE not_owned.id = "Document".id
            RETURNING title
            ''',
        )
        self.assertEqual(res, [['Report (new)']])

    async def test_sql_dml_delete_08(self):
        [document] = await self.squery_values(
            'INSERT INTO "Document" DEFAULT VALUES RETURNING id'
        )
        [user1] = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id'
        )
        [user2] = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id'
        )
        await self.scon.execute(
            '''
            INSERT INTO "Document.shared_with" (source, target)
            VALUES ($1, $2), ($1, $3)
            ''',
            document[0],
            user1[0],
            user2[0],
        )

        # delete where false
        res = await self.scon.execute(
            '''
            DELETE FROM "Document.shared_with" WHERE FALSE
            ''',
        )
        self.assertEqual(res, 'DELETE 0')
        res = await self.squery_values(
            '''
            SELECT COUNT(*) FROM "Document.shared_with"
            ''',
        )
        self.assertEqual(res, [[2]])

        # delete where source
        res = await self.scon.execute(
            '''
            DELETE FROM "Document.shared_with" WHERE source = $1
            ''',
            document[0],
        )
        self.assertEqual(res, 'DELETE 2')
        await self.scon.execute(
            '''
            INSERT INTO "Document.shared_with" (source, target)
            VALUES ($1, $2), ($1, $3)
            ''',
            document[0],
            user1[0],
            user2[0],
        )

        # delete where target
        res = await self.scon.execute(
            '''
            DELETE FROM "Document.shared_with" WHERE target = $1
            ''',
            user1[0],
        )
        self.assertEqual(res, 'DELETE 1')
        await self.scon.execute(
            '''
            INSERT INTO "Document.shared_with" (source, target)
            VALUES ($1, $2), ($1, $3)
            ''',
            document[0],
            user1[0],
            user2[0],
        )

        # delete all
        res = await self.scon.execute(
            '''
            DELETE FROM "Document.shared_with"
            '''
        )
        self.assertEqual(res, 'DELETE 2')

    async def test_sql_dml_delete_09(self):
        # delete with returning clause and using and CTEs

        [document] = await self.squery_values(
            'INSERT INTO "Document" DEFAULT VALUES RETURNING id'
        )
        [user1] = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id'
        )
        [user2] = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id'
        )
        await self.squery_values(
            '''
            INSERT INTO "Document.shared_with" (source, target)
            VALUES ($1, $2), ($1, $3)
            ''',
            document[0],
            user1[0],
            user2[0],
        )

        deleted = await self.squery_values(
            '''
            WITH
              users_to_keep as (SELECT id FROM "User" WHERE id = $1),
              users_to_delete as (
                SELECT u.id
                FROM "User" u
                LEFT JOIN users_to_keep utk ON (u.id = utk.id)
                WHERE utk.id IS NULL
              )
            DELETE FROM "Document.shared_with" dsw
            USING users_to_delete utd
            WHERE utd.id = dsw.target
            RETURNING source, target
            ''',
            user2[0],
        )
        self.assertEqual(deleted, [[document[0], user1[0]]])

    async def test_sql_dml_delete_10(self):
        # delete from a single link table

        [user1] = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id'
        )
        [user2] = await self.squery_values(
            'INSERT INTO "User" DEFAULT VALUES RETURNING id'
        )
        [doc1, _doc2] = await self.squery_values(
            '''
            INSERT INTO "Document" (owner_id) VALUES ($1), ($2) RETURNING id
            ''',
            user1[0],
            user2[0],
        )

        deleted = await self.squery_values(
            '''
            DELETE FROM "Document.owner"
            WHERE source = $1
            RETURNING source, target, is_author
            ''',
            doc1[0],
        )
        self.assertEqual(deleted, [[doc1[0], user1[0], None]])

    async def test_sql_dml_delete_11(self):
        # delete from a single link table

        [document] = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            '''
        )
        await self.squery_values(
            '''
            INSERT INTO "Document.keywords"
            VALUES ($1, 'notes'), ($1, 'priority')
            ''',
            document[0],
        )

        deleted = await self.squery_values(
            '''
            DELETE FROM "Document.keywords"
            WHERE target = 'priority'
            RETURNING source, target
            '''
        )
        self.assertEqual(deleted, [[document[0], 'priority']])

        deleted = await self.squery_values(
            '''
            DELETE FROM "Document.keywords"
            WHERE source = $1
            RETURNING source, target
            ''',
            document[0],
        )
        self.assertEqual(deleted, [[document[0], 'notes']])

    async def test_sql_dml_delete_12(self):
        # Create a new document and try to delete it immediately.

        # This will not delete the document, since DELETE statement cannot "see"
        # the document that has just been inserted (this is Postgres behavior).
        res = await self.scon.execute(
            '''
            WITH inserted as (
                INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            )
            DELETE FROM "Document" d USING inserted WHERE d.id = inserted.id
            '''
        )
        self.assertEqual(res, 'DELETE 0')

        res = await self.squery_values(
            'SELECT COUNT(*) FROM "Document"',
        )
        self.assertEqual(res, [[1]])

    async def test_sql_dml_delete_13(self):
        [[doc_id, user_id]] = await self.squery_values(
            '''
            WITH
                d AS (INSERT INTO "Document" DEFAULT VALUES RETURNING id),
                u AS (INSERT INTO "User" DEFAULT VALUES RETURNING id)
            INSERT INTO "Document.shared_with" (source, target)
            VALUES ((SELECT id FROM d), (SELECT id FROM u))
            RETURNING source, target
            '''
        )

        res = await self.scon.execute(
            '''
            WITH
                inserted(s, t) AS (VALUES ($1::uuid, $2::uuid))
            DELETE FROM "Document.shared_with" dsw
            USING inserted
            WHERE dsw.source = inserted.s AND dsw.target = inserted.t
            ''',
            doc_id,
            user_id,
        )
        self.assertEqual(res, 'DELETE 1')

    async def test_sql_dml_delete_14(self):
        # Test that RETURNING supports "Table".col format

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title)
            VALUES ('Test returning')
            '''
        )
        res = await self.squery_values(
            '''
            DELETE FROM "Document"
            WHERE title LIKE 'Test returning%'
            RETURNING "Document".title
            ''',
        )
        self.assertEqual(res, [['Test returning (new)']])

    async def test_sql_dml_update_01(self):
        # update, inspect CommandComplete tag

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title)
            VALUES ('Report'), ('Report'), ('Briefing')
            '''
        )
        res = await self.scon.execute(
            '''
            UPDATE "Document"
            SET title = '[REDACTED]'
            WHERE title LIKE 'Report%'
            ''',
        )
        self.assertEqual(res, 'UPDATE 2')

        res = await self.squery_values(
            '''
            SELECT title FROM "Document" ORDER BY title
            ''',
        )
        self.assertEqual(
            res,
            [
                ['Briefing (new)'],
                ['[REDACTED] (updated)'],
                ['[REDACTED] (updated)'],
            ],
        )

    async def test_sql_dml_update_02(self):
        # update with returning clause, inspect CommandComplete tag

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title)
            VALUES ('Report'), ('Report'), ('Briefing')
            '''
        )
        res = await self.scon.execute(
            '''
            UPDATE "Document"
            SET title = '[REDACTED]'
            WHERE title LIKE 'Report%'
            RETURNING id
            ''',
        )
        self.assertEqual(res, 'UPDATE 2')

    async def test_sql_dml_update_03(self):
        # update with returning clause

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Report'), ('Briefing')
            '''
        )
        res = await self.squery_values(
            '''
            UPDATE "Document" SET title = title RETURNING title
            ''',
        )
        self.assertEqual(
            res, [['Report (new) (updated)'], ['Briefing (new) (updated)']]
        )

    async def test_sql_dml_update_04(self):
        # update with from clause

        users = await self.squery_values(
            '''
            INSERT INTO "User" (id) VALUES (DEFAULT), (DEFAULT) RETURNING id
            '''
        )
        await self.squery_values(
            '''
            WITH u(id) as (VALUES ($1), ($2))
            INSERT INTO "Document" (title, owner_id)
            SELECT 'Report', u.id FROM u
            RETURNING title, owner_id
            ''',
            str(users[0][0]),
            str(users[1][0]),
        )

        res = await self.squery_values(
            '''
            UPDATE "Document"
            SET owner_id = owner_id
            FROM "User" u
            WHERE "Document".owner_id = u.id AND title = 'Report (new)'
            RETURNING title, owner_id
            ''',
        )
        self.assertEqual(
            res,
            [
                ['Report (new) (updated)', res[0][1]],
                ['Report (new) (updated)', res[1][1]],
            ],
        )

    async def test_sql_dml_update_05(self):
        # update where current of
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            'not supported: CURRENT OF',
        ):
            await self.scon.execute(
                '''
                UPDATE films SET kind = 'Dramatic' WHERE CURRENT OF c_films;
                ''',
            )

    async def test_sql_dml_update_06(self):
        # update returning *

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title) VALUES ('Report')
            '''
        )
        res = await self.squery_values(
            '''
            UPDATE "Document" SET owner_id = NULL RETURNING *
            ''',
        )
        self.assertIsInstance(res[0][0], uuid.UUID)
        self.assertIsInstance(res[0][1], uuid.UUID)
        self.assertEqual(
            res, [[res[0][0], res[0][1], None, 'Report (new) (updated)']]
        )

    async def test_sql_dml_update_07(self):
        # update with CTEs

        await self.scon.execute(
            '''
            INSERT INTO "User" DEFAULT VALUES
            '''
        )
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title, owner_id)
            VALUES
              ('Report', NULL),
              ('Briefing', (SELECT id FROM "User" LIMIT 1))
            '''
        )
        res = await self.squery_values(
            '''
            WITH
              users as (SELECT id FROM "User"),
              not_owned as (
                SELECT d.id
                FROM "Document" d
                LEFT JOIN users u ON d.owner_id = u.id
                WHERE u.id IS NULL
              )
            UPDATE "Document"
            SET title = title
            FROM not_owned
            WHERE not_owned.id = "Document".id
            RETURNING title
            ''',
        )
        self.assertEqual(res, [['Report (new) (updated)']])

    async def test_sql_dml_update_08(self):
        # update with a trivial multi-ref

        [user] = await self.squery_values(
            '''
            INSERT INTO "User" DEFAULT VALUES RETURNING id
            '''
        )
        await self.scon.execute(
            '''
            INSERT INTO "Document" (title, owner_id)
            VALUES (NULL, NULL)
            '''
        )
        res = await self.squery_values(
            '''
            UPDATE "Document"
            SET (title, owner_id) = ROW('hello', $1::uuid)
            RETURNING title, owner_id
            ''',
            user[0],
        )
        self.assertEqual(res, [['hello (updated)', user[0]]])

    @test.xerror('unsupported')
    async def test_sql_dml_update_09(self):
        # update with a non-trivial multi-ref

        await self.squery_values(
            '''
            WITH x AS (SELECT ROW('Report', $1::uuid) as y)
            UPDATE "Document"
            SET (title, owner_id) = x.y
            FROM x;
            ''',
        )

    async def test_sql_dml_update_10(self):
        # update set link id to uuid

        [user] = await self.squery_values(
            '''
            INSERT INTO "User" DEFAULT VALUES RETURNING id
            '''
        )
        await self.scon.execute(
            '''
            INSERT INTO "Document" DEFAULT VALUES
            '''
        )
        res = await self.squery_values(
            '''
            UPDATE "Document" SET owner_id = $1 RETURNING owner_id
            ''',
            user[0],
        )
        self.assertEqual(res, [[user[0]]])

    async def test_sql_dml_update_11(self):
        # update set default

        res = await self.squery_values(
            '''
            INSERT INTO "Post" (title, created_at)
            VALUES (DEFAULT, DEFAULT)
            RETURNING title, created_at
            '''
        )
        self.assertEqual(res, [['untitled', None]])

        res = await self.squery_values(
            '''
            UPDATE "Post" SET
                title = 'Announcing EdgeDB 1.0',
                created_at = '2024-08-08T08:08:08.000'::timestamp
            RETURNING title, created_at::text
            '''
        )
        self.assertEqual(
            res, [['Announcing EdgeDB 1.0', '2024-08-08 08:08:08+00']]
        )

        res = await self.squery_values(
            '''
            UPDATE "Post" SET
                title = DEFAULT,
                created_at = DEFAULT
            RETURNING title, created_at
            '''
        )
        self.assertEqual(res, [['untitled', None]])

    async def test_sql_dml_update_12(self):
        id1 = uuid.uuid4()

        [doc] = await self.squery_values(
            'INSERT INTO "Document" DEFAULT VALUES RETURNING id'
        )

        with self.assertRaisesRegex(
            asyncpg.DataError,
            'cannot update property \'id\': ' 'it is declared as read-only',
        ):
            await self.squery_values(
                '''
                UPDATE "Document"
                SET id = $2
                WHERE id = $1
                RETURNING id
                ''',
                doc[0],
                id1,
            )

    async def test_sql_dml_update_13(self):
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            'UPDATE of link tables is not supported',
        ):
            await self.squery_values(
                '''
                UPDATE "Document.shared_with" SET can_edit = FALSE
                '''
            )

    async def test_sql_dml_update_14(self):
        # UPDATE will not match anything, because the inserted document is not
        # yet "visible" during the UPDATE statement (this is Postgres behavior).
        res = await self.scon.execute(
            '''
            WITH inserted as (
                INSERT INTO "Document" (title) VALUES ('Report') RETURNING id
            )
            UPDATE "Document" d
            SET title = 'Briefing'
            FROM inserted
            WHERE d.id = inserted.id
            '''
        )
        self.assertEqual(res, 'UPDATE 0')

        res = await self.squery_values(
            'SELECT title FROM "Document" ORDER BY title',
        )
        self.assertEqual(res, [['Report (new)']])

        # Now we remove UPDATE condition and expect all documents that existed
        # *before* this statement to be updated.
        res = await self.scon.execute(
            '''
            WITH inserted as (
                INSERT INTO "Document" (title) VALUES ('Receipt') RETURNING id
            )
            UPDATE "Document" d
            SET title = 'Briefing'
            '''
        )
        self.assertEqual(res, 'UPDATE 1')

        res = await self.squery_values(
            'SELECT title FROM "Document" ORDER BY title',
        )
        self.assertEqual(res, [['Briefing (updated)'], ['Receipt (new)']])

    async def test_sql_dml_update_14a(self):
        await self.squery_values(
            'INSERT INTO "Document" DEFAULT VALUES',
        )

        res = await self.scon.execute(
            '''
            WITH
                u AS (INSERT INTO "User" DEFAULT VALUES RETURNING id)
            UPDATE "Document" SET owner_id = (SELECT id FROM u)
            '''
        )
        self.assertEqual(res, 'UPDATE 1')

        res = await self.squery_values(
            'SELECT owner_id IS NULL FROM "Document"',
        )
        self.assertEqual(res, [[False]])

    async def test_sql_dml_update_15(self):
        [[doc_id]] = await self.squery_values(
            '''
            INSERT INTO "Document" (title) VALUES ('Briefing') RETURNING id
            '''
        )

        res = await self.scon.execute(
            '''
            WITH updated as (
                UPDATE "Document" d
                SET title = 'Report'
                WHERE d.id = $1
                RETURNING id, title
            )
            INSERT INTO "Document" (title)
            SELECT 'X' || id::uuid || ','  || title FROM updated
            ''',
            doc_id,
        )
        self.assertEqual(res, 'INSERT 0 1')

        res = await self.squery_values(
            'SELECT id, title FROM "Document" ORDER BY title',
        )
        self.assertEqual(len(res), 2)
        existing_id, existing_title = res[0]
        _inserted_id, inserted_title = res[1]

        self.assertEqual(existing_title, 'Report (updated)')
        self.assertEqual(
            inserted_title,
            'X' + str(existing_id) + ',' + existing_title + ' (new)',
        )

    async def test_sql_dml_update_16(self):
        [[doc_id]] = await self.squery_values(
            'INSERT INTO "Document" DEFAULT VALUES RETURNING id'
        )

        res = await self.squery_values(
            '''
            WITH
            u1 as (
                INSERT INTO "User" DEFAULT VALUES RETURNING id
            )
            UPDATE "Document" SET owner_id = u1.id
            FROM u1
            RETURNING id, owner_id
            '''
        )
        user_id = res[0][1]
        self.assertEqual(
            res,
            [
                [doc_id, user_id],
            ],
        )

        res = await self.squery_values(
            '''
            SELECT id, owner_id FROM "Document"
            '''
        )
        self.assertEqual(
            res,
            [
                [doc_id, user_id],
            ],
        )

    async def test_sql_dml_update_17(self):
        # Test that RETURNING supports "Table".col format

        await self.scon.execute(
            '''
            INSERT INTO "Document" (title)
            VALUES ('Report')
            '''
        )
        res = await self.squery_values(
            '''
            UPDATE "Document"
            SET title = 'Test returning'
            WHERE title LIKE 'Report%'
            RETURNING "Document".title
            ''',
        )
        self.assertEqual(res, [['Test returning (updated)']])

    async def test_sql_dml_01(self):
        # update/delete only

        await self.scon.execute(
            '''
            INSERT INTO "Base" (prop) VALUES ('base')
            '''
        )
        await self.scon.execute(
            '''
            INSERT INTO "Child" (prop) VALUES ('child')
            '''
        )

        res = await self.squery_values('SELECT prop FROM "Base" ORDER BY prop')
        self.assertEqual(res, [['base'], ['child']])

        res = await self.squery_values('SELECT prop FROM ONLY "Base"')
        self.assertEqual(res, [['base']])

        res = await self.squery_values('SELECT prop FROM "Child"')
        self.assertEqual(res, [['child']])

        await self.scon.execute(
            '''
            UPDATE ONLY "Base" SET prop = 'a'
            '''
        )
        res = await self.squery_values('SELECT prop FROM "Base" ORDER BY prop')
        self.assertEqual(res, [['a'], ['child']])

        await self.scon.execute(
            '''
            DELETE FROM ONLY "Base"
            '''
        )
        res = await self.squery_values('SELECT prop FROM "Base" ORDER BY prop')
        self.assertEqual(res, [['child']])

    async def test_sql_dml_02(self):
        # globals

        await self.scon.execute(
            '''
            INSERT INTO "Globals" DEFAULT VALUES
            '''
        )

        await self.scon.execute(
            """
            SET LOCAL "global default::y" TO 'Hello world!';
            """
        )

        await self.scon.execute(
            '''
            INSERT INTO "Globals" DEFAULT VALUES
            '''
        )

        res = await self.squery_values('SELECT gy FROM "Globals" ORDER BY gy')
        self.assertEqual(res, [['Hello world!'], [None]])
