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

import csv
import io
import os.path
from typing import Optional
import unittest
import uuid

from edb.tools import test
from edb.testbase import server as tb

import edgedb

try:
    import asyncpg
    from asyncpg import serverversion
except ImportError:
    pass


class TestSQLQuery(tb.SQLQueryTestCase):
    EXTENSIONS = ["pgvector", "ai"]
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'movies.esdl')
    SCHEMA_INVENTORY = os.path.join(
        os.path.dirname(__file__), 'schemas', 'inventory.esdl'
    )

    TRANSACTION_ISOLATION = False  # needed for test_sql_query_set_04

    SETUP = [
        '''
        alter type novel {
            create deferred index ext::ai::index(
              embedding_model := 'text-embedding-3-large') on (.foo);
            create index fts::index on (
                fts::with_options(.foo, language := fts::Language.eng)
            );
        };

        create module glob_mod;
        create global glob_mod::glob_str: str;
        create global glob_mod::glob_uuid: uuid;
        create global glob_mod::glob_int64: int64;
        create global glob_mod::glob_int32: int32;
        create global glob_mod::glob_int16: int16;
        create global glob_mod::glob_bool: bool;
        create global glob_mod::glob_float64: float64;
        create global glob_mod::glob_float32: float32;
        create type glob_mod::G {
            create property p_str: str {
                set default := global glob_mod::glob_str
            };
            create property p_uuid: uuid {
                set default := global glob_mod::glob_uuid
            };
            create property p_int64: int64 {
                set default := global glob_mod::glob_int64
            };
            create property p_int32: int32 {
                set default := global glob_mod::glob_int32
            };
            create property p_int16: int16 {
                set default := global glob_mod::glob_int16
            };
            create property p_bool: bool {
                set default := global glob_mod::glob_bool
            };
            create property p_float64: float64 {
                set default := global glob_mod::glob_float64
            };
            create property p_float32: float32 {
                set default := global glob_mod::glob_float32
            };
        };
        ''',
        os.path.join(
            os.path.dirname(__file__), 'schemas', 'movies_setup.edgeql'
        ),
    ]

    async def test_sql_query_00(self):
        # basic
        res = await self.squery_values(
            '''
            SELECT title FROM "Movie" order by title
            '''
        )
        self.assertEqual(res, [['Forrest Gump'], ['Saving Private Ryan']])

    async def test_sql_query_01(self):
        # table alias
        res = await self.scon.fetch(
            '''
            SELECT mve.title, mve.release_year, director_id FROM "Movie" as mve
            '''
        )
        self.assert_shape(res, 2, 3)

    async def test_sql_query_02(self):
        # SELECT FROM parent type
        res = await self.scon.fetch(
            '''
            SELECT * FROM "Content"
            '''
        )
        self.assert_shape(res, 5, ['id', '__type__', 'genre_id', 'title'])

    async def test_sql_query_03(self):
        # SELECT FROM parent type only
        res = await self.scon.fetch(
            '''
            SELECT * FROM ONLY "Content" -- should have only one result
            '''
        )
        self.assert_shape(res, 1, ['id', '__type__', 'genre_id', 'title'])

    async def test_sql_query_04(self):
        # multiple FROMs
        res = await self.scon.fetch(
            '''
            SELECT mve.title, "Person".first_name
            FROM "Movie" mve, "Person" WHERE mve.director_id = "Person".id
            '''
        )
        self.assert_shape(res, 1, ['title', 'first_name'])

    async def test_sql_query_05(self):
        res = await self.scon.fetch(
            '''
            SeLeCt mve.title as tiT, perSon.first_name
            FROM "Movie" mve, "Person" person
            '''
        )
        self.assert_shape(res, 6, ['tit', 'first_name'])

    async def test_sql_query_06(self):
        # sub relations
        res = await self.scon.fetch(
            '''
            SELECT id, title, prS.first_name
            FROM "Movie" mve, (SELECT first_name FROM "Person") prs
            '''
        )
        self.assert_shape(res, 6, ['id', 'title', 'first_name'])

    async def test_sql_query_07(self):
        # quoted case sensitive
        res = await self.scon.fetch(
            '''
            SELECT tItLe, release_year "RL year" FROM "Movie" ORDER BY titLe;
            '''
        )
        self.assert_shape(res, 2, ['title', 'RL year'])

    async def test_sql_query_08(self):
        # JOIN
        res = await self.scon.fetch(
            '''
            SELECT "Movie".id, "Genre".id
            FROM "Movie" JOIN "Genre" ON "Movie".genre_id = "Genre".id
            '''
        )
        self.assert_shape(res, 2, ['id', 'col~1'])

    async def test_sql_query_09(self):
        # resolve columns without table names
        res = await self.scon.fetch(
            '''
            SELECT "Movie".id, title, name
            FROM "Movie" JOIN "Genre" ON "Movie".genre_id = "Genre".id
            '''
        )
        self.assert_shape(res, 2, ['id', 'title', 'name'])

    async def test_sql_query_10(self):
        # wildcard SELECT
        res = await self.scon.fetch(
            '''
            SELECT m.* FROM "Movie" m
            '''
        )
        self.assert_shape(
            res,
            2,
            [
                'id',
                '__type__',
                'director_id',
                'genre_id',
                'release_year',
                'title',
            ],
        )

    async def test_sql_query_11(self):
        # multiple wildcard SELECT
        res = await self.scon.fetch(
            '''
            SELECT * FROM "Movie"
            JOIN "Genre" g ON "Movie".genre_id = g.id
            '''
        )
        self.assert_shape(
            res,
            2,
            [
                'id',
                '__type__',
                'director_id',
                'genre_id',
                'release_year',
                'title',
                'id',
                '__type__',
                'name',
            ],
        )

    async def test_sql_query_12(self):
        # JOIN USING
        res = await self.scon.fetch(
            '''
            SELECT * FROM "Movie"
            JOIN (SELECT id as genre_id, name FROM "Genre") g USING (genre_id)
            '''
        )
        self.assert_shape(res, 2, 8)

    async def test_sql_query_13(self):
        # CTE
        res = await self.scon.fetch(
            '''
            WITH g AS (SELECT id as genre_id, name FROM "Genre")
            SELECT * FROM "Movie" JOIN g USING (genre_id)
            '''
        )
        self.assert_shape(res, 2, 8)

    async def test_sql_query_14(self):
        # CASE
        res = await self.squery_values(
            '''
            SELECT title, CASE WHEN title='Forrest Gump' THEN 'forest'
            WHEN title='Saving Private Ryan' THEN 'the war film'
            ELSE 'unknown' END AS nick_name FROM "Movie"
            '''
        )
        self.assertEqual(
            res,
            [
                ['Forrest Gump', 'forest'],
                ['Saving Private Ryan', 'the war film'],
            ],
        )

    async def test_sql_query_15(self):
        # UNION
        res = await self.scon.fetch(
            '''
            SELECT id, title FROM "Movie" UNION SELECT id, title FROM "Book"
            '''
        )
        self.assert_shape(res, 4, 2)

    async def test_sql_query_16(self):
        # casting
        res = await self.scon.fetch(
            '''
            SELECT 1::bigint, 'accbf276-705b-11e7-b8e4-0242ac120002'::UUID
            '''
        )
        self.assert_shape(res, 1, 2)

    async def test_sql_query_17(self):
        # ORDER BY
        res = await self.squery_values(
            '''
            SELECT first_name, last_name
            FROM "Person" ORDER BY last_name DESC NULLS FIRST
            '''
        )
        self.assertEqual(
            res, [['Robin', None], ['Steven', 'Spielberg'], ['Tom', 'Hanks']]
        )

        res = await self.squery_values(
            '''
            SELECT first_name, last_name
            FROM "Person" ORDER BY last_name DESC NULLS LAST
            '''
        )
        self.assertEqual(
            res, [['Steven', 'Spielberg'], ['Tom', 'Hanks'], ['Robin', None]]
        )

    async def test_sql_query_18(self):
        # LIMIT & OFFSET
        res = await self.squery_values(
            '''
            SELECT title FROM "Content" ORDER BY title OFFSET 1 LIMIT 2
            '''
        )
        self.assertEqual(res, [['Forrest Gump'], ['Halo 3']])

    async def test_sql_query_19(self):
        # DISTINCT
        res = await self.squery_values(
            '''
            SELECT DISTINCT name
            FROM "Content" c JOIN "Genre" g ON (c.genre_id = g.id)
            ORDER BY name
            '''
        )
        self.assertEqual(res, [['Drama'], ['Fiction']])

        res = await self.squery_values(
            '''
            SELECT DISTINCT ON (name) name, title
            FROM "Content" c JOIN "Genre" g ON (c.genre_id = g.id)
            ORDER BY name, title
            '''
        )
        self.assertEqual(
            res,
            [['Drama', 'Forrest Gump'], ['Fiction', 'Chronicles of Narnia']],
        )

    async def test_sql_query_20(self):
        # WHERE
        res = await self.squery_values(
            '''
            SELECT first_name FROM "Person"
            WHERE last_name IS NOT NULL AND LENGTH(first_name) < 4
            '''
        )
        self.assertEqual(res, [['Tom']])

    async def test_sql_query_21(self):
        # window functions
        res = await self.squery_values(
            '''
            WITH content AS (
                SELECT c.id, c.title, pages
                FROM "Content" c LEFT JOIN "Book" USING(id)
            ),
            content2 AS (
                SELECT id, COALESCE(pages, 0) as pages FROM content
            )
            SELECT pages, sum(pages) OVER (ORDER BY pages)
            FROM content2 ORDER BY pages DESC
            '''
        )
        self.assertEqual(
            res,
            [[374, 580], [206, 206], [0, 0], [0, 0], [0, 0]],
        )

    async def test_sql_query_22(self):
        # IS NULL/true
        res = await self.scon.fetch(
            '''
            SELECT id FROM "Person" WHERE last_name IS NULL
            '''
        )
        self.assert_shape(res, 1, 1)

        res = await self.scon.fetch(
            '''
            SELECT id FROM "Person" WHERE (last_name = 'Hanks') IS NOT TRUE
            '''
        )
        self.assert_shape(res, 2, 1)

    async def test_sql_query_23(self):
        # ImplicitRow
        res = await self.scon.fetch(
            '''
            SELECT id FROM "Person"
            WHERE (first_name, last_name) IN (
                ('Tom', 'Hanks'), ('Steven', 'Spielberg')
            )
            '''
        )
        self.assert_shape(res, 2, 1)

    async def test_sql_query_24(self):
        # SubLink
        res = await self.squery_values(
            '''
            SELECT title FROM "Movie" WHERE id IN (
                SELECT id FROM "Movie" ORDER BY title LIMIT 1
            )
            '''
        )
        self.assertEqual(res, [['Forrest Gump']])

        res = await self.squery_values(
            '''
            SELECT (SELECT title FROM "Movie" ORDER BY title LIMIT 1)
            '''
        )
        self.assertEqual(res, [['Forrest Gump']])

    async def test_sql_query_25(self):
        # lower case object name
        await self.scon.fetch('SELECT title FROM novel ORDER BY title')

        await self.scon.fetch('SELECT title FROM "novel" ORDER BY title')

        with self.assertRaisesRegex(
            asyncpg.UndefinedTableError,
            "unknown table",
            position="19",
        ):
            await self.scon.fetch('SELECT title FROM "Novel" ORDER BY title')

    async def test_sql_query_26(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedTableError,
            "unknown table",
            position="19",
        ):
            await self.scon.fetch('SELECT title FROM Movie ORDER BY title')

    async def test_sql_query_27(self):
        # FROM LATERAL
        await self.scon.fetch(
            '''
            SELECT name, title
            FROM "Movie" m, LATERAL (
                SELECT g.name FROM "Genre" g WHERE m.genre_id = g.id
            ) t
            ORDER BY title
        '''
        )

    async def test_sql_query_28(self):
        # JOIN LATERAL
        res = await self.scon.fetch(
            '''
            SELECT name, title
            FROM "Movie" m CROSS JOIN LATERAL (
                SELECT g.name FROM "Genre" g WHERE m.genre_id = g.id
            ) t
            ORDER BY title
        '''
        )
        self.assert_shape(res, 2, ['name', 'title'])

    async def test_sql_query_29(self):
        # link tables

        # multi
        res = await self.scon.fetch('SELECT * FROM "Movie.actors"')
        self.assert_shape(res, 3, ['source', 'target', 'role'])

        # single with properties
        res = await self.scon.fetch('SELECT * FROM "Movie.director"')
        self.assert_shape(res, 1, ['source', 'target', 'bar'])

        # single without properties
        with self.assertRaisesRegex(
            asyncpg.UndefinedTableError, "unknown table"
        ):
            await self.scon.fetch('SELECT * FROM "Movie.genre"')

    async def test_sql_query_30(self):
        # VALUES

        res = await self.scon.fetch(
            '''
            SELECT * FROM (VALUES (1, 2), (3, 4)) AS vals(c, d)
            '''
        )
        self.assert_shape(res, 2, ['c', 'd'])

        with self.assertRaisesRegex(
            asyncpg.InvalidColumnReferenceError,
            ", but the query resolves to 2 columns",
            # this points to `1`, because libpg_query does not give better info
            position="41",
        ):
            await self.scon.fetch(
                '''
                SELECT * FROM (VALUES (1, 2), (3, 4)) AS vals(c, d, e)
                '''
            )

    async def test_sql_query_31(self):
        # column aliases in CTEs
        res = await self.scon.fetch(
            '''
            with common as (SELECT 1 a, 2 b)
            SELECT * FROM common
            '''
        )
        self.assert_shape(res, 1, ['a', 'b'])

        res = await self.scon.fetch(
            '''
            with common(c, d) as (SELECT 1 a, 2 b)
            SELECT * FROM common
            '''
        )
        self.assert_shape(res, 1, ['c', 'd'])

        res = await self.scon.fetch(
            '''
            with common(c, d) as (SELECT 1 a, 2 b)
            SELECT * FROM common as cmn(e, f)
            '''
        )
        self.assert_shape(res, 1, ['e', 'f'])

        with self.assertRaisesRegex(
            asyncpg.InvalidColumnReferenceError, "query resolves to 2"
        ):
            await self.scon.fetch(
                '''
                with common(c, d) as (SELECT 1 a, 2 b)
                SELECT * FROM common as cmn(e, f, g)
                '''
            )

    async def test_sql_query_32(self):
        # range functions

        res = await self.squery_values(
            '''
            SELECT *, '_' || value::text
            FROM json_each_text('{"a":"foo", "b":"bar"}') t
            '''
        )
        self.assertEqual(res, [["a", "foo", "_foo"], ["b", "bar", "_bar"]])

        res = await self.scon.fetch(
            '''
            SELECT * FROM
                (SELECT ARRAY[1, 2, 3] a, ARRAY[4, 5, 6] b) t,
                LATERAL unnest(a, b)
            '''
        )
        self.assert_shape(res, 3, ['a', 'b', 'unnest', 'unnest'])

        res = await self.scon.fetch(
            '''
            SELECT unnest(ARRAY[1, 2, 3]) a
            '''
        )
        self.assert_shape(res, 3, ['a'])

        res = await self.scon.fetch(
            '''
            SELECT unnest(ARRAY[]::int8[]) a
            '''
        )
        self.assertEqual(len(res), 0)

        res = await self.scon.fetch(
            '''
            SELECT *, unnested_b + 1 computed
            FROM
                (SELECT ARRAY[1, 2, 3] a, ARRAY[4, 5, 6] b) t,
                LATERAL unnest(a, b) awesome_table(unnested_a, unnested_b)
            '''
        )
        self.assert_shape(
            res, 3, ['a', 'b', 'unnested_a', 'unnested_b', 'computed']
        )

    async def test_sql_query_33(self):
        # system columns

        res = await self.squery_values(
            '''
            SELECT tableoid, xmin, cmin, xmax, cmax, ctid FROM ONLY "Content"
            '''
        )
        # these numbers change, so let's just check that there are 6 of them
        self.assertEqual(len(res[0]), 6)

        res = await self.squery_values(
            '''
            SELECT tableoid, xmin, cmin, xmax, cmax, ctid FROM "Content"
            '''
        )
        self.assertEqual(len(res[0]), 6)

        res = await self.squery_values(
            '''
            SELECT tableoid, xmin, cmin, xmax, cmax, ctid FROM "Movie.actors"
            '''
        )
        self.assertEqual(len(res[0]), 6)

    async def test_sql_query_33a(self):
        # system columns when access policies are applied
        tran = self.scon.transaction()
        await tran.start()
        await self.scon.execute('SET LOCAL apply_access_policies_sql TO true')
        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'Halo 3'"""
        )

        res = await self.squery_values(
            '''
            SELECT tableoid, xmin, cmin, xmax, cmax, ctid FROM ONLY "Content"
            '''
        )
        # these numbers change, so let's just check that there are 6 of them
        self.assertEqual(len(res[0]), 6)

        res = await self.squery_values(
            '''
            SELECT tableoid, xmin, cmin, xmax, cmax, ctid FROM "Content"
            '''
        )
        self.assertEqual(len(res[0]), 6)

        res = await self.squery_values(
            '''
            SELECT tableoid, xmin, cmin, xmax, cmax, ctid FROM "Movie.actors"
            '''
        )
        self.assertEqual(len(res[0]), 6)

        await tran.rollback()

    async def test_sql_query_34(self):
        # GROUP and ORDER BY aliased column

        res = await self.squery_values(
            """
            SELECT substr(title, 2, 4) AS itl, count(*) FROM "Movie"
            GROUP BY itl
            ORDER BY itl
            """
        )
        self.assertEqual(res, [["avin", 1], ["orre", 1]])

    async def test_sql_query_35(self):
        # ORDER BY original column

        res = await self.squery_values(
            """
            SELECT title AS aliased_title, count(*) FROM "Movie"
            GROUP BY title
            ORDER BY title
            """
        )
        self.assertEqual(res, [['Forrest Gump', 1], ['Saving Private Ryan', 1]])

    async def test_sql_query_36(self):
        # ColumnRef to relation

        res = await self.squery_values(
            """
            select rel from (select 1 as a, 2 as b) rel
            """
        )
        self.assertEqual(res, [[(1, 2)]])

    async def test_sql_query_37(self):
        res = await self.squery_values(
            """
            SELECT (pg_column_size(ROW()))::text
            """
        )
        self.assertEqual(res, [['24']])

    async def test_sql_query_38(self):
        res = await self.squery_values(
            '''
            WITH users AS (
              SELECT 1 as id, NULL as managed_by
              UNION ALL
              SELECT 2 as id, 1 as managed_by
            )
            SELECT id, (
              SELECT id FROM users e WHERE id = users.managed_by
            ) as managed_by
            FROM users
            ORDER BY id
            '''
        )
        self.assertEqual(
            res,
            [
                [1, None],
                [2, 1],
            ],
        )

    async def test_sql_query_39(self):
        res = await self.squery_values(
            '''
            SELECT pages, __type__ FROM "Book" ORDER BY pages;
            '''
        )
        self.assert_data_shape(
            res,
            [
                [206, str],
                [374, str],
            ],
        )
        # there should be one `Book` and one `novel`
        self.assertNotEqual(res[0][1], res[1][1])

        res2 = await self.squery_values(
            '''
            SELECT pages, __type__ FROM ONLY "Book" ORDER BY pages;
            '''
        )
        self.assert_data_shape(
            res2,
            [
                [206, str],
            ],
        )
        self.assertEqual(res[0][1], res2[0][1])

    async def test_sql_query_40(self):
        id: uuid.UUID = uuid.uuid4()

        res = await self.squery_values('SELECT $1::uuid;', id)
        self.assertEqual(res, [[id]])

        res = await self.squery_values('SELECT CAST($1 as uuid);', id)
        self.assertEqual(res, [[id]])

        with self.assertRaisesRegex(
            asyncpg.exceptions.DataError, 'expected str, got UUID'
        ):
            res = await self.squery_values('SELECT CAST($1::text as uuid);', id)
            self.assertEqual(res, [[id]])

        res = await self.squery_values(
            'SELECT CAST($1::text as uuid);', str(id)
        )
        self.assertEqual(res, [[id]])

        with self.assertRaisesRegex(
            asyncpg.exceptions.DataError, 'expected str, got UUID'
        ):
            res = await self.squery_values(
                'SELECT column1::uuid FROM (VALUES ($1))', id
            )
            self.assertEqual(res, [[id]])

        res = await self.squery_values('SELECT $1::uuid;', str(id))
        self.assertEqual(res, [[id]])

    async def test_sql_query_41(self):
        # bytea literal
        res = await self.squery_values("SELECT x'00abcdef00';")
        self.assertEqual(res, [[b'\x00\xab\xcd\xef\x00']])

    async def test_sql_query_42(self):
        # params out of order

        res = await self.squery_values(
            'SELECT $2::int, $3::bool, $1::text', 'hello', 42, True,
        )
        self.assertEqual(res, [[42, True, 'hello']])

        tran = self.scon.transaction()
        await tran.start()
        res = await self.scon.execute(
            '''
            UPDATE "Book" SET pages = $1 WHERE (title = $2)
            ''',
            207,
            'Chronicles of Narnia',
        )
        self.assertEqual(res, 'UPDATE 1')
        await tran.rollback()

    async def test_sql_query_43(self):
        # USING factoring

        res = await self.squery_values(
            '''
            WITH
                a(id) AS (SELECT 1 UNION SELECT 2),
                b(id) AS (SELECT 1 UNION SELECT 3)
            SELECT a.id, b.id, id
            FROM a LEFT JOIN b USING (id);
            '''
        )
        self.assertEqual(res, [[1, 1, 1], [2, None, 2]])

        res = await self.squery_values(
            '''
            WITH
                a(id, sub_id) AS (SELECT 1, 'a' UNION SELECT 2, 'b'),
                b(id, sub_id) AS (SELECT 1, 'a' UNION SELECT 3, 'c')
            SELECT a.id, a.sub_id, b.id, b.sub_id, id, sub_id
            FROM a JOIN b USING (id, sub_id);
            '''
        )
        self.assertEqual(res, [[1, 'a', 1, 'a', 1, 'a']])

        res = await self.squery_values(
            '''
            WITH
                a(id) AS (SELECT 1 UNION SELECT 2),
                b(id) AS (SELECT 1 UNION SELECT 3)
            SELECT a.id, b.id, id
            FROM a INNER JOIN b USING (id);
            '''
        )
        self.assertEqual(res, [[1, 1, 1]])

        res = await self.squery_values(
            '''
            WITH
                a(id) AS (SELECT 1 UNION SELECT 2),
                b(id) AS (SELECT 1 UNION SELECT 3)
            SELECT a.id, b.id, id
            FROM a RIGHT JOIN b USING (id);
            '''
        )
        self.assertEqual(res, [[1, 1, 1], [None, 3, 3]])

        res = await self.squery_values(
            '''
            WITH
                a(id) AS (SELECT 1 UNION SELECT 2),
                b(id) AS (SELECT 1 UNION SELECT 3)
            SELECT a.id, b.id, id
            FROM a RIGHT OUTER JOIN b USING (id);
            '''
        )
        self.assertEqual(res, [[1, 1, 1], [None, 3, 3]])

        res = await self.squery_values(
            '''
            WITH
                a(id) AS (SELECT 1 UNION SELECT 2),
                b(id) AS (SELECT 1 UNION SELECT 3)
            SELECT a.id, b.id, id
            FROM a FULL JOIN b USING (id);
            '''
        )
        self.assertEqual(res, [[1, 1, 1], [2, None, 2], [None, 3, 3]])

    async def test_sql_query_44(self):
        # range function that is an "sql value function", whatever that is

        # to be exact: User is *parsed* as function call CURRENT_USER
        # we'd ideally want a message that hints that it should use quotes

        with self.assertRaisesRegex(
            asyncpg.InvalidColumnReferenceError, 'cannot find column `name`'
        ):
            await self.squery_values('SELECT name FROM User')

    async def test_sql_query_introspection_00(self):
        dbname = self.con.dbname
        res = await self.squery_values(
            f'''
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_catalog = '{dbname}' AND table_schema ILIKE 'public%'
            ORDER BY table_schema, table_name
            '''
        )
        self.assertEqual(
            res,
            [
                ['public', 'Book'],
                ['public', 'Book.chapters'],
                ['public', 'Content'],
                ['public', 'ContentSummary'],
                ['public', 'Genre'],
                ['public', 'Movie'],
                ['public', 'Movie.actors'],
                ['public', 'Movie.director'],
                ['public', 'Person'],
                ['public', 'novel'],
                ['public', 'novel.chapters'],
                ['public::nested', 'Hello'],
                ['public::nested::deep', 'Rolling'],
            ],
        )

    async def test_sql_query_introspection_01(self):
        res = await self.squery_values(
            '''
            SELECT table_name, column_name, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            '''
        )

        self.assertEqual(
            res,
            [
                ['Book', 'id', 'NO', 1],
                ['Book', '__type__', 'NO', 2],
                ['Book', 'genre_id', 'YES', 3],
                ['Book', 'pages', 'NO', 4],
                ['Book', 'title', 'NO', 5],
                ['Book.chapters', 'source', 'NO', 1],
                ['Book.chapters', 'target', 'NO', 2],
                ['Content', 'id', 'NO', 1],
                ['Content', '__type__', 'NO', 2],
                ['Content', 'genre_id', 'YES', 3],
                ['Content', 'title', 'NO', 4],
                ['ContentSummary', 'id', 'NO', 1],
                ['ContentSummary', '__type__', 'NO', 2],
                ['ContentSummary', 'x', 'NO', 3],
                ['Genre', 'id', 'NO', 1],
                ['Genre', '__type__', 'NO', 2],
                ['Genre', 'name', 'NO', 3],
                ['Movie', 'id', 'NO', 1],
                ['Movie', '__type__', 'NO', 2],
                ['Movie', 'director_id', 'YES', 3],
                ['Movie', 'genre_id', 'YES', 4],
                ['Movie', 'release_year', 'YES', 5],
                ['Movie', 'title', 'NO', 6],
                ['Movie.actors', 'source', 'NO', 1],
                ['Movie.actors', 'target', 'NO', 2],
                ['Movie.actors', 'role', 'YES', 3],
                ['Movie.director', 'source', 'NO', 1],
                ['Movie.director', 'target', 'NO', 2],
                ['Movie.director', 'bar', 'YES', 3],
                ['Person', 'id', 'NO', 1],
                ['Person', '__type__', 'NO', 2],
                ['Person', 'favorite_genre_id', 'YES', 3],
                ['Person', 'first_name', 'NO', 4],
                ['Person', 'full_name', 'NO', 5],
                ['Person', 'last_name', 'YES', 6],
                ['Person', 'username', 'NO', 7],
                ['novel', 'id', 'NO', 1],
                ['novel', '__type__', 'NO', 2],
                ['novel', 'foo', 'YES', 3],
                ['novel', 'genre_id', 'YES', 4],
                ['novel', 'pages', 'NO', 5],
                ['novel', 'title', 'NO', 6],
                ['novel.chapters', 'source', 'NO', 1],
                ['novel.chapters', 'target', 'NO', 2],
            ],
        )

    async def test_sql_query_introspection_02(self):
        tables = await self.squery_values(
            '''
            SELECT
                tbl_name, array_agg(column_name)
            FROM (
                SELECT
                    '"' || table_schema || '"."' || table_name || '"'
                        AS tbl_name,
                    column_name
                FROM information_schema.columns
                ORDER BY tbl_name, ordinal_position
            ) t
            GROUP BY tbl_name
            '''
        )
        for [tbl_name, columns_from_information_schema] in tables:
            if tbl_name.split('.')[0] in ('cfg', 'schema', 'sys', '"ext::ai"'):
                continue

            try:
                prepared = await self.scon.prepare(f'SELECT * FROM {tbl_name}')

                attributes = prepared.get_attributes()
                columns_from_resolver = [a.name for a in attributes]

                self.assertEqual(
                    columns_from_resolver,
                    columns_from_information_schema,
                )
            except Exception:
                raise Exception(f'introspecting {tbl_name}')

    async def test_sql_query_introspection_03(self):
        res = await self.squery_values(
            '''
            SELECT relname FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'pg_toast'
                AND has_schema_privilege(n.oid, 'USAGE')
            ORDER BY relname LIMIT 1
            '''
        )
        # res may be empty
        for [toast_table] in res:
            # Result will probably be empty, so we cannot validate column names
            await self.squery_values(f'SELECT * FROM pg_toast.{toast_table}')

    async def test_sql_query_introspection_04(self):
        res = await self.squery_values(
            '''
            SELECT pc.relname, pa.attname, pa.attnotnull
            FROM pg_attribute pa
            JOIN pg_class pc ON pc.oid = pa.attrelid
            JOIN pg_namespace n ON n.oid = pc.relnamespace
            WHERE n.nspname = 'public' AND pc.relname = 'novel'
            ORDER BY attnum
            '''
        )

        self.assertEqual(
            res,
            [
                ['novel', 'tableoid', True],
                ['novel', 'cmax', True],
                ['novel', 'xmax', True],
                ['novel', 'cmin', True],
                ['novel', 'xmin', True],
                ['novel', 'ctid', True],
                ['novel', 'id', True],
                ['novel', '__type__', True],
                ['novel', 'foo', False],
                ['novel', 'genre_id', False],
                ['novel', 'pages', True],
                ['novel', 'title', True],
            ],
        )

    async def test_sql_query_schemas_01(self):
        await self.scon.fetch('SELECT id FROM "inventory"."Item";')
        await self.scon.fetch('SELECT id FROM "public"."Person";')

        await self.scon.execute('SET search_path TO inventory, public;')
        await self.scon.fetch('SELECT id FROM "Item";')

        await self.scon.execute('SET search_path TO inventory, public;')
        await self.scon.fetch('SELECT id FROM "Person";')

        await self.scon.execute('SET search_path TO public;')
        await self.scon.fetch('SELECT id FROM "Person";')

        await self.scon.execute('SET search_path TO inventory;')
        await self.scon.fetch('SELECT id FROM "Item";')

        await self.scon.execute('SET search_path TO public;')
        with self.assertRaisesRegex(
            asyncpg.UndefinedTableError,
            "unknown table",
            position="16",
        ):
            await self.squery_values('SELECT id FROM "Item"')

        await self.scon.execute('SET search_path TO inventory;')
        with self.assertRaisesRegex(
            asyncpg.UndefinedTableError,
            "unknown table",
            position="17",
        ):
            await self.scon.fetch('SELECT id FROM "Person";')

        await self.scon.execute(
            '''
            SELECT set_config('search_path', '', FALSE);
            '''
        )

        # HACK: Set search_path back to public
        await self.scon.execute('SET search_path TO public;')

    async def test_sql_query_set_01(self):
        # initial state: search_path=public
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [['public']])

        # enter transaction
        tran = self.scon.transaction()
        await tran.start()

        # set
        await self.scon.execute('SET LOCAL search_path TO inventory;')

        await self.scon.fetch('SELECT id FROM "Item";')
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [["inventory"]])

        # finish
        await tran.commit()

        # because we used LOCAL, value should be reset after transaction is over
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [["public"]])

    async def test_sql_query_set_02(self):
        # initial state: search_path=public
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [['public']])

        # enter transaction
        tran = self.scon.transaction()
        await tran.start()

        # set
        await self.scon.execute('SET search_path TO inventory;')

        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [["inventory"]])

        # commit
        await tran.commit()

        # it should still be changed, since we SET was not LOCAL
        await self.scon.fetch('SELECT id FROM "Item";')
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [["inventory"]])

        # reset to default value
        await self.scon.execute('RESET search_path;')
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [["public"]])

    async def test_sql_query_set_03(self):
        # initial state: search_path=public
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [['public']])

        # start
        tran = self.scon.transaction()
        await tran.start()

        # set
        await self.scon.execute('SET search_path TO inventory;')

        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [["inventory"]])

        # rollback
        await tran.rollback()

        # because transaction was rolled back, value should be reset
        res = await self.squery_values('SHOW search_path;')
        self.assertEqual(res, [["public"]])

    async def test_sql_query_set_04(self):
        # database settings allow_user_specified_ids & apply_access_policies_sql
        # should be unified over EdgeQL and SQL adapter

        async def set_current_database(val: Optional[bool]):
            if val is None:
                await self.con.execute(
                    f'''
                    configure current database
                        reset apply_access_policies_sql;
                    '''
                )
            else:
                await self.con.execute(
                    f'''
                    configure current database
                        set apply_access_policies_sql := {str(val).lower()};
                    '''
                )

        async def set_sql(val: Optional[bool]):
            if val is None:
                await self.scon.execute(
                    f'''
                    RESET apply_access_policies_sql;
                    '''
                )
            else:
                await self.scon.execute(
                    f'''
                    SET apply_access_policies_sql TO '{str(val).lower()}';
                    '''
                )

        async def are_policies_applied() -> bool:
            res = await self.squery_values(
                'SELECT title FROM "Content" ORDER BY title'
            )
            return len(res) == 0

        await set_current_database(True)
        await set_sql(True)
        self.assertEqual(await are_policies_applied(), True)

        await set_sql(False)
        self.assertEqual(await are_policies_applied(), False)

        await set_sql(None)
        self.assertEqual(await are_policies_applied(), True)

        await set_current_database(False)
        await set_sql(True)
        self.assertEqual(await are_policies_applied(), True)

        await set_sql(False)
        self.assertEqual(await are_policies_applied(), False)

        await set_sql(None)
        self.assertEqual(await are_policies_applied(), False)

        await set_current_database(None)
        await set_sql(True)
        self.assertEqual(await are_policies_applied(), True)

        await set_sql(False)
        self.assertEqual(await are_policies_applied(), False)

        await set_sql(None)
        self.assertEqual(await are_policies_applied(), False)

        # setting cleanup not needed, since with end with the None, None

    async def test_sql_query_static_eval_01(self):
        res = await self.squery_values('select current_schema;')
        self.assertEqual(res, [['public']])

        await self.squery_values('set search_path to blah;')
        res = await self.squery_values('select current_schema;')
        self.assertEqual(res, [['blah']])

        await self.squery_values('set search_path to blah,foo;')
        res = await self.squery_values('select current_schema;')
        self.assertEqual(res, [['blah']])

        res = await self.squery_values('select current_catalog;')
        self.assertEqual(res, [[self.con.dbname]])

        res = await self.squery_values('select current_schemas(true);')
        self.assertEqual(res, [[['pg_catalog', 'blah', 'foo']]])

    async def test_sql_query_static_eval_02(self):
        await self.scon.execute(
            '''
            SELECT nspname as table_schema,
                relname as table_name
            FROM   pg_class c
            JOIN   pg_namespace n on c.relnamespace = n.oid
            WHERE  has_table_privilege(c.oid, 'SELECT')
            AND    has_schema_privilege(current_user, nspname, 'USAGE')
            AND    relkind in ('r', 'm', 'v', 't', 'f', 'p')
            '''
        )

    async def test_sql_query_static_eval_03(self):
        await self.scon.execute(
            '''
            SELECT information_schema._pg_truetypid(a.*, t.*)
            FROM pg_attribute a
            JOIN pg_type t ON t.oid = a.atttypid
            '''
        )

    async def test_sql_query_static_eval_04(self):
        [[res1, res2]] = await self.squery_values(
            '''
            SELECT to_regclass('"Movie.actors"'),
                '"public"."Movie.actors"'::regclass
            '''
        )
        self.assertEqual(res1, res2)
        assert isinstance(res1, int)
        assert isinstance(res2, int)

        res = await self.squery_values(
            r'''
            SELECT tbloid
            FROM unnest('{11}'::pg_catalog.oid[]) as src(tbloid)
            '''
        )
        self.assertEqual(res, [[11]])

    async def test_sql_query_static_eval_05(self):
        # pg_get_serial_sequence always returns NULL, we don't expose sequences

        res = await self.squery_values(
            '''
            SELECT
              CAST(
                CAST(
                  pg_catalog.pg_get_serial_sequence('a', 'b')
                  AS REGCLASS
                )
                AS OID
              )
            '''
        )
        self.assertEqual(res, [[None]])

    async def test_sql_query_static_eval_06(self):
        # pg_relation_size requires regclass argument

        res = await self.squery_values(
            """
            SELECT relname, pg_relation_size(tables.oid)
            FROM pg_catalog.pg_class AS tables
            JOIN pg_namespace pn ON (pn.oid = tables.relnamespace)
            WHERE tables.relkind = 'r' AND pn.nspname = 'public'
            ORDER BY relname;
            """
        )
        self.assertEqual(
            res,
            [
                ["Book", 8192],
                ["Book.chapters", 8192],
                ["Content", 8192],
                ["ContentSummary", 8192],
                ["Genre", 8192],
                ["Movie", 8192],
                ["Movie.actors", 8192],
                ["Movie.director", 8192],
                ["Person", 8192],
                ["novel", 8192],
                ["novel.chapters", 0],
            ],
        )

    async def test_sql_query_be_state(self):
        con = await self.connect(database=self.con.dbname)
        try:
            await con.execute(
                '''
                CONFIGURE SESSION SET __internal_sess_testvalue := 1;
                '''
            )
            await self.squery_values(
                "set default_transaction_isolation to 'read committed'"
            )
            self.assertEqual(
                await con.query_single(
                    '''
                    SELECT assert_single(cfg::Config.__internal_sess_testvalue)
                    '''
                ),
                1,
            )
            res = await self.squery_values('show default_transaction_isolation')
            self.assertEqual(res, [['read committed']])
        finally:
            await con.aclose()

    async def test_sql_query_client_encoding_1(self):
        rv1 = await self.squery_values('select * from "Genre" order by id')
        await self.squery_values("set client_encoding to 'GBK'")
        rv2 = await self.squery_values('select * from "Genre" order by id')
        self.assertEqual(rv1, rv2)

    async def test_sql_query_client_encoding_2(self):
        await self.squery_values("set client_encoding to 'sql-ascii'")
        await self.squery_values('select * from "Movie"')
        with self.assertRaises(UnicodeDecodeError):
            await self.squery_values('select * from "Genre"')

        await self.squery_values("set client_encoding to 'latin1'")
        with self.assertRaises(asyncpg.UntranslatableCharacterError):
            await self.squery_values('select * from "Genre"')

    async def test_sql_query_server_version(self):
        version = await self.scon.fetchval("show server_version")
        self.assertEqual(
            self.scon.get_server_version(),
            serverversion.split_server_version_string(version),
        )
        with self.assertRaises(asyncpg.CantChangeRuntimeParamError):
            await self.squery_values("set server_version to blah")
        with self.assertRaises(asyncpg.CantChangeRuntimeParamError):
            await self.squery_values("reset server_version")

    async def test_sql_query_server_version_num(self):
        await self.squery_values("show server_version_num")
        with self.assertRaises(asyncpg.CantChangeRuntimeParamError):
            await self.squery_values("set server_version_num to blah")
        with self.assertRaises(asyncpg.CantChangeRuntimeParamError):
            await self.squery_values("reset server_version_num")

    async def test_sql_query_version(self):
        version = await self.scon.fetchrow("select version()")
        self.assertTrue(version["version"].startswith("PostgreSQL "))
        self.assertIn("Gel", version["version"])

    async def test_sql_transaction_01(self):
        await self.scon.execute(
            """
            BEGIN;
            SELECT * FROM "Genre" ORDER BY id;
            COMMIT;
            """,
        )

    async def test_sql_transaction_02(self):
        await self.scon.execute("BEGIN")
        await self.scon.execute(
            "SET TRANSACTION ISOLATION LEVEL read uncommitted"
        )
        v1 = await self.scon.fetchval("SHOW transaction_isolation")
        self.assertEqual(v1, "read uncommitted")
        await self.scon.execute("ROLLBACK")
        v2 = await self.scon.fetchval("SHOW transaction_isolation")
        self.assertNotEqual(v1, v2)

    async def test_sql_query_copy_01(self):
        # copy without columns should select all columns

        out = io.BytesIO()
        await self.scon.copy_from_table(
            "Movie", output=out, format="csv", delimiter="\t"
        )
        out = io.StringIO(out.getvalue().decode("utf-8"))
        res = list(csv.reader(out, delimiter="\t"))

        # should contain columns:
        # id, __type__, director_id, genre_id, release_year, title
        # 0,  1,        2,           3,        4,            5

        self.assertEqual(
            set(row[5] for row in res), {"Forrest Gump", "Saving Private Ryan"}
        )

    async def test_sql_query_copy_02(self):
        # copy of a link table

        out = io.BytesIO()
        await self.scon.copy_from_table(
            "Movie.director", output=out, format="csv", delimiter="\t"
        )
        out = io.StringIO(out.getvalue().decode("utf-8"))
        res = list(csv.reader(out, delimiter="\t"))

        # should contain columns:
        # source, target, @bar
        # 0,      1,      2

        self.assertEqual({row[2] for row in res}, {"bar"})

    async def test_sql_query_copy_03(self):
        # copy of query

        out = io.BytesIO()
        await self.scon.copy_from_query(
            "SELECT 1, 2 UNION ALL SELECT 3, 4",
            output=out,
            format="csv",
            delimiter="\t",
        )
        out = io.StringIO(out.getvalue().decode("utf-8"))
        res = list(csv.reader(out, delimiter="\t"))

        self.assertEqual(res, [['1', '2'], ['3', '4']])

    async def test_sql_query_copy_04(self):
        # copy of table with columns specified

        out = io.BytesIO()
        await self.scon.copy_from_table(
            "Person",
            columns=['first_name', 'full_name'],
            output=out,
            format="csv",
            delimiter="\t",
        )
        out = io.StringIO(out.getvalue().decode("utf-8"))
        res = list(csv.reader(out, delimiter="\t"))
        self.assert_data_shape(
            res,
            tb.bag(
                [
                    ["Robin", "Robin"],
                    ["Steven", "Steven Spielberg"],
                    ["Tom", "Tom Hanks"],
                ]
            ),
        )

    async def test_sql_query_error_01(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="12",
        ):
            await self.scon.execute("SELECT 1 + 'foo'")

    async def test_sql_query_error_02(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="10",
        ):
            await self.scon.execute("SELECT 1+'foo'")

    async def test_sql_query_error_03(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="28",
        ):
            await self.scon.execute(
                """SELECT 1 +
                'foo'"""
            )

    async def test_sql_query_error_04(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="12",
        ):
            await self.scon.execute(
                '''SELECT 1 + 'foo' FROM "Movie" ORDER BY id'''
            )

    async def test_sql_query_error_05(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="28",
        ):
            await self.scon.execute(
                '''SELECT 1 +
                'foo' FROM "Movie" ORDER BY id'''
            )

    async def test_sql_query_error_06(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="12",
        ):
            await self.scon.fetch("SELECT 1 + 'foo'")

    async def test_sql_query_error_07(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="10",
        ):
            await self.scon.fetch("SELECT 1+'foo'")

    async def test_sql_query_error_08(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="28",
        ):
            await self.scon.fetch(
                """SELECT 1 +
                'foo'"""
            )

    async def test_sql_query_error_09(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="12",
        ):
            await self.scon.fetch(
                '''SELECT 1 + 'foo' FROM "Movie" ORDER BY id'''
            )

    async def test_sql_query_error_10(self):
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position="28",
        ):
            await self.scon.fetch(
                '''SELECT 1 +
                'foo' FROM "Movie" ORDER BY id'''
            )

    async def test_sql_query_error_11(self):
        # extended query protocol
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            'invalid input syntax for type uuid',
            position="8",
        ):
            await self.scon.fetch("""SELECT 'bad uuid'::uuid""")

        # simple query protocol
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            'invalid input syntax for type uuid',
            position="8",
        ):
            await self.scon.execute("""SELECT 'bad uuid'::uuid""")

        # test that the connection has not be spuriously closed
        res = await self.squery_values("SELECT 1")
        self.assertEqual(res, [[1]])

    async def test_sql_query_error_12(self):
        tran = self.scon.transaction()
        await tran.start()

        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            'invalid input syntax for type uuid',
            position="8",
        ):
            await self.scon.fetch("""SELECT 'bad uuid'::uuid""")

        await tran.rollback()

        # test that the connection has not be spuriously closed
        res = await self.squery_values("SELECT 1")
        self.assertEqual(res, [[1]])

    async def test_sql_query_error_13(self):
        # forbidden functions

        with self.assertRaisesRegex(
            asyncpg.InsufficientPrivilegeError,
            'forbidden function',
            position="8",
        ):
            await self.scon.fetch("""SELECT pg_ls_dir('/')""")

        res = await self.squery_values("""SELECT pg_is_in_recovery()""")
        self.assertEqual(res, [[False]])

    @unittest.skip("this test flakes: #5783")
    async def test_sql_query_prepare_01(self):
        await self.scon.execute(
            """
            PREPARE ps1(int) AS (
                SELECT $1
            )
            """,
        )

        res = await self.squery_values(
            """
            EXECUTE ps1(100)
            """,
        )
        self.assertEqual(res, [[100]])

        await self.scon.execute(
            """
            DEALLOCATE ps1
            """,
        )

        with self.assertRaises(
            asyncpg.InvalidSQLStatementNameError,
        ):
            await self.scon.execute(
                """
                EXECUTE ps1(101)
                """,
            )

        with self.assertRaises(
            asyncpg.InvalidSQLStatementNameError,
        ):
            await self.scon.execute(
                """
                DEALLOCATE ps1
                """,
            )

        # Prepare again to make sure DEALLOCATE worked
        await self.scon.execute(
            """
            PREPARE ps1(int) AS (
                SELECT $1 + 4
            )
            """,
        )

        res = await self.squery_values(
            """
            EXECUTE ps1(100)
            """,
        )
        self.assertEqual(res, [[104]])

        # Check that simple query works too.
        res = await self.scon.execute(
            """
            EXECUTE ps1(100)
            """,
        )

    async def test_sql_query_prepare_error_01(self):
        query = "PREPARE pserr1 AS (SELECT * FROM \"Movie\" ORDER BY 1 + 'a')"
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            "type integer",
            position=str(len(query) - 3),
        ):
            await self.scon.execute(query)

    async def test_sql_query_empty(self):
        await self.scon.executemany('', args=[])

    async def test_sql_query_pgadmin_hack(self):
        await self.scon.execute("SET DateStyle=ISO;")
        await self.scon.execute("SET client_min_messages=notice;")
        await self.scon.execute(
            "SELECT set_config('bytea_output','hex',false) FROM pg_settings"
            " WHERE name = 'bytea_output'; "
        )
        await self.scon.execute("SET client_encoding='WIN874';")

    async def test_sql_query_computed_01(self):
        # single property
        res = await self.squery_values(
            """
            SELECT full_name
            FROM "Person" p
            ORDER BY first_name
            """
        )
        self.assertEqual(res, [["Robin"], ["Steven Spielberg"], ["Tom Hanks"]])

    async def test_sql_query_computed_02(self):
        # computeds can only be accessed on the table, not rel vars
        with self.assertRaisesRegex(
            asyncpg.PostgresError, "cannot find column `full_name`"
        ):
            await self.squery_values(
                """
                SELECT t.full_name
                FROM (
                    SELECT first_name, last_name
                    FROM "Person"
                ) t
                """
            )

    async def test_sql_query_computed_03(self):
        # computed in a sublink
        res = await self.squery_values(
            """
            SELECT (SELECT 'Hello ' || full_name) as hello
            FROM "Person"
            ORDER BY first_name DESC
            LIMIT 1
            """
        )
        self.assertEqual(res, [["Hello Tom Hanks"]])

    async def test_sql_query_computed_04(self):
        # computed in a lateral
        res = await self.squery_values(
            """
            SELECT t.hello
            FROM "Person",
                LATERAL (SELECT ('Hello ' || full_name) as hello) t
            ORDER BY first_name DESC
            LIMIT 1
            """
        )
        self.assertEqual(res, [["Hello Tom Hanks"]])

    async def test_sql_query_computed_05(self):
        # computed in ORDER BY
        res = await self.squery_values(
            """
            SELECT first_name
            FROM "Person"
            ORDER BY full_name
            """
        )
        self.assertEqual(res, [["Robin"], ["Steven"], ["Tom"]])

    async def test_sql_query_computed_06(self):
        # globals are empty
        res = await self.squery_values(
            """
            SELECT username FROM "Person"
            ORDER BY first_name LIMIT 1
            """
        )
        self.assertEqual(res, [["u_robin"]])

    async def test_sql_query_computed_07(self):
        # single link
        res = await self.scon.fetch(
            """
            SELECT favorite_genre_id FROM "Person"
            """
        )
        self.assert_shape(res, 3, ['favorite_genre_id'])

        res = await self.squery_values(
            """
            SELECT g.name
            FROM "Person" p
            LEFT JOIN "Genre" g ON (p.favorite_genre_id = g.id)
            """
        )
        self.assertEqual(res, [["Drama"], ["Drama"], ["Drama"]])

    @test.not_implemented("multi computed properties are not implemented")
    async def test_sql_query_computed_08(self):
        # multi property
        await self.scon.fetch(
            """
            SELECT actor_names FROM "Movie"
            """
        )

    @test.not_implemented("multi computed links are not implemented")
    async def test_sql_query_computed_09(self):
        # multi link
        await self.scon.fetch(
            """
            SELECT similar_to FROM "Movie"
            """
        )

    async def test_sql_query_computed_10(self):
        # globals

        tran = self.scon.transaction()
        await tran.start()

        await self.scon.execute(
            """
            SET LOCAL "global default::username_prefix" TO user_;
            """
        )

        res = await self.squery_values(
            """
            SELECT username FROM "Person"
            ORDER BY first_name LIMIT 1
            """
        )
        self.assertEqual(res, [["user_robin"]])

        await tran.rollback()

    async def test_sql_query_computed_11(self):
        # globals

        tran = self.scon.transaction()
        await tran.start()

        await self.scon.execute(
            f"""
            SET LOCAL "global glob_mod::glob_str" TO hello;
            SET LOCAL "global glob_mod::glob_uuid" TO
                'f527c032-ad4c-461e-95e2-67c4e2b42ca7';
            SET LOCAL "global glob_mod::glob_int64" TO 42;
            SET LOCAL "global glob_mod::glob_int32" TO 42;
            SET LOCAL "global glob_mod::glob_int16" TO 42;
            SET LOCAL "global glob_mod::glob_bool" TO 1;
            SET LOCAL "global glob_mod::glob_float64" TO 42.1;
            SET LOCAL "global glob_mod::glob_float32" TO 42.1;
            """
        )
        await self.scon.execute('INSERT INTO glob_mod."G" DEFAULT VALUES')

        res = await self.squery_values(
            '''
            SELECT
                p_str,
                p_uuid,
                p_int64,
                p_int32,
                p_int16,
                p_bool,
                p_float64,
                p_float32
            FROM glob_mod."G"
            '''
        )
        self.assertEqual(
            res,
            [
                [
                    'hello',
                    uuid.UUID('f527c032-ad4c-461e-95e2-67c4e2b42ca7'),
                    42,
                    42,
                    42,
                    True,
                    42.1,
                    42.099998474121094,
                ]
            ],
        )

        await tran.rollback()

    async def test_sql_query_access_policy_01(self):
        tran = self.scon.transaction()
        await tran.start()

        # no access policies
        res = await self.squery_values(
            'SELECT title FROM "Content" ORDER BY title'
        )
        self.assertEqual(
            res,
            [
                ['Chronicles of Narnia'],
                ['Forrest Gump'],
                ['Halo 3'],
                ['Hunger Games'],
                ['Saving Private Ryan'],
            ],
        )

        await self.scon.execute('SET LOCAL apply_access_policies_sql TO true')

        # access policies applied
        res = await self.squery_values(
            'SELECT title FROM "Content" ORDER BY title'
        )
        self.assertEqual(res, [])

        # access policies use globals
        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'Forrest Gump'"""
        )
        res = await self.squery_values(
            'SELECT title FROM "Content" ORDER BY title'
        )
        self.assertEqual(res, [['Forrest Gump']])

        await tran.rollback()

    async def test_sql_query_access_policy_02(self):
        # access policies from computeds

        tran = self.scon.transaction()
        await tran.start()

        # no access policies
        res = await self.squery_values('SELECT x FROM "ContentSummary"')
        self.assertEqual(res, [[5]])

        await self.scon.execute('SET LOCAL apply_access_policies_sql TO true')

        # access policies applied
        res = await self.squery_values('SELECT x FROM "ContentSummary"')
        self.assertEqual(res, [[0]])

        # access policies use globals
        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'Forrest Gump'"""
        )
        res = await self.squery_values('SELECT x FROM "ContentSummary"')
        self.assertEqual(res, [[1]])

        await tran.rollback()

    async def test_sql_query_access_policy_03(self):
        # access policies for dml

        tran = self.scon.transaction()
        await tran.start()

        # allowed without applying access policies

        await self.scon.execute('SET LOCAL apply_access_policies_sql TO true')

        # allowed when filter_title == 'summary'
        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'summary'"""
        )

        # not allowed when filter_title is something else
        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'something else'"""
        )
        with self.assertRaisesRegex(
            asyncpg.exceptions.InsufficientPrivilegeError,
            'access policy violation on insert of default::ContentSummary',
        ):
            await self.scon.execute(
                'INSERT INTO "ContentSummary" DEFAULT VALUES'
            )

        await tran.rollback()

    async def test_sql_query_access_policy_04(self):
        # access policies without inheritance

        tran = self.scon.transaction()
        await tran.start()

        # there is only one object that is of exactly type Content
        res = await self.squery_values('SELECT * FROM ONLY "Content"')
        self.assertEqual(len(res), 1)

        await self.scon.execute('SET LOCAL apply_access_policies_sql TO true')

        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'Halo 3'"""
        )
        res = await self.squery_values('SELECT * FROM ONLY "Content"')
        self.assertEqual(len(res), 1)

        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'Forrest Gump'"""
        )
        res = await self.squery_values('SELECT * FROM ONLY "Content"')
        self.assertEqual(len(res), 0)

        await tran.rollback()

    async def test_native_sql_query_00(self):
        await self.assert_sql_query_result(
            """
                SELECT
                    1 AS a,
                    'two' AS b,
                    to_json('three') AS c,
                    timestamp '2000-12-16 12:21:13' AS d,
                    timestamp with time zone '2000-12-16 12:21:13' AS e,
                    date '0001-01-01 AD' AS f,
                    interval '2000 years' AS g,
                    ARRAY[1, 2, 3] AS h,
                    FALSE AS i
            """,
            [{
                "a": 1,
                "b": "two",
                "c": '"three"',
                "d": "2000-12-16T12:21:13",
                "e": "2000-12-16T12:21:13+00:00",
                "f": "0001-01-01",
                "g": edgedb.RelativeDuration(months=2000 * 12),
                "h": [1, 2, 3],
                "i": False,
            }]
        )

    async def test_native_sql_query_01(self):
        await self.assert_sql_query_result(
            """
                SELECT
                    "Movie".title,
                    "Genre".name AS genre
                FROM
                    "Movie",
                    "Genre"
                WHERE
                    "Movie".genre_id = "Genre".id
                    AND "Genre".name = 'Drama'
                ORDER BY
                    title
            """,
            [{
                "title": "Forrest Gump",
                "genre": "Drama",
            }, {
                "title": "Saving Private Ryan",
                "genre": "Drama",
            }]
        )

    async def test_native_sql_query_02(self):
        await self.assert_sql_query_result(
            """
                SELECT
                    "Movie".title,
                    "Genre".name AS genre
                FROM
                    "Movie",
                    "Genre"
                WHERE
                    "Movie".genre_id = "Genre".id
                    AND "Genre".name = $1::text
                    AND length("Movie".title) > $2::int
                ORDER BY
                    title
            """,
            [{
                "title": "Saving Private Ryan",
                "genre": "Drama",
            }],
            variables={
                "0": "Drama",
                "1": 14,
            },
        )

    async def test_native_sql_query_03(self):
        # No output at all
        await self.assert_sql_query_result(
            """
                SELECT
                WHERE NULL
            """,
            [],
        )

        # Empty tuples
        await self.assert_sql_query_result(
            """
                SELECT
                FROM "Movie"
                LIMIT 1
            """,
            [{}],
        )
