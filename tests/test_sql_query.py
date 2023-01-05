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

import os.path
import asyncpg

from edb.testbase import server as tb
from edb.tools import test


class TestSQL(tb.SQLQueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'movies.esdl')

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas', 'movies_setup.edgeql'
    )

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
        self.assert_shape(res, 5, 3, ['id', 'genre_id', 'title'])

    async def test_sql_query_03(self):
        # SELECT FROM parent type only
        res = await self.scon.fetch(
            '''
            SELECT * FROM ONLY "Content" -- should have only one result
            '''
        )
        self.assert_shape(res, 1, 3, ['id', 'genre_id', 'title'])

    async def test_sql_query_04(self):
        # multiple FROMs
        res = await self.scon.fetch(
            '''
            SELECT mve.title, "Person".first_name
            FROM "Movie" mve, "Person" WHERE mve.director_id = "Person".id
            '''
        )
        self.assert_shape(res, 1, 2, ['title', 'first_name'])

    async def test_sql_query_05(self):
        res = await self.scon.fetch(
            '''
            SeLeCt mve.title as tiT, perSon.first_name
            FROM "Movie" mve, "Person" person
            '''
        )
        self.assert_shape(res, 6, 2, ['tit', 'first_name'])

    async def test_sql_query_06(self):
        # sub relations
        res = await self.scon.fetch(
            '''
            SELECT id, title, prS.first_name
            FROM "Movie" mve, (SELECT first_name FROM "Person") prs
            '''
        )
        self.assert_shape(res, 6, 3, ['id', 'title', 'first_name'])

    async def test_sql_query_07(self):
        # quoted case sensitive
        res = await self.scon.fetch(
            '''
            SELECT tItLe, release_year "RL year" FROM "Movie" ORDER BY titLe;
            '''
        )
        self.assert_shape(res, 2, 2, ['title', 'RL year'])

    async def test_sql_query_08(self):
        # JOIN
        res = await self.scon.fetch(
            '''
            SELECT "Movie".id, "Genre".id
            FROM "Movie" JOIN "Genre" ON "Movie".genre_id = "Genre".id
            '''
        )
        self.assert_shape(res, 2, 2, ['id', 'id'])

    async def test_sql_query_09(self):
        # resolve columns without table names
        res = await self.scon.fetch(
            '''
            SELECT "Movie".id, title, name
            FROM "Movie" JOIN "Genre" ON "Movie".genre_id = "Genre".id
            '''
        )
        self.assert_shape(res, 2, 3, ['id', 'title', 'name'])

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
            5,
            ['id', 'director_id', 'genre_id', 'release_year', 'title'],
        )

    async def test_sql_query_11(self):
        # multiple wildcard SELECT
        res = await self.scon.fetch(
            '''
            SELECT * FROM "Movie"
            JOIN "Genre" g ON "Movie".genre_id = "Genre".id
            '''
        )
        self.assert_shape(res, 2, 7)

    async def test_sql_query_12(self):
        # JOIN USING
        res = await self.scon.fetch(
            '''
            SELECT * FROM "Movie"
            JOIN (SELECT id as genre_id, name FROM "Genre") g USING (genre_id)
            '''
        )
        self.assert_shape(res, 2, 7)

    async def test_sql_query_13(self):
        # CTE
        res = await self.scon.fetch(
            '''
            WITH g AS (SELECT id as genre_id, name FROM "Genre")
            SELECT * FROM "Movie" JOIN g USING (genre_id)
            '''
        )
        self.assert_shape(res, 2, 7)

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
            asyncpg.UndefinedTableError, "unknown table"
        ):
            await self.scon.fetch('SELECT title FROM "Novel" ORDER BY title')

    async def test_sql_query_26(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedTableError, "unknown table"
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
        self.assert_shape(res, 2, 2, ['name', 'title'])

    async def test_sql_query_29(self):
        # link tables

        # multi
        res = await self.scon.fetch('SELECT * FROM "Movie.actors"')
        self.assert_shape(res, 3, 3, ['role', 'source', 'target'])

        # single with properties
        res = await self.scon.fetch('SELECT * FROM "Movie.director"')
        self.assert_shape(res, 1, 3, ['bar', 'source', 'target'])

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
        self.assert_shape(res, 2, 2, ['c', 'd'])

        with self.assertRaisesRegex(
            asyncpg.InvalidColumnReferenceError, "query resolves to 2"
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
        self.assert_shape(res, 1, 2, ['a', 'b'])

        res = await self.scon.fetch(
            '''
            with common(c, d) as (SELECT 1 a, 2 b)
            SELECT * FROM common
            '''
        )
        self.assert_shape(res, 1, 2, ['c', 'd'])

        res = await self.scon.fetch(
            '''
            with common(c, d) as (SELECT 1 a, 2 b)
            SELECT * FROM common as cmn(e, f)
            '''
        )
        self.assert_shape(res, 1, 2, ['e', 'f'])

        with self.assertRaisesRegex(
            asyncpg.InvalidColumnReferenceError, "query resolves to 2"
        ):
            await self.scon.fetch(
                '''
                with common(c, d) as (SELECT 1 a, 2 b)
                SELECT * FROM common as cmn(e, f, g)
                '''
            )

    @test.xerror('asyncpg type inspection query not supported')
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
        self.assert_shape(res, 3, 4, ['a', 'b', 'unnest', 'unnest'])

        res = await self.scon.fetch(
            '''
            SELECT unnest(ARRAY[1, 2, 3]) a
            '''
        )
        self.assert_shape(res, 3, 1, ['a'])

        res = await self.scon.fetch(
            '''
            SELECT *, unnested_b + 1 computed
            FROM
                (SELECT ARRAY[1, 2, 3] a, ARRAY[4, 5, 6] b) t,
                LATERAL unnest(a, b) awesome_table(unnested_a, unnested_b)
            '''
        )
        self.assert_shape(
            res, 3, 5, ['a', 'b', 'unnested_a', 'unnested_b', 'computed']
        )

    async def test_sql_query_introspection_00(self):
        res = await self.squery_values(
            '''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
            '''
        )
        self.assertEqual(
            res,
            [
                ['Book'],
                ['Book.chapters'],
                ['Content'],
                ['Genre'],
                ['Movie'],
                ['Movie.actors'],
                ['Movie.director'],
                ['Person'],
                ['novel'],
                ['novel.chapters'],
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
                ['Book', 'genre_id', 'YES', 2],
                ['Book', 'pages', 'NO', 3],
                ['Book', 'title', 'NO', 4],
                ['Book.chapters', 'source', 'NO', 1],
                ['Book.chapters', 'target', 'NO', 2],
                ['Content', 'id', 'NO', 1],
                ['Content', 'genre_id', 'YES', 2],
                ['Content', 'title', 'NO', 3],
                ['Genre', 'id', 'NO', 1],
                ['Genre', 'name', 'NO', 2],
                ['Movie', 'id', 'NO', 1],
                ['Movie', 'director_id', 'YES', 2],
                ['Movie', 'genre_id', 'YES', 3],
                ['Movie', 'release_year', 'YES', 4],
                ['Movie', 'title', 'NO', 5],
                ['Movie.actors', 'role', 'YES', 1],
                ['Movie.actors', 'source', 'NO', 2],
                ['Movie.actors', 'target', 'YES', 3],
                ['Movie.director', 'bar', 'YES', 1],
                ['Movie.director', 'source', 'NO', 2],
                ['Movie.director', 'target', 'YES', 3],
                ['Person', 'id', 'NO', 1],
                ['Person', 'first_name', 'NO', 2],
                ['Person', 'last_name', 'YES', 3],
                ['novel', 'id', 'NO', 1],
                ['novel', 'foo', 'YES', 2],
                ['novel', 'genre_id', 'YES', 3],
                ['novel', 'pages', 'NO', 4],
                ['novel', 'title', 'NO', 5],
                ['novel.chapters', 'source', 'NO', 1],
                ['novel.chapters', 'target', 'NO', 2],
            ],
        )

    @test.xerror('asyncpg type inspection query not supported')
    async def test_sql_query_introspection_02(self):
        tables = await self.squery_values(
            '''
            SELECT
                tbl_name, array_agg(column_name)
            FROM (
                SELECT
                    table_schema || '."' || table_name || '"' as tbl_name,
                    column_name
                FROM information_schema.columns
                ORDER BY tbl_name, ordinal_position
            ) t
            GROUP BY tbl_name
            '''
        )
        for [table_name, columns_from_information_schema] in tables:
            if table_name.split('.')[0] in ('cfg', 'schema', 'sys'):
                continue

            try:
                prepared = await self.scon.prepare(
                    f'SELECT * FROM {table_name}'
                )

                attributes = prepared.get_attributes()
                columns_from_resolver = [a.name for a in attributes]

                self.assertEqual(
                    columns_from_resolver,
                    columns_from_information_schema,
                )
            except Exception:
                raise Exception(f'introspecting {table_name}')

    @test.skip("unimplemented")
    async def test_sql_schemas(self):
        await self.migrate(
            '''
            mod my_module {
                type Foo;
            }
            '''
        )
        await self.squery('SELECT id FROM "my_module"."Foo";')
        await self.squery('SELECT id FROM "public"."Person";')

        await self.squery(
            '''
            SET search_path TO my_module, public;
            SELECT id FROM "Foo";
            '''
        )
        await self.squery(
            '''
            SET search_path TO my_module, public;
            SELECT id FROM "Person";
            '''
        )
        await self.squery(
            '''
            SET search_path TO public;
            SELECT id FROM "Person";
            '''
        )
        await self.squery(
            '''
            SET search_path TO my_module;
            SELECT id FROM "Foo";
            '''
        )

        with self.assertRaisesRegexTx(
            asyncpg.UndefinedTableError, "unknown table"
        ):
            await self.squery('SELECT id FROM "Foo"')

        with self.assertRaisesRegexTx(
            asyncpg.UndefinedTableError, "unknown table"
        ):
            await self.squery(
                '''
                SET search_path TO my_module;
                SELECT id FROM "Person";
                '''
            )
