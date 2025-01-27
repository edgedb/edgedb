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
import csv
import decimal
import io
import os.path
import subprocess
from typing import Coroutine, Optional, Tuple
import unittest
import uuid

from edb.tools import test
from edb.server import pgcluster
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

        create global glob_mod::a: str;
        create global glob_mod::b: bool;
        create type glob_mod::Computed {
            create property a := global glob_mod::a;
            create property b := global glob_mod::b;
        };
        insert glob_mod::Computed;
        ''',
        os.path.join(
            os.path.dirname(__file__), 'schemas', 'movies_setup.edgeql'
        ),
    ]

    async def test_sql_query_psql_describe_01(self):
        dsn = self.get_sql_proto_dsn()
        pg_bin_dir = await pgcluster.get_pg_bin_dir()

        # Run a describe command in psql
        cmd = [
            pg_bin_dir / 'psql',
            '--dbname', dsn,
            '-c',
            '\\d "Person"',
        ]
        try:
            subprocess.run(
                cmd,
                input=None,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
        except subprocess.CalledProcessError as e:
            raise AssertionError(
                f'command {cmd} returned non-zero exit status '
                f'{e.returncode}\n{e.output}'
            ) from e

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
                'g_id',
                'g___type__',
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
        self.assert_shape(res, 3, ['source', 'target', 'role', 'role_lower'])

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
        await self.scon.execute('SET LOCAL apply_access_policies_pg TO true')
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

    async def test_sql_query_38a(self):
        res = await self.squery_values(
            'VALUES (1), (NULL)'
        )
        self.assertEqual(res, [[1], [None]])

        res = await self.squery_values(
            'VALUES (NULL), (1)'
        )
        self.assertEqual(res, [[None], [1]])

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
        from asyncpg.types import BitString

        # bit string literal
        res = await self.squery_values("SELECT x'00abcdef00';")
        self.assertEqual(res, [[BitString.frombytes(b'\x00\xab\xcd\xef\x00')]])

        res = await self.squery_values("SELECT x'01001ab';")
        self.assertEqual(
            res, [[BitString.frombytes(b'\x01\x00\x1a\xb0', bitlength=28)]]
        )

        res = await self.squery_values("SELECT b'101';")
        self.assertEqual(res, [[BitString.frombytes(b'\xa0', bitlength=3)]])

    async def test_sql_query_42(self):
        # params out of order

        res = await self.squery_values(
            'SELECT $2::int, $3::bool, $1::text',
            'hello',
            42,
            True,
        )
        self.assertEqual(res, [[42, True, 'hello']])

        res = await self.scon.execute(
            '''
            UPDATE "Book" SET pages = $1 WHERE (title = $2)
            ''',
            207,
            'Chronicles of Narnia',
        )
        self.assertEqual(res, 'UPDATE 1')

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
            asyncpg.UndefinedColumnError, 'column \"name\" does not exist'
        ):
            await self.squery_values('SELECT name FROM User')

        val = await self.scon.fetch('SELECT * FROM User')
        self.assert_shape(val, 1, ['user'])
        self.assertEqual(tuple(val[0].values()), ('admin',))

    async def test_sql_query_45(self):
        res = await self.scon.fetch('SELECT 1 AS a, 2 AS a')
        self.assert_shape(res, 1, ['a', 'a'])

    async def test_sql_query_46(self):
        res = await self.scon.fetch(
            '''
            WITH
              x(a) AS (VALUES (1)),
              y(a) AS (VALUES (2)),
              z(a) AS (VALUES (3))
            SELECT * FROM x, y JOIN z u on TRUE
            '''
        )

        # `a` would be duplicated,
        # so second and third instance are prefixed with rel var name
        self.assert_shape(res, 1, ['a', 'y_a', 'u_a'])

    async def test_sql_query_47(self):
        res = await self.scon.fetch(
            '''
            WITH
              x(a) AS (VALUES (1)),
              y(a) AS (VALUES (2), (3))
            SELECT x.*, u.* FROM x, y as u
            '''
        )
        self.assert_shape(res, 2, ['a', 'u_a'])

    async def test_sql_query_48(self):
        res = await self.scon.fetch(
            '''
            WITH
              x(a) AS (VALUES (1)),
              y(a) AS (VALUES (2), (3))
            SELECT * FROM x, y, y
            '''
        )

        # duplicate rel var names can yield duplicate column names
        self.assert_shape(res, 4, ['a', 'y_a', 'y_a'])

    async def test_sql_query_49(self):
        res = await self.scon.fetch(
            '''
            WITH
              x(a) AS (VALUES (2))
            SELECT 1 as x_a, * FROM x, x
            '''
        )

        # duplicate rel var names can yield duplicate column names
        self.assert_shape(res, 1, ['x_a', 'a', 'x_a'])

    async def test_sql_query_50(self):
        res = await self.scon.fetch(
            '''
            WITH
              x(a) AS (VALUES (2))
            SELECT 1 as a, * FROM x
            '''
        )

        # duplicate rel var names can yield duplicate column names
        self.assert_shape(res, 1, ['a', 'x_a'])

    async def test_sql_query_51(self):
        res = await self.scon.fetch(
            '''
            TABLE "Movie"
            '''
        )
        self.assert_shape(res, 2, 6)

    async def test_sql_query_52(self):
        async def count_table(only: str, table_name: str) -> int:
            res = await self.squery_values(
                f'SELECT COUNT(*) FROM {only} "default::links"."{table_name}"'
            )
            return res[0][0]

        # link tables must include elements of link's children
        self.assertEqual(await count_table("", "C.a"), 2)
        self.assertEqual(await count_table("ONLY", "C.a"), 2)
        self.assertEqual(await count_table("", "B.a"), 2)
        self.assertEqual(await count_table("ONLY", "B.a"), 0)
        # same for property tables
        self.assertEqual(await count_table("", "C.prop"), 1)
        self.assertEqual(await count_table("ONLY", "C.prop"), 1)
        self.assertEqual(await count_table("", "B.prop"), 1)
        self.assertEqual(await count_table("ONLY", "B.prop"), 0)
        # same for multi property tables
        self.assertEqual(await count_table("", "C.vals"), 4)
        self.assertEqual(await count_table("ONLY", "C.vals"), 4)
        self.assertEqual(await count_table("", "B.vals"), 4)
        self.assertEqual(await count_table("ONLY", "B.vals"), 0)

    async def test_sql_query_53(self):
        await self.scon.execute(
            '''
            SELECT 'hello' as t;
            SELECT 42 as i;
            '''
        )

        # query params will make asyncpg use the extended protocol,
        # where you can issue only one statement.
        with self.assertRaisesRegex(
            asyncpg.PostgresSyntaxError,
            'cannot insert multiple commands into a prepared statement',
        ):
            await self.scon.execute(
                '''
                SELECT $1::text as t;
                SELECT $2::int as i;
                ''',
                'hello',
                42,
            )

    async def test_sql_query_54(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedParameterError, 'there is no parameter \\$0'
        ):
            await self.scon.fetch(
                '''
                SELECT $0::text as t;
                ''',
                'hello',
            )

    async def test_sql_query_55(self):
        res = await self.squery_values(
            '''
            SELECT ROW(TRUE, 1, 1.1, 'hello', x'012', b'001')
            ''',
        )
        self.assertEqual(res, [[
            (
                True,
                1,
                decimal.Decimal('1.1'),
                'hello',
                asyncpg.BitString.frombytes(b'\x01\x20', 12),
                asyncpg.BitString.frombytes(b'\x20', 3),
            )
        ]])

    async def test_sql_query_56(self):
        # recursive

        res = await self.squery_values(
            '''
            WITH RECURSIVE
              integers(n) AS (
                  SELECT 0
                UNION ALL
                  SELECT n + 1 FROM integers
                  WHERE n + 1 < 5
              )
            SELECT n FROM integers
            ''',
        )
        self.assertEqual(res, [
            [0],
            [1],
            [2],
            [3],
            [4],
        ])

        res = await self.squery_values(
            '''
            WITH RECURSIVE
              fibonacci(n, prev, val) AS (
                  SELECT 1, 0, 1
                UNION ALL
                  SELECT n + 1, val, prev + val
                  FROM fibonacci
                  WHERE n + 1 < 10
              )
            SELECT n, val FROM fibonacci;
            '''
        )
        self.assertEqual(res, [
            [1, 1],
            [2, 1],
            [3, 2],
            [4, 3],
            [5, 5],
            [6, 8],
            [7, 13],
            [8, 21],
            [9, 34],
        ])

        res = await self.squery_values(
            '''
            WITH RECURSIVE
              fibonacci(n, prev, val) AS (
                  SELECT 1, 0, 1
                UNION ALL
                  SELECT n + 1, val, prev + val
                  FROM fibonacci
                  WHERE n + 1 < 8
              ),
              integers(n) AS (
                  SELECT 0
                UNION ALL
                  SELECT n + 1 FROM integers
                  WHERE n + 1 < 5
              )
            SELECT f.n, f.val FROM fibonacci f, integers i where f.n = i.n;
            '''
        )
        self.assertEqual(res, [
            [1, 1],
            [2, 1],
            [3, 2],
            [4, 3],
        ])

        res = await self.squery_values(
            '''
            WITH RECURSIVE
              a as (SELECT 12 as n),
              integers(n) AS (
                  SELECT 0
                UNION ALL
                  SELECT n + 1 FROM integers
                  WHERE n + 1 < 5
              )
            SELECT * FROM a, integers;
            '''
        )
        self.assertEqual(res, [
            [12, 0],
            [12, 1],
            [12, 2],
            [12, 3],
            [12, 4],
        ])

    async def test_sql_query_57(self):
        res = await self.squery_values(
            f'''
            (select 1 limit 1) union (select 2 limit 1);
            '''
        )
        self.assertEqual(
            res,
            [
                [1],
                [2],
            ]
        )

        res = await self.squery_values(
            f'''
            (select 1) union (select 2) LIMIT $1;
            ''',
            1
        )
        self.assertEqual(
            res,
            [
                [1],
            ]
        )

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
                ['public::links', 'A'],
                ['public::links', 'B'],
                ['public::links', 'B.a'],
                ['public::links', 'B.prop'],
                ['public::links', 'B.vals'],
                ['public::links', 'C'],
                ['public::links', 'C.a'],
                ['public::links', 'C.prop'],
                ['public::links', 'C.vals'],
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
                ['Movie.actors', 'role_lower', 'YES', 4],
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

    async def test_sql_query_introspection_05(self):
        # test pg_constraint

        res = await self.squery_values(
            '''
            SELECT pc.relname, pcon.contype, pa.key, pcf.relname, paf.key
            FROM pg_constraint pcon
            JOIN pg_class pc ON pc.oid = pcon.conrelid
            LEFT JOIN pg_class pcf ON pcf.oid = pcon.confrelid
            LEFT JOIN LATERAL (
                SELECT string_agg(attname, ',') as key
                FROM pg_attribute
                WHERE attrelid = pcon.conrelid
                  AND attnum = ANY(pcon.conkey)
            ) pa ON TRUE
            LEFT JOIN LATERAL (
                SELECT string_agg(attname, ',') as key
                FROM pg_attribute
                WHERE attrelid = pcon.confrelid
                  AND attnum = ANY(pcon.confkey)
            ) paf ON TRUE
            WHERE pc.relname IN (
                'Book.chapters', 'Movie', 'Movie.director', 'Movie.actors'
            )
            ORDER BY pc.relname ASC, pcon.contype DESC, pa.key
            '''
        )

        self.assertEqual(
            res,
            [
                ['Book.chapters', b'f', 'source', 'Book', 'id'],
                ['Movie', b'p', 'id', None, None],
                ['Movie', b'f', 'director_id', 'Person', 'id'],
                ['Movie', b'f', 'genre_id', 'Genre', 'id'],
                ['Movie.actors', b'p', 'source,target', None, None],
                ['Movie.actors', b'f', 'source', 'Movie', 'id'],
                ['Movie.actors', b'f', 'target', 'Person', 'id'],
                ['Movie.director', b'p', 'source,target', None, None],
                ['Movie.director', b'f', 'source', 'Movie', 'id'],
                ['Movie.director', b'f', 'target', 'Person', 'id'],
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

        res = await self.squery_values('select current_schemas(false);')
        self.assertEqual(res, [[['blah', 'foo']]])

        # Make sure the static evaluation doesn't get cached incorrectly.
        res = await self.squery_values('select current_schemas(true);')
        self.assertEqual(res, [[['pg_catalog', 'blah', 'foo']]])

        with self.assertRaises(asyncpg.UndefinedFunctionError):
            await self.squery_values('select current_schemas($1);')

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
            LIMIT 500
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

    @test.xerror('TODO')
    async def test_sql_query_static_eval_05a(self):
        # This fails becuase params are not in the compiled query and postgres
        # cannot infer the type of params.

        res = await self.squery_values(
            '''
            SELECT pg_catalog.pg_get_serial_sequence($1, $2)
            ''',
            'a',
            'b',
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

    async def test_sql_native_query_static_eval_01(self):
        await self.assert_sql_query_result(
            'select current_schemas(false);',
            [{'current_schemas': ['public']}],
        )
        await self.assert_sql_query_result(
            'select current_schemas(true);',
            [{'current_schemas': ['pg_catalog', 'public']}],
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
        self.assertEqual(
            self.scon.get_settings().client_encoding.lower(), "utf_8"
        )
        rv1 = await self.squery_values('select * from "Genre" order by id')
        await self.squery_values("set client_encoding to 'GBK'")
        rv2 = await self.squery_values('select * from "Genre" order by id')
        self.assertEqual(
            self.scon.get_settings().client_encoding.lower(), "gbk"
        )
        self.assertEqual(rv1, rv2)

    async def test_sql_query_client_encoding_2(self):
        await self.squery_values("set client_encoding to 'sql-ascii'")
        self.assertEqual(
            self.scon.get_settings().client_encoding.lower(), "sql_ascii"
        )
        await self.squery_values('select * from "Movie"')
        with self.assertRaises(UnicodeDecodeError):
            await self.squery_values('select * from "Genre"')

        await self.squery_values("set client_encoding to 'latin1'")
        self.assertEqual(
            self.scon.get_settings().client_encoding.lower(), "latin1"
        )
        with self.assertRaises(asyncpg.UntranslatableCharacterError):
            await self.squery_values('select * from "Genre"')

        # Bug workaround: because of MagicStack/asyncpg#1215, if an
        # error occurs inside a transaction where a config was set,
        # when the transaction is rolled back the client-side version
        # of that config is not reverted. This was causing other tests
        # to fail with encoding errors.
        # Get things back into a good state.
        await self.stran.rollback()
        self.stran = self.scon.transaction()
        await self.stran.start()
        # ... need to change it away then change it back to have it show up
        await self.squery_values("set client_encoding to 'latin1'")
        await self.squery_values("set client_encoding to 'UTF8'")
        self.assertEqual(
            self.scon.get_settings().client_encoding.lower(), "utf8"
        )

    async def test_sql_query_client_encoding_3(self):
        non_english = "奇奇怪怪"
        rv1 = await self.squery_values('select $1::text', non_english)
        await self.squery_values("set client_encoding to 'GBK'")
        rv2 = await self.squery_values('select $1::text', non_english)
        self.assertEqual(
            self.scon.get_settings().client_encoding.lower(), "gbk"
        )
        self.assertEqual(rv1, rv2)

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

    async def test_sql_query_copy_05(self):
        # copy of a link table with link prop

        out = io.BytesIO()
        await self.scon.copy_from_table(
            "Movie.actors",
            output=out,
            format="csv",
            delimiter="\t",
        )
        out = io.StringIO(out.getvalue().decode("utf-8"))
        res = list(csv.reader(out, delimiter="\t"))

        # columns: 0      1      2
        #          source target role
        self.assertEqual(
            {row[2] for row in res},
            {"Captain Miller", ""}
        )

    async def test_sql_query_error_01(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="12",
        ):
            await self.scon.execute("SELECT 1 + asdf()")

    async def test_sql_query_error_02(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="10",
        ):
            await self.scon.execute("SELECT 1+asdf()")

    async def test_sql_query_error_03(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="28",
        ):
            await self.scon.execute(
                """SELECT 1 +
                asdf()"""
            )

    async def test_sql_query_error_04(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="12",
        ):
            await self.scon.execute(
                '''SELECT 1 + asdf() FROM "Movie" ORDER BY id'''
            )

    async def test_sql_query_error_05(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="28",
        ):
            await self.scon.execute(
                '''SELECT 1 +
                asdf() FROM "Movie" ORDER BY id'''
            )

    async def test_sql_query_error_06(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="12",
        ):
            await self.scon.fetch("SELECT 1 + asdf()")

    async def test_sql_query_error_07(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="10",
        ):
            await self.scon.fetch("SELECT 1+asdf()")

    async def test_sql_query_error_08(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="28",
        ):
            await self.scon.fetch(
                """SELECT 1 +
                asdf()"""
            )

    async def test_sql_query_error_09(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="12",
        ):
            await self.scon.fetch(
                '''SELECT 1 + asdf() FROM "Movie" ORDER BY id'''
            )

    async def test_sql_query_error_10(self):
        with self.assertRaisesRegex(
            asyncpg.UndefinedFunctionError,
            "does not exist",
            position="28",
        ):
            await self.scon.fetch(
                '''SELECT 1 +
                asdf() FROM "Movie" ORDER BY id'''
            )

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
            asyncpg.UndefinedColumnError, "column \"full_name\" does not exist"
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

    @test.skip("This is flaking in CI")
    async def test_sql_query_computed_10(self):
        # globals

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

    @test.skip("This is flaking in CI")
    async def test_sql_query_computed_11(self):
        # globals

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

    async def test_sql_query_computed_12(self):
        # globals
        res = await self.squery_values(
            '''
            SELECT a, b FROM glob_mod."Computed"
            '''
        )
        self.assertEqual(res, [[None, None]])

        await self.scon.execute(
            f"""
            SET LOCAL "global glob_mod::a" TO hello;
            SET LOCAL "global glob_mod::b" TO no;
            """
        )

        res = await self.squery_values(
            '''
            SELECT a, b FROM glob_mod."Computed"
            '''
        )
        self.assertEqual(res, [["hello", False]])

    async def test_sql_query_computed_13(self):
        # globals bool values

        async def query_glob_bool(value: str) -> bool:
            await self.scon.execute(
                f"""
                SET LOCAL "global glob_mod::b" TO {value};
                """
            )
            res = await self.squery_values(
                '''
                SELECT b FROM glob_mod."Computed"
                '''
            )
            return res[0][0]

        self.assertEqual(await query_glob_bool('on'), True)
        self.assertEqual(await query_glob_bool('off'), False)
        self.assertEqual(await query_glob_bool('o'), None)
        self.assertEqual(await query_glob_bool('of'), False)
        self.assertEqual(await query_glob_bool('true'), True)
        self.assertEqual(await query_glob_bool('tru'), True)
        self.assertEqual(await query_glob_bool('tr'), True)
        self.assertEqual(await query_glob_bool('t'), True)
        self.assertEqual(await query_glob_bool('false'), False)
        self.assertEqual(await query_glob_bool('fals'), False)
        self.assertEqual(await query_glob_bool('fal'), False)
        self.assertEqual(await query_glob_bool('fa'), False)
        self.assertEqual(await query_glob_bool('f'), False)
        self.assertEqual(await query_glob_bool('yes'), True)
        self.assertEqual(await query_glob_bool('ye'), True)
        self.assertEqual(await query_glob_bool('y'), True)
        self.assertEqual(await query_glob_bool('no'), False)
        self.assertEqual(await query_glob_bool('n'), False)
        self.assertEqual(await query_glob_bool('"1"'), True)
        self.assertEqual(await query_glob_bool('"0"'), False)
        self.assertEqual(await query_glob_bool('1'), True)
        self.assertEqual(await query_glob_bool('0'), False)
        self.assertEqual(await query_glob_bool('1231231'), True)
        self.assertEqual(await query_glob_bool('hello'), None)
        self.assertEqual(await query_glob_bool("'ON'"), True)
        self.assertEqual(await query_glob_bool("'OFF'"), False)
        self.assertEqual(await query_glob_bool("'HELLO'"), None)

    async def test_sql_query_access_policy_01(self):
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

        await self.scon.execute('SET LOCAL apply_access_policies_pg TO true')

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

    async def test_sql_query_access_policy_02(self):
        # access policies from computeds

        # no access policies
        res = await self.squery_values('SELECT x FROM "ContentSummary"')
        self.assertEqual(res, [[5]])

        await self.scon.execute('SET LOCAL apply_access_policies_pg TO true')

        # access policies applied
        res = await self.squery_values('SELECT x FROM "ContentSummary"')
        self.assertEqual(res, [[0]])

        # access policies use globals
        await self.scon.execute(
            """SET LOCAL "global default::filter_title" TO 'Forrest Gump'"""
        )
        res = await self.squery_values('SELECT x FROM "ContentSummary"')
        self.assertEqual(res, [[1]])

    async def test_sql_query_access_policy_03(self):
        # access policies for dml

        # allowed without applying access policies

        await self.scon.execute('SET LOCAL apply_access_policies_pg TO true')

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

    async def test_sql_query_access_policy_04(self):
        # access policies without inheritance

        # there is only one object that is of exactly type Content
        res = await self.squery_values('SELECT * FROM ONLY "Content"')
        self.assertEqual(len(res), 1)

        await self.scon.execute('SET LOCAL apply_access_policies_pg TO true')

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

    async def test_sql_query_subquery_splat_01(self):
        res = await self.squery_values(
            '''
            with "average_pages" as (select avg("pages") as "value" from "Book")
            select pages from "Book"
            where "Book".pages < (select * from "average_pages")
            '''
        )
        self.assertEqual(
            res,
            [[206]],
        )

    async def test_sql_query_having_01(self):
        res = await self.squery_values(
            '''
            select 1 having false
            '''
        )
        self.assertEqual(
            res,
            [],
        )

    async def test_sql_query_unsupported_01(self):
        # test error messages of unsupported queries

        # we build AST for this not, but throw in resolver
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            "not supported: CREATE",
            position="14",  # TODO: this is confusing
        ):
            await self.squery_values('CREATE TABLE a();')

        # we don't even have AST node for this
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            "not supported: ALTER TABLE",
        ):
            await self.squery_values('ALTER TABLE a ADD COLUMN b INT;')

        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            "not supported: REINDEX",
        ):
            await self.squery_values('REINDEX TABLE a;')

    async def test_sql_query_locking_00(self):
        # Movie is allowed because it has no sub-types and access policies are
        # not enabled.
        await self.squery_values(
            '''
            SELECT id FROM "Movie" LIMIT 1 FOR UPDATE;
            '''
        )

        await self.squery_values(
            '''
            SELECT id FROM "Movie" LIMIT 1 FOR NO KEY UPDATE NOWAIT;
            '''
        )
        await self.squery_values(
            '''
            SELECT id FROM "Movie" LIMIT 1 FOR KEY SHARE SKIP LOCKED;
            '''
        )

    async def test_sql_query_locking_01(self):
        # fail because sub-types
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            "locking clause not supported",
        ):
            await self.squery_values(
                '''
                SELECT id FROM "Content" LIMIT 1 FOR UPDATE;
                '''
            )

        # fail because access policies
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            "locking clause not supported",
        ):
            await self.scon.execute(
                'SET LOCAL apply_access_policies_pg TO TRUE'
            )
            await self.squery_values(
                '''
                SELECT id FROM "Movie" LIMIT 1 FOR UPDATE;
                '''
            )

    async def test_sql_query_locking_02(self):
        # we are locking just Movie
        await self.squery_values(
            '''
            SELECT * FROM "Movie", "Content" LIMIT 1 FOR UPDATE OF "Movie";
            '''
        )

        # we are locking just Movie
        await self.squery_values(
            '''
            SELECT * FROM "Movie" m, "Content" LIMIT 1 FOR UPDATE OF m;
            '''
        )

        # we are locking just Content
        with self.assertRaisesRegex(
            asyncpg.FeatureNotSupportedError,
            "locking clause not supported",
        ):
            await self.squery_values(
                '''
                SELECT * FROM "Movie", "Content" c LIMIT 1 FOR UPDATE OF c;
                '''
            )

    async def test_sql_query_locking_03(self):
        # allowed, but this won't lock anything
        await self.squery_values(
            '''
            SELECT * FROM (VALUES (1)) t FOR UPDATE;
            '''
        )

        # allowed, will not lock Content
        await self.squery_values(
            '''
            WITH c AS (SELECT * FROM "Content")
            SELECT * FROM "Movie" FOR UPDATE;
            '''
        )

    async def test_sql_native_query_00(self):
        await self.assert_sql_query_result(
            """
                SELECT
                    1 AS a,
                    'two' AS b,
                    to_json('three'::text) AS c,
                    timestamp '2000-12-16 12:21:13' AS d,
                    timestamp with time zone '2000-12-16 12:21:13' AS e,
                    date '0001-01-01 AD' AS f,
                    interval '2000 years' AS g,
                    ARRAY[1, 2, 3] AS h,
                    FALSE AS i,
                    3.4 AS j
            """,
            [
                {
                    "a": 1,
                    "b": "two",
                    "c": '"three"',
                    "d": "2000-12-16T12:21:13",
                    "e": "2000-12-16T12:21:13+00:00",
                    "f": "0001-01-01",
                    "g": edgedb.RelativeDuration(months=2000 * 12),
                    "h": [1, 2, 3],
                    "i": False,
                    "j": 3.4,
                }
            ],
        )

    async def test_sql_native_query_01(self):
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
            [
                {
                    "title": "Forrest Gump",
                    "genre": "Drama",
                },
                {
                    "title": "Saving Private Ryan",
                    "genre": "Drama",
                },
            ],
            apply_access_policies=False,
        )

    async def test_sql_native_query_02(self):
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
            [
                {
                    "title": "Saving Private Ryan",
                    "genre": "Drama",
                }
            ],
            variables={
                "1": "Drama",
                "2": 14,
            },
            apply_access_policies=False,
        )

    async def test_sql_native_query_03(self):
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
            apply_access_policies=False,
        )

    async def test_sql_native_query_04(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            'duplicate column name: `a`',
            _position=15,
        ):
            await self.assert_sql_query_result('SELECT 1 AS a, 2 AS a', [])

    async def test_sql_native_query_05(self):
        # `a` would be duplicated,
        # so second and third instance are prefixed with rel var name
        await self.assert_sql_query_result(
            '''
            WITH
              x(a) AS (VALUES (1::int)),
              y(a) AS (VALUES (1::int + 1::int)),
              z(a) AS (VALUES (1::int + 1::int + 1::int))
            SELECT * FROM x, y JOIN z u ON TRUE::bool
            ''',
            [{'a': 1, 'y_a': 2, 'u_a': 3}],
        )

    async def test_sql_native_query_06(self):
        await self.assert_sql_query_result(
            '''
            WITH
              x(a) AS (VALUES (1)),
              y(a) AS (VALUES (2), (3))
            SELECT x.*, u.* FROM x, y as u
            ''',
            [{'a': 1, 'u_a': 2}, {'a': 1, 'u_a': 3}],
        )

    async def test_sql_native_query_07(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            'duplicate column name: `y_a`',
            _position=137,
        ):
            await self.assert_sql_query_result(
                '''
                WITH
                x(a) AS (VALUES (1)),
                y(a) AS (VALUES (1 + 1), (1 + 1 + 1))
                SELECT * FROM x, y, y
                ''',
                [],
            )

    async def test_sql_native_query_08(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            'duplicate column name: `x_a`',
            _position=92,
        ):
            await self.assert_sql_query_result(
                '''
                WITH
                x(a) AS (VALUES (2))
                SELECT 1 as x_a, * FROM x, x
                ''',
                [],
            )

    async def test_sql_native_query_09(self):
        await self.assert_sql_query_result(
            '''
            WITH
              x(a) AS (VALUES (1 + 1))
            SELECT 1 as a, * FROM x
            ''',
            [{'a': 1, 'x_a': 2}],
        )

    async def test_sql_native_query_10(self):
        await self.assert_sql_query_result(
            '''
            WITH
              x(b, c) AS (VALUES (2, 3))
            SELECT 1 as a, * FROM x
            ''',
            [{'a': 1, 'b': 2, 'c': 3}],  # values are swapped around
        )

    async def test_sql_native_query_11(self):
        # JOIN ... ON TRUE fails, saying it expects bool, but it got an int
        await self.assert_sql_query_result(
            '''
            WITH
              x(a) AS (VALUES (1)),
              y(b) AS (VALUES (2)),
              z(c) AS (VALUES (3))
            SELECT * FROM x, y JOIN z ON TRUE
            ''',
            [{'a': 1, 'b': 2, 'c': 3}],
        )

    async def test_sql_native_query_12(self):
        await self.assert_sql_query_result(
            '''
            WITH
                x(a) AS (VALUES (1), (5)),
                y(b) AS (VALUES (2), (3))
            SELECT * FROM x, y
            ''',
            [
                {'a': 1, 'b': 2},
                {'a': 1, 'b': 3},
                {'a': 5, 'b': 2},
                {'a': 5, 'b': 3},
            ],
        )

    async def test_sql_native_query_13(self):
        # globals

        await self.assert_sql_query_result(
            """
            SELECT username FROM "Person"
            ORDER BY first_name LIMIT 1
            """,
            [{'username': "u_robin"}],
        )

        await self.con.execute(
            '''
            SET GLOBAL default::username_prefix := 'user_';
            '''
        )

        await self.assert_sql_query_result(
            """
            SELECT username FROM "Person"
            ORDER BY first_name LIMIT 1
            """,
            [{'username': "user_robin"}],
        )

    async def test_sql_native_query_14(self):
        # globals

        await self.con.execute(
            f"""
            SET GLOBAL glob_mod::glob_str := 'hello';
            SET GLOBAL glob_mod::glob_uuid :=
                <uuid>'f527c032-ad4c-461e-95e2-67c4e2b42ca7';
            SET GLOBAL glob_mod::glob_int64 := 42;
            SET GLOBAL glob_mod::glob_int32 := 42;
            SET GLOBAL glob_mod::glob_int16 := 42;
            SET GLOBAL glob_mod::glob_bool := true;
            SET GLOBAL glob_mod::glob_float64 := 42.1;
            SET GLOBAL glob_mod::glob_float32 := 42.1;
            """
        )
        await self.con.execute_sql('INSERT INTO glob_mod."G" DEFAULT VALUES')

        await self.assert_sql_query_result(
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
            ''',
            [
                {
                    'p_str': 'hello',
                    'p_uuid': uuid.UUID('f527c032-ad4c-461e-95e2-67c4e2b42ca7'),
                    'p_int64': 42,
                    'p_int32': 42,
                    'p_int16': 42,
                    'p_bool': True,
                    'p_float64': 42.1,
                    'p_float32': 42.099998474121094,
                }
            ],
        )

    async def test_sql_native_query_15(self):
        # no access policies
        await self.assert_sql_query_result(
            'SELECT title FROM "Content" ORDER BY title',
            [
                {'title': 'Chronicles of Narnia'},
                {'title': 'Forrest Gump'},
                {'title': 'Halo 3'},
                {'title': 'Hunger Games'},
                {'title': 'Saving Private Ryan'},
            ],
            apply_access_policies=False,
        )

        # access policies applied
        await self.assert_sql_query_result(
            'SELECT title FROM "Content" ORDER BY title', []
        )

        # access policies use globals
        await self.con.execute(
            "SET global default::filter_title := 'Forrest Gump'"
        )
        await self.assert_sql_query_result(
            'SELECT title FROM "Content" ORDER BY title',
            [{'title': 'Forrest Gump'}],
        )

    async def test_sql_native_query_16(self):
        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "not supported: VARIABLE SET",
            _position=14,  # this point to `1`, but hey, better than nothing
        ):
            await self.assert_sql_query_result('SET my_var TO 1', [])

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "not supported: VARIABLE RESET",
        ):
            await self.assert_sql_query_result('RESET my_var', [])

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "not supported: VARIABLE SHOW",
        ):
            await self.assert_sql_query_result('SHOW my_var', [])

    async def test_sql_native_query_17(self):
        await self.assert_sql_query_result(
            """SELECT $1::text as t, $2::int as i""",
            [{"t": "Hello", "i": 42}],
            variables={
                "1": "Hello",
                "2": 42,
            },
            apply_access_policies=False,
        )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            'multi-statement SQL scripts are not supported yet',
        ):
            await self.assert_sql_query_result(
                """
                SELECT 'Hello' as t;
                SELECT 42 as i;
                """,
                [],
                apply_access_policies=False,
            )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            'multi-statement SQL scripts are not supported yet',
        ):
            await self.assert_sql_query_result(
                """
                SELECT 'hello'::text as t;
                SELECT $1::int as i;
                """,
                [],
                variables={
                    "1": 42,
                },
                apply_access_policies=False,
            )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            'multi-statement SQL scripts are not supported yet',
        ):
            await self.assert_sql_query_result(
                """
                SELECT $1::text as t;
                SELECT 42::int as i;
                """,
                [],
                variables={
                    "1": "Hello",
                },
                apply_access_policies=False,
            )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            'multi-statement SQL scripts are not supported yet',
        ):
            await self.assert_sql_query_result(
                """
                SELECT $1::text as t;
                SELECT $2::int as i;
                """,
                [],
                variables={
                    "1": "Hello",
                    "2": 42,
                },
                apply_access_policies=False,
            )

    async def test_sql_native_query_18(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            'column \"asdf\" does not exist',
            _position=35,
        ):
            await self.con.query_sql(
                '''select title, 'aaaaaaaaaaaaaaaaa', asdf from "Content";'''
            )

    async def test_sql_native_query_19(self):
        with self.assertRaisesRegex(
            edgedb.errors.ExecutionError,
            'does not exist',
            _position=35,
            _hint=(
                'No function matches the given name and argument types. '
                'You might need to add explicit type casts.'
            ),
        ):
            await self.con.query_sql(
                '''select title, 'aaaaaaaaaaaaaaaaa', asdf() from "Content";'''
            )

    @test.xfail('We translate the type name to std::int32')
    async def test_sql_native_query_20(self):
        # This test was originally added to test the position is
        # right, but it turns out that postgres doesn't report a
        # position here.
        with self.assertRaisesRegex(
            edgedb.errors.InvalidValueError,
            'invalid input syntax for type integer',
        ):
            await self.con.query_sql('''
                select 'aaaaaaaaaaaaaaaaa', ('goo'::text::integer);
            ''')

    async def test_sql_native_query_21(self):
        await self.assert_sql_query_result(
            "SELECT 1 as a", [{"a": 1}]
        )

        await self.assert_sql_query_result(
            "SELECT 'test' as a", [{"a": "test"}]
        )

    async def test_sql_native_query_22(self):
        await self.assert_sql_query_result(
            "SELECT 1 as a UNION SELECT 2 as a", [{"a": 1}, {"a": 2}]
        )

    async def test_sql_native_query_23(self):
        await self.assert_sql_query_result(
            "VALUES (1, 2)", [{"column1": 1, "column2": 2}]
        )

    async def test_sql_native_query_24(self):
        with self.assertRaisesRegex(
            edgedb.errors.UnsupportedFeatureError, 'not supported: COPY'
        ):
            await self.assert_sql_query_result(
                'COPY "Genre" TO STDOUT', []
            )

    async def test_sql_native_query_25(self):
        # implict limit
        await self.assert_sql_query_result(
            'VALUES (1), (2), (3), (4), (5), (6), (7)',
            [{'column1': 1}, {'column1': 2}, {'column1': 3}],
            implicit_limit=3,
        )

    async def test_sql_native_query_26(self):
        await self.assert_sql_query_result(
            """
                select distinct title, pages from "Book"
                order by title, pages;
            """,
            [
                {'title': 'Chronicles of Narnia', 'pages': 206},
                {'title': 'Hunger Games', 'pages': 374},
            ],
            apply_access_policies=False,
        )

    async def test_sql_native_query_27(self):
        with self.assertRaisesRegex(
            edgedb.errors.EdgeQLSyntaxError,
            'syntax error at or near',
        ):
            await self.con.query_sql('''
                select (), asdf
            ''')


class TestSQLQueryNonTransactional(tb.SQLQueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'movies.esdl')
    SCHEMA_INVENTORY = os.path.join(
        os.path.dirname(__file__), 'schemas', 'inventory.esdl'
    )

    SETUP = [
        os.path.join(
            os.path.dirname(__file__), 'schemas', 'movies_setup.edgeql'
        ),
    ]

    TRANSACTION_ISOLATION = False

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
        # database settings allow_user_specified_ids & apply_access_policies_pg
        # should be unified over EdgeQL and SQL adapter

        async def set_current_database(val: Optional[bool]):
            # for this to have effect, it must not be ran within a transaction
            if val is None:
                await self.con.execute(
                    f'''
                    configure current database
                        reset apply_access_policies_pg;
                    '''
                )
            else:
                await self.con.execute(
                    f'''
                    configure current database
                        set apply_access_policies_pg := {str(val).lower()};
                    '''
                )

        async def set_sql(val: Optional[bool]):
            if val is None:
                await self.scon.execute(
                    f'''
                    RESET apply_access_policies_pg;
                    '''
                )
            else:
                await self.scon.execute(
                    f'''
                    SET apply_access_policies_pg TO '{str(val).lower()}';
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

    async def test_sql_query_set_05(self):
        # IntervalStyle

        await self.scon.execute('SET IntervalStyle TO ISO_8601;')
        [[res]] = await self.squery_values(
            "SELECT '2 years 15 months 100 weeks 99 hours'::interval::text;"
        )
        self.assertEqual(res, 'P3Y3M700DT99H')

        await self.scon.execute('SET IntervalStyle TO postgres_verbose;')
        [[res]] = await self.squery_values(
            "SELECT '2 years 15 months 100 weeks 99 hours'::interval::text;"
        )
        self.assertEqual(res, '@ 3 years 3 mons 700 days 99 hours')

        await self.scon.execute('SET IntervalStyle TO sql_standard;')
        [[res]] = await self.squery_values(
            "SELECT '2 years 15 months 100 weeks 99 hours'::interval::text;"
        )
        self.assertEqual(res, '+3-3 +700 +99:00:00')

    async def test_sql_query_set_06(self):
        # bytea_output

        await self.scon.execute('SET bytea_output TO hex')
        [[res]] = await self.squery_values(
            "SELECT '\\x01abcdef01'::bytea::text"
        )
        self.assertEqual(res, r'\x01abcdef01')

        await self.scon.execute('SET bytea_output TO escape')
        [[res]] = await self.squery_values(
            "SELECT '\\x01abcdef01'::bytea::text"
        )
        self.assertEqual(res, r'\001\253\315\357\001')

    async def test_sql_query_set_07(self):
        # enable_memoize

        await self.scon.execute('SET enable_memoize TO ye')
        [[res]] = await self.squery_values(
            "SELECT 'hello'"
        )
        self.assertEqual(res, 'hello')

        await self.scon.execute('SET enable_memoize TO off')
        [[res]] = await self.squery_values(
            "SELECT 'hello'"
        )
        self.assertEqual(res, 'hello')

    @test.skip(
        'blocking the connection causes other tests which trigger a '
        'PostgreSQL error to encounter a InternalServerError and close '
        'the connection'
    )
    async def test_sql_query_locking_04(self):
        # test that we really obtain a lock

        # we will obtain a lock on the main connection
        # and then check that another connection is blocked

        con_other = await self.create_sql_connection()

        tran = self.scon.transaction()
        await tran.start()

        # obtain a lock
        await self.scon.execute(
            '''
            SELECT * FROM "Movie" WHERE title = 'Forrest Gump'
            FOR UPDATE;
            '''
        )

        async def assert_not_blocked(coroutine: Coroutine) -> None:
            await asyncio.wait_for(coroutine, 0.25)

        async def assert_blocked(coroutine: Coroutine) -> Tuple[asyncio.Task]:
            task: asyncio.Task = asyncio.create_task(coroutine)
            done, pending = await asyncio.wait((task,), timeout=0.25)
            if len(done) != 0:
                self.fail("expected this action to block, but it completed")
            task_t = (next(iter(pending)),)
            return task_t

        # querying is allowed
        await assert_not_blocked(
            con_other.execute(
                '''
                SELECT title FROM "Movie" WHERE title = 'Forrest Gump';
                '''
            )
        )

        # another FOR UPDATE is now blocked
        (task,) = await assert_blocked(
            con_other.execute(
                '''
                SELECT * FROM "Movie" WHERE title = 'Forrest Gump'
                FOR UPDATE;
                '''
            )
        )

        # release the lock
        await tran.rollback()

        # now we can finish the second SELECT FOR UPDATE
        await task

        # and subsequent FOR UPDATE are not blocked
        await assert_not_blocked(
            con_other.execute(
                '''
                SELECT * FROM "Movie" WHERE title = 'Forrest Gump'
                FOR UPDATE;
                '''
            )
        )

        await con_other.close()

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

    async def test_sql_query_error_11(self):
        # extended query protocol
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            'invalid input syntax for type uuid',
            # TODO
            # position="8",
        ):
            await self.scon.fetch("""SELECT 'bad uuid'::uuid""")

        # simple query protocol
        with self.assertRaisesRegex(
            asyncpg.InvalidTextRepresentationError,
            'invalid input syntax for type uuid',
            # TODO
            # position="8",
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
            # TODO
            # position="8",
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
