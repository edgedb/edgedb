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

from edb.testbase import server as tb


class TestSQL(tb.SQLQueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'movies.esdl')

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas', 'movies_setup.edgeql'
    )

    async def test_sql_query_00(self):
        # basic
        res = await self.squery_values(
            '''
            select title from Movie order by title
            '''
        )
        self.assertEqual(res, [['Forrest Gump'], ['Saving Private Ryan']])

    async def test_sql_query_01(self):
        # table alias
        res = await self.squery(
            '''
            select mve.title, mve.release_year, director_id FROM Movie as mve
            '''
        )
        self.assert_shape(res, 2, 3)

    async def test_sql_query_02(self):
        # select from parent type
        res = await self.squery(
            '''
            select * FROM Content
            '''
        )
        self.assert_shape(res, 5, 3, ['id', 'genre_id', 'title'])

    async def test_sql_query_03(self):
        # select from parent type only
        res = await self.squery(
            '''
            select * FROM ONLY Content -- should have only one result
            '''
        )
        self.assert_shape(res, 1, 3, ['id', 'genre_id', 'title'])

    async def test_sql_query_04(self):
        # multiple FROMs
        res = await self.squery(
            '''
            select mve.title, Person.first_name
            FROM Movie mve, Person WHERE mve.director_id = person.id
            '''
        )
        self.assert_shape(res, 1, 2, ['title', 'first_name'])

    async def test_sql_query_05(self):
        # case insensitive
        res = await self.squery(
            '''
            SeLeCt mve.title as tiT, perSon.first_name
            FROM Movie mve, Person
            '''
        )
        self.assert_shape(res, 6, 2, ['tit', 'first_name'])

    async def test_sql_query_06(self):
        # sub relations
        res = await self.squery(
            '''
            select id, title, prS.first_name
            FROM Movie mve, (select first_name FROM Person) prs
            '''
        )
        self.assert_shape(res, 6, 3, ['id', 'title', 'first_name'])

    async def test_sql_query_07(self):
        # quoted case sensitive
        res = await self.squery(
            '''
            SELECT tItLe, release_year "RL year" FROM "movie" ORDER BY titLe;
            '''
        )
        self.assert_shape(res, 2, 2, ['title', 'RL year'])

    async def test_sql_query_08(self):
        # JOIN
        res = await self.squery(
            '''
            SELECT movie.id, genre.id
            FROM Movie JOIN Genre ON Movie.genre_id = Genre.id
            '''
        )
        self.assert_shape(res, 2, 2, ['id', 'id'])

    async def test_sql_query_09(self):
        # resolve columns without table names
        res = await self.squery(
            '''
            SELECT movie.id, title, name
            FROM Movie JOIN Genre ON Movie.genre_id = Genre.id
            '''
        )
        self.assert_shape(res, 2, 3, ['id', 'title', 'name'])

    async def test_sql_query_10(self):
        # wildcard select
        res = await self.squery(
            '''
            SELECT m.* FROM Movie m
            '''
        )
        self.assert_shape(
            res,
            2,
            5,
            ['id', 'director_id', 'genre_id', 'release_year', 'title'],
        )

    async def test_sql_query_11(self):
        # multiple wildcard select
        res = await self.squery(
            '''
            SELECT * FROM Movie JOIN Genre g ON Movie.genre_id = Genre.id
            '''
        )
        self.assert_shape(res, 2, 7)

    async def test_sql_query_12(self):
        # JOIN USING
        res = await self.squery(
            '''
            SELECT * FROM Movie
            JOIN (SELECT id as genre_id, name FROM Genre) g USING (genre_id)
            '''
        )
        self.assert_shape(res, 2, 7)

    async def test_sql_query_13(self):
        # CTE
        res = await self.squery(
            '''
            WITH g AS (SELECT id as genre_id, name FROM Genre)
            SELECT * FROM Movie JOIN g USING (genre_id)
            '''
        )
        self.assert_shape(res, 2, 7)

    async def test_sql_query_14(self):
        # CASE
        res = await self.squery_values(
            '''
            SELECT title, CASE WHEN title='Forrest Gump' THEN 'forest'
            WHEN title='Saving Private Ryan' THEN 'the war film'
            ELSE 'unknown' END AS nick_name FROM Movie
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
        res = await self.squery(
            '''
            SELECT id, title FROM Movie UNION select id, title FROM Book
            '''
        )
        self.assert_shape(res, 4, 2)

    async def test_sql_query_16(self):
        # casting
        res = await self.squery(
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
            FROM Person ORDER BY last_name DESC NULLS FIRST
            '''
        )
        self.assertEqual(
            res, [['Robin', None], ['Steven', 'Spielberg'], ['Tom', 'Hanks']]
        )

        res = await self.squery_values(
            '''
            SELECT first_name, last_name
            FROM Person ORDER BY last_name DESC NULLS LAST
            '''
        )
        self.assertEqual(
            res, [['Steven', 'Spielberg'], ['Tom', 'Hanks'], ['Robin', None]]
        )

    async def test_sql_query_18(self):
        # LIMIT & OFFSET
        res = await self.squery_values(
            '''
            SELECT title FROM Content ORDER BY title OFFSET 1 LIMIT 2
            '''
        )
        self.assertEqual(res, [['Forrest Gump'], ['Halo 3']])

    async def test_sql_query_19(self):
        # DISTINCT
        res = await self.squery_values(
            '''
            SELECT DISTINCT name
            FROM Content c JOIN Genre g ON (c.genre_id = g.id)
            ORDER BY name
            '''
        )
        self.assertEqual(res, [['Drama'], ['Fiction']])

        res = await self.squery_values(
            '''
            SELECT DISTINCT ON (name) name, title
            FROM Content c JOIN Genre g ON (c.genre_id = g.id)
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
            SELECT first_name FROM Person
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
                FROM Content c LEFT JOIN Book USING(id)
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
        res = await self.squery(
            '''
            SELECT id FROM Person WHERE last_name IS NULL
            '''
        )
        self.assert_shape(res, 1, 1)

        res = await self.squery(
            '''
            SELECT id FROM Person WHERE (last_name = 'Hanks') IS NOT TRUE
            '''
        )
        self.assert_shape(res, 2, 1)

    async def test_sql_query_23(self):
        # ImplicitRow
        res = await self.squery(
            '''
            SELECT id FROM person
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
            SELECT title FROM Movie WHERE id IN (
                SELECT id FROM Movie ORDER BY title LIMIT 1
            )
            '''
        )
        self.assertEqual(res, [['Forrest Gump']])

        res = await self.squery_values(
            '''
            SELECT (SELECT title FROM Movie ORDER BY title LIMIT 1)
            '''
        )
        self.assertEqual(res, [['Forrest Gump']])
