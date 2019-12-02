#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


import unittest  # NOQA

from edb.testbase import server as tb


class TestEdgeQLTutorial(tb.QueryTestCase):

    ISOLATED_METHODS = False

    async def test_edgeql_tutorial(self):
        await self.con.execute(r'''
            START TRANSACTION;
            CREATE MIGRATION movies TO {
                module default {
                    type Movie {
                        required property title -> str;
                        # the year of release
                        property year -> int64;
                        required link director -> Person;
                        multi link cast -> Person;
                    }
                    type Person {
                        required property first_name -> str;
                        required property last_name -> str;
                    }
                }
            };
            COMMIT MIGRATION movies;
            COMMIT;

            INSERT Movie {
                title := 'Blade Runner 2049',
                year := 2017,
                director := (
                    INSERT Person {
                        first_name := 'Denis',
                        last_name := 'Villeneuve',
                    }
                ),
                cast := {
                    (INSERT Person {
                        first_name := 'Harrison',
                        last_name := 'Ford',
                    }),
                    (INSERT Person {
                        first_name := 'Ryan',
                        last_name := 'Gosling',
                    }),
                    (INSERT Person {
                        first_name := 'Ana',
                        last_name := 'de Armas',
                    }),
                }
            };

            INSERT Movie {
                title := 'Dune',
                director := (
                    SELECT Person
                    FILTER
                        # the last name is sufficient
                        # to identify the right person
                        .last_name = 'Villeneuve'
                    # the LIMIT is needed to satisfy the single
                    # link requirement validation
                    LIMIT 1
                )
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Movie {
                    title,
                    year
                } ORDER BY .title;
            ''',
            [
                {'title': 'Blade Runner 2049', 'year': 2017},
                {'title': 'Dune', 'year': None},
            ],
        )

        await self.assert_query_result(
            r'''
                SELECT Movie {
                    title,
                    year
                }
                FILTER .title ILIKE 'blade runner%';
            ''',
            [
                {'title': 'Blade Runner 2049', 'year': 2017},
            ],
        )

        await self.assert_query_result(
            r'''
                SELECT Movie {
                    title,
                    year,
                    director: {
                        first_name,
                        last_name
                    },
                    cast: {
                        first_name,
                        last_name
                    } ORDER BY .last_name
                }
                FILTER .title ILIKE 'blade runner%';
            ''',
            [{
                'title': 'Blade Runner 2049',
                'year': 2017,
                'director': {
                    'first_name': 'Denis',
                    'last_name': 'Villeneuve',
                },
                'cast': [{
                    'first_name': 'Harrison',
                    'last_name': 'Ford',
                }, {
                    'first_name': 'Ryan',
                    'last_name': 'Gosling',
                }, {
                    'first_name': 'Ana',
                    'last_name': 'de Armas',
                }],
            }],
        )

        await self.assert_query_result(
            r'''
                SELECT Movie {
                    title,
                    num_actors := count(Movie.cast)
                } ORDER BY .title;
            ''',
            [
                {'title': 'Blade Runner 2049', 'num_actors': 3},
                {'title': 'Dune', 'num_actors': 0},
            ],
        )

        await self.con.execute(r'''
            INSERT Person {
                first_name := 'Jason',
                last_name := 'Momoa'
            };
            INSERT Person {
                first_name := 'Oscar',
                last_name := 'Isaac'
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE Person {
                ALTER PROPERTY first_name {
                    DROP REQUIRED;
                }
            };
        ''')

        await self.con.execute(r'''
            INSERT Person {
                last_name := 'Zendaya'
            };
        ''')

        await self.con.execute(r'''
            UPDATE Movie
            FILTER Movie.title = 'Dune'
            SET {
                cast := (
                    SELECT Person
                    FILTER .last_name IN {
                        'Momoa',
                        'Zendaya',
                        'Isaac'
                    }
                )
            };
        ''')

        await self.con.execute(r'''
            START TRANSACTION;
            CREATE MIGRATION movies TO {
                module default {
                    type Movie {
                        required property title -> str;
                        # the year of release
                        property year -> int64;
                        required link director -> Person;
                        multi link cast -> Person;
                    }
                    type Person {
                        property first_name -> str;
                        required property last_name -> str;
                        property name :=
                            .first_name ++ ' ' ++ .last_name
                            IF EXISTS .first_name
                            ELSE .last_name;
                    }
                }
            };
            COMMIT MIGRATION movies;
            COMMIT;
        ''')

        await self.assert_query_result(
            r'''
                SELECT Movie {
                    title,
                    year,
                    director: { name },
                    cast: { name } ORDER BY .name
                }
                FILTER .title = 'Dune';
            ''',
            [{
                'title': 'Dune',
                'year': None,
                'director': {
                    'name': 'Denis Villeneuve',
                },
                'cast': [{
                    'name': 'Jason Momoa',
                }, {
                    'name': 'Oscar Isaac',
                }, {
                    'name': 'Zendaya',
                }],
            }],
        )
