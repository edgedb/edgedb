#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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

import edgedb

from edb.testbase import server as tb


class TestEdgeQLGroup(tb.QueryTestCase):
    '''These tests are focused on using the internal GROUP statement.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SCHEMA_CARDS = os.path.join(os.path.dirname(__file__), 'schemas',
                                'cards.esdl')

    SETUP = [
        os.path.join(os.path.dirname(__file__), 'schemas',
                     'issues_setup.edgeql'),
        'SET MODULE cards;',
        os.path.join(os.path.dirname(__file__), 'schemas',
                     'cards_setup.edgeql'),
    ]

    async def test_edgeql_group_simple_01(self):
        await self.assert_query_result(
            r'''
            GROUP cards::Card {name} BY .element
            ''',
            tb.bag([
                {
                    "elements": tb.bag(
                        [{"name": "Bog monster"}, {"name": "Giant turtle"}]),
                    "key": {"element": "Water"}
                },
                {
                    "elements": tb.bag([{"name": "Imp"}, {"name": "Dragon"}]),
                    "key": {"element": "Fire"}
                },
                {
                    "elements": tb.bag([{"name": "Dwarf"}, {"name": "Golem"}]),
                    "key": {"element": "Earth"}
                },
                {
                    "elements": tb.bag([
                        {"name": "Sprite"},
                        {"name": "Giant eagle"},
                        {"name": "Djinn"}
                    ]),
                    "key": {"element": "Air"}
                }
            ])
        )

    async def test_edgeql_group_simple_02(self):
        await self.assert_query_result(
            r'''
            SELECT (GROUP cards::Card {name} BY .element)
            ''',
            tb.bag([
                {
                    "elements": tb.bag(
                        [{"name": "Bog monster"}, {"name": "Giant turtle"}]),
                    "key": {"element": "Water"}
                },
                {
                    "elements": tb.bag([{"name": "Imp"}, {"name": "Dragon"}]),
                    "key": {"element": "Fire"}
                },
                {
                    "elements": tb.bag([{"name": "Dwarf"}, {"name": "Golem"}]),
                    "key": {"element": "Earth"}
                },
                {
                    "elements": tb.bag([
                        {"name": "Sprite"},
                        {"name": "Giant eagle"},
                        {"name": "Djinn"}
                    ]),
                    "key": {"element": "Air"}
                }
            ])
        )

    async def test_edgeql_group_simple_03(self):
        # the compilation here is kind of a bummer; could we avoid an
        # unnest?
        await self.assert_query_result(
            r'''
            SELECT (GROUP cards::Card {name} BY .element)
            FILTER .key.element != 'Air';
            ''',
            tb.bag([
                {
                    "elements": tb.bag(
                        [{"name": "Bog monster"}, {"name": "Giant turtle"}]),
                    "key": {"element": "Water"}
                },
                {
                    "elements": tb.bag([{"name": "Imp"}, {"name": "Dragon"}]),
                    "key": {"element": "Fire"}
                },
                {
                    "elements": tb.bag([{"name": "Dwarf"}, {"name": "Golem"}]),
                    "key": {"element": "Earth"}
                },
            ])
        )

    async def test_edgeql_group_simple_no_id_output_01(self):
        # the implicitly injected id was making it into the output
        # in native mode at one point
        res = await self.con.query('GROUP cards::Card {name} BY .element')
        el = tuple(tuple(res)[0].elements)[0]
        self.assertNotIn("id := ", str(el))

    async def test_edgeql_group_simple_unused_alias_01(self):
        await self.con.query('''
            WITH MODULE cards
            SELECT (
              GROUP Card
              USING x := count(.owners), nowners := x,
              BY CUBE (.element, nowners)
            )
        ''')

    async def test_edgeql_group_process_select_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (GROUP Card BY .element) {
                element := .key.element,
                cnt := count(.elements),
            };
            ''',
            tb.bag([
                {"cnt": 2, "element": "Water"},
                {"cnt": 2, "element": "Fire"},
                {"cnt": 2, "element": "Earth"},
                {"cnt": 3, "element": "Air"}
            ])
        )

    async def test_edgeql_group_process_select_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (GROUP Card BY .element) {
                element := .key.element,
                cnt := count(.elements),
            } FILTER .element != 'Water';
            ''',
            tb.bag([
                {"cnt": 2, "element": "Fire"},
                {"cnt": 2, "element": "Earth"},
                {"cnt": 3, "element": "Air"},
            ])
        )

    async def test_edgeql_group_process_select_03(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (GROUP Card BY .element) {
                element := .key.element,
                cnt := count(.elements),
            } ORDER BY .element;
            ''',
            [
                {"cnt": 3, "element": "Air"},
                {"cnt": 2, "element": "Earth"},
                {"cnt": 2, "element": "Fire"},
                {"cnt": 2, "element": "Water"},
            ]
        )

    async def test_edgeql_group_process_for_01a(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            FOR g IN (GROUP Card BY .element) UNION (
                element := g.key.element,
                cnt := count(g.elements),
            );
            ''',
            tb.bag([
                {"cnt": 2, "element": "Water"},
                {"cnt": 2, "element": "Fire"},
                {"cnt": 2, "element": "Earth"},
                {"cnt": 3, "element": "Air"},
            ])
        )

    async def test_edgeql_group_process_select_04(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (GROUP Card BY .element) {
                cnt := count(.elements),
            };
            ''',
            tb.bag([
                {"cnt": 2}, {"cnt": 2}, {"cnt": 2}, {"cnt": 3}
            ])
        )

    async def test_edgeql_group_process_for_01b(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            FOR g IN (SELECT (GROUP Card BY .element)) UNION (
                element := g.key.element,
                cnt := count(g.elements),
            );
            ''',
            tb.bag([
                {"cnt": 2, "element": "Water"},
                {"cnt": 2, "element": "Fire"},
                {"cnt": 2, "element": "Earth"},
                {"cnt": 3, "element": "Air"}
            ])
        )

    async def test_edgeql_group_process_for_01c(self):
        await self.assert_query_result(
            r'''
            with module cards
            for h in (group Card by .element) union (for g in h union (
                element := g.key.element,
                cnt := count(g.elements),
            ));
            ''',
            tb.bag([
                {"cnt": 2, "element": "Water"},
                {"cnt": 2, "element": "Fire"},
                {"cnt": 2, "element": "Earth"},
                {"cnt": 3, "element": "Air"},
            ])
        )

    async def test_edgeql_group_process_for_01d(self):
        await self.assert_query_result(
            r'''
            with module cards
            for g in (group Card by .element) union (for gi in 0 union (
                element := g.key.element,
                cst := sum(g.elements.cost + gi),
            ));
            ''',
            tb.bag([
                {"cst": 5, "element": "Water"},
                {"cst": 6, "element": "Fire"},
                {"cst": 4, "element": "Earth"},
                {"cst": 7, "element": "Air"},
            ])
        )

    async def test_edgeql_group_sets_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            GROUP Card {name}
            USING nowners := count(.owners)
            BY {.element, nowners};
            ''',
            tb.bag([
                {
                    "elements": [
                        {"name": "Bog monster"}, {"name": "Giant turtle"}],
                    "grouping": ["element"],
                    "key": {"element": "Water", "nowners": None}
                },
                {
                    "elements": [{"name": "Dragon"}, {"name": "Imp"}],
                    "grouping": ["element"],
                    "key": {"element": "Fire", "nowners": None}
                },
                {
                    "elements": [{"name": "Dwarf"}, {"name": "Golem"}],
                    "grouping": ["element"],
                    "key": {"element": "Earth", "nowners": None}
                },
                {
                    "elements": [
                        {"name": "Djinn"},
                        {"name": "Giant eagle"},
                        {"name": "Sprite"},
                    ],
                    "grouping": ["element"],
                    "key": {"element": "Air", "nowners": None}
                },
                {
                    "elements": [{"name": "Golem"}],
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 3}
                },
                {
                    "elements": [
                        {"name": "Bog monster"}, {"name": "Giant turtle"}],
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 4}
                },
                {
                    "elements": [
                        {"name": "Djinn"},
                        {"name": "Dragon"},
                        {"name": "Dwarf"},
                        {"name": "Giant eagle"},
                        {"name": "Sprite"},
                    ],
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 2}
                },
                {
                    "elements": [{"name": "Imp"}],
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 1}
                }
            ]),
            sort={'elements': lambda x: x['name']},
        )

    async def test_edgeql_group_sets_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            GROUP Card
            USING nowners := count(.owners)
            BY {.element, nowners};
            ''',
            tb.bag([
                {
                    "elements": [{"id": str}] * 2,
                    "grouping": ["element"],
                    "key": {"element": "Water", "nowners": None}
                },
                {
                    "elements": [{"id": str}] * 2,
                    "grouping": ["element"],
                    "key": {"element": "Fire", "nowners": None}
                },
                {
                    "elements": [{"id": str}] * 2,
                    "grouping": ["element"],
                    "key": {"element": "Earth", "nowners": None}
                },
                {
                    "elements": [{"id": str}] * 3,
                    "grouping": ["element"],
                    "key": {"element": "Air", "nowners": None}
                },
                {
                    "elements": [{"id": str}] * 1,
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 3}
                },
                {
                    "elements": [{"id": str}] * 2,
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 4}
                },
                {
                    "elements": [{"id": str}] * 5,
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 2}
                },
                {
                    "elements": [{"id": str}] * 1,
                    "grouping": ["nowners"],
                    "key": {"element": None, "nowners": 1}
                }
            ]),
        )

    async def test_edgeql_group_grouping_sets_01(self):
        res = [
            {"grouping": [], "num": 9},
            {"grouping": ["element"], "num": int},
            {"grouping": ["element"], "num": int},
            {"grouping": ["element"], "num": int},
            {"grouping": ["element"], "num": int},
            {"grouping": ["element", "nowners"], "num": int},
            {"grouping": ["element", "nowners"], "num": int},
            {"grouping": ["element", "nowners"], "num": int},
            {"grouping": ["element", "nowners"], "num": int},
            {"grouping": ["element", "nowners"], "num": int},
            {"grouping": ["element", "nowners"], "num": int},
            {"grouping": ["nowners"], "num": int},
            {"grouping": ["nowners"], "num": int},
            {"grouping": ["nowners"], "num": int},
            {"grouping": ["nowners"], "num": int},
        ]

        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (
              GROUP Card
              USING nowners := count(.owners)
              BY CUBE (.element, nowners)
            ) {
                num := count(.elements),
                grouping
            } ORDER BY array_agg((SELECT _ := .grouping ORDER BY _))
            ''',
            res
        )

        # With an extra SELECT
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (SELECT (
              GROUP Card
              USING nowners := count(.owners)
              BY CUBE (.element, nowners)
            ) {
                num := count(.elements),
                grouping
            }) ORDER BY array_agg((SELECT _ := .grouping ORDER BY _))
            ''',
            res
        )

        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (
              GROUP Card
              USING x := count(.owners), nowners := x,
              BY CUBE (.element, nowners)
            ) {
                num := count(.elements),
                grouping
            } ORDER BY array_agg((SELECT _ := .grouping ORDER BY _))
            ''',
            res
        )

    async def test_edgeql_group_grouping_sets_02(self):
        # we just care about the grouping names we generate
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            SELECT (
              WITH W := (SELECT Card { name } LIMIT 1)
              GROUP W
              USING nowners := count(.owners)
              BY CUBE (.element, .cost, nowners)
            ) { grouping }
            ORDER BY (
                count(.grouping),
                array_agg((SELECT _ := .grouping ORDER BY _))
            )
            ''',
            [
                {"grouping": set()},
                {"grouping": {"cost"}},
                {"grouping": {"element"}},
                {"grouping": {"nowners"}},
                {"grouping": {"cost", "element"}},
                {"grouping": {"cost", "nowners"}},
                {"grouping": {"element", "nowners"}},
                {"grouping": {"element", "cost", "nowners"}}
            ]
        )

    async def test_edgeql_group_for_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            FOR g in (GROUP Card BY .element) UNION (
                WITH U := g.elements,
                SELECT U {
                    name,
                    cost_ratio := .cost / math::mean(g.elements.cost)
            });
            ''',
            tb.bag([
                {"cost_ratio": 0.42857142857142855, "name": "Sprite"},
                {"cost_ratio": 0.8571428571428571, "name": "Giant eagle"},
                {"cost_ratio": 1.7142857142857142, "name": "Djinn"},
                {"cost_ratio": 0.5, "name": "Dwarf"},
                {"cost_ratio": 1.5, "name": "Golem"},
                {"cost_ratio": 0.3333333333333333, "name": "Imp"},
                {"cost_ratio": 1.6666666666666667, "name": "Dragon"},
                {"cost_ratio": 0.8, "name": "Bog monster"},
                {"cost_ratio": 1.2, "name": "Giant turtle"}
            ])
        )

    async def test_edgeql_group_simple_old_01(self):
        await self.assert_query_result(
            r'''
                for g in (group User by .name)
                union count(g.elements.<owner);
            ''',
            {4, 2},
        )

    async def test_edgeql_group_semi_join_01(self):
        # this is useless, but shouldn't crash
        await self.assert_query_result(
            r'''
                select (group User by .name).elements
            ''',
            [{}, {}],
        )

    async def test_edgeql_group_by_tuple_01(self):
        await self.assert_query_result(
            r"""
                GROUP Issue
                USING B := (Issue.status.name, Issue.time_estimate)
                # This tuple will be {} for Issues lacking
                # time_estimate. So effectively we're expecting only 2
                # subsets, grouped by:
                # - {}
                # - ('Open', 3000)
                BY B
            """,
            tb.bag([
                {
                    'key': {'B': ["Open", 3000]},
                    'elements': [{}] * 1,
                },
                {
                    'key': {'B': None},
                    'elements': [{}] * 3,
                },
            ]),
        )

    async def test_edgeql_group_by_group_by_01(self):
        res = tb.bag([
            {
                "elements": tb.bag([
                    {
                        "agrouping": ["element"],
                        "key": {"element": "Water", "nowners": None},
                        "num": 2
                    },
                    {
                        "agrouping": ["element"],
                        "key": {"element": "Fire", "nowners": None},
                        "num": 2
                    },
                    {
                        "agrouping": ["element"],
                        "key": {"element": "Earth", "nowners": None},
                        "num": 2
                    },
                    {
                        "agrouping": ["element"],
                        "key": {"element": "Air", "nowners": None},
                        "num": 3
                    }
                ]),
                "grouping": ["agrouping"],
                "key": {"agrouping": ["element"]}
            },
            {
                "elements": tb.bag([
                    {
                        "agrouping": ["nowners"],
                        "key": {"element": None, "nowners": 3},
                        "num": 1
                    },
                    {
                        "agrouping": ["nowners"],
                        "key": {"element": None, "nowners": 4},
                        "num": 2
                    },
                    {
                        "agrouping": ["nowners"],
                        "key": {"element": None, "nowners": 2},
                        "num": 5
                    },
                    {
                        "agrouping": ["nowners"],
                        "key": {"element": None, "nowners": 1},
                        "num": 1
                    }
                ]),
                "grouping": ["agrouping"],
                "key": {"agrouping": ["nowners"]}
            }
        ])

        qry = r'''
            WITH MODULE cards
            GROUP (
              SELECT (
                GROUP Card
                USING nowners := count(.owners)
                BY {.element, nowners}
              ) {
                  num := count(.elements),
                  key: {element, nowners},
                  agrouping := array_agg((SELECT _ := .grouping ORDER BY _))
              }
            ) BY .agrouping
        '''

        await self.assert_query_result(qry, res)

        # Wrapping in a select caused trouble
        await self.assert_query_result(f'SELECT ({qry})', res)

    async def test_edgeql_group_by_group_by_02(self):
        res = tb.bag([
            {
                "elements": tb.bag([
                    {"key": {"cost": 1, "element": None}, "n": 3},
                    {"key": {"cost": 2, "element": None}, "n": 2},
                    {"key": {"cost": 3, "element": None}, "n": 2},
                    {"key": {"cost": 4, "element": None}, "n": 1},
                    {"key": {"cost": 5, "element": None}, "n": 1},
                ]),
                "key": {"grouping": ["cost"]}
            },
            {
                "elements": tb.bag([
                    {"key": {"cost": None, "element": "Water"}, "n": 2},
                    {"key": {"cost": None, "element": "Earth"}, "n": 2},
                    {"key": {"cost": None, "element": "Fire"}, "n": 2},
                    {"key": {"cost": None, "element": "Air"}, "n": 3},
                ]),
                "key": {"grouping": ["element"]}
            }
        ])

        await self.assert_query_result(
            '''
            WITH MODULE cards, G := (
            GROUP (
              GROUP Card
              BY {.element, .cost}
            )
            USING grouping := array_agg(.grouping)
            BY grouping),
            SELECT G {
                key: {grouping},
                elements: { n := count(.elements), key: {element, cost}}
            }
            ''',
            res,
        )

        await self.assert_query_result(
            '''
            WITH MODULE cards,
            SELECT (
            GROUP (
              GROUP Card
              BY {.element, .cost}
            )
            USING grouping := array_agg(.grouping)
            BY grouping) {
                key: {grouping},
                elements: { n := count(.elements), key: {element, cost}}
            }
            ''',
            res,
        )

    async def _test_edgeql_group_by_group_by_03(self, qry):
        res = tb.bag([
            {
                "el": "Water",
                "groups": tb.bag([
                    {"elements": [{"cost": 2, "name": "Bog monster"}],
                     "even": 0},
                    {"elements": [{"cost": 3, "name": "Giant turtle"}],
                     "even": 1}
                ])
            },
            {
                "el": "Fire",
                "groups": [
                    {
                        "elements": tb.bag([
                            {"cost": 1, "name": "Imp"},
                            {"cost": 5, "name": "Dragon"}
                        ]),
                        "even": 1
                    }
                ]
            },
            {
                "el": "Earth",
                "groups": [
                    {
                        "elements": tb.bag([
                            {"cost": 1, "name": "Dwarf"},
                            {"cost": 3, "name": "Golem"}
                        ]),
                        "even": 1
                    }
                ]
            },
            {
                "el": "Air",
                "groups": tb.bag([
                    {
                        "elements": tb.bag([
                            {"cost": 2, "name": "Giant eagle"},
                            {"cost": 4, "name": "Djinn"}
                        ]),
                        "even": 0
                    },
                    {"elements": [{"cost": 1, "name": "Sprite"}], "even": 1}
                ])
            }
        ])

        await self.assert_query_result(qry, res)

    async def test_edgeql_group_by_group_by_03a(self):
        await self._test_edgeql_group_by_group_by_03(
            '''
            with module cards
            select (group Card by .element) {
                el := .key.element,
                groups := (
                  with z := (group .elements using x := .cost%2 by x)
                  for z in z union (
                    even := z.key.x,
                    elements := array_agg(z.elements{name, cost}),
                  )
                )
            };
            '''
        )

    async def test_edgeql_group_by_group_by_03b(self):
        await self._test_edgeql_group_by_group_by_03(
            '''
            with module cards
            select (group Card by .element) {
                el := .key.element,
                groups := (
                  with z := (group .elements using x := .cost%2 by x)
                  select (
                    even := z.key.x,
                    elements := array_agg(z.elements{name, cost}),
                  )
                )
            };
            '''
        )

    async def test_edgeql_group_by_group_by_03c(self):
        await self._test_edgeql_group_by_group_by_03(
            '''
            with module cards
            select (group Card by .element) {
                el := .key.element,
                groups := (
                  for z in (group .elements using x := .cost%2 by x) union (
                    even := z.key.x,
                    elements := array_agg(z.elements{name, cost}),
                  )
                )
            };
            '''
        )

    async def test_edgeql_group_id_errors(self):
        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"may not name a grouping alias 'id'"
        ):
            await self.con.execute('''
                group cards::Card{name} using id := .id by id
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"may not group by a field named id",
            _position=44,
        ):
            await self.con.execute('''
                group cards::Card{name} by .id
            ''')

    async def test_edgeql_group_tuple_01(self):
        await self.con.execute('''
            create type tup {
                create multi property tup -> tuple<int64, int64> ;
            };
            insert tup { tup := {(1, 1), (1, 2), (1, 1), (2, 1)} };
        ''')

        await self.assert_query_result(
            '''
                with X := tup.tup,
                group X using z := X by z;
            ''',
            tb.bag([
                {"elements": [[1, 2]], "key": {"z": [1, 2]}},
                {"elements": [[2, 1]], "key": {"z": [2, 1]}},
                {"elements": tb.bag([[1, 1], [1, 1]]), "key": {"z": [1, 1]}}
            ])
        )

    async def test_edgeql_group_tuple_02(self):
        await self.assert_query_result(
            '''
                with X := {(1, 1), (1, 2), (1, 1), (2, 1)},
                group X using z := X by z;
            ''',
            tb.bag([
                {"elements": [[1, 2]], "key": {"z": [1, 2]}},
                {"elements": [[2, 1]], "key": {"z": [2, 1]}},
                {"elements": tb.bag([[1, 1], [1, 1]]), "key": {"z": [1, 1]}}
            ])
        )

    async def test_edgeql_group_semijoin_group_01(self):
        await self.assert_query_result(
            '''
                with module cards
                group (
                    select (group Card{name, cost} by .element)
                    order by .key.element limit 1
                ).elements by .cost;
            ''',
            tb.bag([
                {
                    "elements": [{"cost": 1, "name": "Sprite"}],
                    "grouping": ["cost"],
                    "key": {"cost": 1}
                },
                {
                    "elements": [{"cost": 2, "name": "Giant eagle"}],
                    "grouping": ["cost"],
                    "key": {"cost": 2}
                },
                {
                    "elements": [{"cost": 4, "name": "Djinn"}],
                    "grouping": ["cost"],
                    "key": {"cost": 4}
                }
            ])
        )

    async def test_edgeql_group_simple_agg_01(self):
        await self.assert_query_result(
            r'''
                with module cards
                select (group Card by .element) {
                    el := .key.element, cs := array_agg(.elements)
                };
            ''',
            tb.bag([
                {'el': "Water", 'cs': [{'id': str}] * 2},
                {'el': "Fire", 'cs': [{'id': str}] * 2},
                {'el': "Earth", 'cs': [{'id': str}] * 2},
                {'el': "Air", 'cs': [{'id': str}] * 3},
            ]),
        )

    async def test_edgeql_group_simple_agg_02(self):
        await self.assert_query_result(
            r'''
                with module cards
                select (group Card by .element) {
                    el := .key.element, cs := array_agg(.elements { name })
                };
            ''',
            tb.bag([
                {
                    "cs": tb.bag(
                        [{"name": "Bog monster"}, {"name": "Giant turtle"}]),
                    "el": "Water"
                },
                {
                    "cs": tb.bag([{"name": "Imp"}, {"name": "Dragon"}]),
                    "el": "Fire",
                },
                {
                    "cs": tb.bag([{"name": "Dwarf"}, {"name": "Golem"}]),
                    "el": "Earth",
                },
                {
                    "cs": tb.bag([
                        {"name": "Sprite"},
                        {"name": "Giant eagle"},
                        {"name": "Djinn"}
                    ]),
                    "el": "Air",
                }
            ])
        )

    async def test_edgeql_group_agg_with_free_ref_01(self):
        out = await self.con.query(r'''
            with module cards
            select (group Card by .element) {
                id, els := array_agg((.id, .elements.name))
            };
        ''')

        for obj in out:
            for el in obj.els:
                self.assertEqual(obj.id, el[0])

    async def test_edgeql_group_agg_multi_01(self):
        await self.assert_query_result(
            '''
                with module cards
                for g in (group Card BY .element) union (
                    array_agg(g.elements.name ++ {"!", "?"})
                );
            ''',
            tb.bag([
                {"Bog monster!", "Bog monster?",
                 "Giant turtle!", "Giant turtle?"},
                {"Imp!", "Imp?", "Dragon!", "Dragon?"},
                {"Dwarf!", "Dwarf?", "Golem!", "Golem?"},
                {"Sprite!", "Sprite?", "Giant eagle!",
                 "Giant eagle?", "Djinn!", "Djinn?"}
            ])
        )

    async def test_edgeql_group_agg_multi_02(self):
        await self.assert_query_result(
            '''
                with module cards
                for g in (group Card BY .element) union (
                    count((Award { multi z := g.elements.name }.z))
                );          ''',
            tb.bag([6, 6, 6, 9]),
        )

    async def test_edgeql_group_agg_multi_03(self):
        await self.assert_query_result(
            '''
                for g in (group BooleanTest by .val) union (
                    array_agg(g.elements.tags)
                );
            ''',
            tb.bag([
                ["red"],
                [],
                tb.bag(["red", "green"]),
                tb.bag(["red", "black"]),
            ]),
        )

    async def test_edgeql_group_agg_grouping_01(self):
        # Something about this previously triggered a postgres ISE
        # that we had to work around.
        await self.assert_query_result(
            '''
                select (group cards::Card
                   using awd_size := count(.awards)
                by awd_size, .element) { grouping };
            ''',
            [{"grouping": ["awd_size", "element"]}] * 6,
        )
