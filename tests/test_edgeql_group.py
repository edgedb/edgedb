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
from edb.tools import test


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

    async def test_edgeql_group_simple_04(self):
        await self.assert_query_result(
            r'''
            WITH snapshots := cards::Card
            GROUP snapshots {} BY .element;
            ''',
            tb.bag([
                {
                    "elements": tb.bag([{}, {}]),
                    "key": {"element": "Water"}
                },
                {
                    "elements": tb.bag([{}, {}]),
                    "key": {"element": "Fire"}
                },
                {
                    "elements": tb.bag([{}, {}]),
                    "key": {"element": "Earth"}
                },
                {
                    "elements": tb.bag([{}, {}, {}]),
                    "key": {"element": "Air"}
                }
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

    async def test_edgeql_group_free_object_01(self):
        await self.assert_query_result(
            '''
            group {a := 1, b := 2} by .a;;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': 2},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_free_object_02(self):
        await self.assert_query_result(
            '''
            group {a := 1, b := {2, 3, 4}, c := { d := 5 } }
            using d := .c.d
            by d;
            ''',
            tb.bag([
                {
                    'key': {'d': 5},
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'a': 1, 'b': [2, 3, 4], 'c': {'d': 5}}
                    ]),
                },
            ])
        )

    async def test_edgeql_group_iterator_ptr_sets_01(self):
        await self.assert_query_result(
            '''
            group (
                for n in { 8, 9 }
                    select cards::User { name, b := n }
            ) by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': 8},
                        {'name': 'Alice', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': 8},
                        {'name': 'Bob', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': 8},
                        {'name': 'Carol', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': 8},
                        {'name': 'Dave', 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_iterator_ptr_sets_02(self):
        # Use computed pointer in by clause
        await self.assert_query_result(
            '''
            group (
                for n in { 8, 9 }
                    select cards::User { name, b := n }
            ) by .b;
            ''',
            tb.bag([
                {
                    'key': {'b': 8},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': 8},
                        {'name': 'Bob', 'b': 8},
                        {'name': 'Carol', 'b': 8},
                        {'name': 'Dave', 'b': 8},
                    ]),
                },
                {
                    'key': {'b': 9},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': 9},
                        {'name': 'Bob', 'b': 9},
                        {'name': 'Carol', 'b': 9},
                        {'name': 'Dave', 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_iterator_ptr_sets_03(self):
        await self.assert_query_result(
            '''
            with N := (for n in { 8, 9 } select n)
            group cards::User { name, b := N } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {8, 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_iterator_ptr_sets_04(self):
        await self.assert_query_result(
            '''
            with N := (for n in { 8, 9 } select n)
            group cards::User { name, b := N }
            using total := sum(.b)
            by total;
            ''',
            tb.bag([
                {
                    'key': {'total': 17},
                    'grouping': {'total'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {8, 9}},
                        {'name': 'Bob', 'b': {8, 9}},
                        {'name': 'Carol', 'b': {8, 9}},
                        {'name': 'Dave', 'b': {8, 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_iterator_ptr_sets_05(self):
        await self.assert_query_result(
            '''
            group cards::User {
                name,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_iterator_ptr_sets_06(self):
        # Use computed pointer in by clause
        await self.assert_query_result(
            '''
            group cards::User {
                name,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            using d := .b.d
            by d;
            ''',
            tb.bag([
                {
                    'key': {'d': 9},
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 3, 'd': 9}},
                        {'name': 'Bob', 'b': {'c': 3, 'd': 9}},
                        {'name': 'Carol', 'b': {'c': 3, 'd': 9}},
                        {'name': 'Dave', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_iterator_ptr_free_object_01(self):
        await self.assert_query_result(
            '''
            group (
                for n in { 8, 9 }
                    select { a := 1, b := n }
            ) by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': 8},
                        {'a': 1, 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_iterator_ptr_free_object_02(self):
        # Use computed pointer in by clause
        await self.assert_query_result(
            '''
            group (
                for n in { 8, 9 }
                    select { a := 1, b := n }
            ) by .b;
            ''',
            tb.bag([
                {
                    'key': {'b': 8},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'a': 1, 'b': 8},
                    ]),
                },
                {
                    'key': {'b': 9},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'a': 1, 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_iterator_ptr_free_object_03(self):
        await self.assert_query_result(
            '''
            with N := (for n in { 8, 9 } select n)
            group { a := 1, b := N } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {8, 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_iterator_ptr_free_object_04(self):
        await self.assert_query_result(
            '''
            group {
                a := 1,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_iterator_ptr_free_object_05(self):
        # Use computed pointer in by clause
        await self.assert_query_result(
            '''
            group {
                a := 1,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            using d := .b.d
            by d;
            ''',
            tb.bag([
                {
                    'key': {'d': 9},
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_set_01(self):
        await self.assert_query_result(
            '''
            select (
                group cards::User { name, b := random() } by .name
            ) {
                key,
                grouping,
                elements: { name, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_set_02(self):
        await self.assert_query_result(
            '''
            select (
                group cards::User { name, b := random() } by .b
            ) {
                name: (select .elements.name limit 1),
                grouping,
                elements: { name, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'name': 'Alice',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'z': True},
                    ]),
                },
                {
                    'name': 'Bob',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'z': True},
                    ]),
                },
                {
                    'name': 'Carol',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'z': True},
                    ]),
                },
                {
                    'name': 'Dave',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'z': True},
                    ]),
                },
            ])
        )

    @test.xfail("""
        Issue #8095

        Select group produces incorrect keys
    """)
    async def test_edgeql_group_volatile_ptr_set_03(self):
        await self.assert_query_result(
            '''
            select (
                group (select cards::User { name, b := random() }) by .name
            ) {
                key,
                grouping,
                elements: { name, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_set_04(self):
        await self.assert_query_result(
            '''
            select (
                group cards::User {
                    name,
                    b := { c := 2, d := random() },
                }
                by .name
            ) {
                key,
                grouping,
                elements: {
                    name,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_set_05(self):
        await self.assert_query_result(
            '''
            select (
                group cards::User {
                    name,
                    b := { c := 2, d := random() },
                }
                using d := .b.c
                by d
            ) {
                name: (select .elements.name limit 1),
                grouping,
                elements: {
                    a,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'name': 'Alice',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'name': 'Bob',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'name': 'Carol',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'name': 'Dave',
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xfail("""
        Issue #8095

        Select group produces incorrect keys
    """)
    async def test_edgeql_group_volatile_ptr_set_06(self):
        await self.assert_query_result(
            '''
            select (
                group (
                    select cards::User {
                        name,
                        b := { c := 2, d := random() },
                    }
                )
                by .name
            ) {
                key,
                grouping,
                elements: {
                    name,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_free_object_01(self):
        await self.assert_query_result(
            '''
            select (
                group { a := 1, b := random() } by .a
            ) {
                key,
                grouping,
                elements: { a, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_free_object_02(self):
        await self.assert_query_result(
            '''
            select (
                group { a := 1, b := random() } by .b
            ) {
                key,
                grouping,
                elements: { a, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'a': 1, 'z': True},
                    ]),
                },
            ])
        )

    @test.xfail("""
        Issue #8095

        Select group produces incorrect keys
    """)
    async def test_edgeql_group_volatile_ptr_free_object_03(self):
        await self.assert_query_result(
            '''
            select (
                group (select { a := 1, b := random() }) by .a
            ) {
                key,
                grouping,
                elements: { a, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_free_object_04(self):
        await self.assert_query_result(
            '''
            select (
                group {
                    a := 1,
                    b := { c := 2, d := random() },
                }
                by .a
            ) {
                key,
                grouping,
                elements: {
                    a,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_volatile_ptr_free_object_05(self):
        await self.assert_query_result(
            '''
            select (
                group {
                    a := 1,
                    b := { c := 2, d := random() },
                }
                using d := .b.c
                by d
            ) {
                key,
                grouping,
                elements: {
                    a,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_volatile_ptr_free_object_06(self):
        await self.assert_query_result(
            '''
            select (
                group (
                    select {
                        a := 1,
                        b := { c := 2, d := random() },
                    }
                )
                using d := .b.c
                by d
            ) {
                key,
                grouping,
                elements: {
                    a,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_duplicate_rejected_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "used directly in the BY clause",
        ):
            await self.con.execute('''
                group Card { name }
                using element := .cost
                by cube(.element, element)
            ''')

    async def test_edgeql_group_duplicate_rejected_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "BY clause cannot refer to link property and object property with "
            "the same name",
        ):
            await self.con.execute('''
                WITH MODULE cards
                SELECT Card {
                    invalid := (
                        GROUP .avatar
                        BY @text, .text
                    )
                }
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "BY clause cannot refer to link property and object property with "
            "the same name",
        ):
            await self.con.execute('''
                WITH MODULE cards
                SELECT Card {
                    invalid := (
                        GROUP .avatar
                        BY .text, @text
                    )
                }
            ''')

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

    @tb.needs_factoring_weakly
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

    async def test_edgeql_group_errors_id(self):
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

    async def test_edgeql_group_errors_ref(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"variable 'name' referenced in BY but not declared in USING"
        ):
            await self.con.execute('''
                group User by name
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

    async def test_edgeql_trivial_grouping_01(self):
        await self.assert_query_result(
            '''
            group 0 using x := 0 by cube(x)
            ''',
            tb.bag([
                {"elements": [0], "grouping": [], "key": {"x": None}},
                {"elements": [0], "grouping": ["x"], "key": {"x": 0}}
            ]),
        )

    async def test_edgeql_group_binding_01(self):
        await self.assert_query_result(
            '''
                with GR := (group cards::Card BY .element)
                select GR {
                  multi elements := (
                    with els := .elements
                    select els {name}
                  )
                };
            ''',
            tb.bag([
                {
                    "elements": tb.bag(
                        [{"name": "Bog monster"}, {"name": "Giant turtle"}]),
                },
                {
                    "elements": tb.bag([{"name": "Imp"}, {"name": "Dragon"}]),
                },
                {
                    "elements": tb.bag([{"name": "Dwarf"}, {"name": "Golem"}]),
                },
                {
                    "elements": tb.bag([
                        {"name": "Sprite"},
                        {"name": "Giant eagle"},
                        {"name": "Djinn"}
                    ]),
                }
            ])
        )

    async def test_edgeql_group_binding_free_object_01(self):
        await self.assert_query_result(
            '''
            with X := {a := 1, b := 2}
            group X { a, b } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': 2},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_free_object_02(self):
        await self.assert_query_result(
            '''
            with X := {a := 1, b := {2, 3, 4}, c := { d := 5 } }
            group X { a, b, c: {*} } using d := .c.d by d;
            ''',
            tb.bag([
                {
                    'key': {'d': 5},
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'a': 1, 'b': [2, 3, 4], 'c': {'d': 5}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_volatile_01(self):
        await self.assert_query_result(
            '''
            with N := random()
            group cards::User { name } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice'},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob'},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol'},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave'},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_volatile_02(self):
        await self.assert_query_result(
            '''
            with N := random()
            group { a := 1 } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_03(self):
        await self.assert_query_result(
            '''
            with N := random()
            group cards::User { name }
            using z := N <= 1
            by z;
            ''',
            tb.bag([
                {
                    'key': {'z': True},
                    'grouping': {'z'},
                    'elements': tb.bag([
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_04(self):
        await self.assert_query_result(
            '''
            with N := random()
            group { a := 1 }
            using z := N <= 1
            by z;
            ''',
            tb.bag([
                {
                    'key': {'z': True},
                    'grouping': {'z'},
                    'elements': tb.bag([
                        {'a': 1},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_05(self):
        await self.assert_query_result(
            '''
            select (
                with
                    N := random()
                group cards::User { name, b := N } by .name
            ) {
                key,
                grouping,
                elements: { name, z := .b <= 1},
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_06(self):
        await self.assert_query_result(
            '''
            select (
                with
                    N := random(),
                group { a := 1, b := N } by .a
            ) {
                key,
                grouping,
                elements: { a, z := .b <= 1},
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_set_01(self):
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select cards::User { name, b := n }
            )
            group X { name, b } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': 8},
                        {'name': 'Alice', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': 8},
                        {'name': 'Bob', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': 8},
                        {'name': 'Carol', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': 8},
                        {'name': 'Dave', 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_02(self):
        # Remove computed pointer from output shape
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select cards::User { name, b := n }
            )
            group X { name } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice'},
                        {'name': 'Alice'},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob'},
                        {'name': 'Bob'},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol'},
                        {'name': 'Carol'},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave'},
                        {'name': 'Dave'},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_03(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select cards::User { name, b := n }
            )
            group (select X { name, b }) by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': 8},
                        {'name': 'Alice', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': 8},
                        {'name': 'Bob', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': 8},
                        {'name': 'Carol', 'b': 9},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': 8},
                        {'name': 'Dave', 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_04(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select cards::User { name, b := n }
            )
            group X { name } by .b;
            ''',
            tb.bag([
                {
                    'key': {'b': 8},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ]),
                },
                {
                    'key': {'b': 9},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'name': 'Alice'},
                        {'name': 'Bob'},
                        {'name': 'Carol'},
                        {'name': 'Dave'},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_05(self):
        await self.assert_query_result(
            '''
            with X := cards::User {
                name,
                b := (for n in { 8, 9 } select n),
            }
            group X { name, b } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {8, 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_06(self):
        await self.assert_query_result(
            '''
            with X := cards::User {
                name,
                b := (for n in { 8, 9 } select n),
            }
            group X { name, b }
            using total := sum(.b)
            by total;
            ''',
            tb.bag([
                {
                    'key': {'total': 17},
                    'grouping': {'total'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {8, 9}},
                        {'name': 'Bob', 'b': {8, 9}},
                        {'name': 'Carol', 'b': {8, 9}},
                        {'name': 'Dave', 'b': {8, 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_07(self):
        await self.assert_query_result(
            '''
            with
                N := (for n in { 8, 9 } select n),
                X := cards::User { name, b := N }
            group X { name, b } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {8, 9}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {8, 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_08(self):
        await self.assert_query_result(
            '''
            with
                N := (for n in { 8, 9 } select n),
                X := cards::User { name, b := N }
            group X { name, b }
            using total := sum(.b)
            by total;
            ''',
            tb.bag([
                {
                    'key': {'total': 17},
                    'grouping': {'total'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {8, 9}},
                        {'name': 'Bob', 'b': {8, 9}},
                        {'name': 'Carol', 'b': {8, 9}},
                        {'name': 'Dave', 'b': {8, 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_set_09(self):
        await self.assert_query_result(
            '''
            with X := cards::User {
                name,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group X { name, b: { c, d } } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_set_10(self):
        # Remove pointer from output shape
        await self.assert_query_result(
            '''
            with X := cards::User {
                name,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group X { name, b: { c } } by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 3}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 3}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 3}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 3}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_set_11(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            with X := cards::User {
                name,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group (select X { name, b: { c, d } }) by .name;
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_set_12(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            with X := cards::User {
                name,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group X { name, b: { c } }
            using d := .b.d
            by d;
            ''',
            tb.bag([
                {
                    'key': {'d': 9},
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 3, 'd': 9}},
                        {'name': 'Bob', 'b': {'c': 3, 'd': 9}},
                        {'name': 'Carol', 'b': {'c': 3, 'd': 9}},
                        {'name': 'Dave', 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_free_object_01(self):
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select { a := 1, b := n }
            )
            group X { a, b } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': 8},
                        {'a': 1, 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_02(self):
        # Remove computed pointer from output shape
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select { a := 1, b := n }
            )
            group X { a } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1},
                        {'a': 1},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_03(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select { a := 1, b := n }
            )
            group (select X { a, b }) by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': 8},
                        {'a': 1, 'b': 9},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_04(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            with X := (
                for n in { 8, 9 }
                    select { a := 1, b := n }
            )
            group X { a } by .b;
            ''',
            tb.bag([
                {
                    'key': {'b': 8},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'a': 1},
                    ]),
                },
                {
                    'key': {'b': 9},
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'a': 1},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_05(self):
        await self.assert_query_result(
            '''
            with X := {
                a := 1,
                b := (for n in { 8, 9 } select n),
            }
            group X { a, b } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {8, 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_06(self):
        await self.assert_query_result(
            '''
            with X := {
                a := 1,
                b := (for n in { 8, 9 } select n),
            }
            group X { a, b }
            using total := sum(.b)
            by total;
            ''',
            tb.bag([
                {
                    'key': {'total': 17},
                    'grouping': {'total'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {8, 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_07(self):
        await self.assert_query_result(
            '''
            with
                N := (for n in { 8, 9 } select n),
                X := { a := 1, b := N }
            group X { a, b } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {8, 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_08(self):
        await self.assert_query_result(
            '''
            with
                N := (for n in { 8, 9 } select n),
                X := { a := 1, b := N }
            group X { a, b }
            using total := sum(.b)
            by total;
            ''',
            tb.bag([
                {
                    'key': {'total': 17},
                    'grouping': {'total'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {8, 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_free_object_09(self):
        await self.assert_query_result(
            '''
            with X := {
                a := 1,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group X { a, b: { c, d } } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_binding_iterator_ptr_free_object_10(self):
        # Remove pointer from output shape
        await self.assert_query_result(
            '''
            with X := {
                a := 1,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group X { a, b: { c } } by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 3}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_free_object_11(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            with X := {
                a := 1,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group (select X { a, b: { c, d } }) by .a;
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 3, 'd': 9}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize computed pointers properly""")
    async def test_edgeql_group_binding_iterator_ptr_free_object_12(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            with X := {
                a := 1,
                b := (for n in { 9 } union ({ c := 3, d := n }))
            }
            group X { a, b: { c } }
            using d := .b.d
            by d;
            ''',
            tb.bag([
                {
                    'key': {'d': 9},
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 3}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_01(self):
        await self.assert_query_result(
            '''
            select (
                with X := cards::User { name, b := random() }
                group X { name, b } by .name;
            ) {
                key,
                grouping,
                elements: { name, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_02(self):
        # Remove pointer from output shape
        await self.assert_query_result(
            '''
            select (
                with X := cards::User { name, b := random() }
                group X { name } by .name;
            ) {
                key,
                grouping,
                elements: { name },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice'},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob'},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol'},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave'},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_03(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            select (
                with X := cards::User { name, b := random() }
                group (select X { name, b }) by .name;
            ) {
                key,
                grouping,
                elements: { name, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'z': True},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_04(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            select (
                with X := cards::User { name, b := random() }
                group X { name } by .b;
            ) {
                name: (select .elements.name limit 1),
                grouping,
                elements: { name, z := .b <= 1 },
            };
            ''',
            tb.bag(
                {
                    'name': 'Alice',
                    'grouping': {'b'},
                    'elements': tb.bag(
                        {'name': 'Alice', 'z': True},
                    ),
                },
                {
                    'name': 'Bob',
                    'grouping': {'b'},
                    'elements': tb.bag(
                        {'name': 'Bob', 'z': True},
                    ),
                },
                {
                    'name': 'Carol',
                    'grouping': {'b'},
                    'elements': tb.bag(
                        {'name': 'Carol', 'z': True},
                    ),
                },
                {
                    'name': 'Dave',
                    'grouping': {'b'},
                    'elements': tb.bag(
                        {'name': 'Dave', 'z': True},
                    ),
                },
            )
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_05(self):
        await self.assert_query_result(
            '''
            select (
                with X := cards::User {
                    name,
                    b := { c := 2, d := random() }
                }
                group X { name, b: { c, d } } by .name;
            ) {
                key,
                grouping,
                elements: {
                    name,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_06(self):
        # Remove pointer from output shape
        await self.assert_query_result(
            '''
            select (
                with X := cards::User {
                    name,
                    b := { c := 2, d := random() }
                }
                group (select X { name, b: { c } }) by .name;
            ) {
                key,
                grouping,
                elements: {
                    name,
                    b: { c },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 2}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 2}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 2}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 2}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_07(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            select (
                with X := cards::User {
                    name,
                    b := { c := 2, d := random() }
                }
                group (select X { name, b: { c, d } }) by .name;
            ) {
                key,
                grouping,
                elements: {
                    name,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'name': 'Alice'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Bob'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Carol'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'key': {'name': 'Dave'},
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_set_08(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            select (
                with X := cards::User {
                    name,
                    b := { c := 2, d := random() }
                }
                group (select X { name, b: { c } })
                using d := .b.c
                by d;
            ) {
                name: (select .elements.name limit 1),
                grouping,
                elements: {
                    name,
                    b: { c },
                },
            };
            ''',
            tb.bag([
                {
                    'name': 'Alice',
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Alice', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'name': 'Bob',
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Bob', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'name': 'Carol',
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Carol', 'b': {'c': 2, 'z': True}},
                    ]),
                },
                {
                    'name': 'Dave',
                    'grouping': {'name'},
                    'elements': tb.bag([
                        {'name': 'Dave', 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_01(self):
        await self.assert_query_result(
            '''
            select (
                with X := { a := 1, b := random() }
                group X { a, b } by .a;
            ) {
                key,
                grouping,
                elements: { a, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_02(self):
        # Remove pointer from output shape
        await self.assert_query_result(
            '''
            select (
                with X := { a := 1, b := random() }
                group X { a } by .a;
            ) {
                key,
                grouping,
                elements: { a },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_03(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            select (
                with X := { a := 1, b := random() }
                group (select X { a, b }) by .a;
            ) {
                key,
                grouping,
                elements: { a, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_04(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            select (
                with X := { a := 1, b := random() }
                group X { a } by .b;
            ) {
                key,
                grouping,
                elements: { a, z := .b <= 1 },
            };
            ''',
            tb.bag([
                {
                    'grouping': {'b'},
                    'elements': tb.bag([
                        {'a': 1, 'z': True},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_05(self):
        await self.assert_query_result(
            '''
            select (
                with X := {
                    a := 1,
                    b := { c := 2, d := random() }
                }
                group X { a, b: { c, d } } by .a;
            ) {
                key,
                grouping,
                elements: {
                    a,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_06(self):
        # Remove pointer from output shape
        await self.assert_query_result(
            '''
            select (
                with X := {
                    a := 1,
                    b := { c := 2, d := random() }
                }
                group (select X { a, b: { c } }) by .a;
            ) {
                key,
                grouping,
                elements: {
                    a,
                    b: { c },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 2}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_07(self):
        # Wrap subject in select
        await self.assert_query_result(
            '''
            select (
                with X := {
                    a := 1,
                    b := { c := 2, d := random() }
                }
                group (select X { a, b: { c, d } }) by .a;
            ) {
                key,
                grouping,
                elements: {
                    a,
                    b: {
                        c,
                        z := .d <= 1,
                    },
                },
            };
            ''',
            tb.bag([
                {
                    'key': {'a': 1},
                    'grouping': {'a'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 2, 'z': True}},
                    ]),
                },
            ])
        )

    @test.xerror("""Group by doesn't materialize volatile properly""")
    async def test_edgeql_group_binding_volatile_ptr_free_object_08(self):
        # Use computed pointer only in by clause
        await self.assert_query_result(
            '''
            select (
                with X := {
                    a := 1,
                    b := { c := 2, d := random() }
                }
                group (select X { a, b: { c } })
                using d := .b.c
                by d;
            ) {
                key,
                grouping,
                elements: {
                    a,
                    b: { c },
                },
            };
            ''',
            tb.bag([
                {
                    'grouping': {'d'},
                    'elements': tb.bag([
                        {'a': 1, 'b': {'c': 2}},
                    ]),
                },
            ])
        )

    async def test_edgeql_group_ordering_01(self):
        res = [
            {
                "elements": tb.bag([
                    {"name": "Sprite"},
                    {"name": "Giant eagle"},
                    {"name": "Djinn"}
                ]),
            },
            {
                "elements": tb.bag([{"name": "Dwarf"}, {"name": "Golem"}]),
            },
            {
                "elements": tb.bag([{"name": "Imp"}, {"name": "Dragon"}]),
            },
            {
                "elements": tb.bag(
                    [{"name": "Bog monster"}, {"name": "Giant turtle"}]),
            },
        ]

        await self.assert_query_result(
            '''
                with GR := (group cards::Card BY .element)
                select GR {
                  elements: {name},
                }
                order by .key.element;
            ''',
            res,
        )

        await self.assert_query_result(
            '''
                with GR := (group cards::Card BY .element)
                select GR {
                  key: {element},
                  elements: {name},
                }
                order by .key.element;
            ''',
            res,
        )

    async def test_edgeql_group_of_for_01(self):
        await self.assert_query_result(
            '''
                WITH
                  C := (FOR c IN cards::Card UNION (
                    SELECT c { len := len(c.name) }
                  ))
                GROUP C { name }
                USING l := C.len
                BY l;
            ''',
            tb.bag([
                {
                    "elements": tb.bag([
                        {"name": "Bog monster"}, {"name": "Giant eagle"}]
                    ),
                    "grouping": ["l"],
                    "key": {"l": 11},
                },
                {
                    "elements": tb.bag([{"name": "Imp"}]),
                    "grouping": ["l"],
                    "key": {"l": 3},
                },
                {
                    "elements": tb.bag([
                        {"name": "Dwarf"}, {"name": "Golem"}, {"name": "Djinn"}
                    ]),
                    "grouping": ["l"],
                    "key": {"l": 5}
                },
                {
                    "elements": tb.bag([
                        {"name": "Dragon"}, {"name": "Sprite"}
                    ]),
                    "grouping": ["l"],
                    "key": {"l": 6}
                },
                {
                    "elements": [{"name": "Giant turtle"}],
                    "grouping": ["l"],
                    "key": {"l": 12}
                }
            ])
        )

    async def test_edgeql_group_policies_01(self):
        await self.con.execute('''
            with module cards
            alter type User {
                create access policy ok allow select, delete, update read;
                create access policy two_elements allow insert, update write
                  using (count((group .deck by .element)) = 2);
            }
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on insert"):
            await self.con.query('''
                with module cards
                insert User {
                    name := 'Sully',
                    deck := (select Card filter .element = 'Water')
                };
            ''')

        await self.con.query('''
            with module cards
            insert User {
                name := 'Sully',
                deck := (select Card filter .element IN {'Water', 'Air'})
            };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on update"):
            await self.con.query('''
                with module cards
                update User filter .name = 'Sully' set {
                    deck += (select Card filter .element = 'Earth')
                };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on update"):
            await self.con.query('''
                with module cards
                update User filter .name = 'Sully' set {
                    deck -= (select Card filter .element = 'Water')
                };
            ''')

    async def test_edgeql_group_policies_02(self):
        await self.con.execute(
            '''
                create type T {
                    create multi property vals -> int64;
                    create access policy foo allow all using (
                    # This is pretty pointless but should always be true
                      sum(((
                        (group x := .vals using v := x by v))
                        { x := count(.elements) }).x)
                      = count(.vals)
                    )
                };
                insert T { vals := {1,1,2,3} };
            '''
        )

        await self.assert_query_result(
            r'''select T { vals }''',
            [{'vals': tb.bag([1, 1, 2, 3])}],
        )

    async def test_edgeql_group_rebind_filter_01(self):
        await self.assert_query_result(
            '''
                with cardsByCost := (
                  group cards::Card by .cost
                )
                select cardsByCost {
                  key: {cost},
                  count := count(.elements),
                } filter .count > 1;
            ''',
            tb.bag([
                {"count": 3, "key": {"cost": 1}},
                {"count": 2, "key": {"cost": 2}},
                {"count": 2, "key": {"cost": 3}},
            ])
        )

    async def test_edgeql_group_rebind_filter_02(self):
        await self.assert_query_result(
            '''
                with cardsByCost := (
                  group cards::Card by .cost
                )
                select cardsByCost {
                  key: {cost},
                  count := count(.elements),
                } filter .count > 1 order by .key.cost
            ''',
            [
                {"count": 3, "key": {"cost": 1}},
                {"count": 2, "key": {"cost": 2}},
                {"count": 2, "key": {"cost": 3}},
            ]
        )

    async def test_edgeql_group_rebind_filter_03(self):
        await self.assert_query_result(
            '''
                with cardsByCost := (
                  group cards::Card by .cost
                )
                select (select cardsByCost) {
                  key: {cost},
                  count := count(.elements),
                } filter .count > 1;
            ''',
            tb.bag([
                {"count": 3, "key": {"cost": 1}},
                {"count": 2, "key": {"cost": 2}},
                {"count": 2, "key": {"cost": 3}},
            ])
        )

    async def test_edgeql_group_binding_complex_01(self):
        # This query is an adaptation of the query from #4481 to the
        # cards database. In the process of adaptation I lost track of
        # what it actually *does*. The toy model agrees with the
        # results, though.
        await self.assert_query_result(
            '''
            WITH MODULE cards,
              __scope_0_stdFreeObject := (
                WITH
                  __scope_2_defaultBill := DETACHED User,
                  __scope_2_defaultBill_groups := (
                    GROUP __scope_2_defaultBill
                    USING
                      category := __scope_2_defaultBill.avatar.name
                    BY category
                )
                SELECT __scope_2_defaultBill_groups {
                  key: {category},
                  grouping,
                  elements: {
                    id
                  }
                }
              )
            SELECT __scope_0_stdFreeObject {
              single sum := (
                sum(len(__scope_0_stdFreeObject.elements.name))
                - sum((WITH
                  __scope_1_defaultAssignedPayment :=
                    __scope_0_stdFreeObject.elements.<friends[is User]
                SELECT __scope_1_defaultAssignedPayment {
                  id
                }
                FILTER (exists __scope_1_defaultAssignedPayment.avatar))
              .deck_cost)
              )
            };
          ''',
            tb.bag([{"sum": -7}, {"sum": 5}, {"sum": -23}]),
        )

    async def test_edgeql_group_enumerate_01(self):
        await self.assert_query_result(
            '''
                group enumerate({'a', 'b', 'c', 'd'})
                using groupIndex := .0 // 2
                by groupIndex;
            ''',
            tb.bag([
                {
                    "elements": tb.bag([[0, "a"], [1, "b"]]),
                    "grouping": ["groupIndex"],
                    "key": {"groupIndex": 0}
                },
                {
                    "elements": tb.bag([[2, "c"], [3, "d"]]),
                    "grouping": ["groupIndex"],
                    "key": {"groupIndex": 1}
                }
            ]),
        )

    async def test_edgeql_group_enumerate_02(self):
        await self.assert_query_result(
            '''
                group enumerate(array_unpack(['a', 'b', 'c', 'd']))
                using groupIndex := .0 // 2
                by groupIndex;
            ''',
            tb.bag([
                {
                    "elements": tb.bag([[0, "a"], [1, "b"]]),
                    "grouping": ["groupIndex"],
                    "key": {"groupIndex": 0}
                },
                {
                    "elements": tb.bag([[2, "c"], [3, "d"]]),
                    "grouping": ["groupIndex"],
                    "key": {"groupIndex": 1}
                }
            ]),
        )

    async def test_edgeql_group_uses_name_01(self):
        # Make sure that our crappy optimizations don't break anything
        await self.con.query(
            r'''
            WITH g := (GROUP cards::Card BY .cost)
            SELECT g {
              key: {cost},
              grouping,
              elements: {
                name,
                multi owners := g.elements.owners { name },
              }
            };
            ''',
        )

    async def test_edgeql_group_backlink(self):
        await self.assert_query_result(
            r'''
            select (group cards::Award by .winner) {
              a := .key.winner,
            };
            ''',
            [{"a": {}}, {"a": {}}],
        )

    async def test_edgeql_group_link_property_01(self):
        await self.assert_query_result(
            r'''
            with module cards
            select User {
              cards_by_count := (group .deck by @count) {
                key : {count},
                elements: {name},
              }
            }
            filter .name = 'Alice';
            ''',
            [
                {
                    "cards_by_count": [
                        {
                            "key": {"count": 2},
                            "elements": [
                                {"name": "Imp"},
                                {"name": "Dragon"}
                            ]
                        },
                        {
                            "key": {"count": 3},
                            "elements": [
                                {"name": "Bog monster"},
                                {"name": "Giant turtle"}
                            ]
                        }
                    ]
                }
            ],
        )

        await self.assert_query_result(
            r'''
            with module cards
            select User {
              cards_by_count := (group .deck by (@count, @count)) {
                key : {count},
                elements: {name},
              }
            }
            filter .name = 'Alice';
            ''',
            [
                {
                    "cards_by_count": [
                        {
                            "key": {"count": 2},
                            "elements": [
                                {"name": "Imp"},
                                {"name": "Dragon"}
                            ]
                        },
                        {
                            "key": {"count": 3},
                            "elements": [
                                {"name": "Bog monster"},
                                {"name": "Giant turtle"}
                            ]
                        }
                    ]
                }
            ],
        )

    async def test_edgeql_group_destruct_immediately_01(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            select (group Card by .element).key.element
            ''',
            {"Fire", "Earth", "Water", "Air"},
        )

    async def test_edgeql_group_destruct_immediately_02(self):
        await self.assert_query_result(
            r'''
            WITH MODULE cards
            select (group Card by .element).grouping
            ''',
            ["element", "element", "element", "element"],
        )

    async def test_edgeql_group_issue_5796(self):
        await self.assert_query_result(
            r'''
            with
              module cards,
              groups := (
                group User { deck }
                by .name
              )
            select groups {
              name := .key.name,
            }
            limit 5;
            ''',
            tb.bag([
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Carol"},
                {"name": "Dave"},
            ]),
        )

    async def test_edgeql_group_issue_6059(self):
        await self.assert_query_result(
            r'''
            with
              module cards,
              groups := (group Card by .element)
            select groups {
              keyCard := (
                select .elements { id }
                limit 1
              ),
            }
            order by .keyCard.cost
            limit 100;
            ''',
            [{"keyCard": {}}] * 4,
        )

    async def test_edgeql_group_issue_6060(self):
        await self.assert_query_result(
            r'''
            with
              module cards,
              groups := (group Card by .element),
              submissions := (
                groups {
                  minCost := min(.elements.cost)
                }
              )
            select submissions {
              minCost
            }
            order by .minCost;
            ''',
            [{"minCost": 1}, {"minCost": 1}, {"minCost": 1}, {"minCost": 2}],
        )

    @test.xerror("""
        Issue #6481

        assert stype.is_view(ctx.env.schema) when in _inline_type_computable
    """)
    async def test_edgeql_group_issue_6481(self):
        # NO_FACTOR makes it pass but we don't want it to unexpected
        # pass since it still fails on other modes
        if self.NO_FACTOR:
            raise RuntimeError('sigh')

        await self.assert_query_result(
            r'''
            select (
              group (
                select Comment {iowner:=.issue.owner}
              )
              by .iowner
            ) { }.elements;
            ''',
            [{}],
            always_typenames=True,
        )

    @test.xerror("""
        Issue #6019

        Grouping on key should probably be rejected.
        (And if not, it should not ISE!)
    """)
    async def test_edgeql_group_issue_6019_a(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
        ):
            self.con.execute('''
                group (
                  group (
                    select Issue
                  ) by .owner
                ) by .key
            ''')

    async def test_edgeql_group_issue_6019_b(self):
        # This didn't work because group created free objects which were then
        # materialized as volatile. `group (group X by .x) by .key` has a
        # different cause.
        await self.assert_query_result(
            '''
            with
              module cards,
              g1 := (group Card by .element),
              flattened := (select g1 {element:=(.key.element)}),
            group flattened by .element
            ''',
            [{}] * 4,
        )

    @test.xerror("""
        Issue #5828

        Only fails with implicit_limit and typename injection.
        "there is no range var..."
    """)
    async def test_edgeql_group_issue_5828(self):
        await self.assert_query_result(
            '''
            with module cards
            group User {
              deck: {awards: {name}},
            }
            by .name;
            ''',
            [
                {'elements': [{}]},
                {'elements': [{}]},
                {'elements': [{}]},
                {'elements': [{}]},
            ],
            always_typenames=True,
            implicit_limit=100,
        )

    @test.xerror("""
        Issue #5757

        Only fails with typename injection.
        Materialized set not finalized
    """)
    async def test_edgeql_group_issue_5757(self):
        await self.assert_query_result(
            '''
            select (
              select (group User by .name) {}
            ) {
              xxx := .elements.name,
            };
            ''',
            [{}, {}],
            always_typenames=True,
        )

    async def test_edgeql_group_issue_4897(self):
        await self.assert_query_result(
            '''
            group Issue { name }
            using owner := .owner
            by owner;
            ''',
            tb.bag([
                {
                    "key": {"owner": {"id": str}},
                    "elements": [
                        {"name": "Release EdgeDB"}, {"name": "Regression."}
                    ],
                },
                {
                    "key": {"owner": {"id": str}},
                    "elements": [
                        {"name": "Improve EdgeDB repl output rendering."},
                        {"name": "Repl tweak."},
                    ],
                }
            ])
        )


class TestEdgeQLGroupNoFactor(TestEdgeQLGroup):
    NO_FACTOR = True


class TestEdgeQLGroupWarnFactor(TestEdgeQLGroup):
    WARN_FACTOR = True
