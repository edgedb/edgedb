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


from edb.common import assert_data_shape
import os
from edb.testbase import server as tb
bag = assert_data_shape.bag

# queries to populate the required data for this test
# schema summary:
# Person has name, multi_prop, notes, tag
# Note has name, note
# Foo has val, opt
# initial_schema = """
# type Note {
#     required single property name -> str;
#     optional single property note -> str;
# }
# type Person {
#     required single property name -> str;
#     optional multi property multi_prop -> str;
#     multi link notes -> Note {
#         property metanote -> str;
#     }
#     optional single property tag -> str;
# }
# type Foo {
#     required single property val -> str;
#     optional single property opt -> int64;
# }
# type Award {
#     name : str;
# }
# type Card {
#     single name : str;
#     multi awards : Award;
#     element : str;
#     cost : int;
# }


# type User {
#     required name: str {
#         delegated constraint exclusive;
#     }

#     multi deck: Card {
#         count: int64 {
#             default := 1;
#         };
#         property total_cost := @count * .cost;
#     }

#     property deck_cost := sum(.deck.cost);

#     multi friends: User {
#         nickname: str;
#         # how the friend responded to requests for a favor
#         #favor: array<bool>
#     }

#     multi awards: Award {
#         constraint exclusive;
#     }

#     avatar: Card {
#         text: str;
#         property tag := .name ++ (("-" ++ @text) ?? "");
#     }
# }
# """
# initial_queries = """
# with n0 := (insert Note {name := "boxing", note := {}}),
#      n1 := (insert Note {name := "unboxing", note := "lolol"}),
#      n2 := (insert Note {name := "dynamic", note := "blarg"}),
#      p0 := (insert Person {name := "Phil Emarg",
#                            notes := {n0, n1 {@metanote := "arg!"}}}),
#      p1 := (insert Person {name := "Madeline Hatch",
#                            notes:={n1 {@metanote := "sigh"}}}),
#      p2 := (insert Person {name := "Emmanuel Villip"}),
#      a_15 := (insert Award {name := "1st"}),
#      a_e1 := (insert Award {name := "2nd"}),
#      a_ca := (insert Award {name := "3rd"}),
#      c_27 := (insert Card {name := "Imp", element := "Fire",
#                            cost := 1, awards := {a_e1}}),
#      c_49 := (insert Card {name := "Dragon", element := "Fire",
#                            cost := 5, awards := {a_15}}),
#      c_80 := (insert Card {name := "Bog monster", element := "Water",
#                            cost := 2}),
#      c_d2 := (insert Card {name := "Giant turtle", element := "Water",
#                            cost := 3}),
#      c_46 := (insert Card {name := 'Dwarf', element := 'Earth',
#                            cost := 1}),
#      c_25 := (insert Card {name := 'Golem', element := 'Earth',
#                            cost := 3}),
#      c_bd := (insert Card {name := 'Sprite', element := 'Air',
#                            cost := 1}),
#      c_69 := (insert Card {name := 'Giant eagle', element := 'Air',
#                            cost := 2}),
#      c_87 := (insert Card {name := 'Djinn', element := 'Air',
#                            cost := 4, awards := {a_ca}}),
#      u_3e := (insert User {name := "Carol", deck := {c_80 { @count := 3},
#             c_d2 {@count := 2}, c_46 {@count := 4}, c_25 {@count := 2},
#             c_bd {@count := 4}, c_69 {@count := 3}, c_87 {@count := 1}
#         }}),
#     u_fc := (insert User {name := "Bob", deck := {
#             c_80 {@count := 3},
#             c_d2 {@count := 3},
#             c_46 {@count := 3},
#             c_25 {@count := 3}
#         }}),
#     u_56 := (insert User {name := "Dave", deck := {
#            c_49  {@count:= 1},
#            c_80  {@count:= 1},
#            c_d2  {@count:= 1},
#            c_25  {@count:= 1},
#            c_bd  {@count:= 4},
#            c_69  {@count:= 1},
#            c_87  {@count:= 1}
#         }, friends := {u_fc}, avatar := c_87 {@text := "Wow"}}),
#     u_f3 := (insert User {name := "Alice", deck := {
#             c_27 {@count:= 2},
#             c_49 {@count:= 2},
#             c_80 {@count:= 3},
#             c_d2 {@count:= 3}
#         }, friends := {
#             u_fc {@nickname := "Swampy"},
#             u_3e {@nickname := "Firefighter"},
#             u_56 {@nickname := "Grumpy"}
#         }, awards := {a_15, a_31},
#             avatar := {c_49 {@text := "Best"}}
#         }),

# select 0;
# """


class TestNewInterpreterModelSmokeTests(tb.QueryTestCase):
    """Unit tests for the toy evaluator model.

    These are intended as smoke tests. Since obviously we don't want
    two totally parallel sets of tests for EdgeQL queries, an eventual
    goal should be that we could run the real tests against the model.
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'smoke_test_interp.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'smoke_test_interp_setup.edgeql')

    # db = None
    # def assert_query_result(
    #     self, query, expected, *, sort=None, singleton_cheating=False
    # ):
    #     if self.db is None:
    #         self.db = model.db_with_initial_schema_and_queries(
    #             initial_schema_defs=initial_schema,
    #             initial_queries=initial_queries,
    #             surround_schema_with_default=True,
    #             # debug_print=True
    #         )
    #     # qltree = model.parse(query)
    #     (result, _) = model.run_single_str_get_json(
    #         self.db, query, print_asts=True)

    #     try:
    #         assert_data_shape.assert_data_shape(
    #             result, expected, self.fail)
    #     except AssertionError as e:
    #         raise AssertionError(
    #             str(e),
    #             "Expected", expected, "Actual", result)

    async def test_model_basic_01(self):
        await self.assert_query_result(
            "SELECT 1",
            [1],
        )

    async def test_model_basic_02(self):
        await self.assert_query_result(
            r"""
            SELECT Person.name
            """,
            ['Phil Emarg', 'Madeline Hatch', 'Emmanuel Villip'],
        )

    async def test_model_basic_03(self):
        await self.assert_query_result(
            r"""
            SELECT (Person.name, Person.name)
            """,
            [('Phil Emarg', 'Phil Emarg'),
             ('Madeline Hatch', 'Madeline Hatch'),
             ('Emmanuel Villip', 'Emmanuel Villip')]
        )

    async def test_model_link_dedup(self):
        await self.assert_query_result(
            r"""
            SELECT Person.notes.name
            """,
            {'boxing', 'unboxing'},
        )

    async def test_model_link_dedup_02(self):
        await self.assert_query_result(
            r"""
            SELECT count(User.friends)
            """,
            {3},
        )

    async def test_model_link_dedup_03(self):
        await self.assert_query_result(
            r"""
            SELECT count((User.friends, User.friends@nickname));
            """,
            {3},
        )

    async def test_model_link_dedup_04(self):
        await self.assert_query_result(
            r"""
            SELECT count((User.friends, User.friends@nickname ?? "None"));
            """,
            {4},
        )

    async def test_model_link_dedup_05(self):
        await self.assert_query_result(
            r"""
            SELECT count(((select User.friends), (select User.friends.name)));
            """,
            {9},
        )

    async def test_model_link_dedup_06(self):
        await self.assert_query_result(
            r"""
            SELECT count(((select User.friends),
                          (select User.friends@nickname)));
            """,
            {9},
        )

    async def test_model_link_correlation(self):
        await self.assert_query_result(
            r"""
            SELECT Person.name ++ "-" ++ Person.notes.name
            """,
            {'Phil Emarg-boxing', 'Phil Emarg-unboxing',
             'Madeline Hatch-unboxing'}
        )

    async def test_model_optional_prop_01(self):
        await self.assert_query_result(
            r"""
            SELECT (Note.note ?= "lolol", Note.name)
            """,
            {(False, 'boxing'), (True, 'unboxing'), (False, 'dynamic')}
        )

    async def test_model_optional_prop_02(self):
        await self.assert_query_result(
            r"""
            SELECT (Note.note = "lolol", Note.name)
            """,
            {(False, 'dynamic'), (True, 'unboxing')}
        )

    async def test_model_optional_prop_03(self):
        await self.assert_query_result(
            r"""
            SELECT (Note.name, ('SOME "' ++ Note.note ++ '"') ?? 'NONE')
            """,
            {('boxing', 'NONE'), ('unboxing', 'SOME "lolol"'),
             ('dynamic', 'SOME "blarg"')}
        )

    async def test_model_optional_prop_04(self):
        await self.assert_query_result(
            r"""
            SELECT (Note.name, EXISTS Note.note)
            """,
            {('boxing', False), ('unboxing', True), ('dynamic', True)}
        )

    async def test_model_optional_prop_05(self):
        await self.assert_query_result(
            r"""
            SELECT (User.name ++ User.avatar.name) ?? 'hm';
            """,
            {'AliceDragon', 'DaveDjinn'},
        )

    async def test_model_optional_prop_06(self):
        await self.assert_query_result(
            r"""
            SELECT User.name ?= User.name;
            """,
            [True, True, True, True],
        )

    async def test_model_optional_prop_07(self):
        await self.assert_query_result(
            r"""
            SELECT (User.name ?= 'Alice', count(User.name))
            """,
            bag([(True, 1), (False, 1), (False, 1), (False, 1)]),
        )

    async def test_model_subqueries_01(self):
        await self.assert_query_result(
            r"""
            SELECT count(((SELECT Person), (SELECT Person)))
            """,
            [9]
        )

    async def test_model_subqueries_02(self):
        await self.assert_query_result(
            r"""
            WITH X := {true, false, true} SELECT (any(X), all(X))
            """,
            [(True, False)]
        )

    async def test_model_subqueries_03(self):
        await self.assert_query_result(
            r"""
            SELECT enumerate((SELECT X := {"foo", "bar", "baz"} ORDER BY X));
            """,
            [(0, 'bar'), (1, 'baz'), (2, 'foo')],
        )

    async def test_model_set_union(self):
        await self.assert_query_result(
            r"""
            SELECT count({User, Card})
            """,
            [13],
        )

    async def test_edgeql_coalesce_set_of_01(self):
        await self.assert_query_result(
            r'''

                SELECT <str>Publication.id ?? <str>count(Publication)
            ''',
            ['0'],
        )

    async def test_edgeql_coalesce_set_of_02(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    Publication ?= Publication,
                    (Publication.title++Publication.title
                       ?= Publication.title) ?=
                    (Publication ?!= Publication)
                )
            ''',
            [(True, False)]
        )

    async def test_edgeql_select_clauses_01(self):
        await self.assert_query_result(
            r'''
            SELECT (Person.name, Person.notes.name)
            FILTER Person.name != "Madeline Hatch"
            ORDER BY .1 DESC;
            ''',
            [('Phil Emarg', 'unboxing'), ('Phil Emarg', 'boxing')],
        )

    async def test_edgeql_select_clauses_02(self):
        # This is a funky one.
        await self.assert_query_result(
            r'''
            SELECT (
                Person.name,
                (SELECT Person.notes.name
                 ORDER BY Person.notes.name DESC
                 LIMIT (0 if Person.name = "Madeline Hatch" ELSE 1)));
            ''',
            [('Phil Emarg', 'unboxing')]
        )

    async def test_edgeql_select_clauses_03(self):
        await self.assert_query_result(
            r'''
            WITH X := {9, 8, 7, 6, 5, 4, 3, 2, 1}
            SELECT _ := X
            FILTER _ % 2 = 1
            ORDER BY _
            OFFSET 2
            LIMIT 2
            ''',
            [5, 7],
        )

    async def test_edgeql_for_01(self):
        await self.assert_query_result(
            r'''
            FOR X IN {1,2,3} UNION ((SELECT X), (SELECT X));
            ''',
            {(1, 1), (2, 2), (3, 3)},
        )

    async def test_edgeql_for_02(self):
        await self.assert_query_result(
            r'''
            WITH X := 1, FOR x in {X} UNION (x);
            ''',
            [1],
        )

    async def test_edgeql_with_01(self):
        await self.assert_query_result(
            r'''
            WITH X := {1, 2} SELECT ((SELECT X), (SELECT X));
            ''',
            {(1, 1), (1, 2), (2, 1), (2, 2)}
        )

    async def test_edgeql_with_02(self):
        # For a while, the model produced the right answer here while
        # the real compiler didn't!
        # See https://github.com/edgedb/edgedb/issues/1381
        await self.assert_query_result(
            r'''
            WITH X := {1, 2}, Y := X+1 SELECT (X, Y);
            ''',
            {(1, 2), (1, 3), (2, 2), (2, 3)}
        )

    async def test_edgeql_array_01(self):
        await self.assert_query_result(
            r'''
            WITH X := [0,1,2,3,4,5,6,7,8,9] SELECT X[{1,2}:{5,6}];
            ''',
            bag([[1, 2, 3, 4], [1, 2, 3, 4, 5], [2, 3, 4], [2, 3, 4, 5]]),
        )

    async def test_edgeql_array_02(self):
        await self.assert_query_result(
            r'''
            SELECT array_unpack({[1,2,3],[3,4,5]});
            ''',
            [1, 2, 3, 3, 4, 5]
        )

    async def test_edgeql_array_03(self):
        await self.assert_query_result(
            r'''
            SELECT array_agg(Person.name ORDER BY Person.name);
            ''',
            [['Emmanuel Villip', 'Madeline Hatch', 'Phil Emarg']]
        )

    async def test_edgeql_lprop_01(self):
        await self.assert_query_result(
            r'''
            SELECT (Person.notes.name, Person.notes@metanote ?? '<n/a>')
            ''',
            {('boxing', '<n/a>'), ('unboxing', 'arg!'), ('unboxing', 'sigh')},
        )

    async def test_edgeql_lprop_02(self):
        await self.assert_query_result(
            r'''
            SELECT (User.name,
                    (SELECT (User.friends.name, User.friends@nickname)));
            ''',
            {
                ("Alice", ("Bob", "Swampy")),
                ("Alice", ("Carol", "Firefighter")),
                ("Alice", ("Dave", "Grumpy"))
            }
        )

    async def test_edgeql_lprop_03(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    User.name,
                    array_agg(
                        (SELECT (User.friends.name, User.friends@nickname))));
            ''',
            bag([
                (
                    'Alice',
                    [
                        ('Bob', 'Swampy'),
                        ('Carol', 'Firefighter'),
                        ('Dave', 'Grumpy')
                    ],
                ),
                ('Bob', []),
                ('Carol', []),
                ('Dave', []),
            ])
        )

    async def test_edgeql_lprop_04(self):
        await self.assert_query_result(
            r'''
                SELECT count(Card.<deck[IS User]@count);
            ''',
            [22]
        )

    async def test_edgeql_lprop_reverse_01(self):
        await self.assert_query_result(
            r'''
                SELECT count((
                    Card.name,
                    Card.<deck[IS User].name,
                    Card.<deck[IS User]@count,
                ));
            ''',
            [22]
        )

    async def test_edgeql_lprop_reverse_02(self):
        # This should (I claim), but does not yet, work for real.
        await self.assert_query_result(
            r'''
                SELECT Card {
                    name,
                    z := .<deck[IS User] { name, @count }
                } FILTER .name = 'Dragon'
            ''',
            bag([{"name": "Dragon", "z": [
                {"name": "Alice", "@count": 2},
                {"name": "Dave", "@count": 1},
            ]}])
        )

    async def test_edgeql_partial_path_01(self):
        await self.assert_query_result(
            r'''
            SELECT (SELECT User FILTER User.deck != .deck).name;
            ''',
            []
        )

    async def test_edgeql_partial_path_02(self):
        await self.assert_query_result(
            r'''
            SELECT count((SELECT X := User FILTER User.deck != .deck));
            ''',
            [4]
        )

    async def test_edgeql_partial_path_03(self):
        await self.assert_query_result(
            r'''
            SELECT count((SELECT X := User FILTER X.deck != .deck));
            ''',
            [0]
        )

    async def test_edgeql_shape_01(self):
        await self.assert_query_result(
            r"""
            SELECT User {
                name,
                deck: {
                    name, tag := <str>.cost ++ ' ' ++ .element, @count
               } ORDER BY .cost THEN .name
            } ORDER BY .name DESC;
            """,
            [{
                "deck":
                [{"@count": 4,
                  "name": "Sprite",
                  "tag": "1 Air"},
                 {"@count": 1,
                    "name": "Bog monster",
                    "tag": "2 Water", },
                    {"@count": 1,
                     "name": "Giant eagle",
                     "tag": "2 Air", },
                    {"@count": 1,
                     "name": "Giant turtle",
                     "tag": "3 Water", },
                    {"@count": 1,
                     "name": "Golem",
                     "tag": "3 Earth"},
                    {"@count": 1,
                     "name": "Djinn",
                     "tag": "4 Air"},
                    {"@count": 1,
                     "name": "Dragon",
                     "tag": "5 Fire"}, ],
                "name": "Dave", },
             {
                "deck":
                [{"@count": 4,
                  "name": "Dwarf",
                  "tag": "1 Earth"},
                 {"@count": 4,
                    "name": "Sprite",
                    "tag": "1 Air"},
                    {"@count": 3,
                     "name": "Bog monster",
                     "tag": "2 Water", },
                    {"@count": 3,
                     "name": "Giant eagle",
                     "tag": "2 Air", },
                    {"@count": 2,
                     "name": "Giant turtle",
                     "tag": "3 Water", },
                    {"@count": 2,
                     "name": "Golem",
                     "tag": "3 Earth"},
                    {"@count": 1,
                     "name": "Djinn",
                     "tag": "4 Air"}, ],
                "name": "Carol", },
             {
                "deck":
                [{"@count": 3,
                  "name": "Dwarf",
                  "tag": "1 Earth"},
                 {"@count": 3,
                    "name": "Bog monster",
                    "tag": "2 Water", },
                    {"@count": 3,
                     "name": "Giant turtle",
                     "tag": "3 Water", },
                    {"@count": 3,
                     "name": "Golem",
                     "tag": "3 Earth"}, ],
                "name": "Bob", },
             {
                "deck":
                [{"@count": 2,
                  "name": "Imp",
                  "tag": "1 Fire"},
                 {"@count": 3,
                    "name": "Bog monster",
                    "tag": "2 Water", },
                    {"@count": 3,
                     "name": "Giant turtle",
                     "tag": "3 Water", },
                    {"@count": 2,
                     "name": "Dragon",
                     "tag": "5 Fire"}, ],
                "name": "Alice", }, ],)

    async def test_edgeql_shape_for_01(self):
        # we have a lot of trouble with this one in the real compiler.
        await self.assert_query_result(
            r"""
            SELECT (FOR x IN {1,2} UNION (SELECT User { m := x })) { name, m }
            ORDER BY .name THEN .m;
            """,
            [
                {'m': 1, 'name': 'Alice'},
                {'m': 2, 'name': 'Alice'},
                {'m': 1, 'name': 'Bob'},
                {'m': 2, 'name': 'Bob'},
                {'m': 1, 'name': 'Carol'},
                {'m': 2, 'name': 'Carol'},
                {'m': 1, 'name': 'Dave'},
                {'m': 2, 'name': 'Dave'},
            ],
        )

    async def test_edgeql_detached_01(self):
        await self.assert_query_result(
            r"""
            SELECT count((User.deck.name, DETACHED User.name));
            """,
            [36],
        )

    async def test_edgeql_result_alias_binding_01(self):
        # Because of the result alias being a shorthand for a WITH binding,
        # the two User refs should be in different subqueries
        await self.assert_query_result(
            r"""
            SELECT count((SELECT _ := User {name} FILTER User.name = 'Alice'));
            """,
            [4],
        )

    async def test_model_singleton_cheat(self):
        await self.assert_query_result(
            r"""
            SELECT User { name } FILTER .name = 'Alice';
            """,
            [
                {'name': 'Alice'}
            ],
        )

# TODO : DEFER COMPUTED
    async def test_model_computed_01(self):
        await self.assert_query_result(
            r"""
            SELECT User { name, deck_cost } ORDER BY .name;
            """,
            [
                {"name": "Alice", "deck_cost": 11},
                {"name": "Bob", "deck_cost": 9},
                {"name": "Carol", "deck_cost": 16},
                {"name": "Dave", "deck_cost": 20},
            ]
        )

    # async def test_model_computed_02(self):
    #     await self.assert_query_result(
    #         r"""
    #         SELECT User { deck: {name, @total_cost} ORDER BY .name}
    #         FILTER .name = 'Alice';
    #         """,
    #         [{"deck": [
    #             {"name": "Bog monster", "@total_cost": 6},
    #             {"name": "Dragon", "@total_cost": 10},
    #             {"name": "Giant turtle", "@total_cost": 9},
    #             {"name": "Imp", "@total_cost": 2},
    #         ]}],
    #         singleton_cheating=True,
    #     )

    # async def test_model_alias_correlation_01(self):
    #     await self.assert_query_result(
    #         r"""
    #         SELECT (Note.name, EXISTS (WITH W := Note.note SELECT W))
    #         """,
    #         {('boxing', False), ('unboxing', True), ('dynamic', True)}
    #     )

    async def test_model_alias_shadowing_01(self):
        await self.assert_query_result(
            r"""
            SELECT (User.name, (WITH User := {1,2} SELECT User))
            """,
            bag([
                ["Alice", 1],
                ["Alice", 2],
                ["Bob", 1],
                ["Bob", 2],
                ["Carol", 1],
                ["Carol", 2],
                ["Dave", 1],
                ["Dave", 2],
            ])
        )

# TODO : DEFER COMPUTED
    # def test_model_alias_computable_correlate(self):
    #     await self.assert_query_result(
    #         r"""
    #         WITH X := (SELECT Obj {m := {1, 2}}) SELECT (X {n, m}, X.m);
    #         """,
    #         bag([
    #             [{"n": [1], "m": [1]}, 1],
    #             [{"n": [1], "m": [2]}, 2],
    #             [{"n": [2], "m": [1]}, 1],
    #             [{"n": [2], "m": [2]}, 2],
    #             [{"n": [3], "m": [1]}, 1],
    #             [{"n": [3], "m": [2]}, 2],
    #         ]),
    #     )
