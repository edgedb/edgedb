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
from edb.testbase import experimental_interpreter as tb

bag = assert_data_shape.bag


class TestNewInterpreterModelSmokeTests(tb.ExperimentalInterpreterTestCase):
    """Unit tests for the toy evaluator model.

    These are intended as smoke tests. Since obviously we don't want
    two totally parallel sets of tests for EdgeQL queries, an eventual
    goal should be that we could run the real tests against the model.
    """

    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas', 'smoke_test_interp.esdl'
    )

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas', 'smoke_test_interp_setup.edgeql'
    )

    INTERPRETER_USE_SQLITE = False

    def test_model_basic_01(self):
        self.assert_query_result(
            "SELECT 1",
            [1],
        )

    def test_model_basic_02(self):
        self.assert_query_result(
            r"""
            SELECT Person.name
            """,
            ['Phil Emarg', 'Madeline Hatch', 'Emmanuel Villip'],
        )

    def test_model_basic_03(self):
        self.assert_query_result(
            r"""
            SELECT (Person.name, Person.name)
            """,
            [
                ('Phil Emarg', 'Phil Emarg'),
                ('Madeline Hatch', 'Madeline Hatch'),
                ('Emmanuel Villip', 'Emmanuel Villip'),
            ],
        )

    def test_model_link_dedup(self):
        self.assert_query_result(
            r"""
            SELECT Person.notes.name
            """,
            {'boxing', 'unboxing'},
        )

    def test_model_link_dedup_02(self):
        self.assert_query_result(
            r"""
            SELECT count(User.friends)
            """,
            {3},
        )

    def test_model_link_dedup_03(self):
        self.assert_query_result(
            r"""
            SELECT count((User.friends, User.friends@nickname));
            """,
            {3},
        )

    def test_model_link_dedup_04(self):
        self.assert_query_result(
            r"""
            SELECT count((User.friends, User.friends@nickname ?? "None"));
            """,
            {4},
        )

    def test_model_link_dedup_05(self):
        self.assert_query_result(
            r"""
            SELECT count(((select User.friends), (select User.friends.name)));
            """,
            {9},
        )

    def test_model_link_dedup_06(self):
        self.assert_query_result(
            r"""
            SELECT count(((select User.friends),
                          (select User.friends@nickname)));
            """,
            {9},
        )

    def test_model_link_correlation(self):
        self.assert_query_result(
            r"""
            SELECT Person.name ++ "-" ++ Person.notes.name
            """,
            {
                'Phil Emarg-boxing',
                'Phil Emarg-unboxing',
                'Madeline Hatch-unboxing',
            },
        )

    def test_model_optional_prop_01(self):
        self.assert_query_result(
            r"""
            SELECT (Note.note ?= "lolol", Note.name)
            """,
            {(False, 'boxing'), (True, 'unboxing'), (False, 'dynamic')},
        )

    def test_model_optional_prop_02(self):
        self.assert_query_result(
            r"""
            SELECT (Note.note = "lolol", Note.name)
            """,
            {(False, 'dynamic'), (True, 'unboxing')},
        )

    def test_model_optional_prop_03(self):
        self.assert_query_result(
            r"""
            SELECT (Note.name, ('SOME "' ++ Note.note ++ '"') ?? 'NONE')
            """,
            {
                ('boxing', 'NONE'),
                ('unboxing', 'SOME "lolol"'),
                ('dynamic', 'SOME "blarg"'),
            },
        )

    def test_model_optional_prop_04(self):
        self.assert_query_result(
            r"""
            SELECT (Note.name, EXISTS Note.note)
            """,
            {('boxing', False), ('unboxing', True), ('dynamic', True)},
        )

    def test_model_optional_prop_05(self):
        self.assert_query_result(
            r"""
            SELECT (User.name ++ User.avatar.name) ?? 'hm';
            """,
            {'AliceDragon', 'DaveDjinn'},
        )

    def test_model_optional_prop_06(self):
        self.assert_query_result(
            r"""
            SELECT User.name ?= User.name;
            """,
            [True, True, True, True],
        )

    def test_model_optional_prop_07(self):
        self.assert_query_result(
            r"""
            SELECT (User.name ?= 'Alice', count(User.name))
            """,
            bag([(True, 1), (False, 1), (False, 1), (False, 1)]),
        )

    def test_model_subqueries_01(self):
        self.assert_query_result(
            r"""
            SELECT count(((SELECT Person), (SELECT Person)))
            """,
            [9],
        )

    def test_model_subqueries_02(self):
        self.assert_query_result(
            r"""
            WITH X := {true, false, true} SELECT (any(X), all(X))
            """,
            [(True, False)],
        )

    def test_model_subqueries_03(self):
        self.assert_query_result(
            r"""
            SELECT enumerate((SELECT X := {"foo", "bar", "baz"} ORDER BY X));
            """,
            [(0, 'bar'), (1, 'baz'), (2, 'foo')],
        )

    def test_model_set_union(self):
        self.assert_query_result(
            r"""
            SELECT count({User, Card})
            """,
            [13],
        )

    def test_edgeql_coalesce_set_of_01(self):
        self.assert_query_result(
            r'''

                SELECT <str>Publication.id ?? <str>count(Publication)
            ''',
            ['0'],
        )

    def test_edgeql_coalesce_set_of_02(self):
        self.assert_query_result(
            r'''
                SELECT (
                    Publication ?= Publication,
                    (Publication.title++Publication.title
                       ?= Publication.title) ?=
                    (Publication ?!= Publication)
                )
            ''',
            [(True, False)],
        )

    def test_edgeql_select_clauses_01(self):
        self.assert_query_result(
            r'''
            SELECT (Person.name, Person.notes.name)
            FILTER Person.name != "Madeline Hatch"
            ORDER BY .1 DESC;
            ''',
            [('Phil Emarg', 'unboxing'), ('Phil Emarg', 'boxing')],
        )

    def test_edgeql_select_clauses_02(self):
        # This is a funky one.
        self.assert_query_result(
            r'''
            SELECT (
                Person.name,
                (SELECT Person.notes.name
                 ORDER BY Person.notes.name DESC
                 LIMIT (0 if Person.name = "Madeline Hatch" ELSE 1)));
            ''',
            [('Phil Emarg', 'unboxing')],
        )

    def test_edgeql_select_clauses_03(self):
        self.assert_query_result(
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

    def test_edgeql_for_01(self):
        self.assert_query_result(
            r'''
            FOR X IN {1,2,3} UNION ((SELECT X), (SELECT X));
            ''',
            {(1, 1), (2, 2), (3, 3)},
        )

    def test_edgeql_for_02(self):
        self.assert_query_result(
            r'''
            WITH X := 1, FOR x in {X} UNION (x);
            ''',
            [1],
        )

    def test_edgeql_with_01(self):
        self.assert_query_result(
            r'''
            WITH X := {1, 2} SELECT ((SELECT X), (SELECT X));
            ''',
            {(1, 1), (1, 2), (2, 1), (2, 2)},
        )

    def test_edgeql_with_02(self):
        # For a while, the model produced the right answer here while
        # the real compiler didn't!
        # See https://github.com/edgedb/edgedb/issues/1381
        self.assert_query_result(
            r'''
            WITH X := {1, 2}, Y := X+1 SELECT (X, Y);
            ''',
            {(1, 2), (1, 3), (2, 2), (2, 3)},
        )

    def test_edgeql_array_01(self):
        self.assert_query_result(
            r'''
            WITH X := [0,1,2,3,4,5,6,7,8,9] SELECT X[{1,2}:{5,6}];
            ''',
            bag([[1, 2, 3, 4], [1, 2, 3, 4, 5], [2, 3, 4], [2, 3, 4, 5]]),
        )

    def test_edgeql_array_02(self):
        self.assert_query_result(
            r'''
            SELECT array_unpack({[1,2,3],[3,4,5]});
            ''',
            [1, 2, 3, 3, 4, 5],
        )

    def test_edgeql_array_03(self):
        self.assert_query_result(
            r'''
            SELECT array_agg(Person.name ORDER BY Person.name);
            ''',
            [['Emmanuel Villip', 'Madeline Hatch', 'Phil Emarg']],
        )

    def test_edgeql_lprop_01(self):
        self.assert_query_result(
            r'''
            SELECT (Person.notes.name, Person.notes@metanote ?? '<n/a>')
            ''',
            {('boxing', '<n/a>'), ('unboxing', 'arg!'), ('unboxing', 'sigh')},
        )

    def test_edgeql_lprop_02(self):
        self.assert_query_result(
            r'''
            SELECT (User.name,
                    (SELECT (User.friends.name, User.friends@nickname)));
            ''',
            {
                ("Alice", ("Bob", "Swampy")),
                ("Alice", ("Carol", "Firefighter")),
                ("Alice", ("Dave", "Grumpy")),
            },
        )

    def test_edgeql_lprop_03(self):
        self.assert_query_result(
            r'''
                SELECT (
                    User.name,
                    array_agg(
                        (SELECT (User.friends.name, User.friends@nickname))));
            ''',
            bag(
                [
                    (
                        'Alice',
                        [
                            ('Bob', 'Swampy'),
                            ('Carol', 'Firefighter'),
                            ('Dave', 'Grumpy'),
                        ],
                    ),
                    ('Bob', []),
                    ('Carol', []),
                    ('Dave', []),
                ]
            ),
        )

    def test_edgeql_lprop_04(self):
        self.assert_query_result(
            r'''
                SELECT count(Card.<deck[IS User]@count);
            ''',
            [22],
        )

    def test_edgeql_lprop_reverse_01(self):
        self.assert_query_result(
            r'''
                SELECT count((
                    Card.name,
                    Card.<deck[IS User].name,
                    Card.<deck[IS User]@count,
                ));
            ''',
            [22],
        )

    def test_edgeql_lprop_reverse_02(self):
        # This should (I claim), but does not yet, work for real.
        self.assert_query_result(
            r'''
                SELECT Card {
                    name,
                    z := .<deck[IS User] { name, @count }
                } FILTER .name = 'Dragon'
            ''',
            bag(
                [
                    {
                        "name": "Dragon",
                        "z": [
                            {"name": "Alice", "@count": 2},
                            {"name": "Dave", "@count": 1},
                        ],
                    }
                ]
            ),
        )

    def test_edgeql_partial_path_01(self):
        self.assert_query_result(
            r'''
            SELECT (SELECT User FILTER User.deck != .deck).name;
            ''',
            [],
        )

    def test_edgeql_partial_path_02(self):
        self.assert_query_result(
            r'''
            SELECT count((SELECT X := User FILTER User.deck != .deck));
            ''',
            [4],
        )

    def test_edgeql_partial_path_03(self):
        self.assert_query_result(
            r'''
            SELECT count((SELECT X := User FILTER X.deck != .deck));
            ''',
            [0],
        )

    def test_edgeql_shape_01(self):
        self.assert_query_result(
            r"""
            SELECT User {
                name,
                deck: {
                    name, tag := <str>.cost ++ ' ' ++ .element, @count
               } ORDER BY .cost THEN .name
            } ORDER BY .name DESC;
            """,
            [
                {
                    "deck": [
                        {"@count": 4, "name": "Sprite", "tag": "1 Air"},
                        {
                            "@count": 1,
                            "name": "Bog monster",
                            "tag": "2 Water",
                        },
                        {
                            "@count": 1,
                            "name": "Giant eagle",
                            "tag": "2 Air",
                        },
                        {
                            "@count": 1,
                            "name": "Giant turtle",
                            "tag": "3 Water",
                        },
                        {"@count": 1, "name": "Golem", "tag": "3 Earth"},
                        {"@count": 1, "name": "Djinn", "tag": "4 Air"},
                        {"@count": 1, "name": "Dragon", "tag": "5 Fire"},
                    ],
                    "name": "Dave",
                },
                {
                    "deck": [
                        {"@count": 4, "name": "Dwarf", "tag": "1 Earth"},
                        {"@count": 4, "name": "Sprite", "tag": "1 Air"},
                        {
                            "@count": 3,
                            "name": "Bog monster",
                            "tag": "2 Water",
                        },
                        {
                            "@count": 3,
                            "name": "Giant eagle",
                            "tag": "2 Air",
                        },
                        {
                            "@count": 2,
                            "name": "Giant turtle",
                            "tag": "3 Water",
                        },
                        {"@count": 2, "name": "Golem", "tag": "3 Earth"},
                        {"@count": 1, "name": "Djinn", "tag": "4 Air"},
                    ],
                    "name": "Carol",
                },
                {
                    "deck": [
                        {"@count": 3, "name": "Dwarf", "tag": "1 Earth"},
                        {
                            "@count": 3,
                            "name": "Bog monster",
                            "tag": "2 Water",
                        },
                        {
                            "@count": 3,
                            "name": "Giant turtle",
                            "tag": "3 Water",
                        },
                        {"@count": 3, "name": "Golem", "tag": "3 Earth"},
                    ],
                    "name": "Bob",
                },
                {
                    "deck": [
                        {"@count": 2, "name": "Imp", "tag": "1 Fire"},
                        {
                            "@count": 3,
                            "name": "Bog monster",
                            "tag": "2 Water",
                        },
                        {
                            "@count": 3,
                            "name": "Giant turtle",
                            "tag": "3 Water",
                        },
                        {"@count": 2, "name": "Dragon", "tag": "5 Fire"},
                    ],
                    "name": "Alice",
                },
            ],
        )

    def test_edgeql_shape_for_01(self):
        # we have a lot of trouble with this one in the real compiler.
        self.assert_query_result(
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

    def test_edgeql_detached_01(self):
        self.assert_query_result(
            r"""
            SELECT count((User.deck.name, DETACHED User.name));
            """,
            [36],
        )

    def test_edgeql_result_alias_binding_01(self):
        # Because of the result alias being a shorthand for a WITH binding,
        # the two User refs should be in different subqueries
        self.assert_query_result(
            r"""
            SELECT count((SELECT _ := User {name} FILTER User.name = 'Alice'));
            """,
            [4],
        )

    def test_model_singleton_cheat(self):
        self.assert_query_result(
            r"""
            SELECT User { name } FILTER .name = 'Alice';
            """,
            [{'name': 'Alice'}],
        )

    # TODO : DEFER COMPUTED
    def test_model_computed_01(self):
        self.assert_query_result(
            r"""
            SELECT User { name, deck_cost } ORDER BY .name;
            """,
            [
                {"name": "Alice", "deck_cost": 11},
                {"name": "Bob", "deck_cost": 9},
                {"name": "Carol", "deck_cost": 16},
                {"name": "Dave", "deck_cost": 20},
            ],
        )

    # def test_model_computed_02(self):
    #     self.assert_query_result(
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
    #     )

    # def test_model_alias_correlation_01(self):
    #     self.assert_query_result(
    #         r"""
    #         SELECT (Note.name, EXISTS (WITH W := Note.note SELECT W))
    #         """,
    #         {('boxing', False), ('unboxing', True), ('dynamic', True)}
    #     )

    def test_model_alias_shadowing_01(self):
        self.assert_query_result(
            r"""
            SELECT (User.name, (WITH User := {1,2} SELECT User))
            """,
            bag(
                [
                    ["Alice", 1],
                    ["Alice", 2],
                    ["Bob", 1],
                    ["Bob", 2],
                    ["Carol", 1],
                    ["Carol", 2],
                    ["Dave", 1],
                    ["Dave", 2],
                ]
            ),
        )

    def test_model_delete_01(self):
        self.assert_query_result(
            r"""
            DELETE User FILTER .name = 'Alice';
            """,
            [{}],
        )


# TODO : DEFER COMPUTED
# def test_model_alias_computable_correlate(self):
#     self.assert_query_result(
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


class TestNewInterpreterModelSmokeTestsSQLite(
    TestNewInterpreterModelSmokeTests
):
    INTERPRETER_USE_SQLITE = True
