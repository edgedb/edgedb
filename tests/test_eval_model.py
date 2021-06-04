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


import unittest

from edb.tools import toy_eval_model as model


class TestModelSmokeTests(unittest.TestCase):
    """Unit tests for the toy evaluator model.

    These are intended as smoke tests. Since obviously we don't want
    two totally parallel sets of tests for EdgeQL queries, an eventual
    goal should be that we could run the real tests against the model.
    """

    DB1 = model.DB1

    def assert_test_query(self, query, expected, *, db=DB1, sort=True):
        qltree = model.parse(query)
        result = model.go(qltree, db)
        if sort:
            result.sort()
            expected.sort()

        self.assertEqual(expected, result)

    def test_model_basic_01(self):
        self.assert_test_query(
            "SELECT 1",
            [1],
        )

    def test_model_basic_02(self):
        self.assert_test_query(
            r"""
            SELECT Person.name
            """,
            ['Phil Emarg', 'Madeline Hatch', 'Emmanuel Villip'],
        )

    def test_model_basic_03(self):
        self.assert_test_query(
            r"""
            SELECT (Person.name, Person.name)
            """,
            [('Phil Emarg', 'Phil Emarg'),
             ('Madeline Hatch', 'Madeline Hatch'),
             ('Emmanuel Villip', 'Emmanuel Villip')]
        )

    def test_model_link_dedup(self):
        self.assert_test_query(
            r"""
            SELECT Person.notes.name
            """,
            ['boxing', 'unboxing'],
        )

    def test_model_link_correlation(self):
        self.assert_test_query(
            r"""
            SELECT Person.name ++ "-" ++ Person.notes.name
            """,
            ['Phil Emarg-boxing', 'Phil Emarg-unboxing',
             'Madeline Hatch-unboxing']
        )

    def test_model_optional_prop_01(self):
        self.assert_test_query(
            r"""
            SELECT (Note.note ?= "lolol", Note.name)
            """,
            [(False, 'boxing'), (True, 'unboxing'), (False, 'dynamic')]
        )

    def test_model_optional_prop_02(self):
        self.assert_test_query(
            r"""
            SELECT (Note.note = "lolol", Note.name)
            """,
            [(False, 'dynamic'), (True, 'unboxing')]
        )

    def test_model_optional_prop_03(self):
        self.assert_test_query(
            r"""
            SELECT (Note.name, ('SOME "' ++ Note.note ++ '"') ?? 'NONE')
            """,
            [('boxing', 'NONE'), ('unboxing', 'SOME "lolol"'),
             ('dynamic', 'SOME "blarg"')]
        )

    def test_model_optional_prop_04(self):
        self.assert_test_query(
            r"""
            SELECT (Note.name, EXISTS Note.note)
            """,
            [('boxing', False), ('unboxing', True), ('dynamic', True)]
        )

    def test_model_subqueries_01(self):
        self.assert_test_query(
            r"""
            SELECT count(((SELECT Person), (SELECT Person)))
            """,
            [9]
        )

    def test_model_subqueries_02(self):
        self.assert_test_query(
            r"""
            WITH X := {true, false, true} SELECT (any(X), all(X))
            """,
            [(True, False)]
        )

    def test_model_subqueries_03(self):
        self.assert_test_query(
            r"""
            SELECT enumerate((SELECT X := {"foo", "bar", "baz"} ORDER BY X));
            """,
            [(0, 'bar'), (1, 'baz'), (2, 'foo')],
            sort=False,
        )

    def test_edgeql_coalesce_set_of_01(self):
        self.assert_test_query(
            r'''

                SELECT <str>Publication.id ?? <str>count(Publication)
            ''',
            ['0'],
        )

    def test_edgeql_coalesce_set_of_02(self):
        self.assert_test_query(
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

    def test_edgeql_select_clauses_01(self):
        self.assert_test_query(
            r'''
            SELECT (Person.name, Person.notes.name)
            FILTER Person.name != "Madeline Hatch"
            ORDER BY .1 DESC;
            ''',
            [('Phil Emarg', 'unboxing'), ('Phil Emarg', 'boxing')],
            sort=False,
        )

    def test_edgeql_select_clauses_02(self):
        # This is a funky one.
        self.assert_test_query(
            r'''
            SELECT (
                Person.name,
                (SELECT Person.notes.name
                 ORDER BY Person.notes.name DESC
                 LIMIT (0 if Person.name = "Madeline Hatch" ELSE 1)));
            ''',
            [('Phil Emarg', 'unboxing')]
        )

    def test_edgeql_select_clauses_03(self):
        self.assert_test_query(
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
        self.assert_test_query(
            r'''
            FOR X IN {1,2,3} UNION ((SELECT X), (SELECT X));
            ''',
            [(1, 1), (2, 2), (3, 3)],
        )

    def test_edgeql_with_01(self):
        self.assert_test_query(
            r'''
            WITH X := {1, 2} SELECT ((SELECT X), (SELECT X));
            ''',
            [(1, 1), (1, 2), (2, 1), (2, 2)]
        )

    def test_edgeql_with_02(self):
        # For a while, the model produced the right answer here while
        # the real compiler didn't!
        # See https://github.com/edgedb/edgedb/issues/1381
        self.assert_test_query(
            r'''
            WITH X := {1, 2}, Y := X+1 SELECT (X, Y);
            ''',
            [(1, 2), (1, 3), (2, 2), (2, 3)]
        )

    def test_edgeql_array_01(self):
        self.assert_test_query(
            r'''
            WITH X := [0,1,2,3,4,5,6,7,8,9] SELECT X[{1,2}:{5,6}];
            ''',
            [[1, 2, 3, 4], [1, 2, 3, 4, 5], [2, 3, 4], [2, 3, 4, 5]],
        )

    def test_edgeql_array_02(self):
        self.assert_test_query(
            r'''
            SELECT array_unpack({[1,2,3],[3,4,5]});
            ''',
            [1, 2, 3, 3, 4, 5]
        )

    def test_edgeql_array_03(self):
        self.assert_test_query(
            r'''
            SELECT array_agg(Person.name ORDER BY Person.name);
            ''',
            [['Emmanuel Villip', 'Madeline Hatch', 'Phil Emarg']]
        )

    def test_edgeql_lprop_01(self):
        self.assert_test_query(
            r'''
            SELECT (Person.notes.name, Person.notes@metanote ?? '<n/a>')
            ''',
            [('boxing', '<n/a>'), ('unboxing', 'arg!'), ('unboxing', 'sigh')],
        )

    def test_edgeql_lprop_02(self):
        self.assert_test_query(
            r'''
            SELECT (User.name,
                    (SELECT (User.friends.name, User.friends@nickname)));
            ''',
            [
                ("Alice", ("Bob", "Swampy")),
                ("Alice", ("Carol", "Firefighter")),
                ("Alice", ("Dave", "Grumpy"))
            ]
        )

    def test_edgeql_lprop_03(self):
        self.assert_test_query(
            r'''
                SELECT (
                    User.name,
                    array_agg(
                        (SELECT (User.friends.name, User.friends@nickname))));
            ''',
            [
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
                ('Dave', [])]
        )

    def test_edgeql_partial_path_01(self):
        self.assert_test_query(
            r'''
            SELECT (SELECT User FILTER User.deck != .deck).name;
            ''',
            []
        )

    def test_edgeql_partial_path_02(self):
        self.assert_test_query(
            r'''
            SELECT count((SELECT X := User FILTER User.deck != .deck));
            ''',
            [4]
        )

    def test_edgeql_partial_path_03(self):
        self.assert_test_query(
            r'''
            SELECT count((SELECT X := User FILTER X.deck != .deck));
            ''',
            [0]
        )

    def test_edgeql_shape_01(self):
        self.assert_test_query(
            r"""
            SELECT User {
                name,
                deck: {
                    name, tag := <str>.cost ++ ' ' ++ .element, @count
               } ORDER BY .cost
            } ORDER BY .name DESC;
            """,
            [
                {
                    "deck": [
                        {"@count": [4], "name": ["Sprite"], "tag": ["1 Air"]},
                        {
                            "@count": [1],
                            "name": ["Bog monster"],
                            "tag": ["2 Water"],
                        },
                        {
                            "@count": [1],
                            "name": ["Giant eagle"],
                            "tag": ["2 Air"],
                        },
                        {
                            "@count": [1],
                            "name": ["Giant turtle"],
                            "tag": ["3 Water"],
                        },
                        {"@count": [1], "name": ["Golem"], "tag": ["3 Earth"]},
                        {"@count": [1], "name": ["Djinn"], "tag": ["4 Air"]},
                        {"@count": [1], "name": ["Dragon"], "tag": ["5 Fire"]},
                    ],
                    "name": ["Dave"],
                },
                {
                    "deck": [
                        {"@count": [4], "name": ["Dwarf"], "tag": ["1 Earth"]},
                        {"@count": [4], "name": ["Sprite"], "tag": ["1 Air"]},
                        {
                            "@count": [3],
                            "name": ["Bog monster"],
                            "tag": ["2 Water"],
                        },
                        {
                            "@count": [3],
                            "name": ["Giant eagle"],
                            "tag": ["2 Air"],
                        },
                        {
                            "@count": [2],
                            "name": ["Giant turtle"],
                            "tag": ["3 Water"],
                        },
                        {"@count": [2], "name": ["Golem"], "tag": ["3 Earth"]},
                        {"@count": [1], "name": ["Djinn"], "tag": ["4 Air"]},
                    ],
                    "name": ["Carol"],
                },
                {
                    "deck": [
                        {"@count": [3], "name": ["Dwarf"], "tag": ["1 Earth"]},
                        {
                            "@count": [3],
                            "name": ["Bog monster"],
                            "tag": ["2 Water"],
                        },
                        {
                            "@count": [3],
                            "name": ["Giant turtle"],
                            "tag": ["3 Water"],
                        },
                        {"@count": [3], "name": ["Golem"], "tag": ["3 Earth"]},
                    ],
                    "name": ["Bob"],
                },
                {
                    "deck": [
                        {"@count": [2], "name": ["Imp"], "tag": ["1 Fire"]},
                        {
                            "@count": [3],
                            "name": ["Bog monster"],
                            "tag": ["2 Water"],
                        },
                        {
                            "@count": [3],
                            "name": ["Giant turtle"],
                            "tag": ["3 Water"],
                        },
                        {"@count": [2], "name": ["Dragon"], "tag": ["5 Fire"]},
                    ],
                    "name": ["Alice"],
                },
            ],
            sort=False,
        )

    def test_edgeql_shape_for_01(self):
        # we have a lot of trouble with this one in the real compiler.
        self.assert_test_query(
            r"""
            SELECT (FOR x IN {1,2} UNION (SELECT User { m := x })) { name, m }
            ORDER BY .name THEN .m;
            """,
            [
                {'m': [1], 'name': ['Alice']},
                {'m': [2], 'name': ['Alice']},
                {'m': [1], 'name': ['Bob']},
                {'m': [2], 'name': ['Bob']},
                {'m': [1], 'name': ['Carol']},
                {'m': [2], 'name': ['Carol']},
                {'m': [1], 'name': ['Dave']},
                {'m': [2], 'name': ['Dave']},
            ],
            sort=False,
        )
