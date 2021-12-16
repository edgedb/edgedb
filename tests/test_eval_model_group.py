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


class TestModelGroupests(unittest.TestCase):
    """Tests for GROUP BY in the toy evaluator model."""

    DB1 = model.DB1

    def assert_test_query(
        self, query, expected, *, db=DB1, sort=False, singleton_cheating=True
    ):
        qltree = model.parse(query)
        result = model.go(qltree, db, singleton_cheating)
        if sort:
            result.sort()
            expected.sort()

        self.assertEqual(expected, result)

    def test_model_group_01(self):
        res = [
            {
                "el": "Air",
                "cards": [
                    {"name": "Djinn"},
                    {"name": "Giant eagle"},
                    {"name": "Sprite"},
                ],
            },
            {
                "el": "Earth",
                "cards": [{"name": "Dwarf"}, {"name": "Golem"}],
            },
            {"el": "Fire", "cards": [{"name": "Dragon"}, {"name": "Imp"}]},
            {
                "el": "Water",
                "cards": [
                    {"name": "Bog monster"},
                    {"name": "Giant turtle"},
                ],
            },
        ]

        # Two implementations of the same query
        self.assert_test_query(
            """
            SELECT (
            FOR g in (GROUP Card {name} BY .element)
            UNION g {
                el := .key.element,
                cards := (SELECT .elements {name} ORDER BY .name)
            }) ORDER BY .el
            """,
            res,
        )

        self.assert_test_query(
            """
            SELECT (GROUP Card {name} BY .element) {
                el := .key.element,
                cards := (SELECT .elements {name} ORDER BY .name)
            } ORDER BY .el
            """,
            res,
        )

    def test_model_group_02(self):
        res = [
            {"el": "Air", "avg_cost": 2.3333333333333335},
            {"el": "Earth", "avg_cost": 2.0},
            {"el": "Fire", "avg_cost": 3.0},
            {"el": "Water", "avg_cost": 2.5},
        ]

        # Two implementations of the same query
        self.assert_test_query(
            """
            SELECT (
                FOR g in (GROUP Card {name} BY .element) UNION g {
                    el := .key.element,
                    avg_cost := sum(.elements.cost) / count(.elements)
                }
            ) ORDER BY .el
            """,
            res,
        )

        self.assert_test_query(
            """
            SELECT (GROUP Card {name} BY .element) {
                el := .key.element,
                avg_cost := sum(.elements.cost) / count(.elements)
            } ORDER BY .el
            """,
            res,
        )

    def test_model_group_03(self):
        res = [
            {"name": "Imp", "cost_ratio": 0.3333333333333333},
            {"name": "Dragon", "cost_ratio": 1.6666666666666667},
            {"name": "Bog monster", "cost_ratio": 0.8},
            {"name": "Giant turtle", "cost_ratio": 1.2},
            {"name": "Dwarf", "cost_ratio": 0.5},
            {"name": "Golem", "cost_ratio": 1.5},
            {"name": "Sprite", "cost_ratio": 0.42857142857142855},
            {"name": "Giant eagle", "cost_ratio": 0.8571428571428571},
            {"name": "Djinn", "cost_ratio": 1.7142857142857142},
        ]

        self.assert_test_query(
            """
            SELECT (
              FOR g in (GROUP Card {name} BY .element) UNION (
                WITH U := g.elements,
                SELECT U {
                    name,
                    cost_ratio := .cost / (math::mean(g.elements.cost))
                })
            ) ORDER BY name;
            """,
            res,
        )

    def test_model_group_04(self):
        res = [
            {"key": {"element": [], "nowners": []}, "num": 9, "grouping": []},
            {
                "key": {"element": "Air", "nowners": []},
                "num": 3,
                "grouping": ["element"],
            },
            {
                "key": {"element": "Earth", "nowners": []},
                "num": 2,
                "grouping": ["element"],
            },
            {
                "key": {"element": "Fire", "nowners": []},
                "num": 2,
                "grouping": ["element"],
            },
            {
                "key": {"element": "Water", "nowners": []},
                "num": 2,
                "grouping": ["element"],
            },
            {
                "key": {"element": "Air", "nowners": 2},
                "num": 3,
                "grouping": ["element", "nowners"],
            },
            {
                "key": {"element": "Earth", "nowners": 2},
                "num": 1,
                "grouping": ["element", "nowners"],
            },
            {
                "key": {"element": "Earth", "nowners": 3},
                "num": 1,
                "grouping": ["element", "nowners"],
            },
            {
                "key": {"element": "Fire", "nowners": 1},
                "num": 1,
                "grouping": ["element", "nowners"],
            },
            {
                "key": {"element": "Fire", "nowners": 2},
                "num": 1,
                "grouping": ["element", "nowners"],
            },
            {
                "key": {"element": "Water", "nowners": 4},
                "num": 2,
                "grouping": ["element", "nowners"],
            },
            {
                "key": {"element": [], "nowners": 1},
                "num": 1,
                "grouping": ["nowners"],
            },
            {
                "key": {"element": [], "nowners": 2},
                "num": 5,
                "grouping": ["nowners"],
            },
            {
                "key": {"element": [], "nowners": 3},
                "num": 1,
                "grouping": ["nowners"],
            },
            {
                "key": {"element": [], "nowners": 4},
                "num": 2,
                "grouping": ["nowners"],
            },
        ]

        self.assert_test_query(
            """
            SELECT (
              GROUP Card BY .element, nowners := count(.owners)
              USING CUBE (element, nowners)
            ) {
                key: {element, nowners},
                num := count(.elements),
                grouping
            } ORDER BY .grouping THEN .key.element THEN .key.nowners;
            """,
            res,
        )
