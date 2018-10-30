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
import unittest

from edb.client import exceptions as exc
from edb.server import _testbase as tb


class TestExpressions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_expr_emptyset_01(self):
        await self.assert_query_result(r"""
            SELECT <int64>{};
            SELECT <str>{};
            SELECT {} + 1;
            SELECT 1 + {};
        """, [
            [],
            [],
            [],
            [],
        ])

        with self.assertRaisesRegex(exc.EdgeQLError,
                                    r'could not determine expression type'):

            await self.con.execute("""
                SELECT {};
            """)

    async def test_edgeql_expr_emptyset_02(self):
        await self.assert_query_result(r"""
            SELECT count(<int64>{});
            SELECT count(DISTINCT <int64>{});
        """, [
            [0],
            [0],
        ])

        with self.assertRaisesRegex(exc.EdgeQLError,
                                    r'could not determine expression type'):

            await self.con.execute("""
                SELECT count({});
            """)

    async def test_edgeql_expr_idempotent_01(self):
        await self.assert_query_result(r"""
            SELECT (SELECT (SELECT (SELECT 42)));
        """, [
            [42],
        ])

    async def test_edgeql_expr_idempotent_02(self):
        await self.assert_query_result(r"""
            SELECT 'f';
            SELECT 'f'[0];
            SELECT 'foo'[0];
            SELECT 'f'[0][0][0][0][0];
            SELECT 'foo'[0][0][0][0][0];
        """, [
            ['f'],
            ['f'],
            ['f'],
            ['f'],
            ['f'],
        ])

    async def test_edgeql_expr_op_01(self):
        await self.assert_query_result(r"""
            SELECT 40 + 2;
            SELECT 40 - 2;
            SELECT 40 * 2;
            SELECT 40 / 2;
            SELECT 40 % 2;
        """, [
            [42],
            [38],
            [80],
            [20],
            [0],
        ])

    async def test_edgeql_expr_literals_01(self):
        await self.assert_query_result(r"""
            SELECT (1).__type__.name;
            SELECT (1.0).__type__.name;
            SELECT (9223372036854775807).__type__.name;
            SELECT (-9223372036854775808).__type__.name;
            SELECT (9223372036854775808).__type__.name;
            SELECT (-9223372036854775809).__type__.name;
        """, [
            {'std::int64'},
            {'std::float64'},
            {'std::int64'},
            {'std::int64'},
            {'std::decimal'},
            {'std::decimal'},
        ])

    async def test_edgeql_expr_op_02(self):
        await self.assert_query_result(r"""
            SELECT 40 ^ 2;
            SELECT 121 ^ 0.5;
            SELECT 2 ^ 3 ^ 2;
        """, [
            [1600],
            [11],
            [2 ** 3 ** 2],
        ])

    async def test_edgeql_expr_op_03(self):
        await self.assert_query_result(r"""
            SELECT 40 < 2;
            SELECT 40 > 2;
            SELECT 40 <= 2;
            SELECT 40 >= 2;
            SELECT 40 = 2;
            SELECT 40 != 2;
        """, [
            [False],
            [True],
            [False],
            [True],
            [False],
            [True],
        ])

    async def test_edgeql_expr_op_04(self):
        await self.assert_query_result(r"""
            SELECT -1 + 2 * 3 - 5 - 6.0 / 2;
            SELECT
                -1 + 2 * 3 - 5 - 6.0 / 2 > 0
                OR 25 % 4 = 3 AND 42 IN {12, 42, 14};
            SELECT (-1 + 2) * 3 - (5 - 6.0) / 2;
            SELECT
                ((-1 + 2) * 3 - (5 - 6.0) / 2 > 0 OR 25 % 4 = 3)
                AND 42 IN {12, 42, 14};
            SELECT 1 * 0.2;
            SELECT 0.2 * 1;
            SELECT -0.2 * 1;
            SELECT 0.2 + 1;
            SELECT 1 + 0.2;
            SELECT -0.2 - 1;
            SELECT -1 - 0.2;
            SELECT -1 / 0.2;
            SELECT 0.2 / -1;
            SELECT 5 // 2;
            SELECT 5.5 // 1.2;
            SELECT (5.5 // 1.2).__type__.name;
            SELECT -9.6 // 2;
            SELECT (<float32>-9.6 // 2).__type__.name;
        """, [
            [-3],
            [False],
            [3.5],
            [True],
            [0.2],
            [0.2],
            [-0.2],
            [1.2],
            [1.2],
            [-1.2],
            [-1.2],
            [-5],
            [-0.2],
            [2],
            [4.0],
            ['std::float64'],
            [-5.0],
            ['std::float64'],
        ])

    async def test_edgeql_expr_op_05(self):
        await self.assert_query_result(r"""
            SELECT 'foo' + 'bar';
        """, [
            ['foobar'],
        ])

    async def test_edgeql_expr_op_06(self):
        await self.assert_query_result(r"""
            SELECT {} = <int64>{};
            SELECT {} = 42;
            SELECT {} = '{}';
        """, [
            [],
            [],
            [],
        ])

    async def test_edgeql_expr_op_07(self):
        # Test boolean interaction with {}
        await self.assert_query_result(r"""
            SELECT TRUE OR {};
            SELECT FALSE AND {};
        """, [
            [],
            [],
        ])

    async def test_edgeql_expr_op_08(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'unary operator `-` is not defined .* std::str'):

            await self.con.execute("""
                SELECT -'aaa';
            """)

    async def test_edgeql_expr_op_09(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'unary operator `NOT` is not defined .* std::str'):

            await self.con.execute("""
                SELECT NOT 'aaa';
            """)

    async def test_edgeql_expr_op_10(self):
        await self.assert_query_result(r"""
            # the types are put in to satisfy type infering
            SELECT +<int64>{};
            SELECT -<int64>{};
            SELECT NOT <bool>{};
        """, [
            [],
            [],
            [],
        ])

    async def test_edgeql_expr_op_11(self):
        # Test non-trivial folding
        await self.assert_query_result(r"""
            SELECT 1 + (1 + len([1, 2])) + 1;
            SELECT 2 * (2 * len([1, 2])) * 2;
        """, [
            [5],
            [16],
        ])

    async def test_edgeql_expr_op_12(self):
        # Test power precedence
        await self.assert_query_result(r"""
            SELECT -2^2;
        """, [
            [-4],
        ])

    async def test_edgeql_expr_op_13(self):
        # test equivalence comparison
        await self.assert_query_result(r"""
            SELECT 2 ?= 2;
            SELECT 2 ?= 3;
            SELECT 2 ?!= 2;
            SELECT 2 ?!= 3;

            SELECT 2 ?= {};
            SELECT <int64>{} ?= <int64>{};
            SELECT 2 ?!= {};
            SELECT <int64>{} ?!= <int64>{};
        """, [
            [True],
            [False],
            [False],
            [True],

            [False],
            [True],
            [True],
            [False],
        ])

    async def test_edgeql_expr_op_14(self):
        await self.assert_query_result(r"""
            SELECT _ := {9, 1, 13}
            FILTER _ IN {11, 12, 13};

            SELECT _ := {9, 1, 13, 11}
            FILTER _ IN {11, 12, 13};
        """, [
            {13},
            {11, 13},
        ])

    async def test_edgeql_expr_op_15(self):
        await self.assert_query_result(r"""
            SELECT _ := {9, 12, 13}
            FILTER _ NOT IN {11, 12, 13};

            SELECT _ := {9, 1, 13, 11}
            FILTER _ NOT IN {11, 12, 13};
        """, [
            {9},
            {1, 9},
        ])

    async def test_edgeql_expr_op_16(self):
        await self.assert_query_result(r"""
            WITH a := {11, 12, 13}
            SELECT _ := {9, 1, 13}
            FILTER _ IN a;

            WITH MODULE schema
            SELECT _ := {9, 1, 13}
            FILTER _ IN (
                # Lengths of names for schema::Map, Type, and Array are
                # 11, 12, and 13, respectively.
                SELECT len(ObjectType.name)
                FILTER ObjectType.name LIKE 'schema::%'
            );
        """, [
            {13},
            {13},
        ])

    async def test_edgeql_expr_op_17(self):
        await self.assert_query_result(r"""
            WITH a := {11, 12, 13}
            SELECT _ := {9, 1, 13}
            FILTER _ NOT IN a;

            WITH MODULE schema
            SELECT _ := {9, 1, 13}
            FILTER _ NOT IN (
                # Lengths of names for schema::Map, Type, and Array are
                # 11, 12, and 13, respectively.
                SELECT len(ObjectType.name)
                FILTER ObjectType.name LIKE 'schema::%'
            );

        """, [
            {9, 1},
            {9, 1},
        ])

    async def test_edgeql_expr_op_18(self):
        await self.assert_query_result(r"""
            SELECT _ := {1, 2, 3} IN {3, 4}
            ORDER BY _;
        """, [
            [False, False, True],
        ])

    async def test_edgeql_expr_op_19(self):
        await self.assert_query_result(r"""
            SELECT 1 IN {};
            SELECT {1, 2, 3} IN {};

            SELECT 1 NOT IN {};
            SELECT {1, 2, 3} NOT IN {};
        """, [
            [False],
            [False, False, False],
            [True],
            [True, True, True],
        ])

    async def test_edgeql_expr_paths_01(self):
        cases = [
            "Issue.owner.name",
            "`Issue`.`owner`.`name`",
        ]

        for case in cases:
            await self.con.execute('''
                WITH MODULE test
                SELECT
                    Issue {
                        number
                    }
                FILTER
                    %s = 'Elvis';
            ''' % (case,))

    async def test_edgeql_expr_paths_02(self):
        await self.assert_query_result(r"""
            SELECT (1, (2, 3), 4).1.0;
        """, [
            [2],
        ])

    async def test_edgeql_expr_paths_03(self):
        # NOTE: The expression `.1` in this test is not a float,
        # instead it is a partial path (like `.name`). It is
        # syntactically legal (see test_edgeql_syntax_constants_09),
        # but will fail to resolve to anything.
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'could not resolve partial path'):
            await self.con.execute(r"""
                SELECT .1;
            """)

    async def test_edgeql_expr_paths_04(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
                WITH MODULE test
                SELECT Issue.owner
                FILTER Issue.number > '2';
            """)

    async def test_edgeql_expr_paths_05(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.id` which is defined in an outer
        # scope.
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
                WITH MODULE test
                SELECT Issue.id
                FILTER Issue.number > '2';
            """)

    async def test_edgeql_expr_paths_06(self):
        # `Issue.number` in the shape is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
                WITH MODULE test
                SELECT Issue.owner {
                    foo := Issue.number
                };
            """)

    @unittest.expectedFailure
    async def test_edgeql_expr_paths_07(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
                WITH MODULE test
                FOR x IN {'Elvis', 'Yury'}
                UNION (
                    SELECT Issue.owner
                    FILTER Issue.owner.name = x
                )
                FILTER Issue.number > '2';
            """)

    async def test_edgeql_expr_paths_08(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
                WITH MODULE test
                UPDATE Issue.owner
                FILTER Issue.number > '2'
                SET {
                    name := 'Foo'
                };
            """)

    async def test_edgeql_expr_paths_09(self):
        # `Issue` in SET is illegal because it shares a prefix `Issue`
        # with `Issue.related_to` which is defined in an outer scope.
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r"'Issue' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
                WITH MODULE test
                UPDATE Issue.related_to
                SET {
                    related_to := Issue
                };
            """)

    async def test_edgeql_expr_polymorphic_01(self):
        await self.con.execute(r"""
            WITH MODULE test
            SELECT Text {
                [IS Issue].number,
                [IS Issue].related_to,
                [IS Issue].`priority`,
                [IS test::Comment].owner: {
                    name
                }
            };
        """)

        await self.con.execute(r"""
            WITH MODULE test
            SELECT Owned {
                [IS Named].name
            };
        """)

    async def test_edgeql_expr_cast_01(self):
        await self.assert_query_result(r"""
            SELECT <std::str>123;
            SELECT <std::int64>"123";
            SELECT <std::str>123 + 'qw';
            SELECT <std::int64>"123" + 9000;
            SELECT <std::int64>"123" * 100;
            SELECT <std::str>(123 * 2);
            SELECT <int64>true;
            SELECT <int64>false;
        """, [
            ['123'],
            [123],
            ['123qw'],
            [9123],
            [12300],
            ['246'],
            [1],
            [0],
        ])

    async def test_edgeql_expr_cast_02(self):
        # testing precedence of casting vs. multiplication
        #
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'operator `\*` is not defined .* std::str and std::int64'):

            await self.con.execute("""
                SELECT <std::str>123 * 2;
            """)

    async def test_edgeql_expr_cast_03(self):
        await self.assert_query_result(r"""
            SELECT <std::str><std::int64><std::float64>'123.45' + 'foo';
        """, [
            ['123foo'],
        ])

    async def test_edgeql_expr_cast_04(self):
        await self.assert_query_result(r"""
            SELECT <str><int64><float64>'123.45' + 'foo';
        """, [
            ['123foo'],
        ])

    async def test_edgeql_expr_cast_05(self):
        await self.assert_query_result(r"""
            SELECT <array<int64>>['123', '11'];
        """, [
            [[123, 11]],
        ])

    async def test_edgeql_expr_cast_06(self):
        await self.assert_query_result(r"""
            SELECT <array<bool>>['t', 'tr', 'tru', 'true'];
            SELECT <array<bool>>['T', 'TR', 'TRU', 'TRUE'];
            SELECT <array<bool>>['True', 'TrUe', '1'];
            SELECT <array<bool>>['y', 'ye', 'yes'];
            SELECT <array<bool>>['Y', 'YE', 'YES'];
            SELECT <array<bool>>['Yes', 'yEs', 'YeS'];
        """, [
            [[True, True, True, True]],
            [[True, True, True, True]],
            [[True, True, True]],
            [[True, True, True]],
            [[True, True, True]],
            [[True, True, True]],
        ])

    async def test_edgeql_expr_cast_07(self):
        await self.assert_query_result(r"""
            SELECT <array<bool>>['f', 'fa', 'fal', 'fals', 'false'];
            SELECT <array<bool>>['F', 'FA', 'FAL', 'FALS', 'FALSE'];
            SELECT <array<bool>>['False', 'FaLSe', '0'];
            SELECT <array<bool>>['n', 'no'];
            SELECT <array<bool>>['N', 'NO'];
            SELECT <array<bool>>['No', 'nO'];
        """, [
            [[False, False, False, False, False]],
            [[False, False, False, False, False]],
            [[False, False, False]],
            [[False, False]],
            [[False, False]],
            [[False, False]],
        ])

    async def test_edgeql_expr_cast_08(self):
        with self.assertRaisesRegex(exc.EdgeQLError,
                                    r'cannot cast tuple'):
            await self.con.execute(r"""
                SELECT <array<int64>>(123, 11);
            """)

    async def test_edgeql_expr_cast_09(self):
        await self.assert_query_result(r"""
            SELECT <tuple<str, int64>> ('foo', 42);
            SELECT <tuple<str, int64>> (1, 2);
            SELECT <tuple<a: str, b: int64>> ('foo', 42);
        """, [
            [['foo', 42]],
            [['1', 2]],
            [{'a': 'foo', 'b': 42}],
        ])

    async def test_edgeql_expr_implicit_cast_01(self):
        await self.assert_query_result(r"""
            SELECT (<int32>1 + 3).__type__.name;
            SELECT (<int16>1 + 3).__type__.name;
            SELECT (<int16>1 + <int32>3).__type__.name;
            SELECT (1 + <float32>3.1).__type__.name;
            SELECT (<int16>1 + <float32>3.1).__type__.name;
            SELECT (<int16>1 + <float64>3.1).__type__.name;
            SELECT {1, <float32>2.1}.__type__.name;
            SELECT {1, 2.1}.__type__.name;
            SELECT (-2.1).__type__.name;
            SELECT {1, <decimal>2.1}.__type__.name;
        """, [
            ['std::int64'],
            ['std::int64'],
            ['std::int32'],
            # according to the _implicit_numeric_cast_map, any of the
            # ints can only be upcast to float64, not float32
            ['std::float64'],
            ['std::float64'],
            ['std::float64'],
            ['std::float64'],
            ['std::float64'],
            ['std::float64'],
            ['std::decimal'],
        ])

    async def test_edgeql_expr_implicit_cast_02(self):
        await self.assert_query_result(r"""
            SELECT (<float32>1 + <float64>2).__type__.name;
            SELECT (<int32>1 + <float32>2).__type__.name;
            SELECT (<int64>1 + <float32>2).__type__.name;
        """, [
            ['std::float64'],
            ['std::float64'],
            ['std::float64'],
        ])

    async def test_edgeql_expr_implicit_cast_03(self):
        # coalescing forces the left scalar operand to be implicitly
        # upcast to the right one even if the right one is never
        # technically evaluated (function not called, etc.)
        await self.assert_query_result(r"""
            SELECT (3 // 2).__type__.name;
            SELECT ((3 // 2) ?? <float64>{}).__type__.name;
            SELECT (3 / 2 ?? <decimal>{}).__type__.name;
            SELECT (3 // 2 ?? sum({1, 2.0})).__type__.name;
        """, [
            ['std::int64'],
            ['std::float64'],
            ['std::decimal'],
            ['std::float64'],
        ])

    async def test_edgeql_expr_implicit_cast_04(self):
        # IF should also force implicit casts of the two options
        await self.assert_query_result(r"""
            SELECT 3 / (2 IF TRUE ELSE 2.0);
            SELECT 3 / (2 IF random() > -1 ELSE 2.0);

            SELECT 3 / (2 IF FALSE ELSE 2.0);
            SELECT 3 / (2 IF random() < -1 ELSE 2.0);
        """, [
            [1.5],
            [1.5],
            [1.5],
            [1.5],
        ])

        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'if/else clauses must be of related types, '
                r'got: std::int64/std::str'):

            await self.con.execute("""
                SELECT 3 / (2 IF FALSE ELSE '1');
            """)

    async def test_edgeql_expr_implicit_cast_05(self):
        await self.assert_query_result(r"""
            SELECT {[1, 2.0], [3, 4.5]};
            SELECT {[1, 2], [3, 4.5]};
        """, [
            [[1, 2], [3, 4.5]],
            [[1, 2], [3, 4.5]],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_implicit_cast_06(self):
        await self.assert_query_result(r"""
            SELECT {(1, 2.0), (3, 4.5)};
            SELECT {(1, 2), (3, 4.5)};

            SELECT {(x := 1, y := 2.0), (x := 3, y := 4.5)};
            SELECT {(x := 1, y := 2), (x := 3, y := 4.5)};

            SELECT {(x := 1, y := 2), (a := 3, b := 4.5)};
            SELECT {(1, 2), (a := 3, b := 4.5)};
        """, [
            [[1, 2], [3, 4.5]],
            [[1, 2], [3, 4.5]],

            [{"x": 1, "y": 2}, {"x": 3, "y": 4.5}],
            [{"x": 1, "y": 2}, {"x": 3, "y": 4.5}],

            [[1, 2], [3, 4.5]],
            [[1, 2], [3, 4.5]],
        ])

    async def test_edgeql_expr_implicit_cast_07(self):
        await self.assert_query_result(r"""
            WITH
                MODULE schema,
                A := (
                    SELECT ObjectType {
                        a := 1,
                        b := 1 + 0 * random(),  # float64
                        c := 1 + 0 * <int64>random(),
                    })
            SELECT (3 / (A.a + A.b), 3 / (A.a + A.c)) LIMIT 1;
        """, [
            [[1.5, 1.5]],
        ])

    async def test_edgeql_expr_implicit_cast_08(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, 'could not determine expression type'):
            await self.query(r'''
                SELECT {1.0, <decimal>2.0};
            ''')

    async def test_edgeql_expr_type_01(self):
        await self.assert_query_result(r"""
            SELECT 'foo'.__type__.name;
        """, [
            ['std::str'],
        ])

    async def test_edgeql_expr_type_02(self):
        await self.assert_query_result(r"""
            SELECT (1.0 + 2).__type__.name;
        """, [
            ['std::float64'],
        ])

    async def test_edgeql_expr_set_01(self):
        await self.assert_query_result("""
            SELECT <int64>{};
            SELECT {1};
            SELECT {'foo'};
            SELECT {1} = 1;
        """, [
            [],
            [1],
            ['foo'],
            [True],
        ])

    async def test_edgeql_expr_set_02(self):
        await self.assert_query_result("""
            WITH
                MODULE schema,
                A := (
                    SELECT ObjectType
                    FILTER ObjectType.name ILIKE 'schema::a%'
                ),
                D := (
                    SELECT ObjectType
                    FILTER ObjectType.name ILIKE 'schema::d%'
                ),
                O := (
                    SELECT ObjectType
                    FILTER ObjectType.name ILIKE 'schema::o%'
                )
            SELECT _ := {A, D, O}.name
            ORDER BY _;
        """, [
            [
                'schema::Array',
                'schema::Attribute',
                'schema::Database',
                'schema::Delta',
                'schema::DerivedLink',
                'schema::DerivedObjectType',
                'schema::Object',
                'schema::ObjectType'
            ],
        ])

    async def test_edgeql_expr_set_03(self):
        await self.assert_query_result(r"""
            # "nested" sets are merged using UNION
            SELECT _ := {{2, 3, {1, 4}, 4}, {4, 1}}
            ORDER BY _;
        """, [
            [1, 1, 2, 3, 4, 4, 4],
        ])

    async def test_edgeql_expr_array_01(self):
        await self.assert_query_result("""
            SELECT [1];
            SELECT [1, 2, 3, 4, 5];
            SELECT [1, 2, 3, 4, 5][2];
            SELECT [1, 2, 3, 4, 5][-2];

            SELECT [1, 2, 3, 4, 5][2:4];
            SELECT [1, 2, 3, 4, 5][2:];
            SELECT [1, 2, 3, 4, 5][:2];

            SELECT [1, 2, 3, 4, 5][2:-1];
            SELECT [1, 2, 3, 4, 5][-2:];
            SELECT [1, 2, 3, 4, 5][:-2];

            # slice of something non-existent
            SELECT [1, 2][10:11];

            SELECT <array<int64>>[];

            SELECT [1, 2, 3, 4, 5][<int16>2];
            SELECT [1, 2, 3, 4, 5][<int32>2];
        """, [
            [[1]],
            [[1, 2, 3, 4, 5]],
            [3],
            [4],

            [[3, 4]],
            [[3, 4, 5]],
            [[1, 2]],

            [[3, 4]],
            [[4, 5]],
            [[1, 2, 3]],

            [[]],

            [[]],

            [3],
            [3],
        ])

    async def test_edgeql_expr_array_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'could not determine array type'):

            await self.con.execute("""
                SELECT [1, '1'];
            """)

    async def test_edgeql_expr_array_03(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index array by.*str'):

            await self.con.execute("""
                SELECT [1, 2]['1'];
            """)

    async def test_edgeql_expr_array_04(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'could not determine type of empty array'):

            await self.con.execute("""
                SELECT [];
            """)

    async def test_edgeql_expr_array_05(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'binary operator `\+` is not defined'):

            await self.con.execute('''
                SELECT [1, 2] + [3, 4];
            ''')

    async def test_edgeql_expr_array_06(self):
        await self.assert_query_result('''
            SELECT [1, <int64>{}];
        ''', [
            [],
        ])

    async def test_edgeql_expr_array_07(self):
        await self.assert_query_result('''
            WITH
                A := {1, 2},
                B := <int64>{}
            SELECT [A, B];
        ''', [
            [],
        ])

    async def test_edgeql_expr_array_08(self):
        await self.assert_query_result('''
            WITH
                MODULE schema,
                A := {'a', 'b'},
                # B is an empty set
                B := (SELECT Type FILTER Type.name = 'n/a').name
            SELECT [A, B];
        ''', [
            [],
        ])

    async def test_edgeql_expr_array_09(self):
        await self.assert_query_result('''
            WITH
                MODULE schema,
                A := (SELECT Type FILTER .name = 'test::issue_num_t')
            SELECT [A.name, A.description];
        ''', [
            [],
        ])

    async def test_edgeql_expr_array_10(self):
        with self.assertRaisesRegex(exc.EdgeQLError, 'nested array'):
            await self.con.execute(r'''
                SELECT [[1, 2], [3, 4]];
            ''')

    async def test_edgeql_expr_array_11(self):
        with self.assertRaisesRegex(exc.EdgeQLError, 'nested array'):
            await self.con.execute(r'''
                SELECT [array_agg({1, 2})];
            ''')

    async def test_edgeql_expr_array_12(self):
        with self.assertRaisesRegex(
                exc.SchemaError,
                r"nested arrays are not supported"):
            await self.con.execute(r'''
                SELECT array_agg([1, 2, 3]);
            ''')

    async def test_edgeql_expr_array_13(self):
        with self.assertRaisesRegex(
                exc.SchemaError,
                r"nested arrays are not supported"):
            await self.con.execute(r'''
                SELECT array_agg(array_agg({1, 2 ,3}));
            ''')

    async def test_edgeql_expr_array_14(self):
        await self.assert_query_result('''
            SELECT [([([1],)],)];
        ''', [
            [   # result set
                [[[[[1]]]]]
            ],
        ])

    async def test_edgeql_expr_array_15(self):
        with self.assertRaisesRegex(
                # FIXME: possibly a different error should be used here
                exc.UnknownEdgeDBError,
                r'array index 10 is out of bounds'):
            await self.con.execute("""
                SELECT [1, 2, 3][10];
            """)

    async def test_edgeql_expr_array_16(self):
        with self.assertRaisesRegex(
                # FIXME: possibly a different error should be used here
                exc.UnknownEdgeDBError,
                r'array index -10 is out of bounds'):
            await self.con.execute("""
                SELECT [1, 2, 3][-10];
            """)

    async def test_edgeql_expr_array_17(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index array by.*float'):

            await self.con.execute("""
                SELECT [1, 2][1.0];
            """)

    async def test_edgeql_expr_array_18(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot slice array by.*float'):

            await self.con.execute("""
                SELECT [1, 2][1.0:3];
            """)

    async def test_edgeql_expr_array_19(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot slice array by.*str'):

            await self.con.execute("""
                SELECT [1, 2][1:'3'];
            """)

    async def test_edgeql_expr_array_20(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'cannot index array by std::decimal'):

            await self.con.execute("""
                SELECT [1, 2][2^40];
            """)

    async def test_edgeql_expr_coalesce_01(self):
        await self.assert_query_result(r"""
            SELECT {} ?? 4 ?? 5;
            SELECT {} ?? 'foo' ?? 'bar';
            SELECT 4 ?? {} ?? 5;

            SELECT 'foo' ?? {} ?? 'bar';
            SELECT {} ?? 'bar' = 'bar';

            SELECT 4^{} ?? 2;
            SELECT 4+{} ?? 2;
            SELECT 4*{} ?? 2;

            SELECT -<int64>{} ?? 2;
            SELECT -<int64>{} ?? -2 + 1;

            SELECT <int64>({} ?? {});
            SELECT <int64>({} ?? {} ?? {});
        """, [
            [4],
            ['foo'],
            [4],

            ['foo'],
            [True],

            [2],  # ^ binds more tightly
            [6],
            [8],

            [2],
            [-1],

            [],
            [],
        ])

    async def test_edgeql_expr_string_01(self):
        await self.assert_query_result("""
            SELECT 'qwerty';
            SELECT 'qwerty'[2];
            SELECT 'qwerty'[-2];

            SELECT 'qwerty'[2:4];
            SELECT 'qwerty'[2:];
            SELECT 'qwerty'[:2];

            SELECT 'qwerty'[2:-1];
            SELECT 'qwerty'[-2:];
            SELECT 'qwerty'[:-2];

            SELECT 'qwerty'[<int16>2];
            SELECT 'qwerty'[<int32>2];
        """, [
            ['qwerty'],
            ['e'],
            ['t'],

            ['er'],
            ['erty'],
            ['qw'],

            ['ert'],
            ['ty'],
            ['qwer'],

            ['e'],
            ['e'],
        ])

    async def test_edgeql_expr_string_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index string by.*str'):

            await self.con.execute("""
                SELECT '123'['1'];
            """)

    async def test_edgeql_expr_string_03(self):
        with self.assertRaisesRegex(
                # FIXME: possibly a different error should be used here
                exc.UnknownEdgeDBError,
                r'string index 10 is out of bounds'):
            await self.con.execute("""
                SELECT '123'[10];
            """)

    async def test_edgeql_expr_string_04(self):
        with self.assertRaisesRegex(
                # FIXME: possibly a different error should be used here
                exc.UnknownEdgeDBError,
                r'string index -10 is out of bounds'):
            await self.con.execute("""
                SELECT '123'[-10];
            """)

    async def test_edgeql_expr_string_05(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index string by.*float'):

            await self.con.execute("""
                SELECT '123'[-1.0];
            """)

    async def test_edgeql_expr_string_06(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot slice string by.*float'):

            await self.con.execute("""
                SELECT '123'[1.0:];
            """)

    async def test_edgeql_expr_string_07(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot slice string by.*str'):

            await self.con.execute("""
                SELECT '123'[:'1'];
            """)

    async def test_edgeql_expr_string_08(self):
        await self.assert_query_result(r"""
            SELECT ':\x62:\u2665:\U000025C6:☎️:';
            SELECT '\'"\\\'\""\\x\\u';
            SELECT "'\"\\\'\"\\x\\u";

            SELECT 'aa\
            bb \
            aa';

            SELECT r'\n';

            SELECT r'aa\
            bb \
            aa';
        """, [
            [':b:♥:◆:☎️:'],
            ['\'"\\\'\""\\x\\u'],
            ['\'"\\\'"\\x\\u'],

            ['aa            bb             aa'],

            ['\\n'],

            ['aa\\\n            bb \\\n            aa'],
        ])

    async def test_edgeql_expr_tuple_01(self):
        await self.assert_query_result(r"""
            SELECT (1, 'foo');
        """, [
            [[1, 'foo']],
        ])

    async def test_edgeql_expr_tuple_02(self):
        await self.assert_query_result(r"""
            SELECT (1, 'foo') = (1, 'foo');
            SELECT (1, 'foo') = (2, 'foo');
            SELECT (1, 'foo') != (1, 'foo');
            SELECT (1, 'foo') != (2, 'foo');
        """, [
            [True],
            [False],
            [False],
            [True],
        ])

    async def test_edgeql_expr_tuple_03(self):
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError, r'operator does not exist'):
            await self.con.execute(r"""
                SELECT (1, 'foo') = ('1', 'foo');
            """)

    async def test_edgeql_expr_tuple_04(self):
        await self.assert_query_result(r"""
            SELECT array_agg((1, 'foo'));
        """, [
            [[[1, 'foo']]],
        ])

    async def test_edgeql_expr_tuple_05(self):
        await self.assert_query_result(r"""
            SELECT (1, 2) UNION (3, 4);
        """, [
            [[1, 2], [3, 4]],
        ])

    async def test_edgeql_expr_tuple_06(self):
        await self.assert_query_result(r"""
            SELECT (1, 'foo') = (a := 1, b := 'foo');
            SELECT (a := 1, b := 'foo') = (a := 1, b := 'foo');
            SELECT (a := 1, b := 'foo') = (c := 1, d := 'foo');
            SELECT (a := 1, b := 'foo') = (b := 1, a := 'foo');
            SELECT (a := 1, b := 9001) != (b := 9001, a := 1);
            SELECT (a := 1, b := 9001).a = (b := 9001, a := 1).a;
            SELECT (a := 1, b := 9001).b = (b := 9001, a := 1).b;
        """, [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_expr_tuple_07(self):
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError, r'operator does not exist'):
            await self.con.execute(r"""
                SELECT (a := 1, b := 'foo') != (b := 'foo', a := 1);
            """)

    async def test_edgeql_expr_tuple_08(self):
        await self.assert_query_result(r"""
            SELECT ();
        """, [
            [[]],
        ])

    async def test_edgeql_expr_tuple_09(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'operator `\+` is not defined .*tuple<.*> and std::int64'):

            await self.con.execute(r'''
                SELECT (spam := 1, ham := 2) + 1;
            ''')

    async def test_edgeql_expr_tuple_10(self):
        await self.assert_query_result('''\
            SELECT _ := (spam := {1, 2}, ham := {3, 4})
            ORDER BY _.spam THEN _.ham;
        ''', [[
            {'ham': 3, 'spam': 1},
            {'ham': 4, 'spam': 1},
            {'ham': 3, 'spam': 2},
            {'ham': 4, 'spam': 2}
        ]])

    async def test_edgeql_expr_tuple_11(self):
        await self.assert_query_result('''\
            SELECT (1, 2) = (1, 2);
            SELECT (1, 2) UNION (1, 2);
            SELECT DISTINCT ((1, 2) UNION (1, 2));
        ''', [
            [True],
            [[1, 2], [1, 2]],
            [[1, 2]],
        ])

    async def test_edgeql_expr_tuple_12(self):
        await self.assert_query_result(r'''
            WITH A := {1, 2, 3}
            SELECT _ := ({'a', 'b'}, A)
            ORDER BY _;
        ''', [
            [['a', 1], ['a', 2], ['a', 3], ['b', 1], ['b', 2], ['b', 3]],
        ])

    async def test_edgeql_expr_tuple_13(self):
        await self.assert_query_result(r"""
            SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3);

            # should be the same as above
            WITH _ := (1, ('a', 'b', (0.1, 0.2)), 2, 3)
            SELECT _;
        """, [
            [[1, ['a', 'b', [0.1, 0.2]], 2, 3]],
            [[1, ['a', 'b', [0.1, 0.2]], 2, 3]],
        ])

    async def test_edgeql_expr_tuple_14(self):
        await self.assert_query_result('''
            SELECT (1, <int64>{});
        ''', [
            [],
        ])

    async def test_edgeql_expr_tuple_15(self):
        await self.assert_query_result('''
            WITH
                A := {1, 2},
                B := <int64>{}
            SELECT (A, B);
        ''', [
            [],
        ])

    async def test_edgeql_expr_tuple_16(self):
        await self.assert_query_result('''
            WITH
                MODULE schema,
                A := {'a', 'b'},
                # B is an empty set
                B := (SELECT Type FILTER Type.name = 'n/a').name
            SELECT (A, B);
        ''', [
            [],
        ])

    async def test_edgeql_expr_tuple_indirection_01(self):
        await self.assert_query_result(r"""
            SELECT ('foo', 42).0;
            SELECT ('foo', 42).1;
        """, [
            ['foo'],
            [42],
        ])

    async def test_edgeql_expr_tuple_indirection_02(self):
        await self.assert_query_result(r"""
            SELECT (name := 'foo', val := 42).name;
            SELECT (name := 'foo', val := 42).val;
        """, [
            ['foo'],
            [42],
        ])

    async def test_edgeql_expr_tuple_indirection_03(self):
        await self.assert_query_result(r"""
            WITH _ := (SELECT ('foo', 42)) SELECT _.1;
        """, [
            [42],
        ])

    async def test_edgeql_expr_tuple_indirection_04(self):
        await self.assert_query_result(r"""
            WITH _ := (SELECT (name := 'foo', val := 42)) SELECT _.name;
        """, [
            ['foo'],
        ])

    async def test_edgeql_expr_tuple_indirection_05(self):
        await self.assert_query_result(r"""
            WITH _ := (SELECT (1,2) UNION (3,4)) SELECT _.0;
        """, [
            [1, 3],
        ])

    async def test_edgeql_expr_tuple_indirection_06(self):
        await self.assert_query_result(r"""
            SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).0;
            SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).1;
            SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).1.2;
            SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).1.2.0;
        """, [
            [1],
            [['a', 'b', [0.1, 0.2]]],
            [[0.1, 0.2]],
            [0.1],
        ])

    async def test_edgeql_expr_tuple_indirection_07(self):
        await self.assert_query_result(r"""
            WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.0;
            WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.1;
            WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.1.2;
            WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.1.2.0;
        """, [
            [1],
            [['a', 'b', [0.1, 0.2]]],
            [[0.1, 0.2]],
            [0.1],
        ])

    async def test_edgeql_expr_tuple_indirection_08(self):
        await self.assert_query_result(r"""
            SELECT _ := (1, ({55, 66}, {77, 88}), 2)
            ORDER BY _.1 DESC;
        """, [[
            [1, [66, 88], 2],
            [1, [66, 77], 2],
            [1, [55, 88], 2],
            [1, [55, 77], 2],
        ]])

    async def test_edgeql_expr_tuple_indirection_09(self):
        await self.assert_query_result(r"""
            SELECT _ := (1, ({55, 66}, {77, 88}), 2)
            ORDER BY _.1.1 THEN _.1.0;
        """, [[
            [1, [55, 77], 2],
            [1, [66, 77], 2],
            [1, [55, 88], 2],
            [1, [66, 88], 2],
        ]])

    async def test_edgeql_expr_tuple_indirection_10(self):
        await self.assert_query_result(r"""
            SELECT [(0, 1)][0].1;
        """, [[
            1,
        ]])

    async def test_edgeql_expr_tuple_indirection_11(self):
        await self.assert_query_result(r"""
            SELECT [(a := 1, b := 2)][0].b;
        """, [[
            2,
        ]])

    async def test_edgeql_expr_tuple_indirection_12(self):
        await self.assert_query_result(r"""
            SELECT (name := 'foo', val := 42).0;
            SELECT (name := 'foo', val := 42).1;
            SELECT [(name := 'foo', val := 42)][0].name;
            SELECT [(name := 'foo', val := 42)][0].1;
        """, [
            ['foo'],
            [42],
            ['foo'],
            [42],
        ])

    async def test_edgeql_expr_tuple_indirection_13(self):
        await self.assert_query_result(r"""
            SELECT (a:=(b:=(c:=(e:=1))));

            SELECT (a:=(b:=(c:=(e:=1)))).a;
            SELECT (a:=(b:=(c:=(e:=1)))).0;

            SELECT (a:=(b:=(c:=(e:=1)))).a.b;
            SELECT (a:=(b:=(c:=(e:=1)))).0.0;

            SELECT (a:=(b:=(c:=(e:=1)))).a.b.c;
            SELECT (a:=(b:=(c:=(e:=1)))).0.0.0;

            SELECT (a:=(b:=(c:=(e:=1)))).a.b.c.e;
            SELECT (a:=(b:=(c:=(e:=1)))).0.b.c.0;
        """, [
            [{"a": {"b": {"c": {"e": 1}}}}],

            [{"b": {"c": {"e": 1}}}],
            [{"b": {"c": {"e": 1}}}],

            [{"c": {"e": 1}}],
            [{"c": {"e": 1}}],

            [{"e": 1}],
            [{"e": 1}],

            [1],
            [1],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_tuple_indirection_14(self):
        await self.assert_query_result(r"""
            SELECT [(a:=(b:=(c:=(e:=1))))][0].a;
            SELECT [(a:=(b:=(c:=(e:=1))))][0].0;
        """, [
            [{"b": {"c": {"e": 1}}}],
            [{"b": {"c": {"e": 1}}}],
        ])

    async def test_edgeql_expr_cannot_assign_dunder_type_01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot assign to __type__'):
            await self.con.execute(r"""
                SELECT test::Text {
                    __type__ := 42
                };
            """)

    async def test_edgeql_expr_cannot_assign_id_01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot assign to id'):
            await self.con.execute(r"""
                SELECT test::Text {
                    id := <uuid>'77841036-8e35-49ce-b509-2cafa0c25c4f'
                };
            """)

    async def test_edgeql_expr_if_else_01(self):
        await self.assert_query_result(r"""
            SELECT 'yes' IF True ELSE 'no';
            SELECT 'yes' IF 1=1 ELSE 'no';
            SELECT 'yes' IF 1=0 ELSE 'no';
            SELECT 's1' IF 1=0 ELSE 's2' IF 2=2 ELSE 's3';
        """, [
            ['yes'],
            ['yes'],
            ['no'],
            ['s2'],
        ])

    async def test_edgeql_expr_if_else_02(self):
        await self.assert_query_result(r"""
            SELECT 'yes' IF True ELSE {'no', 'or', 'maybe'};
            SELECT 'yes' IF False ELSE {'no', 'or', 'maybe'};

            SELECT {'maybe', 'yes'} IF True ELSE {'no', 'or'};
            SELECT {'maybe', 'yes'} IF False ELSE {'no', 'or'};

            SELECT {'maybe', 'yes'} IF True ELSE 'no';
            SELECT {'maybe', 'yes'} IF False ELSE 'no';

            SELECT 'yes' IF {True, False} ELSE 'no';
            SELECT 'yes' IF {True, False} ELSE {'no', 'or', 'maybe'};
            SELECT {'maybe', 'yes'} IF {True, False} ELSE {'no', 'or'};
            SELECT {'maybe', 'yes'} IF {True, False} ELSE 'no';
        """, [
            ['yes'],
            ['no', 'or', 'maybe'],

            ['maybe', 'yes'],
            ['no', 'or'],

            ['maybe', 'yes'],
            ['no'],

            ['yes', 'no'],
            ['yes', 'no', 'or', 'maybe'],
            ['maybe', 'yes', 'no', 'or'],
            ['maybe', 'yes', 'no'],
        ])

    async def test_edgeql_expr_if_else_03(self):
        await self.assert_sorted_query_result(r"""
            SELECT 1 IF {1, 2, 3} < {2, 3, 4} ELSE 100;
            SELECT {1, 10} IF {1, 2, 3} < {2, 3, 4} ELSE 100;

            SELECT sum(1 IF {1, 2, 3} < {2, 3, 4} ELSE 100);
            SELECT sum({1, 10} IF {1, 2, 3} < {2, 3, 4} ELSE 100);
        """, lambda x: x, [
            sorted([1, 1, 1, 100, 1, 1, 100, 100, 1]),
            sorted([1, 10, 1, 10, 1, 10, 100, 1, 10, 1, 10, 100, 100, 1, 10]),
            [306],
            [366],
        ])

    async def test_edgeql_expr_if_else_04(self):
        await self.assert_sorted_query_result(r"""
            WITH x := <str>{}
            SELECT
                1   IF x = 'a' ELSE
                10  IF x = 'b' ELSE
                100 IF x = 'c' ELSE
                0;

            WITH x := {'c', 'a', 't'}
            SELECT
                1   IF x = 'a' ELSE
                10  IF x = 'b' ELSE
                100 IF x = 'c' ELSE
                0;

            WITH x := {'b', 'a', 't'}
            SELECT
                1   IF x = 'a' ELSE
                10  IF x = 'b' ELSE
                100 IF x = 'c' ELSE
                0;

            FOR w IN {<array<str>>[], ['c', 'a', 't'], ['b', 'a', 't']}
            UNION (
                WITH x := array_unpack(w)
                SELECT sum(
                    1   IF x = 'a' ELSE
                    10  IF x = 'b' ELSE
                    100 IF x = 'c' ELSE
                    0
                )
            );
        """, lambda x: x, [
            [],
            sorted([100, 1, 0]),
            sorted([10, 1, 0]),
            sorted([0, 101, 11]),
        ])

    async def test_edgeql_expr_if_else_05(self):
        await self.assert_sorted_query_result(r"""
            # this creates a 3 x 3 x 3 cross product
            SELECT
                1   IF {'c', 'a', 't'} = 'a' ELSE
                10  IF {'c', 'a', 't'} = 'b' ELSE
                100 IF {'c', 'a', 't'} = 'c' ELSE
                0;
        """, lambda x: x, [
            sorted([
                100,    # ccc
                0,      # cca
                0,      # cct
                100,    # cac
                0,      # caa
                0,      # cat
                100,    # ctc
                0,      # cta
                0,      # ctt
                1,      # a--
                        #       The other clauses don't get evaluated,
                        #       when 'a' is in the first test.  More
                        #       accurately, they get evaluated and
                        #       their results are not included in the
                        #       return value.

                100,    # tcc
                0,      # tca
                0,      # tct
                100,    # tac
                0,      # taa
                0,      # tat
                100,    # ttc
                0,      # tta
                0,      # ttt
            ]),
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_if_else_06(self):
        await self.assert_query_result(r"""
            WITH a := {'c', 'a', 't'}
            SELECT
                (a, 'hit' IF a = 'c' ELSE 'miss')
            ORDER BY .0;

            WITH a := {'c', 'a', 't'}
            SELECT
                (a, 'hit') IF a = 'c' ELSE (a, 'miss')
            ORDER BY .0;
        """, [
            [['a', 'miss'], ['c', 'hit'], ['t', 'miss']],
            [['a', 'miss'], ['c', 'hit'], ['t', 'miss']],
        ])

    async def test_edgeql_expr_setop_01(self):
        await self.assert_query_result(r"""
            SELECT EXISTS {};
            SELECT NOT EXISTS {};
        """, [
            [False],
            [True],
        ])

    async def test_edgeql_expr_setop_02(self):
        await self.assert_query_result(r"""
            SELECT 2 * ((SELECT 1) UNION (SELECT 2));
            SELECT (SELECT 2) * (1 UNION 2);
            SELECT 2 * DISTINCT (1 UNION 2 UNION 1);
            SELECT 2 * (1 UNION 2 UNION 1);

            WITH
                a := (SELECT 1 UNION 2)
            SELECT (SELECT 2) * a;
        """, [
            [2, 4],
            [2, 4],
            [2, 4],
            [2, 4, 2],
            [2, 4],
        ])

    async def test_edgeql_expr_setop_03(self):
        await self.assert_query_result('''
            SELECT array_agg(1 UNION 2 UNION 3);
            SELECT array_agg(3 UNION 2 UNION 3);
            SELECT array_agg(3 UNION 3 UNION 2);
        ''', [
            [[1, 2, 3]],
            [[3, 2, 3]],
            [[3, 3, 2]],
        ])

    async def test_edgeql_expr_setop_04(self):
        await self.assert_query_result('''
            SELECT DISTINCT {1, 2, 2, 3};
        ''', [
            {1, 2, 3},
        ])

    async def test_edgeql_expr_setop_05(self):
        await self.assert_query_result('''
            SELECT (2 UNION 2 UNION 2);
        ''', [
            [2, 2, 2],
        ])

    async def test_edgeql_expr_setop_06(self):
        await self.assert_query_result('''
            SELECT DISTINCT (2 UNION 2 UNION 2);
        ''', [
            [2],
        ])

    async def test_edgeql_expr_setop_07(self):
        await self.assert_query_result('''
            SELECT DISTINCT (2 UNION 2) UNION 2;
        ''', [
            [2, 2],
        ])

    async def test_edgeql_expr_setop_08(self):
        res = await self.con.execute('''
            WITH MODULE schema
            SELECT ObjectType;

            WITH MODULE schema
            SELECT Attribute;

            WITH MODULE schema
            SELECT ObjectType UNION Attribute;
        ''')
        separate = [obj['id'] for obj in res[0]]
        separate += [obj['id'] for obj in res[1]]
        union = [obj['id'] for obj in res[2]]
        separate.sort()
        union.sort()
        self.assert_data_shape(separate, union)

    async def test_edgeql_expr_setop_09(self):
        res = await self.con.execute('''
            SELECT _ := DISTINCT {[1, 2], [1, 2], [2, 3]} ORDER BY _;
        ''')
        self.assert_data_shape(res, [
            [[1, 2], [2, 3]],
        ])

    async def test_edgeql_expr_setop_10(self):
        res = await self.con.execute('''
            SELECT _ := DISTINCT {(1, 2), (2, 3), (1, 2)} ORDER BY _;
            SELECT _ := DISTINCT {(a := 1, b := 2),
                                  (a := 2, b := 3),
                                  (a := 1, b := 2)}
            ORDER BY _;
        ''')
        self.assert_data_shape(res, [
            [[1, 2], [2, 3]],
            [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}],
        ])

    async def test_edgeql_expr_setop_11(self):
        res = await self.con.execute('''
            WITH
                MODULE schema,
                C := (SELECT ObjectType
                      FILTER ObjectType.name LIKE 'schema::%')
            SELECT _ := len(C.name)
            ORDER BY _;

            WITH
                MODULE schema,
                C := (SELECT ObjectType
                      FILTER ObjectType.name LIKE 'schema::%')
            SELECT _ := DISTINCT len(C.name)
            ORDER BY _;
        ''')

        # test the results of DISTINCT directly, rather than relying
        # on an aggregate function
        self.assertGreater(
            len(res[0]), len(res[1]),
            'DISTINCT len(ObjectType.name) failed to filter out dupplicates')

    async def test_edgeql_expr_cardinality_01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=39):

            await self.query('''\
                WITH MODULE test
                SELECT Issue ORDER BY Issue.watchers.name;
            ''')

    async def test_edgeql_expr_cardinality_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=30):

            await self.query('''\
                WITH MODULE test
                SELECT Issue LIMIT LogEntry.spent_time;
            ''')

    async def test_edgeql_expr_cardinality_03(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=30):

            await self.query('''\
                WITH MODULE test
                SELECT Issue OFFSET LogEntry.spent_time;
            ''')

    async def test_edgeql_expr_cardinality_04(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=46):

            await self.query('''\
                WITH MODULE test
                SELECT EXISTS Issue ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_05(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=53):

            await self.query('''\
                WITH MODULE test
                SELECT 'foo' IN Issue.name ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_06(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=50):

            await self.query('''\
                WITH MODULE test
                SELECT Issue UNION Text ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_07(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=48):

            await self.query('''\
                WITH MODULE test
                SELECT DISTINCT Issue ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_type_filter_01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'invalid type filter operand: std::int64 is not '
                r'an object type',
                position=7):

            await self.query('''\
                SELECT 10[IS std::Object];
            ''')

    async def test_edgeql_expr_type_filter_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'invalid type filter operand: std::str is not an object type',
                position=17):

            await self.query('''\
                SELECT Object[IS str];
            ''')

    async def test_edgeql_expr_type_filter_03(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'invalid type filter operand: '
                r'std::uuid is not an object type',
                position=20):

            await self.query('''\
                SELECT Object.id[IS uuid];
            ''')

    @unittest.expectedFailure
    async def test_edgeql_expr_comparison_01(self):
        with self.assertRaisesRegex(exc.UnknownEdgeDBError,
                                    r'operator does not exist'):
            await self.con.execute(r'''
                SELECT (1, 2) = [1, 2];
            ''')

    async def test_edgeql_expr_comparison_02(self):
        with self.assertRaisesRegex(exc.UnknownEdgeDBError,
                                    r'operator does not exist'):
            await self.con.execute(r'''
                SELECT {1, 2} = [1, 2];
            ''')

    @unittest.expectedFailure
    async def test_edgeql_expr_comparison_03(self):
        with self.assertRaisesRegex(exc.UnknownEdgeDBError,
                                    r'operator does not exist'):
            await self.con.execute(r'''
                SELECT {1, 2} = (1, 2);
            ''')

    async def test_edgeql_expr_aggregate_01(self):
        await self.assert_query_result(r"""
            SELECT count(DISTINCT {1, 1, 1});
            SELECT count(DISTINCT {1, 2, 3});
            SELECT count(DISTINCT {1, 2, 3, 2, 3});

            SELECT count({1, 1, 1});
            SELECT count({1, 2, 3});
            SELECT count({1, 2, 3, 2, 3});
        """, [
            [1],
            [3],
            [3],

            [3],
            [3],
            [5],
        ])

    async def test_edgeql_expr_view_01(self):
        await self.assert_query_result(r"""
            WITH
                a := {1, 2},
                b := {2, 3}
            SELECT a
            FILTER a = b;
        """, [
            [2],
        ])

    async def test_edgeql_expr_view_02(self):
        await self.assert_query_result(r"""
            WITH
                b := {2, 3}
            SELECT a := {1, 2}
            FILTER a = b;
        """, [
            [2],
        ])

    async def test_edgeql_expr_view_03(self):
        await self.assert_query_result(r"""
            SELECT (
                name := 'a',
                foo := (
                    WITH a := {1, 2}
                    SELECT a
                )
            );
        """, [
            [{'name': 'a', 'foo': 1}, {'name': 'a', 'foo': 2}],
        ])

    async def test_edgeql_expr_view_04(self):
        await self.assert_query_result(r"""
            SELECT (
                name := 'a',
                foo := (
                    WITH a := {1, 2}
                    SELECT a
                    FILTER a < 2
                )
            );
        """, [
            [{'name': 'a', 'foo': 1}],
        ])

    async def test_edgeql_expr_view_05(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                foo := (
                    WITH a := {1, 2}
                    SELECT a
                )
            }
            FILTER .name LIKE 'schema::%'
            ORDER BY .name LIMIT 1;
        """, [
            [{'name': 'schema::Array', 'foo': {1, 2}}],
        ])

    async def test_edgeql_expr_view_06(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                foo := (
                    WITH a := {1, 2}
                    SELECT a
                    FILTER a < 2
                )
            }
            FILTER .name LIKE 'schema::%'
            ORDER BY .name LIMIT 1;
        """, [
            [{'name': 'schema::Array', 'foo': {1}}],
        ])

    async def test_edgeql_expr_view_07(self):
        await self.assert_query_result(r"""
            # test variable masking
            WITH x := (
                WITH x := {2, 3, 4} SELECT {4, 5, x}
            )
            SELECT x ORDER BY x;
        """, [
            [2, 3, 4, 4, 5],
        ])

    async def test_edgeql_expr_view_08(self):
        await self.assert_query_result(r"""
            # test variable masking
            WITH x := (
                FOR x IN {2, 3}
                UNION x + 2
            )
            SELECT x ORDER BY x;
        """, [
            [4, 5],
        ])

    # XXX: DETACHED may no longer work this way at all
    @unittest.expectedFailure
    async def test_edgeql_expr_view_09(self):
        await self.assert_sorted_query_result(r"""
            # set some base cases
            WITH a := {1, 2}
            SELECT a + a;
            # normally all scalar set literals are "DETACHED"
            SELECT {1, 2} + {1, 2};

            # DETACHED literals
            WITH a := {1, 2}
            SELECT a + DETACHED a;

            WITH
                a := {1, 2},
                b := DETACHED a
            SELECT a + b;

            WITH
                a := {1, 2},
                b := DETACHED a
            SELECT b + b;
        """, lambda x: x, [
            [2, 4],
            [2, 3, 3, 4],
            [2, 3, 3, 4],
            [2, 3, 3, 4],
            [2, 4],
        ])

    async def test_edgeql_expr_for_01(self):
        await self.assert_query_result(r"""
            FOR x IN {1, 3, 5, 7}
            UNION x
            ORDER BY x;

            FOR x IN {1, 3, 5, 7}
            UNION x + 1
            ORDER BY x;
        """, [
            [1, 3, 5, 7],
            [2, 4, 6, 8],
        ])

    async def test_edgeql_expr_for_02(self):
        await self.assert_query_result(r"""
            FOR x IN {2, 3}
            UNION {x, x + 2};
        """, [
            {2, 3, 4, 5},
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_group_01(self):
        await self.assert_query_result(r"""
            WITH I := {1, 2, 3, 4}
            GROUP I
            USING _ := I % 2 = 0
            BY _
            INTO I
            UNION _r := (
                values := array_agg(I ORDER BY I)
            ) ORDER BY _r.values;
        """, [
            [
                {'values': [1, 3]},
                {'values': [2, 4]}
            ]
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_group_02(self):
        await self.assert_sorted_query_result(r'''
            # handle a number of different aliases
            WITH x := {(1, 2), (3, 4), (4, 2)}
            GROUP y := x
            USING _ := y.1
            BY _
            INTO y
            UNION array_agg(y.0 ORDER BY y.0);
        ''', lambda x: x, [
            [[1, 4], [3]],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_group_03(self):
        await self.assert_sorted_query_result(r'''
            WITH x := {(1, 2), (3, 4), (4, 2)}
            GROUP x
            USING _ := x.1
            BY _
            INTO x
            UNION array_agg(x.0 ORDER BY x.0);
        ''', lambda x: x, [
            [[1, 4], [3]],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_group_04(self):
        await self.assert_query_result(r'''
            WITH x := {(1, 2), (3, 4), (4, 2)}
            GROUP x
            USING B := x.1
            BY B
            INTO x
            UNION (B, array_agg(x.0 ORDER BY x.0))
            ORDER BY
                B;
        ''', [
            [[2, [1, 4]], [4, [3]]],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_group_05(self):
        await self.assert_query_result(r'''
            # handle the case where the value to be computed depends
            # on both, the grouped subset and the original set
            WITH
                x1 := {(1, 0), (1, 0), (1, 0), (2, 0), (3, 0), (3, 0)},
                x2 := x1
            GROUP y := x1
            USING z := y.0
            BY z
            INTO y
            UNION (
                # we expect that count(x1) and count(x2) will be
                # identical in this context, whereas count(y) will
                # represent the size of each subset
                z, count(y), count(x1), count(x2)
            )
            ORDER BY z;
        ''', [
            [[1, 3, 6, 6], [2, 1, 6, 6], [3, 2, 6, 6]]
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_group_06(self):
        await self.assert_query_result(r'''
            GROUP X := {1, 1, 1, 2, 3, 3}
            USING y := X
            BY y
            INTO y
            UNION (y, count(X))
            ORDER BY y;
        ''', [
            [[1, 3], [2, 1], [3, 2]]
        ])

    async def test_edgeql_expr_schema_01(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT ObjectType {
                name,
                attributes: {
                    name,
                    @value
                } ORDER BY ObjectType.attributes.name
            }
            FILTER .name = 'test::User';

        ''', [
            [{
                'name': 'test::User',
                'attributes': [
                    {'name': 'stdattrs::description', '@value': None},
                    {'name': 'stdattrs::expr', '@value': None},
                    {'name': 'stdattrs::is_abstract', '@value': 'false'},
                    {'name': 'stdattrs::is_derived', '@value': 'false'},
                    {'name': 'stdattrs::is_final', '@value': 'false'},
                    {'name': 'stdattrs::is_virtual', '@value': 'false'},
                    {'name': 'stdattrs::name', '@value': 'test::User'},
                    {'name': 'stdattrs::title', '@value': None},
                    {'name': 'stdattrs::view_type', '@value': None},
                ]
            }]
        ])
