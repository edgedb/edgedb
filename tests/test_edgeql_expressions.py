##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestExpressions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_expr_emptyset_01(self):
        await self.assert_query_result(r"""
            SELECT <int>{};
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
            SELECT count(<int>{});
            SELECT count(DISTINCT <int>{});
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
        ])

    async def test_edgeql_expr_op_05(self):
        await self.assert_query_result(r"""
            SELECT 'foo' + 'bar';
        """, [
            ['foobar'],
        ])

    async def test_edgeql_expr_op_06(self):
        await self.assert_query_result(r"""
            SELECT {} = {};
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
            SELECT +<int>{};
            SELECT -<int>{};
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
            SELECT <int>{} ?= <int>{};
            SELECT 2 ?!= {};
            SELECT <int>{} ?!= <int>{};
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
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

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_op_16(self):
        await self.assert_query_result(r"""
            WITH a := {11, 12, 13}
            SELECT _ := {9, 1, 13}
            FILTER _ IN a;

            WITH MODULE schema
            SELECT _ := {9, 1, 13}
            FILTER _ IN (
                # Lengths of names for schema::Map, Node, and Array are
                # 11, 12, and 13, respectively.
                SELECT len(Concept.name)
                FILTER Concept.name LIKE 'schema::%'
            );
        """, [
            {13},
            {13},
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_op_17(self):
        await self.assert_query_result(r"""
            WITH a := {11, 12, 13}
            SELECT _ := {9, 1, 13}
            FILTER _ NOT IN a;

            WITH MODULE schema
            SELECT _ := {9, 1, 13}
            FILTER _ NOT IN (
                # Lengths of names for schema::Map, Node, and Array are
                # 11, 12, and 13, respectively.
                SELECT len(Concept.name)
                FILTER Concept.name LIKE 'schema::%'
            );

        """, [
            {9, 1},
            {9, 1},
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_op_18(self):
        await self.assert_query_result(r"""
            SELECT _ := {1, 2, 3} IN {3, 4}
            ORDER BY _;
        """, [
            [False, False, True],
        ])

    @unittest.expectedFailure
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
            "Issue.(test::owner).name",
            "`Issue`.(`test`::`owner`).`name`",
            "Issue.(owner).(name)",
            "test::`Issue`.(`test`::`owner`).`name`",
            "Issue.((owner)).(((test::name)))",
        ]

        for case in cases:
            await self.con.execute('''
                WITH MODULE test
                SELECT
                    Issue {
                        test::number
                    }
                FILTER
                    %s = 'Elvis';
            ''' % (case,))

    @unittest.expectedFailure
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

    async def test_edgeql_expr_polymorphic_01(self):
        await self.con.execute(r"""
            WITH MODULE test
            SELECT Text {
                Issue.number,
                (Issue).related_to,
                (Issue).((`priority`)),
                test::Comment.owner: {
                    name
                }
            };
        """)

        await self.con.execute(r"""
            WITH MODULE test
            SELECT Owned {
                Named.name
            };
        """)

    async def test_edgeql_expr_cast_01(self):
        await self.assert_query_result(r"""
            SELECT <std::str>123;
            SELECT <std::int>"123";
            SELECT <std::str>123 + 'qw';
            SELECT <std::int>"123" + 9000;
            SELECT <std::int>"123" * 100;
            SELECT <std::str>(123 * 2);
            SELECT <int>true;
            SELECT <int>false;
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
                r'operator `\*` is not defined .* std::str and std::int'):

            await self.con.execute("""
                SELECT <std::str>123 * 2;
            """)

    async def test_edgeql_expr_cast_03(self):
        await self.assert_query_result(r"""
            SELECT <std::str><std::int><std::float>'123.45' + 'foo';
        """, [
            ['123foo'],
        ])

    async def test_edgeql_expr_cast_04(self):
        await self.assert_query_result(r"""
            SELECT <str><int><float>'123.45' + 'foo';
        """, [
            ['123foo'],
        ])

    async def test_edgeql_expr_cast_05(self):
        await self.assert_query_result(r"""
            SELECT <array<int>>['123', '11'];
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
                SELECT <array<int>>(123, 11);
            """)

    async def test_edgeql_expr_cast_09(self):
        await self.assert_query_result(r"""
            SELECT <tuple<str, int>> ('foo', 42);
            SELECT <tuple<a: str, b: int>> ('foo', 42);
        """, [
            [['foo', 42]],
            [{'a': 'foo', 'b': 42}],
        ])

    async def test_edgeql_expr_type_01(self):
        await self.assert_query_result(r"""
            SELECT 'foo'.__class__.name;
        """, [
            ['std::str'],
        ])

    async def test_edgeql_expr_type_02(self):
        await self.assert_query_result(r"""
            SELECT (1.0 + 2).__class__.name;
        """, [
            ['std::float'],
        ])

    async def test_edgeql_expr_set_01(self):
        await self.assert_query_result("""
            SELECT <int>{};
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
                A := (SELECT Concept FILTER Concept.name ILIKE 'schema::a%'),
                C := (SELECT Concept FILTER Concept.name ILIKE 'schema::c%'),
                D := (SELECT Concept FILTER Concept.name ILIKE 'schema::d%')
            SELECT _ := {A, D, C}.name
            ORDER BY _;
        """, [
            [
                'schema::Array', 'schema::Atom', 'schema::Attribute',
                'schema::Class', 'schema::Concept',
                'schema::ConsistencySubject', 'schema::Constraint',
                'schema::Delta', 'schema::DerivedConcept',
                'schema::DerivedLink'
            ],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_set_03(self):
        await self.assert_query_result(r"""
            # "nested" sets are merged using UNION ALL
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

            SELECT [1, 2][10] ?? 42;

            SELECT <array<int>>[];
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

            [42],

            [[]],
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
                r'could not determine type of empty collection'):

            await self.con.execute("""
                SELECT [];
            """)

    @unittest.expectedFailure
    async def test_edgeql_expr_array_05(self):
        await self.assert_query_result('''
            SELECT [1, 2] + [3, 4];
        ''', [
            [[1, 2, 3, 4]],
        ])

    async def test_edgeql_expr_map_01(self):
        await self.assert_query_result(r"""
            SELECT ['fo' + 'o' -> 42];
            SELECT <map<str,int>>['foo' -> '42'];
            SELECT <map<int,int>>['+1' -> '42'];

            SELECT <map<str,float>>['foo' -> '1.1'];
            SELECT <map<str,float>>['foo' -> '1.0'];
            SELECT <map<float,int>>['+1.5' -> '42'];

            SELECT <map<float,bool>>['+1.5' -> 42];

            SELECT ['foo' -> '42', 'bar' -> 'something'];
            SELECT [lower('FOO') -> '42', 'bar' -> 'something']['foo'];

            SELECT ['foo' -> '42', 'bar' -> 'something'][lower('FO') + 'o'];
            SELECT '+/-' + ['foo' -> '42', 'bar' -> 'something']['foo'];
            SELECT ['foo' -> 42]['foo'] + 1;

            SELECT ['a' -> <datetime>'2017-10-10']['a'] + <timedelta>'1 day';
            SELECT [100 -> 42][100];
            SELECT ['1' -> '2']['spam'] ?? 'ham';

            SELECT [ [[1],[2],[3]] -> 42] [[[1],[2],[3]]];
            SELECT [ [[1] -> 1] -> 42 ] [[[1] -> 1]];
            SELECT [[10+1 ->1] -> 100, [2 ->2] -> 200]
                    [<map<int,int>>['1'+'1' ->'1']];

            SELECT ['aaa' -> [ [['a'->1]], [['b'->2]], [['c'->3]] ] ];

            SELECT <map<int, int>>[];
        """, [
            [{'foo': 42}],
            [{'foo': 42}],
            [{'1': 42}],

            [{'foo': 1.1}],
            [{'foo': 1.0}],
            [{'1.5': 42}],

            [{'1.5': True}],

            [{'foo': '42', 'bar': 'something'}],
            ['42'],

            ['42'],
            ['+/-42'],
            [43],

            ['2017-10-11T00:00:00+00:00'],
            [42],
            ['ham'],

            [42],
            [42],
            [100],

            [{'aaa': [[{'a': 1}], [{'b': 2}], [{'c': 3}]]}],

            [{}]
        ])

    async def test_edgeql_expr_map_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'could not determine map values type'):

            await self.con.execute(r'''
                SELECT ['a' -> 'b', '1' -> 1];
            ''')

        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'binary operator `\+` is not defined.*str.*int'):

            await self.con.execute(r'''
                SELECT ['a' -> '1']['a'] + 1;
            ''')

    async def test_edgeql_expr_map_03(self):
        await self.con.execute('''
            CREATE FUNCTION test::take(map<std::str, std::int>, std::str)
                    -> std::int
                FROM EdgeQL $$
                    SELECT $1[$2] + 100
                $$;

            CREATE FUNCTION test::make(std::int)
                    -> map<std::str, std::int>
                FROM EdgeQL $$
                    SELECT ['aaa' -> $1]
                $$;
        ''')

        await self.assert_query_result(r"""
            SELECT test::take(['foo' -> 42], 'foo') + 1;
            SELECT test::make(1000)['aaa'] + 8000;
        """, [
            [143],
            [9000],
        ])

    async def test_edgeql_expr_map_04(self):
        await self.assert_query_result(r"""
            SELECT <map<str, datetime>>['foo' -> '2020-10-10'];
            SELECT (<map<int,int>>['+1' -> '+42'])[1];  # '+1'::bigint = 1
            SELECT (<map<datetime, datetime>>['2020-10-10' -> '2010-01-01'])
                   [<datetime>'2020-10-10'];
            SELECT (<map<int,int>>[true -> '+42'])[1];
            SELECT (<map<bool,int>>(<map<int,str>>[true -> 142]))[true];
        """, [
            [{'foo': '2020-10-10T00:00:00+00:00'}],
            [42],
            ['2010-01-01T00:00:00+00:00'],
            [42],
            [142],
        ])

        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index map.*by.*str.*int.*expected'):

            await self.con.execute(r'''
                SELECT [1 -> 1]['1'];
            ''')

    async def test_edgeql_expr_map_05(self):
        await self.assert_query_result(r"""
            SELECT [1 -> [ [[1]], [[-2]], [[3]] ] ]   [1];
            SELECT [1 -> [ [[true]], [[false]], [[true]] ] ]   [1];
            SELECT [1 -> [ [[1.1]], [[-2.2]], [[3.3]] ] ]   [1];
            SELECT [1 -> [ [['aa']], [['bb']], [['cc']] ] ]   [1];
            SELECT [1 -> [ [['aa'->1]], [['bb'->2]], [['cc'->3]] ] ]   [1];
            SELECT [1 -> ['a'->[1,2], 'b'->[1,3]]] [1];
            SELECT [1 -> ['a'->[['x'->10]], 'b'->[['y'->20]]]] [1];
            SELECT [1 -> ['a'->[['x'->10]],'b'->[['y'->20]]]] [1]['a'][0]['x'];
        """, [
            [[[[1]], [[-2]], [[3]]]],
            [[[[True]], [[False]], [[True]]]],
            [[[[1.1]], [[-2.2]], [[3.3]]]],
            [[[['aa']], [['bb']], [['cc']]]],
            [[[{'aa': 1}], [{'bb': 2}], [{'cc': 3}]]],
            [{'a': [1, 2], 'b': [1, 3]}],
            [{'a': [{'x': 10}], 'b': [{'y': 20}]}],
            [10],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_map_06(self):
        await self.assert_query_result(r"""
            SELECT [1 -> [ [[1]], [[-2]], [[3]] ] ]   [1][0];
            SELECT [1 -> [ [[1]], [[-2]], [[3]] ] ]   [1][0][0];
            SELECT [1 -> [ [[1]], [[-2]], [[3]] ] ]   [1][0][0][0];
        """, [
            [[[1]]],
            [[1]],
            [1],
        ])

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

            SELECT -<int>{} ?? 2;
            SELECT -<int>{} ?? -2 + 1;

            SELECT <int>({} ?? {});
            SELECT <int>({} ?? {} ?? {});
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
        ])

    async def test_edgeql_expr_string_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index string by.*str'):

            await self.con.execute("""
                SELECT '123'['1'];
            """)

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

    @tb.expected_optimizer_failure
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
                r'operator `\+` is not defined .* tuple<.*> and std::int'):

            await self.con.execute(r'''
                SELECT (spam := 1, ham := 2) + 1;
            ''')

    @tb.expected_optimizer_failure
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
            SELECT (1, 2) UNION ALL (1, 2);
        ''', [
            [True],
            [[1, 2]],
            [[1, 2], [1, 2]],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_tuple_12(self):
        await self.assert_query_result(r'''
            WITH A := {1, 2, 3}
            SELECT _ := ({'a', 'b'}, A)
            ORDER BY _;
        ''', [
            [['a', 1], ['a', 2], ['a', 3], ['b', 1], ['b', 2], ['b', 3]],
        ])

    @unittest.expectedFailure
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

    @unittest.expectedFailure
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

    @unittest.expectedFailure
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

    @tb.expected_optimizer_failure
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

    @unittest.expectedFailure
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

    async def test_edgeql_expr_cannot_assign_dunder_class(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot assign to __class__'):
            await self.con.execute(r"""
                SELECT test::Text {
                    std::__class__ := 42
                };
            """)

    async def test_edgeql_expr_if_else_01(self):
        await self.assert_query_result(r"""
            SELECT 'yes' IF 1=1 ELSE 'no';
            SELECT 'yes' IF 1=0 ELSE 'no';
            SELECT 's1' IF 1=0 ELSE 's2' IF 2=2 ELSE 's3';
        """, [
            ['yes'],
            ['no'],
            ['s2'],
        ])

    async def test_edgeql_expr_setop_01(self):
        await self.assert_query_result(r"""
            SELECT EXISTS {};
            SELECT NOT EXISTS {};
        """, [
            [False],
            [True],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_setop_02(self):
        await self.assert_query_result(r"""
            SELECT 2 * ((SELECT 1) UNION (SELECT 2));
            SELECT (SELECT 2) * (1 UNION 2);
            SELECT 2 * (1 UNION 2 UNION 1);
            SELECT 2 * (1 UNION ALL 2 UNION ALL 1);

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
            SELECT array_agg(1 UNION ALL 2 UNION ALL 3);
            SELECT array_agg(3 UNION ALL 2 UNION ALL 3);
            SELECT array_agg(3 UNION ALL 3 UNION ALL 2);
        ''', [
            [[1, 2, 3]],
            [[3, 2, 3]],
            [[3, 3, 2]],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_setop_04(self):
        await self.assert_query_result('''
            SELECT DISTINCT {1, 2, 2, 3};
        ''', [
            {1, 2, 3},
        ])

    async def test_edgeql_expr_setop_05(self):
        await self.assert_query_result('''
            SELECT (2 UNION ALL 2 UNION ALL 2);
        ''', [
            [2, 2, 2],
        ])

    async def test_edgeql_expr_setop_06(self):
        await self.assert_query_result('''
            SELECT (2 UNION 2 UNION 2);
        ''', [
            [2],
        ])

    async def test_edgeql_expr_setop_07(self):
        await self.assert_query_result('''
            SELECT (2 UNION ALL 2 UNION 2);
            SELECT (2 UNION 2 UNION ALL 2);
            SELECT (2 UNION ALL 1 UNION 2);
        ''', [
            [2],
            [2, 2],
            {1, 2},
        ])

    async def test_edgeql_expr_setop_08(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                'invalid UNION ALL operand: schema::Concept is a concept'):
            await self.con.execute('''
                WITH MODULE schema
                SELECT Concept UNION ALL Attribute;
            ''')

    @unittest.expectedFailure
    async def test_edgeql_expr_setop_09(self):
        res = await self.con.execute('''
            SELECT DISTINCT {[1, 2], [1, 2], [2, 3]};
        ''')
        self.assert_data_shape(res, [
            {[1, 2], [2, 3]},
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_setop_10(self):
        res = await self.con.execute('''
            SELECT DISTINCT {(1, 2), (2, 3), (1, 2)};
            SELECT DISTINCT {(a := 1, b := 2),
                             (a := 2, b := 3),
                             (a := 1, b := 2)};
        ''')
        self.assert_data_shape(res, [
            {[1, 2], [2, 3]},
            {{'a': 1, 'b': 2}, {'a': 2, 'b': 3}},
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_setop_11(self):
        res = await self.con.execute('''
            WITH
                MODULE schema,
                C := (SELECT Concept FILTER Concept.name LIKE 'schema::%')
            SELECT _ := len(C.name)
            ORDER BY _;

            WITH
                MODULE schema,
                C := (SELECT Concept FILTER Concept.name LIKE 'schema::%')
            SELECT _ := DISTINCT len(C.name)
            ORDER BY _;
        ''')

        # test the results of DISTINCT directly, rather than relying
        # on an aggregate function
        self.assertEqual(
            len(res[0]) > len(res[1]),
            'DISTINCT len(Concept.name) failed to filter out dupplicates')

    async def test_edgeql_expr_cardinality_01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=44):

            await self.query('''\
                WITH MODULE test
                SELECT Issue.name ORDER BY Issue.watchers.name;
            ''')

    async def test_edgeql_expr_cardinality_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=30):

            await self.query('''\
                WITH MODULE test
                SELECT Issue LIMIT User.name;
            ''')

    async def test_edgeql_expr_cardinality_03(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=30):

            await self.query('''\
                WITH MODULE test
                SELECT Issue OFFSET User.name;
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

    @unittest.expectedFailure
    async def test_edgeql_expr_cardinality_08(self):
        # we expect some sort of error because the set {1, 2} is not a
        # SINGLETON
        with self.assertRaisesRegex(exc.UnknownEdgeDBError, ''):
            await self.query(r'''
                WITH MODULE test
                SELECT SINGLETON {1, 2};
            ''')

    async def test_edgeql_expr_type_filter_01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'invalid type filter operand: std::int is not a concept',
                position=7):

            await self.query('''\
                SELECT 10[IS std::Object];
            ''')

    async def test_edgeql_expr_type_filter_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'invalid type filter operand: std::str is not a concept',
                position=17):

            await self.query('''\
                SELECT Object[IS str];
            ''')

    async def test_edgeql_expr_type_filter_03(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'invalid type filter operand: std::uuid is not a concept',
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

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_alias_01(self):
        await self.assert_query_result(r"""
            WITH
                a := {1, 2},
                b := {2, 3}
            SELECT a
            FILTER a = b;
        """, [
            [2],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_alias_02(self):
        await self.assert_query_result(r"""
            WITH
                b := {2, 3}
            SELECT a := {1, 2}
            FILTER a = b;
        """, [
            [2],
        ])

    async def test_edgeql_expr_alias_03(self):
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

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_alias_04(self):
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

    async def test_edgeql_expr_alias_05(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT Concept {
                name,
                foo := (
                    WITH a := {1, 2}
                    SELECT a
                )
            }
            ORDER BY .name LIMIT 1;
        """, [
            [{'name': 'schema::Array', 'foo': {1, 2}}],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_alias_06(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT Concept {
                name,
                foo := (
                    WITH a := {1, 2}
                    SELECT a
                    FILTER a < 2
                )
            }
            ORDER BY .name LIMIT 1;
        """, [
            [{'name': 'schema::Array', 'foo': {1}}],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_alias_07(self):
        await self.assert_query_result(r"""
            # test variable masking
            WITH x := (
                WITH x := {2, 3} SELECT {4, 5, x}
            )
            SELECT x ORDER BY x;
        """, [
            [2, 3, 4, 5],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_alias_08(self):
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

    # TODO: this test indicates that the current semantics of FOR
    # needs a review due to odd interpretation of what the ORDER
    # clause actually applies to (similar to dangling else problem).
    #
    # See also test_edgeql_select_for_02.
    @tb.expected_optimizer_failure
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

    @unittest.expectedFailure
    async def test_edgeql_expr_for_02(self):
        await self.assert_query_result(r"""
            FOR x IN {2, 3}
            UNION {x, x + 2};
        """, [
            {2, 3, 4, 5},
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_forgroup_01(self):
        await self.assert_query_result(r"""
            WITH I := {1, 2, 3, 4}
            GROUP I := I
            USING _ := I % 2 = 0
            BY _
            UNION _r := (
                values := array_agg(I ORDER BY I)
            ) ORDER BY _r.values;
        """, [
            [
                {'values': [1, 3]},
                {'values': [2, 4]}
            ]
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_forgroup_02(self):
        await self.assert_sorted_query_result(r'''
            # handle a number of different aliases
            WITH x := {(1, 2), (3, 4), (4, 2)}
            GROUP y := x
            USING _ := y.1
            BY _
            UNION array_agg(y.0);
        ''', lambda x: x, [
            [[1, 4], [3]],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_forgroup_03(self):
        await self.assert_sorted_query_result(r'''
            WITH x := {(1, 2), (3, 4), (4, 2)}
            GROUP x := x
            USING _ := x.1
            BY _
            UNION array_agg(x.0);
        ''', lambda x: x, [
            [[1, 4], [3]],
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_expr_forgroup_04(self):
        await self.assert_query_result(r'''
            WITH x := {(1, 2), (3, 4), (4, 2)}
            GROUP x := x
            USING B := x.1
            BY B
            UNION (B, array_agg(x.0))
            ORDER BY
                B;
        ''', [
            [[2, [1, 4]], [4, [3]]],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_forgroup_05(self):
        await self.assert_query_result(r'''
            # handle the case where the value to be computed depends
            # on both, the grouped subset and the original set
            WITH
                x1 := {(1, 0), (1, 0), (1, 0), (2, 0), (3, 0), (3, 0)},
                x2 := x1
            GROUP y := x1
            USING z := y.0
            BY z
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
    async def test_edgeql_expr_forgroup_06(self):
        await self.assert_query_result(r'''
            GROUP X := {1, 1, 1, 2, 3, 3}
            USING y := X
            BY y
            UNION (y, count(X))
            ORDER BY y;
        ''', [
            [[1, 3], [2, 1], [3, 2]]
        ])
