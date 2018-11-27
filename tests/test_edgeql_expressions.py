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

import edgedb

from edb.server import _testbase as tb
from edb.tools import test


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
            SELECT <int64>{} + 1;
            SELECT 1 + <int64>{};
        """, [
            [],
            [],
            [],
            [],
        ])

        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'could not determine expression type'):

            await self.query("""
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

        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'could not determine expression type'):

            await self.query("""
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
            SELECT 'foo' ++ 'bar';
        """, [
            ['foobar'],
        ])

    async def test_edgeql_expr_op_06(self):
        await self.assert_query_result(r"""
            SELECT <int64>{} = <int64>{};
            SELECT <int64>{} = 42;
        """, [
            [],
            [],
        ])

    async def test_edgeql_expr_op_07(self):
        # Test boolean interaction with {}
        await self.assert_query_result(r"""
            SELECT TRUE OR <bool>{};
            SELECT FALSE AND <bool>{};
        """, [
            [],
            [],
        ])

    async def test_edgeql_expr_op_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '-' cannot .* 'std::str'"):

            await self.query("""
                SELECT -'aaa';
            """)

    async def test_edgeql_expr_op_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator 'NOT' cannot .* 'std::str'"):

            await self.query("""
                SELECT NOT 'aaa';
            """)

    async def test_edgeql_expr_op_10(self):
        await self.assert_query_result(r"""
            # the types are put in to satisfy type inference
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

            SELECT 2 ?= <int64>{};
            SELECT <int64>{} ?= <int64>{};
            SELECT 2 ?!= <int64>{};
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
            SELECT 1 IN <int64>{};
            SELECT {1, 2, 3} IN <int64>{};

            SELECT 1 NOT IN <int64>{};
            SELECT {1, 2, 3} NOT IN <int64>{};
        """, [
            [False],
            [False, False, False],
            [True],
            [True, True, True],
        ])

    async def test_edgeql_expr_op_20(self):
        # Test that power applied to int64 is producing a float64 even
        # in the underlying implementation.
        await self.assert_query_result(r"""
            # use of floor(random()) is to prevent constant folding
            # optimizations
            SELECT (10 + math::floor(random()))^308;
            SELECT (10 + math::floor(random()))^308 = 1e308;
        """, [
            # FIXME: due to limitations of the Python [test] driver,
            # we get a Python 'int' back, instead of the 'float' and
            # the 'int' is not going to be equal to 1e308.
            #
            # If the driver cast the result into a 'float', then the
            # correct answer in Python would be 1e308.
            [10**308],
            [True],
        ])

        # overflow is expected for float64, but would not happen for decimal
        with self.assertRaisesRegex(edgedb.NumericOutOfRangeError, 'overflow'):
            await self.query(r"""
                SELECT (10 + math::floor(random()))^309;
            """)

    async def _test_boolop(self, left, right, op, not_op, result):
        if isinstance(result, bool):
            # this operation should be valid and produce opposite
            # results for op and not_op
            await self.assert_query_result(
                f"""SELECT {left} {op} {right};""", [{result}])
            await self.assert_query_result(
                f"""SELECT {left} {not_op} {right};""", [{not result}])
        else:
            # operation is expected to be invalid
            for binop in [op, not_op]:
                query = f"""SELECT {left} {binop} {right};"""
                with self.assertRaisesRegex(edgedb.QueryError, result,
                                            msg=query):
                    await self.query(query)

    # type casts for literals help recovering type info for tests
    unorderable = [
        '<bool>True',
        '<uuid>"d4288330-eea3-11e8-bc5f-7faf132b1d84"',
        '<bytes>b"Hello"',
        '<json>"Hello"',
    ]
    onlyorderable = [
        '<str>"Hello"',
    ]
    datetime = [
        '<datetime>"2018-05-07T20:01:22.306916+00:00"',
        '<naive_datetime>"2018-05-07T20:01:22.306916"',
        '<naive_date>"2018-05-07"',
        '<naive_time>"20:01:22.306916"',
        '<timedelta>"20:01:22.306916"',
    ]
    numeric = [
        '<int16>1',
        '<int32>1',
        '<int64>1',
        '<float32>1',
        '<float64>1',
        '<decimal>1',
    ]
    orderable = onlyorderable + datetime + numeric
    orderable_nonnumeric = onlyorderable + datetime
    nonnumeric = unorderable + onlyorderable + datetime
    scalar_samples = unorderable + orderable

    async def test_edgeql_expr_valid_eq_01(self):
        # compare all numerics to all other scalars via equality
        for left in self.numeric:
            for right in self.nonnumeric:
                for op, not_op in [('=', '!='), ('?=', '?!=')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        'cannot be applied to operands'
                    )

    async def test_edgeql_expr_valid_eq_02(self):
        # compare all numerics to each other via equality
        for left in self.numeric:
            for right in self.numeric:
                for op, not_op in [('=', '!='), ('?=', '?!=')]:
                    await self._test_boolop(
                        left, right, op, not_op, True
                    )

    async def test_edgeql_expr_valid_eq_03(self):
        expected_error_msg = 'cannot be applied to operands'
        # compare all non-numerics to all scalars via equality
        for left in self.nonnumeric:
            for right in self.scalar_samples:
                for op, not_op in [('=', '!='), ('?=', '?!=')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if left == right else expected_error_msg
                    )

    async def test_edgeql_expr_valid_comp_01(self):
        # compare all unorderable scalars to every scalar via ordering operator
        for left in self.unorderable:
            for right in self.scalar_samples:
                for op, not_op in [('>=', '<'), ('<=', '>')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        'cannot be applied to operands'
                    )

    async def test_edgeql_expr_valid_comp_02(self):
        expected_error_msg = 'cannot be applied to operands'
        # compare all orderable non-numerics to all scalars via
        # ordering operator
        for left in self.orderable_nonnumeric:
            for right in self.scalar_samples:
                for op, not_op in [('>=', '<'), ('<=', '>')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if left == right else expected_error_msg
                    )

    async def test_edgeql_expr_valid_comp_03(self):
        expected_error_msg = 'cannot be applied to operands'
        # compare numerics to all scalars via ordering comparators
        for left in self.numeric:
            for right in self.scalar_samples:
                for op, not_op in [('>=', '<'), ('<=', '>')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if right.endswith('>1') else expected_error_msg
                    )

    async def test_edgeql_expr_valid_bool_01(self):
        expected_error_msg = 'cannot be applied to operands'
        # use every scalar combination with AND and OR
        for left in self.scalar_samples:
            for right in self.scalar_samples:
                for op in ['AND', 'OR']:
                    query = f"""SELECT {left} {op} {right};"""
                    if left == right == '<bool>True':
                        # this operation should be valid and True
                        await self.assert_query_result(query, [{True}])
                    else:
                        # every combination except for bool OP bool is invalid
                        with self.assertRaisesRegex(edgedb.QueryError,
                                                    expected_error_msg,
                                                    msg=query):
                            await self.query(query)

    async def test_edgeql_expr_valid_bool_02(self):
        expected_error_msg = 'cannot be applied to operands'
        # use every scalar with NOT
        for right in self.scalar_samples:
            query = f"""SELECT NOT {right};"""
            if right == '<bool>True':
                # this operation should be valid and False
                await self.assert_query_result(query, [{False}])
            else:
                # every other scalar must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    await self.query(query)

    async def test_edgeql_expr_valid_setbool_01(self):
        # Use scalar combinations with IN and NOT IN. The expressions
        # are trivial and are equivalent to = and != so there's a
        # one-to-one correspondence between these and "expr_eq" tests.
        for left in self.numeric:
            for right in self.nonnumeric:
                for op, not_op in [('IN', 'NOT IN')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        'cannot be applied to operands'
                    )

    async def test_edgeql_expr_valid_setbool_02(self):
        # Use scalar combinations with IN and NOT IN. The expressions
        # are trivial and are equivalent to = and != so there's a
        # one-to-one correspondence between these and "expr_eq" tests.
        for left in self.numeric:
            for right in self.numeric:
                for op, not_op in [('IN', 'NOT IN')]:
                    await self._test_boolop(
                        left, right, op, not_op, True
                    )

    async def test_edgeql_expr_valid_setbool_03(self):
        expected_error_msg = 'cannot be applied to operands'
        # Use scalar combinations with IN and NOT IN. The expressions
        # are trivial and are equivalent to = and != so there's a
        # one-to-one correspondence between these and "expr_eq" tests.
        for left in self.nonnumeric:
            for right in self.scalar_samples:
                for op, not_op in [('IN', 'NOT IN')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if left == right else expected_error_msg
                    )

    async def test_edgeql_expr_valid_setbool_04(self):
        # use every scalar with EXISTS
        for right in self.scalar_samples:
            query = f"""SELECT EXISTS {right};"""
            # this operation should always be valid and True
            await self.assert_query_result(query, [{True}])

    async def test_edgeql_expr_valid_setop_01(self):
        # use every scalar with DISTINCT
        for right in self.scalar_samples:
            query = f"""SELECT count(DISTINCT {{{right}, {right}}});"""
            # this operation should always be valid and get count of 1
            await self.assert_query_result(query, [{1}])

            rtype = right.split('>')[0][1:]
            query = f"""SELECT (DISTINCT {{{right}, {right}}}) IS {rtype};"""
            # this operation should always be valid
            await self.assert_query_result(query, [{True}])

    async def test_edgeql_expr_valid_setop_02(self):
        expected_error_msg = 'could not determine expression type'
        # UNION all non-decimal numerics with all other scalars
        for left in self.numeric[:-1]:
            for right in self.nonnumeric:
                query = f"""SELECT {left} UNION {right};"""
                # every combination must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    await self.query(query)

    async def test_edgeql_expr_valid_setop_03(self):
        # UNION all non-decimal numerics with each other
        for left in self.numeric[:-1]:
            for right in self.numeric[:-1]:
                query = f"""SELECT {left} UNION {right};"""
                # every combination must be valid and be {1, 1}
                await self.assert_query_result(query, [[1, 1]])

                types = [left.split('>')[0][1:], right.split('>')[0][1:]]
                types.sort()

                # Two floats upcast to the longer float.
                # Two ints upcast to the longer int.
                #
                # int16 and float32 upcast to float32, all other
                # float and int combos upcast to float64.
                if types[0][0] == types[1][0]:
                    # same category, so we just pick the longer one
                    rtype = types[1]
                else:
                    # it's a mix, so check if it's special
                    if types == ['float32', 'int16']:
                        rtype = 'float32'
                    else:
                        rtype = 'float64'

                query = f"""SELECT ({left} UNION {right}) IS {rtype};"""
                # this operation should always be valid
                await self.assert_query_result(query, [{True}])

    async def test_edgeql_expr_valid_setop_04(self):
        expected_error_msg = 'could not determine expression type'
        # UNION all non-numerics with all scalars
        for left in self.nonnumeric:
            for right in self.scalar_samples:
                query = f"""SELECT count({left} UNION {right});"""

                if left == right:
                    # these scalars can only be UNIONed with
                    # themselves implicitly
                    await self.assert_query_result(query, [[2]])

                    rtype = right.split('>')[0][1:]
                    query = f"""SELECT ({left} UNION {right}) IS {rtype};"""
                    # this operation should always be valid
                    await self.assert_query_result(query, [{True}])

                else:
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_setop_05(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = 'could not determine expression type'
        # decimal UNION non-numerics
        for left in self.numeric[-1:]:
            for right in self.nonnumeric:
                query = f"""SELECT {left} UNION {right};"""
                # every combination must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    await self.query(query)

    async def test_edgeql_expr_valid_setop_06(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = 'could not determine expression type'
        # decimal UNION numerics
        for left in self.numeric[-1:]:
            for right in self.numeric:
                query = f"""SELECT count({left} UNION {right});"""
                if right.startswith('<float'):
                    # decimal UNION float is illegal
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)
                else:
                    # decimals and integers can be UNIONed in any
                    # combination
                    await self.assert_query_result(query, [[2]])

                    query = f"""SELECT ({left} UNION {right}) IS decimal;"""
                    # this operation should always be valid
                    await self.assert_query_result(query, [{True}])

    async def test_edgeql_expr_valid_setop_07(self):
        expected_error_msg = 'condition must be of type std::bool'
        # IF ELSE with every scalar as the condition
        for val in self.scalar_samples:
            query = f"""SELECT 1 IF {val} ELSE 2;"""
            if val == '<bool>True':
                await self.assert_query_result(query, [[1]])
            else:
                # every other combination must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    await self.query(query)

    # Operator '??' should work just like UNION in terms of types.
    # Operator A IF C ELSE B should work exactly like A UNION B in
    # terms of types.
    async def test_edgeql_expr_valid_setop_08(self):
        expected_error_msg = 'of related types'
        # test all non-decimal numerics with all other scalars
        for left in self.numeric[:-1]:
            for right in self.nonnumeric:
                # random is used in the IF to prevent constant
                # folding, because we really care about both of the
                # outer operands being processed
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_setop_09(self):
        # test all non-decimal numerics with each other
        for left in self.numeric[:-1]:
            for right in self.numeric[:-1]:
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every combination must be valid and be 1
                    await self.assert_query_result(query, [[1]])

                    types = [left.split('>')[0][1:], right.split('>')[0][1:]]
                    types.sort()

                    # Two floats upcast to the longer float.
                    # Two ints upcast to the longer int.
                    #
                    # int16 and float32 upcast to float32, all other
                    # float and int combos upcast to float64.
                    if types[0][0] == types[1][0]:
                        # same category, so we just pick the longer one
                        rtype = types[1]
                    else:
                        # it's a mix, so check if it's special
                        if types == ['float32', 'int16']:
                            rtype = 'float32'
                        else:
                            rtype = 'float64'

                    query = f"""SELECT ({left} {op} {right}) IS {rtype};"""
                    # this operation should always be valid
                    await self.assert_query_result(query, [{True}])

    async def test_edgeql_expr_valid_setop_10(self):
        expected_error_msg = 'of related types'
        # test all non-numerics with all scalars
        for left in self.nonnumeric:
            for right in self.scalar_samples:
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT count({left} {op} {right});"""

                    if left == right:
                        # these scalars can only be combined with
                        # themselves implicitly
                        await self.assert_query_result(query, [[1]])

                        rtype = right.split('>')[0][1:]
                        query = f"""SELECT ({left} {op} {right}) IS {rtype};"""
                        # this operation should always be valid
                        await self.assert_query_result(query, [{True}])

                    else:
                        # every other combination must produce an error
                        with self.assertRaisesRegex(edgedb.QueryError,
                                                    expected_error_msg,
                                                    msg=query):
                            await self.query(query)

    async def test_edgeql_expr_valid_setop_11(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = 'of related types'
        # decimal combined with non-numerics
        for left in self.numeric[-1:]:
            for right in self.nonnumeric:
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_setop_12(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = 'of related types'
        # decimal combined with numerics
        for left in self.numeric[-1:]:
            for right in self.numeric:
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    if right.startswith('<float'):
                        # decimal combined with float is illegal
                        with self.assertRaisesRegex(edgedb.QueryError,
                                                    expected_error_msg,
                                                    msg=query):
                            await self.query(query)
                    else:
                        # decimals and integers can be UNIONed in any
                        # combination
                        await self.assert_query_result(query, [[1]])

                        query = f"""SELECT ({left} {op} {right}) IS decimal;"""
                        # this operation should always be valid
                        await self.assert_query_result(query, [{True}])

    async def test_edgeql_expr_valid_arithmetic_01(self):
        scalars = self.scalar_samples
        # unary minus should work for numeric scalars and timedelta
        for right in scalars[-7:]:
            query = f"""SELECT count(-{right});"""
            await self.assert_query_result(query, [[1]])

    async def test_edgeql_expr_valid_arithmetic_02(self):
        scalars = self.scalar_samples
        expected_error_msg = 'cannot be applied to operands'
        # unary minus should not work for other scalars
        for right in scalars[:-7]:
            query = f"""SELECT -{right};"""
            with self.assertRaisesRegex(edgedb.QueryError,
                                        expected_error_msg,
                                        msg=query):
                await self.query(query)

    # NOTE: Generalized Binop `+` and `-` rules:
    #
    # 1) There are some scalars that simply don't support these operators
    #    at all.
    #
    # 2) Date/time scalars support `+` if one of the operands is
    #    `timedelta`. The result is always of the type of the other
    #    operand.
    #
    # 3) Date/time scalars support `-` when the right operand is
    #    `timedelta`. The result is always of the type of the first
    #    operand. Technically this is dictated by the equivalence of
    #    A - B and A + (-B).
    #
    # 4) Date/time scalars support `-` when both operands are of the
    #    same type. The result is always `timedelta`.
    #
    # 5) Numeric scalars support `+` and `-` iff it is possible to
    #    implicitly cast one operand into the other. The result is
    #    always of the type of this implicit cast. This makes
    #    `decimal` basically incompatible with float types without an
    #    explicit cast. As far as types are concerned the operators
    #    `+` and `-` are "commutative".
    async def test_edgeql_expr_valid_arithmetic_03(self):
        scalars = self.scalar_samples
        expected_error_msg = 'cannot be applied to operands'
        # Test (1) - scalars that don't support + and - at all.
        #
        # Incidentally, this also implies that they don't support '*',
        # '/', '//', '%', '^' as these are derived or defined from `+`
        # and `-`.
        #
        # In particular, `str` and `bytes` don't support `*` with
        # meaning of "repeated concatenation".
        for left in scalars[:5]:
            for right in self.scalar_samples:
                for op in ['+', '-', '*', '/', '//', '%', '^']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_arithmetic_04(self):
        # Tests (2), (3), (4) - various date/time with non-date/time
        # combinations.
        dts = self.datetime
        expected_error_msg = 'cannot be applied to operands'

        # none of the date/time scalars support '+', '-', '*', '/',
        # '//', '%', '^' with non-date/time
        for left in dts:
            for right in self.scalar_samples:
                if right in dts:
                    continue
                for op in ['+', '-', '*', '/', '//', '%', '^']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_arithmetic_05(self):
        # Tests (2) - various date/time combinations.
        dts = self.datetime
        tdelta = dts[-1]
        expected_error_msg = 'cannot be applied to operands'

        for left in dts:
            for right in dts:
                query = f"""SELECT count({left} + {right});"""
                restype = None

                if left == tdelta:
                    restype = right[1:].split('>')[0]
                elif right == tdelta:
                    restype = left[1:].split('>')[0]

                if restype:
                    await self.assert_query_result(query, [[1]])
                    await self.assert_query_result(
                        f"""SELECT ({left} + {right}) IS {restype};""",
                        [[True]])
                else:
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_arithmetic_06(self):
        # Tests (3), (4) - various date/time combinations.
        dts = self.datetime
        tdelta = dts[-1]
        expected_error_msg = 'cannot be applied to operands'

        for left in dts:
            for right in dts:
                query = f"""SELECT count({left} - {right});"""
                restype = None

                if right == tdelta:
                    restype = left[1:].split('>')[0]
                elif right == left:
                    restype = 'timedelta'

                if restype:
                    await self.assert_query_result(query, [[1]])
                    await self.assert_query_result(
                        f"""SELECT ({left} - {right}) IS {restype};""",
                        [[True]])
                else:
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_arithmetic_07(self):
        # various date/time combinations don't define '*', '/', '//',
        # '%', '^'.
        dts = self.datetime
        expected_error_msg = 'cannot be applied to operands'

        for left in dts:
            for right in dts:
                for op in ['*', '/', '//', '%', '^']:
                    query = f"""SELECT count({left} {op} {right});"""
                    # every combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_arithmetic_08(self):
        # Test (5) - decimal is incompatible with everything except integers
        expected_error_msg = 'cannot be applied to operands'

        left = self.numeric[-1]
        for right in self.scalar_samples[:-1]:
            for op in ['+', '-', '*', '/', '//', '%', '^']:
                if right.startswith('<int'):
                    # decimal is "contagious"
                    await self.assert_query_result(
                        f"""SELECT ({left} {op} {right}) IS decimal;""",
                        [[True]])
                else:
                    # every other combination must produce an error
                    query = f"""SELECT {left} {op} {right};"""
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        await self.query(query)

    async def test_edgeql_expr_valid_arithmetic_09(self):
        # Test (5) '+', '-', '*' for non-decimals. These operators are
        # expected to work and have the result of the same type as the
        # implicit cast type of the 2 operands. In a very fundamental
        # way they are based on +.
        #
        # The '//' and '%' operators treats types the same way as '*'
        # largely because the result type doesn't ever have to change
        # to float unless the operands are already floats. Basically
        # the result is always smaller in magnitude than either
        # operand and can be represented in terms of operand types.

        for left in self.numeric[:-1]:
            for right in self.numeric[:-1]:
                for op in ['+', '-', '*', '//', '%']:
                    types = [left.split('>')[0][1:], right.split('>')[0][1:]]
                    types.sort()

                    # Two floats upcast to the longer float.
                    # Two ints upcast to the longer int.
                    #
                    # int16 and float32 upcast to float32, all other
                    # float and int combos upcast to float64.
                    if types[0][0] == types[1][0]:
                        # same category, so we just pick the longer one
                        rtype = types[1]
                    else:
                        # it's a mix, so check if it's special
                        if types == ['float32', 'int16']:
                            rtype = 'float32'
                        else:
                            rtype = 'float64'

                    query = f"""SELECT ({left} {op} {right}) IS {rtype};"""
                    await self.assert_query_result(query, [[True]], msg=query)

    async def test_edgeql_expr_valid_arithmetic_10(self):
        # Test (5) '/', '^' for non-decimals.

        for left in self.numeric[:-1]:
            for right in self.numeric[:-1]:
                for op in ['/', '^']:
                    # The result type is always a float because power
                    # can act as division.
                    #
                    # If the operands are int16 or float32, then the
                    # result is float32.
                    # In all other cases the result is a float64.
                    types = {left.split('>')[0][1:], right.split('>')[0][1:]}

                    if types.issubset({'int16', 'float32'}):
                        rtype = 'float32'
                    else:
                        rtype = 'float64'

                    query = f"""SELECT ({left} {op} {right}) IS {rtype};"""
                    await self.assert_query_result(query, [[True]], msg=query)

    @test.xfail('''
        The result of aggregate `sum` is not cast back into the same
        type as the argument set.

        Until this validation is added "fixing" it in std is going to
        mask the issue.
    ''')
    async def test_edgeql_expr_valid_arithmetic_11(self):
        # A special test for type of 'sum'. In principle the type of
        # the result of aggregate sum should not be different from the
        # '+' operator:
        #
        #   SELECT sum({1, 2.2, 3});
        #   SELECT 1 + 2.2 + 3;
        #
        # Both of the above should produce the same type of result and
        # ideally the same value. Although, it is possible to get
        # different value due to rounding errors with floats.
        #
        # This is even more obvious for type of `sum(a)` vs type of `a`.
        #
        # We trust that the implicit casts for the set work as tested
        # by UNION so we only test that a sum of a singleton set
        # preserves the type.
        expected_error_msg = 'could not find a function variant sum'

        for val in self.scalar_samples:
            if val in self.numeric:
                rtype = val.split('>')[0][1:]
                query = f"""SELECT sum({val}) IS {rtype};"""
                await self.assert_query_result(query, [[True]], msg=query)
            else:
                query = f"""SELECT sum({val});"""
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    await self.query(query)

    async def test_edgeql_expr_bytes_op_01(self):
        await self.assert_query_result(r'''
            SELECT len(b'123' ++ b'54');
        ''', [
            [5]
        ])

    async def test_edgeql_expr_bytes_op_02(self):
        await self.assert_query_result(r'''
            SELECT (b'123' ++ b'54')[-1] = b'4';
            SELECT (b'123' ++ b'54')[0:2] = b'12';
        ''', [
            [True],
            [True],
        ])

    async def test_edgeql_expr_paths_01(self):
        cases = [
            "Issue.owner.name",
            "`Issue`.`owner`.`name`",
        ]

        for case in cases:
            await self.query('''
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
                edgedb.QueryError, r'could not resolve partial path'):
            await self.query(r"""
                SELECT .1;
            """)

    async def test_edgeql_expr_paths_04(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.query(r"""
                WITH MODULE test
                SELECT Issue.owner
                FILTER Issue.number > '2';
            """)

    async def test_edgeql_expr_paths_05(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.id` which is defined in an outer
        # scope.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.query(r"""
                WITH MODULE test
                SELECT Issue.id
                FILTER Issue.number > '2';
            """)

    async def test_edgeql_expr_paths_06(self):
        # `Issue.number` in the shape is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.query(r"""
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
                edgedb.QueryError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.query(r"""
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
                edgedb.QueryError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.query(r"""
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
                edgedb.QueryError,
                r"'Issue' changes the interpretation of 'Issue'"):
            await self.query(r"""
                WITH MODULE test
                UPDATE Issue.related_to
                SET {
                    related_to := Issue
                };
            """)

    async def test_edgeql_expr_polymorphic_01(self):
        await self.query(r"""
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

        await self.query(r"""
            WITH MODULE test
            SELECT Owned {
                [IS Named].name
            };
        """)

    async def test_edgeql_expr_cast_01(self):
        await self.assert_query_result(r"""
            SELECT <std::str>123;
            SELECT <std::int64>"123";
            SELECT <std::str>123 ++ 'qw';
            SELECT <std::int64>"123" + 9000;
            SELECT <std::int64>"123" * 100;
            SELECT <std::str>(123 * 2);
        """, [
            ['123'],
            [123],
            ['123qw'],
            [9123],
            [12300],
            ['246'],
        ])

    async def test_edgeql_expr_cast_02(self):
        # testing precedence of casting vs. multiplication
        #
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\*' cannot .* 'std::str' and 'std::int64'"):

            await self.query("""
                SELECT <std::str>123 * 2;
            """)

    async def test_edgeql_expr_cast_03(self):
        await self.assert_query_result(r"""
            SELECT <std::str><std::int64><std::float64>'123.45' ++ 'foo';
        """, [
            ['123foo'],
        ])

    async def test_edgeql_expr_cast_04(self):
        await self.assert_query_result(r"""
            SELECT <str><int64><float64>'123.45' ++ 'foo';
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
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'cannot cast.*tuple.*to.*array.*'):
            await self.query(r"""
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
            # according to the standard implicit casts, most of the
            # ints can only be upcast to float64
            ['std::float64'],
            # int16 can upcast to float32
            ['std::float32'],
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
                edgedb.QueryError,
                r'if/else clauses must be of related types, '
                r'got: std::int64/std::str'):

            await self.query("""
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

    async def test_edgeql_expr_implicit_cast_06(self):
        await self.assert_query_result(r"""
            SELECT {(1, 2.0), (3, 4.5)};
            SELECT {(1, 2), (3, 4.5)};
            SELECT {(3, 4.5), (1, 2.0)};

            SELECT {(x := 1, y := 2.0), (x := 3, y := 4.5)};
            SELECT {(x := 1, y := 2), (x := 3, y := 4.5)};
            SELECT {(x := 3, y := 4.5), (x := 1, y := 2)};

            SELECT {(x := 1, y := 2), (a := 3, b := 4.5)};
            SELECT {(a := 3, b := 4.5), (x := 1, y := 2)};

            SELECT {(1, 2), (a := 3, b := 4.5)};
            SELECT {(a := 3, b := 4.5), (1, 2)};
        """, [
            [[1, 2], [3, 4.5]],
            [[1, 2], [3, 4.5]],
            [[3, 4.5], [1, 2]],

            [{"x": 1, "y": 2}, {"x": 3, "y": 4.5}],
            [{"x": 1, "y": 2}, {"x": 3, "y": 4.5}],
            [{"x": 3, "y": 4.5}, {"x": 1, "y": 2}],

            [[1, 2], [3, 4.5]],
            [[3, 4.5], [1, 2]],

            [[1, 2], [3, 4.5]],
            [[3, 4.5], [1, 2]],
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
                edgedb.QueryError, 'could not determine expression type'):
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
                'schema::AttributeSubject',
                'schema::Database',
                'schema::Delta',
                'schema::DerivedLink',
                'schema::DerivedObjectType',
                'schema::Object',
                'schema::ObjectType',
                'schema::Operator',
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
                edgedb.QueryError, r'could not determine array type'):

            await self.query("""
                SELECT [1, '1'];
            """)

    async def test_edgeql_expr_array_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot index array by.*str'):

            await self.query("""
                SELECT [1, 2]['1'];
            """)

    async def test_edgeql_expr_array_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not determine type of empty array'):

            await self.query("""
                SELECT [];
            """)

    async def test_edgeql_expr_array_concat_01(self):
        await self.assert_query_result('''
            SELECT [1, 2] ++ [3, 4];
        ''', [
            [
                [1, 2, 3, 4]
            ]
        ])

    async def test_edgeql_expr_array_concat_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\+\+' cannot.*int64.*str"):

            await self.query('''
                SELECT [1, 2] ++ ['a'];
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
                A := (SELECT ScalarType FILTER .name = 'test::issue_num_t')
            SELECT [A.name, A.default];
        ''', [
            [],
        ])

    async def test_edgeql_expr_array_10(self):
        with self.assertRaisesRegex(edgedb.QueryError, 'nested array'):
            await self.query(r'''
                SELECT [[1, 2], [3, 4]];
            ''')

    async def test_edgeql_expr_array_11(self):
        with self.assertRaisesRegex(edgedb.QueryError, 'nested array'):
            await self.query(r'''
                SELECT [array_agg({1, 2})];
            ''')

    async def test_edgeql_expr_array_12(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.query(r'''
                SELECT array_agg([1, 2, 3]);
            ''')

    async def test_edgeql_expr_array_13(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.query(r'''
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
                edgedb.InternalServerError,
                r'array index 10 is out of bounds'):
            await self.query("""
                SELECT [1, 2, 3][10];
            """)

    async def test_edgeql_expr_array_16(self):
        with self.assertRaisesRegex(
                # FIXME: possibly a different error should be used here
                edgedb.InternalServerError,
                r'array index -10 is out of bounds'):
            await self.query("""
                SELECT [1, 2, 3][-10];
            """)

    async def test_edgeql_expr_array_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot index array by.*float'):

            await self.query("""
                SELECT [1, 2][1.0];
            """)

    async def test_edgeql_expr_array_18(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice array by.*float'):

            await self.query("""
                SELECT [1, 2][1.0:3];
            """)

    async def test_edgeql_expr_array_19(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice array by.*str'):

            await self.query("""
                SELECT [1, 2][1:'3'];
            """)

    async def test_edgeql_expr_array_20(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot index array by std::float64'):

            await self.query("""
                SELECT [1, 2][2^40];
            """)

    async def test_edgeql_expr_array_21(self):
        # it should be technically possible to infer the type of the array
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not determine type of empty array'):

            await self.query("""
                SELECT {[1, 2], []};
            """)

    async def test_edgeql_expr_coalesce_01(self):
        await self.assert_query_result(r"""
            SELECT <int64>{} ?? 4 ?? 5;
            SELECT <str>{} ?? 'foo' ?? 'bar';
            SELECT 4 ?? <int64>{} ?? 5;

            SELECT 'foo' ?? <str>{} ?? 'bar';
            SELECT <str>{} ?? 'bar' = 'bar';

            SELECT 4^<int64>{} ?? 2;
            SELECT 4+<int64>{} ?? 2;
            SELECT 4*<int64>{} ?? 2;

            SELECT -<int64>{} ?? 2;
            SELECT -<int64>{} ?? -2 + 1;

            SELECT <int64>{} ?? <int64>{};
            SELECT <int64>{} ?? <int64>{} ?? <int64>{};
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
                edgedb.QueryError, r'cannot index string by.*str'):

            await self.query("""
                SELECT '123'['1'];
            """)

    async def test_edgeql_expr_string_03(self):
        with self.assertRaisesRegex(
                # FIXME: possibly a different error should be used here
                edgedb.InternalServerError,
                r'string index 10 is out of bounds'):
            await self.query("""
                SELECT '123'[10];
            """)

    async def test_edgeql_expr_string_04(self):
        with self.assertRaisesRegex(
                # FIXME: possibly a different error should be used here
                edgedb.InternalServerError,
                r'string index -10 is out of bounds'):
            await self.query("""
                SELECT '123'[-10];
            """)

    async def test_edgeql_expr_string_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot index string by.*float'):

            await self.query("""
                SELECT '123'[-1.0];
            """)

    async def test_edgeql_expr_string_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice string by.*float'):

            await self.query("""
                SELECT '123'[1.0:];
            """)

    async def test_edgeql_expr_string_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice string by.*str'):

            await self.query("""
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

            SELECT (1, 2) = (1, 2.0);
            SELECT (1, 2.0) = (1, 2);
            SELECT (1, 2.1) != (1, 2);
        """, [
            [True],
            [False],
            [False],
            [True],

            [True],
            [True],
            [True],
        ])

    async def test_edgeql_expr_tuple_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot"):
            await self.query(r"""
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
                edgedb.QueryError,
                r"operator '!=' cannot"):
            await self.query(r"""
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
                edgedb.QueryError,
                r"operator '\+'.*cannot.*tuple<.*>' and 'std::int64'"):

            await self.query(r'''
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

    async def test_edgeql_expr_tuple_indirection_14(self):
        await self.assert_query_result(r"""
            SELECT [(a:=(b:=(c:=(e:=1))))][0].a;
            SELECT [(a:=(b:=(c:=(e:=1))))][0].0;
            SELECT [(a:=(b:=(c:=(1,))))][0].0;
        """, [
            [{"b": {"c": {"e": 1}}}],
            [{"b": {"c": {"e": 1}}}],
            [{"b": {"c": [1]}}],
        ])

    async def test_edgeql_expr_cannot_assign_dunder_type_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot assign to __type__'):
            await self.query(r"""
                SELECT test::Text {
                    __type__ := 42
                };
            """)

    async def test_edgeql_expr_cannot_assign_id_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot assign to id'):
            await self.query(r"""
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
            SELECT EXISTS <str>{};
            SELECT NOT EXISTS <str>{};
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
        res = await self.query('''
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
        res = await self.query('''
            SELECT _ := DISTINCT {[1, 2], [1, 2], [2, 3]} ORDER BY _;
        ''')
        self.assert_data_shape(res, [
            [[1, 2], [2, 3]],
        ])

    async def test_edgeql_expr_setop_10(self):
        res = await self.query('''
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
        res = await self.query('''
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
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=39):

            await self.query('''\
                WITH MODULE test
                SELECT Issue ORDER BY Issue.watchers.name;
            ''')

    async def test_edgeql_expr_cardinality_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=30):

            await self.query('''\
                WITH MODULE test
                SELECT Issue LIMIT LogEntry.spent_time;
            ''')

    async def test_edgeql_expr_cardinality_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=30):

            await self.query('''\
                WITH MODULE test
                SELECT Issue OFFSET LogEntry.spent_time;
            ''')

    async def test_edgeql_expr_cardinality_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=46):

            await self.query('''\
                WITH MODULE test
                SELECT EXISTS Issue ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=53):

            await self.query('''\
                WITH MODULE test
                SELECT 'foo' IN Issue.name ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=50):

            await self.query('''\
                WITH MODULE test
                SELECT Issue UNION Text ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                position=48):

            await self.query('''\
                WITH MODULE test
                SELECT DISTINCT Issue ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_type_filter_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'invalid type filter operand: std::int64 is not '
                r'an object type',
                position=7):

            await self.query('''\
                SELECT 10[IS std::Object];
            ''')

    async def test_edgeql_expr_type_filter_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'invalid type filter operand: std::str is not an object type',
                position=17):

            await self.query('''\
                SELECT Object[IS str];
            ''')

    async def test_edgeql_expr_type_filter_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'invalid type filter operand: '
                r'std::uuid is not an object type',
                position=20):

            await self.query('''\
                SELECT Object.id[IS uuid];
            ''')

    async def test_edgeql_expr_comparison_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot.*tuple.*and.*array<std::int64>"):
            await self.query(r'''
                SELECT (1, 2) = [1, 2];
            ''')

    async def test_edgeql_expr_comparison_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot.* 'std::int64' and.*array<std::int64>"):
            await self.query(r'''
                SELECT {1, 2} = [1, 2];
            ''')

    async def test_edgeql_expr_comparison_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot.*'std::int64' and.*tuple.*"):
            await self.query(r'''
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
