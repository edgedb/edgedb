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


import functools
import os.path
import typing

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class value(typing.NamedTuple):
    typename: str

    anyreal: bool
    anyint: bool
    anyfloat: bool
    anynumeric: bool

    signed: bool
    datetime: bool


VALUES = {
    '<bool>True':
        value(typename='bool',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=False, signed=False, anynumeric=False),

    '<uuid>"d4288330-eea3-11e8-bc5f-7faf132b1d84"':
        value(typename='uuid',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=False, signed=False, anynumeric=False),

    '<bytes>b"Hello"':
        value(typename='bytes',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=False, signed=False, anynumeric=False),

    '<str>"Hello"':
        value(typename='str',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=False, signed=False, anynumeric=False),

    '<json>"Hello"':
        value(typename='json',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=False, signed=False, anynumeric=False),

    '<datetime>"2018-05-07T20:01:22.306916+00:00"':
        value(typename='datetime',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=False, anynumeric=False),

    '<cal::local_datetime>"2018-05-07T20:01:22.306916"':
        value(typename='cal::local_datetime',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=False, anynumeric=False),

    '<cal::local_date>"2018-05-07"':
        value(typename='cal::local_date',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=False, anynumeric=False),

    '<cal::local_time>"20:01:22.306916"':
        value(typename='cal::local_time',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=False, anynumeric=False),

    '<duration>"20:01:22.306916"':
        value(typename='duration',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=True, anynumeric=False),

    '<int16>1':
        value(typename='int16',
              anyreal=True, anyint=True, anyfloat=False,
              datetime=False, signed=True, anynumeric=False),

    '<int32>1':
        value(typename='int32',
              anyreal=True, anyint=True, anyfloat=False,
              datetime=False, signed=True, anynumeric=False),

    '<int64>1':
        value(typename='int64',
              anyreal=True, anyint=True, anyfloat=False,
              datetime=False, signed=True, anynumeric=False),

    '1':  # same as <int64>1
        value(typename='int64',
              anyreal=True, anyint=True, anyfloat=False,
              datetime=False, signed=True, anynumeric=False),

    '<float32>1':
        value(typename='float32',
              anyreal=True, anyint=False, anyfloat=True,
              datetime=False, signed=True, anynumeric=False),

    '<float64>1':
        value(typename='float64',
              anyreal=True, anyint=False, anyfloat=True,
              datetime=False, signed=True, anynumeric=False),

    '1.0':  # same as <float64>1
        value(typename='float64',
              anyreal=True, anyint=False, anyfloat=True,
              datetime=False, signed=True, anynumeric=False),

    '<bigint>1':
        value(typename='bigint',
              anyreal=True, anyint=True, anyfloat=False,
              datetime=False, signed=True, anynumeric=True),

    '1n':
        value(typename='bigint',
              anyreal=True, anyint=True, anyfloat=False,
              datetime=False, signed=True, anynumeric=True),

    '<decimal>1.0':
        value(typename='decimal',
              anyreal=True, anyint=False, anyfloat=False,
              datetime=False, signed=True, anynumeric=True),

    '1.0n':
        value(typename='decimal',
              anyreal=True, anyint=False, anyfloat=False,
              datetime=False, signed=True, anynumeric=True),

    '<cal::relative_duration>"P1Y2M3DT20H1M22.306916S"':
        value(typename='cal::relative_duration',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=True, anynumeric=False),
}


@functools.lru_cache()
def get_test_values(**flags):
    res = []
    for val, desc in VALUES.items():
        if all(bool(getattr(desc, fname)) == bool(fval)
               for fname, fval in flags.items()):
            res.append(val)
    return tuple(res)


@functools.lru_cache()
def get_test_items(**flags):
    res = []
    for val, desc in VALUES.items():
        if all(bool(getattr(desc, fname)) == bool(fval)
               for fname, fval in flags.items()):
            res.append((val, desc))
    return tuple(res)


class TestExpressionsWithoutConstantFolding(tb.QueryTestCase):

    SETUP_METHOD = """
        CONFIGURE SESSION SET __internal_no_const_folding := true;
    """

    async def test_edgeql_no_const_folding_str_concat_01(self):
        await self.assert_query_result(
            r"""
                SELECT 'aaaa' ++ 'bbbb';
            """,
            ['aaaabbbb'],
        )

    async def test_edgeql_no_const_folding_str_concat_02(self):
        await self.assert_query_result(
            r"""
                SELECT 'aaaa' ++ r'\q' ++ $$\n$$;
            """,
            [R'aaaa\q\n'],
        )


class TestExpressions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_expr_emptyset_01(self):
        await self.assert_query_result(
            r'''SELECT <int64>{};''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT <str>{};''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT <int64>{} + 1;''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT 1 + <int64>{};''',
            [],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'expression returns value of indeterminate type'):

            await self.con.execute("""
                SELECT {};
            """)

    async def test_edgeql_expr_emptyset_02(self):
        await self.assert_query_result(
            r'''SELECT count(<int64>{});''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT count(DISTINCT <int64>{});''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT count({});''',
            [0],
        )

    async def test_edgeql_expr_emptyset_03(self):
        await self.assert_query_result(
            r'''SELECT {1, {}};''',
            [1],
        )

    async def test_edgeql_expr_emptyset_04(self):
        await self.assert_query_result(
            r'''SELECT sum({1, 1, {}});''',
            [2],
        )

    async def test_edgeql_expr_emptyset_05(self):
        await self.assert_query_result(
            r'''SELECT {False, <bool>{}};''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT {False, {False, <bool>{}}};''',
            [False, False],
        )

        await self.assert_query_result(
            r'''SELECT {<bool>{}, <bool>{}};''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT {False, {<bool>{}, <bool>{}}};''',
            [False],
        )

    async def test_edgeql_expr_idempotent_01(self):
        await self.assert_query_result(
            r"""
                SELECT (SELECT (SELECT (SELECT 42)));
            """,
            [42],
        )

    async def test_edgeql_expr_idempotent_02(self):
        await self.assert_query_result(
            r'''SELECT 'f';''',
            ['f'],
        )

        await self.assert_query_result(
            r'''SELECT 'f'[0];''',
            ['f'],
        )

        await self.assert_query_result(
            r'''SELECT 'foo'[0];''',
            ['f'],
        )

        await self.assert_query_result(
            r'''SELECT 'f'[0][0][0][0][0];''',
            ['f'],
        )

        await self.assert_query_result(
            r'''SELECT 'foo'[0][0][0][0][0];''',
            ['f'],
        )

    async def test_edgeql_expr_op_01(self):
        await self.assert_query_result(
            r'''SELECT 40 + 2;''',
            [42],
        )

        await self.assert_query_result(
            r'''SELECT 40 - 2;''',
            [38],
        )

        await self.assert_query_result(
            r'''SELECT 40 * 2;''',
            [80],
        )

        await self.assert_query_result(
            r'''SELECT 40 / 2;''',
            [20],
        )

        await self.assert_query_result(
            r'''SELECT 40 % 2;''',
            [0],
        )

        await self.assert_query_result(
            r'''SELECT 40000000000000000000000000n + 1;''',
            [40000000000000000000000001],
        )

    async def test_edgeql_expr_literals_01(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 1).name;''',
            {'std::int64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 1.0).name;''',
            {'std::float64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 9223372036854775807).name;''',
            {'std::int64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF -9223372036854775808).name;''',
            {'std::int64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 9223372036854775808).name;''',
            {'std::int64'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 1n).name;''',
            {'std::bigint'},
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 1.0n).name;''',
            {'std::decimal'},
        )

    async def test_edgeql_expr_literals_02(self):
        with self.assertRaisesRegex(edgedb.NumericOutOfRangeError,
                                    'std::int16 out of range'):
            async with self.con.transaction():
                await self.con.query_single(
                    r'''SELECT <int16>36893488147419''',
                )

        with self.assertRaisesRegex(edgedb.NumericOutOfRangeError,
                                    'std::int32 out of range'):
            async with self.con.transaction():
                await self.con.query_single(
                    r'''SELECT <int32>36893488147419''',
                )

        with self.assertRaisesRegex(edgedb.NumericOutOfRangeError,
                                    'is out of range for type std::int64'):
            async with self.con.transaction():
                await self.con.query_single(
                    r'''SELECT <int64>'3689348814741900000000000' ''',
                )

        with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                    'expected digit after dot'):
            async with self.con.transaction():
                await self.con.query_single('SELECT 0. ')

        with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                    'number is out of range for std::float64'):
            async with self.con.transaction():
                await self.con.query_single(
                    r'''SELECT 1e999''',
                )

        with self.assertRaisesRegex(edgedb.NumericOutOfRangeError,
                                    'interval field value out of range'):
            async with self.con.transaction():
                await self.con.query_single(
                    r'''SELECT <duration>'3074457345618258602us' ''',
                )

    async def test_edgeql_expr_op_02(self):
        await self.assert_query_result(
            r'''SELECT 40 ^ 2;''',
            [1600],
        )

        await self.assert_query_result(
            r'''SELECT 121 ^ 0.5;''',
            [11],
        )

        await self.assert_query_result(
            r'''SELECT 2 ^ 3 ^ 2;''',
            [2 ** 3 ** 2],
        )

    async def test_edgeql_expr_op_03(self):
        await self.assert_query_result(
            r'''SELECT 40 < 2;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 40 > 2;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 40 <= 2;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 40 >= 2;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 40 = 2;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 40 != 2;''',
            [True],
        )

    async def test_edgeql_expr_op_04(self):
        await self.assert_query_result(
            r'''SELECT -1 + 2 * 3 - 5 - 6.0 / 2;''',
            [-3],
        )

        await self.assert_query_result(
            r'''
                SELECT
                    -1 + 2 * 3 - 5 - 6.0 / 2 > 0
                    OR 25 % 4 = 3 AND 42 IN {12, 42, 14};
            ''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT (-1 + 2) * 3 - (5 - 6.0) / 2;''',
            [3.5],
        )

        await self.assert_query_result(
            r'''
                SELECT
                    ((-1 + 2) * 3 - (5 - 6.0) / 2 > 0 OR 25 % 4 = 3)
                    AND 42 IN {12, 42, 14};
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 1 * 0.2;''',
            [0.2],
        )

        await self.assert_query_result(
            r'''SELECT 0.2 * 1;''',
            [0.2],
        )

        await self.assert_query_result(
            r'''SELECT -0.2 * 1;''',
            [-0.2],
        )

        await self.assert_query_result(
            r'''SELECT 0.2 + 1;''',
            [1.2],
        )

        await self.assert_query_result(
            r'''SELECT 1 + 0.2;''',
            [1.2],
        )

        await self.assert_query_result(
            r'''SELECT -0.2 - 1;''',
            [-1.2],
        )

        await self.assert_query_result(
            r'''SELECT -1 - 0.2;''',
            [-1.2],
        )

        await self.assert_query_result(
            r'''SELECT -1 / 0.2;''',
            [-5],
        )

        await self.assert_query_result(
            r'''SELECT 0.2 / -1;''',
            [-0.2],
        )

        await self.assert_query_result(
            r'''SELECT 5 // 2;''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT 5.5 // 1.2;''',
            [4.0],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF (5.5 // 1.2)).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT -9.6 // 2;''',
            [-5.0],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF (<float32>-9.6 // 2)).name;''',
            ['std::float64'],
        )

    async def test_edgeql_expr_op_05(self):
        await self.assert_query_result(
            r"""
                SELECT 'foo' ++ 'bar';
            """,
            ['foobar'],
        )

    async def test_edgeql_expr_op_06(self):
        await self.assert_query_result(
            r"""SELECT <int64>{} = <int64>{};""",
            []
        )

        await self.assert_query_result(
            r"""SELECT <int64>{} = 42;""",
            []
        )

    async def test_edgeql_expr_op_07(self):
        # Test boolean interaction with {}
        await self.assert_query_result(
            r"""SELECT TRUE OR <bool>{};""",
            [])

        await self.assert_query_result(
            r"""SELECT FALSE AND <bool>{};""",
            []
        )

    async def test_edgeql_expr_op_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '-' cannot .* 'std::str'"):

            await self.con.query("""
                SELECT -'aaa';
            """)

    async def test_edgeql_expr_op_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator 'NOT' cannot .* 'std::str'"):

            await self.con.query_json("""
                SELECT NOT 'aaa';
            """)

    async def test_edgeql_expr_op_10(self):
        # the types are put in to satisfy type inference
        await self.assert_query_result(
            r'''SELECT +<int64>{};''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT -<int64>{};''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT NOT <bool>{};''',
            [],
        )

    async def test_edgeql_expr_op_11(self):
        # Test non-trivial folding
        await self.assert_query_result(
            r'''SELECT 1 + (1 + len([1, 2])) + 1;''',
            [5],
        )

        await self.assert_query_result(
            r'''SELECT 2 * (2 * len([1, 2])) * 2;''',
            [16],
        )

    async def test_edgeql_expr_op_12(self):
        # Test power precedence
        await self.assert_query_result(
            r"""SELECT -2^2;""",
            [-4],
        )

    async def test_edgeql_expr_op_13(self):
        # test equivalence comparison
        await self.assert_query_result(
            r'''SELECT 2 ?= 2;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 2 ?= 3;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 2 ?!= 2;''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT 2 ?!= 3;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 2 ?= <int64>{};''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT <int64>{} ?= <int64>{};''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 2 ?!= <int64>{};''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int64>{} ?!= <int64>{};''',
            [False],
        )

    async def test_edgeql_expr_op_14(self):
        await self.assert_query_result(
            r"""
                SELECT _ := {9, 1, 13}
                FILTER _ IN {11, 12, 13};
            """,
            {13},
        )

        await self.assert_query_result(
            r"""
                SELECT _ := {9, 1, 13, 11}
                FILTER _ IN {11, 12, 13};
            """,
            {11, 13},
        )

    async def test_edgeql_expr_op_15(self):
        await self.assert_query_result(
            r"""
                SELECT _ := {9, 12, 13}
                FILTER _ NOT IN {11, 12, 13};
            """,
            {9},
        )

        await self.assert_query_result(
            r"""
                SELECT _ := {9, 1, 13, 11}
                FILTER _ NOT IN {11, 12, 13};
            """,
            {1, 9},
        )

    async def test_edgeql_expr_op_16(self):
        await self.assert_query_result(
            r"""
                WITH a := {11, 12, 13}
                SELECT _ := {9, 1, 13}
                FILTER _ IN a;
            """,
            {13},
        )

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT _ := {9, 1, 13}
                FILTER _ IN (
                    # Lengths of names for schema::Map, Type, and Array are
                    # 11, 12, and 13, respectively.
                    SELECT len(ObjectType.name)
                    FILTER ObjectType.name LIKE 'schema::%'
                );
            """,
            {13},
        )

    async def test_edgeql_expr_op_17(self):
        await self.assert_query_result(
            r"""
                WITH a := {11, 12, 13}
                SELECT _ := {9, 1, 13}
                FILTER _ NOT IN a;
            """,
            {9, 1},
        )

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT _ := {9, 1, 13}
                FILTER _ NOT IN (
                    # Lengths of names for schema::Map, Type, and Array are
                    # 11, 12, and 13, respectively.
                    SELECT len(ObjectType.name)
                    FILTER ObjectType.name LIKE 'schema::%'
                );
            """,
            {9, 1},
        )

    async def test_edgeql_expr_op_18(self):
        await self.assert_query_result(
            r"""
                SELECT _ := {1, 2, 3} IN {3, 4}
                ORDER BY _;
            """,
            [False, False, True],
        )

    async def test_edgeql_expr_op_19(self):
        await self.assert_query_result(
            r'''SELECT 1 IN <int64>{};''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT {1, 2, 3} IN <int64>{};''',
            [False, False, False],
        )

        await self.assert_query_result(
            r'''SELECT 1 NOT IN <int64>{};''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT {1, 2, 3} NOT IN <int64>{};''',
            [True, True, True],
        )

    async def test_edgeql_expr_op_20(self):
        # Test that power applied to int64 is producing a float64 even
        # in the underlying implementation.
        await self.assert_query_result(
            # use of floor(random()) is to prevent constant folding
            # optimizations
            r'''SELECT (10 + math::floor(random()))^308;''',
            # FIXME: due to limitations of the Python [test] driver,
            # we get a Python 'int' back, instead of the 'float' and
            # the 'int' is not going to be equal to 1e308.
            #
            # If the driver cast the result into a 'float', then the
            # correct answer in Python would be 1e308.
            [float(10**308)],
        )

        await self.assert_query_result(
            r'''SELECT (10 + math::floor(random()))^308 = 1e308;''',
            [True],
        )

        # overflow is expected for float64, but would not happen for decimal
        with self.assertRaisesRegex(edgedb.NumericOutOfRangeError, 'overflow'):
            await self.con.query_single(r"""
                SELECT (10 + math::floor(random()))^309;
            """)

    async def test_edgeql_expr_op_21(self):
        # There was a bug that caused `=` to not always be equivalent
        # to `>= AND <=` due to difference in casting decimals to
        # floats or floats into decimal.
        await self.assert_query_result(
            r'''
            SELECT 0.797693134862311111111n = <decimal>0.797693134862311111111;
            ''',
            [False],
        )

        await self.assert_query_result(
            r'''
            SELECT
                0.797693134862311111111n >= <decimal>0.797693134862311111111
                AND
                0.797693134862311111111n <= <decimal>0.797693134862311111111;
            ''',
            [False],
        )

    async def test_edgeql_expr_mod_01(self):
        # Test integer division and remainder.
        tests = [
            ('5', '3', 1, 2),
            ('-5', '3', -2, 1),
            ('-5', '-3', 1, -2),
            ('5', '-3', -2, -1),
        ]

        types = [v.typename for v in VALUES.values() if v.anyreal]

        for t in types:
            for a, b, div, mod in tests:
                await self.assert_query_result(
                    f'''SELECT <{t}>{a} // <{t}>{b};''',
                    [div],
                )
                await self.assert_query_result(
                    f'''SELECT <{t}>{a} % <{t}>{b};''',
                    [mod],
                )

    async def test_edgeql_expr_variables_01(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF <int64>$0).name;''',
            {'std::int64'},
            variables=(7,),
        )

    async def test_edgeql_expr_variables_02(self):
        await self.assert_query_result(
            r'''SELECT <str>$1 ++ (INTROSPECT TYPEOF <int64>$0).name;''',
            {'xstd::int64'},
            variables=(7, 'x'),
        )

    async def test_edgeql_expr_variables_03(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF <int64>$x).name;''',
            {'std::int64'},
            variables={'x': 7},
        )

    async def test_edgeql_expr_variables_04(self):
        self.con._clear_codecs_cache()

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r"argument \$x is required"):
            await self.assert_query_result(
                r'''SELECT <int64>$x;''',
                None,  # unused
                variables={'x': None},
            )

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r"argument \$0 is required"):
            await self.assert_query_result(
                r'''SELECT <int64>$0;''',
                None,  # unused
                variables=(None,),
            )

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r"argument \$x is required"):
            await self.assert_query_result(
                r'''SELECT <REQUIRED int64>$x;''',
                None,  # unused
                variables={'x': None},
            )

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r"argument \$0 is required"):
            await self.assert_query_result(
                r'''SELECT <REQUIRED int64>$0;''',
                None,  # unused
                variables=(None,),
            )

        await self.assert_query_result(
            r'''SELECT <OPTIONAL int64>$x ?? -1;''',
            [-1],
            variables={'x': None},
        )

        await self.assert_query_result(
            r'''SELECT <OPTIONAL int64>$0 ?? -1;''',
            [-1],
            variables=(None,),
        )

        await self.assert_query_result(
            r'''SELECT <REQUIRED int64>$x ?? -1;''',
            [7],
            variables={'x': 7},
        )

        await self.assert_query_result(
            r'''SELECT <REQUIRED int64>$0 ?? -1;''',
            [11],
            variables=(11,),
        )

        # Optional cardinality modifier doesn't affect type
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF <OPTIONAL int64>$x).name;''',
            {"std::int64"},
            variables={'x': None},
        )

        # Enforce parameter is passed even if we don't actually care
        # about the value
        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r"argument \$x is required"):
            await self.assert_query_result(
                r'''SELECT (INTROSPECT TYPEOF <int64>$x).name;''',
                None,  # unused
                variables={'x': None},
            )

    async def test_edgeql_expr_variables_05(self):
        for typ in {'bigint', 'decimal', 'int64', 'int32', 'int16'}:
            with self.annotate(type=typ):
                await self.assert_query_result(
                    f'''SELECT <{typ}>$x;''',
                    [123],
                    variables={'x': 123},
                )

                await self.assert_query_result(
                    f'''SELECT <array<{typ}>>$x;''',
                    [[123]],
                    variables={'x': [123]},
                )

    async def test_edgeql_expr_variables_06(self):
        await self.assert_query_result(
            f'''SELECT <OPTIONAL int64>$x + <int64>$y;''',
            [5],
            variables={'x': 2, 'y': 3},
        )

        await self.assert_query_result(
            f'''SELECT <OPTIONAL int64>$x + <int64>$y;''',
            [],
            variables={'x': None, 'y': 3},
        )

        await self.assert_query_result(
            f'''SELECT len(<OPTIONAL str>$x);''',
            [],
            variables={'x': None},
        )

    async def _test_boolop(self, left, right, op, not_op, result):
        if isinstance(result, bool):
            # this operation should be valid and produce opposite
            # results for op and not_op
            await self.assert_query_result(
                f"""SELECT {left} {op} {right};""", {result})

            await self.assert_query_result(
                f"""SELECT {left} {not_op} {right};""", {not result})
        else:
            # operation is expected to be invalid
            for binop in [op, not_op]:
                query = f"""SELECT {left} {binop} {right};"""
                with self.assertRaisesRegex(edgedb.QueryError, result,
                                            msg=query):
                    async with self.con.transaction():
                        await self.con.query(query)

    async def test_edgeql_expr_valid_eq_01(self):
        # compare all numerics to all other scalars via equality
        ops = [('=', '!='), ('?=', '?!='), ('IN', 'NOT IN')]

        for left in get_test_values(anyreal=True):
            for right in get_test_values(anyreal=False):
                for op, not_op in ops:
                    await self._test_boolop(
                        left, right, op, not_op,
                        'cannot be applied to operands'
                    )

    async def test_edgeql_expr_valid_eq_02(self):
        # compare all numerics to each other via equality
        ops = [('=', '!='), ('?=', '?!='), ('IN', 'NOT IN')]

        for left, ldesc in get_test_items(anyreal=True):
            for right, rdesc in get_test_items(anyreal=True):
                if (ldesc.anynumeric and rdesc.anyfloat
                        or rdesc.anynumeric and ldesc.anyfloat):
                    # decimals are not implicitly comparable to floats
                    expected = 'cannot be applied to operands'
                else:
                    expected = True

                for op, not_op in ops:
                    await self._test_boolop(
                        left, right, op, not_op, expected
                    )

    async def test_edgeql_expr_valid_eq_03(self):
        expected_error_msg = 'cannot be applied to operands'

        ops = [('=', '!='), ('?=', '?!='), ('IN', 'NOT IN')]
        # compare all non-numerics to all scalars via equality
        for left in get_test_values(anyreal=False):
            for right in get_test_values():
                for op, not_op in ops:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if left == right else expected_error_msg
                    )

    async def test_edgeql_expr_valid_comp_02(self):
        expected_error_msg = 'cannot be applied to operands'
        # compare all orderable non-numerics to all scalars via
        # ordering operator
        for left in get_test_values(anyreal=False):
            for right in get_test_values():
                for op, not_op in [('>=', '<'), ('<=', '>')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if left == right else expected_error_msg
                    )

    async def test_edgeql_expr_valid_comp_03(self):
        # compare numerics to all scalars via ordering comparators
        for left, ldesc in get_test_items(anyreal=True):
            for right, rdesc in get_test_items():
                if (ldesc.anynumeric and rdesc.anyfloat
                        or rdesc.anynumeric and ldesc.anyfloat
                        or not rdesc.anyreal):
                    # decimals are not implicitly comparable to floats
                    expected = 'cannot be applied to operands'
                else:
                    expected = True

                for op, not_op in [('>=', '<'), ('<=', '>')]:
                    await self._test_boolop(
                        left, right, op, not_op, expected
                    )

    async def test_edgeql_expr_valid_comp_04(self):
        # bytes and uuids are orderable in the same way as a "similar"
        # ascii string. For uuid this works out because ord('9') < ord('a').
        #
        # Motivation: In some sense str and uuid are a special kind of
        # byte-string. A different way of representing them would be
        # as arrays (sequences) of bytes. Conceptually, as long as the
        # individual elements of these arrays are orderable (and a
        # total ordering can be naturally defined on actual bytes),
        # the array of these elements is also orderable.

        # "ordered" uuid-like strings
        uuids = [
            '04b4318e-1a01-41e4-b29c-b57b94db9402',
            '94b4318e-1a01-41e4-b29c-b57b94db9402',
            'a4b4318e-1a01-41e4-b29c-b57b94db9402',
            'a5b4318e-1a01-41e4-b29c-b57b94db9402',
            'f4b4318e-1a01-41e4-b29c-b57b94db9402',
            'f4b4318e-1a01-41e4-b29c-b67b94db9402',
            'f4b4318e-1a01-41e4-b29c-b68b94db9402',
        ]

        for left in uuids[:-1]:
            for right in uuids[1:]:
                for op in ('>=', '<', '<=', '>'):
                    query = f'''
                        SELECT (b'{left}' {op} b'{right}') =
                            ('{left}' {op} '{right}');
                    '''
                    await self.assert_query_result(
                        query, {True}, msg=query)

                    query = f'''
                        SELECT (<uuid>'{left}' {op} <uuid>'{right}') =
                            ('{left}' {op} '{right}');
                    '''
                    await self.assert_query_result(
                        query, {True}, msg=query)

    async def test_edgeql_expr_valid_comp_05(self):
        # just some ascii strings that can be simple byte literals
        raw_ascii = [
            R'hello',
            R'94b4318e-1a01-41e4-b29c-b57b94db9402',
            R'hello world',
            R'123',
            R'',
            R'&*%#',
            R'&*@#',
        ]
        raw_ascii.sort()

        # we want to see that the sorting worked out the same way for
        # bytes and str
        for left in raw_ascii[:-1]:
            for right in raw_ascii[1:]:
                for op in ('>=', '<', '<=', '>'):
                    query = f'''
                        SELECT (b'{left}' {op} b'{right}') =
                            ('{left}' {op} '{right}');
                    '''
                    await self.assert_query_result(
                        query, {True}, msg=query)

    async def test_edgeql_expr_valid_order_01(self):
        # JSON ordering is a bit difficult to conceptualize across
        # non-homogeneous JSON types, but it is stable and can be used
        # reliably in ORDER BY clauses. In fact, many tests rely on this.
        await self.assert_query_result(
            r'''SELECT <json>2 < <json>'2';''',
            [False],
        )

        await self.assert_query_result(
            r'''
                WITH X := {<json>1, <json>True, <json>'1'}
                SELECT X ORDER BY X;
            ''',
            # JSON
            ['1', 1, True],
            # Binary
            ['"1"', '1', 'true'],
        )

        await self.assert_query_result(
            r'''
                WITH X := {
                    <json>1,
                    <json>2,
                    <json>'b',
                    to_json('{"a":1,"b":2}'),
                    to_json('{"b":3,"a":1,"b":2}'),
                    to_json('["a", 1, "b", 2]')
                }
                SELECT X ORDER BY X;
            ''',
            # JSON
            ['b', 1, 2, ['a', 1, 'b', 2], {'a': 1, 'b': 2}, {'a': 1, 'b': 2}],
            # Binary
            [
                '"b"', '1', '2', '["a", 1, "b", 2]',
                '{"a": 1, "b": 2}', '{"a": 1, "b": 2}'
            ],
        )

    async def test_edgeql_expr_valid_order_02(self):
        # test bool ordering
        await self.assert_query_result(
            r'''SELECT False < True;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT X := {True, False, True, False} ORDER BY X;''',
            [False, False, True, True],
        )

        await self.assert_query_result(
            r'''SELECT X := {True, False, True, False} ORDER BY X DESC;''',
            [True, True, False, False],
        )

    async def test_edgeql_expr_valid_order_03(self):
        # "unordered" uuid-like strings
        uuids = [
            '04b4318e-1a01-41e4-b29c-b57b94db9402',
            'f4b4318e-1a01-41e4-b29c-b57b94db9402',
            'a4b4318e-1a01-41e4-b29c-b57b94db9402',
            'f4b4318e-1a01-41e4-b29c-b68b94db9402',
            'a5b4318e-1a01-41e4-b29c-b57b94db9402',
            '94b4318e-1a01-41e4-b29c-b57b94db9402',
            'f4b4318e-1a01-41e4-b29c-b67b94db9402',
        ]

        await self.assert_query_result(
            f'''
                WITH A := <uuid>{{
                    '{"', '".join(uuids)}'
                }}
                SELECT array_agg(A ORDER BY A) =
                    [<uuid>'{"', <uuid>'".join(sorted(uuids))}'];
            ''',
            {True},
        )

    async def test_edgeql_expr_valid_order_04(self):
        # just some ascii strings that can be simple byte literals
        raw_ascii = [
            R'hello',
            R'94b4318e-1a01-41e4-b29c-b57b94db9402',
            R'hello world',
            R'123',
            R'',
            R'&*%#',
            R'&*@#',
        ]

        await self.assert_query_result(
            f'''
                WITH A := {{
                    b'{"', b'".join(raw_ascii)}'
                }}
                SELECT array_agg(A ORDER BY A) =
                    [b'{"', b'".join(sorted(raw_ascii))}'];
            ''',
            {True},
        )

    async def test_edgeql_expr_valid_order_05(self):
        # just some ascii strings that can be simple byte literals
        raw_ascii = [
            R'hello',
            R'94b4318e-1a01-41e4-b29c-b57b94db9402',
            R'hello world',
            R'123',
            R'',
            R'&*%#',
            R'&*@#',
        ]

        await self.assert_query_result(
            f'''
                WITH A := {{
                    '{"', '".join(raw_ascii)}'
                }}
                SELECT A ORDER BY A;
            ''',
            sorted(raw_ascii),
        )

    async def test_edgeql_expr_valid_order_06(self):
        # make sure various date&time scalaras are usable in order by clause
        await self.assert_query_result(
            r'''
                WITH A := <datetime>{
                    "2018-05-07T20:01:22.306916+00:00",
                    "2017-05-07T20:01:22.306916+00:00"
                }
                SELECT A ORDER BY A;
            ''',
            [
                "2017-05-07T20:01:22.306916+00:00",
                "2018-05-07T20:01:22.306916+00:00",
            ],
        )

        await self.assert_query_result(
            r'''
                WITH A := <cal::local_datetime>{
                    "2018-05-07T20:01:22.306916",
                    "2017-05-07T20:01:22.306916"
                }
                SELECT A ORDER BY A;
            ''',
            [
                "2017-05-07T20:01:22.306916",
                "2018-05-07T20:01:22.306916",
            ],
        )

        await self.assert_query_result(
            r'''
                WITH A := <cal::local_date>{
                    "2018-05-07",
                    "2017-05-07"
                }
                SELECT A ORDER BY A;
            ''',
            [
                "2017-05-07",
                "2018-05-07",
            ],
        )

        await self.assert_query_result(
            r'''
                WITH A := <cal::local_time>{
                    "20:01:22.306916",
                    "19:01:22.306916"
                }
                SELECT A ORDER BY A;
            ''',
            [
                "19:01:22.306916",
                "20:01:22.306916",
            ],
        )

        await self.assert_query_result(
            r'''
                WITH A := to_str(
                    <duration>{
                        "20:01:22.306916",
                        "19:01:22.306916"
                    }
                )
                SELECT A ORDER BY A;
            ''',
            [
                "PT19H1M22.306916S",
                "PT20H1M22.306916S",
            ]
        )

    async def test_edgeql_expr_valid_order_07(self):
        # make sure that any numeric type is orderable and produces
        # expected result
        numbers = list(range(-4, 5))
        str_numbers = ', '.join([str(n) for n in numbers])

        # ensure that unorderable scalars cannot be used in 'ORDER BY'
        for _val, vdesc in get_test_items(anyreal=True):
            query = f'''
                WITH X := <{vdesc.typename}>{{ {str_numbers} }}
                SELECT X ORDER BY X DESC;
            '''
            await self.assert_query_result(
                query,
                sorted(numbers, reverse=True),
                msg=query)

    async def test_edgeql_expr_valid_arithmetic_01(self):
        # unary minus should work for numeric scalars and duration
        for right in get_test_values(signed=True):
            query = f"""SELECT count(-{right});"""
            await self.assert_query_result(query, [1])

    async def test_edgeql_expr_valid_arithmetic_02(self):
        expected_error_msg = 'cannot be applied to operands'
        # unary minus should not work for other scalars
        for right in get_test_values(signed=False):
            query = f"""SELECT -{right};"""
            with self.assertRaisesRegex(edgedb.QueryError,
                                        expected_error_msg,
                                        msg=query):
                async with self.con.transaction():
                    await self.con.query_single(query)

    # NOTE: Generalized Binop `+` and `-` rules:
    #
    # 1) There are some scalars that simply don't support these operators
    #    at all.
    #
    # 2) Date/time scalars support `+` if one of the operands is
    #    `duration`. The result is always of the type of the other
    #    operand.
    #
    # 3) Date/time scalars support `-` when the right operand is
    #    `duration`. The result is always of the type of the first
    #    operand. Technically this is dictated by the equivalence of
    #    A - B and A + (-B).
    #
    # 4) Date/time scalars support `-` when both operands are of the
    #    same type. The result is always `duration`.
    #
    # 5) Numeric scalars support `+` and `-` iff it is possible to
    #    implicitly cast one operand into the other. The result is
    #    always of the type of this implicit cast. This makes
    #    `decimal` basically incompatible with float types without an
    #    explicit cast. As far as types are concerned the operators
    #    `+` and `-` are "commutative".
    async def test_edgeql_expr_valid_arithmetic_03(self):
        expected_error_msg = 'cannot be applied to operands'
        # Test (1) - scalars that don't support + and - at all.
        #
        # Incidentally, this also implies that they don't support '*',
        # '/', '//', '%', '^' as these are derived or defined from `+`
        # and `-`.
        #
        # In particular, `str` and `bytes` don't support `*` with
        # meaning of "repeated concatenation".
        for left in get_test_values(datetime=False, anyreal=False):
            for right in get_test_values():
                for op in ['+', '-', '*', '/', '//', '%', '^']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_arithmetic_04(self):
        # Tests (2), (3), (4) - various date/time with non-date/time
        # combinations.
        dts = get_test_values(datetime=True)
        expected_error_msg = 'cannot be applied to operands'

        # none of the date/time scalars support '+', '-', '*', '/',
        # '//', '%', '^' with non-date/time
        for left in dts:
            for right in get_test_values():
                if right in dts:
                    continue
                for op in ['+', '-', '*', '/', '//', '%', '^']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_arithmetic_05(self):
        # Tests (2) - various date/time combinations.
        expected_error_msg = 'cannot be applied to operands'

        for left, ldesc in get_test_items(datetime=True):
            for right, rdesc in get_test_items(datetime=True):
                query = f"""SELECT count({left} + {right});"""
                restype = None

                if 'cal::relative_duration' in [
                        ldesc.typename, rdesc.typename] \
                        and 'duration' in [ldesc.typename, rdesc.typename]:
                    restype = 'cal::relative_duration'
                elif ldesc.signed:  # duration/cal::relative_duration
                    restype = rdesc.typename
                elif rdesc.signed:  # duration/cal::relative_duration
                    restype = ldesc.typename

                if restype:
                    await self.assert_query_result(query, [1])
                    await self.assert_query_result(
                        f"""SELECT ({left} + {right}) IS {restype};""",
                        [True])
                else:
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_arithmetic_06(self):
        # Tests (3), (4) - various date/time combinations.
        expected_error_msg = 'cannot be applied to operands'

        for left, ldesc in get_test_items(datetime=True):
            for right, rdesc in get_test_items(datetime=True):
                query = f"""SELECT count({left} - {right});"""

                if 'cal::relative_duration' in [
                        ldesc.typename, rdesc.typename] \
                        and 'duration' in [ldesc.typename, rdesc.typename]:
                    restype = 'cal::relative_duration'
                elif rdesc.signed:  # duration/cal::relative_duration
                    restype = ldesc.typename
                elif rdesc.typename == ldesc.typename:
                    if rdesc.typename.startswith('cal::local_'):
                        # TODO(tailhook) restype = 'cal::relative_duration'
                        restype = None
                    else:
                        restype = 'duration'
                else:
                    restype = None

                if restype:
                    await self.assert_query_result(query, [1])
                    await self.assert_query_result(
                        f"""SELECT ({left} - {right}) IS {restype};""",
                        [True])
                else:
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_arithmetic_07(self):
        # various date/time combinations don't define '*', '/', '//',
        # '%', '^'.
        dts = get_test_values(datetime=True)
        expected_error_msg = 'cannot be applied to operands'

        for left in dts:
            for right in dts:
                for op in ['*', '/', '//', '%', '^']:
                    query = f"""SELECT count({left} {op} {right});"""
                    # every combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_arithmetic_08(self):
        # Test (5) - decimal is incompatible with everything except integers
        expected_error_msg = 'cannot be applied to operands'

        for left in get_test_values(anynumeric=True):
            for right in get_test_values(anyint=False, anynumeric=False):
                for op in ['+', '-', '*', '/', '//', '%', '^']:
                    query = f"""SELECT {left} {op} {right};"""
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

        for left, ldesc in get_test_items(anynumeric=True):
            for right in get_test_values(anyint=True):
                for op in ['+', '-', '*', '%']:
                    # bigint/decimal are "contagious"
                    await self.assert_query_result(
                        f"""
                            SELECT ({left} {op} {right}) IS {ldesc.typename};
                        """,
                        [True],
                    )

                    await self.assert_query_result(
                        f"""
                            SELECT ({right} {op} {left}) IS {ldesc.typename};
                        """,
                        [True],
                    )

        for left, ldesc in get_test_items(anynumeric=True):
            for right in get_test_values(anyint=True):
                op = '//'
                await self.assert_query_result(
                    f"""
                        SELECT ({left} {op} {right}) IS {ldesc.typename};
                    """,
                    [True],
                )

                await self.assert_query_result(
                    f"""
                        SELECT ({right} {op} {left}) IS {ldesc.typename};
                    """,
                    [True],
                )

        for left in get_test_values(anynumeric=True):
            for right in get_test_values(anyint=True):
                # regular division and power with anynumeric always
                # results in decimal.
                for op in ['/', '^']:
                    await self.assert_query_result(
                        f"""
                            SELECT ({left} {op} {right}) IS decimal;
                        """,
                        [True],
                    )

                    await self.assert_query_result(
                        f"""
                            SELECT ({right} {op} {left}) IS decimal;
                        """,
                        [True],
                    )

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

        for left, ldesc in get_test_items(anyreal=True, anynumeric=False):
            for right, rdesc in get_test_items(anyreal=True, anynumeric=False):
                for op in ['+', '-', '*', '//', '%']:
                    types = [ldesc.typename, rdesc.typename]
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
                    await self.assert_query_result(
                        query, [True], msg=query)

    async def test_edgeql_expr_valid_arithmetic_10(self):
        # Test (5) '/', '^' for non-decimals.

        for left, ldesc in get_test_items(anyreal=True, anynumeric=False):
            for right, rdesc in get_test_items(anyreal=True, anynumeric=False):
                for op in ['/', '^']:
                    # The result type is always a float because power
                    # can act as division.
                    #
                    # If the operands are int16 or float32, then the
                    # result is float32.
                    # In all other cases the result is a float64.
                    types = {ldesc.typename, rdesc.typename}

                    if types.issubset({'int16', 'float32'}):
                        rtype = 'float32'
                    else:
                        rtype = 'float64'

                    query = f"""SELECT ({left} {op} {right}) IS {rtype};"""
                    await self.assert_query_result(
                        query, [True], msg=query)

    async def test_edgeql_expr_valid_arithmetic_11(self):
        # Test that we're suggesting to use the "++" operator instead
        # of "+" for strings/bytes/arrays.

        for query in {'SELECT "a" + "b"', 'SELECT b"a" + b"b"',
                      'SELECT ["a"] + ["b"]'}:
            with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\+' cannot be applied .*",
                _hint='Consider using the "++" operator for concatenation'
            ):
                async with self.con.transaction():
                    await self.con.query_single(query)

    async def test_edgeql_expr_valid_setop_01(self):
        # use every scalar with DISTINCT
        for right, desc in get_test_items():
            query = f"""SELECT count(DISTINCT {{{right}, {right}}});"""
            # this operation should always be valid and get count of 1
            await self.assert_query_result(query, {1})

            query = f"""
                SELECT (DISTINCT {{{right}, {right}}}) IS {desc.typename};
            """
            # this operation should always be valid
            await self.assert_query_result(query, {True})

    async def test_edgeql_expr_valid_setop_02(self):
        expected_error_msg = "operator 'UNION' cannot be applied"
        # UNION all non-decimal numerics with all other scalars
        for left in get_test_values(anyreal=True, anynumeric=False):
            for right in get_test_values(anyreal=False):
                query = f"""SELECT {left} UNION {right};"""
                # every combination must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    async with self.con.transaction():
                        await self.con.execute(query)

    async def test_edgeql_expr_valid_setop_03(self):
        # UNION all non-decimal numerics with each other
        for left, ldesc in get_test_items(anyreal=True, anynumeric=False):
            for right, rdesc in get_test_items(anyreal=True, anynumeric=False):
                query = f"""SELECT {left} UNION {right};"""
                # every combination must be valid and be {1, 1}
                await self.assert_query_result(query, [1, 1])

                types = [ldesc.typename, rdesc.typename]
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

                query = f"""
                    SELECT (INTROSPECT TYPEOF ({left} UNION {right})).name;
                """
                # this operation should always be valid
                await self.assert_query_result(
                    query, {f'std::{rtype}'})

    async def test_edgeql_expr_valid_setop_04(self):
        expected_error_msg = "operator 'UNION' cannot be applied"
        # UNION all non-numerics with all scalars
        for left, ldesc in get_test_items(anyreal=False):
            for right, rdesc in get_test_items():
                query = f"""SELECT count({left} UNION {right});"""

                if ldesc.typename == rdesc.typename:
                    # these scalars can only be UNIONed with
                    # themselves implicitly
                    await self.assert_query_result(query, [2])

                    query = f"""
                        SELECT (INTROSPECT TYPEOF ({left} UNION {right})).name;
                    """
                    # this operation should always be valid
                    if rdesc.typename.startswith('cal::'):
                        desc_typename = rdesc.typename
                    else:
                        desc_typename = 'std::' + rdesc.typename
                    await self.assert_query_result(
                        query, {desc_typename})

                else:
                    # every other combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_setop_05(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = "operator 'UNION' cannot be applied"
        # decimal UNION non-numerics
        for left in get_test_values(anynumeric=True):
            for right in get_test_values(anyreal=False):
                query = f"""SELECT {left} UNION {right};"""
                # every combination must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    async with self.con.transaction():
                        await self.con.execute(query)

    async def test_edgeql_expr_valid_setop_06(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = "operator 'UNION' cannot be applied"
        # decimal UNION numerics
        for left, left_t in get_test_items(anynumeric=True):
            for right in get_test_values(anyint=True):
                query = f"""SELECT count({left} UNION {right});"""
                # decimals and integers can be UNIONed in any
                # combination
                await self.assert_query_result(query, [2])

                query = f"""
                    SELECT (INTROSPECT TYPEOF ({left} UNION {right})).name;
                """
                # this operation should always be valid
                await self.assert_query_result(
                    query, {f'std::{left_t.typename}'})

        for left in get_test_values(anynumeric=True):
            for right in get_test_values(anyfloat=True):
                query = f"""SELECT count({left} UNION {right});"""

                # decimal UNION float is illegal
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    async with self.con.transaction():
                        await self.con.execute(query)

    async def test_edgeql_expr_valid_setop_07(self):
        expected_error_msg = 'cannot be applied to operands'
        # IF ELSE with every scalar as the condition
        for val in get_test_values():
            query = f"""SELECT 1 IF {val} ELSE 2;"""
            if val == '<bool>True':
                await self.assert_query_result(query, [1])
            else:
                # every other combination must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    async with self.con.transaction():
                        await self.con.execute(query)

    # Operator '??' should work just like UNION in terms of types.
    # Operator A IF C ELSE B should work exactly like A UNION B in
    # terms of types.
    async def test_edgeql_expr_valid_setop_08(self):
        expected_error_msg = "cannot be applied to operands"
        # test all non-decimal numerics with all other scalars
        for left in get_test_values(anyreal=True, anynumeric=False):
            for right in get_test_values(anyreal=False):
                # random is used in the IF to prevent constant
                # folding, because we really care about both of the
                # outer operands being processed
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_setop_09(self):
        # test all non-decimal numerics with each other
        for left, ldesc in get_test_items(anyreal=True, anynumeric=False):
            for right, rdesc in get_test_items(anyreal=True, anynumeric=False):
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every combination must be valid and be 1
                    await self.assert_query_result(query, [1])

                    types = [ldesc.typename, rdesc.typename]
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

                    query = f"""
                        SELECT (INTROSPECT TYPEOF ({left} {op} {right})).name;
                    """
                    # this operation should always be valid
                    await self.assert_query_result(
                        query, {f'std::{rtype}'})

    async def test_edgeql_expr_valid_setop_10(self):
        expected_error_msg = "cannot be applied to operands"
        # test all non-numerics with all scalars
        for left in get_test_values(anyreal=False):
            for right, rdesc in get_test_items():
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT count({left} {op} {right});"""

                    if left == right:
                        # these scalars can only be combined with
                        # themselves implicitly
                        await self.assert_query_result(query, [1])

                        query = f"""
                            SELECT ({left} {op} {right}) IS {rdesc.typename};
                        """
                        # this operation should always be valid
                        await self.assert_query_result(query, {True})

                    else:
                        # every other combination must produce an error
                        with self.assertRaisesRegex(edgedb.QueryError,
                                                    expected_error_msg,
                                                    msg=query):
                            async with self.con.transaction():
                                await self.con.execute(query)

    async def test_edgeql_expr_valid_setop_11(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = 'cannot be applied to operands'
        # decimal combined with non-numerics
        for left in get_test_values(anynumeric=True):
            for right in get_test_values(anyreal=False):
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    # every combination must produce an error
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

    async def test_edgeql_expr_valid_setop_12(self):
        # decimals are tricky because integers implicitly cast into
        # them and floats don't
        expected_error_msg = 'cannot be applied to operands'
        # decimal combined with numerics
        for left in get_test_values(anynumeric=True):
            for right in get_test_values(anyfloat=True):
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""
                    # decimal combined with float is illegal
                    with self.assertRaisesRegex(edgedb.QueryError,
                                                expected_error_msg,
                                                msg=query):
                        async with self.con.transaction():
                            await self.con.execute(query)

        for left, left_t in get_test_items(anynumeric=True):
            for right in get_test_values(anyint=True):
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT {left} {op} {right};"""

                    # decimals and integers can be UNIONed in any
                    # combination
                    await self.assert_query_result(query, [1])

                    query = f"""
                        SELECT ({left} {op} {right}) IS {left_t.typename};
                    """
                    # this operation should always be valid
                    await self.assert_query_result(query, {True})

    async def test_edgeql_expr_valid_setop_13(self):
        # IF ELSE with every numeric scalar combined with an int64
        for val in get_test_values(anyreal=True):
            query = f"""SELECT 1 IF True ELSE {val};"""
            await self.assert_query_result(query, [1])

    async def test_edgeql_expr_valid_setop_14(self):
        # testing IF ELSE with mismatched operand types
        expected_error_msg = 'cannot be applied to operands'
        hint = ("The IF and ELSE result clauses must be of compatible "
                "types, while the condition clause must be "
                "'std::bool'. "
                "Consider using an explicit type cast or a conversion "
                "function.")
        # IF ELSE with every non-numeric scalar combined with an int64
        for val in get_test_values(anyreal=False):
            query = f"""SELECT 1 IF True ELSE {val};"""
            # every other combination must produce an error
            with self.assertRaisesRegex(edgedb.QueryError,
                                        expected_error_msg,
                                        msg=query, _hint=hint):
                async with self.con.transaction():
                    await self.con.execute(query)

    async def test_edgeql_expr_valid_bool_01(self):
        expected_error_msg = 'cannot be applied to operands'
        # use every scalar combination with AND and OR
        for left in get_test_values():
            for right in get_test_values():
                for op in ['AND', 'OR']:
                    query = f"""SELECT {left} {op} {right};"""
                    if left == right == '<bool>True':
                        # this operation should be valid and True
                        await self.assert_query_result(query, {True})
                    else:
                        # every combination except for bool OP bool is invalid
                        with self.assertRaisesRegex(edgedb.QueryError,
                                                    expected_error_msg,
                                                    msg=query):
                            async with self.con.transaction():
                                await self.con.execute(query)

    async def test_edgeql_expr_valid_bool_02(self):
        expected_error_msg = 'cannot be applied to operands'
        # use every scalar with NOT
        for right in get_test_values():
            query = f"""SELECT NOT {right};"""
            if right == '<bool>True':
                # this operation should be valid and False
                await self.assert_query_result(query, {False})
            else:
                # every other scalar must produce an error
                with self.assertRaisesRegex(edgedb.QueryError,
                                            expected_error_msg,
                                            msg=query):
                    async with self.con.transaction():
                        await self.con.execute(query)

    async def test_edgeql_expr_valid_setbool_01(self):
        # use every scalar with EXISTS
        for right in get_test_values():
            query = f"""SELECT EXISTS {right};"""
            # this operation should always be valid and True
            await self.assert_query_result(query, {True})

    async def test_edgeql_expr_valid_collection_01(self):
        await self.assert_query_result(
            r'''SELECT [1] = [<decimal>1];''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_02(self):
        await self.assert_query_result(
            r'''
                SELECT [<int16>1] = [<decimal>1];
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_03(self):
        await self.assert_query_result(
            r'''
                SELECT (1,) = (<decimal>1,);
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_04(self):
        await self.assert_query_result(
            '''
                SELECT
                    [([(1,          )],)] =
                    [([(<decimal>1, )],)];
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_05(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (1, <int32>2, (
                        (<int16>3, <int64>4), <decimal>5)) =
                    (<decimal>1, <decimal>2, (
                        (<decimal>3, <decimal>4), <decimal>5));
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_06(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (1, <int32>2, (
                        [<int16>3, <int32>4], <decimal>5)) =
                    (<decimal>1, <decimal>2, (
                        [<decimal>3, <decimal>4], <decimal>5));
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_07(self):
        await self.assert_query_result(
            r'''
                SELECT [1] ?= [<decimal>1];
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_08(self):
        await self.assert_query_result(
            r'''
                SELECT (1,) ?= (<decimal>1,);
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_09(self):
        await self.assert_query_result(
            '''
                SELECT
                    [([(1,          )],)] ?=
                    [([(<decimal>1, )],)];
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_10(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (1, <int32>2, (
                        (<int16>3, <int32>4), <decimal>5)) ?=
                    (<decimal>1, <decimal>2, (
                        (<decimal>3, <decimal>4), <decimal>5));
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_11(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (1, <int32>2, (
                        [<int16>3, <int32>4], <decimal>5)) ?=
                    (<decimal>1, <decimal>2, (
                        [<decimal>3, <decimal>4], <decimal>5));
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_12(self):
        await self.assert_query_result(
            r'''
                SELECT [1] IN [<decimal>1];
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_13(self):
        await self.assert_query_result(
            r'''
                SELECT (1,) IN (<decimal>1,);
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_14(self):
        await self.assert_query_result(
            '''
                SELECT
                    [([(1,          )],)] IN
                    [([(<decimal>1, )],)];
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_15(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (1, <int32>2, ( (<int16>3, <int32>4), <decimal>5)) IN
                    (<decimal>1, <decimal>2, (
                        (<decimal>3, <decimal>4), <decimal>5));
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_collection_16(self):
        await self.assert_query_result(
            r'''
                SELECT
                    (1, <int32>2, (
                        [<int16>3, <int32>4], <decimal>5)) IN
                    (<decimal>1, <decimal>2, (
                        [<decimal>3, <decimal>4], <decimal>5));
            ''',
            [True]
        )

    async def test_edgeql_expr_valid_minmax_01(self):
        for val in get_test_values():
            for fn in ['min', 'max']:
                query = f"""SELECT {fn}({val}) = {val};"""
                await self.assert_query_result(query, {True})

    async def test_edgeql_expr_valid_minmax_02(self):
        for val in get_test_values():
            for fn in ['min', 'max']:
                # same as the previous test, but for arrays
                query = f"""SELECT {fn}([{val}]) = [{val}];"""
                await self.assert_query_result(query, {True})

    async def test_edgeql_expr_valid_minmax_03(self):
        for val in get_test_values():
            for fn in ['min', 'max']:
                # same as the previous test, but for tuples
                query = f"""SELECT {fn}(({val},)) = ({val},);"""
                await self.assert_query_result(query, {True})

    async def test_edgeql_expr_bytes_op_01(self):
        await self.assert_query_result(
            r'''
                SELECT len(b'123' ++ b'54');
            ''',
            [5]
        )

    async def test_edgeql_expr_bytes_op_02(self):
        await self.assert_query_result(
            r'''SELECT (b'123' ++ b'54')[-1] = b'4';''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (b'123' ++ b'54')[0:2] = b'12';''',
            [True],
        )

    async def test_edgeql_expr_bytes_op_03(self):
        await self.assert_query_result(
            r'''
                WITH x := rb'test\raw\x01' ++ br'\now\x02' ++ b'\x03\x04',
                SELECT x = b"test\\raw\\x01\\now\\x02\x03\x04";
            ''',
            [True],
        )

    async def test_edgeql_expr_paths_01(self):
        cases = [
            "Issue.owner.name",
            "`Issue`.`owner`.`name`",
        ]

        for case in cases:
            await self.con.execute('''
                SELECT
                    Issue {
                        number
                    }
                FILTER
                    %s = 'Elvis';
            ''' % (case,))

    async def test_edgeql_expr_paths_02(self):
        await self.assert_query_result(
            r"""
                SELECT (1, (2, 3), 4).1.0;
            """,
            [2],
        )

    async def test_edgeql_expr_paths_03(self):
        # NOTE: The expression `.1` in this test is not a float,
        # instead it is a partial path (like `.name`). It is
        # syntactically legal (see test_edgeql_syntax_constants_09),
        # but will fail to resolve to anything.
        with self.assertRaisesRegex(
                edgedb.QueryError, r'could not resolve partial path'):
            await self.con.execute(r"""
                SELECT .1;
            """)

    async def test_edgeql_expr_paths_04(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
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
            await self.con.execute(r"""
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
            await self.con.execute(r"""
                SELECT Issue.owner {
                    foo := Issue.number
                };
            """)

    async def test_edgeql_expr_paths_08(self):
        # `Issue.number` in FILTER is illegal because it shares a
        # prefix `Issue` with `Issue.owner` which is defined in an
        # outer scope.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'Issue.number' changes the interpretation of 'Issue'"):
            await self.con.execute(r"""
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
            await self.con.execute(r"""
                UPDATE Issue.related_to
                SET {
                    related_to := Issue
                };
            """)

    async def test_edgeql_expr_polymorphic_01(self):
        await self.con.execute(r"""
            SELECT Text {
                [IS Issue].number,
                [IS Issue].related_to,
                [IS Issue].`priority`,
                [IS Comment].owner: {
                    name
                }
            };
        """)

        await self.con.execute(r"""
            SELECT Owned {
                [IS Named].name
            };
        """)

    async def test_edgeql_expr_cast_01(self):
        await self.assert_query_result(
            r'''SELECT <std::str>123;''',
            ['123'],
        )

        await self.assert_query_result(
            r'''SELECT <std::int64>"123";''',
            [123],
        )

        await self.assert_query_result(
            r'''SELECT <std::str>123 ++ 'qw';''',
            ['123qw'],
        )

        await self.assert_query_result(
            r'''SELECT <std::int64>"123" + 9000;''',
            [9123],
        )

        await self.assert_query_result(
            r'''SELECT <std::int64>"123" * 100;''',
            [12300],
        )

        await self.assert_query_result(
            r'''SELECT <std::str>(123 * 2);''',
            ['246'],
        )

    async def test_edgeql_expr_cast_02(self):
        # testing precedence of casting vs. multiplication
        #
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\*' cannot .* 'std::str' and 'std::int64'"):

            await self.con.query_single("""
                SELECT <std::str>123 * 2;
            """)

    async def test_edgeql_expr_cast_03(self):
        await self.assert_query_result(
            r"""
                SELECT <std::str><std::int64><std::float64>'123.45' ++ 'foo';
            """,
            ['123foo'],
        )

    async def test_edgeql_expr_cast_04(self):
        await self.assert_query_result(
            r"""
                SELECT <str><int64><float64>'123.45' ++ 'foo';
            """,
            ['123foo'],
        )

    async def test_edgeql_expr_cast_05(self):
        await self.assert_query_result(
            r"""
                SELECT <array<int64>>['123', '11'];
            """,
            [[123, 11]],
        )

    async def test_edgeql_expr_cast_08(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'cannot cast.*tuple.*to.*array.*'):
            await self.con.query_json(r"""
                SELECT <array<int64>>(123, 11);
            """)

    async def test_edgeql_expr_cast_09(self):
        await self.assert_query_result(
            r'''SELECT <tuple<str, int64>> ('foo', 42);''',
            [['foo', 42]],
        )

        await self.assert_query_result(
            r'''SELECT <tuple<str, int64>> (1, 2);''',
            [['1', 2]],
        )

        await self.assert_query_result(
            r'''SELECT <tuple<a: str, b: int64>> ('foo', 42);''',
            [{'a': 'foo', 'b': 42}],
        )

        await self.assert_query_result(
            r'''SELECT <tuple<__std__::str, int64>> ('foo', 42);''',
            [['foo', 42]],
        )

        await self.assert_query_result(
            r'''SELECT <__std__::int16>1;''',
            [1],
        )

    async def test_edgeql_expr_cast_10(self):
        await self.assert_query_result(
            r'''
                SELECT <array<tuple<EmulatedEnum>>>
                  (SELECT [('v1',)] ++ [('v2',)])
            ''',
            [[["v1"], ["v2"]]],
        )

        await self.assert_query_result(
            r'''
                SELECT <tuple<array<EmulatedEnum>>>
                  (SELECT (['v1'] ++ ['v2'],))
            ''',
            [[["v1", "v2"]]],
        )

    async def test_edgeql_expr_implicit_cast_01(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<int32>1 + 3)).name;''',
            ['std::int64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<int16>1 + 3)).name;''',
            ['std::int64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<int16>1 + <int32>3)).name;''',
            ['std::int32'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(1 + <float32>3.1)).name;''',
            # according to the standard implicit casts, most of the
            # ints can only be upcast to float64
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<int16>1 + <float32>3.1)).name;''',
            # int16 can upcast to float32
            ['std::float32'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<int16>1 + <float64>3.1)).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF({1, <float32>2.1})).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF({1, 2.1})).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(-2.1)).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF({1, <decimal>2.1})).name;''',
            ['std::decimal'],
        )

    async def test_edgeql_expr_implicit_cast_02(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<float32>1 + <float64>2)).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<int32>1 + <float32>2)).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(<int64>1 + <float32>2)).name;''',
            ['std::float64'],
        )

    async def test_edgeql_expr_implicit_cast_03(self):
        # coalescing forces the left scalar operand to be implicitly
        # upcast to the right one even if the right one is never
        # technically evaluated (function not called, etc.)
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(3 // 2)).name;''',
            ['std::int64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF((3 // 2) ?? <float64>{})).name;''',
            ['std::float64'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(3 / 2 ?? <decimal>{})).name;''',
            ['std::decimal'],
        )

        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF(3 // 2 ?? sum({1, 2.0}))).name;''',
            ['std::float64'],
        )

    async def test_edgeql_expr_implicit_cast_04(self):
        # IF should also force implicit casts of the two options
        await self.assert_query_result(
            r'''SELECT 3 / (2 IF TRUE ELSE 2.0);''',
            [1.5],
        )

        await self.assert_query_result(
            r'''SELECT 3 / (2 IF random() > -1 ELSE 2.0);''',
            [1.5],
        )

        await self.assert_query_result(
            r'''SELECT 3 / (2 IF FALSE ELSE 2.0);''',
            [1.5],
        )

        await self.assert_query_result(
            r'''SELECT 3 / (2 IF random() < -1 ELSE 2.0);''',
            [1.5],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator.*IF.*cannot.*'std::int64'.*'std::str'"):

            await self.con.query("""
                SELECT 3 / (2 IF FALSE ELSE '1');
            """)

    async def test_edgeql_expr_implicit_cast_05(self):
        await self.assert_query_result(
            r'''SELECT {[1, 2.0], [3, 4.5]};''',
            [[1, 2], [3, 4.5]],
        )

        await self.assert_query_result(
            r'''SELECT {[1, 2], [3, 4.5]};''',
            [[1, 2], [3, 4.5]],
        )

    async def test_edgeql_expr_implicit_cast_06(self):
        await self.assert_query_result(
            r'''SELECT {(1, 2.0), (3, 4.5)};''',
            [[1, 2], [3, 4.5]],
        )

        await self.assert_query_result(
            r'''SELECT {(1, 2), (3, 4.5)};''',
            [[1, 2], [3, 4.5]],
        )

        await self.assert_query_result(
            r'''SELECT {(1, 2), (3, 4.5)} FILTER true;''',
            [[1, 2], [3, 4.5]],
        )

        await self.assert_query_result(
            r'''SELECT {(3, 4.5), (1, 2.0)};''',
            [[3, 4.5], [1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT {(x := 1, y := 2.0), (x := 3, y := 4.5)};''',
            [{"x": 1, "y": 2}, {"x": 3, "y": 4.5}],
        )

        await self.assert_query_result(
            r'''SELECT {(x := 1, y := 2), (x := 3, y := 4.5)};''',
            [{"x": 1, "y": 2}, {"x": 3, "y": 4.5}],
        )

        await self.assert_query_result(
            r'''SELECT {(x := 3, y := 4.5), (x := 1, y := 2)};''',
            [{"x": 3, "y": 4.5}, {"x": 1, "y": 2}],
        )

        await self.assert_query_result(
            r'''SELECT {(x := 1, y := 2), (a := 3, b := 4.5)};''',
            [[1, 2], [3, 4.5]],
        )

        await self.assert_query_result(
            r'''SELECT {(a := 3, b := 4.5), (x := 1, y := 2)};''',
            [[3, 4.5], [1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT {(1, 2), (a := 3, b := 4.5)};''',
            [[1, 2], [3, 4.5]],
        )

        await self.assert_query_result(
            r'''SELECT {(a := 3, b := 4.5), (1, 2)};''',
            [[3, 4.5], [1, 2]],
        )

    async def test_edgeql_expr_implicit_cast_07(self):
        await self.assert_query_result(
            r"""
                WITH
                    MODULE schema,
                    A := (
                        SELECT ObjectType {
                            a := 1,
                            b := 1 + 0 * random(),  # float64
                            c := 1 + 0 * <int64>random(),
                        })
                SELECT (3 / (A.a + A.b), 3 / (A.a + A.c)) LIMIT 1;
            """,
            [[1.5, 1.5]],
        )

    async def test_edgeql_expr_implicit_cast_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, "operator 'UNION' cannot be applied"):
            await self.con.execute(r'''
                SELECT 1.0 UNION <decimal>2.0;
            ''')

    async def test_edgeql_expr_introspect_01(self):
        await self.assert_query_result(
            r"""
                SELECT (INTROSPECT TYPEOF 'foo').name;
            """,
            ['std::str'],
        )

    async def test_edgeql_expr_introspect_02(self):
        await self.assert_query_result(
            r"""
                SELECT (INTROSPECT std::float64).name;
            """,
            ['std::float64'],
        )

    async def test_edgeql_expr_introspect_03(self):
        await self.assert_query_result(
            r"""
                SELECT (INTROSPECT TYPEOF schema::ObjectType).name;
            """,
            ['schema::ObjectType'],
        )

    async def test_edgeql_expr_introspect_04(self):
        await self.assert_query_result(
            r"""
                WITH A := {1.0, 2.0}
                SELECT (count(A), (INTROSPECT TYPEOF A).name);
            """,
            [[2, 'std::float64']],
        )

    async def test_edgeql_expr_introspect_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot introspect collection types'):
            await self.assert_query_result(
                r"""
                    SELECT (INTROSPECT (tuple<int64>)).name;
                """,
                ['tuple<std::int64>'],
            )

    async def test_edgeql_expr_introspect_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot introspect transient type variant'):
            await self.assert_query_result(
                r"""
                    WITH A := (SELECT schema::Type { foo := 'bar' })
                    SELECT 'foo' IN (INTROSPECT A).pointers.name;
                """,
                [True],
            )

    async def test_edgeql_expr_set_01(self):
        await self.assert_query_result(
            r'''SELECT <int64>{};''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT {1};''',
            {1},
        )

        await self.assert_query_result(
            r'''SELECT {'foo'};''',
            ['foo'],
        )

        await self.assert_query_result(
            r'''SELECT {1} = 1;''',
            [True],
        )

    async def test_edgeql_expr_set_02(self):
        await self.assert_query_result(
            """
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
            """,
            [
                'schema::Alias',
                'schema::Annotation',
                'schema::AnnotationSubject',
                'schema::Array',
                'schema::Delta',
                'schema::Object',
                'schema::ObjectType',
                'schema::Operator',
            ],
        )

    async def test_edgeql_expr_set_03(self):
        await self.assert_query_result(
            r"""
                # "nested" sets are merged using UNION
                SELECT _ := {{2, 3, {1, 4}, 4}, {4, 1}}
                ORDER BY _;
            """,
            [1, 1, 2, 3, 4, 4, 4],
        )

    async def test_edgeql_expr_array_01(self):
        await self.assert_query_result(
            r'''SELECT [1];''',
            [[1]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5];''',
            [[1, 2, 3, 4, 5]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][2];''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][-2];''',
            [4],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][2:4];''',
            [[3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][2:];''',
            [[3, 4, 5]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][:2];''',
            [[1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][2:-1];''',
            [[3, 4]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][-2:];''',
            [[4, 5]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][:-2];''',
            [[1, 2, 3]],
            # slice of something non-existent
        )

        await self.assert_query_result(
            r'''SELECT [1, 2][10:11];''',
            [[]],
        )

        await self.assert_query_result(
            r'''SELECT <array<int64>>[];''',
            [[]],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][<int16>2];''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT [1, 2, 3, 4, 5][<int32>2];''',
            [3],
        )

    async def test_edgeql_expr_array_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'could not determine array type'):

            await self.con.query("""
                SELECT [1, '1'];
            """)

    async def test_edgeql_expr_array_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot index array by.*str'):

            await self.con.query_single("""
                SELECT [1, 2]['1'];
            """)

    async def test_edgeql_expr_array_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'expression returns value of indeterminate type'):

            await self.con.query_json("""
                SELECT [];
            """)

    async def test_edgeql_expr_array_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"index indirection cannot be applied to "
                r"scalar type 'std::int64'"):

            await self.con.query_json("""
                SELECT [0, 1, 2][[1][0] [2][0]];
            """)

    async def test_edgeql_expr_array_concat_01(self):
        await self.assert_query_result(
            '''
                SELECT [1, 2] ++ [3, 4];
            ''',
            [
                [1, 2, 3, 4]
            ]
        )

    async def test_edgeql_expr_array_concat_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\+\+' cannot.*int64.*str"):

            await self.con.execute('''
                SELECT [1, 2] ++ ['a'];
            ''')

    async def test_edgeql_expr_array_concat_03(self):
        await self.assert_query_result(
            R'''
                SELECT [(1, 'a')] ++ [(2.0, $$\$$), (3.0, r'\n')];
            ''',
            [
                [[1, 'a'], [2, '\\'], [3, R'\n']]
            ]
        )

    async def test_edgeql_expr_array_06(self):
        await self.assert_query_result(
            '''
                SELECT [1, <int64>{}];
            ''',
            [],
        )

    async def test_edgeql_expr_array_07(self):
        await self.assert_query_result(
            '''
                WITH
                    A := {1, 2},
                    B := <int64>{}
                SELECT [A, B];
            ''',
            [],
        )

    async def test_edgeql_expr_array_08(self):
        await self.assert_query_result(
            '''
                WITH
                    MODULE schema,
                    A := {'a', 'b'},
                    # B is an empty set
                    B := (SELECT Type FILTER Type.name = 'n/a').name
                SELECT [A, B];
            ''',
            [],
        )

    async def test_edgeql_expr_array_09(self):
        await self.assert_query_result(
            '''
                WITH
                    MODULE schema,
                    A := (SELECT ScalarType
                          FILTER .name = 'default::issue_num_t')
                SELECT [A.name, A.default];
            ''',
            [],
        )

    async def test_edgeql_expr_array_10(self):
        with self.assertRaisesRegex(edgedb.QueryError, 'nested array'):
            await self.con.execute(r'''
                SELECT [[1, 2], [3, 4]];
            ''')

    async def test_edgeql_expr_array_11(self):
        with self.assertRaisesRegex(edgedb.QueryError, 'nested array'):
            await self.con.execute(r'''
                SELECT [array_agg({1, 2})];
            ''')

    async def test_edgeql_expr_array_12(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.con.execute(r'''
                SELECT array_agg([1, 2, 3]);
            ''')

    async def test_edgeql_expr_array_13(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r"nested arrays are not supported"):
            await self.con.execute(r'''
                SELECT array_agg(array_agg({1, 2 ,3}));
            ''')

    async def test_edgeql_expr_array_14(self):
        await self.assert_query_result(
            '''
                SELECT [([([1],)],)];
            ''',
            [   # result set
                [[[[[1]]]]]
            ],
        )

    async def test_edgeql_expr_array_15(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'array index 10 is out of bounds'):
            await self.con.execute("""
                SELECT [1, 2, 3][10];
            """)

    async def test_edgeql_expr_array_16(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'array index -10 is out of bounds'):
            await self.con.execute("""
                SELECT [1, 2, 3][-10];
            """)

    async def test_edgeql_expr_array_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot index array by.*float'):

            await self.con.execute("""
                SELECT [1, 2][1.0];
            """)

    async def test_edgeql_expr_array_18(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice array by.*float'):

            await self.con.execute("""
                SELECT [1, 2][1.0:3];
            """)

    async def test_edgeql_expr_array_19(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice array by.*str'):

            await self.con.execute("""
                SELECT [1, 2][1:'3'];
            """)

    async def test_edgeql_expr_array_20(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot index array by std::float64'):

            await self.con.execute("""
                SELECT [1, 2][2^40];
            """)

    async def test_edgeql_expr_array_21(self):
        # it should be technically possible to infer the type of the array
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator 'UNION' cannot be applied to operands.*anytype.*"):

            await self.con.execute("""
                SELECT [1, 2] UNION [];
            """)

    async def test_edgeql_expr_array_22(self):
        await self.assert_query_result(
            '''
                SELECT schema::ObjectType {
                    foo := {
                        [(a := 1, b := 2)],
                        [(a := 3, b := 4)],
                        <array <tuple<a: int64, b: int64>>>[],
                    }
                } LIMIT 1
            ''',
            [
                {
                    'foo': [
                        [{'a': 1, 'b': 2}],
                        [{'a': 3, 'b': 4}],
                        [],
                    ],
                }
            ],
        )

    async def test_edgeql_expr_array_23(self):
        await self.assert_query_result(
            r'''
            WITH X := [(1, 2)],
            SELECT X FILTER X[0].0 = 1;
            ''',
            [[[1, 2]]],
        )

    async def test_edgeql_expr_array_24(self):
        await self.assert_query_result(
            r'''
            WITH X := [(foo := 1, bar := 2)],
            SELECT X FILTER X[0].foo = 1;
            ''',
            [[{"bar": 2, "foo": 1}]],
        )

    async def test_edgeql_expr_array_25(self):
        await self.assert_query_result(
            r'''
            SELECT X := [(foo := 1, bar := 2)] FILTER X[0].foo = 1;
            ''',
            [[{"bar": 2, "foo": 1}]],
        )

    async def test_edgeql_expr_coalesce_01(self):
        await self.assert_query_result(
            r'''SELECT <int64>{} ?? 4 ?? 5;''',
            [4],
        )

        await self.assert_query_result(
            r'''SELECT <str>{} ?? 'foo' ?? 'bar';''',
            ['foo'],
        )

        await self.assert_query_result(
            r'''SELECT 4 ?? <int64>{} ?? 5;''',
            [4],
        )

        await self.assert_query_result(
            r'''SELECT 'foo' ?? <str>{} ?? 'bar';''',
            ['foo'],
        )

        await self.assert_query_result(
            r'''SELECT <str>{} ?? 'bar' = 'bar';''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT 4^<int64>{} ?? 2;''',
            [2],  # ^ binds more tightly
        )

        await self.assert_query_result(
            r'''SELECT 4+<int64>{} ?? 2;''',
            [6],
        )

        await self.assert_query_result(
            r'''SELECT 4*<int64>{} ?? 2;''',
            [8],
        )

        await self.assert_query_result(
            r'''SELECT -<int64>{} ?? 2;''',
            [2],
        )

        await self.assert_query_result(
            r'''SELECT -<int64>{} ?? -2 + 1;''',
            [-1],
        )

        await self.assert_query_result(
            r'''SELECT <int64>{} ?? <int64>{};''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT <int64>{} ?? <int64>{} ?? <int64>{};''',
            [],
        )

    async def test_edgeql_expr_string_01(self):
        await self.assert_query_result(
            r'''SELECT 'qwerty';''',
            ['qwerty'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[2];''',
            ['e'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[-2];''',
            ['t'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[2:4];''',
            ['er'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[2:];''',
            ['erty'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[:2];''',
            ['qw'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[2:-1];''',
            ['ert'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[-2:];''',
            ['ty'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[:-2];''',
            ['qwer'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[<int16>2];''',
            ['e'],
        )

        await self.assert_query_result(
            r'''SELECT 'qwerty'[<int32>2];''',
            ['e'],
        )

    async def test_edgeql_expr_string_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot index string by.*str'):

            await self.con.query_single("""
                SELECT '123'['1'];
            """)

    async def test_edgeql_expr_string_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'string index 10 is out of bounds'):
            await self.con.query_json("""
                SELECT '123'[10];
            """)

    async def test_edgeql_expr_string_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'string index -10 is out of bounds'):
            await self.con.query("""
                SELECT '123'[-10];
            """)

    async def test_edgeql_expr_string_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot index string by.*float'):

            await self.con.query("""
                SELECT '123'[-1.0];
            """)

    async def test_edgeql_expr_string_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice string by.*float'):

            await self.con.query_json("""
                SELECT '123'[1.0:];
            """)

    async def test_edgeql_expr_string_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice string by.*str'):

            await self.con.execute("""
                SELECT '123'[:'1'];
            """)

    async def test_edgeql_expr_string_08(self):
        await self.assert_query_result(
            r'''SELECT ':\x62:\u2665:\U000025C6:☎️:';''',
            [':b:♥:◆:☎️:'],
        )

        await self.assert_query_result(
            r'''SELECT '\'"\\\'\""\\x\\u';''',
            ['\'"\\\'\""\\x\\u'],
        )

        await self.assert_query_result(
            r'''SELECT "'\"\\\'\"\\x\\u";''',
            ['\'"\\\'"\\x\\u'],
        )

        await self.assert_query_result(
            r'''SELECT r'\n';''',
            ['\\n'],
        )

    async def test_edgeql_expr_string_09(self):
        await self.assert_query_result(
            r'''SELECT 'bb\
            aa \
            bb';
            ''',
            ['bbaa bb'],
        )

        await self.assert_query_result(
            r'''SELECT 'bb\
            aa \


            bb';
            ''',
            ['bbaa bb'],
        )

        await self.assert_query_result(
            r'''SELECT r'aa\
            bb \
            aa';''',
            ['aa\\\n            bb \\\n            aa'],
        )

    async def test_edgeql_expr_string_10(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"invalid string literal: invalid escape sequence '\\ '",
                _hint="consider removing trailing whitespace"):
            await self.con.execute(
                r"SELECT 'bb\   "
                "\naa';"
            )

    async def test_edgeql_expr_string_11(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"invalid string literal: invalid escape sequence '\\ '"):
            await self.con.execute(
                r"SELECT 'bb\   aa';"
            )

    async def test_edgeql_expr_string_12(self):
        # Issue #1269
        await self.assert_query_result(
            r'''SELECT 'bb\
aa \
            bb';
            ''',
            ['bbaa bb'],
        )

    async def test_edgeql_expr_string_13(self):
        # Issue #1269
        #
        # \r or \n should both be allowed after line continuation symbol
        await self.assert_query_result(
            "SELECT 'bb\\\n   aa';",
            ['bbaa'],
        )

        await self.assert_query_result(
            "SELECT 'bb\\\r   aa';",
            ['bbaa'],
        )

        await self.assert_query_result(
            "SELECT 'bb\\\r\n   aa';",
            ['bbaa'],
        )

    async def test_edgeql_expr_tuple_01(self):
        await self.assert_query_result(
            r"""
                SELECT (1, 'foo');
            """,
            [[1, 'foo']],
        )

    async def test_edgeql_expr_tuple_02(self):
        await self.assert_query_result(
            r'''SELECT (1, 'foo') = (1, 'foo');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (1, 'foo') = (2, 'foo');''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT (1, 'foo') != (1, 'foo');''',
            [False],
        )

        await self.assert_query_result(
            r'''SELECT (1, 'foo') != (2, 'foo');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (1, 2) = (1, 2.0);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (1, 2.0) = (1, 2);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (1, 2.1) != (1, 2);''',
            [True],
        )

    async def test_edgeql_expr_tuple_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot"):
            await self.con.query(r"""
                SELECT (1, 'foo') = ('1', 'foo');
            """)

    async def test_edgeql_expr_tuple_04(self):
        await self.assert_query_result(
            r"""
                SELECT array_agg((1, 'foo'));
            """,
            [[[1, 'foo']]],
        )

    async def test_edgeql_expr_tuple_05(self):
        await self.assert_query_result(
            r"""
                SELECT (1, 2) UNION (3, 4);
            """,
            [[1, 2], [3, 4]],
        )

    async def test_edgeql_expr_tuple_06(self):
        await self.assert_query_result(
            r'''SELECT (1, 'foo') = (a := 1, b := 'foo');''',
            [True]
        )

        await self.assert_query_result(
            r'''SELECT (a := 1, b := 'foo') = (a := 1, b := 'foo');''',
            [True]
        )

        await self.assert_query_result(
            r'''SELECT (a := 1, b := 'foo') = (c := 1, d := 'foo');''',
            [True]
        )

        await self.assert_query_result(
            r'''SELECT (a := 1, b := 'foo') = (b := 1, a := 'foo');''',
            [True]
        )

        await self.assert_query_result(
            r'''SELECT (a := 1, b := 9001) != (b := 9001, a := 1);''',
            [True]
        )

        await self.assert_query_result(
            r'''SELECT (a := 1, b := 9001).a = (b := 9001, a := 1).a;''',
            [True]
        )

        await self.assert_query_result(
            r'''SELECT (a := 1, b := 9001).b = (b := 9001, a := 1).b;''',
            [True]
        )

    async def test_edgeql_expr_tuple_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '!=' cannot"):
            await self.con.query_single(r"""
                SELECT (a := 1, b := 'foo') != (b := 'foo', a := 1);
            """)

    async def test_edgeql_expr_tuple_08(self):
        await self.assert_query_result(
            r"""
                SELECT ();
            """,
            [[]],
        )

    async def test_edgeql_expr_tuple_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\+'.*cannot.*tuple<.*>' and 'std::int64'"):

            await self.con.execute(r'''
                SELECT (spam := 1, ham := 2) + 1;
            ''')

    async def test_edgeql_expr_tuple_10(self):
        await self.assert_query_result(
            '''\
                SELECT _ := (spam := {1, 2}, ham := {3, 4})
                ORDER BY _.spam THEN _.ham;
            ''',
            [
                {'ham': 3, 'spam': 1},
                {'ham': 4, 'spam': 1},
                {'ham': 3, 'spam': 2},
                {'ham': 4, 'spam': 2}
            ]
        )

    async def test_edgeql_expr_tuple_11(self):
        await self.assert_query_result(
            r'''SELECT (1, 2) = (1, 2);''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (1, 2) UNION (1, 2);''',
            [[1, 2], [1, 2]],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT ((1, 2) UNION (1, 2));''',
            [[1, 2]],
        )

    async def test_edgeql_expr_tuple_12(self):
        await self.assert_query_result(
            r'''
                WITH A := {1, 2, 3}
                SELECT _ := ({'a', 'b'}, A)
                ORDER BY _;
            ''',
            [['a', 1], ['a', 2], ['a', 3], ['b', 1], ['b', 2], ['b', 3]],
        )

    async def test_edgeql_expr_tuple_13(self):
        await self.assert_query_result(
            r"""
                SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3);
            """,
            [[1, ['a', 'b', [0.1, 0.2]], 2, 3]],
        )

        await self.assert_query_result(
            r"""
                # should be the same as above
                WITH _ := (1, ('a', 'b', (0.1, 0.2)), 2, 3)
                SELECT _;
            """,
            [[1, ['a', 'b', [0.1, 0.2]], 2, 3]],
        )

    async def test_edgeql_expr_tuple_14(self):
        await self.assert_query_result(
            '''
                SELECT (1, <int64>{});
            ''',
            [],
        )

    async def test_edgeql_expr_tuple_15(self):
        await self.assert_query_result(
            '''
                WITH
                    A := {1, 2},
                    B := <int64>{}
                SELECT (A, B);
            ''',
            [],
        )

    async def test_edgeql_expr_tuple_16(self):
        await self.assert_query_result(
            '''
                WITH
                    MODULE schema,
                    A := {'a', 'b'},
                    # B is an empty set
                    B := (SELECT Type FILTER Type.name = 'n/a').name
                SELECT (A, B);
            ''',
            [],
        )

    async def test_edgeql_expr_tuple_17(self):
        # We want to do these tests with the tuple both persistent and not
        # I'm relatively confident that tuple<int64, tuple<int64>>
        # won't make it into the standard library.

        for i in range(2):
            # Make the tuple persistent on the second time around
            if i == 1:
                await self.con.execute(r"""
                    CREATE TYPE Foo {
                        CREATE PROPERTY x -> tuple<int64, tuple<int64>>;
                    }
                """)

            await self.assert_query_result(
                '''SELECT (1, (2,)) ?= (1, (2,))''',
                [True],
            )

            await self.assert_query_result(
                '''SELECT (0, (2,)) ?= enumerate((2,))''',
                [True],
            )

            await self.assert_query_result(
                '''WITH A := enumerate((2,)) SELECT (0, (2,)) ?= A''',
                [True],
            )

            await self.assert_query_result(
                '''SELECT <tuple<int64, tuple<int64>>>{} ?? (1, (2,));''',
                [[1, [2]]],
            )

            await self.assert_query_result(
                '''SELECT (1, (2,)) ?? <tuple<int64, tuple<int64>>>{};''',
                [[1, [2]]],
            )

    async def test_edgeql_expr_tuple_18(self):
        await self.assert_query_result(
            '''
                WITH TUP := (1, (2, 3))
                SELECT TUP.1.1;
            ''',
            [3],
        )

    async def test_edgeql_expr_tuple_indirection_01(self):
        await self.assert_query_result(
            r"""
                SELECT ('foo', 42).0;
            """,
            ['foo'],
        )

        await self.assert_query_result(
            r"""
                SELECT ('foo', 42).1;
            """,
            [42],
        )

    async def test_edgeql_expr_tuple_indirection_02(self):
        await self.assert_query_result(
            r'''SELECT (name := 'foo', val := 42).name;''',
            ['foo'],
        )

        await self.assert_query_result(
            r'''SELECT (name := 'foo', val := 42).val;''',
            [42],
        )

    async def test_edgeql_expr_tuple_indirection_03(self):
        await self.assert_query_result(
            r"""
                WITH _ := (SELECT ('foo', 42)) SELECT _.1;
            """,
            [42],
        )

    async def test_edgeql_expr_tuple_indirection_04(self):
        await self.assert_query_result(
            r"""
                WITH _ := (SELECT (name := 'foo', val := 42)) SELECT _.name;
            """,
            ['foo'],
        )

    async def test_edgeql_expr_tuple_indirection_05(self):
        await self.assert_query_result(
            r"""
                WITH _ := (SELECT (1,2) UNION (3,4)) SELECT _.0;
            """,
            [1, 3],
        )

    async def test_edgeql_expr_tuple_indirection_06(self):
        await self.assert_query_result(
            r'''SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).0;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).1;''',
            [['a', 'b', [0.1, 0.2]]],
        )

        await self.assert_query_result(
            r'''SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).1.2;''',
            [[0.1, 0.2]],
        )

        await self.assert_query_result(
            r'''SELECT (1, ('a', 'b', (0.1, 0.2)), 2, 3).1.2.0;''',
            [0.1],
        )

    async def test_edgeql_expr_tuple_indirection_07(self):
        await self.assert_query_result(
            r'''WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.0;''',
            [1],
        )

        await self.assert_query_result(
            r'''WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.1;''',
            [['a', 'b', [0.1, 0.2]]],
        )

        await self.assert_query_result(
            r'''WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.1.2;''',
            [[0.1, 0.2]],
        )

        await self.assert_query_result(
            r'''WITH A := (1, ('a', 'b', (0.1, 0.2)), 2, 3) SELECT A.1.2.0;''',
            [0.1],
        )

    async def test_edgeql_expr_tuple_indirection_08(self):
        await self.assert_query_result(
            r"""
                SELECT _ := (1, ({55, 66}, {77, 88}), 2)
                ORDER BY _.1 DESC;
            """,
            [
                [1, [66, 88], 2],
                [1, [66, 77], 2],
                [1, [55, 88], 2],
                [1, [55, 77], 2],
            ]
        )

    async def test_edgeql_expr_tuple_indirection_09(self):
        await self.assert_query_result(
            r"""
                SELECT _ := (1, ({55, 66}, {77, 88}), 2)
                ORDER BY _.1.1 THEN _.1.0;
            """,
            [
                [1, [55, 77], 2],
                [1, [66, 77], 2],
                [1, [55, 88], 2],
                [1, [66, 88], 2],
            ]
        )

    async def test_edgeql_expr_tuple_indirection_10(self):
        await self.assert_query_result(
            r"""
                SELECT [(0, 1)][0].1;
            """,
            [1]
        )

    async def test_edgeql_expr_tuple_indirection_11(self):
        await self.assert_query_result(
            r"""
                SELECT [(a := 1, b := 2)][0].b;
            """,
            [2]
        )

    async def test_edgeql_expr_tuple_indirection_12(self):
        await self.assert_query_result(
            r'''SELECT (name := 'foo', val := 42).0;''',
            ['foo'],
        )

        await self.assert_query_result(
            r'''SELECT (name := 'foo', val := 42).1;''',
            [42],
        )

        await self.assert_query_result(
            r'''SELECT [(name := 'foo', val := 42)][0].name;''',
            ['foo'],
        )

        await self.assert_query_result(
            r'''SELECT [(name := 'foo', val := 42)][0].1;''',
            [42],
        )

    async def test_edgeql_expr_tuple_indirection_13(self):
        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1))));''',
            [{"a": {"b": {"c": {"e": 1}}}}],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).a;''',
            [{"b": {"c": {"e": 1}}}],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).0;''',
            [{"b": {"c": {"e": 1}}}],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).a.b;''',
            [{"c": {"e": 1}}],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).0.0;''',
            [{"c": {"e": 1}}],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).a.b.c;''',
            [{"e": 1}],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).0.0.0;''',
            [{"e": 1}],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).a.b.c.e;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT (a:=(b:=(c:=(e:=1)))).0.b.c.0;''',
            [1],
        )

    async def test_edgeql_expr_tuple_indirection_14(self):
        await self.assert_query_result(
            r'''SELECT [(a:=(b:=(c:=(e:=1))))][0].a;''',
            [{"b": {"c": {"e": 1}}}],
        )

        await self.assert_query_result(
            r'''SELECT [(a:=(b:=(c:=(e:=1))))][0].0;''',
            [{"b": {"c": {"e": 1}}}],
        )

        await self.assert_query_result(
            r'''SELECT [(a:=(b:=(c:=(1,))))][0].0;''',
            [{"b": {"c": [1]}}],
        )

    async def test_edgeql_expr_cannot_assign_dunder_type_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot assign to __type__'):
            await self.con.query(r"""
                SELECT Text {
                    __type__ := (SELECT schema::ObjectType
                                 FILTER .name = 'default::Named')
                };
            """)

    async def test_edgeql_expr_cannot_assign_id_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot assign to id'):
            await self.con.execute(r"""
                SELECT Text {
                    id := <uuid>'77841036-8e35-49ce-b509-2cafa0c25c4f'
                };
            """)

    async def test_edgeql_expr_if_else_01(self):
        await self.assert_query_result(
            r'''SELECT 'yes' IF True ELSE 'no';''',
            ['yes'],
        )

        await self.assert_query_result(
            r'''SELECT 'yes' IF 1=1 ELSE 'no';''',
            ['yes'],
        )

        await self.assert_query_result(
            r'''SELECT 'yes' IF 1=0 ELSE 'no';''',
            ['no'],
        )

        await self.assert_query_result(
            r'''SELECT 's1' IF 1=0 ELSE 's2' IF 2=2 ELSE 's3';''',
            ['s2'],
        )

    async def test_edgeql_expr_if_else_02(self):
        await self.assert_query_result(
            r'''SELECT 'yes' IF True ELSE {'no', 'or', 'maybe'};''',
            ['yes'],
        )

        await self.assert_query_result(
            r'''SELECT 'yes' IF False ELSE {'no', 'or', 'maybe'};''',
            ['no', 'or', 'maybe'],
        )

        await self.assert_query_result(
            r'''SELECT {'maybe', 'yes'} IF True ELSE {'no', 'or'};''',
            ['maybe', 'yes'],
        )

        await self.assert_query_result(
            r'''SELECT {'maybe', 'yes'} IF False ELSE {'no', 'or'};''',
            ['no', 'or'],
        )

        await self.assert_query_result(
            r'''SELECT {'maybe', 'yes'} IF True ELSE 'no';''',
            ['maybe', 'yes'],
        )

        await self.assert_query_result(
            r'''SELECT {'maybe', 'yes'} IF False ELSE 'no';''',
            ['no'],
        )

        await self.assert_query_result(
            r'''SELECT 'yes' IF {True, False} ELSE 'no';''',
            ['yes', 'no'],
        )

        await self.assert_query_result(
            r'''SELECT 'yes' IF {True, False} ELSE {'no', 'or', 'maybe'};''',
            ['yes', 'no', 'or', 'maybe'],
        )

        await self.assert_query_result(
            r'''SELECT {'maybe', 'yes'} IF {True, False} ELSE {'no', 'or'};''',
            ['maybe', 'yes', 'no', 'or'],
        )

        await self.assert_query_result(
            r'''SELECT {'maybe', 'yes'} IF {True, False} ELSE 'no';''',
            ['maybe', 'yes', 'no'],
        )

    async def test_edgeql_expr_if_else_03(self):
        await self.assert_query_result(
            r'''SELECT 1 IF {1, 2, 3} < {2, 3, 4} ELSE 100;''',
            sorted([1, 1, 1, 100, 1, 1, 100, 100, 1]),
            sort=True
        )

        await self.assert_query_result(
            r'''SELECT {1, 10} IF {1, 2, 3} < {2, 3, 4} ELSE 100;''',
            sorted([1, 10, 1, 10, 1, 10, 100, 1, 10, 1, 10, 100, 100, 1, 10]),
            sort=True
        )

        await self.assert_query_result(
            r'''SELECT sum(1 IF {1, 2, 3} < {2, 3, 4} ELSE 100);''',
            [306],
            sort=True
        )

        await self.assert_query_result(
            r'''SELECT sum({1, 10} IF {1, 2, 3} < {2, 3, 4} ELSE 100);''',
            [366],
            sort=True
        )

    async def test_edgeql_expr_if_else_04(self):
        await self.assert_query_result(
            r'''
                WITH x := <str>{}
                SELECT
                    1   IF x = 'a' ELSE
                    10  IF x = 'b' ELSE
                    100 IF x = 'c' ELSE
                    0;
            ''',
            [],
            sort=True
        )

        await self.assert_query_result(
            r'''
                WITH x := {'c', 'a', 't'}
                SELECT
                    1   IF x = 'a' ELSE
                    10  IF x = 'b' ELSE
                    100 IF x = 'c' ELSE
                    0;
            ''',
            sorted([100, 1, 0]),
            sort=True
        )

        await self.assert_query_result(
            r'''
                WITH x := {'b', 'a', 't'}
                SELECT
                    1   IF x = 'a' ELSE
                    10  IF x = 'b' ELSE
                    100 IF x = 'c' ELSE
                    0;
            ''',
            sorted([10, 1, 0]),
            sort=True
        )

        await self.assert_query_result(
            r'''
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
            ''',
            sorted([0, 101, 11]),
            sort=True
        )

    async def test_edgeql_expr_if_else_05(self):
        await self.assert_query_result(
            r"""
                # this creates a 3 x 3 x 3 cross product
                SELECT
                    1   IF {'c', 'a', 't'} = 'a' ELSE
                    10  IF {'c', 'a', 't'} = 'b' ELSE
                    100 IF {'c', 'a', 't'} = 'c' ELSE
                    0;
            """,
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
            sort=True
        )

    async def test_edgeql_expr_if_else_06(self):
        await self.assert_query_result(
            r"""
                WITH a := {'c', 'a', 't'}
                SELECT
                    (a, 'hit' IF a = 'c' ELSE 'miss')
                ORDER BY .0;
            """,
            [['a', 'miss'], ['c', 'hit'], ['t', 'miss']],
        )

        await self.assert_query_result(
            r"""
                WITH a := {'c', 'a', 't'}
                SELECT
                    (a, 'hit') IF a = 'c' ELSE (a, 'miss')
                ORDER BY .0;
            """,
            [['a', 'miss'], ['c', 'hit'], ['t', 'miss']],
        )

    async def test_edgeql_expr_if_else_07(self):
        await self.assert_query_result(
            r"""
                FOR x IN {<str>{} IF false ELSE <str>{'1'}}
                UNION (SELECT x);
            """,
            ['1'],
        )

    async def test_edgeql_expr_if_else_08(self):
        await self.assert_query_result(
            r"""
                SELECT <str>{} IF true ELSE '';
            """,
            [],
        )

    async def test_edgeql_expr_if_else_09(self):
        await self.assert_query_result(
            r"""
                FOR _ IN {<str>{'1'} IF false ELSE <str>{}}
                UNION ();
            """,
            [],
        )

    async def test_edgeql_expr_setop_01(self):
        await self.assert_query_result(
            r"""SELECT EXISTS <str>{};""",
            [False],
        )

        await self.assert_query_result(
            r"""SELECT NOT EXISTS <str>{};""",
            [True],
        )

    async def test_edgeql_expr_setop_02(self):
        await self.assert_query_result(
            r'''SELECT 2 * ((SELECT 1) UNION (SELECT 2));''',
            [2, 4],
        )

        await self.assert_query_result(
            r'''SELECT (SELECT 2) * (1 UNION 2);''',
            [2, 4],
        )

        await self.assert_query_result(
            r'''SELECT 2 * DISTINCT (1 UNION 2 UNION 1);''',
            [2, 4],
        )

        await self.assert_query_result(
            r'''SELECT 2 * (1 UNION 2 UNION 1);''',
            [2, 4, 2],
        )

        await self.assert_query_result(
            r'''
                WITH
                    a := (SELECT 1 UNION 2)
                SELECT (SELECT 2) * a;
            ''',
            [2, 4],
        )

    async def test_edgeql_expr_setop_03(self):
        await self.assert_query_result(
            r'''SELECT array_agg(1 UNION 2 UNION 3);''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            r'''SELECT array_agg(3 UNION 2 UNION 3);''',
            [[3, 2, 3]],
        )

        await self.assert_query_result(
            r'''SELECT array_agg(3 UNION 3 UNION 2);''',
            [[3, 3, 2]],
        )

    async def test_edgeql_expr_setop_04(self):
        await self.assert_query_result(
            '''
                SELECT DISTINCT {1, 2, 2, 3};
            ''',
            {1, 2, 3},
        )

    async def test_edgeql_expr_setop_05(self):
        await self.assert_query_result(
            '''
                SELECT (2 UNION 2 UNION 2);
            ''',
            [2, 2, 2],
        )

    async def test_edgeql_expr_setop_06(self):
        await self.assert_query_result(
            '''
                SELECT DISTINCT (2 UNION 2 UNION 2);
            ''',
            [2],
        )

    async def test_edgeql_expr_setop_07(self):
        await self.assert_query_result(
            '''
                SELECT DISTINCT (2 UNION 2) UNION 2;
            ''',
            [2, 2],
        )

    async def test_edgeql_expr_setop_08(self):
        obj = await self.con.query(r"""
            SELECT schema::ObjectType;
        """)
        attr = await self.con.query(r"""
            SELECT schema::Annotation;
        """)

        union = [{'id': str(o.id)} for o in [*obj, *attr]]
        union.sort(key=lambda x: x['id'])

        await self.assert_query_result(
            '''
                WITH MODULE schema
                SELECT ObjectType UNION Annotation;
            ''',
            union,
            sort=lambda x: x['id']
        )

    async def test_edgeql_expr_setop_09(self):
        await self.assert_query_result(
            '''
                SELECT _ := DISTINCT {[1, 2], [1, 2], [2, 3]} ORDER BY _;
            ''',
            [[1, 2], [2, 3]],
        )

    async def test_edgeql_expr_setop_10(self):
        await self.assert_query_result(
            r'''SELECT _ := DISTINCT {(1, 2), (2, 3), (1, 2)} ORDER BY _;''',
            [[1, 2], [2, 3]],
        )

        await self.assert_query_result(
            r'''
                SELECT _ := DISTINCT {(a := 1, b := 2),
                                      (a := 2, b := 3),
                                      (a := 1, b := 2)}
                ORDER BY _;
            ''',
            [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}],
        )

    async def test_edgeql_expr_setop_11(self):
        everything = await self.con.query('''
            WITH
                MODULE schema,
                C := (SELECT ObjectType
                      FILTER ObjectType.name LIKE 'schema::%')
            SELECT _ := len(C.name)
            ORDER BY _;
        ''')

        distinct = await self.con.query('''
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
            len(everything), len(distinct),
            'DISTINCT len(ObjectType.name) failed to filter out dupplicates')

    async def test_edgeql_expr_setop_12(self):
        await self.assert_query_result(
            r'''SELECT DISTINCT {(), ()};''',
            [[]],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (SELECT ({1,2,3}, ()) FILTER .0 > 1).1;''',
            [[]],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT (SELECT ({1,2,3}, ()) FILTER .0 > 3).1;''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT DISTINCT {(1,(2,3)), (1,(2,3))};''',
            [[1, [2, 3]]],
        )

    async def test_edgeql_expr_setop_13(self):
        await self.assert_query_result(
            r'''SELECT <tuple<int64, int64>>{} UNION (1, 2);''',
            [[1, 2]],
        )

    async def test_edgeql_expr_setop_14(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"^set constructor has arguments of incompatible "
                r"types 'std::float64' and 'std::decimal'$"):
            await self.con.execute(r'''
                SELECT {1.0, <decimal>2.0};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"^set constructor has arguments of incompatible "
                r"types 'std::float64' and 'std::decimal'$"):
            await self.con.execute(r'''
                SELECT {{1.0, 2.0}, {1.0, <decimal>2.0}};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"^set constructor has arguments of incompatible "
                r"types 'std::float64' and 'std::decimal'$"):
            await self.con.execute(r'''
                SELECT {{1.0, <decimal>2.0}, {1.0, 2.0}};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"^set constructor has arguments of incompatible "
                r"types 'std::decimal' and 'std::float64'$"):
            await self.con.execute(r'''
                SELECT {1.0, 2.0, 5.0, <decimal>2.0, 3.0, 4.0};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"operator 'UNION' cannot be applied to operands of type "
                r"'std::int64' and 'std::str'$"):
            await self.con.execute(r'''
                SELECT {1, 2, 3, 4 UNION 'a', 5, 6, 7};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"operator 'UNION' cannot be applied to operands of type "
                r"'std::int64' and 'std::str'$"):
            await self.con.execute(r'''
                SELECT {1, 2, 3, {{1, 4} UNION 'a'}, 5, 6, 7};
            ''')

    async def test_edgeql_expr_cardinality_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=38):

            await self.con.execute('''\
                SELECT Issue ORDER BY Issue.watchers.name;
            ''')

    async def test_edgeql_expr_cardinality_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=29):

            await self.con.execute('''\
                SELECT Issue LIMIT LogEntry.spent_time;
            ''')

    async def test_edgeql_expr_cardinality_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=29):

            await self.con.execute('''\
                SELECT Issue OFFSET LogEntry.spent_time;
            ''')

    async def test_edgeql_expr_cardinality_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=45):

            await self.con.execute('''\
                SELECT EXISTS Issue ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=52):

            await self.con.execute('''\
                SELECT 'foo' IN Issue.name ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=49):

            await self.con.execute('''\
                SELECT Issue UNION Text ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_cardinality_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=47):

            await self.con.execute('''\
                SELECT DISTINCT Issue ORDER BY Issue.name;
            ''')

    async def test_edgeql_expr_type_intersection_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                f"cannot apply type intersection operator to scalar type "
                f"'std::int64': it is not an object type",
                _position=25):

            await self.con.execute('''\
                SELECT 10[IS std::Object];
            ''')

    async def test_edgeql_expr_type_intersection_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot create an intersection of std::Object, std::str',
                _position=33):

            await self.con.execute('''\
                SELECT Object[IS str];
            ''')

    async def test_edgeql_expr_type_intersection_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                f"cannot apply type intersection operator to scalar type "
                f"'std::uuid': it is not an object type",
                _position=32):

            await self.con.execute('''\
                SELECT Object.id[IS uuid];
            ''')

    async def test_edgeql_expr_comparison_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot.*tuple.*and.*array<std::int64>"):
            await self.con.execute(r'''
                SELECT (1, 2) = [1, 2];
            ''')

    async def test_edgeql_expr_comparison_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot.* 'std::int64' and.*array<std::int64>"):
            await self.con.execute(r'''
                SELECT {1, 2} = [1, 2];
            ''')

    async def test_edgeql_expr_comparison_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot.*'std::int64' and.*tuple.*"):
            await self.con.execute(r'''
                SELECT {1, 2} = (1, 2);
            ''')

    async def test_edgeql_expr_aggregate_01(self):
        await self.assert_query_result(
            r'''SELECT count(DISTINCT {1, 1, 1});''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT count(DISTINCT {1, 2, 3});''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT count(DISTINCT {1, 2, 3, 2, 3});''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT count({1, 1, 1});''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT count({1, 2, 3});''',
            [3],
        )

        await self.assert_query_result(
            r'''SELECT count({1, 2, 3, 2, 3});''',
            [5],
        )

    async def test_edgeql_expr_alias_01(self):
        await self.assert_query_result(
            r"""
                WITH
                    a := {1, 2},
                    b := {2, 3}
                SELECT a
                FILTER a = b;
            """,
            [2],
        )

    async def test_edgeql_expr_alias_02(self):
        await self.assert_query_result(
            r"""
                WITH
                    b := {2, 3}
                SELECT a := {1, 2}
                FILTER a = b;
            """,
            [2],
        )

    async def test_edgeql_expr_alias_03(self):
        await self.assert_query_result(
            r"""
                SELECT (
                    name := 'a',
                    foo := (
                        WITH a := {1, 2}
                        SELECT a
                    )
                );
            """,
            [{'name': 'a', 'foo': 1}, {'name': 'a', 'foo': 2}],
        )

    async def test_edgeql_expr_alias_04(self):
        await self.assert_query_result(
            r"""
                SELECT (
                    name := 'a',
                    foo := (
                        WITH a := {1, 2}
                        SELECT a
                        FILTER a < 2
                    )
                );
            """,
            [{'name': 'a', 'foo': 1}],
        )

    async def test_edgeql_expr_alias_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    foo := (
                        WITH a := {1, 2}
                        SELECT a
                    )
                }
                FILTER .name LIKE 'schema::Arr%'
                ORDER BY .name LIMIT 1;
            """,
            [{'name': 'schema::Array', 'foo': {1, 2}}],
        )

    async def test_edgeql_expr_alias_06(self):
        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    foo := (
                        WITH a := {1, 2}
                        SELECT a
                        FILTER a < 2
                    )
                }
                FILTER .name LIKE  'schema::Arr%'
                ORDER BY .name LIMIT 1;
            """,
            [{'name': 'schema::Array', 'foo': {1}}],
        )

    async def test_edgeql_expr_alias_07(self):
        await self.assert_query_result(
            r"""
                # test variable masking
                WITH x := (
                    WITH x := {2, 3, 4} SELECT {4, 5, x}
                )
                SELECT x ORDER BY x;
            """,
            [2, 3, 4, 4, 5],
        )

    async def test_edgeql_expr_alias_08(self):
        await self.assert_query_result(
            r"""
                # test variable masking
                WITH x := (
                    FOR x IN {2, 3}
                    UNION x + 2
                )
                SELECT x ORDER BY x;
            """,
            [4, 5],
        )

    async def test_edgeql_expr_alias_09(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := [1],
                    y := [2]
                SELECT
                    'OK'
                FILTER
                    x[0] = 1
                    AND y[0] = 2
            """,
            ['OK'],
        )

    async def test_edgeql_expr_for_01(self):
        await self.assert_query_result(
            r"""
                SELECT x := (
                    FOR x IN {1, 3, 5, 7}
                    UNION x
                )
                ORDER BY x;
            """,
            [1, 3, 5, 7],
        )

        await self.assert_query_result(
            r"""
                SELECT x := (
                    FOR x IN {1, 3, 5, 7}
                    UNION x + 1
                )
                ORDER BY x;
            """,
            [2, 4, 6, 8],
        )

    async def test_edgeql_expr_for_02(self):
        await self.assert_query_result(
            r"""
                FOR x IN {2, 3}
                UNION {x, x + 2};
            """,
            {2, 3, 4, 5},
        )

    @test.not_implemented('GROUP statement is not yet implemented')
    async def test_edgeql_expr_group_01(self):
        await self.assert_query_result(
            r"""
                WITH I := {1, 2, 3, 4}
                GROUP I
                USING _ := I % 2 = 0
                BY _
                INTO I
                UNION _r := (
                    values := array_agg(I ORDER BY I)
                ) ORDER BY _r.values;
            """,
            [
                {'values': [1, 3]},
                {'values': [2, 4]}
            ]
        )

    @test.not_implemented('GROUP statement is not yet implemented')
    async def test_edgeql_expr_group_02(self):
        await self.assert_query_result(
            r'''
                # handle a number of different aliases
                WITH x := {(1, 2), (3, 4), (4, 2)}
                GROUP y := x
                USING _ := y.1
                BY _
                INTO y
                UNION array_agg(y.0 ORDER BY y.0);
            ''',
            [[1, 4], [3]],
            sort=True
        )

    @test.not_implemented('GROUP statement is not yet implemented')
    async def test_edgeql_expr_group_03(self):
        await self.assert_query_result(
            r'''
                WITH x := {(1, 2), (3, 4), (4, 2)}
                GROUP x
                USING _ := x.1
                BY _
                INTO x
                UNION array_agg(x.0 ORDER BY x.0);
            ''',
            [[1, 4], [3]],
            sort=True
        )

    @test.not_implemented('GROUP statement is not yet implemented')
    async def test_edgeql_expr_group_04(self):
        await self.assert_query_result(
            r'''
                WITH x := {(1, 2), (3, 4), (4, 2)}
                GROUP x
                USING B := x.1
                BY B
                INTO x
                UNION (B, array_agg(x.0 ORDER BY x.0))
                ORDER BY
                    B;
            ''',
            [[2, [1, 4]], [4, [3]]],
        )

    @test.not_implemented('GROUP statement is not yet implemented')
    async def test_edgeql_expr_group_05(self):
        await self.assert_query_result(
            r'''
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
            ''',
            [[1, 3, 6, 6], [2, 1, 6, 6], [3, 2, 6, 6]]
        )

    @test.not_implemented('GROUP statement is not yet implemented')
    async def test_edgeql_expr_group_06(self):
        await self.assert_query_result(
            r'''
                GROUP X := {1, 1, 1, 2, 3, 3}
                USING y := X
                BY y
                INTO y
                UNION (y, count(X))
                ORDER BY y;
            ''',
            [[1, 3], [2, 1], [3, 2]]
        )

    async def test_edgeql_expr_slice_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"scalar type 'std::int64' cannot be sliced"):

            await self.con.execute("""
                SELECT 1[1:3];
            """)

    async def test_edgeql_expr_slice_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"scalar type 'std::int64' cannot be sliced"):

            await self.con.execute("""
                SELECT 1[:3];
            """)

    async def test_edgeql_expr_slice_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"scalar type 'std::int64' cannot be sliced"):

            await self.con.execute("""
                SELECT 1[1:];
            """)

    async def test_edgeql_expr_index_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"index indirection cannot be applied to scalar type "
                r"'std::int64'"):

            await self.con.execute("""
                SELECT 1[1];
            """)

    async def test_edgeql_expr_error_after_extraction_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "Unexpected \"'1'\""):

            await self.con.query("""
                SELECT '''1''';
            """)

    async def test_edgeql_expr_invalid_object_scalar_op_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\?\?' cannot be applied to operands of type"
                r" 'std::Object' and 'std::str'"):

            await self.con.query("""
                SELECT Object ?? '';
            """)

    async def test_edgeql_expr_static_eval_casts_01(self):
        # The static evaluator produced some really silly results for
        # these at one point.

        await self.assert_query_result(
            r'''
                WITH x := {1, 2}, SELECT ("wtf" ++ <str>x);
            ''',
            ["wtf1", "wtf2"],
            sort=True,
        )

        await self.assert_query_result(
            r'''
                FOR x in {1, 2} UNION (SELECT ("wtf" ++ <str>x));
            ''',
            ["wtf1", "wtf2"],
            sort=True,
        )

        await self.assert_query_result(
            r'''
                WITH x := <int64>{}, SELECT ("wtf" ++ <str>x);
            ''',
            [],
        )

    async def test_edgeql_normalization_mismatch_01(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError, "Unexpected type expression"):

            await self.con.query('SELECT <tuple<"">>1;')

    async def test_edgeql_typeop_01(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "type operator '&' is not implemented",
        ):
            await self.con.query('select <Named & Owned>{};')

    async def test_edgeql_typeop_02(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "cannot use type operator '|' with non-object type",
        ):
            await self.con.query('select 1 is (int64 | float64);')

    async def test_edgeql_typeop_03(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "cannot use type operator '|' with non-object type",
        ):
            await self.con.query('select 1 is (Object | float64);')

    async def test_edgeql_typeop_04(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "cannot use type operator '|' with non-object type",
        ):
            await self.con.query(
                'select [1] is (array<int64> | array<float64>);')

    async def test_edgeql_typeop_05(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "cannot use type operator '|' with non-object type",
        ):
            await self.con.query(
                'select (1,) is (tuple<int64> | tuple<float64>);')

    async def test_edgeql_typeop_06(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "cannot use type operator '|' with non-object type",
        ):
            await self.con.query(
                'select [1] is (typeof [2] | typeof [2.2]);')

    async def test_edgeql_typeop_07(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "cannot use type operator '|' with non-object type",
        ):
            await self.con.query(
                'select (1,) is (typeof (2,) | typeof (2.2,));')

    async def test_edgeql_typeop_08(self):
        await self.assert_query_result(
            'select {x := 1} is (typeof Issue.references | Object);',
            {False}
        )
        await self.assert_query_result(
            'select {x := 1} is (typeof Issue.references | BaseObject);',
            {True}
        )

    async def test_edgeql_assert_single_01(self):
        await self.con.execute("""
            INSERT User {
                name := "He Who Remains"
            }
        """)

        await self.assert_query_result("""
            SELECT assert_single((
                SELECT User { name } FILTER .name ILIKE "He Who%"
            ))
        """, [{
            "name": "He Who Remains",
        }])

        await self.con.query_single("""
            SELECT assert_single((
                SELECT User { name } FILTER .name ILIKE "He Who%"
            ))
        """)

        await self.con.query("""
            FOR x IN {1, 2, 3}
            UNION (
                SELECT assert_single(x)
            );
        """)

    async def test_edgeql_assert_single_02(self):
        await self.con.execute("""
            FOR name IN {"Hunter B-15", "Hunter B-22"}
            UNION (
                INSERT User {
                    name := name
                }
            );
        """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_single violation",
        ):
            await self.con.query("""
                SELECT assert_single({1, 2});
            """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_single violation",
        ):
            await self.con.query("""
                SELECT assert_single(
                    (SELECT User FILTER .name ILIKE "Hunter B%")
                );
            """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_single violation",
        ):
            await self.con.query("""
                SELECT User {
                    single name := assert_single(.name ++ {"!", "?"})
                };
            """)

    async def test_edgeql_assert_single_no_op(self):
        await self.con.query("""
            SELECT assert_single(1)
        """)

        await self.con.query("""
            FOR x IN {User}
            UNION assert_single(x.name)
        """)

        await self.con.query("""
            SELECT User {
                single foo := assert_single(.name) ++ "!"
            }
        """)

    async def test_edgeql_assert_exists_01(self):
        await self.con.execute("""
            INSERT User {
                name := "User 1",
            };
            INSERT User {
                name := "User 2",
            }
        """)

        await self.assert_query_result(
            """
                SELECT assert_exists((
                    SELECT User { name } FILTER .name IN {"User 1", "User 2"}
                )) ORDER BY .name
            """,
            [{
                "name": "User 1",
            }, {
                "name": "User 2",
            }],
        )

        await self.assert_query_result(
            """
                SELECT {
                    user := assert_exists(
                        (SELECT User FILTER .name = "User 1").name
                    )
                }
            """,
            [{
                "user": "User 1",
            }],
        )

        # Same but with explicit lower cardinality
        await self.assert_query_result(
            """
                SELECT {
                    required user := assert_exists(
                        (SELECT User FILTER .name = "User 1").name
                    )
                }
            """,
            [{
                "user": "User 1",
            }],
        )

        await self.con.query_single(
            """
                SELECT {
                    required all_users := assert_exists(User)
                }
            """,
        )

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            await self.con.query("""
                SELECT assert_exists(<str>{});
            """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            await self.con.query("""
                SELECT assert_exists(
                    (SELECT User FILTER .name = "nonexistent")
                );
            """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            await self.con.query("""
                SELECT User {
                    bff := assert_exists((
                        SELECT User FILTER .name = "nonexistent"))
                }
                FILTER .name = "User 2";
            """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            await self.con.query_json("""
                SELECT assert_exists(
                    (SELECT User { name } FILTER .name = "nonexistent")
                );
            """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            await self.con.query("""
                SELECT assert_exists(
                    (SELECT User FILTER .name = "nonexistent")
                ).name;
            """)

    async def test_edgeql_assert_exists_no_op(self):
        await self.con.query("""
            SELECT assert_exists(1)
        """)

        await self.con.query("""
            FOR x IN {User}
            UNION assert_exists(x.name)
        """)

        await self.con.query("""
            SELECT User {
                single foo := assert_exists(.name) ++ "!"
            }
        """)

    async def test_edgeql_assert_distinct_01(self):
        await self.con.execute("""
            INSERT File {
                name := "File 1",
            };
            INSERT File {
                name := "File 2",
            };
            INSERT User {
                name := "User 1",
            };
            INSERT Status {
                name := "Open",
            };
            INSERT Issue {
                name := "Issue 1",
                body := "Issue 1",
                owner := (SELECT User FILTER .name = "User 1"),
                number := "1",
                status := (SELECT Status FILTER .name = "Open"),
                references := (SELECT File FILTER .name = "File 1"),
            };
            INSERT Issue {
                name := "Issue 2",
                body := "Issue 2",
                owner := (SELECT User FILTER .name = "User 1"),
                number := "2",
                status := (SELECT Status FILTER .name = "Open"),
                references := (SELECT File FILTER .name = "File 2"),
            }
        """)

        await self.assert_query_result(
            """
                SELECT assert_distinct((
                    (SELECT Issue FILTER .references[IS File].name = "File 1")
                    UNION
                    (SELECT Issue FILTER .references[IS File].name = "File 2")
                )) {
                    number
                }
                ORDER BY .number
            """,
            [{
                "number": "1",
            }, {
                "number": "2",
            }],
        )

        await self.assert_query_result(
            "SELECT assert_distinct({2, 1, 3, 4})",
            [2, 1, 3, 4],
        )

        await self.assert_query_result(
            "SELECT assert_distinct(array_unpack([2, 1, 3, 4]))",
            [2, 1, 3, 4],
        )

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "assert_distinct violation",
        ):
            await self.con.query("""
                SELECT assert_distinct({1, 2, 1});
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "assert_distinct violation",
        ):
            await self.con.query("""
                SELECT assert_distinct(
                    (SELECT User FILTER .name = "User 1")
                    UNION
                    (SELECT User FILTER .name = "User 1")
                );
            """)

        await self.assert_query_result(
            r"""
                SELECT assert_distinct(
                    {(0,), (1,)}
                );
            """,
            {(0,), (1,)},
        )

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "assert_distinct violation",
        ):
            await self.con.query("""
                SELECT assert_distinct(
                    {(0, 1, (0,)), (0, 1, (0,))}
                );
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "assert_distinct violation",
        ):
            await self.con.query("""
                SELECT assert_distinct({(), ()});
            """)

    async def test_edgeql_assert_distinct_no_op(self):
        await self.con.query("""
            SELECT assert_distinct(<int64>{})
        """)

        await self.con.query("""
            FOR x IN {User}
            UNION assert_distinct(x.name)
        """)

        await self.con.query("""
            SELECT User {
                single foo := assert_distinct(.name)
            }
        """)

    async def test_edgeql_introspect_without_shape(self):
        await self.assert_query_result(
            """
                SELECT (INTROSPECT TYPEOF BaseObject)
            """,
            [
                {"id": str}
            ]
        )
        res = await self.con._fetchall("""
            SELECT (INTROSPECT TYPEOF BaseObject)
        """, __typenames__=True)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].__tname__, "schema::ObjectType")

    async def test_edgeql_object_injections(self):
        await self.con._fetchall("""
            SELECT <Object>{}
        """, __typenames__=True)

        await self.con._fetchall("""
            WITH Z := (Object,), SELECT Z;
        """, __typenames__=True)

        await self.con._fetchall("""
            FOR Z IN {(Object,)} UNION Z;
        """, __typenames__=True)
