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


import datetime
import functools
import json
import os.path
import random
import typing

import edgedb

from edb import errors
from edb.common import assert_data_shape
from edb.testbase import server as tb


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

    '<cal::local_datetime>"2018-05-07T00:00:00"':
        value(typename='std::cal::local_datetime',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=False, anynumeric=False),

    '<cal::local_date>"2018-05-07"':
        value(typename='std::cal::local_date',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=False, anynumeric=False),

    '<cal::local_time>"20:01:22.306916"':
        value(typename='std::cal::local_time',
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

    '<cal::relative_duration>"P1Y2M3D"':
        value(typename='std::cal::relative_duration',
              anyreal=False, anyint=False, anyfloat=False,
              datetime=True, signed=True, anynumeric=False),

    # Much like integer and float values are all setup to be 1 and equal to
    # each other, so are relative_duration and date_duration equal.
    '<cal::date_duration>"P1Y2M3D"':
        value(typename='std::cal::date_duration',
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


class TestExpressions(tb.QueryTestCase):
    NO_FACTOR = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

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
                    r'''SELECT <duration>'13074457345618258602us' ''',
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
                    len((
                      SELECT ObjectType
                      FILTER ObjectType.name LIKE 'schema::%'
                    ).name)
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
                    len((
                      SELECT ObjectType
                      FILTER ObjectType.name LIKE 'schema::%'
                    ).name)
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

    async def test_edgeql_expr_mod_02(self):
        # Test integer mod.
        #
        # We've had a bug where 2000000 %  9223372036854770000 produced
        # overflow. This is die to the fact that internally we were adding
        # the divisor and the remainder together.
        await self.assert_query_result(
            f'''select 2000000 % 9223372036854770000;''',
            [2000000],
        )
        await self.assert_query_result(
            f'''select -2000000 % -9223372036854770000;''',
            [-2000000],
        )
        await self.assert_query_result(
            f'''select (<int32>2000000) % <int32>2147483000;''',
            [2000000],
        )
        await self.assert_query_result(
            f'''select (-<int32>2000000) % -<int32>2147483000;''',
            [-2000000],
        )
        await self.assert_query_result(
            f'''select (<int16>20000) % <int16>32000;''',
            [20000],
        )
        await self.assert_query_result(
            f'''select (-<int16>20000) % -<int16>32000;''',
            [-20000],
        )

    async def test_edgeql_expr_mod_03(self):
        # Test integer division.
        #
        # We've had a bug where 100000000000000001 // 2 produced
        # 50000000000000001. Basically the floor division sometimes
        # rounded up.
        await self.assert_query_result(
            f'''select 100000000000000001 // 2;''',
            [50000000000000000],
        )
        await self.assert_query_result(
            f'''select -100000000000000001 // 2;''',
            [-50000000000000001],
        )
        await self.assert_query_result(
            f'''select (<int32>1000000001) // <int32>2;''',
            [500000000],
        )
        await self.assert_query_result(
            f'''select (-<int32>1000000001) // <int32>2;''',
            [-500000001],
        )
        await self.assert_query_result(
            f'''select (<int16>10001) // <int16>2;''',
            [5000],
        )
        await self.assert_query_result(
            f'''select (-<int16>10001) // <int16>2;''',
            [-5001],
        )
        await self.assert_query_result(
            f'''select 10000000000000000000000000000000001n // 2n;''',
            [5000000000000000000000000000000000],
        )
        await self.assert_query_result(
            f'''select -10000000000000000000000000000000001n // 2n;''',
            [-5000000000000000000000000000000001],
        )
        await self.assert_query_result(
            f'''select 10000000000000000000000000000000001.0n // 2.0n;''',
            [5000000000000000000000000000000000],
        )
        await self.assert_query_result(
            f'''select -10000000000000000000000000000000001.0n // 2.0n;''',
            [-5000000000000000000000000000000001],
        )

    async def test_edgeql_expr_mod_04(self):
        # Fuzztest of integer %
        for maxval, tname in [(2 ** 15 - 1, 'int16'),
                              (2 ** 31 - 1, 'int32'),
                              (2 ** 63 - 1, 'int64'),
                              (10 ** 25, 'bigint')]:

            vals = []
            for i in range(1000):
                a = random.randrange(-maxval, maxval)
                # Can't divide by 0
                b = random.choice([-1, 1]) * random.randrange(1, maxval)

                vals.append([i, a, b, a // b])

            nums, arr1, arr2, _ = zip(*vals)
            results = await self.con.query_json(
                f'''
                    with
                        N := <array<int64>>$nums,
                        A1 := <array<{tname}>>$arr1,
                        A2 := <array<{tname}>>$arr2,
                    for X in array_unpack(N)
                    select _ := [X, A1[X], A2[X], A1[X] // A2[X]]
                    order by _[0];
                ''',
                nums=list(nums),
                arr1=list(arr1),
                arr2=list(arr2),
            )

            for res in json.loads(results):
                i, a, b, c = res
                _, va, vb, vc = vals[i]
                msg = (
                    f'original: {va} // {vb} = {vc}, '
                    f'edgeql: {a} // {b} = {c}'
                )
                assert_data_shape.assert_data_shape(
                    res, vals[i], self.fail, message=msg)

    async def test_edgeql_expr_mod_05(self):
        # Fuzztest of integer %
        for maxval, tname in [(2 ** 15 - 1, 'int16'),
                              (2 ** 31 - 1, 'int32'),
                              (2 ** 63 - 1, 'int64'),
                              (10 ** 25, 'bigint')]:

            vals = []
            for i in range(1000):
                a = random.randrange(-maxval, maxval)
                # Can't divide by 0
                b = random.choice([-1, 1]) * random.randrange(1, maxval)

                vals.append([i, a, b, a % b])

            nums, arr1, arr2, _ = zip(*vals)
            results = await self.con.query_json(
                f'''
                    with
                        N := <array<int64>>$nums,
                        A1 := <array<{tname}>>$arr1,
                        A2 := <array<{tname}>>$arr2,
                    for X in array_unpack(N)
                    select _ := [X, A1[X], A2[X], A1[X] % A2[X]]
                    order by _[0];
                ''',
                nums=list(nums),
                arr1=list(arr1),
                arr2=list(arr2),
            )

            for res in json.loads(results):
                i, a, b, c = res
                _, va, vb, vc = vals[i]
                msg = (
                    f'original: {va} % {vb} = {vc}, '
                    f'edgeql: {a} % {b} = {c}'
                )
                assert_data_shape.assert_data_shape(
                    res, vals[i], self.fail, message=msg)

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
        for left, ldesc in get_test_items(anyreal=False):
            for right, rdesc in get_test_items():
                for op, not_op in ops:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if (
                            (left == right)
                            or
                            # relative_duration and date_duration are
                            # compatible for comparison due to implicit
                            # casting
                            (
                                {
                                    ldesc.typename,
                                    rdesc.typename
                                } == {
                                    'std::cal::relative_duration',
                                    'std::cal::date_duration'
                                }
                            )
                            or
                            # local_date and local_datetime are
                            # compatible for comparison due to implicit
                            # casting
                            (
                                {
                                    ldesc.typename,
                                    rdesc.typename
                                } == {
                                    'std::cal::local_date',
                                    'std::cal::local_datetime'
                                }
                            )
                        ) else expected_error_msg
                    )

    async def test_edgeql_expr_valid_comp_02(self):
        expected_error_msg = 'cannot be applied to operands'
        # compare all orderable non-numerics to all scalars via
        # ordering operator
        for left, ldesc in get_test_items(anyreal=False):
            for right, rdesc in get_test_items():
                for op, not_op in [('>=', '<'), ('<=', '>')]:
                    await self._test_boolop(
                        left, right, op, not_op,
                        True if (
                            (left == right)
                            or
                            # relative_duration and date_duration are
                            # compatible for comparison due to implicit
                            # casting
                            (
                                {
                                    ldesc.typename,
                                    rdesc.typename
                                } == {
                                    'std::cal::relative_duration',
                                    'std::cal::date_duration'
                                }
                            )
                            or
                            # local_date and local_datetime are
                            # compatible for comparison due to implicit
                            # casting
                            (
                                {
                                    ldesc.typename,
                                    rdesc.typename
                                } == {
                                    'std::cal::local_date',
                                    'std::cal::local_datetime'
                                }
                            )
                        ) else expected_error_msg
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
                argtypes = {ldesc.typename, rdesc.typename}

                if argtypes == {
                    'std::cal::local_date',
                    'std::cal::date_duration'
                }:
                    # whole day arithmetic
                    restype = 'std::cal::local_date'
                elif {ldesc.typename, rdesc.typename} == {
                    'std::cal::date_duration',
                }:
                    # whole day arithmetic
                    restype = 'std::cal::date_duration'
                elif argtypes.intersection({
                    'duration',
                    'std::cal::relative_duration',
                    'std::cal::date_duration'
                }):
                    # Whole day arithemtic is accounted for, so what's left is
                    # fractional date arithemtic. The result is always some
                    # kind of fractional datetime scalar.
                    otherarg = argtypes - {
                        'duration',
                        'std::cal::relative_duration',
                        'std::cal::date_duration'
                    }
                    if ldesc.typename == rdesc.typename:
                        # duration flavour is preserved
                        restype = ldesc.typename
                    elif len(otherarg) == 0:
                        # Some combo of durations make relative_duration.
                        restype = 'std::cal::relative_duration'
                    else:
                        othertype = otherarg.pop()
                        if othertype == 'std::cal::local_date':
                            # local_date + fractional durarion makes
                            # local_datetime
                            restype = 'std::cal::local_datetime'
                        else:
                            # Everything else is laready fractional, so just
                            # use the type of the other argument.
                            restype = othertype

                if restype:
                    await self.assert_query_result(query, [1])
                    await self.assert_query_result(
                        f"""SELECT ({left} + {right}) IS {restype};""",
                        [True],
                        msg=f'({left} + {right}) IS {restype}')
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
                restype = None

                if rdesc.signed and ldesc.signed:
                    # some kind of duration
                    if ldesc.typename == rdesc.typename:
                        restype = rdesc.typename
                    else:
                        # mixing duration types makes relative_duration
                        restype = 'std::cal::relative_duration'
                elif (ldesc.typename == 'std::cal::local_date' and
                        rdesc.typename == 'std::cal::local_date'):
                    restype = 'std::cal::date_duration'
                elif (ldesc.typename == 'std::cal::local_date' and
                        rdesc.typename == 'std::cal::date_duration'):
                    restype = 'std::cal::local_date'
                elif rdesc.signed:
                    # Subtracting some flavour of duration.
                    if ldesc.typename == 'std::cal::local_date':
                        restype = 'std::cal::local_datetime'
                    else:
                        # Preserve the [fractional] date/time type of the left
                        # argument.
                        restype = ldesc.typename
                elif rdesc.typename == ldesc.typename == 'datetime':
                    restype = 'duration'
                elif {rdesc.typename, ldesc.typename} == {
                    'std::cal::local_datetime'
                }:
                    restype = 'std::cal::relative_duration'
                elif {rdesc.typename, ldesc.typename} == {
                    'std::cal::local_time'
                }:
                    restype = 'std::cal::relative_duration'
                elif {rdesc.typename, ldesc.typename} == {
                    'std::cal::local_datetime',
                    'std::cal::local_date',
                }:
                    # mix of local_date and local_datetime
                    restype = 'std::cal::relative_duration'

                if restype:
                    await self.assert_query_result(query, [1])
                    await self.assert_query_result(
                        f"""SELECT ({left} - {right}) IS {restype};""",
                        [True],
                        msg=f'({left} - {right}) IS {restype}')
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

    async def test_edgeql_expr_valid_arithmetic_12(self):
        with self.assertRaises(edgedb.errors.InvalidValueError):
            await self.con.query('SELECT (-4)^(-0.5);')

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
                argtypes = {ldesc.typename, rdesc.typename}

                if (
                    (ldesc.typename == rdesc.typename)
                    or
                    # relative_duration and date_duration are
                    # compatible for union due to implicit
                    # casting
                    (
                        argtypes == {
                            'std::cal::relative_duration',
                            'std::cal::date_duration'
                        }
                    )
                    or
                    # local_date and local_datetime are
                    # compatible for union due to implicit
                    # casting
                    (
                        argtypes == {
                            'std::cal::local_date',
                            'std::cal::local_datetime'
                        }
                    )
                ):
                    # these scalars can only be UNIONed with
                    # themselves implicitly
                    await self.assert_query_result(query, [2])

                    query = f"""
                        SELECT (INTROSPECT TYPEOF ({left} UNION {right})).name
                    """
                    # this operation should always be valid
                    if 'std::cal::relative_duration' in argtypes:
                        # This is possible when relative_duration and
                        # date_duration mix and the result is implicitly cast
                        # to relative_duration.
                        desc_typename = 'std::cal::relative_duration'
                    elif 'std::cal::local_datetime' in argtypes:
                        # This is possible when local_datetime and
                        # local_date mix and the result is implicitly cast
                        # to local_datetime.
                        desc_typename = 'std::cal::local_datetime'
                    elif rdesc.typename.startswith('std::cal::'):
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
        for left, ldesc in get_test_items(anyreal=False):
            for right, rdesc in get_test_items():
                for op in ['??', 'IF random() > 0.5 ELSE']:
                    query = f"""SELECT count({left} {op} {right});"""
                    argtypes = {ldesc.typename, rdesc.typename}

                    if (
                        (ldesc.typename == rdesc.typename)
                        or
                        # relative_duration and date_duration are
                        # compatible for union due to implicit
                        # casting
                        (
                            argtypes == {
                                'std::cal::relative_duration',
                                'std::cal::date_duration'
                            }
                        )
                        or
                        # local_date and local_datetime are
                        # compatible for union due to implicit
                        # casting
                        (
                            argtypes == {
                                'std::cal::local_date',
                                'std::cal::local_datetime'
                            }
                        )
                    ):
                        # these scalars can only be UNIONed with
                        # themselves implicitly
                        await self.assert_query_result(query, [1])

                        # this operation should always be valid
                        if 'std::cal::relative_duration' in argtypes:
                            # This is possible when relative_duration and
                            # date_duration mix and the result is implicitly
                            # cast to relative_duration.
                            desc_typename = 'std::cal::relative_duration'
                        elif 'std::cal::local_datetime' in argtypes:
                            # This is possible when local_datetime and
                            # local_date mix and the result is implicitly cast
                            # to local_datetime.
                            desc_typename = 'std::cal::local_datetime'
                        elif rdesc.typename.startswith('std::cal::'):
                            desc_typename = rdesc.typename
                        else:
                            desc_typename = 'std::' + rdesc.typename

                        query = f"""
                            SELECT ({left} {op} {right}) IS {desc_typename};
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
        # This is OK because Issue.id is a property, not a link
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
                r"type 'A' does not exist"):
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
                'schema::AccessPolicy',
                'schema::Alias',
                'schema::Annotation',
                'schema::AnnotationSubject',
                'schema::Array',
                'schema::ArrayExprAlias',
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

    async def test_edgeql_expr_set_04(self):
        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} except 2
                order by _;
            """,
            [1, 3, 4],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} except 2 except 4
                order by _;
            """,
            [1, 3],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} except {1, 2}
                order by _;
            """,
            [3, 4],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} except {4, 5}
                order by _;
            """,
            [1, 2, 3],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} except {5, 6}
                order by _;
            """,
            [1, 2, 3, 4],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 1, 1, 2, 2, 3} except {1, 3, 3, 2}
                order by _;
            """,
            [1, 1, 2],
        )

    async def test_edgeql_expr_set_06(self):
        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} intersect 2
                order by _;
            """,
            [2],
        )

        await self.assert_query_result(
            r"""
                select _ :=
                    {1, 2, 3, 4} intersect {2, 3, 4} intersect {2, 4}
                order by _;
            """,
            [2, 4],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} intersect {5, 6}
                order by _;
            """,
            [],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 2, 3, 4} intersect 4
                order by _;
            """,
            [4],
        )

        await self.assert_query_result(
            r"""
                select _ := {1, 1, 1, 2, 2, 3} intersect {1, 3, 3, 2, 2, 5}
                order by _;
            """,
            [1, 2, 2, 3],
        )

    async def test_edgeql_expr_set_07(self):
        await self.assert_query_result(
            r"""
                select {<optional int64>$0, <optional int64>$0};
            """,
            [],
            variables=(None,)
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
        await self.assert_query_result(
            """
                SELECT [1, 2] UNION [];
            """,
            [[1, 2], []],
        )

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

    async def test_edgeql_expr_array_26(self):
        await self.assert_query_result(
            r'''
            with x := [1]
            select <array<int64>>x;
            ''',
            [[1]],
        )

    async def test_edgeql_expr_array_27(self):
        # Big array with nontrivial elements
        N = 350
        body = '0+1, ' * N

        await self.assert_query_result(
            f'select [{body}]',
            [[1] * N],
            json_only=True,
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

    async def test_edgeql_expr_range_empty_01(self):
        # Test handling of empty ranges

        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    select
                      range(
                          <{st}>1, <{st}>1
                        ) = range(<{st}>{{}}, empty := true);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select
                      range(
                          <{st}>1,
                          <{st}>1,
                          inc_lower := false,
                          inc_upper := true,
                      ) = range(<{st}>{{}}, empty := true);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select
                      range(<{st}>1, <{st}>1, inc_upper := true)
                        = range(<{st}>{{}}, empty := true);
                ''',
                [False],
            )

            await self.assert_query_result(
                f'''
                    select
                      range_is_empty(
                        range(
                          <{st}>{{}},
                          empty := true,
                        )
                      )
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select
                      range_is_empty(
                        range(
                          <{st}>{{}},
                          empty := false,
                        )
                      )
                ''',
                [False],
            )

    async def test_edgeql_expr_range_empty_02(self):
        # Test handling of bad empty ranges
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                "conflicting arguments in range constructor",
            ):
                await self.assert_query_result(
                    f'''
                        select range(<{st}>1, <{st}>2, empty := true)
                    ''',
                    [True],
                )

    async def test_edgeql_expr_range_empty_03(self):
        # Test handling of empty multiranges
        for st in [
            'int32', 'int64', 'float32', 'float64', 'decimal',
            'datetime', 'cal::local_datetime', 'cal::local_date',
        ]:
            await self.assert_query_result(
                f'''
                    select
                      multirange(<array<range<{st}>>>[]) =
                        multirange([range(<{st}>{{}}, empty := true)]);
                ''',
                [True],
            )

    async def _test_range_op(self, r0, r1, op, answer):
        # Test operators for various combinaitons of ranges and multiranges.
        await self.assert_query_result(
            f'''
            select (
                    {r0} {op} {r1},
                    multirange([{r0}]) {op} {r1},
                    {r0} {op} multirange([{r1}]),
                    multirange([{r0}]) {op} multirange([{r1}]),
            );''',
            [[answer, answer, answer, answer]],
            msg=f'problem with {op!r}',
        )

    async def test_edgeql_expr_range_01(self):
        # Test equality for numeric ranges and multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            for ops in [('=', '!='), ('?=', '?!=')]:
                # equals
                for op in ops:
                    answer = op == ops[0]
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>-1, <{st}>2)''',
                        op, answer,
                    )
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>-1, <{st}>2,
                                  inc_lower := true)''',
                        op, answer,
                    )
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>-1, <{st}>2,
                                  inc_upper := false)''',
                        op, answer,
                    )
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>-1, <{st}>2,
                                  inc_lower := true,
                                  inc_upper := false)''',
                        op, answer,
                    )
                    await self._test_range_op(
                        f'''range(<{st}>{{}}, <{st}>2)''',
                        f'''range(<{st}>{{}}, <{st}>2)''',
                        op, answer,
                    )

                    await self._test_range_op(
                        f'''range(<{st}>1, <{st}>{{}})''',
                        f'''range(<{st}>1, <{st}>{{}})''',
                        op, answer,
                    )

                # not equals
                for op in ops:
                    answer = op != ops[0]
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>1, <{st}>3)''',
                        op, answer,
                    )
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>-1, <{st}>2,
                                  inc_lower := false)''',
                        op, answer,
                    )
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>-1, <{st}>2,
                                  inc_upper := true)''',
                        op, answer,
                    )
                    await self._test_range_op(
                        f'''range(<{st}>-1, <{st}>2)''',
                        f'''range(<{st}>-1, <{st}>2,
                                  inc_lower := false,
                                  inc_upper := true)''',
                        op, answer,
                    )

    async def test_edgeql_expr_range_02(self):
        # Test comparison for numeric ranges and multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            for op in ['>', '<=']:
                answer = op == '>'
                await self._test_range_op(
                    f'''range(<{st}>1, <{st}>2)''',
                    f'''range(<{st}>-1, <{st}>2)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>1, <{st}>2)''',
                    f'''range(<{st}>-1, <{st}>20)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>1, <{st}>3)''',
                    f'''range(<{st}>1, <{st}>2)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>1, <{st}>{{}})''',
                    f'''range(<{st}>1, <{st}>2)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>1, <{st}>3)''',
                    f'''range(<{st}>{{}}, <{st}>2)''',
                    op, answer,
                )

            for op in ['<', '>=']:
                answer = op == '<'
                await self._test_range_op(
                    f'''range(<{st}>-2, <{st}>2)''',
                    f'''range(<{st}>1, <{st}>2)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>-2, <{st}>20)''',
                    f'''range(<{st}>1, <{st}>2)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>1, <{st}>2)''',
                    f'''range(<{st}>1, <{st}>3)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>1, <{st}>2)''',
                    f'''range(<{st}>1, <{st}>{{}})''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<{st}>{{}}, <{st}>2)''',
                    f'''range(<{st}>1, <{st}>3)''',
                    op, answer,
                )

    async def test_edgeql_expr_range_03(self):
        # Test bound for numeric ranges and multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            is_int = st.startswith('int')

            for r, res in [
                (f'range(<{st}>1, <{st}>5)', [5]),
                (
                    f'range(<{st}>1, <{st}>5, inc_upper := true)',
                    # The upper bound for integers is never included
                    [6 if is_int else 5],
                ),
                (f'range(<{st}>1)', [])
            ]:
                await self.assert_query_result(
                    f'select range_get_upper({r});', res)
                await self.assert_query_result(
                    f'select range_get_upper(multirange([{r}]));', res)

            for r, res in [
                (f'range(<{st}>1, <{st}>5)', [1]),
                (
                    f'range(<{st}>1, <{st}>5, inc_lower := false)',
                    # The lower bound for integers is always included
                    [2 if is_int else 1],
                ),
                (f'range(<{st}>{{}}, <{st}>5)', []),
            ]:
                await self.assert_query_result(
                    f'select range_get_lower({r});', res)
                await self.assert_query_result(
                    f'select range_get_lower(multirange([{r}]));', res)

    async def test_edgeql_expr_range_04(self):
        # Test bound for numeric ranges and multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            is_int = st.startswith('int')

            for r, res in [
                (f'range(<{st}>1, <{st}>5)', [False]),
                (
                    f'range(<{st}>1, <{st}>5, inc_upper := true)',
                    # The upper bound for integers is never included
                    [not is_int],
                ),
                (f'range(<{st}>{{}})', [False])
            ]:
                await self.assert_query_result(
                    f'select range_is_inclusive_upper({r});', res)
                await self.assert_query_result(
                    f'select range_is_inclusive_upper(multirange([{r}]));',
                    res,
                )

            for r, res in [
                (f'range(<{st}>1, <{st}>5)', [True]),
                (
                    f'range(<{st}>1, <{st}>5, inc_lower := false)',
                    # The lower bound for integers is always included
                    [is_int],
                ),
                (f'range(<{st}>{{}})', [False])
            ]:
                await self.assert_query_result(
                    f'select range_is_inclusive_lower({r});', res)
                await self.assert_query_result(
                    f'select range_is_inclusive_lower(multirange([{r}]));',
                    res,
                )

            await self.assert_query_result(
                f'''select range_is_inclusive_upper(
                        range(<{st}>1, <{st}>5));''',
                [False],
            )

    async def test_edgeql_expr_range_05(self):
        # Test addition for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    select range(<{st}>1, <{st}>5) + range(<{st}>2, <{st}>7) =
                        range(<{st}>1, <{st}>7);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select
                        range(<{st}>{{}}, <{st}>5) + range(<{st}>2, <{st}>7) =
                        range(<{st}>{{}}, <{st}>7);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select range(<{st}>2) + range(<{st}>1, <{st}>7) =
                        range(<{st}>1);
                ''',
                [True],
            )

    async def test_edgeql_expr_range_06(self):
        # Test intersection for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    select range(<{st}>1, <{st}>5) * range(<{st}>2, <{st}>7) =
                        range(<{st}>2, <{st}>5);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select
                        range(<{st}>{{}}, <{st}>5) * range(<{st}>2, <{st}>7) =
                        range(<{st}>2, <{st}>5);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select range(<{st}>2) * range(<{st}>1, <{st}>7) =
                        range(<{st}>2, <{st}>7);
                ''',
                [True],
            )

    async def test_edgeql_expr_range_07(self):
        # Test subtraction for numeric ranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    select range(<{st}>1, <{st}>5) - range(<{st}>2, <{st}>7) =
                        range(<{st}>1, <{st}>2);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select
                        range(<{st}>{{}}, <{st}>5) - range(<{st}>2, <{st}>7) =
                        range(<{st}>{{}}, <{st}>2);
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    select range(<{st}>2) - range(<{st}>1, <{st}>7) =
                        range(<{st}>7);
                ''',
                [True],
            )

    async def test_edgeql_expr_range_08(self):
        # Test equality for datetime ranges and multiranges.
        for ops in [('=', '!='), ('?=', '?!=')]:
            # equals
            for op in ops:
                answer = op == ops[0]
                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    op, answer,
                )

                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z',
                              inc_lower := true)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z',
                              inc_upper := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z',
                              inc_lower := true,
                              inc_upper := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<datetime>{{}},
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>{{}},
                              <datetime>'2022-06-16T00:00:00Z')''',
                    op, answer,
                )

                await self._test_range_op(
                    f'''range(<datetime>'2022-06-06T00:00:00Z',
                              <datetime>{{}})''',
                    f'''range(<datetime>'2022-06-06T00:00:00Z',
                              <datetime>{{}})''',
                    op, answer,
                )

            # not equals
            for op in ops:
                answer = op != ops[0]
                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-06T00:00:00Z',
                              <datetime>'2022-06-26T00:00:00Z')''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z',
                              inc_lower := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z',
                              inc_upper := true)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z')''',
                    f'''range(<datetime>'2022-06-01T00:00:00Z',
                              <datetime>'2022-06-16T00:00:00Z',
                              inc_lower := false,
                              inc_upper := true)''',
                    op, answer,
                )

    async def test_edgeql_expr_range_09(self):
        # Test comparison for datetime ranges and multiranges.
        for op in ['>', '<=']:
            answer = op == '>'
            await self._test_range_op(
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                f'''range(<datetime>'2022-06-02T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                f'''range(<datetime>'2022-06-02T00:00:00Z',
                          <datetime>'2022-06-30T00:00:00Z')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-13T00:00:00Z')''',
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>{{}})''',
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-13T00:00:00Z')''',
                f'''range(<datetime>{{}},
                          <datetime>'2022-06-10T00:00:00Z')''',
                op, answer,
            )

        for op in ['<', '>=']:
            answer = op == '<'
            await self._test_range_op(
                f'''range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>'2022-06-01T00:00:00Z',
                          <datetime>'2022-06-30T00:00:00Z')''',
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-13T00:00:00Z')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-10T00:00:00Z')''',
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>{{}})''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<datetime>{{}},
                          <datetime>'2022-06-10T00:00:00Z')''',
                f'''range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-13T00:00:00Z')''',
                op, answer,
            )

    async def test_edgeql_expr_range_10(self):
        # Test bound for datetime ranges and multiranges.
        await self.assert_query_result(
            f'''select <str>range_get_upper(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z'));''',
            ['2022-06-15T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_upper(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_upper := true));''',
            ['2022-06-15T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_upper(
                    range(<datetime>'2022-06-06T00:00:00Z'));''',
            [],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z'));''',
            ['2022-06-06T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_lower := false));''',
            ['2022-06-06T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_lower(
                    range(<datetime>{{}},
                          <datetime>'2022-06-15T00:00:00Z'));''',
            [],
        )

        # multiranges
        await self.assert_query_result(
            f'''select <str>range_get_upper(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z')]));''',
            ['2022-06-15T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_upper(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_upper := true)]));''',
            ['2022-06-15T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_upper(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z')]));''',
            [],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z')]));''',
            ['2022-06-06T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_lower := false)]));''',
            ['2022-06-06T00:00:00+00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_lower(multirange([
                    range(<datetime>{{}},
                          <datetime>'2022-06-15T00:00:00Z')]));''',
            [],
        )

    async def test_edgeql_expr_range_11(self):
        # Test bound for datetime ranges and multirange.
        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z'));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_upper := true));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<datetime>{{}}));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z'));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_lower := false));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(range(<datetime>{{}}));''',
            [False],
        )

        # multiranges
        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z')]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_upper := true)]));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<datetime>{{}})]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z')]));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z',
                          inc_lower := false)]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<datetime>{{}})]));''',
            [False],
        )

    async def test_edgeql_expr_range_12(self):
        # Test addition for datetime ranges.
        await self.assert_query_result(
            f'''
                select range(<datetime>'2022-06-06T00:00:00Z',
                             <datetime>'2022-06-15T00:00:00Z') +
                       range(<datetime>'2022-06-10T00:00:00Z',
                             <datetime>'2022-06-17T00:00:00Z') =
                    range(<datetime>'2022-06-06T00:00:00Z',
                          <datetime>'2022-06-17T00:00:00Z');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<datetime>{{}},
                          <datetime>'2022-06-15T00:00:00Z') +
                    range(<datetime>'2022-06-10T00:00:00Z',
                          <datetime>'2022-06-17T00:00:00Z') =
                    range(<datetime>{{}},
                          <datetime>'2022-06-17T00:00:00Z');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select range(<datetime>'2022-06-10T00:00:00Z') +
                       range(<datetime>'2022-06-06T00:00:00Z',
                             <datetime>'2022-06-17T00:00:00Z') =
                       range(<datetime>'2022-06-06T00:00:00Z');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_13(self):
        # Test intersection for datetime ranges.
        await self.assert_query_result(
            f'''
                select range(<datetime>'2022-06-06T00:00:00Z',
                             <datetime>'2022-06-15T00:00:00Z') *
                       range(<datetime>'2022-06-10T00:00:00Z',
                             <datetime>'2022-06-17T00:00:00Z') =
                       range(<datetime>'2022-06-10T00:00:00Z',
                             <datetime>'2022-06-15T00:00:00Z');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<datetime>{{}},
                          <datetime>'2022-06-15T00:00:00Z') *
                    range(<datetime>'2022-06-10T00:00:00Z',
                          <datetime>'2022-06-17T00:00:00Z') =
                    range(<datetime>'2022-06-10T00:00:00Z',
                          <datetime>'2022-06-15T00:00:00Z');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select range(<datetime>'2022-06-10T00:00:00Z') *
                       range(<datetime>'2022-06-06T00:00:00Z',
                             <datetime>'2022-06-17T00:00:00Z') =
                    range(<datetime>'2022-06-10T00:00:00Z',
                          <datetime>'2022-06-17T00:00:00Z');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_14(self):
        # Test subtraction for datetime ranges.
        await self.assert_query_result(
            f'''
                select range(<datetime>'2022-06-06T00:00:00Z',
                             <datetime>'2022-06-15T00:00:00Z') -
                       range(<datetime>'2022-06-10T00:00:00Z',
                             <datetime>'2022-06-17T00:00:00Z') =
                       range(<datetime>'2022-06-06T00:00:00Z',
                             <datetime>'2022-06-10T00:00:00Z');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<datetime>{{}}, <datetime>'2022-06-15T00:00:00Z') -
                    range(<datetime>'2022-06-10T00:00:00Z',
                          <datetime>'2022-06-17T00:00:00Z') =
                    range(<datetime>{{}}, <datetime>'2022-06-10T00:00:00Z');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select range(<datetime>'2022-06-10T00:00:00Z') -
                       range(<datetime>'2022-06-06T00:00:00Z',
                             <datetime>'2022-06-17T00:00:00Z') =
                       range(<datetime>'2022-06-17T00:00:00Z');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_15(self):
        # Test equality for datetime range and multiranges.
        for ops in [('=', '!='), ('?=', '?!=')]:
            # equals
            for op in ops:
                answer = op == ops[0]
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00',
                              inc_lower := true)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00',
                              inc_upper := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00',
                              inc_lower := true,
                              inc_upper := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_datetime>{{}},
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>{{}},
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    op, answer,
                )

                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                              <cal::local_datetime>{{}})''',
                    f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                              <cal::local_datetime>{{}})''',
                    op, answer,
                )

            # not equals
            for op in ops:
                answer = op != ops[0]
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                              <cal::local_datetime>'2022-06-26T00:00:00')''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00',
                              inc_lower := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00',
                              inc_upper := true)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00')''',
                    f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                              <cal::local_datetime>'2022-06-16T00:00:00',
                              inc_lower := false,
                              inc_upper := true)''',
                    op, answer,
                )

    async def test_edgeql_expr_range_16(self):
        # Test comparison for datetime ranges.
        for op in ['>', '<=']:
            answer = op == '>'
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-02T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-02T00:00:00',
                          <cal::local_datetime>'2022-06-30T00:00:00')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-13T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>{{}})''',
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-13T00:00:00')''',
                f'''range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                op, answer,
            )

        for op in ['<', '>=']:
            answer = op == '<'
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-01T00:00:00',
                          <cal::local_datetime>'2022-06-30T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-13T00:00:00')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>{{}})''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-10T00:00:00')''',
                f'''range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-13T00:00:00')''',
                op, answer,
            )

    async def test_edgeql_expr_range_17(self):
        # Test bound for datetime ranges and multiranges.
        await self.assert_query_result(
            f'''select <str>range_get_upper(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00'));''',
            ['2022-06-15T00:00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_upper(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_upper := true));''',
            ['2022-06-15T00:00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_upper(
                    range(<cal::local_datetime>'2022-06-06T00:00:00'));''',
            [],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00'));''',
            ['2022-06-06T00:00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_lower := false));''',
            ['2022-06-06T00:00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_lower(
                    range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-15T00:00:00'));''',
            [],
        )

        # multiranges
        await self.assert_query_result(
            f'''select <str>range_get_upper(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00')]));''',
            ['2022-06-15T00:00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_upper(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_upper := true)]));''',
            ['2022-06-15T00:00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_upper(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00')]));''',
            [],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00')]));''',
            ['2022-06-06T00:00:00'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_lower := false)]));''',
            ['2022-06-06T00:00:00'],
        )

        await self.assert_query_result(
            f'''select range_get_lower(multirange([
                    range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-15T00:00:00')]));''',
            [],
        )

        # check that return type of sql is correct
        # https://github.com/edgedb/edgedb/issues/6786
        await self.assert_query_result(
            f'''select <str>(range_get_upper(
                    range(
                        <cal::local_datetime>'2024-01-11T00:00:00',
                        <cal::local_datetime>'2025-01-11T00:00:00'
                    )
                ) - <cal::date_duration>"1 day");''',
            ['2025-01-10T00:00:00'],
        )

        await self.assert_query_result(
            f'''select <str>(range_get_lower(
                    range(
                        <cal::local_datetime>'2024-01-11T00:00:00',
                        <cal::local_datetime>'2025-01-11T00:00:00'
                    )
                ) - <cal::date_duration>"1 day");''',
            ['2024-01-10T00:00:00'],
        )

        await self.assert_query_result(
            f'''select <str>(range_get_upper(
                    multirange([range(
                        <cal::local_datetime>'2024-01-11T00:00:00',
                        <cal::local_datetime>'2025-01-11T00:00:00'
                    )])
                ) - <cal::date_duration>"1 day");''',
            ['2025-01-10T00:00:00'],
        )

        await self.assert_query_result(
            f'''select <str>(range_get_lower(
                    multirange([range(
                        <cal::local_datetime>'2024-01-11T00:00:00',
                        <cal::local_datetime>'2025-01-11T00:00:00'
                    )])
                ) - <cal::date_duration>"1 day");''',
            ['2024-01-10T00:00:00'],
        )

    async def test_edgeql_expr_range_18(self):
        # Test bound for datetime ranges and multiranges.
        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00'));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_upper := true));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<cal::local_datetime>{{}}));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00'));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_lower := false));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<cal::local_datetime>{{}}));''',
            [False],
        )

        # multiranges
        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00')]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_upper := true)]));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<cal::local_datetime>{{}})]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00')]));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00',
                          inc_lower := false)]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<cal::local_datetime>{{}})]));''',
            [False],
        )

    async def test_edgeql_expr_range_19(self):
        # Test addition for datetime ranges.
        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00') +
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-15T00:00:00') +
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-17T00:00:00');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>'2022-06-10T00:00:00') +
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>'2022-06-06T00:00:00');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_20(self):
        # Test intersection for datetime ranges.
        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00') *
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-15T00:00:00') *
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>'2022-06-10T00:00:00') *
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_21(self):
        # Test subtraction for datetime ranges.
        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-15T00:00:00') -
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-10T00:00:00');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-15T00:00:00') -
                    range(<cal::local_datetime>'2022-06-10T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>{{}},
                          <cal::local_datetime>'2022-06-10T00:00:00');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_datetime>'2022-06-10T00:00:00') -
                    range(<cal::local_datetime>'2022-06-06T00:00:00',
                          <cal::local_datetime>'2022-06-17T00:00:00') =
                    range(<cal::local_datetime>'2022-06-17T00:00:00');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_22(self):
        # Test equality for date ranges and multiranges.
        for ops in [('=', '!='), ('?=', '?!=')]:
            # equals
            for op in ops:
                answer = op == ops[0]
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16',
                              inc_lower := true)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16',
                              inc_upper := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16',
                              inc_lower := true,
                              inc_upper := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_date>{{}},
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>{{}},
                              <cal::local_date>'2022-06-16')''',
                    op, answer,
                )

                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-06',
                              <cal::local_date>{{}})''',
                    f'''range(<cal::local_date>'2022-06-06',
                              <cal::local_date>{{}})''',
                    op, answer,
                )

            # not equals
            for op in ops:
                answer = op != ops[0]
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-06',
                              <cal::local_date>'2022-06-26')''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16',
                              inc_lower := false)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16',
                              inc_upper := true)''',
                    op, answer,
                )
                await self._test_range_op(
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16')''',
                    f'''range(<cal::local_date>'2022-06-01',
                              <cal::local_date>'2022-06-16',
                              inc_lower := false,
                              inc_upper := true)''',
                    op, answer,
                )

    async def test_edgeql_expr_range_23(self):
        # Test comparison for date ranges.
        for op in ['>', '<=']:
            answer = op == '>'
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                f'''range(<cal::local_date>'2022-06-02',
                          <cal::local_date>'2022-06-10')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                f'''range(<cal::local_date>'2022-06-02',
                          <cal::local_date>'2022-06-30')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-13')''',
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>{{}})''',
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-13')''',
                f'''range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-10')''',
                op, answer,
            )

        for op in ['<', '>=']:
            answer = op == '<'
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-10')''',
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-01',
                          <cal::local_date>'2022-06-30')''',
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-13')''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10')''',
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>{{}})''',
                op, answer,
            )
            await self._test_range_op(
                f'''range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-10')''',
                f'''range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-13')''',
                op, answer,
            )

    async def test_edgeql_expr_range_24(self):
        # Test bound for date ranges and multiranges.
        await self.assert_query_result(
            f'''select <str>range_get_upper(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15'));''',
            ['2022-06-15'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_upper(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_upper := true));''',
            ['2022-06-16'],
        )

        await self.assert_query_result(
            f'''select range_get_upper(
                    range(<cal::local_date>'2022-06-06'));''',
            [],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15'));''',
            ['2022-06-06'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_lower := false));''',
            ['2022-06-07'],
        )

        await self.assert_query_result(
            f'''select range_get_lower(
                    range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-15'));''',
            [],
        )

        # multiranges
        await self.assert_query_result(
            f'''select <str>range_get_upper(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15')]));''',
            ['2022-06-15'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_upper(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_upper := true)]));''',
            ['2022-06-16'],
        )

        await self.assert_query_result(
            f'''select range_get_upper(multirange([
                    range(<cal::local_date>'2022-06-06')]));''',
            [],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15')]));''',
            ['2022-06-06'],
        )

        await self.assert_query_result(
            f'''select <str>range_get_lower(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_lower := false)]));''',
            ['2022-06-07'],
        )

        await self.assert_query_result(
            f'''select range_get_lower(multirange([
                    range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-15')]));''',
            [],
        )

        # check that return type of sql is correct
        # https://github.com/edgedb/edgedb/issues/6786
        await self.assert_query_result(
            f'''select <str>(range_get_upper(
                    range(
                        <cal::local_date>'2024-01-11',
                        <cal::local_date>'2025-01-11'
                    )
                ) - <cal::date_duration>"1 day");''',
            ['2025-01-10'],
        )

        await self.assert_query_result(
            f'''select <str>(range_get_lower(
                    range(
                        <cal::local_date>'2024-01-11',
                        <cal::local_date>'2025-01-11'
                    )
                ) - <cal::date_duration>"1 day");''',
            ['2024-01-10'],
        )

        await self.assert_query_result(
            f'''select <str>(range_get_upper(
                    multirange([range(
                        <cal::local_date>'2024-01-11',
                        <cal::local_date>'2025-01-11'
                    )])
                ) - <cal::date_duration>"1 day");''',
            ['2025-01-10'],
        )

        await self.assert_query_result(
            f'''select <str>(range_get_lower(
                    multirange([range(
                        <cal::local_date>'2024-01-11',
                        <cal::local_date>'2025-01-11'
                    )])
                ) - <cal::date_duration>"1 day");''',
            ['2024-01-10'],
        )

    async def test_edgeql_expr_range_25(self):
        # Test bound for date ranges and multiranges.
        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15'));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_upper := true));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(
                    range(<cal::local_date>{{}}));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15'));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_lower := false));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(
                    range(<cal::local_date>{{}}));''',
            [False],
        )

        # multiranges
        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15')]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_upper := true)]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_upper(multirange([
                    range(<cal::local_date>{{}})]));''',
            [False],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15')]));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15',
                          inc_lower := false)]));''',
            [True],
        )

        await self.assert_query_result(
            f'''select range_is_inclusive_lower(multirange([
                    range(<cal::local_date>{{}})]));''',
            [False],
        )

    async def test_edgeql_expr_range_26(self):
        # Test addition for date ranges.
        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15') +
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-17');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-15') +
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-17');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>'2022-06-10') +
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>'2022-06-06');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_27(self):
        # Test intersection for date ranges.
        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15') *
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-15');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-15') *
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-15');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>'2022-06-10') *
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-17');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_28(self):
        # Test subtraction for date ranges.
        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-15') -
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-10');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-15') -
                    range(<cal::local_date>'2022-06-10',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>{{}},
                          <cal::local_date>'2022-06-10');
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                select
                    range(<cal::local_date>'2022-06-10') -
                    range(<cal::local_date>'2022-06-06',
                          <cal::local_date>'2022-06-17') =
                    range(<cal::local_date>'2022-06-17');
            ''',
            [True],
        )

    async def test_edgeql_expr_range_29(self):
        # Test casts between numeric ranges. Only a subset of numeric types
        # support ranges.
        valid_types = {'int32', 'int64', 'float32', 'float64', 'decimal'}

        # Test valid range casts.
        for _, desc0 in get_test_items(anyreal=True):
            t0 = desc0.typename
            for _, desc1 in get_test_items(anyreal=True):
                t1 = desc1.typename

                # The query is intended to test casts, not constructors, so we
                # use a constructor that's definitely valid and then perform
                # two consecutive casts on it to determine if any of the casts
                # are legal.
                #
                # We test casting:
                # 1) range -> range
                # 2) multirange -> multirange
                # 3) range -> multirange (of matching subtype)
                query = f'''
                    with r := range(2, 9)
                    select count(
                        (
                            <range<{t0}>><range<{t1}>>r,
                            <multirange<{t0}>><multirange<{t1}>>
                                multirange([r]),
                            <multirange<{t0}>><range<{t0}>>r,
                        )
                    );
                '''

                if {t0, t1}.issubset(valid_types):
                    await self.assert_query_result(
                        query, [1], msg=query
                    )

                    # Test casting of empty ranges.
                    query = f'''
                        with r := range(<{t1}>{{}}, empty := true)
                        select (
                            range_is_empty(
                                <range<{t0}>>r
                            ),
                            range_is_empty(
                                <multirange<{t1}>>r
                            ),
                            range_is_empty(
                                <multirange<{t0}>>multirange(
                                    <array<range<{t1}>>>[]
                                )
                            ),
                        );
                    '''

                    await self.assert_query_result(
                        query, [[True, True, True]], msg=query
                    )
                else:
                    async with self.assertRaisesRegexTx(
                        edgedb.UnsupportedFeatureError,
                        r'unsupported range subtype',
                        msg=query,
                    ):
                        await self.con.query_single(query)

    async def test_edgeql_expr_range_30(self):
        # Test implicit casting of numeric ranges by using `=`, which is
        # defined only for matching operand range types.
        #
        # In the end this makes most numeric ranges compatible because every
        # one of them except for decimal are implicitly castable to
        # range<float64>, which then allows comparisons.
        #
        # Same tests are also run for pairs of multiranges.
        for t0 in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            for t1 in ['int32', 'int64', 'float32', 'float64', 'decimal']:
                query = f'''
                    with
                        r0 := range(<{t0}>2, <{t0}>9),
                        r1 := range(<{t1}>2, <{t1}>9),
                    select (
                        r0 = r1,
                        multirange([r0]) = multirange([r1]),
                    );
                '''

                if (
                    t0 == t1 or
                    (t0, t1) not in {
                        ('float32', 'decimal'), ('decimal', 'float32'),
                        ('float64', 'decimal'), ('decimal', 'float64'),
                    }
                ):
                    await self.assert_query_result(
                        query, [[True, True]], msg=query
                    )
                else:
                    async with self.assertRaisesRegexTx(
                        edgedb.InvalidTypeError,
                        r'cannot be applied to operands of type',
                        msg=query,
                    ):
                        await self.con.query_single(query)

    async def test_edgeql_expr_range_31(self):
        # Test casting of local_datetime and local_date ranges and
        # multiranges.

        await self.assert_query_result(
            f'''
                with
                    r := range(
                        <cal::local_date>'2022-06-10',
                        <cal::local_date>'2022-06-17'
                    )
                select count((
                    <range<cal::local_datetime>>r,
                    <multirange<cal::local_datetime>>multirange([r]),
                    <multirange<cal::local_date>>r,
                ))
            ''',
            [1],
        )

        await self.assert_query_result(
            f'''
                with
                    r := range(
                        <cal::local_datetime>'2022-06-10T00:00:00',
                        <cal::local_datetime>'2022-06-17T00:00:00'
                    )
                select count((
                    <range<cal::local_date>>r,
                    <multirange<cal::local_date>>multirange([r]),
                    <multirange<cal::local_datetime>>r,
                ))
            ''',
            [1],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(
                        <cal::local_datetime>'2022-06-10T00:00:00',
                        <cal::local_datetime>'2022-06-17T00:00:00'
                    ),
                    r1 := range(
                        <cal::local_date>'2022-06-10',
                        <cal::local_date>'2022-06-17'
                    ),
                select (
                    r0 = r1,
                    multirange([r0]) = multirange([r1])
                )
            ''',
            [[True, True]],
        )

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single(r'''
                select <range<datetime>>range(
                    <cal::local_datetime>'2022-06-10T00:00:00',
                    <cal::local_datetime>'2022-06-17T00:00:00'
                )
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single(r'''
                select <range<datetime>>range(
                    <cal::local_date>'2022-06-10',
                    <cal::local_date>'2022-06-17'
                )
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single(r'''
                select <range<cal::local_datetime>>range(
                    <datetime>'2022-06-10T00:00:00Z',
                    <datetime>'2022-06-17T00:00:00Z'
                )
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single(r'''
                select <range<cal::local_date>>range(
                    <datetime>'2022-06-10T00:00:00Z',
                    <datetime>'2022-06-17T00:00:00Z'
                )
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single('''
                select <multirange<datetime>>multirange([range(
                    <cal::local_datetime>'2022-06-10T00:00:00',
                    <cal::local_datetime>'2022-06-17T00:00:00'
                )])
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single('''
                select <multirange<datetime>>multirange([range(
                    <cal::local_date>'2022-06-10',
                    <cal::local_date>'2022-06-17'
                )])
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single('''
                select <multirange<cal::local_datetime>>multirange([range(
                    <datetime>'2022-06-10T00:00:00Z',
                    <datetime>'2022-06-17T00:00:00Z'
                )])
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'cannot cast'
        ):
            await self.con.query_single('''
                select <multirange<cal::local_date>>multirange([range(
                    <datetime>'2022-06-10T00:00:00Z',
                    <datetime>'2022-06-17T00:00:00Z'
                )])
            ''')

    async def test_edgeql_expr_range_32(self):
        # Test casting ranges to JSON.

        await self.assert_query_result(
            f'''
                select <json>range(<int32>2, <int32>10);
            ''',
            [{
                "lower": 2,
                "inc_lower": True,
                "upper": 10,
                "inc_upper": False,
            }],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>range(<int64>2, <int64>10);
            ''',
            [{
                "lower": 2,
                "inc_lower": True,
                "upper": 10,
                "inc_upper": False,
            }],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>range(<float32>2.5, <float32>10.5);
            ''',
            [{
                "lower": 2.5,
                "inc_lower": True,
                "upper": 10.5,
                "inc_upper": False,
            }],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>range(<float64>2.5, <float64>10.5);
            ''',
            [{
                "lower": 2.5,
                "inc_lower": True,
                "upper": 10.5,
                "inc_upper": False,
            }],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>range(2.5n, 10.5n);
            ''',
            [{
                "lower": 2.5,
                "inc_lower": True,
                "upper": 10.5,
                "inc_upper": False,
            }],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>range(
                    <datetime>'2022-06-10T13:00:00Z',
                    <datetime>'2022-06-17T12:00:00Z'
                );
            ''',
            [{
                "lower": "2022-06-10T13:00:00+00:00",
                "inc_lower": True,
                "upper": "2022-06-17T12:00:00+00:00",
                "inc_upper": False,
            }],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>range(
                    <cal::local_datetime>'2022-06-10T13:00:00',
                    <cal::local_datetime>'2022-06-17T12:00:00'
                );
            ''',
            [{
                "lower": "2022-06-10T13:00:00",
                "inc_lower": True,
                "upper": "2022-06-17T12:00:00",
                "inc_upper": False,
            }],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>range(
                    <cal::local_date>'2022-06-10',
                    <cal::local_date>'2022-06-17'
                );
            ''',
            [{
                "lower": "2022-06-10",
                "inc_lower": True,
                "upper": "2022-06-17",
                "inc_upper": False,
            }],
            json_only=True,
        )

    async def test_edgeql_expr_range_33(self):
        # Test casting ranges from JSON.

        await self.assert_query_result(
            r'''
                select <range<int32>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 10,
                    "inc_upper": false
                }') = range(<int32>2, <int32>10);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<int64>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 10,
                    "inc_upper": false
                }') = range(<int64>2, <int64>10);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<float32>>to_json('{
                    "lower": 2.5,
                    "inc_lower": true,
                    "upper": 10.5,
                    "inc_upper": false
                }') = range(<float32>2.5, <float32>10.5);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<float64>>to_json('{
                    "lower": 2.5,
                    "inc_lower": true,
                    "upper": 10.5,
                    "inc_upper": false
                }') = range(<float64>2.5, <float64>10.5);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<decimal>>to_json('{
                    "lower": 2.5,
                    "inc_lower": true,
                    "upper": 10.5,
                    "inc_upper": false
                }') = range(2.5n, 10.5n);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<datetime>>to_json('{
                    "lower": "2022-06-10T13:00:00+00:00",
                    "inc_lower": true,
                    "upper": "2022-06-17T12:00:00+00:00",
                    "inc_upper": false
                }') = range(
                    <datetime>'2022-06-10T13:00:00Z',
                    <datetime>'2022-06-17T12:00:00Z'
                );
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<cal::local_datetime>>to_json('{
                    "lower": "2022-06-10T13:00:00",
                    "inc_lower": true,
                    "upper": "2022-06-17T12:00:00",
                    "inc_upper": false
                }') = range(
                    <cal::local_datetime>'2022-06-10T13:00:00',
                    <cal::local_datetime>'2022-06-17T12:00:00'
                );
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<cal::local_date>>to_json('{
                    "lower": "2022-06-10",
                    "inc_lower": true,
                    "upper": "2022-06-17",
                    "inc_upper": false
                }') = range(
                    <cal::local_date>'2022-06-10',
                    <cal::local_date>'2022-06-17'
                );
            ''',
            [True],
        )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"conflicting arguments in range constructor",
        ):
            await self.con.query_single(r'''
                select exists <range<int64>>to_json('{
                    "lower": 0,
                    "upper": 1,
                    "inc_lower": true,
                    "inc_upper": false,
                    "empty": true
                }')
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"unexpected keys: bar, foo",
        ):
            await self.con.query_single(r'''
                select exists <range<int64>>to_json('{
                    "lower": 0,
                    "upper": 1,
                    "inc_lower": true,
                    "inc_upper": false,
                    "foo": "junk",
                    "bar": "huh?"
                }')
            ''')

    async def test_edgeql_expr_range_34(self):
        # Test casting ranges from JSON.

        await self.assert_query_result(
            r'''
                select <range<int64>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 10,
                    "inc_upper": true
                }') = range(2, 11);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<int64>>to_json('{
                    "lower": null,
                    "inc_lower": true,
                    "upper": 10,
                    "inc_upper": false
                }') = range(<int64>{}, 10);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                select <range<int64>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "inc_upper": false
                }') = range(2);
            ''',
            [True],
        )

        async with self.assertRaisesRegexTx(
            edgedb.NumericOutOfRangeError,
            r'"2147483648" is out of range for type std::int32'
        ):
            await self.con.execute(r"""
                select <range<int32>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 2147483648,
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.NumericOutOfRangeError,
            r'"9223372036854775808" is out of range for '
            r'type std::int64'
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 9223372036854775808,
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.NumericOutOfRangeError,
            r'.+ is out of range for type std::float32'
        ):
            await self.con.execute(r"""
                select <range<float32>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 1e100,
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.NumericOutOfRangeError,
            r'.+ is out of range for type std::float64'
        ):
            await self.con.execute(r"""
                select <range<float64>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 1e500,
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r'expected JSON number or null; got JSON string'
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('{
                    "lower": "2",
                    "inc_lower": true,
                    "upper": 10,
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r'invalid input syntax for type std::int64: "2.5"'
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('{
                    "lower": 2.5,
                    "inc_lower": true,
                    "upper": 10,
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"JSON object representing a range must include an 'inc_upper'"
            r" boolean property"
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('{
                    "lower": 2,
                    "inc_lower": true,
                    "upper": 10,
                    "inc_upper": null
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"JSON object representing a range must include an 'inc_lower'"
            r" boolean property"
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('{
                    "lower": 2,
                    "upper": 10,
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"expected JSON object or null; got JSON array"
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('["bad", null]');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"JSON object representing a range must include an 'inc_lower'"
            r" boolean property"
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('{"bad": null}');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"expected JSON object or null; got JSON string"
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('"bad"');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"expected JSON object or null; got JSON number"
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('1312');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"expected JSON object or null; got JSON boolean"
        ):
            await self.con.execute(r"""
                select <range<int64>>to_json('true');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"invalid input syntax for type std::cal::local_date: "
            r"'2022.06.10'"
        ):
            await self.con.execute(r"""
                select <range<cal::local_date>>to_json('{
                    "lower": "2022.06.10",
                    "inc_lower": true,
                    "upper": "2022-06-17",
                    "inc_upper": false
                }');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"invalid input syntax for type std::cal::local_date: "
            r"'12022-06-17'"
        ):
            await self.con.execute(r"""
                select <range<cal::local_date>>to_json('{
                    "lower": "2022-06-10",
                    "inc_lower": true,
                    "upper": "12022-06-17",
                    "inc_upper": false
                }');
            """)

        await self.assert_query_result(
            r'''
                select range_is_empty(<range<int64>>to_json('{
                    "empty": true
                }'))
            ''',
            [True],
        )

    async def test_edgeql_expr_range_35(self):
        # Test casting shapes containing ranges to JSON.

        await self.assert_query_result(
            r'''
                select <json>{
                    int := 42,
                    range0 := range(2, 10),
                    nested := {
                        range1 := range(5)
                    }
                };
            ''',
            [{
                "int": 42,
                "range0": {
                    "lower": 2,
                    "inc_lower": True,
                    "upper": 10,
                    "inc_upper": False,
                },
                "nested": {
                    "range1": {
                        "lower": 5,
                        "inc_lower": True,
                        "upper": None,
                        "inc_upper": False,
                    },
                },
            }],
            json_only=True,
        )

    async def test_edgeql_expr_range_36(self):
        # Test incorrect range bounds.

        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                "range lower bound must be",
            ):
                await self.con.execute(f"""
                    select range(<{st}>5, <{st}>1);
                """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "range lower bound must be",
        ):
            await self.con.execute("""
                select range(<datetime>'2022-07-09T23:56:17Z',
                             <datetime>'2022-07-08T23:56:17Z');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "range lower bound must be",
        ):
            await self.con.execute("""
                select range(<cal::local_datetime>'2022-07-09T23:56:17',
                             <cal::local_datetime>'2022-07-08T23:56:17');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "range lower bound must be",
        ):
            await self.con.execute("""
                select range(<cal::local_date>'2022-07-09',
                             <cal::local_date>'2022-07-08');
            """)

    async def test_edgeql_expr_range_37(self):
        # Test nullable arguments to range
        await self.assert_query_result(
            r'''
                select range(<int64>{}, empty:=<optional bool>$0);
            ''',
            [],
            variables=(None,),
        )

        await self.assert_query_result(
            r'''
                select range(<int64>{}, inc_lower:=<optional bool>$0);
            ''',
            [],
            variables=(None,),
        )

        await self.assert_query_result(
            r'''
                select range(<int64>{}, inc_upper:=<optional bool>$0);
            ''',
            [],
            variables=(None,),
        )

        await self.assert_query_result(
            r'''
                select range(
                    <int64>{}, inc_upper:=<optional bool>$0, empty := true);
            ''',
            [],
            variables=(None,),
        )

    async def test_edgeql_expr_range_38(self):
        # Test array of range as argument. We want to test all possible range
        # subtypes here.
        for typedval, desc in get_test_items():
            if desc.typename not in {
                'int32', 'int64', 'float32', 'float64', 'decimal',
                'datetime', 'cal::local_datetime', 'cal::local_date',
            }:
                continue

            if '>' in typedval:
                val = typedval.split('>')[1]
            else:
                val = typedval

            query = f'select <array<range<{desc.typename}>>>$0'
            ranges = [edgedb.Range(empty=True)]
            if desc.datetime:
                val = val.strip('"')
                if desc.typename == 'std::cal::local_date':
                    ranges.append(edgedb.Range(
                        datetime.date.fromisoformat(val)))
                else:
                    ranges.append(edgedb.Range(
                        datetime.datetime.fromisoformat(val)))
            else:
                val = 1
                ranges.append(edgedb.Range(val))

            await self.assert_query_result(
                query,
                [
                    [
                        {
                            "empty": True,
                        },
                        {
                            "lower": val,
                            "inc_lower": True,
                            "upper": None,
                            "inc_upper": False,
                        },
                    ]
                ],
                variables=(ranges,),
                msg=query
            )

    async def test_edgeql_expr_range_39(self):
        # Test array of multirange as argument. We want to test all possible
        # range subtypes here.
        for typedval, desc in get_test_items():
            if desc.typename not in {
                'int32', 'int64', 'float32', 'float64', 'decimal',
                'datetime', 'cal::local_datetime', 'cal::local_date',
            }:
                continue

            if '>' in typedval:
                val = typedval.split('>')[1]
            else:
                val = typedval

            query = f'select <array<multirange<{desc.typename}>>>$0'
            if desc.datetime:
                val = val.strip('"')
                if desc.typename == 'std::cal::local_date':
                    ranges = [
                        edgedb.Range(datetime.date.fromisoformat(val))
                    ]
                else:
                    ranges = [
                        edgedb.Range(datetime.datetime.fromisoformat(val))
                    ]
            else:
                val = 1
                ranges = [edgedb.Range(val)]

            await self.assert_query_result(
                query,
                [
                    [
                        [
                            {
                                "lower": val,
                                "inc_lower": True,
                                "upper": None,
                                "inc_upper": False,
                            },
                        ]
                    ]
                ],
                variables=([edgedb.MultiRange(ranges)],),
                msg=query
            )

    async def test_edgeql_expr_range_40(self):
        # test casting aliased range expr into range
        await self.assert_query_result(
            '''
            with x := range(1, 2)
            select <range<int64>>x
            ''',
            [{
                "lower": 1,
                "inc_lower": True,
                "upper": 2,
                "inc_upper": False,
            }],
            json_only=True
        )

    async def test_edgeql_expr_range_41(self):
        # Test casting multiranges to JSON.

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(<int32>12, <int32>20),
                    range(<int32>2, <int32>10),
                ]);
            ''',
            [
                [
                    {
                        "lower": 2,
                        "inc_lower": True,
                        "upper": 10,
                        "inc_upper": False,
                    },
                    {
                        "lower": 12,
                        "inc_lower": True,
                        "upper": 20,
                        "inc_upper": False,
                    },
                ],
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(<int64>12, <int64>20),
                    range(<int64>2, <int64>10),
                ]);
            ''',
            [
                [
                    {
                        "lower": 2,
                        "inc_lower": True,
                        "upper": 10,
                        "inc_upper": False,
                    },
                    {
                        "lower": 12,
                        "inc_lower": True,
                        "upper": 20,
                        "inc_upper": False,
                    },
                ],
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(<float32>2.5, <float32>10.5),
                    range(<float32>-2.5, <float32>0.5),
                ]);
            ''',
            [
                [
                    {
                        "lower": -2.5,
                        "inc_lower": True,
                        "upper": 0.5,
                        "inc_upper": False,
                    },
                    {
                        "lower": 2.5,
                        "inc_lower": True,
                        "upper": 10.5,
                        "inc_upper": False,
                    },
                ],
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(<float64>2.5, <float64>10.5),
                    range(<float64>-2.5, <float64>0.5),
                ]);
            ''',
            [
                [
                    {
                        "lower": -2.5,
                        "inc_lower": True,
                        "upper": 0.5,
                        "inc_upper": False,
                    },
                    {
                        "lower": 2.5,
                        "inc_lower": True,
                        "upper": 10.5,
                        "inc_upper": False,
                    },
                ],
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(2.5n, 10.5n),
                    range(-2.5n, 0.5n),
                ]);
            ''',
            [
                [
                    {
                        "lower": -2.5,
                        "inc_lower": True,
                        "upper": 0.5,
                        "inc_upper": False,
                    },
                    {
                        "lower": 2.5,
                        "inc_lower": True,
                        "upper": 10.5,
                        "inc_upper": False,
                    },
                ],
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(
                        <datetime>'2022-06-10T13:00:00Z',
                        <datetime>'2022-06-17T12:00:00Z'
                    ),
                    range(
                        <datetime>'2021-06-10T13:00:00Z',
                        <datetime>'2021-06-17T12:00:00Z'
                    ),
                ]);
            ''',
            [
                [
                    {
                        "lower": "2021-06-10T13:00:00+00:00",
                        "inc_lower": True,
                        "upper": "2021-06-17T12:00:00+00:00",
                        "inc_upper": False,
                    },
                    {
                        "lower": "2022-06-10T13:00:00+00:00",
                        "inc_lower": True,
                        "upper": "2022-06-17T12:00:00+00:00",
                        "inc_upper": False,
                    },
                ]
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(
                        <cal::local_datetime>'2022-06-10T13:00:00',
                        <cal::local_datetime>'2022-06-17T12:00:00'
                    ),
                    range(
                        <cal::local_datetime>'2021-06-10T13:00:00',
                        <cal::local_datetime>'2021-06-17T12:00:00'
                    ),
                ]);
            ''',
            [
                [
                    {
                        "lower": "2021-06-10T13:00:00",
                        "inc_lower": True,
                        "upper": "2021-06-17T12:00:00",
                        "inc_upper": False,
                    },
                    {
                        "lower": "2022-06-10T13:00:00",
                        "inc_lower": True,
                        "upper": "2022-06-17T12:00:00",
                        "inc_upper": False,
                    },
                ]
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange([
                    range(
                        <cal::local_date>'2022-06-10',
                        <cal::local_date>'2022-06-17'
                    ),
                    range(
                        <cal::local_date>'2021-06-10',
                        <cal::local_date>'2021-06-17'
                    ),
                ]);
            ''',
            [
                [
                    {
                        "lower": "2021-06-10",
                        "inc_lower": True,
                        "upper": "2021-06-17",
                        "inc_upper": False,
                    },
                    {
                        "lower": "2022-06-10",
                        "inc_lower": True,
                        "upper": "2022-06-17",
                        "inc_upper": False,
                    },
                ]
            ],
            json_only=True,
        )

        await self.assert_query_result(
            f'''
                select <json>multirange(<array<range<int64>>>[]);
            ''',
            [[]],
            json_only=True,
        )

    async def test_edgeql_expr_range_42(self):
        # Test casting multiranges from JSON.

        await self.assert_query_result(
            '''
                select <multirange<int64>>to_json('
                    [
                        {
                            "lower": 2,
                            "inc_lower": true,
                            "upper": 10,
                            "inc_upper": false
                        },
                        {
                            "lower": 12,
                            "inc_lower": true,
                            "upper": 20,
                            "inc_upper": false
                        }
                    ]
                ');
            ''',
            [
                [
                    {
                        "lower": 2,
                        "inc_lower": True,
                        "upper": 10,
                        "inc_upper": False,
                    },
                    {
                        "lower": 12,
                        "inc_lower": True,
                        "upper": 20,
                        "inc_upper": False,
                    },
                ],
            ],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with x := <json>[
                    range(12, 20),
                    range(2, 10)
                ]
                select <multirange<float64>>x;
            ''',
            [
                [
                    {
                        "lower": 2,
                        "inc_lower": True,
                        "upper": 10,
                        "inc_upper": False,
                    },
                    {
                        "lower": 12,
                        "inc_lower": True,
                        "upper": 20,
                        "inc_upper": False,
                    },
                ],
            ],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with x := <json>[range(<int64>{}, empty:=True)]
                select range_is_empty(<multirange<int64>>x);
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select range_is_empty(<multirange<int64>>to_json('[]'));
            ''',
            [True],
        )

    async def test_edgeql_expr_range_43(self):
        # Test casting shapes containing multiranges to JSON.

        await self.assert_query_result(
            r'''
                select <json>{
                    int := 42,
                    multirange0 := multirange(
                        [range(2, 10), range(12, 20)]
                    ),
                    nested := {
                        multirange1 := multirange(
                            [range(0, 1), range(5)]
                        )
                    }
                };
            ''',
            [{
                "int": 42,
                "multirange0": [
                    {
                        "lower": 2,
                        "inc_lower": True,
                        "upper": 10,
                        "inc_upper": False,
                    },
                    {
                        "lower": 12,
                        "inc_lower": True,
                        "upper": 20,
                        "inc_upper": False,
                    },
                ],
                "nested": {
                    "multirange1": [
                        {
                            "lower": 0,
                            "inc_lower": True,
                            "upper": 1,
                            "inc_upper": False,
                        },
                        {
                            "lower": 5,
                            "inc_lower": True,
                            "upper": None,
                            "inc_upper": False,
                        },
                    ],
                },
            }],
            json_only=True,
        )

    async def test_edgeql_expr_range_44(self):
        # Test addition for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    with
                        r0 := range(<{st}>1, <{st}>2),
                        r1 := range(<{st}>5, <{st}>7),
                        r2 := range(<{st}>{{}}, <{st}>-1),
                        r3 := range(<{st}>4),
                    select
                        multirange([r0, r1]) + multirange([r2, r3])
                        =
                        multirange([r0, r1, r2]) + r3
                ''',
                [True],
            )

    async def test_edgeql_expr_range_45(self):
        # Test intersection for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    with
                        r0 := range(<{st}>1, <{st}>3),
                        r1 := range(<{st}>5, <{st}>7),
                        r2 := range(<{st}>{{}}, <{st}>2),
                        r3 := range(<{st}>6),
                    select
                        multirange([r0, r1]) * multirange([r2, r3])
                        =
                        multirange([
                            range(<{st}>1, <{st}>2),
                            range(<{st}>6, <{st}>7),
                        ])
                ''',
                [True],
            )

            await self.assert_query_result(
                f'''
                    with
                        r0 := range(<{st}>1, <{st}>3),
                        r1 := range(<{st}>5, <{st}>7),
                        r2 := range(<{st}>2),
                    select
                        multirange([r0, r1]) * r2
                        =
                        multirange([
                            range(<{st}>2, <{st}>3),
                            range(<{st}>5, <{st}>7),
                        ])
                ''',
                [True],
            )

    async def test_edgeql_expr_range_46(self):
        # Test subtraction for numeric multiranges.
        for st in ['int32', 'int64', 'float32', 'float64', 'decimal']:
            await self.assert_query_result(
                f'''
                    with
                        r0 := range(<{st}>1, <{st}>3),
                        r1 := range(<{st}>5, <{st}>7),
                        r2 := range(<{st}>{{}}, <{st}>2),
                        r3 := range(<{st}>6),
                    select
                        multirange([r0, r1]) - multirange([r2, r3])
                        =
                        multirange([r0, r1, r2]) - r2 - r3
                ''',
                [True],
            )

    async def test_edgeql_expr_range_47(self):
        # Test addition for datetime multiranges.
        await self.assert_query_result(
            f'''
                with
                    r0 := range(<datetime>'2022-06-06T00:00:00Z',
                                <datetime>'2022-06-10T00:00:00Z'),
                    r1 := range(<datetime>'2022-06-12T00:00:00Z',
                                <datetime>'2022-06-17T00:00:00Z'),
                    r2 := range(<datetime>{{}},
                                <datetime>'2022-06-01T00:00:00Z'),
                    r3 := range(<datetime>'2022-06-10T00:00:00Z'),
                select
                    multirange([r0, r1]) + multirange([r2, r3])
                    =
                    multirange([r0, r1, r2]) + r3
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_datetime>'2022-06-06T00:00:00',
                                <cal::local_datetime>'2022-06-10T00:00:00'),
                    r1 := range(<cal::local_datetime>'2022-06-12T00:00:00',
                                <cal::local_datetime>'2022-06-17T00:00:00'),
                    r2 := range(<cal::local_datetime>{{}},
                                <cal::local_datetime>'2022-06-01T00:00:00'),
                    r3 := range(<cal::local_datetime>'2022-06-10T00:00:00'),
                select
                    multirange([r0, r1]) + multirange([r2, r3])
                    =
                    multirange([r0, r1, r2]) + r3
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_date>'2022-06-06',
                                <cal::local_date>'2022-06-10'),
                    r1 := range(<cal::local_date>'2022-06-12',
                                <cal::local_date>'2022-06-17'),
                    r2 := range(<cal::local_date>{{}},
                                <cal::local_date>'2022-06-01'),
                    r3 := range(<cal::local_date>'2022-06-10'),
                select
                    multirange([r0, r1]) + multirange([r2, r3])
                    =
                    multirange([r0, r1, r2]) + r3
            ''',
            [True],
        )

    async def test_edgeql_expr_range_48(self):
        # Test intersection for datetime multiranges.
        await self.assert_query_result(
            f'''
                with
                    r0 := range(<datetime>'2022-06-06T00:00:00Z',
                                <datetime>'2022-06-10T00:00:00Z'),
                    r1 := range(<datetime>'2022-06-12T00:00:00Z',
                                <datetime>'2022-06-17T00:00:00Z'),
                    r2 := range(<datetime>{{}},
                                <datetime>'2022-06-07T00:00:00Z'),
                    r3 := range(<datetime>'2022-06-10T00:00:00Z'),
                select
                    multirange([r0, r1]) * multirange([r2, r3])
                    =
                    multirange([
                        range(<datetime>'2022-06-06T00:00:00Z',
                              <datetime>'2022-06-07T00:00:00Z'),
                        range(<datetime>'2022-06-12T00:00:00Z',
                              <datetime>'2022-06-17T00:00:00Z'),
                    ])
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<datetime>'2022-06-06T00:00:00Z',
                                <datetime>'2022-06-10T00:00:00Z'),
                    r1 := range(<datetime>'2022-06-12T00:00:00Z',
                                <datetime>'2022-06-17T00:00:00Z'),
                    r2 := range(<datetime>{{}},
                                <datetime>'2022-06-15T00:00:00Z'),
                select
                    multirange([r0, r1]) * r2
                    =
                    multirange([
                        range(<datetime>'2022-06-06T00:00:00Z',
                              <datetime>'2022-06-10T00:00:00Z'),
                        range(<datetime>'2022-06-12T00:00:00Z',
                              <datetime>'2022-06-15T00:00:00Z'),
                    ])
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_datetime>'2022-06-06T00:00:00',
                                <cal::local_datetime>'2022-06-10T00:00:00'),
                    r1 := range(<cal::local_datetime>'2022-06-12T00:00:00',
                                <cal::local_datetime>'2022-06-17T00:00:00'),
                    r2 := range(<cal::local_datetime>{{}},
                                <cal::local_datetime>'2022-06-07T00:00:00'),
                    r3 := range(<cal::local_datetime>'2022-06-10T00:00:00'),
                select
                    multirange([r0, r1]) * multirange([r2, r3])
                    =
                    multirange([
                        range(<cal::local_datetime>'2022-06-06T00:00:00',
                              <cal::local_datetime>'2022-06-07T00:00:00'),
                        range(<cal::local_datetime>'2022-06-12T00:00:00',
                              <cal::local_datetime>'2022-06-17T00:00:00'),
                    ])
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_datetime>'2022-06-06T00:00:00',
                                <cal::local_datetime>'2022-06-10T00:00:00'),
                    r1 := range(<cal::local_datetime>'2022-06-12T00:00:00',
                                <cal::local_datetime>'2022-06-17T00:00:00'),
                    r2 := range(<cal::local_datetime>{{}},
                                <cal::local_datetime>'2022-06-15T00:00:00'),
                select
                    multirange([r0, r1]) * r2
                    =
                    multirange([
                        range(<cal::local_datetime>'2022-06-06T00:00:00',
                              <cal::local_datetime>'2022-06-10T00:00:00'),
                        range(<cal::local_datetime>'2022-06-12T00:00:00',
                              <cal::local_datetime>'2022-06-15T00:00:00'),
                    ])
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_date>'2022-06-06',
                                <cal::local_date>'2022-06-10'),
                    r1 := range(<cal::local_date>'2022-06-12',
                                <cal::local_date>'2022-06-17'),
                    r2 := range(<cal::local_date>{{}},
                                <cal::local_date>'2022-06-07'),
                    r3 := range(<cal::local_date>'2022-06-10'),
                select
                    multirange([r0, r1]) * multirange([r2, r3])
                    =
                    multirange([
                        range(<cal::local_date>'2022-06-06',
                              <cal::local_date>'2022-06-07'),
                        range(<cal::local_date>'2022-06-12',
                              <cal::local_date>'2022-06-17'),
                    ])
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_date>'2022-06-06',
                                <cal::local_date>'2022-06-10'),
                    r1 := range(<cal::local_date>'2022-06-12',
                                <cal::local_date>'2022-06-17'),
                    r2 := range(<cal::local_date>{{}},
                                <cal::local_date>'2022-06-15'),
                select
                    multirange([r0, r1]) * r2
                    =
                    multirange([
                        range(<cal::local_date>'2022-06-06',
                              <cal::local_date>'2022-06-10'),
                        range(<cal::local_date>'2022-06-12',
                              <cal::local_date>'2022-06-15'),
                    ])
            ''',
            [True],
        )

    async def test_edgeql_expr_range_49(self):
        # Test subtraction for datetime multiranges.
        await self.assert_query_result(
            f'''
                with
                    r0 := range(<datetime>'2022-06-06T00:00:00Z',
                                <datetime>'2022-06-10T00:00:00Z'),
                    r1 := range(<datetime>'2022-06-12T00:00:00Z',
                                <datetime>'2022-06-17T00:00:00Z'),
                    r2 := range(<datetime>{{}},
                                <datetime>'2022-06-08T00:00:00Z'),
                    r3 := range(<datetime>'2022-06-10T00:00:00Z'),
                select
                    multirange([r0, r1]) - multirange([r2, r3])
                    =
                    multirange([r0, r1]) - r2 - r3
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_datetime>'2022-06-06T00:00:00',
                                <cal::local_datetime>'2022-06-10T00:00:00'),
                    r1 := range(<cal::local_datetime>'2022-06-12T00:00:00',
                                <cal::local_datetime>'2022-06-17T00:00:00'),
                    r2 := range(<cal::local_datetime>{{}},
                                <cal::local_datetime>'2022-06-08T00:00:00'),
                    r3 := range(<cal::local_datetime>'2022-06-10T00:00:00'),
                select
                    multirange([r0, r1]) - multirange([r2, r3])
                    =
                    multirange([r0, r1]) - r2 - r3
            ''',
            [True],
        )

        await self.assert_query_result(
            f'''
                with
                    r0 := range(<cal::local_date>'2022-06-06',
                                <cal::local_date>'2022-06-10'),
                    r1 := range(<cal::local_date>'2022-06-12',
                                <cal::local_date>'2022-06-17'),
                    r2 := range(<cal::local_date>{{}},
                                <cal::local_date>'2022-06-08'),
                    r3 := range(<cal::local_date>'2022-06-10'),
                select
                    multirange([r0, r1]) - multirange([r2, r3])
                    =
                    multirange([r0, r1]) - r2 - r3
            ''',
            [True],
        )

    async def test_edgeql_expr_range_50(self):
        # Test range values bindings.
        await self.assert_query_result(
            '''select (
                range(<int64>{}, empty := true),
                range(1, 4),
                range(<int64>{}, 4),
                range(1),
            );''',
            [
                [
                    {
                        "empty": True,
                    },
                    {
                        "lower": 1,
                        "inc_lower": True,
                        "upper": 4,
                        "inc_upper": False,
                    },
                    {
                        "lower": None,
                        "inc_lower": False,
                        "upper": 4,
                        "inc_upper": False,
                    },
                    {
                        "lower": 1,
                        "inc_lower": True,
                        "upper": None,
                        "inc_upper": False,
                    },
                ]
            ]
        )

    async def test_edgeql_expr_range_51(self):
        # Test range values bindings.
        await self.assert_query_result(
            '''select (
                range(<float64>{}, empty := true),
                range(1.1, 4.2),
                range(<float64>{}, 4.2, inc_upper := true),
                range(1.1, inc_lower := false),
            );''',
            [
                [
                    {
                        "empty": True,
                    },
                    {
                        "lower": 1.1,
                        "inc_lower": True,
                        "upper": 4.2,
                        "inc_upper": False,
                    },
                    {
                        "lower": None,
                        "inc_lower": False,
                        "upper": 4.2,
                        "inc_upper": True,
                    },
                    {
                        "lower": 1.1,
                        "inc_lower": False,
                        "upper": None,
                        "inc_upper": False,
                    },
                ]
            ]
        )

    async def test_edgeql_expr_range_52(self):
        # Test range values bindings.
        await self.assert_query_result(
            '''select (
                range(<datetime>{}, empty := true),
                range(<cal::local_date>'2022-06-06',
                      <cal::local_date>'2022-06-10'),
                range(<cal::local_datetime>{},
                      <cal::local_datetime>'2022-06-08T00:00:00',
                      inc_upper := true),
                range(<datetime>'2022-06-10T00:00:00Z',
                      inc_lower := false),
            );''',
            [
                [
                    {"empty": True},
                    {
                        "lower": "2022-06-06",
                        "inc_lower": True,
                        "upper": "2022-06-10",
                        "inc_upper": False
                    },
                    {
                        "lower": None,
                        "inc_lower": False,
                        "upper": "2022-06-08T00:00:00",
                        "inc_upper": True
                    },
                    {
                        "lower": "2022-06-10T00:00:00+00:00",
                        "inc_lower": False,
                        "upper": None,
                        "inc_upper": False
                    }
                ]
            ]
        )

    async def test_edgeql_expr_range_53(self):
        # Test multirange values bindings.
        await self.assert_query_result(
            '''select multirange([
                range(<int64>{}, 0),
                range(2, 5),
                range(10),
            ]);''',
            [
                [
                    {
                        "lower": None,
                        "inc_lower": False,
                        "upper": 0,
                        "inc_upper": False
                    },
                    {
                        "lower": 2,
                        "inc_lower": True,
                        "upper": 5,
                        "inc_upper": False
                    },
                    {
                        "lower": 10,
                        "inc_lower": True,
                        "upper": None,
                        "inc_upper": False
                    }
                ]
            ]
        )

    async def test_edgeql_expr_range_54(self):
        # Test range values bindings.
        await self.assert_query_result(
            '''select multirange([
                range(<float64>{}, 0, inc_upper := true),
                range(2.1, 5),
                range(10.5, inc_lower := false),
            ]);''',
            [
                [
                    {
                        "lower": None,
                        "inc_lower": False,
                        "upper": 0,
                        "inc_upper": True
                    },
                    {
                        "lower": 2.1,
                        "inc_lower": True,
                        "upper": 5,
                        "inc_upper": False
                    },
                    {
                        "lower": 10.5,
                        "inc_lower": False,
                        "upper": None,
                        "inc_upper": False
                    }
                ]
            ]
        )

    async def test_edgeql_expr_range_55(self):
        # Test range values bindings.
        await self.assert_query_result(
            '''select multirange([
                range(<cal::local_date>{},
                      <cal::local_date>'2022-06-01'),
                range(<cal::local_date>'2022-06-02',
                      <cal::local_date>'2022-06-05'),
                range(<cal::local_date>'2022-06-10'),
            ]);''',
            [
                [
                    {
                        "lower": None,
                        "inc_lower": False,
                        "upper": "2022-06-01",
                        "inc_upper": False
                    },
                    {
                        "lower": "2022-06-02",
                        "inc_lower": True,
                        "upper": "2022-06-05",
                        "inc_upper": False
                    },
                    {
                        "lower": "2022-06-10",
                        "inc_lower": True,
                        "upper": None,
                        "inc_upper": False
                    }
                ]
            ]
        )

    async def test_edgeql_expr_cannot_assign_id_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot assign to property 'id'",
                _hint=None):
            await self.con.execute(r"""
                SELECT Text {
                    id := <uuid>'77841036-8e35-49ce-b509-2cafa0c25c4f'
                };
            """)

    async def test_edgeql_expr_if_else_01(self):
        await self.assert_query_result(
            r'''SELECT IF true THEN 'yes' ELSE 'no';''',
            ['yes'],
        )

        await self.assert_query_result(
            r'''SELECT IF false THEN 'yes' ELSE 'no';''',
            ['no'],
        )

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

    @tb.needs_factoring
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
                WITH x := {'b', 'a', 't'}
                SELECT
                    IF x = 'a' THEN 1 ELSE
                    IF x = 'b' THEN 10 ELSE
                    IF x = 'c' THEN 100 ELSE
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
        res = sorted([
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
        ])

        await self.assert_query_result(
            r"""
                # this creates a 3 x 3 x 3 cross product
                SELECT
                    1   IF {'c', 'a', 't'} = 'a' ELSE
                    10  IF {'c', 'a', 't'} = 'b' ELSE
                    100 IF {'c', 'a', 't'} = 'c' ELSE
                    0;
            """,
            res,
            sort=True
        )

        await self.assert_query_result(
            r"""
                # this creates a 3 x 3 x 3 cross product
                SELECT
                    IF {'c', 'a', 't'} = 'a' THEN 1 ELSE
                    IF {'c', 'a', 't'} = 'b' THEN 10 ELSE
                    IF {'c', 'a', 't'} = 'c' THEN 100 ELSE
                    0;
            """,
            res,
            sort=True
        )

        # Try nesting on in the THEN branch
        await self.assert_query_result(
            r"""
                # this creates a 3 x 3 x 3 cross product
                SELECT
                    IF {'c', 'a', 't'} != 'a' THEN
                      IF {'c', 'a', 't'} != 'b' THEN
                        IF {'c', 'a', 't'} != 'c' THEN
                          0
                        ELSE 100
                      ELSE 10
                    ELSE 1;
            """,
            res,
            sort=True
        )

    @tb.needs_factoring
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

        await self.assert_query_result(
            r"""
                with test := ['']
                select test if false else <array<str>>[];
            """,
            [[]],
        )

    async def test_edgeql_expr_if_else_10(self):
        await self.assert_query_result(
            r"""
                select if true then 10 else {}
            """,
            [10],
        )

        await self.assert_query_result(
            r"""
                select if false then 10 else {}
            """,
            [],
        )

        await self.assert_query_result(
            r"""
                select if true then [10] else []
            """,
            [[10]],
        )

        await self.assert_query_result(
            r"""
                select if false then [10] else []
            """,
            [[]],
        )

        await self.assert_query_result(
            r"""
                with test := ['']
                select test if false else [];
            """,
            [[]],
        )

    async def test_edgeql_expr_if_else_11(self):
        await self.assert_query_result(
            r"""
                select if 1 = <int64>$x then 2 else 3
            """,
            [2],
            variables=dict(x=1),
        )

        await self.assert_query_result(
            r"""
                select if 1 = <int64>$x then 2 else 3
            """,
            [3],
            variables=dict(x=-1),
        )

        await self.assert_query_result(
            r"""
                select 2 if 1 = <int64>$x else 3
            """,
            [2],
            variables=dict(x=1),
        )

        await self.assert_query_result(
            r"""
                select 2 if 1 = <int64>$x else 3
            """,
            [3],
            variables=dict(x=-1),
        )

    async def test_edgeql_expr_if_else_toplevel(self):
        await self.assert_query_result(
            r"""
                if true then 10 else 11
            """,
            [10],
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
                r"set constructor has arguments of incompatible "
                r"types 'std::float64' and 'std::decimal'"):
            await self.con.execute(r'''
                SELECT {1.0, <decimal>2.0};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"set constructor has arguments of incompatible "
                r"types 'std::float64' and 'std::decimal'"):
            await self.con.execute(r'''
                SELECT {{1.0, 2.0}, {1.0, <decimal>2.0}};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"set constructor has arguments of incompatible "
                r"types 'std::float64' and 'std::decimal'"):
            await self.con.execute(r'''
                SELECT {{1.0, <decimal>2.0}, {1.0, 2.0}};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"set constructor has arguments of incompatible "
                r"types 'std::decimal' and 'std::float64'"):
            await self.con.execute(r'''
                SELECT {1.0, 2.0, 5.0, <decimal>2.0, 3.0, 4.0};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"operator 'UNION' cannot be applied to operands of type "
                r"'std::int64' and 'std::str'"):
            await self.con.execute(r'''
                SELECT {1, 2, 3, 4 UNION 'a', 5, 6, 7};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidTypeError,
                r"operator 'UNION' cannot be applied to operands of type "
                r"'std::int64' and 'std::str'"):
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
                _position=35):

            await self.con.execute('''\
                SELECT Issue LIMIT LogEntry.spent_time;
            ''')

    async def test_edgeql_expr_cardinality_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed',
                _position=36):

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

    async def test_edgeql_expr_type_intersection_04(self):
        await self.con.execute('''\
            SELECT Named[IS Issue].id
                ?? <uuid>'00000000-0000-0000-0000-000000000000';
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
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
                "Unexpected ''1''"):

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
        await self.assert_query_result(
            "select <Named & Owned>{};",
            [],
        )

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
            {False}
        )
        await self.assert_query_result(
            'select {x := 1} is (typeof Issue.references | FreeObject);',
            {True}
        )

    async def test_edgeql_typeop_09(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 1e100n).name ++ "!" ++ <str>$test''',
            ['std::bigint!?'],
            variables={'test': '?'},
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

        await self.con.query("""
            select {
                xy := assert_single({<optional str>$0, <optional str>$1}) };
        """, None, None)
        await self.con.query("""
            select {
                xy := assert_single({<optional str>$0, <optional str>$1}) };
        """, None, 'test')

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

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "custom message",
        ):
            await self.con.query("""
                SELECT assert_single(
                    (SELECT User FILTER .name ILIKE "Hunter B%"),
                    message := "custom message",
                );
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

    async def test_edgeql_assert_message_crossproduct(self):
        # Nobody should do this, but specifying a set to the message
        # argument of an assert function should produce repeated
        # output.
        await self.assert_query_result(
            """
                select
                    assert_single(1, message := {"uh", "oh"}) +
                    assert_distinct(1, message := {"uh", "oh"}) +
                    assert_exists({1, 2}, message := {"uh", "oh"});
            """,
            tb.bag([3] * 8 + [4] * 8),
        )

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

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "custom message",
        ):
            await self.con.query("""
                SELECT assert_exists(
                    (SELECT User FILTER .name = "nonexistent"),
                    message := "custom message",
                ).name;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            await self.con.query("""
                with x := assert_exists(
                    (select {(1, 2), (3, 4)} filter false)),
                select x.0;
            """)

    async def test_edgeql_assert_exists_02(self):
        await self.con.execute('''
            insert BooleanTest { name := "" }
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            await self.con.query_json("""
                select BooleanTest { name, val := assert_exists(.val) }
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
                    (SELECT Issue FILTER "File 1" IN .references[IS File].name)
                    UNION
                    (SELECT Issue FILTER "File 2" IN .references[IS File].name)
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

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "custom message",
        ):
            await self.con.query("""
                SELECT assert_distinct({(), ()}, message := "custom message");
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

    async def test_edgeql_assert_00(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            # TODO: We don't have edgedb.QueryAssertionError in the
            # python bindings yet, so check the _code on the exception
            try:
                await self.con.query("""SELECT assert(false)""")
            except edgedb.InvalidValueError as e:
                self.assertEqual(e._code, errors.QueryAssertionError._code)
                raise

    async def test_edgeql_assert_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "custom message",
        ):
            await self.con.query("""
                SELECT assert(<bool>$0, message := "custom message")
            """, False)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            await self.con.query("""
                SELECT assert(<bool>$0)
            """, False)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            await self.con.query("""
                SELECT assert(<bool>$0, message := <optional str>$1)
            """, False, None)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "test",
        ):
            await self.con.query("""
                SELECT assert(<bool>$0, message := <optional str>$1)
            """, False, "test")

        await self.assert_query_result(
            r'''
                SELECT assert(<bool>$0)
            ''',
            [True],
            variables=(True,),
        )

        await self.assert_query_result(
            r'''
                SELECT assert(<optional bool>$0)
            ''',
            [],
            variables=(None,),
        )

    async def test_edgeql_assert_02(self):
        await self.con.execute("""
            INSERT File {
                name := "File 1",
            };
            INSERT File {
                name := "File 2",
            };
            INSERT File {
                name := "Asdf 3",
            };
            INSERT User {
                name := "User 1",
            };
            INSERT Status {
                name := "Open",
            };

            CREATE TYPE Dummy;
        """)

        # assert about object contents using order by
        await self.assert_query_result(
            r'''
                select File { name }
                filter .name like 'File%'
                order by .name
                then assert(not .name like '%3', message := "bogus " ++ .name);
            ''',
            [{"name": "File 1"}, {"name": "File 2"}],
        )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "bogus File 2",
        ):
            await self.con.query("""
                select File { name }
                filter .name like 'File%'
                order by .name
                then assert(not .name like '%2', message := "bogus " ++ .name);
            """)

        # Force an assert for non-empty query with a FOR loop
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            await self.con.query("""
                for _ in assert(count(File) = 2) union (
                    select User filter .name = 'User 1'
                )
            """)

        # Force an assert for an obviously empty query by wrapping it
        # in a shape
        shape_assert_q = """
            select { val := (select 1 filter <bool>$0) }
            filter assert(count(File) = <int64>$1);
        """

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            await self.con.query(shape_assert_q, True, 2)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            await self.con.query(shape_assert_q, False, 2)

        await self.assert_query_result(
            shape_assert_q,
            [{'val': 1}],
            variables=(True, 3),
        )
        await self.assert_query_result(
            shape_assert_q,
            [{'val': None}],
            variables=(False, 3),
        )

        # Force an assert for an obviously empty query using fake DML
        fake_dml_q = """
        with cond := assert(count(File) = <int64>$1),
             _  := (for _ in (select 0 filter not cond) union (insert Dummy)),
        select 1 filter <bool>$0
        """

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            await self.con.query(fake_dml_q, True, 2)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "assertion failed",
        ):
            await self.con.query(fake_dml_q, False, 2)

        await self.assert_query_result(
            fake_dml_q,
            [1],
            variables=(True, 3),
        )
        await self.assert_query_result(
            fake_dml_q,
            [],
            variables=(False, 3),
        )

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

    async def test_edgeql_str_concat(self):
        await self.assert_query_result(
            r"""
                SELECT 'aaaa' ++ 'bbbb';
            """,
            ['aaaabbbb'],
        )

        await self.assert_query_result(
            r"""
                SELECT 'aaaa' ++ r'\q' ++ $$\n$$;
            """,
            [R'aaaa\q\n'],
        )

    async def test_edgeql_overflow_error(self):
        body = 'x+' * 1600 + '0'

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "caused the compiler stack to overflow",
        ):
            await self.con.query(f'''
                with x := 1337, select {body}
            ''')

    async def test_edgeql_cast_to_function_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidReferenceError,
            "does not exist",
            _hint="did you mean to call 'to_str'?"
        ):
            await self.con.execute(f"""
                select <to_str>1;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidReferenceError,
            "does not exist",
            _hint="did you mean to call 'round'?"
        ):
            await self.con.execute(f"""
                select <round>1;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidReferenceError,
            "does not exist",
            _hint="did you mean to call 'std::cal::to_local_date'?"
        ):
            await self.con.execute(f"""
                select <cal::to_local_date>1;
            """)

    async def test_edgeql_expr_with_module_01(self):
        await self.con.execute(f"""
            create module dummy;
            create module A;
            create type A::Foo;
        """)

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH MODULE A '
        queries += [
            (NO_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dum::Foo>{}'),
        ]
        with_mod = 'WITH AAA as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <AAA::Foo>{}'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <s::Foo>{}'),
        ]
        with_mod = 'WITH std as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH A as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "Foo' does not exist",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_with_module_02(self):
        await self.con.execute(f"""
            create module dummy;
            create module A;
            create function A::abs(x: int64) -> int64 using (x);
        """)

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH MODULE A '
        queries += [
            (NO_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dum::abs(1)'),
        ]
        with_mod = 'WITH AAA as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
            (NO_ERR, with_mod + 'SELECT AAA::abs(1)'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
            (REF_ERR, with_mod + 'SELECT s::abs(1)'),
        ]
        with_mod = 'WITH std as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (NO_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH A as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (REF_ERR, with_mod + 'SELECT A::abs(1)'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "abs' does not exist",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_with_module_03(self):
        await self.con.execute(f"""
            create module dummy;
        """)

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dum::int64>{} = 1'),
        ]
        with_mod = 'WITH def as MODULE default '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <def::int64>{} = 1'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <s::int64>{} = 1'),
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "int64' does not exist",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_with_module_04(self):
        await self.con.execute(f"""
            create module dummy;
            create type default::int64;
        """)

        NO_ERR = 1
        REF_ERR = 2
        TYPE_ERR = 3

        queries = []

        with_mod = ''
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dum::int64>{} = 1'),
        ]
        with_mod = 'WITH def as MODULE default '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <def::int64>{} = 1'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <s::int64>{} = 1'),
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH std as MODULE default '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "int64' does not exist",
                ):
                    await self.con.execute(query)

            elif error == TYPE_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidTypeError,
                    "operator '=' cannot be applied",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_with_module_05(self):
        await self.con.execute(f"""
            create module dummy;
        """)

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module dummy '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module _test '
        queries += [
            (NO_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std::_test '
        queries += [
            (NO_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with t as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select t::abs(1)'),
        ]
        with_mod = 'with s as module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select s::abs(1)'),
        ]
        with_mod = 'with st as module std::_test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select st::abs(1)'),
        ]
        with_mod = 'with std as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (REF_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select std::abs(1)'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "abs' does not exist",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_with_module_06(self):
        await self.con.execute(f"""
            create module dummy;
            create module _test;
        """)

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module dummy '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std::_test '
        queries += [
            (NO_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with t as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select t::abs(1)'),
        ]
        with_mod = 'with s as module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select s::abs(1)'),
        ]
        with_mod = 'with st as module std::_test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select st::abs(1)'),
        ]
        with_mod = 'with std as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (REF_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select std::abs(1)'),
        ]
        with_mod = 'with std as module std::_test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (REF_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select std::abs(1)'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "abs' does not exist",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_with_module_07(self):
        await self.con.execute(f"""
            create module dummy;
            create module std::test;
            create scalar type std::test::Foo extending int64;
        """)

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module dummy '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module test '
        queries += [
            (NO_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module std '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module std::test '
        queries += [
            (NO_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with t as module test '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
            (NO_ERR, with_mod + 'select <t::Foo>1'),
        ]
        with_mod = 'with s as module std '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
            (REF_ERR, with_mod + 'select <s::Foo>1'),
        ]
        with_mod = 'with st as module std::test '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
            (NO_ERR, with_mod + 'select <st::Foo>1'),
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (REF_ERR, with_mod + 'select <std::test::Foo>1'),
            (REF_ERR, with_mod + 'select <std::Foo>1'),
        ]
        with_mod = 'WITH std as MODULE test '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (NO_ERR, with_mod + 'select <test::Foo>1'),
            (REF_ERR, with_mod + 'select <std::test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::Foo>1'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "Foo' does not exist",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_with_module_08(self):
        await self.con.execute(f"""
            create module dummy;
            create module std::test;
            create scalar type std::test::Foo extending int64;
            create module test;
        """)

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module dummy '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module test '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module std '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with module std::test '
        queries += [
            (NO_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
        ]
        with_mod = 'with t as module test '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
            (REF_ERR, with_mod + 'select <t::Foo>1'),
        ]
        with_mod = 'with s as module std '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
            (REF_ERR, with_mod + 'select <s::Foo>1'),
        ]
        with_mod = 'with st as module std::test '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (NO_ERR, with_mod + 'select <std::test::Foo>1'),
            (NO_ERR, with_mod + 'select <st::Foo>1'),
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (REF_ERR, with_mod + 'select <std::test::Foo>1'),
            (REF_ERR, with_mod + 'select <std::Foo>1'),
        ]
        with_mod = 'WITH std as MODULE test '
        queries += [
            (REF_ERR, with_mod + 'select <Foo>1'),
            (REF_ERR, with_mod + 'select <test::Foo>1'),
            (REF_ERR, with_mod + 'select <std::test::Foo>1'),
            (REF_ERR, with_mod + 'select <std::Foo>1'),
        ]

        for error, query in queries:
            if error == NO_ERR:
                await self.con.execute(query)

            elif error == REF_ERR:
                async with self.assertRaisesRegexTx(
                    edgedb.errors.InvalidReferenceError,
                    "Foo' does not exist",
                ):
                    await self.con.execute(query)

    async def test_edgeql_expr_str_interpolation_01(self):
        await self.assert_query_result(
            r'''
                select "1 + 1 = \(1 + 1)"
            ''',
            ['1 + 1 = 2'],
        )

        await self.assert_query_result(
            r'''
                select ("1 + 1 = \(1 + 1)")
            ''',
            ['1 + 1 = 2'],
        )

        # Have some more fun. Nest it a bit.
        await self.assert_query_result(
            r'''select "asdf \(str_reverse("1234") ++
"[\(sum({1,2,3}))]")! count(User)=\
\(
count(User))" ++ "!";''',
            ['asdf 4321[6]! count(User)=0!'],
        )
