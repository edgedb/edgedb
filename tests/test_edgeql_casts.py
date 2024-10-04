#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


import itertools
import os.path

import edgedb

from edb.testbase import server as tb


class TestEdgeQLCasts(tb.QueryTestCase):
    '''Testing symmetry and validity of casts.

    Scalar casting is symmetric in the sense that if casting scalar
    type X into Y is valid then it is also valid to cast Y into X.

    Some casts are lossless. A cast from X into Y is lossless if all
    the relevant details of the value of type X can be unambiguously
    represented by a value of type Y. Examples of lossless casts:
    - any scalar can be losslessly cast into a str
    - int16 and int32 can be losslessly cast into int64
    - int16 can be losslessly cast into float32
    - any numeric type can be losslessly cast into a decimal

    Sometimes only specific values (a subset of the entire domain of
    the scalar) can be cast losslessly:
    - 2147299968 can be cast losslessly into a float32, but not 2147299969
    - decimal 2.5 can be cast losslessly into a float32, but not decimal
      2.5000000001

    Consider two types X and Y with corresponding values x and y.
    If x can be losslessly cast into Y, then casting it back is also lossless:
        x = <X><Y>x
    '''
    # FIXME: a special schema should be used here since we need to
    # cover all known scalars and even some arrays and tuples.
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'casts.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'casts_setup.edgeql')

    # NOTE: nothing can be cast into bytes
    async def test_edgeql_casts_bytes_01(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>True;
            """)

    async def test_edgeql_casts_bytes_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>uuid_generate_v1mc();
            """)

    async def test_edgeql_casts_bytes_03(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>'Hello';
            """)

    async def test_edgeql_casts_bytes_04(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r'expected JSON string or null',
        ):
            await self.con.query_single("SELECT <bytes>to_json('1');")

        self.assertEqual(
            await self.con.query_single(r'''
                SELECT <bytes>to_json('"aGVsbG8="');
            '''),
            b'hello',
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError, r'invalid symbol'):
            await self.con.query_single("""
                SELECT <bytes>to_json('"not base64!"');
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError, r'invalid base64 end sequence'):
            await self.con.query_single("""
                SELECT <bytes>to_json('"a"');
            """)

    async def test_edgeql_casts_bytes_05(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>datetime_current();
            """)

    async def test_edgeql_casts_bytes_06(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT
                  <bytes>cal::to_local_datetime('2018-05-07T20:01:22.306916');
            """)

    async def test_edgeql_casts_bytes_07(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>cal::to_local_date('2018-05-07');
            """)

    async def test_edgeql_casts_bytes_08(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>cal::to_local_time('20:01:22.306916');
            """)

    async def test_edgeql_casts_bytes_09(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_duration(hours:=20);
            """)

    async def test_edgeql_casts_bytes_10(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_int16('2');
            """)

    async def test_edgeql_casts_bytes_11(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_int32('2');
            """)

    async def test_edgeql_casts_bytes_12(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_int64('2');
            """)

    async def test_edgeql_casts_bytes_13(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_float32('2');
            """)

    async def test_edgeql_casts_bytes_14(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_float64('2');
            """)

    async def test_edgeql_casts_bytes_15(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_decimal('2');
            """)

    async def test_edgeql_casts_bytes_16(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast'):
            await self.con.execute("""
                SELECT <bytes>to_bigint('2');
            """)

    # NOTE: casts are idempotent

    async def test_edgeql_casts_idempotence_01(self):
        await self.assert_query_result(
            r'''SELECT <bool><bool>True IS bool;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <bytes><bytes>b'Hello' IS bytes;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <str><str>'Hello' IS str;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <json><json>to_json('1') IS json;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <uuid><uuid>uuid_generate_v1mc() IS uuid;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <datetime><datetime>datetime_current() IS datetime;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_datetime><cal::local_datetime>
                    cal::to_local_datetime(
                    '2018-05-07T20:01:22.306916') IS cal::local_datetime;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_date><cal::local_date>cal::to_local_date(
                    '2018-05-07') IS cal::local_date;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_time><cal::local_time>cal::to_local_time(
                    '20:01:22.306916') IS cal::local_time;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <duration><duration>to_duration(
                    hours:=20) IS duration;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int16><int16>to_int16('12345') IS int16;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int32><int32>to_int32('1234567890') IS int32;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int64><int64>to_int64('1234567890123') IS int64;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float32><float32>to_float32('2.5') IS float32;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float64><float64>to_float64('2.5') IS float64;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <bigint><bigint>to_bigint(
                    '123456789123456789123456789')
                IS bigint;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <decimal><decimal>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                IS decimal;
            ''',
            [True],
        )

    async def test_edgeql_casts_idempotence_02(self):
        await self.assert_query_result(
            r'''SELECT <bool><bool>True = True;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <bytes><bytes>b'Hello' = b'Hello';''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <str><str>'Hello' = 'Hello';''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <json><json>to_json('1') = to_json('1');''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH U := uuid_generate_v4()
                SELECT <uuid><uuid>U = U;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <datetime><datetime>datetime_of_statement() =
                    datetime_of_statement();
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_datetime><cal::local_datetime>
                    cal::to_local_datetime('2018-05-07T20:01:22.306916') =
                    cal::to_local_datetime('2018-05-07T20:01:22.306916');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_date><cal::local_date>
                    cal::to_local_date('2018-05-07') =
                    cal::to_local_date('2018-05-07');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_time><cal::local_time>cal::to_local_time(
                    '20:01:22.306916') = cal::to_local_time('20:01:22.306916');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <duration><duration>to_duration(hours:=20) =
                    to_duration(hours:=20);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int16><int16>to_int16('12345') = 12345;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int32><int32>to_int32('1234567890') = 1234567890;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <int64><int64>to_int64('1234567890123') =
                    1234567890123;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float32><float32>to_float32('2.5') = 2.5;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float64><float64>to_float64('2.5') = 2.5;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <bigint><bigint>to_bigint(
                    '123456789123456789123456789')
                = to_bigint(
                    '123456789123456789123456789');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <decimal><decimal>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                = to_decimal(
                    '123456789123456789123456789.123456789123456789123456789');
            ''',
            [True],
        )

    async def test_edgeql_casts_str_01(self):
        # Casting to str and back is lossless for every scalar (if
        # legal). It's still not legal to cast bytes into str or some
        # of the json values.
        await self.assert_query_result(
            r'''SELECT <bool><str>True = True;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <bool><str>False = False;''',
            [True],
            # only JSON strings can be cast into EdgeQL str
        )

        await self.assert_query_result(
            r'''SELECT <json><str>to_json('"Hello"') = to_json('"Hello"');''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH U := uuid_generate_v1mc()
                SELECT <uuid><str>U = U;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <datetime><str>datetime_of_statement() =
                    datetime_of_statement();
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_datetime><str>cal::to_local_datetime(
                        '2018-05-07T20:01:22.306916') =
                    cal::to_local_datetime('2018-05-07T20:01:22.306916');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_date><str>cal::to_local_date('2018-05-07') =
                    cal::to_local_date('2018-05-07');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_time><str>
                    cal::to_local_time('20:01:22.306916') =
                    cal::to_local_time('20:01:22.306916');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <duration><str>to_duration(hours:=20) =
                    to_duration(hours:=20);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int16><str>to_int16('12345') = 12345;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int32><str>to_int32('1234567890') = 1234567890;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <int64><str>to_int64(
                    '1234567890123') = 1234567890123;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float32><str>to_float32('2.5') = 2.5;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float64><str>to_float64('2.5') = 2.5;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <bigint><str>to_bigint(
                    '123456789123456789123456789')
                = to_bigint(
                    '123456789123456789123456789');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <decimal><str>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                = to_decimal(
                    '123456789123456789123456789.123456789123456789123456789');
            ''',
            [True],
        )

    async def test_edgeql_casts_str_02(self):
        # Certain strings can be cast into other types losslessly,
        # making them "canonical" string representations of those
        # values.
        await self.assert_query_result(
            r'''
                WITH x := {'true', 'false'}
                SELECT <str><bool>x = x;
            ''',
            [True, True],
        )

        await self.assert_query_result(
            r'''
                WITH x := {'True', 'False', 'TRUE', 'FALSE', '  TrUe   '}
                SELECT <str><bool>x = x;
            ''',
            [False, False, False, False, False],
        )

        await self.assert_query_result(
            r'''
                WITH x := {'True', 'False', 'TRUE', 'FALSE', 'TrUe'}
                SELECT <str><bool>x = str_lower(x);
            ''',
            [True, True, True, True, True],
        )

        for variant in {'😈', 'yes', '1', 'no', 'on', 'OFF',
                        't', 'f', 'tr', 'fa'}:
            async with self.assertRaisesRegexTx(
                    edgedb.InvalidValueError,
                    fr"invalid input syntax for type std::bool: '{variant}'"):
                await self.con.query_single(f'SELECT <bool>"{variant}"')

        self.assertTrue(
            await self.con.query_single('SELECT <bool>"    TruE   "'))
        self.assertFalse(
            await self.con.query_single('SELECT <bool>"    FalsE   "'))

    async def test_edgeql_casts_str_03(self):
        # str to json is always lossless
        await self.assert_query_result(
            r'''
                WITH x := {'any', 'arbitrary', '♠gibberish♠'}
                SELECT <str><json>x = x;
            ''',
            [True, True, True],
        )

    async def test_edgeql_casts_str_04(self):
        # canonical uuid representation as a string is using lowercase
        await self.assert_query_result(
            r'''
                WITH x := 'd4288330-eea3-11e8-bc5f-7faf132b1d84'
                SELECT <str><uuid>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # non-canonical
            r'''
                WITH x := {
                    'D4288330-EEA3-11E8-BC5F-7FAF132B1D84',
                    'D4288330-Eea3-11E8-Bc5F-7Faf132B1D84',
                    'D4288330-eea3-11e8-bc5f-7faf132b1d84',
                }
                SELECT <str><uuid>x = x;
            ''',
            [False, False, False],
        )

        await self.assert_query_result(
            r'''
                WITH x := {
                    'D4288330-EEA3-11E8-BC5F-7FAF132B1D84',
                    'D4288330-Eea3-11E8-Bc5F-7Faf132B1D84',
                    'D4288330-eea3-11e8-bc5f-7faf132b1d84',
                }
                SELECT <str><uuid>x = str_lower(x);
            ''',
            [True, True, True],
        )

    async def test_edgeql_casts_str_05(self):
        # Canonical date and time str representations must follow ISO
        # 8601. This test assumes that the server is configured to be
        # in UTC time zone.
        await self.assert_query_result(
            r'''
                WITH x := '2018-05-07T20:01:22.306916+00:00'
                SELECT <str><datetime>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same datetime
            r'''
                WITH x := {
                    '2018-05-07T15:01:22.306916-05:00',
                    '2018-05-07T15:01:22.306916-05',
                    '2018-05-07T20:01:22.306916Z',
                    '2018-05-07T20:01:22.306916+0000',
                    '2018-05-07T20:01:22.306916+00',
                    # the '-' and ':' separators may be omitted
                    '20180507T200122.306916+00',
                    # acceptable RFC 3339
                    '2018-05-07 20:01:22.306916+00:00',
                    '2018-05-07t20:01:22.306916z',
                }
                SELECT <datetime>x =
                    <datetime>'2018-05-07T20:01:22.306916+00:00';
            ''',
            [True, True, True, True, True, True, True, True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime>"2018-05-07;20:01:22.306916+00:00"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime>"2018-05-07T20:01:22.306916"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime>"2018-05-07T20:01:22.306916 1000"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime>"2018-05-07T20:01:22.306916 US/Central"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime>"2018-05-07T20:01:22.306916 +GMT1"')

    async def test_edgeql_casts_str_06(self):
        # Canonical date and time str representations must follow ISO
        # 8601. This test assumes that the server is configured to be
        # in UTC time zone.
        await self.assert_query_result(
            r'''
                WITH x := '2018-05-07T20:01:22.306916'
                SELECT <str><cal::local_datetime>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same datetime
            r'''
                WITH x := {
                    # the '-' and ':' separators may be omitted
                    '20180507T200122.306916',
                    # acceptable RFC 3339
                    '2018-05-07 20:01:22.306916',
                    '2018-05-07t20:01:22.306916',
                }
                SELECT <cal::local_datetime>x =
                    <cal::local_datetime>'2018-05-07T20:01:22.306916';
            ''',
            [True, True, True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_datetime>"2018-05-07;20:01:22.306916"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''
                    SELECT
                        <cal::local_datetime>"2018-05-07T20:01:22.306916+01:00"
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_datetime>"2018-05-07T20:01:22.306916 GMT"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''
                    SELECT
                      <cal::local_datetime>"2018-05-07T20:01:22.306916 GMT0"
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''SELECT <cal::local_datetime>
                    "2018-05-07T20:01:22.306916 US/Central"
                ''')

    async def test_edgeql_casts_str_07(self):
        # Canonical date and time str representations must follow ISO
        # 8601.
        await self.assert_query_result(
            r'''
                WITH x := '2018-05-07'
                SELECT <str><cal::local_date>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same date
            r'''
                WITH x := {
                    # the '-' separators may be omitted
                    '20180507',
                }
                SELECT <cal::local_date>x = <cal::local_date>'2018-05-07';
            ''',
            [True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date>"2018-05-07T20:01:22.306916"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date>"2018/05/07"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date>"2018.05.07"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date>"2018-05-07+01:00"')

    async def test_edgeql_casts_str_08(self):
        # Canonical date and time str representations must follow ISO
        # 8601.
        await self.assert_query_result(
            r'''
                WITH x := '20:01:22.306916'
                SELECT <str><cal::local_time>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH x := {
                    '20:01',
                    '20:01:00',
                    # the ':' separators may be omitted
                    '2001',
                    '200100',
                }
                SELECT <cal::local_time>x = <cal::local_time>'20:01:00';
            ''',
            [True, True, True, True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                'invalid input syntax for type std::cal::local_time'):
            await self.con.query_single(
                "SELECT <cal::local_time>'2018-05-07 20:01:22'")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_time>"20:01:22.306916+01:00"')

    async def test_edgeql_casts_str_09(self):
        # Canonical duration
        await self.assert_query_result(
            r'''
                WITH x := 'PT20H1M22.306916S'
                SELECT <str><duration>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # non-canonical
            r'''
                WITH x := {
                    '20:01:22.306916',
                    '20h 1m 22.306916s',
                    '20 hours 1 minute 22.306916 seconds',
                    '72082.306916',  # the duration in seconds
                    '0.834285959675926 days',
                }
                SELECT <str><duration>x = x;
            ''',
            [False, False, False, False, False],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same duration
            r'''
                WITH x := {
                    '20:01:22.306916',
                    '20h 1m 22.306916s',
                    '20 hours 1 minute 22.306916 seconds',
                    '72082.306916',  # the duration in seconds
                    '0.834285959675926 days',
                }
                SELECT <duration>x = <duration>'PT20H1M22.306916S';
            ''',
            [True, True, True, True, True],
        )

    async def test_edgeql_casts_str_10(self):
        # valid casts from str to any integer is lossless, as long as
        # there's no whitespace, which is trimmed
        await self.assert_query_result(
            r'''
                WITH x := {'-20', '0', '7', '12345'}
                SELECT <str><int16>x = x;
            ''',
            [True, True, True, True],
        )

        await self.assert_query_result(
            r'''
                WITH x := {'-20', '0', '7', '12345'}
                SELECT <str><int32>x = x;
            ''',
            [True, True, True, True],
        )

        await self.assert_query_result(
            r'''
                WITH x := {'-20', '0', '7', '12345'}
                SELECT <str><int64>x = x;
            ''',
            [True, True, True, True],
        )

        await self.assert_query_result(
            # with whitespace
            r'''
                WITH x := {
                    '       42',
                    '42     ',
                    '       42      ',
                }
                SELECT <str><int16>x = x;
            ''',
            [False, False, False],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same value
            r'''
                WITH x := {
                    '       42',
                    '42     ',
                    '       42      ',
                }
                SELECT <int16>x = 42;
            ''',
            [True, True, True],
        )

    async def test_edgeql_casts_str_11(self):
        # There's too many ways of representing floats. Outside of
        # trivial 1-2 digit cases, relying on any str being
        # "canonical" is not safe, making most casts from str to float
        # lossy.
        await self.assert_query_result(
            r'''
                WITH x := {'-20', '0', '7.2'}
                SELECT <str><float32>x = x;
            ''',
            [True, True, True],
        )

        await self.assert_query_result(
            r'''
                WITH x := {'-20', '0', '7.2'}
                SELECT <str><float64>x = x;
            ''',
            [True, True, True],
        )

        await self.assert_query_result(
            # non-canonical
            r'''
                WITH x := {
                    '0.0000000001234',
                    '1234E-13',
                    '0.1234e-9',
                }
                SELECT <str><float32>x = x;
            ''',
            [False, False, False],
        )

        await self.assert_query_result(
            r'''
                WITH x := {
                    '0.0000000001234',
                    '1234E-13',
                    '0.1234e-9',
                }
                SELECT <str><float64>x = x;
            ''',
            [False, False, False],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same value
            r'''
                WITH x := {
                    '0.0000000001234',
                    '1234E-13',
                    '0.1234e-9',
                }
                SELECT <float64>x = 1234e-13;
            ''',
            [True, True, True],
        )

    async def test_edgeql_casts_str_12(self):
        # The canonical string representation of decimals is without
        # use of scientific notation.
        await self.assert_query_result(
            r'''
                WITH x := {
                    '-20', '0', '7.2', '0.0000000001234', '1234.00000001234'
                }
                SELECT <str><decimal>x = x;
            ''',
            [True, True, True, True, True],
        )

        await self.assert_query_result(
            # non-canonical
            r'''
                WITH x := {
                    '1234E-13',
                    '0.1234e-9',
                }
                SELECT <str><decimal>x = x;
            ''',
            [False, False],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same date
            r'''
                WITH x := {
                    '1234E-13',
                    '0.1234e-9',
                }
                SELECT <decimal>x = <decimal>'0.0000000001234';
            ''',
            [True, True],
        )

    async def test_edgeql_casts_str_13(self):
        # Casting to str and back is lossless for every scalar (if
        # legal). It's still not legal to cast bytes into str or some
        # of the json values.
        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <uuid><str>T.id = T.id;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <bool><str>T.p_bool = T.p_bool;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <str><str>T.p_str = T.p_str;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <datetime><str>T.p_datetime = T.p_datetime;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <cal::local_datetime><str>T.p_local_datetime =
                    T.p_local_datetime;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <cal::local_date><str>T.p_local_date = T.p_local_date;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <cal::local_time><str>T.p_local_time = T.p_local_time;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <duration><str>T.p_duration = T.p_duration;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <int16><str>T.p_int16 = T.p_int16;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <int32><str>T.p_int32 = T.p_int32;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <int64><str>T.p_int64 = T.p_int64;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <float32><str>T.p_float32 = T.p_float32;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <float64><str>T.p_float64 = T.p_float64;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <bigint><str>T.p_bigint = T.p_bigint;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <decimal><str>T.p_decimal = T.p_decimal;
            ''',
            [True],
        )

    async def test_edgeql_casts_numeric_01(self):
        # Casting to decimal and back should be lossless for any other
        # integer type.
        for numtype in {'bigint', 'decimal'}:
            await self.assert_query_result(
                # technically we're already casting a literal int64
                # to int16 first
                f'''
                    WITH x := <int16>{{-32768, -32767, -100,
                                      0, 13, 32766, 32767}}
                    SELECT <int16><{numtype}>x = x;
                ''',
                [True, True, True, True, True, True, True],
            )

            await self.assert_query_result(
                # technically we're already casting a literal int64
                # to int32 first
                f'''
                    WITH x := <int32>{{-2147483648, -2147483647, -65536, -100,
                                      0, 13, 32768, 2147483646, 2147483647}}
                    SELECT <int32><{numtype}>x = x;
                ''',
                [True, True, True, True, True, True, True, True, True],
            )

            await self.assert_query_result(
                f'''
                    WITH x := <int64>{{
                        -9223372036854775808,
                        -9223372036854775807,
                        -4294967296,
                        -65536,
                        -100,
                        0,
                        13,
                        65536,
                        4294967296,
                        9223372036854775806,
                        9223372036854775807
                    }}
                    SELECT <int64><{numtype}>x = x;
                ''',
                [True, True, True, True, True, True,
                 True, True, True, True, True],
            )

    async def test_edgeql_casts_numeric_02(self):
        # Casting to decimal and back should be lossless for any other
        # float type of low precision (a couple of digits less than
        # the maximum possible float precision).
        await self.assert_query_result(
            # technically we're already casting a literal int64 or
            # float64 to float32 first
            r'''
                WITH x := <float32>{-3.31234e+38, -1.234e+12, -1.234e-12,
                                    -100, 0, 13, 1.234e-12, 1.234e+12, 3.4e+38}
                SELECT <float32><decimal>x = x;
            ''',
            [True, True, True, True, True, True, True, True, True],
        )

        await self.assert_query_result(
            r'''
                WITH x := <float64>{-1.61234e+308, -1.234e+42, -1.234e-42,
                                    -100, 0, 13, 1.234e-42, 1.234e+42,
                                    1.7e+308}
                SELECT <float64><decimal>x = x;
            ''',
            [True, True, True, True, True, True, True, True, True],
        )

    async def test_edgeql_casts_numeric_03(self):
        # It is especially dangerous to cast an int32 into float32 and
        # back because float32 cannot losslessly represent the entire
        # range of int32, but it can represent some of it, so no
        # obvious errors would be raised (as any int32 value is
        # technically withing valid range of float32), but the value
        # could be mangled.
        await self.assert_query_result(
            # ints <= 2^24 can be represented exactly in a float32
            r'''
            WITH x := <int32>{16777216, 16777215, 16777214,
                              1677721, 167772, 16777}
            SELECT <int32><float32>x = x;
            ''',
            [True, True, True, True, True, True],
        )

        await self.assert_query_result(
            # max int32 -100, -1000
            r'''
            WITH x := <int32>{2147483548, 2147482648}
            SELECT <int32><float32>x = x;
            ''',
            [False, False],
        )

        await self.assert_query_result(
            r'''
            WITH x := <int32>{2147483548, 2147482648}
            SELECT <int32><float32>x;
            ''',
            [2147483520, 2147482624],
        )

    async def test_edgeql_casts_numeric_04(self):
        await self.assert_query_result(
            # ints <= 2^24 can be represented exactly in a float32
            r'''
                WITH x := <int32>{16777216, 16777215, 16777214,
                                  1677721, 167772, 16777}
                SELECT <int32><float64>x = x;
            ''',
            [True, True, True, True, True, True],
        )

        await self.assert_query_result(
            # max int32 -1, -2, -3, -10, -100, -1000
            r'''
            WITH x := <int32>{2147483647, 2147483646, 2147483645,
                              2147483638, 2147483548, 2147482648}
            SELECT <int32><float64>x = x;
            ''',
            [True, True, True, True, True, True],
        )

    async def test_edgeql_casts_numeric_05(self):
        # Due to the sparseness of float values large integers may not
        # be representable exactly if they require better precision
        # than float provides.
        await self.assert_query_result(
            r'''
                # 2^31 -1, -2, -3, -10
                WITH x := <int32>{2147483647, 2147483646, 2147483645,
                                  2147483638}
                # 2147483647 is the max int32
                SELECT x <= <int32>2147483647;
            ''',
            [True, True, True, True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.NumericOutOfRangeError, r"std::int32 out of range"):
            async with self.con.transaction():
                await self.con.execute("""
                    SELECT <int32><float32><int32>2147483647;
                """)

        async with self.assertRaisesRegexTx(
                edgedb.NumericOutOfRangeError, r"std::int32 out of range"):
            async with self.con.transaction():
                await self.con.execute("""
                    SELECT <int32><float32><int32>2147483646;
                """)

        async with self.assertRaisesRegexTx(
                edgedb.NumericOutOfRangeError, r"std::int32 out of range"):
            async with self.con.transaction():
                await self.con.execute("""
                    SELECT <int32><float32><int32>2147483645;
                """)

        async with self.assertRaisesRegexTx(
                edgedb.NumericOutOfRangeError, r"std::int32 out of range"):
            async with self.con.transaction():
                await self.con.execute("""
                    SELECT <int32><float32><int32>2147483638;
                """)

    async def test_edgeql_casts_numeric_06(self):
        await self.assert_query_result(
            r'''SELECT <int16>1;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT <int32>1;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT <int64>1;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT <float32>1;''',
            [1.0],
        )

        await self.assert_query_result(
            r'''SELECT <float64>1;''',
            [1.0],
        )

        await self.assert_query_result(
            r'''SELECT <bigint>1;''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT <decimal>1;''',
            [1],
        )

    async def test_edgeql_casts_numeric_07(self):
        numerics = ['int16', 'int32', 'int64', 'float32', 'float64', 'bigint',
                    'decimal']

        for t1, t2 in itertools.product(numerics, numerics):
            await self.assert_query_result(
                f'''
                    SELECT <{t1}><{t2}>1;
                ''',
                [1],
            )

    async def test_edgeql_casts_numeric_08(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type std::bigint'):
            await self.con.query_single(
                'SELECT <bigint>"100000n"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type std::decimal'):
            await self.con.query_single(
                'SELECT <decimal>"12313.132n"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"invalid input syntax for type std::bigint: 'bigint'"):
            await self.con.query_single(
                'SELECT <bigint>"bigint"')

    async def test_edgeql_casts_collections_01(self):
        await self.assert_query_result(
            r'''SELECT <array<str>>[1, 2, 3];''',
            [['1', '2', '3']],
        )

        await self.assert_query_result(
            r'''WITH X := [1, 2, 3] SELECT <array<str>> X;''',
            [['1', '2', '3']],
        )

        await self.assert_query_result(
            r'''SELECT <tuple<str, float64>> (1, '2');''',
            [['1', 2.0]],
        )

        await self.assert_query_result(
            r'''WITH X := (1, '2') SELECT <tuple<str, float64>> X;''',
            [['1', 2.0]],
        )

        await self.assert_query_result(
            r'''SELECT <array<tuple<str, float64>>> [(1, '2')];''',
            [[['1', 2.0]]],
        )

        await self.assert_query_result(
            r'''WITH X := [(1, '2')]
                SELECT <array<tuple<str, float64>>> X;''',
            [[['1', 2.0]]],
        )

        await self.assert_query_result(
            r'''SELECT <tuple<array<float64>>> (['1'],);''',
            [[[1.0]]],
        )

    async def test_edgeql_casts_collections_02(self):
        await self.assert_query_result(
            R'''
                WITH
                    std AS MODULE math,
                    foo := (SELECT [1, 2, 3])
                SELECT <array<str>>foo;
            ''',
            [['1', '2', '3']],
        )

        await self.assert_query_result(
            R'''
                WITH
                    std AS MODULE math,
                    foo := (SELECT [<int32>1, <int32>2, <int32>3])
                SELECT <array<str>>foo;
            ''',
            [['1', '2', '3']],
        )

        await self.assert_query_result(
            R'''
                WITH
                    std AS MODULE math,
                    foo := (SELECT [(1,), (2,), (3,)])
                SELECT <array<tuple<str>>>foo;
            ''',
            [[['1'], ['2'], ['3']]],
        )

    # check that casting to collections produces the correct error messages
    async def test_edgeql_casts_collection_errors_01(self):
        # scalar to array
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"cannot cast 'std::int64' to 'array<std::int64>'"):
            await self.con.execute("""
                SELECT <array<int64>>1;
            """)

    async def test_edgeql_casts_collection_errors_02(self):
        # tuple to array
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"cannot cast 'tuple<std::int64>' to 'array<std::int64>'"):
            await self.con.execute("""
                SELECT <array<int64>>(1,);
            """)

    async def test_edgeql_casts_collection_errors_03(self):
        # object to array
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"cannot cast 'std::FreeObject' to 'array<std::int64>'"):
            await self.con.execute("""
                SELECT <array<int64>>{a := 1};
            """)

    async def test_edgeql_casts_collection_errors_04(self):
        # array to array, mismatched element types
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"while casting 'array<tuple<std::int64>>' "
                r"to 'array<std::int64>', "
                r"in array elements, "
                r"cannot cast 'tuple<std::int64>' to 'std::int64'"):
            await self.con.execute("""
                SELECT <array<int64>>[(1,)];
            """)

    async def test_edgeql_casts_collection_errors_05(self):
        # scalar to tuple
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"cannot cast 'std::int64' to 'tuple<std::int64>'"):
            await self.con.execute("""
                SELECT <tuple<int64>>1;
            """)

    async def test_edgeql_casts_collection_errors_06(self):
        # array to tuple
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"cannot cast 'array<std::int64>' to 'tuple<std::int64>'"):
            await self.con.execute("""
                SELECT <tuple<int64>>[1];
            """)

    async def test_edgeql_casts_collection_errors_07(self):
        # object to array
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"cannot cast 'std::FreeObject' to 'tuple<std::int64>'"):
            await self.con.execute("""
                SELECT <tuple<int64>>{a := 1};
            """)

    async def test_edgeql_casts_collection_errors_08(self):
        # tuple to tuple, mismatched element types
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"while casting 'tuple<array<std::int64>>' "
                r"to 'tuple<std::int64>', "
                r"at tuple element '0', "
                r"cannot cast 'array<std::int64>' to 'std::int64'"):
            await self.con.execute("""
                SELECT <tuple<int64>>([1],);
            """)

    async def test_edgeql_casts_collection_errors_09(self):
        # named tuple to named tuple, use new element name
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"while casting 'tuple<b: array<std::int64>>' "
                r"to 'tuple<a: std::int64>', "
                r"at tuple element 'a', "
                r"cannot cast 'array<std::int64>' to 'std::int64'"):
            await self.con.execute("""
                SELECT <tuple<a: int64>>(b := [1]);
            """)

    async def test_edgeql_casts_collection_errors_10(self):
        # nested tuple to nested tuple
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"while casting 'tuple<tuple<array<std::int64>>>' "
                r"to 'tuple<a: tuple<b: std::int64>>', "
                r"at tuple element 'a', "
                r"at tuple element 'b', "
                r"cannot cast 'array<std::int64>' to 'std::int64'"):
            await self.con.execute("""
                SELECT <tuple<a: tuple<b: int64>>>(([1],),);
            """)

    async def test_edgeql_casts_collection_errors_11(self):
        # nested array to nested array
        # note: arrays can't be directly nested
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"while casting 'array<tuple<array<tuple<std::int64>>>>' "
                r"to 'array<tuple<array<std::int64>>>', "
                r"in array elements, "
                r"at tuple element '0', "
                r"in array elements, "
                r"cannot cast 'tuple<std::int64>' to 'std::int64'"):
            await self.con.execute("""
                SELECT <array<tuple<array<int64>>>>[([(1,)],)];
            """)

    async def test_edgeql_casts_collection_errors_12(self):
        # tuple with multiple elements, error in later element
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"while casting 'tuple<std::int64, std::int64, std::int64>' "
                r"to 'tuple<std::int64, std::int64, array<std::int64>>', "
                r"at tuple element '2', "
                r"cannot cast 'std::int64' to 'array<std::int64>"):
            await self.con.execute("""
                SELECT <tuple<int64, int64, array<int64>>>(1, 2, 3);
            """)

    # casting into an abstract scalar should be illegal
    async def test_edgeql_casts_illegal_01(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*'anytype'"):
            await self.con.execute("""
                SELECT <anytype>123;
            """)

    async def test_edgeql_casts_illegal_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anyscalar'"):
            await self.con.execute("""
                SELECT <anyscalar>123;
            """)

    async def test_edgeql_casts_illegal_03(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anyreal'"):
            await self.con.execute("""
                SELECT <anyreal>123;
            """)

    async def test_edgeql_casts_illegal_04(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anyint'"):
            await self.con.execute("""
                SELECT <anyint>123;
            """)

    async def test_edgeql_casts_illegal_05(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot cast.*'):
            await self.con.execute("""
                SELECT <anyfloat>123;
            """)

    async def test_edgeql_casts_illegal_06(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*sequence'"):
            await self.con.execute("""
                SELECT <sequence>123;
            """)

    async def test_edgeql_casts_illegal_07(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anytype"):
            await self.con.execute("""
                SELECT <array<anytype>>[123];
            """)

    async def test_edgeql_casts_illegal_08(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anytype"):
            await self.con.execute("""
                SELECT <tuple<int64, anytype>>(123, 123);
            """)

    async def test_edgeql_casts_illegal_09(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"cannot cast.*std::Object.*use.*IS schema::Object.*"):
            await self.con.execute("""
                SELECT <schema::Object>std::Object;
            """)

    async def test_edgeql_casts_illegal_10(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anyenum"):
            await self.con.execute("""
                SELECT <array<anyenum>>{};
            """)

    async def test_edgeql_casts_illegal_11(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anyenum"):
            await self.con.execute("""
                SELECT <tuple<int64, anyenum>>{};
            """)

    async def test_edgeql_casts_illegal_12(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anypoint"):
            await self.con.execute("""
                SELECT <range<anypoint>>{};
            """)

    async def test_edgeql_casts_illegal_13(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r"cannot cast into generic.*anypoint"):
            await self.con.execute("""
                SELECT <multirange<anypoint>>{};
            """)

    # abstract scalar params should be illegal
    async def test_edgeql_casts_illegal_param_01(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*'anytype'"):
            await self.con.execute("""
                SELECT <anytype>$0;
            """, 123)

    async def test_edgeql_casts_illegal_param_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anyscalar'"):
            await self.con.execute("""
                SELECT <anyscalar>$0;
            """, 123)

    async def test_edgeql_casts_illegal_param_03(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anyreal'"):
            await self.con.execute("""
                SELECT <anyreal>$0;
            """, 123)

    async def test_edgeql_casts_illegal_param_04(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anyint'"):
            await self.con.execute("""
                SELECT <anyint>$0;
            """, 123)

    async def test_edgeql_casts_illegal_param_05(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anyfloat'"):
            await self.con.execute("""
                SELECT <anyfloat>$0;
            """, 123)

    async def test_edgeql_casts_illegal_param_06(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*sequence'"):
            await self.con.execute("""
                SELECT <sequence>$0;
            """, 123)

    async def test_edgeql_casts_illegal_param_07(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anytype"):
            await self.con.execute("""
                SELECT <array<anytype>>$0;
            """, [123])

    async def test_edgeql_casts_illegal_param_08(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anytype"):
            await self.con.execute("""
                SELECT <tuple<int64, anytype>>$0;
            """, (123, 123))

    async def test_edgeql_casts_illegal_param_10(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anyenum"):
            await self.con.execute("""
                SELECT <array<anyenum>>$0;
            """, [])

    async def test_edgeql_casts_illegal_param_11(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anyenum"):
            await self.con.execute("""
                SELECT <optional tuple<int64, anyenum>>$0;
            """, None)

    async def test_edgeql_casts_illegal_param_12(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anypoint"):
            await self.con.execute("""
                SELECT <optional range<anypoint>>$0;
            """, None)

    async def test_edgeql_casts_illegal_param_13(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"parameter cannot be a generic type.*anypoint"):
            await self.con.execute("""
                SELECT <optional multirange<anypoint>>$0;
            """, None)

    # NOTE: json is a special type as it has its own type system. A
    # json value can be JSON array, object, boolean, number, string or
    # null. All of these JSON types have their own semantics. Casting
    # into json converts data into one of those specific JSON types.
    # Any of the EdgeDB numeric types (derived from anyreal) are cast
    # into JSON number, str is cast into JSON string, bool is cast
    # into JSON bool. Other EdgeDB scalars (like datetime) are cast
    # into JSON string that represents that value (similar to casting
    # to str first). Thus json values also have some type information
    # and when casting back to EdgeDB scalars this type information is
    # used to determine the valid casts (e.g. it's illegal to cast a
    # JSON string "true" into a bool).
    #
    # Casting to json is lossless (in the same way and for the same
    # reason as casting into str).

    async def test_edgeql_casts_json_01(self):
        await self.assert_query_result(
            r'''SELECT <bool><json>True = True;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <bool><json>False = False;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <str><json>"Hello" = 'Hello';''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH U := uuid_generate_v1mc()
                SELECT <uuid><json>U = U;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <datetime><json>datetime_of_statement() =
                    datetime_of_statement();
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_datetime><json>cal::to_local_datetime(
                        '2018-05-07T20:01:22.306916') =
                    cal::to_local_datetime('2018-05-07T20:01:22.306916');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_date><json>cal::to_local_date('2018-05-07')
                    = cal::to_local_date('2018-05-07');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_time><json>
                    cal::to_local_time('20:01:22.306916') =
                    cal::to_local_time('20:01:22.306916');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <duration><json>to_duration(hours:=20) =
                    to_duration(hours:=20);
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int16><json>to_int16('12345') = 12345;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int32><json>to_int32('1234567890') = 1234567890;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <int64><json>to_int64(
                    '1234567890123') = 1234567890123;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float32><json>to_float32('2.5') = 2.5;''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float64><json>to_float64('2.5') = 2.5;''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <bigint><json>to_bigint(
                    '123456789123456789123456789')
                = to_bigint(
                    '123456789123456789123456789');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT <decimal><json>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                = to_decimal(
                    '123456789123456789123456789.123456789123456789123456789');
            ''',
            [True],
        )

    async def test_edgeql_casts_json_02(self):
        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <bool><json>T.p_bool = T.p_bool;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <str><json>T.p_str = T.p_str;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <datetime><json>T.p_datetime = T.p_datetime;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <cal::local_datetime><json>T.p_local_datetime =
                    T.p_local_datetime;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <cal::local_date><json>T.p_local_date = T.p_local_date;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <cal::local_time><json>T.p_local_time = T.p_local_time;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <duration><json>T.p_duration = T.p_duration;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <int16><json>T.p_int16 = T.p_int16;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <int32><json>T.p_int32 = T.p_int32;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <int64><json>T.p_int64 = T.p_int64;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <float32><json>T.p_float32 = T.p_float32;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <float64><json>T.p_float64 = T.p_float64;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <bigint><json>T.p_bigint = T.p_bigint;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH T := (SELECT Test FILTER .p_str = 'Hello')
                SELECT <decimal><json>T.p_decimal = T.p_decimal;
            ''',
            [True],
        )

    async def test_edgeql_casts_json_03(self):
        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <bool>J.j_bool = T.p_bool;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <str>J.j_str = T.p_str;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <datetime>J.j_datetime = T.p_datetime;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <cal::local_datetime>J.j_local_datetime =
                    T.p_local_datetime;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <cal::local_date>J.j_local_date = T.p_local_date;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <cal::local_time>J.j_local_time = T.p_local_time;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <duration>J.j_duration = T.p_duration;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <int16>J.j_int16 = T.p_int16;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <int32>J.j_int32 = T.p_int32;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <int64>J.j_int64 = T.p_int64;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <float32>J.j_float32 = T.p_float32;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <float64>J.j_float64 = T.p_float64;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <bigint>J.j_bigint = T.p_bigint;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH
                    T := (SELECT Test FILTER .p_str = 'Hello'),
                    J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
                SELECT <decimal>J.j_decimal = T.p_decimal;
            ''',
            [True],
        )

    async def test_edgeql_casts_json_04(self):
        self.assertEqual(
            await self.con.query('''
                select <json>(
                    select schema::Type{name} filter .name = 'std::bool'
                )
            '''),
            edgedb.Set(('{"name": "std::bool"}',))
        )

    async def test_edgeql_casts_json_05(self):
        self.assertEqual(
            await self.con.query(
                'select <json>{(1, 2), (3, 4)}'),
            ['[1, 2]', '[3, 4]'])

        self.assertEqual(
            await self.con.query(
                'select <json>{(a := 1, b := 2), (a := 3, b := 4)}'),
            ['{"a": 1, "b": 2}', '{"a": 3, "b": 4}'])

        self.assertEqual(
            await self.con.query(
                'select <json>{[1, 2], [3, 4]}'),
            ['[1, 2]', '[3, 4]'])

        self.assertEqual(
            await self.con.query(
                'select <json>{[(1, 2)], [(3, 4)]}'),
            ['[[1, 2]]', '[[3, 4]]'])

    async def test_edgeql_casts_json_06(self):
        self.assertEqual(
            await self.con.query_json(
                'select <json>{(1, 2), (3, 4)}'),
            '[[1, 2], [3, 4]]')

        self.assertEqual(
            await self.con.query_json(
                'select <json>{[1, 2], [3, 4]}'),
            '[[1, 2], [3, 4]]')

        self.assertEqual(
            await self.con.query_json(
                'select <json>{[(1, 2)], [(3, 4)]}'),
            '[[[1, 2]], [[3, 4]]]')

    async def test_edgeql_casts_json_07(self):
        # This is the same suite of tests as for str. The point is
        # that when it comes to casting into various date and time
        # types JSON strings and regular strings should behave
        # identically.
        #
        # Canonical date and time str representations must follow ISO
        # 8601. This test assumes that the server is configured to be
        # in UTC time zone.
        await self.assert_query_result(
            r'''
                WITH x := <json>'2018-05-07T20:01:22.306916+00:00'
                SELECT <json><datetime>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same datetime
            r'''
                WITH x := <json>{
                    '2018-05-07T15:01:22.306916-05:00',
                    '2018-05-07T15:01:22.306916-05',
                    '2018-05-07T20:01:22.306916Z',
                    '2018-05-07T20:01:22.306916+0000',
                    '2018-05-07T20:01:22.306916+00',
                    # the '-' and ':' separators may be omitted
                    '20180507T200122.306916+00',
                    # acceptable RFC 3339
                    '2018-05-07 20:01:22.306916+00:00',
                    '2018-05-07t20:01:22.306916z',
                }
                SELECT <datetime>x =
                    <datetime><json>'2018-05-07T20:01:22.306916+00:00';
            ''',
            [True, True, True, True, True, True, True, True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime><json>"2018-05-07;20:01:22.306916+00:00"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime><json>"2018-05-07T20:01:22.306916"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime><json>"2018-05-07T20:01:22.306916 1000"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                '''SELECT <datetime><json>
                    "2018-05-07T20:01:22.306916 US/Central"
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax'):
            await self.con.query_single(
                'SELECT <datetime><json>"2018-05-07T20:01:22.306916 +GMT1"')

    async def test_edgeql_casts_json_08(self):
        # This is the same suite of tests as for str. The point is
        # that when it comes to casting into various date and time
        # types JSON strings and regular strings should behave
        # identically.
        #
        # Canonical date and time str representations must follow ISO
        # 8601. This test assumes that the server is configured to be
        # in UTC time zone.
        await self.assert_query_result(
            r'''
                WITH x := <json>'2018-05-07T20:01:22.306916'
                SELECT <json><cal::local_datetime>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same datetime
            r'''
                WITH x := <json>{
                    # the '-' and ':' separators may be omitted
                    '20180507T200122.306916',
                    # acceptable RFC 3339
                    '2018-05-07 20:01:22.306916',
                    '2018-05-07t20:01:22.306916',
                }
                SELECT <cal::local_datetime>x =
                    <cal::local_datetime><json>'2018-05-07T20:01:22.306916';
            ''',
            [True, True, True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''SELECT
                    <cal::local_datetime><json>"2018-05-07;20:01:22.306916"
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''SELECT <cal::local_datetime><json>
                    "2018-05-07T20:01:22.306916+01:00"
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''SELECT <cal::local_datetime><json>
                    "2018-05-07T20:01:22.306916 GMT"''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''SELECT <cal::local_datetime><json>
                    "2018-05-07T20:01:22.306916 GMT0"''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                '''SELECT <cal::local_datetime><json>
                    "2018-05-07T20:01:22.306916 US/Central"
                ''')

    async def test_edgeql_casts_json_09(self):
        # This is the same suite of tests as for str. The point is
        # that when it comes to casting into various date and time
        # types JSON strings and regular strings should behave
        # identically.
        #
        # Canonical date and time str representations must follow ISO
        # 8601.
        await self.assert_query_result(
            r'''
                WITH x := <json>'2018-05-07'
                SELECT <json><cal::local_date>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            # validating that these are all in fact the same date
            r'''
                # the '-' separators may be omitted
                WITH x := <json>'20180507'
                SELECT
                    <cal::local_date>x = <cal::local_date><json>'2018-05-07';
            ''',
            [True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date><json>"2018-05-07T20:01:22.306916"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date><json>"2018/05/07"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date><json>"2018.05.07"')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_date><json>"2018-05-07+01:00"')

    async def test_edgeql_casts_json_10(self):
        # This is the same suite of tests as for str. The point is
        # that when it comes to casting into various date and time
        # types JSON strings and regular strings should behave
        # identically.
        #
        # Canonical date and time str representations must follow ISO
        # 8601.
        await self.assert_query_result(
            r'''
                WITH x := <json>'20:01:22.306916'
                SELECT <json><cal::local_time>x = x;
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                WITH x := <json>{
                    '20:01',
                    '20:01:00',
                    # the ':' separators may be omitted
                    '2001',
                    '200100',
                }
                SELECT <cal::local_time>x = <cal::local_time>'20:01:00';
            ''',
            [True, True, True, True],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                'invalid input syntax for type std::cal::local_time'):
            await self.con.query_single(
                "SELECT <cal::local_time><json>'2018-05-07 20:01:22'")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'invalid input syntax for type'):
            await self.con.query_single(
                'SELECT <cal::local_time><json>"20:01:22.306916+01:00"')

    async def test_edgeql_casts_json_11(self):
        await self.assert_query_result(
            r"SELECT <array<int64>><json>[1, 1, 2, 3, 5]",
            [[1, 1, 2, 3, 5]]
        )

        # string to array
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'expected JSON array; got JSON string'):
            await self.con.query_single(
                r"SELECT <array<int64>><json>'asdf'")

        # array of string to array of int
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'array<std::int64>', "
                r"in array elements, "
                r"expected JSON number or null; got JSON string"):
            await self.con.query_single(
                r"SELECT <array<int64>><json>['asdf']")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'array<std::int64>', "
                r"in array elements, "
                r"expected JSON number or null; got JSON string"):
            await self.con.query_single(
                r"SELECT <array<int64>>to_json('[1, 2, \"asdf\"]')")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'array<std::int64>', "
                r"in array elements, "
                r"expected JSON number or null; got JSON string"):
            await self.con.execute("""
                SELECT <array<int64>>to_json('["a"]');
            """)

        # array with null to array
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'array<std::json>' "
                r"to 'array<std::int64>', "
                r"in array elements, "
                r"invalid null value in cast"):
            await self.con.query_single(
                r"SELECT <array<int64>>[to_json('1'), to_json('null')]")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"array<std::int64>', "
                r"in array elements, "
                r"invalid null value in cast"):
            await self.con.query_single(
                r"SELECT <array<int64>>to_json('[1, 2, null]')")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'array<std::json>' "
                r"to 'array<std::int64>', "
                r"in array elements, "
                r"invalid null value in cast"):
            await self.con.query_single(
                r"SELECT <array<int64>><array<json>>to_json('[1, 2, null]')")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<array<std::str>>', "
                r"at tuple element '0', "
                r"invalid null value in cast"):
            await self.con.query_single(
                r"select <tuple<array<str>>>to_json('[null]')")

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<array<std::str>>', "
                r"at tuple element '0', "
                r"in array elements, "
                r"invalid null value in cast"):
            await self.con.query_single(
                r"select <tuple<array<str>>>to_json('[[null]]')")

        # object to array
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"expected JSON array; got JSON object"):
            await self.con.execute("""
                SELECT <array<int64>>to_json('{"a": 1}');
            """)

        # array of object to array of scalar
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'array<std::int64>', "
                r"in array elements, "
                r"expected JSON number or null; got JSON object"):
            await self.con.execute("""
                SELECT <array<int64>>to_json('[{"a": 1}]');
            """)

        # nested array
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'array<tuple<array<std::str>>>', "
                r"in array elements, "
                r"at tuple element '0', "
                r"in array elements, "
                r"expected JSON string or null; got JSON number"):
            await self.con.execute("""
                SELECT <array<tuple<array<str>>>>to_json('[[[1]]]');
            """)

    async def test_edgeql_casts_json_12(self):
        self.assertEqual(
            await self.con.query(
                r"""
                    SELECT <tuple<a: int64, b: int64>>
                    to_json('{"a": 1, "b": 2}')
                """
            ),
            [edgedb.NamedTuple(a=1, b=2)],
        )

        await self.assert_query_result(
            r"""
                SELECT <tuple<a: int64, b: int64>>
                to_json({'{"a": 3000, "b": -1}', '{"a": 1, "b": 12}'});
            """,
            [{"a": 3000, "b": -1}, {"a": 1, "b": 12}],
        )

        await self.assert_query_result(
            r"""
                SELECT <tuple<int64, int64>>
                to_json({'[3000, -1]', '[1, 12]'})
            """,
            [[3000, -1], [1, 12]],
        )

        self.assertEqual(
            await self.con.query(
                r"""
                    SELECT <tuple<int64, int64>>
                    to_json({'[3000, -1]', '[1, 12]'})
                """
            ),
            [(3000, -1), (1, 12)],
        )

        self.assertEqual(
            await self.con.query(
                r"""
                    SELECT <tuple<json, json>>
                    to_json({'[3000, -1]', '[1, 12]'})
                """
            ),
            [('3000', '-1'), ('1', '12')],
        )

        self.assertEqual(
            await self.con.query(
                r"""
                    SELECT <tuple<json, json>>
                    to_json({'[3000, -1]', '[1, null]'})
                """
            ),
            [('3000', '-1'), ('1', 'null')],
        )

        self.assertEqual(
            await self.con.query_single(
                r"""
                    SELECT <tuple<int64, tuple<a: int64, b: int64>>>
                    to_json('[3000, {"a": 1, "b": 2}]')
                """
            ),
            (3000, edgedb.NamedTuple(a=1, b=2))
        )

        self.assertEqual(
            await self.con.query_single(
                r"""
                    SELECT <tuple<int64, array<tuple<a: int64, b: str>>>>
                    to_json('[3000, [{"a": 1, "b": "foo"},
                                     {"a": 12, "b": "bar"}]]')
                """
            ),
            (3000,
             [edgedb.NamedTuple(a=1, b="foo"),
              edgedb.NamedTuple(a=12, b="bar")])
        )

        # object with wrong element type to tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: std::int64, b: std::int64>', "
                r"at tuple element 'b', "
                r"expected JSON number or null; got JSON string"):
            await self.con.query(
                r"""
                    SELECT <tuple<a: int64, b: int64>>
                    to_json('{"a": 1, "b": "2"}')
                """
            )

        # object with null value to tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: std::int64>', "
                r"at tuple element 'a', "
                r"invalid null value in cast"):
            await self.con.query(
                r"""SELECT <tuple<a: int64>>to_json('{"a": null}')"""
            )

        # object with missing element to tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: std::int64, b: std::int64>', "
                r"at tuple element 'b', "
                r"missing value in JSON object"):
            await self.con.query(
                r"""SELECT <tuple<a: int64, b: int64>>to_json('{"a": 1}')"""
            )

        # short array to unnamed tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<std::int64, std::int64>', "
                r"at tuple element '1', "
                r"missing value in JSON object"):
            await self.con.query(
                r"""SELECT <tuple<int64, int64>>to_json('[3000]')"""
            )

        # array to named tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: std::int64, b: std::int64>', "
                r"at tuple element 'a', "
                r"missing value in JSON object"):
            await self.con.query(
                r"""
                    SELECT <tuple<a: int64, b: int64>>
                    to_json('[3000, 1000]')
                """
            )

        # string to tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'expected JSON array or object or null; got JSON string'):
            await self.con.query(
                r"""SELECT <tuple<a: int64, b: int64>> to_json('"test"')"""
            )

        # short array to unnamed tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<std::json, std::json>', "
                r"at tuple element '1', "
                r"missing value in JSON object"):
            await self.con.query(
                r"""SELECT <tuple<json, json>> to_json('[3000]')"""
            )

        # object to unnamed tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' to "
                r"'tuple<std::int64>', "
                r"at tuple element '0', "
                r"missing value in JSON object"):
            await self.con.execute("""
                SELECT <tuple<int64>>to_json('{"a": 1}');
            """)

        # nested tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: tuple<b: std::str>>', "
                r"at tuple element 'a', "
                r"at tuple element 'b', "
                r"expected JSON string or null; got JSON number"):
            await self.con.execute("""
                SELECT <tuple<a: tuple<b: str>>>to_json('{"a": {"b": 1}}');
            """)

        # array with null to tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<std::int64, std::int64>', "
                r"at tuple element '1', "
                r"invalid null value in cast"):
            await self.con.execute("""
                SELECT <tuple<int64, int64>>to_json('[1, null]');
            """)

        # object with null to tuple
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: std::int64>', "
                r"at tuple element 'a', "
                r"invalid null value in cast"):
            await self.con.execute("""
                SELECT <tuple<a: int64>>to_json('{"a": null}');
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: array<std::int64>>', "
                r"at tuple element 'a', "
                r"invalid null value in cast"):
            await self.con.execute("""
                SELECT <tuple<a: array<int64>>>to_json('{"a": null}');
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'tuple<a: tuple<b: std::str>>', "
                r"at tuple element 'a', "
                r"invalid null value in cast"):
            await self.con.execute("""
                SELECT <tuple<a: tuple<b: str>>>to_json('{"a": null}');
            """)

    async def test_edgeql_casts_json_13(self):
        await self.assert_query_result(
            r'''
                select <array<json>>to_json('null')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <array<str>>to_json('null')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <array<int64>>json_get(to_json('{}'), 'foo')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <tuple<str>>to_json('null')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <tuple<json>>to_json('null')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <bigint>to_json('null')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <decimal>to_json('null')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <bigint><str>to_json('null')
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                select <decimal><str>to_json('null')
            ''',
            [],
        )

    async def test_edgeql_casts_json_14(self):
        await self.assert_query_result(
            r'''
                select <array<json>>to_json('[]')
            ''',
            [[]],
        )

        await self.assert_query_result(
            r'''
                select <array<str>>to_json('[]')
            ''',
            [[]],
        )

    async def test_edgeql_casts_json_15(self):
        # At one point, a cast from an object inside a binary
        # operation triggered an infinite loop in staeval if the
        # object had a self link.
        await self.con.execute('''
            create type Z { create link z -> Z; };
        ''')
        await self.con.query('''
            select <json>Z union <json>Z;
        ''')

    async def test_edgeql_casts_json_16(self):
        # number to range
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"expected JSON object or null; got JSON number"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('1');
            """)

        # array to range
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"expected JSON object or null; got JSON array"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('[1]');
            """)

        # object to range, bad empty
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'range<std::int64>', "
                r"in range parameter 'empty', "
                r"expected JSON boolean or null; got JSON number"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "empty": 1
                }');
            """)

        # object to range, empty with distinct lower and upper
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"conflicting arguments in range constructor: 'empty' is "
                r"`true` while the specified bounds suggest otherwise"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "empty": true,
                    "lower": 1,
                    "upper": 2
                }');
            """)

        # object to range, empty with same lower and upper
        # and inc_lower and inc_upper
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"conflicting arguments in range constructor: 'empty' is "
                r"`true` while the specified bounds suggest otherwise"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "empty": true,
                    "lower": 1,
                    "upper": 2,
                    "inc_lower": true,
                    "inc_upper": true
                }');
            """)

        # object to range, missing inc_lower
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"JSON object representing a range must include an "
                r"'inc_lower'"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "inc_upper": false
                }');
            """)

        # object to range, missing inc_upper
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"JSON object representing a range must include an "
                r"'inc_upper'"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "inc_lower": false
                }');
            """)

        # object to range, bad inc_lower
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'range<std::int64>', "
                r"in range parameter 'inc_lower', "
                r"expected JSON boolean or null; got JSON number"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "inc_lower": 1,
                    "inc_upper": false
                }');
            """)

        # object to range, bad inc_upper
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"while casting 'std::json' "
                r"to 'range<std::int64>', "
                r"in range parameter 'inc_upper', "
                r"expected JSON boolean or null; got JSON number"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "inc_lower": false,
                    "inc_upper": 1
                }');
            """)

        # object to range, extra parameters
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"JSON object representing a range contains unexpected keys: "
                r"bar, foo"):
            await self.con.execute("""
                SELECT <range<int64>>to_json('{
                    "lower": 1,
                    "upper": 2,
                    "inc_lower": true,
                    "inc_upper": true,
                    "foo": "foo",
                    "bar": "bar"
                }');
            """)

    async def test_edgeql_casts_json_17(self):
        # number to multirange
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"expected JSON array; got JSON number"):
            await self.con.execute("""
                SELECT <multirange<int64>>to_json('1');
            """)

        # object to multirange
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"expected JSON array; got JSON object"):
            await self.con.execute("""
                SELECT <multirange<int64>>to_json('{"a": 1}');
            """)

    async def test_edgeql_casts_assignment_01(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""

                # int64 is assignment castable or implicitly castable
                # into any other numeric type
                INSERT ScalarTest {
                    p_int16 := 1,
                    p_int32 := 1,
                    p_int64 := 1,
                    p_float32 := 1,
                    p_float64 := 1,
                    p_bigint := 1,
                    p_decimal := 1,
                };
            """)

            await self.assert_query_result(
                r"""
                    SELECT ScalarTest {
                        p_int16,
                        p_int32,
                        p_int64,
                        p_float32,
                        p_float64,
                        p_bigint,
                        p_decimal,
                    };
                """,
                [{
                    'p_int16': 1,
                    'p_int32': 1,
                    'p_int64': 1,
                    'p_float32': 1,
                    'p_float64': 1,
                    'p_bigint': 1,
                    'p_decimal': 1,
                }],
            )

    async def test_edgeql_casts_assignment_02(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""

                # float64 is assignment castable to float32
                INSERT ScalarTest {
                    p_float32 := 1.5,
                };
            """)

            await self.assert_query_result(
                r"""
                    SELECT ScalarTest {
                        p_float32,
                    };
                """,
                [{
                    'p_float32': 1.5,
                }],
            )

    async def test_edgeql_casts_assignment_03(self):
        async with self._run_and_rollback():
            # in particular, bigint and decimal are not assignment-castable
            # into any other numeric type
            for typename in ['int16',
                             'int32',
                             'int64',
                             'float32',
                             'float64']:

                for numtype in {'bigint', 'decimal'}:

                    query = f'''
                        INSERT ScalarTest {{
                            p_{typename} := <{numtype}>3,
                            p_{numtype} := 1001,
                        }};
                    '''
                    async with self.assertRaisesRegexTx(
                            edgedb.QueryError,
                            r'invalid target for property',
                            msg=query):
                        await self.con.execute(query + f'''
                            # clean up, so other tests can proceed
                            DELETE (
                                SELECT ScalarTest
                                FILTER .p_{numtype} = 1001
                            );
                        ''')

    async def test_edgeql_casts_custom_scalar_01(self):
        await self.assert_query_result(
            '''
                SELECT <custom_str_t>'ABC'
            ''',
            ['ABC']
        )

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                'invalid custom_str_t'):
            await self.con.query(
                "SELECT <custom_str_t>'123'")

    async def test_edgeql_casts_custom_scalar_02(self):
        await self.assert_query_result(
            """
                SELECT <foo><bar>'test'
            """,
            ['test'],
        )

        await self.assert_query_result(
            """
                SELECT <array<foo>><array<bar>>['test']
            """,
            [['test']],
        )

    async def test_edgeql_casts_custom_scalar_03(self):
        await self.assert_query_result(
            """
                SELECT <array<custom_str_t>><array<bar>>['TEST']
            """,
            [['TEST']],
        )

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError, r'invalid'
        ):
            await self.con.query("""
                SELECT <custom_str_t><bar>'test'
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError, r'invalid'
        ):
            await self.con.query("""
                SELECT <array<custom_str_t>><array<bar>>['test']
            """)

    async def test_edgeql_casts_custom_scalar_04(self):
        await self.con.execute('''
            create abstract scalar type abs extending int64;
            create scalar type foo2 extending abs;
            create scalar type bar2 extending abs;
        ''')

        await self.assert_query_result(
            """
                SELECT <foo2><bar2>42
            """,
            [42],
        )

        await self.assert_query_result(
            """
                SELECT <array<foo2>><array<bar2>>[42]
            """,
            [[42]],
        )

    async def test_edgeql_casts_custom_scalar_05(self):
        await self.con.execute('''
            create abstract scalar type xfoo extending int64;
            create abstract scalar type xbar extending int64;
            create scalar type bar1 extending xfoo, xbar;
            create scalar type bar2 extending xfoo, xbar;
        ''')

        await self.assert_query_result(
            """
                SELECT <bar1><bar2>42
            """,
            [42],
        )

        await self.assert_query_result(
            """
                SELECT <array<bar1>><array<bar2>>[42]
            """,
            [[42]],
        )

    async def test_edgeql_casts_custom_scalar_06(self):
        await self.con.execute(
            '''
            create scalar type x extending str {
                create constraint expression on (false)
            };
        '''
        )

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError, 'invalid x'
        ):
            await self.con.query("""SELECT <x>42""")

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError, 'invalid x'
        ):
            await self.con.query("""SELECT <x>to_json('"a"')""")

    async def test_edgeql_casts_tuple_params_01(self):
        # insert tuples into a nested array
        def nest(data):
            return [(nest(x),) if isinstance(x, list) else x for x in data]

        tests = {
            # Basic tuples
            'tuple<str, bool>': [('x', True), ('y', False)],
            'optional tuple<str, bool>': [('x', True), None],

            # Some pointlessly nested tuples
            'tuple<tuple<str, bool>>': [(('x', True),)],
            'tuple<tuple<str, bool>, int64>': [(('x', True), 1)],

            # Basic array examples
            'array<tuple<int64, str>>': [
                [],
                [(0, 'zero')],
                [(0, 'zero'), (1, 'one')],
            ],

            'optional array<tuple<int64, str>>': [
                None,
                [],
                [(0, 'zero')],
                [(0, 'zero'), (1, 'one')],
            ],

            'array<tuple<str, array<int64>>>': [
                [],
                [('x', [])],
                [('x', [1])],
                [('x', []), ('y', []), ('z', [])],
                [('x', [1]), ('y', []), ('z', [])],
                [('x', []), ('y', [1]), ('z', [])],
                [('x', []), ('y', []), ('z', [1])],
                [('x', []), ('y', [1, 2]), ('z', [1, 2, 3])],
            ],

            # Arrays of pointlessly nested tuples
            'array<tuple<tuple<str, bool>, int64>>': [
                [],
                [(('x', True), 1)],
                [(('x', True), 1), (('z', False), 2)],
            ],
            'array<tuple<tuple<array<str>, bool>, int64>>': [
                [],
                [(([], True), 1)],
                [((['x', 'y', 'z'], True), 1), ((['z'], False), 2)],
            ],

            # Using tuples to produce just a pure nested array
            'array<tuple<array<int64>>>': [nest(x) for x in [
                [],
                [[], []],
                [[], [], []],
                [[1], [], []],
                [[], [1], []],
                [[], [], [1]],
                [[1, 2, 3], [], [4, 5, 6]],
                [[1], [2, 3], [4, 5, 6]],
            ]],

            'array<tuple<array<tuple<array<int64>>>>>': [nest(x) for x in [
                [],
                [[], []],
                [[], [], []],
                [[[], [], []], [[], []], [[]]],
                [[[1]], [], []],
                [[], [[1]], []],
                [[], [], [[1]]],
                [[[1, 2, 3], [], [4, 5, 6]]],
                [[[1, 2, 3], []], [[4, 5, 6]]],
                [[[1, 2], [3]], [], [[4, 5], [6]]],
            ]],
        }

        for typ, vals in tests.items():
            qry = f"SELECT <{typ}>$0"
            for val in vals:
                await self.assert_query_result(
                    qry,
                    [v for v in [val] if v is not None],
                    variables=(val,),
                    msg=f'type: {typ}, data: {val}',
                )

    async def test_edgeql_casts_tuple_params_02(self):
        await self.assert_query_result(
            '''
            SELECT Test {
                id,
                num := (<tuple<int64, float64, str, bytes>>$tup).0,
                st := (<tuple<int64, float64, str, bytes>>$tup).2,
            };
            ''',
            [{'num': 0, 'st': "str"}],
            variables={'tup': (0, 1.0, "str", b"bytes")},
        )

    async def test_edgeql_casts_tuple_params_03(self):
        # try *doing* something with the input
        await self.con.query(
            r'''
            create type Record {
                 create required property name -> str;
                 create multi property tags -> int64;
            }
            '''
        )

        data = [
            [],
            [('x', [])],
            [('x', [1])],
            [('x', []), ('y', []), ('z', [])],
            [('x', [1]), ('y', []), ('z', [])],
            [('x', []), ('y', [1]), ('z', [])],
            [('x', []), ('y', []), ('z', [1])],
            [('x', []), ('y', [1, 2]), ('z', [1, 2, 3])],
        ]

        qry = r'''
        for row in array_unpack(<array<tuple<str, array<int64>>>>$0) union ((
            insert Record { name := row.0, tags := array_unpack(row.1) }
        ))
        '''

        for inp in data:
            exp = tb.bag([
                {'name': name, 'tags': tb.bag(tags)}
                for name, tags in inp
            ])

            async with self._run_and_rollback():
                await self.con.execute(qry, inp)

                await self.assert_query_result(
                    '''
                    select Record { name, tags }
                    ''',
                    exp,
                    msg=f'inp: {inp}',
                )

    async def test_edgeql_casts_tuple_params_04(self):
        # Test doing a coalesce on an optional tuple input
        await self.assert_query_result(
            '''
            select (<optional tuple<str, int64>>$0) ?? ('foo', 0)
            ''',
            [('foo', 0)],
            variables=(None,),
        )

    async def test_edgeql_casts_tuple_params_05(self):
        max_depth = 20

        # Test deep nesting
        t = 'int64'
        v = 0
        for _ in range(max_depth):
            t = f'tuple<{t}>'
            v = (v,)

        await self.assert_query_result(
            f'''
            select <{t}>$0
            ''',
            [v],
            variables=(v,),
        )

        # One more and it should fail
        t = f'tuple<{t}>'
        v = (v,)
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'too deeply nested'):
            await self.con.query(f"""
                select <{t}>$0
            """, v)

    async def test_edgeql_casts_tuple_params_06(self):
        # Test multiple tuple params mixed with other stuff
        await self.assert_query_result(
            '''
            select
                (<tuple<str, str>>$0).0 ++ <str>$1 ++ (<tuple<str, str>>$0).1
            ''',
            ['foo bar'],
            variables=(('foo', 'bar'), ' ',),
        )
        await self.assert_query_result(
            '''
            select
                (<tuple<str, str>>$0).0 ++ <str>$1 ++ (<tuple<str, str>>$0).1
                ++ '!'
            ''',
            ['foo bar!'],
            variables=(('foo', 'bar'), ' ',),
        )
        await self.assert_query_result(
            '''
            select
                (<tuple<str, str>>$0).0 ++ <str>$1 ++ (<tuple<str, str>>$0).1
                ++ (with z := (<tuple<str, str>>$2) select (z.0 ++ z.1))
                ++ '!'
            ''',
            ['foo barxy!'],
            variables=(('foo', 'bar'), ' ', ('x', 'y')),
        )

    async def test_edgeql_casts_tuple_params_07(self):
        await self.assert_query_result(
            '''
            select <tuple<name: str, flag: bool>>$0
            ''',
            [{'name': 'a', 'flag': True}],
            # The server supports named tuple input, but edgedb-python
            # doesn't let you specify them nicely.
            variables=(('a', True),)
        )

    async def test_edgeql_casts_tuple_params_08(self):
        await self.assert_query_result(
            '''
            select { x := <optional tuple<str, str>>$0, y := <str>$1 };
            ''',
            [{'x': None, 'y': "test"}],
            variables=(None, 'test'),
        )

        await self.assert_query_result(
            '''
            select { x := <optional tuple<str, str>>$0, y := <int64>$1 };
            ''',
            [{'x': None, 'y': 11111}],
            variables=(None, 11111),
        )

    async def test_edgeql_casts_tuple_params_09(self):
        await self.con.query('''
            WITH
              p := <tuple<test: str>>$0
            insert Test { p_tup := p };
        ''', ('foo',))

        await self.assert_query_result(
            '''
            select Test { p_tup } filter exists .p_tup
            ''',
            [{'p_tup': {'test': 'foo'}}],
        )
        await self.assert_query_result(
            '''
            WITH
              p := <tuple<test: str>>$0
            select p
            ''',
            [{'test': 'foo'}],
            variables=(('foo',),),
        )
        await self.assert_query_result(
            '''
            select <tuple<test: str>>$0
            ''',
            [{'test': 'foo'}],
            variables=(('foo',),),
        )
        await self.assert_query_result(
            '''
            select <array<tuple<test: str>>>$0
            ''',
            [[{'test': 'foo'}, {'test': 'bar'}]],
            variables=([('foo',), ('bar',)],),
        )

    async def test_edgeql_cast_empty_set_to_array_01(self):
        await self.assert_query_result(
            r'''
                SELECT <array<Object>>{};
            ''',
            [],
        )

    async def test_edgeql_casts_std_enum_01(self):
        await self.assert_query_result(
            '''
            select <schema::Cardinality>{}
            ''',
            [],
        )

    async def test_edgeql_casts_json_set_02(self):
        await self.assert_query_result(
            '''
            select <tuple<str>>json_set(
                to_json('["b"]'), "0", value := <json>"a");
            ''',
            [('a',)],
        )

    async def test_edgeql_casts_all_null(self):
        # For *every* cast, try casting a value we know
        # will be represented as NULL.
        casts = await self.con.query('''
            select schema::Cast { from_type: {name}, to_type: {name} }
            filter not .from_type is schema::ObjectType
        ''')

        def _t(s):
            # Instantiate polymorphic types
            return (
                s
                .replace('anytype', 'str')
                .replace('anytuple', 'tuple<str, int64>')
                .replace('std::anyenum', 'schema::Cardinality')
                .replace('std::anypoint', 'int64')
            )

        from_types = {_t(cast.from_type.name) for cast in casts}
        type_keys = {
            name: f'x{i}' for i, name in enumerate(sorted(from_types))
        }
        # Populate a type that has an optional field for each cast source type
        sep = '\n                '
        props = sep.join(
            f'CREATE PROPERTY {n} -> {_t(t)};'
            for t, n in type_keys.items()
        )
        setup = f'''
            CREATE TYPE Null {{
                {props}
            }};
            INSERT Null;
        '''
        await self.con.execute(setup)

        # Do each cast
        for cast in casts:
            prop = type_keys[_t(cast.from_type.name)]

            await self.assert_query_result(
                f'''
                SELECT Null {{
                    res := <{_t(cast.to_type.name)}>.{prop}
                }}
                ''',
                [{"res": None}],
                msg=f'{cast.from_type.name} to {cast.to_type.name}',
            )

            # For casts from JSON, also do the related operation of
            # casting from a JSON null, which should produce an empty
            # set.
            if cast.from_type.name == 'std::json':
                await self.assert_query_result(
                    f'''
                    SELECT <{_t(cast.to_type.name)}>to_json('null')
                    ''',
                    [],
                    msg=f'json null cast to {cast.to_type.name}',
                )

    async def test_edgeql_casts_uuid_to_object(self):
        persons = await self.con.query('select Person { id }')

        dummy_uuid = '1' * 32

        res = await self.con.query('select <Person><uuid>$0', persons[0].id)
        self.assertEqual(len(res), 1)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError, r'with id .* does not exist'
        ):
            await self.con.query('select <Person><uuid>$0', dummy_uuid)

        await self.assert_query_result(
            '''
            select (<Person>{<uuid>$0, <uuid>$1}) { name }
            order by .name;
            ''',
            [{'name': 'kelly'}, {'name': 'tom'}],
            json_only=True,
            variables=(persons[0].id, persons[1].id),
        )

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError, r'with id .* does not exist'
        ):
            await self.con.query(
                '''
                select (<Person>{<uuid>$0, <uuid>$1}) { name }
                order by .name;
                ''', persons[0].id, dummy_uuid
            )

        res = await self.con.query(
            'select <Person><optional uuid>$0', persons[0].id
        )
        self.assertEqual(len(res), 1)

        res = await self.con.query(
            'select <optional Person><optional uuid>$0', None
        )
        self.assertEqual(len(res), 0)

        res = await self.con.query('select <optional Person><optional uuid>{}')
        self.assertEqual(len(res), 0)

        res = await self.con.query('select <Person><optional uuid>$0', None)
        self.assertEqual(len(res), 0)

        res = await self.con.query('select <Person>$0', persons[0].id)
        self.assertEqual(len(res), 1)

        res = await self.con.query('select <optional Person>$0', None)
        self.assertEqual(len(res), 0)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError, r'with id .* does not exist'
        ):
            await self.con.query('select <optional Person>$0', dummy_uuid)

        async with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError, r'with id .* does not exist'
        ):
            await self.con.query('select <Person>$0', dummy_uuid)
