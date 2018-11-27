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
import unittest  # NOQA

import edgedb

from edb.server import _testbase as tb


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
                          'casts.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'casts_setup.eql')

    ISOLATED_METHODS = False

    # NOTE: nothing can be cast into bytes

    async def test_edgeql_casts_bytes_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>True;
            """)

    async def test_edgeql_casts_bytes_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>uuid_generate_v1mc();
            """)

    async def test_edgeql_casts_bytes_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>'Hello';
            """)

    async def test_edgeql_casts_bytes_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_json('1');
            """)

    async def test_edgeql_casts_bytes_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>datetime_current();
            """)

    async def test_edgeql_casts_bytes_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_naive_datetime('2018-05-07T20:01:22.306916');
            """)

    async def test_edgeql_casts_bytes_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_naive_date('2018-05-07');
            """)

    async def test_edgeql_casts_bytes_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_naive_time('20:01:22.306916');
            """)

    async def test_edgeql_casts_bytes_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_timedelta(hours:=20);
            """)

    async def test_edgeql_casts_bytes_10(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_int16('2');
            """)

    async def test_edgeql_casts_bytes_11(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_int32('2');
            """)

    async def test_edgeql_casts_bytes_12(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_int64('2');
            """)

    async def test_edgeql_casts_bytes_13(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_float32('2');
            """)

    async def test_edgeql_casts_bytes_14(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_float64('2');
            """)

    async def test_edgeql_casts_bytes_15(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast'):
            await self.query("""
                SELECT <bytes>to_decimal('2');
            """)

    # NOTE: casts are idempotent

    async def test_edgeql_casts_idempotence_01(self):
        await self.assert_query_result(r'''
            SELECT <bool><bool>True IS bool;
            SELECT <bytes><bytes>b'Hello' IS bytes;
            SELECT <str><str>'Hello' IS str;
            SELECT <json><json>to_json('1') IS json;
            SELECT <uuid><uuid>uuid_generate_v1mc() IS uuid;

            SELECT <datetime><datetime>datetime_current() IS datetime;
            SELECT <naive_datetime><naive_datetime>to_naive_datetime(
                '2018-05-07T20:01:22.306916') IS naive_datetime;
            SELECT <naive_date><naive_date>to_naive_date('2018-05-07') IS
                naive_date;
            SELECT <naive_time><naive_time>to_naive_time('20:01:22.306916') IS
                naive_time;
            SELECT <timedelta><timedelta>to_timedelta(hours:=20) IS timedelta;

            SELECT <int16><int16>to_int16('12345') IS int16;
            SELECT <int32><int32>to_int32('1234567890') IS int32;
            SELECT <int64><int64>to_int64('1234567890123') IS int64;
            SELECT <float32><float32>to_float32('2.5') IS float32;
            SELECT <float64><float64>to_float64('2.5') IS float64;

            SELECT <decimal><decimal>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                IS decimal;
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_casts_idempotence_02(self):
        await self.assert_query_result(r'''
            SELECT <bool><bool>True = True;
            SELECT <bytes><bytes>b'Hello' = b'Hello';
            SELECT <str><str>'Hello' = 'Hello';
            SELECT <json><json>to_json('1') = to_json('1');

            WITH U := uuid_generate_v1mc()
            SELECT <uuid><uuid>U = U;

            SELECT <datetime><datetime>datetime_of_statement() =
                datetime_of_statement();
            SELECT <naive_datetime><naive_datetime>to_naive_datetime(
                    '2018-05-07T20:01:22.306916') =
                to_naive_datetime('2018-05-07T20:01:22.306916');
            SELECT <naive_date><naive_date>to_naive_date('2018-05-07') =
                to_naive_date('2018-05-07');
            SELECT <naive_time><naive_time>to_naive_time('20:01:22.306916') =
                to_naive_time('20:01:22.306916');
            SELECT <timedelta><timedelta>to_timedelta(hours:=20) =
                to_timedelta(hours:=20);

            SELECT <int16><int16>to_int16('12345') = 12345;
            SELECT <int32><int32>to_int32('1234567890') = 1234567890;
            SELECT <int64><int64>to_int64('1234567890123') = 1234567890123;
            SELECT <float32><float32>to_float32('2.5') = 2.5;
            SELECT <float64><float64>to_float64('2.5') = 2.5;

            SELECT <decimal><decimal>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                = to_decimal(
                    '123456789123456789123456789.123456789123456789123456789');
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_casts_str_01(self):
        # Casting to str and back is lossless for every scalar (if
        # legal). It's still not legal to cast bytes into str or some
        # of the json values.
        await self.assert_query_result(r'''
            SELECT <bool><str>True = True;
            SELECT <bool><str>False = False;
            # only JSON strings can be cast into EdgeQL str
            SELECT <json><str>to_json('"Hello"') = to_json('"Hello"');

            WITH U := uuid_generate_v1mc()
            SELECT <uuid><str>U = U;

            SELECT <datetime><str>datetime_of_statement() =
                datetime_of_statement();
            SELECT <naive_datetime><str>to_naive_datetime(
                    '2018-05-07T20:01:22.306916') =
                to_naive_datetime('2018-05-07T20:01:22.306916');
            SELECT <naive_date><str>to_naive_date('2018-05-07') =
                to_naive_date('2018-05-07');
            SELECT <naive_time><str>to_naive_time('20:01:22.306916') =
                to_naive_time('20:01:22.306916');
            SELECT <timedelta><str>to_timedelta(hours:=20) =
                to_timedelta(hours:=20);

            SELECT <int16><str>to_int16('12345') = 12345;
            SELECT <int32><str>to_int32('1234567890') = 1234567890;
            SELECT <int64><str>to_int64('1234567890123') = 1234567890123;
            SELECT <float32><str>to_float32('2.5') = 2.5;
            SELECT <float64><str>to_float64('2.5') = 2.5;

            SELECT <decimal><str>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                = to_decimal(
                    '123456789123456789123456789.123456789123456789123456789');
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_casts_str_02(self):
        # Certain strings can be cast into other types losslessly,
        # making them "canonical" string representations of those
        # values.
        await self.assert_query_result(r'''
            WITH x := {'true', 'false'}
            SELECT <str><bool>x = x;

            # non-canonical
            WITH x := {'True', 'False', 'TRUE', 'FALSE'}
            SELECT <str><bool>x = x;

            WITH x := {'True', 'False', 'TRUE', 'FALSE'}
            SELECT <str><bool>x = str_lower(x);
        ''', [
            [True, True],
            [False, False, False, False],
            [True, True, True, True],
        ])

    async def test_edgeql_casts_str_03(self):
        # str to json is always lossless
        await self.assert_query_result(r'''
            WITH x := {'any', 'arbitrary', '♠gibberish♠'}
            SELECT <str><json>x = x;
        ''', [
            [True, True, True],
        ])

    async def test_edgeql_casts_str_04(self):
        # canonical uuid representation as a string is using lowercase
        await self.assert_query_result(r'''
            WITH x := 'd4288330-eea3-11e8-bc5f-7faf132b1d84'
            SELECT <str><uuid>x = x;

            # non-canonical
            WITH x := {
                'D4288330-EEA3-11E8-BC5F-7FAF132B1D84',
                'D4288330-Eea3-11E8-Bc5F-7Faf132B1D84',
                'D4288330-eea3-11e8-bc5f-7faf132b1d84',
            }
            SELECT <str><uuid>x = x;

            WITH x := {
                'D4288330-EEA3-11E8-BC5F-7FAF132B1D84',
                'D4288330-Eea3-11E8-Bc5F-7Faf132B1D84',
                'D4288330-eea3-11e8-bc5f-7faf132b1d84',
            }
            SELECT <str><uuid>x = str_lower(x);
        ''', [
            [True],
            [False, False, False],
            [True, True, True],
        ])

    async def test_edgeql_casts_str_05(self):
        # Canonical date and time str representations must follow ISO
        # 8601. This test assumes that the server is configured to be
        # in UTC time zone.
        await self.assert_query_result(r'''
            WITH x := '2018-05-07T20:01:22.306916+00:00'
            SELECT <str><datetime>x = x;

            # non-canonical
            WITH x := {
                '2018-05-07 20:01:22.306916+00:00',
                '2018-05-07T20:01:22.306916',
                '2018-05-07T20:01:22.306916+00',
                '2018-05-07T15:01:22.306916-05:00',
                '2018-05-07T15:01:22.306916-05',
            }
            SELECT <str><datetime>x = x;

            # validating that these are all in fact the same datetime
            WITH x := {
                '2018-05-07 20:01:22.306916+00:00',
                '2018-05-07T20:01:22.306916',
                '2018-05-07T20:01:22.306916+00',
                '2018-05-07T15:01:22.306916-05:00',
                '2018-05-07T15:01:22.306916-05',
            }
            SELECT <datetime>x = <datetime>'2018-05-07T20:01:22.306916+00:00';
        ''', [
            [True],
            [False, False, False, False, False],
            [True, True, True, True, True],
        ])

    async def test_edgeql_casts_str_06(self):
        # Canonical date and time str representations must follow ISO
        # 8601. This test assumes that the server is configured to be
        # in UTC time zone.
        await self.assert_query_result(r'''
            WITH x := '2018-05-07T20:01:22.306916'
            SELECT <str><naive_datetime>x = x;

            # non-canonical
            WITH x := {
                '2018-05-07 20:01:22.306916',
                '2018-05-07,20:01:22.306916',
                '2018-05-07;20:01:22.306916',
            }
            SELECT <str><naive_datetime>x = x;

            # validating that these are all in fact the same naive_datetime
            WITH x := {
                '2018-05-07 20:01:22.306916',
                '2018-05-07,20:01:22.306916',
                '2018-05-07;20:01:22.306916',
            }
            SELECT <naive_datetime>x =
                <naive_datetime>'2018-05-07T20:01:22.306916';
        ''', [
            [True],
            [False, False, False],
            [True, True, True],
        ])

    async def test_edgeql_casts_str_07(self):
        # Canonical date and time str representations must follow ISO
        # 8601.
        await self.assert_query_result(r'''
            WITH x := '2018-05-07'
            SELECT <str><naive_date>x = x;

            # non-canonical
            WITH x := {
                '2018-05-07T20:01:22.306916',
                '2018/05/07',
                '2018.05.07',
                '2018`05`07',
                '20180507',
            }
            SELECT <str><naive_date>x = x;

            # validating that these are all in fact the same date
            WITH x := {
                '2018-05-07T20:01:22.306916',
                '2018/05/07',
                '2018.05.07',
                '2018`05`07',
                '20180507',
            }
            SELECT <naive_date>x = <naive_date>'2018-05-07';
        ''', [
            [True],
            [False, False, False, False, False],
            [True, True, True, True, True],
        ])

    async def test_edgeql_casts_str_08(self):
        # Canonical date and time str representations must follow ISO
        # 8601.
        await self.assert_query_result(r'''
            WITH x := '20:01:22.306916'
            SELECT <str><naive_time>x = x;

            # non-canonical
            WITH x := {
                '2018-05-07 20:01:22.306916',
                '200122.306916',
            }
            SELECT <str><naive_time>x = x;

            # validating that these are all in fact the same time
            WITH x := {
                '2018-05-07 20:01:22.306916',
                '200122.306916',
            }
            SELECT <naive_time>x = <naive_time>'20:01:22.306916';
        ''', [
            [True],
            [False, False],
            [True, True],
        ])

    async def test_edgeql_casts_str_09(self):
        # Canonical timedelta is a bit weird.
        await self.assert_query_result(r'''
            WITH x := '20:01:22.306916'
            SELECT <str><timedelta>x = x;

            # non-canonical
            WITH x := {
                '20h 1m 22.306916s',
                '20 hours 1 minute 22.306916 seconds',
                '72082.306916',  # the timedelta in seconds
                '0.834285959675926 days',
            }
            SELECT <str><timedelta>x = x;

            # validating that these are all in fact the same timedelta
            WITH x := {
                '20h 1m 22.306916s',
                '20 hours 1 minute 22.306916 seconds',
                '72082.306916',  # the timedelta in seconds
                '0.834285959675926 days',
            }
            SELECT <timedelta>x = <timedelta>'20:01:22.306916';
        ''', [
            [True],
            [False, False, False, False],
            [True, True, True, True],
        ])

    async def test_edgeql_casts_str_10(self):
        # valid casts from str to any integer is lossless, as long as
        # there's no whitespace, which is trimmed
        await self.assert_query_result(r'''
            WITH x := {'-20', '0', '7', '12345'}
            SELECT <str><int16>x = x;
            WITH x := {'-20', '0', '7', '12345'}
            SELECT <str><int32>x = x;
            WITH x := {'-20', '0', '7', '12345'}
            SELECT <str><int64>x = x;

            # with whitespace
            WITH x := {
                '       42',
                '42     ',
                '       42      ',
            }
            SELECT <str><int16>x = x;

            # validating that these are all in fact the same value
            WITH x := {
                '       42',
                '42     ',
                '       42      ',
            }
            SELECT <int16>x = 42;
        ''', [
            [True, True, True, True],
            [True, True, True, True],
            [True, True, True, True],

            [False, False, False],
            [True, True, True],
        ])

    async def test_edgeql_casts_str_11(self):
        # There's too many ways of representing floats. Outside of
        # trivial 1-2 digit cases, relying on any str being
        # "canonical" is not safe, making most casts from str to float
        # lossy.
        await self.assert_query_result(r'''
            WITH x := {'-20', '0', '7.2'}
            SELECT <str><float32>x = x;
            WITH x := {'-20', '0', '7.2'}
            SELECT <str><float64>x = x;

            # non-canonical
            WITH x := {
                '0.0000000001234',
                '1234E-13',
                '0.1234e-9',
            }
            SELECT <str><float32>x = x;
            WITH x := {
                '0.0000000001234',
                '1234E-13',
                '0.1234e-9',
            }
            SELECT <str><float64>x = x;

            # validating that these are all in fact the same value
            WITH x := {
                '0.0000000001234',
                '1234E-13',
                '0.1234e-9',
            }
            SELECT <float64>x = 1234e-13;
        ''', [
            [True, True, True],
            [True, True, True],

            [False, False, False],
            [False, False, False],

            [True, True, True],
        ])

    async def test_edgeql_casts_str_12(self):
        # The canonical string representation of decimals is without
        # use of scientific notation.
        await self.assert_query_result(r'''
            WITH x := {
                '-20', '0', '7.2', '0.0000000001234', '1234.00000001234'
            }
            SELECT <str><decimal>x = x;

            # non-canonical
            WITH x := {
                '1234E-13',
                '0.1234e-9',
            }
            SELECT <str><decimal>x = x;

            # validating that these are all in fact the same date
            WITH x := {
                '1234E-13',
                '0.1234e-9',
            }
            SELECT <decimal>x = <decimal>'0.0000000001234';
        ''', [
            [True, True, True, True, True],
            [False, False],
            [True, True],
        ])

    async def test_edgeql_casts_str_13(self):
        # Casting to str and back is lossless for every scalar (if
        # legal). It's still not legal to cast bytes into str or some
        # of the json values.
        await self.assert_query_result(r'''
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <uuid><str>T.id = T.id;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <bool><str>T.p_bool = T.p_bool;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <str><str>T.p_str = T.p_str;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <datetime><str>T.p_datetime = T.p_datetime;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <naive_datetime><str>T.p_naive_datetime =
                T.p_naive_datetime;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <naive_date><str>T.p_naive_date = T.p_naive_date;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <naive_time><str>T.p_naive_time = T.p_naive_time;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <timedelta><str>T.p_timedelta = T.p_timedelta;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <int16><str>T.p_int16 = T.p_int16;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <int32><str>T.p_int32 = T.p_int32;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <int64><str>T.p_int64 = T.p_int64;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <float32><str>T.p_float32 = T.p_float32;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <float64><str>T.p_float64 = T.p_float64;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <decimal><str>T.p_decimal = T.p_decimal;
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_casts_numeric_01(self):
        # Casting to decimal and back should be lossless for any other
        # numeric type.
        await self.assert_query_result(r'''
            # technically we're already casting a literal int64 to int16 first
            WITH x := <int16>{-32768, -32767, -100, 0, 13, 32766, 32767}
            SELECT <int16><decimal>x = x;

            # technically we're already casting a literal int64 to int32 first
            WITH x := <int32>{-2147483648, -2147483647, -65536, -100, 0,
                              13, 32768, 2147483646, 2147483647}
            SELECT <int32><decimal>x = x;

            WITH x := <int64>{
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
            }
            SELECT <int64><decimal>x = x;
        ''', [
            [True, True, True, True, True, True, True],
            [True, True, True, True, True, True, True, True, True],
            [True, True, True, True, True, True, True, True, True, True, True],
        ])

    async def test_edgeql_casts_numeric_02(self):
        # Casting to decimal and back should be lossless for any other
        # numeric type.
        await self.assert_query_result(r'''
            # technically we're already casting a literal int64 or
            # float64 to float32 first
            WITH x := <float32>{-3.31234e+38, -1.234e+12, -1.234e-12, -100, 0,
                                13, 1.234e-12, 1.234e+12, 3.4e+38}
            SELECT <float32><decimal>x = x;

            WITH x := <float64>{-1.61234e+308, -1.234e+42, -1.234e-42, -100, 0,
                                13, 1.234e-42, 1.234e+42, 1.7e+308}
            SELECT <float64><decimal>x = x;
        ''', [
            [True, True, True, True, True, True, True, True, True],
            [True, True, True, True, True, True, True, True, True],
        ])

    async def test_edgeql_casts_numeric_03(self):
        # It is especially dangerous to cast an int32 into float32 and
        # back because float32 cannot losslessly represent the entire
        # range of int32, but it can represent some of it, so no
        # obvious errors would be raised (as any int32 value is
        # technically withing valid range of float32), but the value
        # could be mangled.
        await self.assert_query_result(r'''
            # ints <= 2^24 can be represented exactly in a float32
            WITH x := <int32>{16777216, 16777215, 16777214,
                              1677721, 167772, 16777}
            SELECT <int32><float32>x = x;

            # max int32 -100, -1000
            WITH x := <int32>{2147483548, 2147482648}
            SELECT <int32><float32>x = x;

            WITH x := <int32>{2147483548, 2147482648}
            SELECT <int32><float32>x;
        ''', [
            [True, True, True, True, True, True],
            [False, False],
            [2147483520, 2147482624],
        ])

    async def test_edgeql_casts_numeric_04(self):
        await self.assert_query_result(r'''
            # ints <= 2^24 can be represented exactly in a float32
            WITH x := <int32>{16777216, 16777215, 16777214,
                              1677721, 167772, 16777}
            SELECT <int32><float64>x = x;

            # max int32 -1, -2, -3, -10, -100, -1000
            WITH x := <int32>{2147483647, 2147483646, 2147483645,
                              2147483638, 2147483548, 2147482648}
            SELECT <int32><float64>x = x;
        ''', [
            [True, True, True, True, True, True],
            [True, True, True, True, True, True],
        ])

    async def test_edgeql_casts_numeric_05(self):
        # Due to the sparseness of float values large integers may not
        # be representable exactly if they require better precision
        # than float provides.
        await self.assert_query_result(r'''
            # 2^31 -1, -2, -3, -10
            WITH x := <int32>{2147483647, 2147483646, 2147483645,
                              2147483638}
            # 2147483647 is the max int32
            SELECT x <= <int32>2147483647;
        ''', [
            [True, True, True, True],
        ])

        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError, r"integer out of range"):
            async with self.con.transaction():
                await self.query("""
                    SELECT <int32><float32><int32>2147483647;
                """)

        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError, r"integer out of range"):
            async with self.con.transaction():
                await self.query("""
                    SELECT <int32><float32><int32>2147483646;
                """)

        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError, r"integer out of range"):
            async with self.con.transaction():
                await self.query("""
                    SELECT <int32><float32><int32>2147483645;
                """)

        with self.assertRaisesRegex(
                edgedb.NumericOutOfRangeError, r"integer out of range"):
            async with self.con.transaction():
                await self.query("""
                    SELECT <int32><float32><int32>2147483638;
                """)

    async def test_edgeql_casts_numeric_06(self):
        await self.assert_query_result(r'''
            SELECT <int16>1;
            SELECT <int32>1;
            SELECT <int64>1;
            SELECT <float32>1;
            SELECT <float64>1;
            SELECT <decimal>1;
        ''', [
            [1],
            [1],
            [1],
            [1],
            [1],
            [1],
        ])

    async def test_edgeql_casts_numeric_07(self):
        numerics = ['int16', 'int32', 'int64', 'float32', 'float64', 'decimal']

        for t1, t2 in itertools.product(numerics, numerics):
            await self.assert_query_result(f'''
                SELECT <{t1}><{t2}>1;
            ''', [
                [1],
            ])

    # casting into an abstract scalar should be illegal
    async def test_edgeql_casts_illegal_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot cast into generic.*'anytype'"):
            await self.query("""
                SELECT <anytype>123;
            """)

    async def test_edgeql_casts_illegal_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot cast into generic.*anyscalar'"):
            await self.query("""
                SELECT <anyscalar>123;
            """)

    async def test_edgeql_casts_illegal_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot cast into generic.*anyreal'"):
            await self.query("""
                SELECT <anyreal>123;
            """)

    async def test_edgeql_casts_illegal_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot cast into generic.*anyint'"):
            await self.query("""
                SELECT <anyint>123;
            """)

    async def test_edgeql_casts_illegal_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast.*'):
            await self.query("""
                SELECT <anyfloat>123;
            """)

    async def test_edgeql_casts_illegal_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot cast into generic.*sequence'"):
            await self.query("""
                SELECT <sequence>123;
            """)

    async def test_edgeql_casts_illegal_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot cast into generic.*anytype'"):
            await self.query("""
                SELECT <array<anytype>>[123];
            """)

    async def test_edgeql_casts_illegal_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"cannot cast into generic.*'anytype'"):
            await self.query("""
                SELECT <tuple<int64, anytype>>(123, 123);
            """)

    async def test_edgeql_casts_illegal_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"cannot cast.*std::Object.*use.*IS schema::Object.*"):
            await self.query("""
                SELECT <schema::Object>std::Object;
            """)

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
        await self.assert_query_result(r'''
            SELECT <bool><json>True = True;
            SELECT <bool><json>False = False;
            SELECT <str><json>"Hello" = 'Hello';

            WITH U := uuid_generate_v1mc()
            SELECT <uuid><json>U = U;

            SELECT <datetime><json>datetime_of_statement() =
                datetime_of_statement();
            SELECT <naive_datetime><json>to_naive_datetime(
                    '2018-05-07T20:01:22.306916') =
                to_naive_datetime('2018-05-07T20:01:22.306916');
            SELECT <naive_date><json>to_naive_date('2018-05-07') =
                to_naive_date('2018-05-07');
            SELECT <naive_time><json>to_naive_time('20:01:22.306916') =
                to_naive_time('20:01:22.306916');
            SELECT <timedelta><json>to_timedelta(hours:=20) =
                to_timedelta(hours:=20);

            SELECT <int16><json>to_int16('12345') = 12345;
            SELECT <int32><json>to_int32('1234567890') = 1234567890;
            SELECT <int64><json>to_int64('1234567890123') = 1234567890123;
            SELECT <float32><json>to_float32('2.5') = 2.5;
            SELECT <float64><json>to_float64('2.5') = 2.5;

            SELECT <decimal><json>to_decimal(
                    '123456789123456789123456789.123456789123456789123456789')
                = to_decimal(
                    '123456789123456789123456789.123456789123456789123456789');
        ''', [
            [True],
            [True],
            [True],

            [True],

            [True],
            [True],
            [True],
            [True],
            [True],

            [True],
            [True],
            [True],
            [True],
            [True],

            [True],
        ])

    async def test_edgeql_casts_json_02(self):
        await self.assert_query_result(r'''
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <bool><json>T.p_bool = T.p_bool;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <str><json>T.p_str = T.p_str;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <datetime><json>T.p_datetime = T.p_datetime;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <naive_datetime><json>T.p_naive_datetime =
                T.p_naive_datetime;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <naive_date><json>T.p_naive_date = T.p_naive_date;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <naive_time><json>T.p_naive_time = T.p_naive_time;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <timedelta><json>T.p_timedelta = T.p_timedelta;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <int16><json>T.p_int16 = T.p_int16;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <int32><json>T.p_int32 = T.p_int32;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <int64><json>T.p_int64 = T.p_int64;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <float32><json>T.p_float32 = T.p_float32;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <float64><json>T.p_float64 = T.p_float64;
            WITH T := (SELECT test::Test FILTER .p_str = 'Hello')
            SELECT <decimal><json>T.p_decimal = T.p_decimal;
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_casts_json_03(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <bool>J.j_bool = T.p_bool;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <str>J.j_str = T.p_str;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <datetime>J.j_datetime = T.p_datetime;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <naive_datetime>J.j_naive_datetime = T.p_naive_datetime;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <naive_date>J.j_naive_date = T.p_naive_date;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <naive_time>J.j_naive_time = T.p_naive_time;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <timedelta>J.j_timedelta = T.p_timedelta;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <int16>J.j_int16 = T.p_int16;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <int32>J.j_int32 = T.p_int32;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <int64>J.j_int64 = T.p_int64;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <float32>J.j_float32 = T.p_float32;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <float64>J.j_float64 = T.p_float64;

            WITH
                MODULE test,
                T := (SELECT Test FILTER .p_str = 'Hello'),
                J := (SELECT JSONTest FILTER .j_str = <json>'Hello')
            SELECT <decimal>J.j_decimal = T.p_decimal;
        ''', [
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
            [True],
        ])
