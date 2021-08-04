#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file exceptionsept in compliance with the License.
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

from datetime import timedelta

import edgedb
import decimal

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLDT(tb.QueryTestCase):
    SETUP = '''
        START MIGRATION TO {
            module default {
                scalar type seq_t extending sequence;
                scalar type seq2_t extending sequence;
                scalar type enum_t extending enum<'foo', 'bar'>;

                type Obj {
                    property seq_prop -> seq_t;
                };

                type Obj2 {
                    property seq_prop -> seq2_t;
                };
            };
        };
        POPULATE MIGRATION;
        COMMIT MIGRATION;
    '''

    async def test_edgeql_dt_realativedelta(self):
        await self.assert_query_result(
            r"SELECT <cal::relative_duration>'1 year 2 seconds'",
            ['P1YT2S'],
            [edgedb.RelativeDuration(months=12, microseconds=2_000_000)],
        )

        await self.assert_query_result(
            r"SELECT <str><cal::relative_duration>'1 year 2 seconds'",
            ['P1YT2S'],
        )

        await self.assert_query_result(
            r"""
            WITH
                dt := <datetime>'2000-01-01T00:00:00Z',
                rd := <cal::relative_duration>'3 years 2 months 14 days'
            SELECT (dt + rd, rd + dt, dt - rd)
            """,
            [(
                '2003-03-15T00:00:00+00:00',
                '2003-03-15T00:00:00+00:00',
                '1996-10-18T00:00:00+00:00',
            )],
        )

        await self.assert_query_result(
            r"""
            WITH
                dt := <cal::local_datetime>'2000-01-01T00:00:00',
                rd := <cal::relative_duration>'3 years 2 months 14 days'
            SELECT (dt + rd, rd + dt, dt - rd)
            """,
            [(
                '2003-03-15T00:00:00',
                '2003-03-15T00:00:00',
                '1996-10-18T00:00:00',
            )],
        )

        await self.assert_query_result(
            r"""
            WITH
                d := <cal::local_date>'2000-01-01',
                rd := <cal::relative_duration>'3 years 2 months 14 days'
            SELECT (d + rd, rd + d, d - rd)
            """,
            [('2003-03-15', '2003-03-15', '1996-10-18')],
        )

        await self.assert_query_result(
            r"""
            WITH
                t := <cal::local_time>'00:00:00',
                rd := <cal::relative_duration>'3h2m1s'
            SELECT (t + rd, rd + t, t - rd)
            """,
            [('03:02:01', '03:02:01', '20:57:59')],
        )

        await self.assert_query_result(
            r"""
            WITH rd := <cal::relative_duration>'3h2m1s'
            SELECT (
                rd = rd, rd ?= rd,
                rd != rd, rd ?!= rd,
                rd > rd, rd >= rd,
                rd < rd, rd <= rd,
                rd + rd, rd - rd,
                -rd,
            )
            """,
            [(
                True, True,
                False, False,
                False, True,
                False, True,
                'PT6H4M2S', 'PT0S',
                'PT-3H-2M-1S',
            )],
            [(
                True, True,
                False, False,
                False, True,
                False, True,
                edgedb.RelativeDuration(microseconds=21_842_000_000),
                edgedb.RelativeDuration(),
                edgedb.RelativeDuration(microseconds=-10_921_000_000),
            )],
        )

        await self.assert_query_result(
            r" SELECT <json><cal::relative_duration>'3y2h' ",
            ['P3YT2H'],
            ['"P3YT2H"'],
        )

        await self.assert_query_result(
            r" SELECT <cal::relative_duration><json>'P3YT2H' ",
            ['P3YT2H'],
            [edgedb.RelativeDuration(months=36, microseconds=7200000000)],
        )

        await self.assert_query_result(
            r"""
            SELECT (
                to_str(
                    <cal::relative_duration>'3y' +
                    <cal::relative_duration>'1h'
                ),
                to_str(<cal::relative_duration>'3y1h', 'YYYY"y"HH24"h"'),
            )
            """,
            [['P3YT1H', '0003y01h']],
        )

        await self.assert_query_result(
            r"""
            SELECT cal::to_relative_duration(
                years := 1,
                months := 2,
                days := 3,
                hours := 4,
                minutes := 5,
                seconds := 6,
                microseconds := 7,
            )
            """,
            ['P1Y2M3DT4H5M6.000007S'],
            [edgedb.RelativeDuration(months=14, days=3,
                                     microseconds=14706000007)],
        )

        await self.assert_query_result(
            r"""
            WITH
                x := <cal::relative_duration>'1y',
                y := <cal::relative_duration>'5y',
            SELECT (
                max({x, y}),
                min({x, y}),
            )
            """,
            [['P5Y', 'P1Y']],
            [(
                edgedb.RelativeDuration(months=60),
                edgedb.RelativeDuration(months=12),
            )]
        )

        await self.assert_query_result(
            r"""
            WITH
                rd := <cal::relative_duration>'1s',
                d := <duration>'5s',
            SELECT (<duration>rd, <cal::relative_duration>d)
            """,
            [['PT1S', 'PT5S']],
            [(
                timedelta(seconds=1),
                edgedb.RelativeDuration(microseconds=5_000_000),
            )]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'value for domain duration_t violates .* "duration_t_check"'):
            await self.con.query(r"""
                WITH rd := <cal::relative_duration>'1y'
                SELECT <duration>rd
                """)

    async def test_edgeql_dt_datetime_01(self):
        await self.assert_query_result(
            r'''SELECT <datetime>'2017-10-10T00:00:00+00' +
                <duration>'24 hours';''',
            ['2017-10-11T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <duration>'24 hours' +
                <datetime>'2017-10-10 00:00:00+00';''',
            ['2017-10-11T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <datetime>'2017-10-10T00:00:00+00' -
                <duration>'24 hours';''',
            ['2017-10-09T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT to_str(<duration>'24 hours' + <duration>'24 hours')''',
            ['PT48H'],
        )

        await self.assert_query_result(
            r'''SELECT to_str(<duration>'4 hours' - <duration>'1 hour')''',
            ['PT3H'],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "operator '-' cannot be applied.*duration.*datetime"):

            await self.con.query("""
                SELECT <duration>'1 hour' - <datetime>'2017-10-10T00:00:00+00';
            """)

    async def test_edgeql_dt_datetime_02(self):
        await self.assert_query_result(
            r'''SELECT <str><datetime>'2017-10-10T00:00:00+00';''',
            ['2017-10-10T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <str>(<datetime>'2017-10-10T00:00:00+00' -
                             <duration>'24 hours');
            ''',
            ['2017-10-09T00:00:00+00:00'],
        )

    async def test_edgeql_dt_datetime_03(self):
        await self.assert_query_result(
            r'''SELECT <tuple<str,datetime>>(
                'foo', '2017-10-10T00:00:00+00');
            ''',
            [['foo', '2017-10-10T00:00:00+00:00']],
        )

        await self.assert_query_result(
            r'''
                SELECT (<tuple<str,datetime>>(
                    'foo', '2017-10-10T00:00:00+00')).1 +
                   <duration>'744 hours';
            ''',
            ['2017-11-10T00:00:00+00:00'],
        )

    async def test_edgeql_dt_datetime_04(self):
        await self.assert_query_result(
            r'''SELECT <datetime>'2017-10-11T00:00:00+00' -
                <datetime>'2017-10-10T00:00:00+00';''',
            ['PT24H'],
            [timedelta(days=1)],
        )

        await self.assert_query_result(
            r'''SELECT <datetime>'2018-10-10T00:00:00+00' -
                <datetime>'2017-10-10T00:00:00+00';''',
            ['PT8760H'],
            [timedelta(days=365)],
        )

        await self.assert_query_result(
            r'''SELECT <datetime>'2017-10-17T01:02:03.004005+00' -
                <datetime>'2017-10-10T00:00:00+00';''',
            ['PT169H2M3.004005S'],
            [timedelta(days=7, seconds=3723, microseconds=4005)],
        )

        await self.assert_query_result(
            r'''SELECT <datetime>'2017-10-10T01:02:03.004005-02' -
                <datetime>'2017-10-10T00:00:00+00';''',
            ['PT3H2M3.004005S'],
            [timedelta(seconds=10923, microseconds=4005)],
        )

    async def test_edgeql_dt_duration_01_err(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                "invalid input syntax for type std::duration: '7 days'"):
            await self.con.execute("SELECT <duration>'7 days';")

    async def test_edgeql_dt_duration_02_err(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                "invalid input syntax for type std::duration: '13 months'"):
            await self.con.execute("SELECT <duration>'13 months';")

    async def test_edgeql_dt_duration_03_err(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                "invalid input syntax for type std::duration: '17 years'"):
            await self.con.execute("SELECT <duration>'17 years';")

    async def test_edgeql_dt_duration_04_err(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                "invalid input syntax for type std::duration: "
                "'100 centuries'"):
            await self.con.execute("SELECT <duration>'100 centuries';")

    async def test_edgeql_dt_duration_05_err(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid input syntax for type std::duration: "100 cats"'):
            await self.con.execute("SELECT <duration>'100 cats';")

    async def test_edgeql_dt_duration_06_interval_style(self):
        await self.assert_query_result(
            r'''SELECT <duration>'-6h51m14.045854s';''',
            ['PT-5H-8M-45.954146S'],
            [-timedelta(seconds=18525, microseconds=954146)],
        )

        await self.assert_query_result(
            r'''SELECT <duration>'-6h -51m -14.045854s';''',
            ['PT-6H-51M-14.045854S'],
            [-timedelta(seconds=24674, microseconds=45854)],
        )

    async def test_edgeql_dt_duration_07_datetime_range(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT <datetime>'9999-12-31T00:00:00Z' + <duration>'30 hours'
                """
            )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT <datetime>'0001-01-01T00:00:00Z' - <duration>'30 hours'
                """
            )

    async def test_edgeql_dt_duration_08_local_datetime_range(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT
                    <cal::local_datetime>'9999-12-31T00:00:00'
                    + <duration>'30 hours'
                """
            )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT
                    <cal::local_datetime>'0001-01-01T00:00:00'
                    - <duration>'30 hours'
                """
            )

    async def test_edgeql_dt_duration_09_local_date_range(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT
                    <cal::local_date>'9999-12-31'
                    + <duration>'30 hours'
                """
            )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT
                    <cal::local_date>'0001-01-01'
                    - <duration>'30 hours'
                """
            )

    async def test_edgeql_dt_local_datetime_01(self):
        await self.assert_query_result(
            r'''
                SELECT <cal::local_datetime>'2017-10-10T13:11' +
                    <duration>'24 hours';
            ''',
            ['2017-10-11T13:11:00'],
        )

        await self.assert_query_result(
            r'''
                SELECT <duration>'24 hours' +
                    <cal::local_datetime>'2017-10-10T13:11';
            ''',
            ['2017-10-11T13:11:00'],
        )

        await self.assert_query_result(
            r'''
                SELECT <cal::local_datetime>'2017-10-10T13:11' -
                    <duration>'24 hours';
            ''',
            ['2017-10-09T13:11:00'],
        )

    @test.not_implemented('local_datetime diff is cal::relative_duration')
    async def test_edgeql_dt_local_datetime_02(self):
        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2017-10-11T00:00:00' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['24:00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2018-10-10T00:00:00' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['8760:00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2017-10-17T01:02:03.004005' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['169:02:03.004005'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2017-10-10T01:02:03.004005' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['01:02:03.004005'],
        )

    async def test_edgeql_dt_local_date_01(self):
        await self.assert_query_result(
            r'''SELECT
                    <cal::local_date>'2017-10-10' + <duration>'24 hours';
            ''',
            ['2017-10-11'],
        )

        await self.assert_query_result(
            r'''SELECT
                <duration>'24 hours' + <cal::local_date>'2017-10-10';
            ''',
            ['2017-10-11'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_date>'2017-10-10' - <duration>'24 hours';
            ''',
            ['2017-10-09'],
        )

    @test.not_implemented(
        'local date diff should return cal::relative_duration')
    async def test_edgeql_dt_local_date_02(self):
        await self.assert_query_result(
            r'''SELECT <cal::local_date>'2017-10-11' -
                <cal::local_date>'2017-10-10';''',
            ['24:00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_date>'2018-10-10' -
                <cal::local_date>'2017-10-10';''',
            ['8760:00:00'],
        )

    @test.not_implemented('local_time diff is cal::relative_duration')
    async def test_edgeql_dt_local_time_01(self):
        await self.assert_query_result(
            r'''SELECT <cal::local_time>'10:01:01' + <duration>'24 hours';''',
            ['10:01:01'],
        )

        await self.assert_query_result(
            r'''SELECT <duration>'1 hour' + <cal::local_time>'10:01:01';''',
            ['11:01:01'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_time>'10:01:01' - <duration>'1 hour';''',
            ['09:01:01'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_time>'01:02:03.004005' -
                <cal::local_time>'00:00:00';''',
            ['01:02:03.004005'],
        )

    async def test_edgeql_dt_sequence_01(self):
        await self.con.execute(
            r'''
                INSERT Obj;
                INSERT Obj;
                INSERT Obj2;
            '''
        )

        await self.assert_query_result(
            r'''SELECT Obj { seq_prop } ORDER BY Obj.seq_prop;''',
            [
                {'seq_prop': 1}, {'seq_prop': 2}
            ],
        )

        await self.assert_query_result(
            r'''SELECT Obj2 { seq_prop };''',
            [
                {'seq_prop': 1}
            ],
        )

    async def test_edgeql_dt_enum_01(self):
        await self.assert_query_result(
            r'''
                SELECT <enum_t>'foo' = <enum_t>'bar'
            ''',
            [
                False
            ],
        )

        await self.assert_query_result(
            r'''
                SELECT <enum_t>'foo' = <enum_t>'foo'
            ''',
            [
                True
            ],
        )

        await self.assert_query_result(
            r'''
                SELECT <enum_t>'foo' < <enum_t>'bar'
            ''',
            [
                True
            ],
        )

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                'invalid input value for enum \'default::enum_t\': "bad"'):
            await self.con.execute(
                'SELECT <enum_t>"bad";'
            )

    async def test_edgeql_dt_bigint_01(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            'invalid syntax for std::bigint'
        ):
            await self.con.execute(
                r'''
                    SELECT <bigint>'NaN'
                '''
            )

    async def test_edgeql_dt_bigint_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "invalid value for scalar type 'std::bigint'",
        ):
            await self.con.execute(
                r'''
                    SELECT <bigint><float64>'NaN'
                '''
            )

    async def test_edgeql_dt_decimal_01(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "invalid value for std::decimal",
        ):
            await self.con.execute(
                r'''
                    SELECT <decimal><float64>'NaN'
                '''
            )

    async def test_edgeql_dt_decimal_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "invalid value for std::decimal",
        ):
            await self.con.execute(
                r'''
                    SELECT <decimal><float64>'Infinity'
                '''
            )

    async def test_edgeql_dt_decimal_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "invalid value for std::decimal",
        ):
            await self.con.execute(
                r'''
                    SELECT <decimal><float64>'-Infinity'
                '''
            )

    async def test_edgeql_dt_decimal_04(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "invalid value for std::decimal",
        ):
            await self.con.execute(
                r'''
                    SELECT <decimal>(<float64>'Infinity' / <float64>'Infinity')
                '''
            )

    async def test_edgeql_dt_decimal_05(self):
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 1e100n).name''',
            ['std::bigint'],
        )
        await self.assert_query_result(
            r'''SELECT (INTROSPECT TYPEOF 1.0e100n).name''',
            ['std::decimal'],
        )
        await self.assert_query_result(
            r'''SELECT 1e100n''',
            [10**100],
        )
        await self.assert_query_result(
            r'''SELECT 1.0e100n''',
            [10.0**100],
            [decimal.Decimal('1e100')],
        )

    async def test_edgeql_named_tuple_typing_01(self):
        await self.con.execute(r"""
            CREATE TYPE Foo { CREATE PROPERTY x -> tuple<a: int64, b: int64> };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "invalid target for property 'x' of object type "
                "'default::Foo': 'tuple<b: std::int64, a: std::int64>' "
                "\\(expecting 'tuple<a: std::int64, b: std::int64>'"):
            await self.con.execute("INSERT Foo { x := (b := 1, a := 2) };")

    async def test_edgeql_named_tuple_typing_02(self):
        await self.assert_query_result(
            r'''SELECT (b := 1, a := 2) UNION (a := 3, b := 4)''',
            [[1, 2], [3, 4]],
            sort=True,
        )

    async def test_edgeql_named_tuple_typing_03(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "named tuple has duplicate field 'a'"):
            await self.con.execute("SELECT (a := 1, a := 2);")

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "named tuple has duplicate field 'a'"):
            await self.con.execute("""
                CREATE TYPE Foo {
                    CREATE PROPERTY x -> tuple<a: int64, a: str>;
                };
            """)
