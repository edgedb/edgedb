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
import immutables

from edb import errors
from edb.ir import statypes
from edb.server import config
from edb.testbase import server as tb


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

    async def test_edgeql_dt_realativedelta_01(self):
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
            r"SELECT <json><cal::relative_duration><json>'1 year 2 seconds'",
            ['P1YT2S'],
            ['"P1YT2S"'],
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
            [('2003-03-15T00:00:00',
              '2003-03-15T00:00:00',
              '1996-10-18T00:00:00')],
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
                "invalid value for scalar type 'std::duration'"):
            await self.con.query(r"""
                WITH rd := <cal::relative_duration>'1y'
                SELECT <duration>rd
                """)

    async def test_edgeql_dt_realativedelta_02(self):
        await self.assert_query_result(
            r"SELECT <str><cal::date_duration>'1 year 2 days'",
            ['P1Y2D'],
        )

        await self.assert_query_result(
            r"SELECT <json><cal::date_duration><json>'1 year 2 days'",
            ['P1Y2D'],
            ['"P1Y2D"'],
        )

        await self.assert_query_result(
            r"SELECT <str><cal::date_duration>'0 days'",
            ['P0D'],
        )

        await self.assert_query_result(
            r"SELECT <json><cal::date_duration>'0 days'",
            ['P0D'],
            ['"P0D"'],
        )

        await self.assert_query_result(
            r"SELECT <json><cal::date_duration>'5 months -150 days'",
            ['P5M-150D'],
            ['"P5M-150D"'],
        )

        await self.assert_query_result(
            r"""
            WITH
                dt := <datetime>'2000-01-01T00:00:00Z',
                rd := <cal::date_duration>'3 years 2 months 14 days'
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
                rd := <cal::date_duration>'3 years 2 months 14 days'
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
                rd := <cal::date_duration>'3 years 2 months 14 days'
            SELECT (d + rd, rd + d, d - rd)
            """,
            [('2003-03-15', '2003-03-15', '1996-10-18')],
        )

        await self.assert_query_result(
            r" SELECT <json><cal::date_duration>'3y2d' ",
            ['P3Y2D'],
            ['"P3Y2D"'],
        )

        await self.assert_query_result(
            r"""
            SELECT (
                to_str(
                    <cal::date_duration>'3y' +
                    <cal::date_duration>'1d'
                ),
                to_str(<cal::date_duration>'3y1d', 'YYYY"y"DD"d"'),
            )
            """,
            [['P3Y1D', '0003y01d']],
        )

        await self.assert_query_result(
            r"""
            SELECT <str>cal::to_date_duration(
                years := 1,
                months := 2,
                days := 3,
            )
            """,
            ['P1Y2M3D'],
        )

        await self.assert_query_result(
            r"""
            WITH
                x := <cal::date_duration>'1y',
                y := <cal::date_duration>'5y',
            SELECT (
                <str>max({x, y}),
                <str>min({x, y}),
            )
            """,
            [['P5Y', 'P1Y']],
        )

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                "invalid input syntax for type std::cal::date_duration: '1s'"):
            async with self.con.transaction():
                await self.con.query(r"""
                    SELECT <str><cal::date_duration>'1s'
                    """)

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                "invalid input syntax for type std::cal::date_duration: '1s'"):
            async with self.con.transaction():
                await self.con.query(r"""
                    SELECT <str><cal::date_duration><json>'1s'
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

    async def test_edgeql_dt_duration_10_datetime_range(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT <datetime>'9999-12-31T00:00:00Z' +
                    <cal::relative_duration>'1 week'
                """
            )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT <datetime>'0001-01-01T00:00:00Z' -
                    <cal::relative_duration>'1 week'
                """
            )

    async def test_edgeql_dt_duration_11_local_datetime_range(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT <cal::local_datetime>'9999-12-31T00:00:00' +
                    <cal::relative_duration>'1 week'
                """
            )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT <cal::local_datetime>'0001-01-01T00:00:00' -
                    <cal::relative_duration>'1 week'
                """
            )

    async def test_edgeql_dt_duration_12_local_date_range(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'value out of range',
        ):
            await self.con.execute(
                """
                SELECT
                    <cal::local_date>'9999-12-31'
                    + <cal::relative_duration>'30 hours'
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
                    - <cal::relative_duration>'30 hours'
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

    async def test_edgeql_dt_local_datetime_02(self):
        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2017-10-11T00:00:00' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['P1D'],
            [edgedb.RelativeDuration(days=1)],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2018-10-10T00:00:00' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['P365D'],
            [edgedb.RelativeDuration(days=365)],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2017-10-17T01:02:03.004005' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['P7DT1H2M3.004005S'],
            [edgedb.RelativeDuration(days=7, microseconds=3723004005)],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_datetime>'2017-10-10T01:02:03.004005' -
                <cal::local_datetime>'2017-10-10T00:00:00';''',
            ['PT1H2M3.004005S'],
            [edgedb.RelativeDuration(microseconds=3723004005)],
        )

    async def test_edgeql_dt_local_datetime_03(self):
        # Corner case which interprets month in a fuzzy way.
        await self.assert_query_result(
            r'''
            with dur := <cal::relative_duration>'1 month'
            select <cal::local_datetime>'2021-01-31T00:00:00' + dur;
            ''',
            ['2021-02-28T00:00:00'],
        )

        # + not always associative
        await self.assert_query_result(
            r'''
            with
                dur := <cal::relative_duration>'1 month',
                date := <cal::local_datetime>'2021-01-31T00:00:00',
            select date + (dur + dur) = (date + dur) + dur;
            ''',
            [False],
        )

        # - not always inverse of plus
        await self.assert_query_result(
            r'''
            with
                dur := <cal::relative_duration>'1 month',
                date := <cal::local_datetime>'2021-01-31T00:00:00',
            select date + dur - dur = date;
            ''',
            [False],
        )

        # - not always inverse of plus
        await self.assert_query_result(
            r'''
            with
                m1 := <cal::relative_duration>'1 month',
                m11 := <cal::relative_duration>'11 month',
                y1 := <cal::relative_duration>'1 year',
                date := <cal::local_datetime>'2021-01-31T00:00:00',
            select (
                # duration alone
                y1 = m1 + m11,
                # date + duration
                date + y1 = date + m1 + m11,
            );
            ''',
            [[True, False]],
        )

    async def test_edgeql_dt_local_datetime_04(self):
        # Order in which different parts of relative_duration is applied.
        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 hr' +
                <cal::relative_duration>'1 month';''',
            ['2021-06-01T00:59:59'],
        )

        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 month' +
                <cal::relative_duration>'1 hr';''',
            ['2021-05-31T00:59:59'],
        )

        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 hr 1 month';''',
            ['2021-05-31T00:59:59'],
        )

        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 month 1 hr';''',
            ['2021-05-31T00:59:59'],
        )

    async def test_edgeql_dt_local_datetime_05(self):
        # Order in which different parts of relative_duration is applied.
        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 day' +
                <cal::relative_duration>'1 month';''',
            ['2021-06-01T23:59:59'],
        )

        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 month' +
                <cal::relative_duration>'1 day';''',
            ['2021-05-31T23:59:59'],
        )

        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 day 1 month';''',
            ['2021-05-31T23:59:59'],
        )

        await self.assert_query_result(
            r'''select <cal::local_datetime>'2021-04-30T23:59:59' +
                <cal::relative_duration>'1 month 1 day';''',
            ['2021-05-31T23:59:59'],
        )

    async def test_edgeql_dt_local_date_01(self):
        await self.assert_query_result(
            r'''SELECT
                    <cal::local_date>'2017-10-10' + <duration>'24 hours';
            ''',
            ['2017-10-11T00:00:00'],
        )

        await self.assert_query_result(
            r'''SELECT
                <duration>'24 hours' + <cal::local_date>'2017-10-10';
            ''',
            ['2017-10-11T00:00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <cal::local_date>'2017-10-10' - <duration>'24 hours';
            ''',
            ['2017-10-09T00:00:00'],
        )

    async def test_edgeql_dt_local_date_02(self):
        await self.assert_query_result(
            r'''select <str>(
                    <cal::local_date>'2017-10-11' -
                    <cal::local_date>'2017-10-10');''',
            ['P1D'],
        )

        await self.assert_query_result(
            r'''select <str>(
                    <cal::local_date>'2018-10-10' -
                    <cal::local_date>'2017-10-10');''',
            ['P365D'],
        )

    async def test_edgeql_dt_local_date_03(self):
        # Corner case which interprets month in a fuzzy way.
        await self.assert_query_result(
            r'''
            with dur := <cal::date_duration>'1 month'
            select <cal::local_date>'2021-01-31' + dur;
            ''',
            ['2021-02-28'],
        )

        # + not always associative
        await self.assert_query_result(
            r'''
            with
                dur := <cal::date_duration>'1 month',
                date := <cal::local_date>'2021-01-31',
            select date + (dur + dur) = (date + dur) + dur;
            ''',
            [False],
        )

        # - not always inverse of plus
        await self.assert_query_result(
            r'''
            with
                dur := <cal::date_duration>'1 month',
                date := <cal::local_date>'2021-01-31',
            select date + dur - dur = date;
            ''',
            [False],
        )

        # - not always inverse of plus
        await self.assert_query_result(
            r'''
            with
                m1 := <cal::date_duration>'1 month',
                m11 := <cal::date_duration>'11 month',
                y1 := <cal::date_duration>'1 year',
                date := <cal::local_date>'2021-01-31',
            select (
                # duration alone
                y1 = m1 + m11,
                # date + duration
                date + y1 = date + m1 + m11,
            );
            ''',
            [[True, False]],
        )

    async def test_edgeql_dt_local_date_04(self):
        # Order in which different parts of relative_duration is applied.
        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::relative_duration>'1 hr' +
                <cal::relative_duration>'1 month';''',
            ['2021-05-30T01:00:00'],
        )

        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::relative_duration>'1 month' +
                <cal::relative_duration>'1 hr';''',
            ['2021-05-30T01:00:00'],
        )

        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::relative_duration>'1 hr 1 month';''',
            ['2021-05-30T01:00:00'],
        )

        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::relative_duration>'1 month 1 hr';''',
            ['2021-05-30T01:00:00'],
        )

    async def test_edgeql_dt_local_date_05(self):
        # Order in which different parts of date_duration is applied.
        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::date_duration>'1 day' +
                <cal::date_duration>'1 month';''',
            ['2021-06-01'],
        )

        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::date_duration>'1 month' +
                <cal::date_duration>'1 day';''',
            ['2021-05-31'],
        )

        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::date_duration>'1 day 1 month';''',
            ['2021-05-31'],
        )

        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::date_duration>'1 month 1 day';''',
            ['2021-05-31'],
        )

    async def test_edgeql_dt_local_date_06(self):
        # Fractional day values make the result fractional
        await self.assert_query_result(
            r'''select <cal::local_date>'2021-04-30' +
                <cal::relative_duration>'20 hr' +
                <cal::relative_duration>'20 hr';''',
            ['2021-05-01T16:00:00'],
        )

    async def test_edgeql_dt_local_time_01(self):
        await self.assert_query_result(
            r'''select <cal::local_time>'10:01:01' +
                <cal::relative_duration>'24 hours';''',
            ['10:01:01'],
        )

        await self.assert_query_result(
            r'''select <cal::relative_duration>'1 hour' +
                <cal::local_time>'10:01:01';''',
            ['11:01:01'],
        )

        await self.assert_query_result(
            r'''select <cal::local_time>'10:01:01' -
                <cal::relative_duration>'1 hour';''',
            ['09:01:01'],
        )

        await self.assert_query_result(
            r'''select <cal::local_time>'01:02:03.004005' -
                <cal::local_time>'00:00:00';''',
            ['PT1H2M3.004005S'],
            [edgedb.RelativeDuration(microseconds=3723004005)],
        )

        await self.assert_query_result(
            r'''select <cal::local_time>'01:02:03.004005' -
                <cal::local_time>'10:00:00';''',
            ['PT-8H-57M-56.995995S'],
            [edgedb.RelativeDuration(microseconds=-32276995995)],
        )

    async def test_edgeql_dt_sequence_01(self):
        await self.con.execute(
            r'''
                INSERT Obj;
                INSERT Obj;
                INSERT Obj2;
            '''
        )

        try:
            await self.assert_query_result(
                r'''SELECT Obj { seq_prop } ORDER BY Obj.seq_prop;''',
                [
                    {'seq_prop': 1}, {'seq_prop': 2}
                ],
            )
        except AssertionError:
            if self.is_repeat:
                await self.assert_query_result(
                    r'''SELECT Obj { seq_prop } ORDER BY Obj.seq_prop;''',
                    [
                        {'seq_prop': 3}, {'seq_prop': 4}
                    ],
                )
            else:
                raise

        try:
            await self.assert_query_result(
                r'''SELECT Obj2 { seq_prop };''',
                [
                    {'seq_prop': 1},
                ],
            )
        except AssertionError:
            if self.is_repeat:
                await self.assert_query_result(
                    r'''SELECT Obj2 { seq_prop };''',
                    [
                        {'seq_prop': 2},
                    ],
                )
            else:
                raise

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
            'invalid input syntax for type std::bigint'
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

    async def test_edgeql_memory_01(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                "invalid value for scalar type 'cfg::memory'"):
            await self.con.execute("SELECT <cfg::memory>-1")

        self.assertEqual(
            await self.con.query_single("SELECT <int64><cfg::memory>'1KiB'"),
            1024)

        self.assertEqual(
            await self.con.query_single("SELECT <int64><cfg::memory>1025"),
            1025)

        self.assertEqual(
            await self.con.query_single("SELECT <str><cfg::memory>1025"),
            '1025B')

        self.assertEqual(
            await self.con.query_single(
                "SELECT <str><cfg::memory>2272753910888172544"),
            '2219486241101731KiB')

        self.assertEqual(
            await self.con.query_single("SELECT <str><cfg::memory>0"),
            '0B')

        self.assertEqual(
            await self.con.query_single("SELECT <str><cfg::memory>'0B'"),
            '0B')

    async def test_edgeql_staeval_duration_01(self):
        valid = [
            ' 100   ',
            '123',
            '-123',
            '  20 mins 1hr ',
            '  20 mins -1hr ',
            '  20us  1h    20   ',
            '  -20us  1h    20   ',
            '  -20US  1H    20   ',
            '1 hour 20 minutes 30 seconds 40 milliseconds 50 microseconds',
            '1 hour 20 minutes +30seconds 40 milliseconds -50microseconds',
            '1 houR  20 minutes 30SECOND 40 milliseconds 50 us',
            '  20 us 1H 20 minutes ',
            '-1h',
            '100h',
            '   12:12:12.2131   ',
            '-12:12:12.21313',
            '-12:12:12.213134',
            '-12:12:12.2131341',
            '-12:12:12.2131341111111',
            '-12:12:12.2131315111111',
            '-12:12:12.2131316111111',
            '-12:12:12.2131314511111',
            '-0:12:12.2131',
            '12:12',
            '-12:12',
            '-12:1:1',
            '+12:1:1',
            '-12:1:1.1234',
            '1211:59:59.9999',
            '-12:',
            '0',
            '00:00:00',
            '00:00:10.9',
            '00:00:10.09',
            '00:00:10.009',
            '00:00:10.0009',
        ]

        invalid = [
            'blah',
            '!',
            '-',
            '  20 us 1H 20 30 minutes ',
            '   12:12:121.2131   ',
            '   12:60:21.2131   ',
            '  20us 20   1h       ',
            '  20us $ 20   1h       ',
            '1 houR  20 minutes 30SECOND 40 milliseconds 50 uss',
        ]

        v = await self.con.query_single(
            '''
            SELECT <array<duration>><array<str>>$0
            ''',
            valid
        )
        vs = await self.con.query_single(
            '''
            SELECT <array<str>><array<duration>><array<str>>$0
            ''',
            valid
        )

        for text, value, svalue in zip(valid, v, vs):
            ref_value = int(value / timedelta(microseconds=1))

            try:
                parsed = statypes.Duration(text)
            except Exception:
                raise AssertionError(
                    f'could not parse a valid std::duration: {text!r}')

            self.assertEqual(
                ref_value,
                parsed.to_microseconds(),
                text)

            self.assertEqual(
                svalue,
                parsed.to_iso8601(),
                text)

            self.assertEqual(
                statypes.Duration.from_iso8601(svalue).to_microseconds(),
                parsed.to_microseconds(),
                text)

            self.assertEqual(
                statypes.Duration(svalue).to_microseconds(),
                parsed.to_microseconds(),
                text)

        for text in invalid:
            async with self.assertRaisesRegexTx(
                    edgedb.InvalidValueError,
                    r'(invalid input syntax)|(interval field value out)'):
                await self.con.query_single(
                    '''SELECT <duration><str>$0''',
                    text
                )

            with self.assertRaises(
                    (errors.InvalidValueError, errors.NumericOutOfRangeError)):
                statypes.Duration(text)

    async def test_edgeql_staeval_memory_01(self):
        valid = [
            '0',
            '0B',
            '123KiB',
            '11MiB',
            '0PiB',
            '1PiB',
            '111111GiB',
            '123B',
            '2219486241101731KiB',
        ]

        invalid = [
            '12kB',
            '22KB',
            '-1B',
            '-1',
            '+1',
            '+12TiB',
            '123TIB',
        ]

        v = await self.con.query_single(
            '''
            SELECT  <array<int64>><array<cfg::memory>><array<str>>$0
            ''',
            valid
        )
        vs = await self.con.query_single(
            '''
            SELECT <array<str>><array<cfg::memory>><array<str>>$0
            ''',
            valid
        )

        for text, ref_value, svalue in zip(valid, v, vs):
            try:
                parsed = statypes.ConfigMemory(text)
            except Exception:
                raise AssertionError(
                    f'could not parse a valid cfg::memory: {text!r}')

            self.assertEqual(
                ref_value,
                parsed.to_nbytes(),
                text)

            self.assertEqual(
                svalue,
                parsed.to_str(),
                text)

            self.assertEqual(
                statypes.ConfigMemory(svalue).to_nbytes(),
                parsed.to_nbytes(),
                text)

        for text in invalid:
            async with self.assertRaisesRegexTx(
                    edgedb.InvalidValueError,
                    r'(unsupported memory size)|(unable to parse memory)'):
                await self.con.query_single(
                    '''SELECT <int64><cfg::memory><str>$0''',
                    text
                )

            with self.assertRaises(errors.InvalidValueError):
                statypes.ConfigMemory(text)

    async def test_edgeql_as_cache_key(self):
        def make_setting_value(name, value):
            return config.SettingValue(
                name, value, "session", config.ConfigScope.SESSION
            )

        def make_key():
            return immutables.Map(
                dict(
                    duration=make_setting_value(
                        "duration", statypes.Duration("123")
                    ),
                    memory=make_setting_value(
                        "memory", statypes.ConfigMemory("11MiB")
                    ),
                )
            )

        cache = {make_key(): True}
        self.assertIn(make_key(), cache)
