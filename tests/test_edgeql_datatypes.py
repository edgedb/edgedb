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


import edgedb

from edb.testbase import server as tb


class TestEdgeQLDT(tb.QueryTestCase):
    SETUP = '''
        CREATE MIGRATION mig TO {
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

        COMMIT MIGRATION mig;
    '''

    async def test_edgeql_dt_datetime_01(self):
        await self.assert_query_result(
            r'''SELECT <datetime>'2017-10-10T00:00:00+00' +
                <duration>'1 day';''',
            ['2017-10-11T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <duration>'1 day' +
                <datetime>'2017-10-10 00:00:00+00';''',
            ['2017-10-11T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <datetime>'2017-10-10T00:00:00+00' -
                <duration>'1 day';''',
            ['2017-10-09T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT to_str(<duration>'1 day' + <duration>'1 day')''',
            ['2 days'],
        )

        await self.assert_query_result(
            r'''SELECT to_str(<duration>'4 days' - <duration>'1 day')''',
            ['3 days'],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "operator '-' cannot be applied.*duration.*datetime"):

            await self.con.fetchall("""
                SELECT <duration>'1 day' - <datetime>'2017-10-10T00:00:00+00';
            """)

    async def test_edgeql_dt_datetime_02(self):
        await self.assert_query_result(
            r'''SELECT <str><datetime>'2017-10-10T00:00:00+00';''',
            ['2017-10-10T00:00:00+00:00'],
        )

        await self.assert_query_result(
            r'''SELECT <str>(<datetime>'2017-10-10T00:00:00+00' -
                             <duration>'1 day');
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
                   <duration>'1 month';
            ''',
            ['2017-11-10T00:00:00+00:00'],
        )

    async def test_edgeql_dt_local_datetime_01(self):
        await self.assert_query_result(
            r'''
                SELECT <local_datetime>'2017-10-10T13:11' +
                    <duration>'1 day';
            ''',
            ['2017-10-11T13:11:00'],
        )

        await self.assert_query_result(
            r'''
                SELECT <duration>'1 day' +
                    <local_datetime>'2017-10-10T13:11';
            ''',
            ['2017-10-11T13:11:00'],
        )

        await self.assert_query_result(
            r'''
                SELECT <local_datetime>'2017-10-10T13:11' -
                    <duration>'1 day';
            ''',
            ['2017-10-09T13:11:00'],
        )

    async def test_edgeql_dt_local_date_01(self):
        await self.assert_query_result(
            r'''SELECT <local_date>'2017-10-10' + <duration>'1 day';''',
            ['2017-10-11'],
        )

        await self.assert_query_result(
            r'''SELECT <duration>'1 day' + <local_date>'2017-10-10';''',
            ['2017-10-11'],
        )

        await self.assert_query_result(
            r'''SELECT <local_date>'2017-10-10' - <duration>'1 day';''',
            ['2017-10-09'],
        )

    async def test_edgeql_dt_local_time_01(self):
        await self.assert_query_result(
            r'''SELECT <local_time>'10:01:01' + <duration>'1 hour';''',
            ['11:01:01'],
        )

        await self.assert_query_result(
            r'''SELECT <duration>'1 hour' + <local_time>'10:01:01';''',
            ['11:01:01'],
        )

        await self.assert_query_result(
            r'''SELECT <local_time>'10:01:01' - <duration>'1 hour';''',
            ['09:01:01'],
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
