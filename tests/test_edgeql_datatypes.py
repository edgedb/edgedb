#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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


import unittest  # NOQA

from edgedb.server import _testbase as tb
from edgedb.client import exceptions as exc


class TestEdgeQLDT(tb.QueryTestCase):
    SETUP = '''
        CREATE MIGRATION default::m TO eschema $$
            scalar type seq_t extending sequence
            scalar type seq2_t extending sequence

            type Obj:
                property seq_prop -> seq_t

            type Obj2:
                property seq_prop -> seq2_t
        $$;

        COMMIT MIGRATION default::m;
    '''

    async def test_edgeql_dt_datetime_01(self):
        await self.assert_query_result('''
            SELECT <datetime>'2017-10-10' + <timedelta>'1 day';
            SELECT <timedelta>'1 day' + <datetime>'2017-10-10';
            SELECT <datetime>'2017-10-10' - <timedelta>'1 day';
            SELECT <timedelta>'1 day' + <timedelta>'1 day';
            SELECT <timedelta>'4 days' - <timedelta>'1 day';
        ''', [
            ['2017-10-11T00:00:00+00:00'],
            ['2017-10-11T00:00:00+00:00'],
            ['2017-10-09T00:00:00+00:00'],
            ['2 days'],
            ['3 days'],
        ])

        with self.assertRaisesRegex(
            exc.EdgeQLError,
                'operator `-` is not defined for types.*timedelta.*datetime'):

            await self.con.execute("""
                SELECT <timedelta>'1 day' - <datetime>'2017-10-10';
            """)

    async def test_edgeql_dt_datetime_02(self):
        await self.assert_query_result('''
            SELECT <str><datetime>'2017-10-10';
            SELECT <str>(<datetime>'2017-10-10' - <timedelta>'1 day');
        ''', [
            ['2017-10-10T00:00:00+00:00'],
            ['2017-10-09T00:00:00+00:00'],
        ])

    @unittest.expectedFailure
    async def test_edgeql_dt_datetime_03(self):
        await self.assert_query_result('''
            SELECT <tuple<str,datetime>>('foo', '2020-10-10');
            SELECT (<tuple<str,datetime>>('foo', '2020-10-10')).1 +
                   <timedelta>'1 month';
        ''', [
            [{'foo': '2020-10-10T00:00:00+00:00'}],
            ['2020-11-10T00:00:00+00:00'],
        ])

    async def test_edgeql_dt_sequence_01(self):
        await self.assert_query_result('''
            INSERT Obj;
            INSERT Obj;
            INSERT Obj2;
            SELECT Obj { seq_prop } ORDER BY Obj.seq_prop;
            SELECT Obj2 { seq_prop };
        ''', [
            [1],
            [1],
            [1],
            [
                {'seq_prop': 1}, {'seq_prop': 2}
            ],
            [
                {'seq_prop': 1}
            ],
        ])
