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


import unittest  # NOQA

from edb.client import exceptions as exc
from edb.server import _testbase as tb


class TestEdgeQLCasts(tb.QueryTestCase):
    # casting into an abstract scalar should be illegal
    async def test_edgeql_casts_illegal_01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLSyntaxError, r"Unexpected 'anytype'"):
            await self.con.execute("""
                SELECT <anytype>123;
            """)

    async def test_edgeql_casts_illegal_02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot cast.*abstract'):
            await self.con.execute("""
                SELECT <anyscalar>123;
            """)

    async def test_edgeql_casts_illegal_03(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot cast.*abstract'):
            await self.con.execute("""
                SELECT <anyreal>123;
            """)

    async def test_edgeql_casts_illegal_04(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot cast.*abstract'):
            await self.con.execute("""
                SELECT <anyint>123;
            """)

    async def test_edgeql_casts_illegal_05(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot cast.*abstract'):
            await self.con.execute("""
                SELECT <anyfloat>123;
            """)

    async def test_edgeql_casts_illegal_06(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot cast.*abstract'):
            await self.con.execute("""
                SELECT <sequence>123;
            """)

    async def test_edgeql_casts_illegal_07(self):
        with self.assertRaisesRegex(
                exc.EdgeQLSyntaxError, r"Unexpected 'anytype'"):
            await self.con.execute("""
                SELECT <array<anytype>>[123];
            """)

    async def test_edgeql_casts_illegal_08(self):
        with self.assertRaisesRegex(
                exc.EdgeQLSyntaxError, r"Unexpected 'anytype'"):
            await self.con.execute("""
                SELECT <tuple<int64, anytype>>(123, 123);
            """)
