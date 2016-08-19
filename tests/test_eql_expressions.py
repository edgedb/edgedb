##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.lang.common import datetime
from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestExpressions(tb.QueryTestCase):
    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_eql_expression01(self):
        await self.assert_query_result(r"""
            Select 40 + 2;
            select 40 + 2;
            SELECT 40 + 2;
            SeLeCT 40 + 2;
            """, [
                [42],
                [42],
                [42],
                [42],
            ])

    async def test_eql_expression02(self):
        with self.assertRaisesRegex(exc.EdgeQLSyntaxError,
                                    r'Unexpected token.*?"40"'):
            await self.con.execute("""
                40 + 2;
            """)

    async def test_eql_expression03(self):
        with self.assertRaisesRegex(exc.EdgeQLSyntaxError,
                                    r'Unexpected token.*?">"'):
            await self.con.execute("""
                SELECT 40 >> 2;
            """)

    async def test_eql_expression04(self):
        with self.assertRaisesRegex(exc.EdgeQLSyntaxError,
                                    r'Unexpected token.*?"2"'):
            await self.con.execute("""
                SELECT 40 << 2;
            """)

    async def test_eql_expression05(self):
        await self.assert_query_result(r"""
            SELECT 40 >= 2;
            SELECT 40 <= 2;
            """, [
                [True],
                [False],
            ])

    async def test_eql_cast01(self):
        await self.assert_query_result(r"""
            SELECT <std::str>123;
            SELECT <std::int>"123";
            SELECT <std::str>123 + 'qw';
            SELECT <std::int>"123" + 9000;
            SELECT <std::int>"123" * 100;
            SELECT <std::str>(123 * 2);
            """, [
                ['123'],
                [123],
                ['123qw'],
                [9123],
                [12300],
                ['246'],
            ])

    async def test_eql_cast02(self):
        # testing precedence of casting vs. multiplication
        #
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError,
                r'operator does not exist: text \* integer'):
            await self.con.execute("""
                SELECT <std::str>123 * 2;
            """)

    async def test_eql_cast03(self):
        await self.assert_query_result(r"""
            SELECT <std::str><std::int><std::float>'123.45' + 'foo';
            """, [
                ['123foo'],
            ])
