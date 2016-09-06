##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.lang.common import datetime
from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestExpressions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'queries.eschema')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_expression01(self):
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

    async def test_edgeql_expression02(self):
        await self.assert_query_result(r"""
            SELECT 40 >= 2;
            SELECT 40 <= 2;
            SELECT 1 + 2 * 3;
            SELECT (1 + 2) * 3;
            """, [
                [True],
                [False],
                [7],
                [9]
            ])

    async def test_edgeql_paths_01(self):
        cases = [
            "Issue.owner.name",
            "`Issue`.`owner`.`name`",
            "Issue.(test::owner).name",
            "`Issue`.(`test`::`owner`).`name`",
            "Issue.(owner).(name)",
            "test::`Issue`.(`test`::`owner`).`name`",
            "Issue.((owner)).(((test::name)))",
        ]

        for case in cases:
            await self.con.execute('''
                USING MODULE test
                SELECT
                    Issue {
                        test::number
                    }
                WHERE
                    %s = 'Elvis';
            ''' % (case,))

    async def test_edgeql_polymorphic_01(self):
        await self.con.execute(r"""
            USING MODULE test
            SELECT Text {
                Issue.number,
                (Issue).related_to,
                (Issue).((`priority`)),
                test::Comment.owner: {
                    name
                }
            };
        """)

        await self.con.execute(r"""
            USING MODULE test
            SELECT Owned {
                Named.name
            };
        """)

    async def test_edgeql_cast01(self):
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

    async def test_edgeql_cast02(self):
        # testing precedence of casting vs. multiplication
        #
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError,
                r'operator does not exist: text \* integer'):
            await self.con.execute("""
                SELECT <std::str>123 * 2;
            """)

    async def test_edgeql_cast03(self):
        await self.assert_query_result(r"""
            SELECT <std::str><std::int><std::float>'123.45' + 'foo';
            """, [
                ['123foo'],
            ])
