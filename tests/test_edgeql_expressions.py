##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

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

    async def test_edgeql_expr_op01(self):
        await self.assert_query_result(r"""
            SELECT 40 + 2;
            SELECT 40 - 2;
            SELECT 40 * 2;
            SELECT 40 / 2;
            SELECT 40 % 2;
            """, [
                [42],
                [38],
                [80],
                [20],
                [0],
            ])

    async def test_edgeql_expr_op02(self):
        await self.assert_query_result(r"""
            SELECT 40 ^ 2;
            SELECT 121 ^ 0.5;
            """, [
                [1600],
                [11],
            ])

    async def test_edgeql_expr_op03(self):
        await self.assert_query_result(r"""
            SELECT 40 < 2;
            SELECT 40 > 2;
            SELECT 40 <= 2;
            SELECT 40 >= 2;
            SELECT 40 = 2;
            SELECT 40 != 2;
            """, [
                [False],
                [True],
                [False],
                [True],
                [False],
                [True],
            ])

    async def test_edgeql_expr_op04(self):
        await self.assert_query_result(r"""
            SELECT -1 + 2 * 3 - 5 - 6.0 / 2;
            SELECT
                -1 + 2 * 3 - 5 - 6.0 / 2 > 0
                OR 25 % 4 = 3 AND 42 IN (12, 42, 14);
            SELECT (-1 + 2) * 3 - (5 - 6.0) / 2;
            SELECT
                ((-1 + 2) * 3 - (5 - 6.0) / 2 > 0 OR 25 % 4 = 3)
                AND 42 IN (12, 42, 14);
            """, [
                [-3],
                [False],
                [3.5],
                [True],
            ])

    async def test_edgeql_expr_op05(self):
        await self.assert_query_result(r"""
            SELECT 'foo' + 'bar';
            """, [
                ['foobar'],
            ])

    async def test_edgeql_expr_op06(self):
        await self.assert_query_result(r"""
            SELECT NULL = NULL;
            SELECT NULL = 42;
            SELECT NULL = 'NULL';
            """, [
                [None],
                [None],
                [None],
            ])

    async def test_edgeql_expr_paths_01(self):
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
                WITH MODULE test
                SELECT
                    Issue {
                        test::number
                    }
                WHERE
                    %s = 'Elvis';
            ''' % (case,))

    async def test_edgeql_expr_polymorphic_01(self):
        await self.con.execute(r"""
            WITH MODULE test
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
            WITH MODULE test
            SELECT Owned {
                Named.name
            };
        """)

    async def test_edgeql_expr_cast01(self):
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

    async def test_edgeql_expr_cast02(self):
        # testing precedence of casting vs. multiplication
        #
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError,
                r'operator does not exist: text \* integer'):
            await self.con.execute("""
                SELECT <std::str>123 * 2;
            """)

    async def test_edgeql_expr_cast03(self):
        await self.assert_query_result(r"""
            SELECT <std::str><std::int><std::float>'123.45' + 'foo';
        """, [
            ['123foo'],
        ])

    async def test_edgeql_expr_cast04(self):
        await self.assert_query_result(r"""
            SELECT <str><int><float>'123.45' + 'foo';
        """, [
            ['123foo'],
        ])

    async def test_edgeql_expr_cast05(self):
        await self.assert_query_result(r"""
            SELECT <list<int>>['123', '11'];
        """, [
            [[123, 11]],
        ])

    @unittest.expectedFailure
    # this is currently a syntax error, but that should be changed in
    # the future
    async def test_edgeql_expr_type01(self):
        await self.assert_query_result(r"""
            SELECT 'foo'.__class__.name;
        """, [
            ['std::str'],
        ])

    async def test_edgeql_expr_list01(self):
        await self.assert_query_result("""
            SELECT [1];
            SELECT [1, 2, 3, 4, 5];
            SELECT [1, 2, 3, 4, 5][2];
            SELECT [1, 2, 3, 4, 5][-2];

            SELECT [1, 2, 3, 4, 5][2:4];
            SELECT [1, 2, 3, 4, 5][2:];
            SELECT [1, 2, 3, 4, 5][:2];

            SELECT [1, 2, 3, 4, 5][2:-1];
            SELECT [1, 2, 3, 4, 5][-2:];
            SELECT [1, 2, 3, 4, 5][:-2];
        """, [
            [[1]],
            [[1, 2, 3, 4, 5]],
            [3],
            [4],

            [[3, 4]],
            [[3, 4, 5]],
            [[1, 2]],

            [[3, 4]],
            [[4, 5]],
            [[1, 2, 3]],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_map01(self):
        await self.assert_query_result(r"""
            SELECT {'foo': 42};
            SELECT {'foo': 42, 'bar': 'something'};
            SELECT {'foo': 42, 'bar': 'something'}['foo'];
        """, [
            [{'foo': 42}],
            [{'foo': 42, 'bar': 'something'}],
            [42],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_coalesce01(self):
        await self.assert_query_result(r"""
            SELECT coalesce(NULL, 4, 5);
            SELECT coalesce(NULL, 'foo', 'bar');
            SELECT coalesce(4, NULL, 5);
            SELECT coalesce('foo', NULL, 'bar');
        """, [
            [4],
            ['foo'],
            [4],
            ['foo'],
        ])

    async def test_edgeql_expr_string01(self):
        await self.assert_query_result("""
            SELECT 'qwerty';
            SELECT 'qwerty'[2];
            SELECT 'qwerty'[-2];

            SELECT 'qwerty'[2:4];
            SELECT 'qwerty'[2:];
            SELECT 'qwerty'[:2];

            SELECT 'qwerty'[2:-1];
            SELECT 'qwerty'[-2:];
            SELECT 'qwerty'[:-2];
        """, [
            ['qwerty'],
            ['e'],
            ['t'],

            ['er'],
            ['erty'],
            ['qw'],

            ['ert'],
            ['ty'],
            ['qwer'],
        ])

    @unittest.expectedFailure
    async def test_edgeql_expr_tuple01(self):
        await self.assert_query_result(r"""
            SELECT (1, 'foo');
        """, [
            [(1, 'foo')],
        ])

    async def test_edgeql_expr_tuple02(self):
        await self.assert_query_result(r"""
            SELECT (1, 'foo') = (1, 'foo');
            SELECT (1, 'foo') = (2, 'foo');
            SELECT (1, 'foo') != (1, 'foo');
            SELECT (1, 'foo') != (2, 'foo');
        """, [
            [True],
            [False],
            [False],
            [True],
        ])

    async def test_edgeql_expr_tuple03(self):
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError, r'unexpected binop operands'):
            await self.con.execute(r"""
                SELECT (1, 2) = [1, 2];
            """)

    async def test_edgeql_expr_tuple04(self):
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError, r'operator does not exist'):
            await self.con.execute(r"""
                SELECT (1, 'foo') = ('1', 'foo');
            """)
