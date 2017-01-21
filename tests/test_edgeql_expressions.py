##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestExpressions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'queries.eschema')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_expr_emptyset_01(self):
        await self.assert_query_result(r"""
            SELECT <int>NULL;
            SELECT <str>NULL;
            SELECT NULL + 1;
            SELECT 1 + NULL;
        """, [
            [None],
            [None],
            [None],
            [None],
        ])

        with self.assertRaisesRegex(exc.EdgeQLError,
                                    r'could not determine expression type'):

            await self.con.execute("""
                SELECT NULL;
            """)

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

    async def test_edgeql_expr_op07(self):
        await self.assert_query_result(r"""
            SELECT EXISTS NULL;
            SELECT NOT EXISTS NULL;
        """, [
            [False],
            [True],
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
            SELECT <int>true;
            SELECT <int>false;
        """, [
            ['123'],
            [123],
            ['123qw'],
            [9123],
            [12300],
            ['246'],
            [1],
            [0],
        ])

    async def test_edgeql_expr_cast02(self):
        # testing precedence of casting vs. multiplication
        #
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'operator does not exist: std::str \* std::int'):
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
            SELECT <array<int>>['123', '11'];
        """, [
            [[123, 11]],
        ])

    async def test_edgeql_expr_cast06(self):
        await self.assert_query_result(r"""
            SELECT <array<bool>>['t', 'tr', 'tru', 'true'];
            SELECT <array<bool>>['T', 'TR', 'TRU', 'TRUE'];
            SELECT <array<bool>>['True', 'TrUe', '1'];
            SELECT <array<bool>>['y', 'ye', 'yes'];
            SELECT <array<bool>>['Y', 'YE', 'YES'];
            SELECT <array<bool>>['Yes', 'yEs', 'YeS'];
        """, [
            [[True, True, True, True]],
            [[True, True, True, True]],
            [[True, True, True]],
            [[True, True, True]],
            [[True, True, True]],
            [[True, True, True]],
        ])

    async def test_edgeql_expr_cast07(self):
        await self.assert_query_result(r"""
            SELECT <array<bool>>['f', 'fa', 'fal', 'fals', 'false'];
            SELECT <array<bool>>['F', 'FA', 'FAL', 'FALS', 'FALSE'];
            SELECT <array<bool>>['False', 'FaLSe', '0'];
            SELECT <array<bool>>['n', 'no'];
            SELECT <array<bool>>['N', 'NO'];
            SELECT <array<bool>>['No', 'nO'];
        """, [
            [[False, False, False, False, False]],
            [[False, False, False, False, False]],
            [[False, False, False]],
            [[False, False]],
            [[False, False]],
            [[False, False]],
        ])

    async def test_edgeql_expr_type01(self):
        await self.assert_query_result(r"""
            SELECT 'foo'.__class__.name;
        """, [
            ['std::str'],
        ])

    async def test_edgeql_expr_type02(self):
        await self.assert_query_result(r"""
            SELECT (1.0 + 2).__class__.name;
        """, [
            ['std::float'],
        ])

    async def test_edgeql_expr_array01(self):
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

            SELECT [1, 2][10] ?? 42;
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

            [42],
        ])

    async def test_edgeql_expr_array02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'could not determine array type'):

            await self.con.execute("""
                SELECT [1, '1'];
            """)

    async def test_edgeql_expr_array03(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index array by.*str'):

            await self.con.execute("""
                SELECT [1, 2]['1'];
            """)

    async def test_edgeql_expr_array04(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'could not determine type of empty array'):

            await self.con.execute("""
                SELECT [];
            """)

    async def test_edgeql_expr_map01(self):
        await self.assert_query_result(r"""
            SELECT {'foo': 42};
            SELECT {'foo': '42', 'bar': 'something'};
            SELECT {'foo': '42', 'bar': 'something'}['foo'];

            SELECT {'foo': '42', 'bar': 'something'}[lower('FO') + 'o'];
            SELECT '+/-' + {'foo': '42', 'bar': 'something'}['foo'];
            SELECT {'foo': 42}['foo'] + 1;

            SELECT {'a': <datetime>'2017-10-10'}['a'] + <timedelta>'1 day';
            SELECT {100: 42}[100];
            SELECT {'1': '2'}['spam'] ?? 'ham';
        """, [
            [{'foo': 42}],
            [{'foo': '42', 'bar': 'something'}],
            ['42'],

            ['42'],
            ['+/-42'],
            [43],

            ['2017-10-11T00:00:00+00:00'],
            [42],
            ['ham'],
        ])

    async def test_edgeql_expr_map02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'could not determine map values type'):

            await self.con.execute(r'''
                SELECT {'a': 'b', '1': 1};
            ''')

        with self.assertRaisesRegex(
                exc.EdgeQLError, r'operator does not exist: .*str.*int'):

            await self.con.execute(r'''
                SELECT {'a': '1'}['a'] + 1;
            ''')

    async def test_edgeql_expr_map03(self):
        await self.con.execute('''
            CREATE FUNCTION test::take(std::map<std::str, std::int>, std::str)
                RETURNING std::int
                FROM EdgeQL $$
                    SELECT $1[$2] + 100
                $$;

            CREATE FUNCTION test::make(std::int)
                RETURNING std::map<std::str, std::int>
                FROM EdgeQL $$
                    SELECT {'aaa': $1}
                $$;
        ''')

        await self.assert_query_result(r"""
            SELECT test::take({'foo': 42}, 'foo') + 1;
            SELECT test::make(1000)['aaa'] + 8000;
        """, [
            [143],
            [9000],
        ])

    async def test_edgeql_expr_map04(self):
        await self.assert_query_result(r"""
            SELECT <map<str, datetime>>{'foo': '2020-10-10'};
            SELECT (<map<int,int>>{'+1':'+42'})[1];  # '+1'::bigint = 1
            SELECT (<map<datetime, datetime>>{'2020-10-10': '2010-01-01'})
                   [<datetime>'2020-10-10'];
            SELECT (<map<int,int>>{true:'+42'})[1];
            SELECT (<map<bool,int>>(<map<int,str>>{true:142}))[true];
        """, [
            [{'foo': '2020-10-10T00:00:00+00:00'}],
            [42],
            ['2010-01-01T00:00:00+00:00'],
            [42],
            [142],
        ])

        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index map.*by.*str.*int.*expected'):

            await self.con.execute(r'''
                SELECT {1:1}['1'];
            ''')

    async def test_edgeql_expr_coalesce01(self):
        await self.assert_query_result(r"""
            SELECT NULL ?? 4 ?? 5;
            SELECT NULL ?? 'foo' ?? 'bar';
            SELECT 4 ?? NULL ?? 5;

            SELECT 'foo' ?? NULL ?? 'bar';
            SELECT NULL ?? 'bar' = 'bar';

            SELECT 4^NULL ?? 2;
            SELECT 4+NULL ?? 2;
            SELECT 4*NULL ?? 2;

            SELECT -<int>NULL ?? 2;
            SELECT -<int>NULL ?? -2 + 1;

            SELECT <int>(NULL ?? NULL);
            SELECT <int>(NULL ?? NULL ?? NULL);
        """, [
            [4],
            ['foo'],
            [4],

            ['foo'],
            [True],

            [2],  # ^ binds more tightly
            [6],
            [8],

            [2],
            [-1],

            [None],
            [None],
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

    async def test_edgeql_expr_string02(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot index string by.*str'):

            await self.con.execute("""
                SELECT '123'['1'];
            """)

    async def test_edgeql_expr_tuple01(self):
        await self.assert_query_result(r"""
            SELECT (1, 'foo');
        """, [
            [[1, 'foo']],
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
                exc._base.UnknownEdgeDBError, r'operator does not exist'):
            await self.con.execute(r"""
                SELECT (1, 2) = [1, 2];
            """)

    async def test_edgeql_expr_tuple04(self):
        with self.assertRaisesRegex(
                exc._base.UnknownEdgeDBError, r'operator does not exist'):
            await self.con.execute(r"""
                SELECT (1, 'foo') = ('1', 'foo');
            """)

    async def test_edgeql_expr_cannot_assign_dunder_class(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError, r'cannot assign to __class__'):
            await self.con.execute(r"""
                SELECT test::Text {
                    std::__class__ := 42
                };
            """)

    async def test_edgeql_expr_if_else_01(self):
        await self.assert_query_result(r"""
            SELECT 'yes' IF 1=1 ELSE 'no';
            SELECT 'yes' IF 1=0 ELSE 'no';
            SELECT 's1' IF 1=0 ELSE 's2' IF 2=2 ELSE 's3';
        """, [
            ['yes'],
            ['no'],
            ['s2'],
        ])

    async def test_edgeql_expr_select(self):
        await self.assert_query_result(r"""
            SELECT 2 * (SELECT 1 UNION SELECT 2);

            SELECT (SELECT 2) * (SELECT 1 UNION SELECT 2);

            SELECT (SELECT 2) * (SELECT 1 UNION SELECT 2 EXCEPT SELECT 1);

            WITH
                a := (SELECT 1 UNION SELECT 2 EXCEPT SELECT 1)
            SELECT (SELECT 2) * a;
        """, [
            [2, 4],
            [2, 4],
            [4],
            [4],
        ])
